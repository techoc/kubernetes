/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package exec 实现 client-go 的 "exec" 凭据插件：运行用户提供的可执行文件
// 获取 bearer token 或客户端证书，并在过期前轮换。
//
// exec 包实现 kubeconfig 中的 exec 认证方式：调用用户指定的可执行文件，
// 由其在 stdout 输出 client.authentication.k8s.io 组的 ExecCredential 对象，
// 客户端据此获得 bearer token 或客户端证书。凭据会按 expirationTimestamp 过期，
// 并在必要时重新执行插件；同一进程内对相同配置复用 Authenticator 与 TLS 连接。
package exec

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"golang.org/x/term"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/pkg/apis/clientauthentication"
	"k8s.io/client-go/pkg/apis/clientauthentication/install"
	clientauthenticationv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
	clientauthenticationv1beta1 "k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/client-go/transport"
	"k8s.io/client-go/util/connrotation"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/dump"
)

// execInfoEnv 是传给插件的环境变量名，值为序列化后的 ExecCredential（含 spec）。
const execInfoEnv = "KUBERNETES_EXEC_INFO"
const installHintVerboseHelp = `

It looks like you are trying to use a client-go credential plugin that is not installed.

To learn more about this feature, consult the documentation available at:
      https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins`

// scheme/codecs 用于编解码 ExecCredential；init 中安装外部版本以完成转换。
var scheme = runtime.NewScheme()
var codecs = serializer.NewCodecFactory(scheme)

func init() {
	install.Install(scheme)
}

var (
	// kubectl 等程序会反复重建 transport，因此按配置哈希缓存已初始化的认证器。
	//
	// 缓存以配置内容的 dump 为键，避免相同配置反复创建 Authenticator 与 TLS 连接池。
	globalCache = newCache()
	// 本包接受的 ExecCredential API 版本列表；未列出的版本会在创建时被拒绝。
	apiVersions = map[string]schema.GroupVersion{
		clientauthenticationv1beta1.SchemeGroupVersion.String(): clientauthenticationv1beta1.SchemeGroupVersion,
		clientauthenticationv1.SchemeGroupVersion.String():      clientauthenticationv1.SchemeGroupVersion,
	}
)

func newCache() *cache {
	return &cache{m: make(map[string]*Authenticator)}
}

// cacheKey 用配置与集群信息的结构化 dump 生成稳定键；包含 cluster 是因为
// provideClusterInfo 时插件输出可能依赖它。
func cacheKey(conf *api.ExecConfig, cluster *clientauthentication.Cluster) string {
	key := struct {
		conf    *api.ExecConfig
		cluster *clientauthentication.Cluster
	}{
		conf:    conf,
		cluster: cluster,
	}
	return dump.Pretty(key)
}

// cache 是并发安全的 Authenticator 缓存。
type cache struct {
	mu sync.Mutex
	m  map[string]*Authenticator
}

func (c *cache) get(s string) (*Authenticator, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.m[s]
	return a, ok
}

// put 把认证器放入缓存；若 key 已关联一个认证器，则返回已有的那个。
//
// 同一配置并发创建时只保留先到者，保证进程内共享同一个 Authenticator 与连接跟踪器。
func (c *cache) put(s string, a *Authenticator) *Authenticator {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.m[s]
	if ok {
		return existing
	}
	c.m[s] = a
	return a
}

// sometimes 是固定窗口限流器：每个 interval 内最多执行 threshold 次。
//
// sometimes 是固定窗口限流器：每个 interval 内最多执行 threshold 次。
// 用于限制安装提示等日志的输出频率。
type sometimes struct {
	threshold int
	interval  time.Duration

	clock clock.Clock
	mu    sync.Mutex

	count  int       // 本窗口内调用 f() 的次数。
	window time.Time // 长度为 interval 的当前窗口起点。
}

func (s *sometimes) Do(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	if s.window.IsZero() {
		s.window = now
	}

	// 离开已保存的时间窗口后，把计数重置为 0 重新累计。
	if inWindow := now.Sub(s.window) < s.interval; !inWindow {
		s.window = now
		s.count = 0
	}

	// 当前窗口内执行次数未超过 threshold，就立即执行。
	if underThreshold := s.count < s.threshold; underThreshold {
		s.count++
		f()
	}
}

// GetAuthenticator 返回基于 exec 插件提供客户端凭据的认证器。
func GetAuthenticator(config *api.ExecConfig, cluster *clientauthentication.Cluster) (*Authenticator, error) {
	metrics.EnsureRegistered()
	return newAuthenticator(globalCache, term.IsTerminal, config, cluster)
}

// newAuthenticator 构造（或复用）Authenticator。可注入 cache 与 isTerminalFunc 以便测试。
func newAuthenticator(c *cache, isTerminalFunc func(int) bool, config *api.ExecConfig, cluster *clientauthentication.Cluster) (*Authenticator, error) {
	key := cacheKey(config, cluster)
	if a, ok := c.get(key); ok {
		return a, nil
	}

	gv, ok := apiVersions[config.APIVersion]
	if !ok {
		return nil, fmt.Errorf("exec plugin: invalid apiVersion %q", config.APIVersion)
	}

	// 连接跟踪器用于在客户端证书轮换后关闭旧连接，迫使后续请求使用新证书。
	connTracker := connrotation.NewConnectionTracker()
	defaultDialer := connrotation.NewDialerWithTracker(
		(&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		connTracker,
	)

	if err := ValidatePluginPolicy(config.PluginPolicy); err != nil {
		return nil, fmt.Errorf("invalid plugin policy: %w", err)
	}

	// 仅把显式列出的命令加入查找集；其余条目不预置，避免未经解析的路径被信任。
	allowlistLookup := sets.New[string]()
	for _, entry := range config.PluginPolicy.Allowlist {
		if entry.Command != "" {
			allowlistLookup.Insert(entry.Command)
		}
	}

	a := &Authenticator{
		// 调用 Clean 规范化路径，便于与允许列表比较（若配置了允许列表）。
		cmd:                filepath.Clean(config.Command),
		args:               config.Args,
		group:              gv,
		cluster:            cluster,
		provideClusterInfo: config.ProvideClusterInfo,

		allowlistLookup:  allowlistLookup,
		execPluginPolicy: config.PluginPolicy,

		installHint: config.InstallHint,
		sometimes: &sometimes{
			threshold: 10,
			interval:  time.Hour,
			clock:     clock.RealClock{},
		},

		stdin:           os.Stdin,
		stderr:          os.Stderr,
		interactiveFunc: func() (bool, error) { return isInteractive(isTerminalFunc, config) },
		now:             time.Now,
		environ:         os.Environ,

		connTracker: connTracker,
	}

	for _, env := range config.Env {
		a.env = append(a.env, env.Name+"="+env.Value)
	}

	// 这些函数被做成可比较并存入缓存，使相同 rest.Config 反复构造 clientset 时
	// 只产生一份 TLS 缓存与 Authenticator。
	//
	// 以指针形式持有回调，使 transport.Config 可比较，从而命中 TLS 配置缓存。
	a.getCert = &transport.GetCertHolder{GetCert: a.cert}
	a.dial = &transport.DialHolder{Dial: defaultDialer.DialContext}

	return c.put(key, a), nil
}

// isInteractive 根据 interactiveMode 判断是否需要为插件提供交互式 stdin。
// stdin 不可用或非终端时会给出明确错误，而不是让插件静默挂起等待输入。
func isInteractive(isTerminalFunc func(int) bool, config *api.ExecConfig) (bool, error) {
	var shouldBeInteractive bool
	switch config.InteractiveMode {
	case api.NeverExecInteractiveMode:
		shouldBeInteractive = false
	case api.IfAvailableExecInteractiveMode:
		shouldBeInteractive = !config.StdinUnavailable && isTerminalFunc(int(os.Stdin.Fd()))
	case api.AlwaysExecInteractiveMode:
		if !isTerminalFunc(int(os.Stdin.Fd())) {
			return false, errors.New("standard input is not a terminal")
		}
		if config.StdinUnavailable {
			suffix := ""
			if len(config.StdinUnavailableMessage) > 0 {
				// 只有用户确实指定了消息，才额外打印 ": <message>"。
				suffix = fmt.Sprintf(": %s", config.StdinUnavailableMessage)
			}
			return false, fmt.Errorf("standard input is unavailable%s", suffix)
		}
		shouldBeInteractive = true
	default:
		return false, fmt.Errorf("unknown interactiveMode: %q", config.InteractiveMode)
	}

	return shouldBeInteractive, nil
}

// Authenticator 通过执行插件获取并轮换客户端凭据。插件的输入输出由 API 组
// client.authentication.k8s.io 定义。
//
// Authenticator 通过执行插件获取并轮换客户端凭据。并发安全：mu 同时保护缓存与插件调用，
// 以确保交互式插件不会被并发执行多次。
type Authenticator struct {
	// 由配置设置。
	cmd                string
	args               []string
	group              schema.GroupVersion
	env                []string
	cluster            *clientauthentication.Cluster
	provideClusterInfo bool

	// allowlistLookup 是已解析的允许命令集合；execPluginPolicy 是原始策略用于校验。
	allowlistLookup  sets.Set[string]
	execPluginPolicy api.PluginPolicy

	// 对安装提示的打印做限流以免日志刷屏；不能只用固定间隔限流，否则 kubectl 用户
	// 可能根本看不到提示。
	sometimes   *sometimes
	installHint string

	// 可替换以便测试。
	stdin           io.Reader
	stderr          io.Writer
	interactiveFunc func() (bool, error)
	now             func() time.Time
	environ         func() []string

	// connTracker 记录所有需要轮换证书时关闭的连接。
	connTracker *connrotation.ConnectionTracker

	// 缓存的结果。
	//
	// 互斥锁也保护插件调用。插件可能是交互式的，必须确保同一时刻只调用一次。
	mu          sync.Mutex
	cachedCreds *credentials
	exp         time.Time

	// getCert 让 Authenticator.cert 可比较，以支持 TLS 配置缓存。
	getCert *transport.GetCertHolder
	// dial 供未指定自定义 dialer 的客户端使用；它可比较，以支持 TLS 配置缓存。
	dial *transport.DialHolder
}

// credentials 是插件返回的凭据；datapolicy 标签确保其可被脱敏工具识别。
type credentials struct {
	token string           `datapolicy:"token"`
	cert  *tls.Certificate `datapolicy:"secret-key"`
}

// UpdateTransportConfig 更新 transport.Config 以使用插件返回的凭据。
//
// UpdateTransportConfig 把 Authenticator 注入 transport.Config：安装包装 RoundTripper，
// 并设置证书回调与拨号器。若用户已显式提供 token/basic/cert 认证，
// 则完全跳过，以尊重调用方的显式选择。
func (a *Authenticator) UpdateTransportConfig(c *transport.Config) error {
	// 若请求中已带 bearer token，设置 transport 时避免触发 GetCert 回调——服务端同时
	// 允许证书认证时它会触发 exec 插件。对于 "kubectl get --token (token) pods" 这类
	// 命令，应认为用户意图是用给定 token 认证。basic auth 或证书认证同理。
	if c.HasTokenAuth() || c.HasBasicAuth() || c.HasCertAuth() {
		return nil
	}

	c.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &roundTripper{a, rt}
	})

	if c.HasCertCallback() {
		return errors.New("can't add TLS certificate callback: transport.Config.TLS.GetCert already set")
	}
	c.TLS.GetCertHolder = a.getCert // 可比较，以支持 TLS 配置缓存。

	if c.DialHolder != nil {
		if c.DialHolder.Dial == nil {
			return errors.New("invalid transport.Config.DialHolder: wrapped Dial function is nil")
		}

		// 这里配有自定义拨号器，必须把它包装起来并接入连接跟踪，以便证书轮换后能关闭旧连接；
		// 代价是该配置不支持 TLS config 缓存。
		d := connrotation.NewDialerWithTracker(c.DialHolder.Dial, a.connTracker)
		c.DialHolder = &transport.DialHolder{Dial: d.DialContext}
	} else {
		c.DialHolder = a.dial // 可比较，以支持 TLS 配置缓存。
	}

	return nil
}

// 编译期断言：roundTripper 实现了 utilnet.RoundTripperWrapper。
var _ utilnet.RoundTripperWrapper = &roundTripper{}

// roundTripper 负责在每次请求时注入 Bearer token，并在收到 401 时触发凭据刷新。
type roundTripper struct {
	a    *Authenticator
	base http.RoundTripper
}

func (r *roundTripper) WrappedRoundTripper() http.RoundTripper {
	return r.base
}

func (r *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// 用户已设置凭据时直接使用，以支持这类命令：
	// "kubectl get --token (token) pods" work.
	if req.Header.Get("Authorization") != "" {
		return r.base.RoundTrip(req)
	}

	creds, err := r.a.getCreds()
	if err != nil {
		return nil, fmt.Errorf("getting credentials: %v", err)
	}
	if creds.token != "" {
		req.Header.Set("Authorization", "Bearer "+creds.token)
	}

	res, err := r.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	// 401 可能表示服务端认为凭据已失效（如提前吊销），尽力刷新一次供后续请求使用；
	// 本次请求仍按原响应返回，避免重试带来副作用。
	if res.StatusCode == http.StatusUnauthorized {
		if err := r.a.maybeRefreshCreds(creds); err != nil {
			klog.Errorf("refreshing credentials: %v", err)
		}
	}
	return res, nil
}

// credsExpired 判断缓存凭据是否已过期；exp 为零值表示永不过期。
func (a *Authenticator) credsExpired() bool {
	if a.exp.IsZero() {
		return false
	}
	return a.now().After(a.exp)
}

// cert 实现 transport 的 GetCert 回调，按需返回客户端证书。
func (a *Authenticator) cert() (*tls.Certificate, error) {
	creds, err := a.getCreds()
	if err != nil {
		return nil, err
	}
	return creds.cert, nil
}

// getCreds 返回缓存凭据；缓存缺失或已过期时执行插件刷新。持锁调用插件。
// 保证并发场景下插件只被执行一次（尤其是交互式插件）。
func (a *Authenticator) getCreds() (*credentials, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cachedCreds != nil && !a.credsExpired() {
		return a.cachedCreds, nil
	}

	if err := a.refreshCredsLocked(); err != nil {
		return nil, err
	}

	return a.cachedCreds, nil
}

// maybeRefreshCreds 执行插件强制轮换凭据，除非凭据已经被轮换过。
func (a *Authenticator) maybeRefreshCreds(creds *credentials) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// getCreds 不会给 a.cachedCreds 换新指针，所以这里无需深比较：
	// 指针不一致说明期间已被其他请求刷新，无需重复执行插件。
	if creds != a.cachedCreds {
		// 凭据已轮换。
		return nil
	}

	return a.refreshCredsLocked()
}

// refreshCredsLocked 执行插件并从 stdout 读取凭据；调用时必须持有 Authenticator 的互斥锁。
//
// refreshCredsLocked 执行插件、校验输出并更新缓存。调用前必须持有 a.mu。
func (a *Authenticator) refreshCredsLocked() error {
	interactive, err := a.interactiveFunc()
	if err != nil {
		return fmt.Errorf("exec plugin cannot support interactive mode: %w", err)
	}

	// 通过 KUBERNETES_EXEC_INFO 把 spec 传给插件（API 版本、集群信息、interactive 标志）。
	cred := &clientauthentication.ExecCredential{
		Spec: clientauthentication.ExecCredentialSpec{
			Interactive: interactive,
		},
	}
	if a.provideClusterInfo {
		cred.Spec.Cluster = a.cluster
	}

	env := append(a.environ(), a.env...)
	data, err := runtime.Encode(codecs.LegacyCodec(a.group), cred)
	if err != nil {
		return fmt.Errorf("encode ExecCredentials: %v", err)
	}
	env = append(env, fmt.Sprintf("%s=%s", execInfoEnv, data))

	stdout := &bytes.Buffer{}
	cmd := exec.Command(a.cmd, a.args...)
	cmd.Env = env
	cmd.Stderr = a.stderr
	cmd.Stdout = stdout
	// 仅交互模式才暴露 stdin，避免插件在无人值守场景下阻塞读取输入。
	if interactive {
		cmd.Stdin = a.stdin
	}

	// 执行前先按插件策略检查/解析可执行文件路径；这是安全边界，必须在 Run 之前完成。
	err = a.updateCommandAndCheckAllowlistLocked(cmd)
	incrementPolicyMetric(err)
	if err != nil {
		return err
	}

	err = cmd.Run()
	incrementCallsMetric(err)
	if err != nil {
		return a.wrapCmdRunErrorLocked(err)
	}

	_, gvk, err := codecs.UniversalDecoder(a.group).Decode(stdout.Bytes(), nil, cred)
	if err != nil {
		return fmt.Errorf("decoding stdout: %v", err)
	}
	// 拒绝版本不匹配的输出：不同版本的语义不同，混用会导致难以排查的问题。
	if gvk.Group != a.group.Group || gvk.Version != a.group.Version {
		return fmt.Errorf("exec plugin is configured to use API version %s, plugin returned version %s",
			a.group, schema.GroupVersion{Group: gvk.Group, Version: gvk.Version})
	}

	if cred.Status == nil {
		return fmt.Errorf("exec plugin didn't return a status field")
	}
	if cred.Status.Token == "" && cred.Status.ClientCertificateData == "" && cred.Status.ClientKeyData == "" {
		return fmt.Errorf("exec plugin didn't return a token or cert/key pair")
	}
	// 证书与私钥必须成对出现，否则 tls.X509KeyPair 会失败且错误信息不直观。
	if (cred.Status.ClientCertificateData == "") != (cred.Status.ClientKeyData == "") {
		return fmt.Errorf("exec plugin returned only certificate or key, not both")
	}

	if cred.Status.ExpirationTimestamp != nil {
		a.exp = cred.Status.ExpirationTimestamp.Time
	} else {
		a.exp = time.Time{}
	}

	newCreds := &credentials{
		token: cred.Status.Token,
	}
	if cred.Status.ClientKeyData != "" && cred.Status.ClientCertificateData != "" {
		cert, err := tls.X509KeyPair([]byte(cred.Status.ClientCertificateData), []byte(cred.Status.ClientKeyData))
		if err != nil {
			return fmt.Errorf("failed parsing client key/certificate: %v", err)
		}

		// Leaf 初始化为 nil。
		//  https://golang.org/pkg/crypto/tls/#X509KeyPair
		// 第一张证书就是叶证书。
		//  https://golang.org/pkg/crypto/tls/#Certificate
		// 填充 leaf 便于快速访问底层 x509 证书值。
		//
		// 提前解析叶子证书以便读取 NotBefore/NotAfter，无需每次重新解析。
		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return fmt.Errorf("failed parsing client leaf certificate: %v", err)
		}
		newCreds.cert = &cert
	}

	oldCreds := a.cachedCreds
	a.cachedCreds = newCreds
	// 只有在 TLS 证书轮换时才关闭所有连接（mTLS 下旧连接仍持有旧证书）；
	// token 轮换通过请求头即可生效，不必制造这些额外的断连噪音。
	if oldCreds != nil && !reflect.DeepEqual(oldCreds.cert, a.cachedCreds.cert) {
		// exec 插件只返回 token 认证时，这里可能为 nil。
		if oldCreds.cert != nil && oldCreds.cert.Leaf != nil {
			metrics.ClientCertRotationAge.Observe(time.Since(oldCreds.cert.Leaf.NotBefore))
		}
		a.connTracker.CloseAll()
	}

	expiry := time.Time{}
	if a.cachedCreds.cert != nil && a.cachedCreds.cert.Leaf != nil {
		expiry = a.cachedCreds.cert.Leaf.NotAfter
	}
	expirationMetrics.set(a, expiry)
	return nil
}

// wrapCmdRunErrorLocked 把“插件二进制 Run() 失败”的处理逻辑抽出来，构造更有用的
// 错误消息。调用时必须持有 Authenticator 的互斥锁。
func (a *Authenticator) wrapCmdRunErrorLocked(err error) error {
	switch err.(type) {
	case *exec.Error: // 二进制不存在（见 exec.Error）。
		// 可执行文件缺失是最常见的用户错误，给出安装指引；
		// 通过 sometimes 限流避免每条请求都重复打印长提示。
		builder := strings.Builder{}
		fmt.Fprintf(&builder, "exec: executable %s not found", a.cmd)

		a.sometimes.Do(func() {
			fmt.Fprint(&builder, installHintVerboseHelp)
			if a.installHint != "" {
				fmt.Fprintf(&builder, "\n\n%s", a.installHint)
			}
		})

		return errors.New(builder.String())

	case *exec.ExitError: // 二进制执行失败（见 exec.Cmd.Run()）。
		e := err.(*exec.ExitError)
		return fmt.Errorf(
			"exec: executable %s failed with exit code %d",
			a.cmd,
			e.ProcessState.ExitCode(),
		)

	default:
		return fmt.Errorf("exec: %v", err)
	}
}

// `updateCommandAndCheckAllowlistLocked` 判断指定可执行文件是否允许运行。
// 按凭据插件策略检查：允许时返回 `nil`，否则返回解释原因的错误。
func (a *Authenticator) updateCommandAndCheckAllowlistLocked(cmd *exec.Cmd) error {
	switch a.execPluginPolicy.PolicyType {
	// "" 与 AllowAll 等价，保持向后兼容。
	case "", api.PluginPolicyAllowAll:
		return nil
	case api.PluginPolicyDenyAll:
		return fmt.Errorf("plugin %q not allowed: policy set to %q", a.cmd, api.PluginPolicyDenyAll)
	case api.PluginPolicyAllowlist:
		return a.checkAllowlistLocked(cmd)
	default:
		return fmt.Errorf("unknown plugin policy %q", a.execPluginPolicy.PolicyType)
	}
}

// `checkAllowlistLocked` 按允许列表检查指定插件。
// 并可能更新 Authenticator 的 allowlistLookup 集合。
//
// 检查同时接受配置中的原始命令与 exec 解析后的绝对路径，
// 并在必要时把允许列表项解析为绝对路径后再比较，防止 PATH 差异造成误判。
func (a *Authenticator) checkAllowlistLocked(cmd *exec.Cmd) error {
	// a.cmd 是配置中指定的原始命令经 filepath.Clean() 处理的结果；cmd.Path 是可能已
	// 解析的路径。任一在允许列表中精确匹配即返回成功。
	if a.allowlistLookup.Has(a.cmd) || a.allowlistLookup.Has(cmd.Path) {
		return nil
	}

	var cmdResolvedPath string
	var cmdResolvedErr error
	if cmd.Path != a.cmd {
		// cmd.Path 变了，直接使用已解析的 LookPath 结果。
		cmdResolvedPath = cmd.Path
		cmdResolvedErr = cmd.Err
	} else {
		// cmd.Path 未变，自行执行 LookPath。
		cmdResolvedPath, cmdResolvedErr = exec.LookPath(cmd.Path)
		// 把 cmd.Path 更新为 cmdResolvedPath，只执行已解析出的路径，
		// 避免检查与执行之间 PATH 变化导致 TOCTOU。
		if cmdResolvedPath != "" {
			cmd.Path = cmdResolvedPath
		}
	}

	if cmdResolvedErr != nil {
		return fmt.Errorf("plugin path %q cannot be resolved for credential plugin allowlist check: %w", cmd.Path, cmdResolvedErr)
	}

	// cmdResolvedPath 可能变了，变化后的值可能位于允许列表中。
	if a.allowlistLookup.Has(cmdResolvedPath) {
		return nil
	}

	// 没有逐字匹配：允许列表中的条目可能也是相对命令名，尝试解析它们再比对。
	a.resolveAllowListEntriesLocked(cmd.Path)

	// allowlistLookup 可能变了，重新检查。
	if a.allowlistLookup.Has(cmdResolvedPath) {
		return nil
	}

	return fmt.Errorf("plugin path %q is not permitted by the credential plugin allowlist", cmd.Path)
}

// resolveAllowListEntriesLocked 用 LookPath 解析允许列表条目，把成功解析的条目加入
// allowlistLookup。可选的 commandHint 可限定只解析与提示 basename 匹配的条目。
func (a *Authenticator) resolveAllowListEntriesLocked(commandHint string) {
	hintName := filepath.Base(commandHint)
	for _, entry := range a.execPluginPolicy.Allowlist {
		entryBasename := filepath.Base(entry.Command)
		if hintName != "" && hintName != entryBasename {
			// 有提示但该允许列表条目不匹配。
			continue
		}
		entryResolvedPath, err := exec.LookPath(entry.Command)
		if err != nil {
			klog.V(5).ErrorS(err, "resolving credential plugin allowlist", "name", entry.Command)
			continue
		}
		if entryResolvedPath != "" {
			a.allowlistLookup.Insert(entryResolvedPath)
		}
	}
}

// ValidatePluginPolicy 校验插件策略配置的合法性与自洽性。
// 由于这是安全开关，遇到疑似拼写错误时立即失败而非退化处理。
func ValidatePluginPolicy(policy api.PluginPolicy) error {
	switch policy.PolicyType {
	// "" 等价于 "AllowAll"。
	case "", api.PluginPolicyAllowAll, api.PluginPolicyDenyAll:
		if policy.Allowlist != nil {
			return fmt.Errorf("misconfigured credential plugin allowlist: plugin policy is %q but allowlist is non-nil", policy.PolicyType)
		}
		return nil
	case api.PluginPolicyAllowlist:
		return validateAllowlist(policy.Allowlist)
	default:
		return fmt.Errorf("unknown plugin policy: %q", policy.PolicyType)
	}
}

// 空条目用于识别零值条目，例如用户写了 `- {}`。
var emptyAllowlistEntry api.AllowlistEntry

func validateAllowlist(list []api.AllowlistEntry) error {
	// 用户把字段名拼错时会出现这种情况：
	// 允许列表是安全相关配置，用户配错时应立即失败，而不是继续执行。
	if list == nil {
		return fmt.Errorf("credential plugin policy set to %q, but allowlist is unspecified", api.PluginPolicyAllowlist)
	}

	// 空允许列表等价于拒绝一切，但语义容易误读，明确建议改用 DenyAll。
	if len(list) == 0 {
		return fmt.Errorf("credential plugin policy set to %q, but allowlist is empty; use %q policy instead", api.PluginPolicyAllowlist, api.PluginPolicyDenyAll)
	}

	for i, item := range list {
		if item == emptyAllowlistEntry {
			return fmt.Errorf("misconfigured credential plugin allowlist: empty allowlist entry #%d", i+1)
		}

		// 要求条目已规范化，避免同一路径因写法不同而绕过或误伤比较。
		if cleaned := filepath.Clean(item.Command); cleaned != item.Command {
			return fmt.Errorf("non-normalized file path: %q vs %q", item.Command, cleaned)
		} else if item.Command == "" {
			return fmt.Errorf("empty file path: %q", item.Command)
		}
	}

	return nil
}
