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

// Package exec implements the client-go "exec" credential plugin: it runs a
// user-provided binary to obtain bearer tokens or client certificates, and
// rotates them before expiration.
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
	// Since transports can be constantly re-initialized by programs like kubectl,
	// keep a cache of initialized authenticators keyed by a hash of their config.
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

// put inserts an authenticator into the cache. If an authenticator is already
// associated with the key, the first one is returned instead.
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

// sometimes rate limits how often a function f() is called. Specifically, Do()
// will run the provided function f() up to threshold times every interval
// duration.
//
// sometimes 是固定窗口限流器：每个 interval 内最多执行 threshold 次。
// 用于限制安装提示等日志的输出频率。
type sometimes struct {
	threshold int
	interval  time.Duration

	clock clock.Clock
	mu    sync.Mutex

	count  int       // times we have called f() in this window
	window time.Time // beginning of current window of length interval
}

func (s *sometimes) Do(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	if s.window.IsZero() {
		s.window = now
	}

	// If we are no longer in our saved time window, then we get to reset our run
	// count back to 0 and start increasing towards the threshold again.
	if inWindow := now.Sub(s.window) < s.interval; !inWindow {
		s.window = now
		s.count = 0
	}

	// If we have not run the function more than threshold times in this current
	// time window, we get to run it now!
	if underThreshold := s.count < s.threshold; underThreshold {
		s.count++
		f()
	}
}

// GetAuthenticator returns an exec-based plugin for providing client credentials.
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
		// Clean is called to normalize the path to facilitate comparison with
		// the allowlist, when present
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

	// these functions are made comparable and stored in the cache so that repeated clientset
	// construction with the same rest.Config results in a single TLS cache and Authenticator
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
				// only print extra ": <message>" if the user actually specified a message
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

// Authenticator is a client credential provider that rotates credentials by executing a plugin.
// The plugin input and output are defined by the API group client.authentication.k8s.io.
//
// Authenticator 通过执行插件获取并轮换客户端凭据。并发安全：mu 同时保护缓存与插件调用，
// 以确保交互式插件不会被并发执行多次。
type Authenticator struct {
	// Set by the config
	cmd                string
	args               []string
	group              schema.GroupVersion
	env                []string
	cluster            *clientauthentication.Cluster
	provideClusterInfo bool

	// allowlistLookup 是已解析的允许命令集合；execPluginPolicy 是原始策略用于校验。
	allowlistLookup  sets.Set[string]
	execPluginPolicy api.PluginPolicy

	// Used to avoid log spew by rate limiting install hint printing. We didn't do
	// this by interval based rate limiting alone since that way may have prevented
	// the install hint from showing up for kubectl users.
	sometimes   *sometimes
	installHint string

	// Stubbable for testing
	stdin           io.Reader
	stderr          io.Writer
	interactiveFunc func() (bool, error)
	now             func() time.Time
	environ         func() []string

	// connTracker tracks all connections opened that we need to close when rotating a client certificate
	connTracker *connrotation.ConnectionTracker

	// Cached results.
	//
	// The mutex also guards calling the plugin. Since the plugin could be
	// interactive we want to make sure it's only called once.
	mu          sync.Mutex
	cachedCreds *credentials
	exp         time.Time

	// getCert makes Authenticator.cert comparable to support TLS config caching
	getCert *transport.GetCertHolder
	// dial is used for clients which do not specify a custom dialer
	// it is comparable to support TLS config caching
	dial *transport.DialHolder
}

// credentials 是插件返回的凭据；datapolicy 标签确保其可被脱敏工具识别。
type credentials struct {
	token string           `datapolicy:"token"`
	cert  *tls.Certificate `datapolicy:"secret-key"`
}

// UpdateTransportConfig updates the transport.Config to use credentials
// returned by the plugin.
//
// UpdateTransportConfig 把 Authenticator 注入 transport.Config：安装包装 RoundTripper，
// 并设置证书回调与拨号器。若用户已显式提供 token/basic/cert 认证，
// 则完全跳过，以尊重调用方的显式选择。
func (a *Authenticator) UpdateTransportConfig(c *transport.Config) error {
	// If a bearer token is present in the request - avoid the GetCert callback when
	// setting up the transport, as that triggers the exec action if the server is
	// also configured to allow client certificates for authentication. For requests
	// like "kubectl get --token (token) pods" we should assume the intention is to
	// use the provided token for authentication. The same can be said for when the
	// user specifies basic auth or cert auth.
	if c.HasTokenAuth() || c.HasBasicAuth() || c.HasCertAuth() {
		return nil
	}

	c.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &roundTripper{a, rt}
	})

	if c.HasCertCallback() {
		return errors.New("can't add TLS certificate callback: transport.Config.TLS.GetCert already set")
	}
	c.TLS.GetCertHolder = a.getCert // comparable for TLS config caching

	if c.DialHolder != nil {
		if c.DialHolder.Dial == nil {
			return errors.New("invalid transport.Config.DialHolder: wrapped Dial function is nil")
		}

		// 这里配有自定义拨号器，必须把它包装起来并接入连接跟踪，以便证书轮换后能关闭旧连接；
		// 代价是该配置不支持 TLS config 缓存。
		d := connrotation.NewDialerWithTracker(c.DialHolder.Dial, a.connTracker)
		c.DialHolder = &transport.DialHolder{Dial: d.DialContext}
	} else {
		c.DialHolder = a.dial // comparable for TLS config caching
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
	// If a user has already set credentials, use that. This makes commands like
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

// getCreds 返回缓存凭据；缓存缺失或已过期时执行插件刷新。持锁调用插件，
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

// maybeRefreshCreds executes the plugin to force a rotation of the
// credentials, unless they were rotated already.
func (a *Authenticator) maybeRefreshCreds(creds *credentials) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// getCreds 不会给 a.cachedCreds 换新指针，所以这里无需深比较：
	// 指针不一致说明期间已被其他请求刷新，无需重复执行插件。
	if creds != a.cachedCreds {
		// Credentials already rotated.
		return nil
	}

	return a.refreshCredsLocked()
}

// refreshCredsLocked executes the plugin and reads the credentials from
// stdout. It must be called while holding the Authenticator's mutex.
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

		// Leaf is initialized to be nil:
		//  https://golang.org/pkg/crypto/tls/#X509KeyPair
		// Leaf certificate is the first certificate:
		//  https://golang.org/pkg/crypto/tls/#Certificate
		// Populating leaf is useful for quickly accessing the underlying x509
		// certificate values.
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
		// Can be nil if the exec auth plugin only returned token auth.
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

// wrapCmdRunErrorLocked pulls out the code to construct a helpful error message
// for when the exec plugin's binary fails to Run().
//
// It must be called while holding the Authenticator's mutex.
func (a *Authenticator) wrapCmdRunErrorLocked(err error) error {
	switch err.(type) {
	case *exec.Error: // Binary does not exist (see exec.Error).
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

	case *exec.ExitError: // Binary execution failed (see exec.Cmd.Run()).
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

// `updateCommandAndCheckAllowlistLocked` determines whether or not the specified executable may run
// according to the credential plugin policy. If the plugin is allowed, `nil`
// is returned. If the plugin is not allowed, an error must be returned
// explaining why.
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

// `checkAllowlistLocked` checks the specified plugin against the allowlist,
// and may update the Authenticator's allowlistLookup set.
//
// 检查同时接受配置中的原始命令与 exec 解析后的绝对路径，
// 并在必要时把允许列表项解析为绝对路径后再比较，防止 PATH 差异造成误判。
func (a *Authenticator) checkAllowlistLocked(cmd *exec.Cmd) error {
	// a.cmd is the original command as specified in the configuration, then filepath.Clean().
	// cmd.Path is the possibly-resolved command.
	// If either are an exact match in the allowlist, return success.
	if a.allowlistLookup.Has(a.cmd) || a.allowlistLookup.Has(cmd.Path) {
		return nil
	}

	var cmdResolvedPath string
	var cmdResolvedErr error
	if cmd.Path != a.cmd {
		// cmd.Path changed, use the already-resolved LookPath results
		cmdResolvedPath = cmd.Path
		cmdResolvedErr = cmd.Err
	} else {
		// cmd.Path is unchanged, do LookPath ourselves
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

	// cmdResolvedPath may have changed, and the changed value may be in the allowlist
	if a.allowlistLookup.Has(cmdResolvedPath) {
		return nil
	}

	// 没有逐字匹配：允许列表中的条目可能也是相对命令名，尝试解析它们再比对。
	a.resolveAllowListEntriesLocked(cmd.Path)

	// allowlistLookup may have changed, recheck
	if a.allowlistLookup.Has(cmdResolvedPath) {
		return nil
	}

	return fmt.Errorf("plugin path %q is not permitted by the credential plugin allowlist", cmd.Path)
}

// resolveAllowListEntriesLocked tries to resolve allowlist entries with LookPath,
// and adds successfully resolved entries to allowlistLookup.
// The optional commandHint can be used to limit which entries are resolved to ones which match the hint basename.
func (a *Authenticator) resolveAllowListEntriesLocked(commandHint string) {
	hintName := filepath.Base(commandHint)
	for _, entry := range a.execPluginPolicy.Allowlist {
		entryBasename := filepath.Base(entry.Command)
		if hintName != "" && hintName != entryBasename {
			// we got a hint, and this allowlist entry does not match it
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

// ValidatePluginPolicy 校验插件策略配置的合法性与自洽性；
// 由于这是安全开关，遇到疑似拼写错误时立即失败而非退化处理。
func ValidatePluginPolicy(policy api.PluginPolicy) error {
	switch policy.PolicyType {
	// "" is equivalent to "AllowAll"
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
	// This will be the case if the user has misspelled the field name for the
	// allowlist. Because this is a security knob, fail immediately rather than
	// proceed when the user has made a mistake.
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
