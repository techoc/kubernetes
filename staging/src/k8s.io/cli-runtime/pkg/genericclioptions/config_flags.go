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

package genericclioptions

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/discovery"
	diskcached "k8s.io/client-go/discovery/cached/disk"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/utils/ptr"
)

// 这些 flag 名称属于 kubectl 对外的稳定 CLI 契约，外部脚本和文档都按名字引用，改动即破坏兼容性。
const (
	flagClusterName          = "cluster"
	flagAuthInfoName         = "user"
	flagContext              = "context"
	flagNamespace            = "namespace"
	flagAPIServer            = "server"
	flagTLSServerName        = "tls-server-name"
	flagInsecure             = "insecure-skip-tls-verify"
	flagCertFile             = "client-certificate"
	flagKeyFile              = "client-key"
	flagCAFile               = "certificate-authority"
	flagBearerToken          = "token"
	flagImpersonate          = "as"
	flagImpersonateUID       = "as-uid"
	flagImpersonateGroup     = "as-group"
	flagImpersonateUserExtra = "as-user-extra"
	flagUsername             = "username"
	flagPassword             = "password"
	flagTimeout              = "request-timeout"
	flagCacheDir             = "cache-dir"
	flagDisableCompression   = "disable-compression"
	flagProxyURL             = "proxy-url"
)

// RESTClientGetter 把命令真正需要的能力（REST 配置、发现客户端、REST 映射、原始 kubeconfig 加载器）抽成接口，
// 既方便命令侧 mock，也消除对具体结构体类型的直接耦合。
// 按 Go 接口重叠的惯例，使用方也可以在自己的包里复制一份同样的类型定义。
type RESTClientGetter interface {
	// ToRESTConfig 返回最终用于连接集群的 rest.Config（已应用 flag 覆盖与 WrapConfigFn）。
	ToRESTConfig() (*rest.Config, error)
	// ToDiscoveryClient 返回带缓存的发现客户端；持久化模式下同一实例会被复用。
	ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error)
	// ToRESTMapper 返回在发现客户端之上构建的 REST 映射器，用于 GVK 与资源路径互转。
	ToRESTMapper() (meta.RESTMapper, error)
	// ToRawKubeConfigLoader 原样返回 kubeconfig 加载器，供需要自行组合加载规则的调用方使用。
	ToRawKubeConfigLoader() clientcmd.ClientConfig
}

var _ RESTClientGetter = &ConfigFlags{}

// ConfigFlags 汇集获取 REST 客户端配置所需的一组取值。
// 各字段存放 flag 指针，AddFlags 绑定后由用户输入写入。字段为 nil 表示该 flag 不注册，
// 命令可借此裁剪暴露给用户的选项。
type ConfigFlags struct {
	// 发现缓存目录，优先级：显式 flag > KUBECACHEDIR > ~/.kube/cache。
	CacheDir *string
	// 显式 kubeconfig 路径；非空时覆盖默认加载规则。
	KubeConfig *string

	// 以下为 kubeconfig 覆盖项，分身份认证、集群连接、上下文三类。
	ClusterName          *string
	AuthInfoName         *string
	Context              *string
	Namespace            *string
	APIServer            *string
	TLSServerName        *string
	Insecure             *bool
	CertFile             *string
	KeyFile              *string
	CAFile               *string
	BearerToken          *string
	Impersonate          *string
	ImpersonateUID       *string
	ImpersonateGroup     *[]string
	ImpersonateUserExtra *[]string
	Username             *string
	Password             *string
	Timeout              *string
	DisableCompression   *bool
	ProxyURL             *string
	// WrapConfigFn 非 nil 时，ToRESTConfig 会在返回配置前用它转换 Config，
	// 相当于给调用方一个钩子，统一注入自定义行为（如客户端限流、用户代理）。
	WrapConfigFn func(*rest.Config) *rest.Config

	// 持久化模式下缓存的原始 kubeconfig 加载器，用互斥锁保护懒初始化。
	clientConfig     clientcmd.ClientConfig
	clientConfigLock sync.Mutex

	// 持久化模式下缓存的 REST 映射器。
	restMapper     meta.RESTMapper
	restMapperLock sync.Mutex

	// 持久化模式下缓存的发现客户端。
	discoveryClient     discovery.CachedDiscoveryInterface
	discoveryClientLock sync.Mutex

	// usePersistentConfig 为 true 时复用同一份 client config、REST 映射器和发现客户端，
	// 并把它们传递给需要的一方，避免一次命令里重复构建这些对象、重复打发现请求。
	usePersistentConfig bool
	// Allows increasing burst used for discovery, this is useful
	// in clusters with many registered resources
	discoveryBurst int
	// Allows increasing qps used for discovery, this is useful
	// in clusters with many registered resources
	discoveryQPS float32
	// warningPrinter 用于以标准格式输出所有可能的警告，
	// 例如发现过程中（废弃 API 用法）的警告，统一打到 stderr。
	warningPrinter *printers.WarningPrinter
}

// ToRESTConfig implements RESTClientGetter.
// Returns a REST client configuration based on a provided path
// to a .kubeconfig file, loading rules, and config flag overrides.
// Expects the AddFlags method to have been called. If WrapConfigFn
// is non-nil this function can transform config before return.
func (f *ConfigFlags) ToRESTConfig() (*rest.Config, error) {
	// 先由 kubeconfig 加载规则叠加 flag 覆盖得到基础配置，再交给可选的包装函数做最终加工。
	c, err := f.ToRawKubeConfigLoader().ClientConfig()
	if err != nil {
		return nil, err
	}
	if f.WrapConfigFn != nil {
		return f.WrapConfigFn(c), nil
	}
	return c, nil
}

// ToRawKubeConfigLoader binds config flag values to config overrides
// Returns an interactive clientConfig if the password flag is enabled,
// or a non-interactive clientConfig otherwise.
func (f *ConfigFlags) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	// 持久化模式下复用同一加载器，避免一次命令中重复解析 kubeconfig 文件。
	if f.usePersistentConfig {
		return f.toRawKubePersistentConfigLoader()
	}
	return f.toRawKubeConfigLoader()
}

func (f *ConfigFlags) toRawKubeConfigLoader() clientcmd.ClientConfig {
	// 使用标准默认加载顺序（KUBECONFIG、~/.kube/config 等），保证与其他 kubectl 工具一致。
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	// use the standard defaults for this client command
	// DEPRECATED: remove and replace with something more accurate
	loadingRules.DefaultClientConfig = &clientcmd.DefaultClientConfig

	if f.KubeConfig != nil {
		// 显式路径优先级最高，会跳过默认的合并加载规则。
		loadingRules.ExplicitPath = *f.KubeConfig
	}

	overrides := &clientcmd.ConfigOverrides{ClusterDefaults: clientcmd.ClusterDefaults}

	// 把身份认证相关的 flag 值绑定到 overrides：文件类 flag 与内嵌数据互斥，指定文件时清空内嵌数据，避免加载器选错来源。
	if f.CertFile != nil {
		overrides.AuthInfo.ClientCertificate = *f.CertFile
		overrides.AuthInfo.ClientCertificateData = nil
	}
	if f.KeyFile != nil {
		overrides.AuthInfo.ClientKey = *f.KeyFile
		overrides.AuthInfo.ClientKeyData = nil
	}
	if f.BearerToken != nil {
		overrides.AuthInfo.Token = *f.BearerToken
		overrides.AuthInfo.TokenFile = ""
	}
	if f.Impersonate != nil {
		overrides.AuthInfo.Impersonate = *f.Impersonate
	}
	if f.ImpersonateUserExtra != nil && len(*f.ImpersonateUserExtra) > 0 {
		// 每个元素格式为 key=value，同一 key 可出现多次并累加成多值。
		userExtras := make(map[string][]string)
		for _, extra := range *f.ImpersonateUserExtra {
			parts := strings.SplitN(extra, "=", 2)
			if len(parts) != 2 {
				// 格式不合法则静默跳过，不阻断整条命令。
				continue
			}
			key := parts[0]
			value := parts[1]
			userExtras[key] = append(userExtras[key], value)
		}
		overrides.AuthInfo.ImpersonateUserExtra = userExtras
	}
	if f.ImpersonateUID != nil {
		overrides.AuthInfo.ImpersonateUID = *f.ImpersonateUID
	}
	if f.ImpersonateGroup != nil {
		overrides.AuthInfo.ImpersonateGroups = *f.ImpersonateGroup
	}
	if f.Username != nil {
		overrides.AuthInfo.Username = *f.Username
	}
	if f.Password != nil {
		overrides.AuthInfo.Password = *f.Password
	}

	// 绑定集群 flag：集群连接相关覆盖，直接映射到集群条目。
	if f.APIServer != nil {
		overrides.ClusterInfo.Server = *f.APIServer
	}
	if f.TLSServerName != nil {
		overrides.ClusterInfo.TLSServerName = *f.TLSServerName
	}
	if f.CAFile != nil {
		overrides.ClusterInfo.CertificateAuthority = *f.CAFile
	}
	if f.Insecure != nil {
		overrides.ClusterInfo.InsecureSkipTLSVerify = *f.Insecure
	}
	if f.DisableCompression != nil {
		overrides.ClusterInfo.DisableCompression = *f.DisableCompression
	}
	if f.ProxyURL != nil {
		overrides.ClusterInfo.ProxyURL = *f.ProxyURL
	}

	// 绑定上下文 flag：上下文覆盖，决定当前选中哪个 cluster/authinfo/namespace。
	if f.Context != nil {
		overrides.CurrentContext = *f.Context
	}
	if f.ClusterName != nil {
		overrides.Context.Cluster = *f.ClusterName
	}
	if f.AuthInfoName != nil {
		overrides.Context.AuthInfo = *f.AuthInfoName
	}
	if f.Namespace != nil {
		overrides.Context.Namespace = *f.Namespace
	}

	if f.Timeout != nil {
		overrides.Timeout = *f.Timeout
	}

	// 只有允许输入密码时才存在交互式提示：仅当注册了 --password 时才启用交互式加载，避免无终端场景阻塞等待输入。
	if f.Password == nil {
		return &clientConfig{clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)}
	}
	return &clientConfig{clientcmd.NewInteractiveDeferredLoadingClientConfig(loadingRules, overrides, os.Stdin)}
}

// toRawKubePersistentConfigLoader binds config flag values to config overrides
// Returns a persistent clientConfig for propagation.
func (f *ConfigFlags) toRawKubePersistentConfigLoader() clientcmd.ClientConfig {
	// 双检锁式懒初始化：只构建一次，之后所有调用方共享同一加载器。
	f.clientConfigLock.Lock()
	defer f.clientConfigLock.Unlock()

	if f.clientConfig == nil {
		f.clientConfig = f.toRawKubeConfigLoader()
	}

	return f.clientConfig
}

// ToDiscoveryClient implements RESTClientGetter.
// Expects the AddFlags method to have been called.
// Returns a CachedDiscoveryInterface using a computed RESTConfig.
func (f *ConfigFlags) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	if f.usePersistentConfig {
		return f.toPersistentDiscoveryClient()
	}
	return f.toDiscoveryClient()
}

func (f *ConfigFlags) toPersistentDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	// 缓存发现客户端可显著减少一次命令内的 discovery 请求。
	f.discoveryClientLock.Lock()
	defer f.discoveryClientLock.Unlock()

	if f.discoveryClient == nil {
		discoveryClient, err := f.toDiscoveryClient()
		if err != nil {
			return nil, err
		}
		f.discoveryClient = discoveryClient
	}
	return f.discoveryClient, nil
}

func (f *ConfigFlags) toDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	config, err := f.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	// 发现请求量大，单独设置 burst/QPS，避免被默认限流失真（尤其在大集群里）。
	config.Burst = f.discoveryBurst
	config.QPS = f.discoveryQPS

	cacheDir := getDefaultCacheDir()

	// retrieve a user-provided value for the "cache-dir"
	// override httpCacheDir and discoveryCacheDir if user-value is given.
	// user-provided value has higher precedence than default
	// and KUBECACHEDIR environment variable.
	if f.CacheDir != nil && *f.CacheDir != "" && *f.CacheDir != getDefaultCacheDir() {
		cacheDir = *f.CacheDir
	}

	httpCacheDir := filepath.Join(cacheDir, "http")
	// 按 host 分目录，避免不同集群的发现缓存互相污染。
	discoveryCacheDir := computeDiscoverCacheDir(filepath.Join(cacheDir, "discovery"), config.Host)

	// 缓存有效期 6 小时，在新鲜度与避免频繁重扫之间折中。
	return diskcached.NewCachedDiscoveryClientForConfig(config, discoveryCacheDir, httpCacheDir, time.Duration(6*time.Hour))
}

// getDefaultCacheDir returns default caching directory path.
// it first looks at KUBECACHEDIR env var if it is set, otherwise
// it returns standard kube cache dir.
func getDefaultCacheDir() string {
	// KUBECACHEDIR 作为环境级覆盖，便于测试或共享缓存目录。
	if kcd := os.Getenv("KUBECACHEDIR"); kcd != "" {
		return kcd
	}

	return filepath.Join(homedir.HomeDir(), ".kube", "cache")
}

// ToRESTMapper returns a mapper.
func (f *ConfigFlags) ToRESTMapper() (meta.RESTMapper, error) {
	if f.usePersistentConfig {
		return f.toPersistentRESTMapper()
	}
	return f.toRESTMapper()
}

func (f *ConfigFlags) toPersistentRESTMapper() (meta.RESTMapper, error) {
	// REST 映射器构建依赖发现，缓存它可以省掉重复的 mapping 查询。
	f.restMapperLock.Lock()
	defer f.restMapperLock.Unlock()

	if f.restMapper == nil {
		restMapper, err := f.toRESTMapper()
		if err != nil {
			return nil, err
		}
		f.restMapper = restMapper
	}
	return f.restMapper, nil
}

func (f *ConfigFlags) toRESTMapper() (meta.RESTMapper, error) {
	discoveryClient, err := f.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}

	// 延迟发现：首次解析时才拉取 API 列表，未用到映射的命令不必付出这笔开销。
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)
	// 包装一层快捷名展开（如 "all"），并在遇到歧义时通过 warningPrinter 提示用户。
	expander := restmapper.NewShortcutExpander(mapper, discoveryClient, func(a string) {
		if f.warningPrinter != nil {
			f.warningPrinter.Print(a)
		}
	})
	return expander, nil
}

// AddFlags 把客户端配置 flag 绑定到给定的 flagset。
// 只为非 nil 字段注册 flag，nil 即表示该选项不暴露，供不同命令复用同一结构体。
func (f *ConfigFlags) AddFlags(flags *pflag.FlagSet) {
	if f.KubeConfig != nil {
		flags.StringVar(f.KubeConfig, "kubeconfig", *f.KubeConfig, "Path to the kubeconfig file to use for CLI requests.")
	}
	if f.CacheDir != nil {
		flags.StringVar(f.CacheDir, flagCacheDir, *f.CacheDir, "Default cache directory")
	}

	// add config options
	if f.CertFile != nil {
		flags.StringVar(f.CertFile, flagCertFile, *f.CertFile, "Path to a client certificate file for TLS")
	}
	if f.KeyFile != nil {
		flags.StringVar(f.KeyFile, flagKeyFile, *f.KeyFile, "Path to a client key file for TLS")
	}
	if f.BearerToken != nil {
		flags.StringVar(f.BearerToken, flagBearerToken, *f.BearerToken, "Bearer token for authentication to the API server")
	}
	if f.Impersonate != nil {
		flags.StringVar(f.Impersonate, flagImpersonate, *f.Impersonate, "Username to impersonate for the operation. User could be a regular user or a service account in a namespace.")
	}
	if f.ImpersonateUID != nil {
		flags.StringVar(f.ImpersonateUID, flagImpersonateUID, *f.ImpersonateUID, "UID to impersonate for the operation.")
	}
	if f.ImpersonateGroup != nil {
		flags.StringArrayVar(f.ImpersonateGroup, flagImpersonateGroup, *f.ImpersonateGroup, "Group to impersonate for the operation, this flag can be repeated to specify multiple groups.")
	}
	if f.ImpersonateUserExtra != nil {
		flags.StringArrayVar(f.ImpersonateUserExtra, flagImpersonateUserExtra, *f.ImpersonateUserExtra, "User extras to impersonate for the operation, this flag can be repeated to specify multiple values for the same key.")
	}
	if f.Username != nil {
		flags.StringVar(f.Username, flagUsername, *f.Username, "Username for basic authentication to the API server")
	}
	if f.Password != nil {
		flags.StringVar(f.Password, flagPassword, *f.Password, "Password for basic authentication to the API server")
	}
	if f.ClusterName != nil {
		flags.StringVar(f.ClusterName, flagClusterName, *f.ClusterName, "The name of the kubeconfig cluster to use")
	}
	if f.AuthInfoName != nil {
		flags.StringVar(f.AuthInfoName, flagAuthInfoName, *f.AuthInfoName, "The name of the kubeconfig user to use")
	}
	if f.Namespace != nil {
		flags.StringVarP(f.Namespace, flagNamespace, "n", *f.Namespace, "If present, the namespace scope for this CLI request")
	}
	if f.Context != nil {
		flags.StringVar(f.Context, flagContext, *f.Context, "The name of the kubeconfig context to use")
	}

	if f.APIServer != nil {
		flags.StringVarP(f.APIServer, flagAPIServer, "s", *f.APIServer, "The address and port of the Kubernetes API server")
	}
	if f.TLSServerName != nil {
		flags.StringVar(f.TLSServerName, flagTLSServerName, *f.TLSServerName, "Server name to use for server certificate validation. If it is not provided, the hostname used to contact the server is used")
	}
	if f.Insecure != nil {
		flags.BoolVar(f.Insecure, flagInsecure, *f.Insecure, "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure")
	}
	if f.CAFile != nil {
		flags.StringVar(f.CAFile, flagCAFile, *f.CAFile, "Path to a cert file for the certificate authority")
	}
	if f.Timeout != nil {
		flags.StringVar(f.Timeout, flagTimeout, *f.Timeout, "The length of time to wait before giving up on a single server request. Non-zero values should contain a corresponding time unit (e.g. 1s, 2m, 3h). A value of zero means don't timeout requests.")
	}
	if f.DisableCompression != nil {
		flags.BoolVar(f.DisableCompression, flagDisableCompression, *f.DisableCompression, "If true, opt-out of response compression for all requests to the server")
	}
	if f.ProxyURL != nil {
		flags.StringVar(f.ProxyURL, flagProxyURL, *f.ProxyURL, "Proxy URL to use for requests to the API server")
	}
}

// WithDeprecatedPasswordFlag 开启用户名/密码配置 flag。
// 用户名/密码属于已废弃的认证方式，默认不注册，只有显式调用才暴露。
func (f *ConfigFlags) WithDeprecatedPasswordFlag() *ConfigFlags {
	f.Username = ptr.To("")
	f.Password = ptr.To("")
	return f
}

// WithDiscoveryBurst sets the RESTClient burst for discovery.
func (f *ConfigFlags) WithDiscoveryBurst(discoveryBurst int) *ConfigFlags {
	f.discoveryBurst = discoveryBurst
	return f
}

// WithDiscoveryQPS sets the RESTClient QPS for discovery.
func (f *ConfigFlags) WithDiscoveryQPS(discoveryQPS float32) *ConfigFlags {
	f.discoveryQPS = discoveryQPS
	return f
}

// WithWrapConfigFn allows providing a wrapper function for the client Config.
func (f *ConfigFlags) WithWrapConfigFn(wrapConfigFn func(*rest.Config) *rest.Config) *ConfigFlags {
	f.WrapConfigFn = wrapConfigFn
	return f
}

// WithWarningPrinter initializes WarningPrinter with the given IOStreams
func (f *ConfigFlags) WithWarningPrinter(ioStreams genericiooptions.IOStreams) *ConfigFlags {
	f.warningPrinter = printers.NewWarningPrinter(ioStreams.ErrOut, printers.WarningPrinterOptions{Color: printers.AllowsColorOutput(ioStreams.ErrOut)})
	return f
}

// NewConfigFlags 返回已设置默认值的 ConfigFlags。
// 所有字段预置为空指针值，保证 AddFlags 能注册完整的 kubeconfig 选项集。
func NewConfigFlags(usePersistentConfig bool) *ConfigFlags {
	impersonateGroup := []string{}
	impersonateUserExtra := []string{}
	insecure := false
	disableCompression := false

	return &ConfigFlags{
		Insecure:   &insecure,
		Timeout:    ptr.To("0"),
		KubeConfig: ptr.To(""),

		CacheDir:             ptr.To(getDefaultCacheDir()),
		ClusterName:          ptr.To(""),
		AuthInfoName:         ptr.To(""),
		Context:              ptr.To(""),
		Namespace:            ptr.To(""),
		APIServer:            ptr.To(""),
		TLSServerName:        ptr.To(""),
		CertFile:             ptr.To(""),
		KeyFile:              ptr.To(""),
		CAFile:               ptr.To(""),
		BearerToken:          ptr.To(""),
		Impersonate:          ptr.To(""),
		ImpersonateUID:       ptr.To(""),
		ImpersonateGroup:     &impersonateGroup,
		ImpersonateUserExtra: &impersonateUserExtra,
		DisableCompression:   &disableCompression,
		ProxyURL:             ptr.To(""),

		usePersistentConfig: usePersistentConfig,
		// The more groups you have, the more discovery requests you need to make.
		// with a burst of 300, we will not be rate-limiting for most clusters but
		// the safeguard will still be here. This config is only used for discovery.
		discoveryBurst: 300,
	}
}

// overlyCautiousIllegalFileCharacters matches characters that *might* not be supported.  Windows is really restrictive, so this is really restrictive
var overlyCautiousIllegalFileCharacters = regexp.MustCompile(`[^(\w/.)]`)

// computeDiscoverCacheDir takes the parentDir and the host and comes up with a "usually non-colliding" name.
func computeDiscoverCacheDir(parentDir, host string) string {
	// 若 host 带有 scheme 则先去掉，只保留 host，作为缓存子目录名的基础。
	schemelessHost := strings.Replace(strings.Replace(host, "https://", "", 1), "http://", "", 1)
	// 再把非字母数字字符统一折叠替换。碰撞理论上可能但概率很低，即使碰撞，影响也很短暂。
	safeHost := overlyCautiousIllegalFileCharacters.ReplaceAllString(schemelessHost, "_")
	return filepath.Join(parentDir, safeHost)
}
