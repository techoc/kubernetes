/*
Copyright 2015 The Kubernetes Authors.

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

// Package options 包含 Kubelet 的主要启动参数。
package options

import (
	"fmt"
	_ "net/http/pprof" // 启用 pprof HTTP 处理器。
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cliflag "k8s.io/component-base/cli/flag"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubelet/config/v1beta1"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	"k8s.io/kubernetes/pkg/cluster/ports"
	kubeletconfigapi "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
	kubeletconfigvalidation "k8s.io/kubernetes/pkg/kubelet/apis/config/validation"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	utilflag "k8s.io/kubernetes/pkg/util/flag"
)

const defaultRootDir = "/var/lib/kubelet"

// KubeletFlags 保存节点级启动参数；与可共享的 KubeletConfiguration 区分，主要容纳不适合在节点间共享的本地信息。
// 满足以下任一条件的字段应放在 KubeletFlags，而不是 KubeletConfiguration：
//   - 字段值在节点生命周期内不会改变，或不能安全改变；
//   - 字段值不能同时在多个节点间安全共享，例如主机名。
//
// KubeletConfiguration 用于节点间共享。新增参数或配置字段前应尽量避免进一步扩大配置面。
type KubeletFlags struct {
	// KubeConfig 指向 API Server 连接的 kubeconfig；为空时 Kubelet 以独立模式运行。
	KubeConfig string
	// BootstrapKubeconfig 指向首次申请客户端证书时使用的引导凭据。
	BootstrapKubeconfig string

	// HostnameOverride 是用于标识 Kubelet 的主机名，优先于实际主机名。
	HostnameOverride string
	// NodeIP 是节点使用的 IP 地址；设置后 Kubelet 用它注册节点。
	NodeIP string

	// ContainerRuntimeOptions 保存容器运行时和镜像凭据提供程序的本地设置。
	kubeletconfig.ContainerRuntimeOptions

	// CertDirectory 是 TLS 证书目录；同时指定 tlsCertFile 和 tlsPrivateKeyFile 时忽略此设置。
	CertDirectory string

	// CloudProvider 是云服务提供程序名称。
	// +optional
	CloudProvider string

	// CloudConfigFile 是云服务提供程序配置文件路径。
	// +optional
	CloudConfigFile string

	// RootDirectory 是 Kubelet 存放卷挂载等本地文件的目录。
	RootDirectory string

	// KubeletConfigFile 指向 Kubelet 的初始配置文件。
	// 路径可为绝对路径或相对于 Kubelet 当前工作目录的路径；省略时使用内置默认值和参数。
	KubeletConfigFile string

	// KubeletDropinConfigDirectory 指向覆盖默认值及 --config 配置的 Drop-in 文件目录。
	// 相对路径以 Kubelet 当前工作目录为基准。目录及子目录中的 .conf 文件按字典序合并，其他文件忽略。
	KubeletDropinConfigDirectory string

	// WindowsService 表示 Kubelet 是否作为 Windows 服务运行；对应参数仅在 Windows 构建中注册。
	WindowsService bool

	// WindowsPriorityClass 设置 Kubelet 进程的 Windows 优先级类别；对应参数仅在 Windows 构建中注册。
	// 默认使用 NORMAL_PRIORITY_CLASS 以保持兼容。
	// Source: https://docs.microsoft.com/en-us/windows/win32/procthread/scheduling-priorities
	WindowsPriorityClass string

	// ExperimentalMounterPath 是 mounter 二进制路径；为空时使用默认挂载程序。
	ExperimentalMounterPath string
	// ExperimentalNodeAllocatableIgnoreEvictionThreshold 控制计算 Node Allocatable 时是否忽略 EvictionHard 限制。
	// 参见 [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)。
	ExperimentalNodeAllocatableIgnoreEvictionThreshold bool
	// NodeLabels 是注册节点时添加的标签。
	NodeLabels map[string]string
	// LockFilePath 是 Kubelet 用于互斥的锁文件路径，防止同一节点并发运行多个进程。
	LockFilePath string
	// ExitOnLockContention 表示 Kubelet 处于引导模式；要求已设置 LockFilePath。
	// 启用后监听锁文件的 inotify 事件，并在其他进程尝试打开锁文件时释放锁并退出。
	ExitOnLockContention bool
	// 已弃用参数。
	// MinimumGCAge 是已结束容器被垃圾回收前的最小保留时间。
	MinimumGCAge metav1.Duration
	// MaxPerPodContainerCount 是每个容器保留的旧实例上限；保留实例会占用磁盘空间。
	MaxPerPodContainerCount int32
	// MaxContainerCount 是全局保留的旧容器实例上限；保留实例会占用磁盘空间。
	MaxContainerCount int32
	// SeccompDefault 对节点上的所有工作负载启用 RuntimeDefault 作为默认 seccomp 配置。
	SeccompDefault bool
}

// NewKubeletFlags 创建 Kubelet 命令行参数的本地默认值。
func NewKubeletFlags() *KubeletFlags {
	return &KubeletFlags{
		ContainerRuntimeOptions: kubeletconfig.ContainerRuntimeOptions{},
		CertDirectory:           "/var/lib/kubelet/pki",
		RootDirectory:           filepath.Clean(defaultRootDir),
		MaxContainerCount:       -1,
		MaxPerPodContainerCount: 1,
		MinimumGCAge:            metav1.Duration{Duration: 0},
		NodeLabels:              make(map[string]string),
	}
}

// ValidateKubeletFlags 校验节点级参数，当前重点检查 --node-labels 的命名空间和键值格式。
func ValidateKubeletFlags(f *KubeletFlags) error {
	unknownLabels := sets.NewString()
	invalidLabelErrs := make(map[string][]string)
	for k, v := range f.NodeLabels {
		if isKubernetesLabel(k) && !kubeletapis.IsKubeletLabel(k) {
			unknownLabels.Insert(k)
		}

		if errs := validation.IsQualifiedName(k); len(errs) > 0 {
			invalidLabelErrs[k] = append(invalidLabelErrs[k], errs...)
		}
		if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
			invalidLabelErrs[v] = append(invalidLabelErrs[v], errs...)
		}
	}
	if len(unknownLabels) > 0 {
		return fmt.Errorf("unknown 'kubernetes.io' or 'k8s.io' labels specified with --node-labels: %v\n--node-labels in the 'kubernetes.io' namespace must begin with an allowed prefix (%s) or be in the specifically allowed set (%s)", unknownLabels.List(), strings.Join(kubeletapis.KubeletLabelNamespaces(), ", "), strings.Join(kubeletapis.KubeletLabels(), ", "))
	}
	if len(invalidLabelErrs) > 0 {
		labelErrs := []string{}
		for k, v := range invalidLabelErrs {
			labelErrs = append(labelErrs, fmt.Sprintf("'%s' - %s", k, strings.Join(v, ", ")))
		}
		return fmt.Errorf("invalid node labels: %s", strings.Join(labelErrs, "; "))
	}
	return nil
}

func isKubernetesLabel(key string) bool {
	namespace := getLabelNamespace(key)
	if namespace == "kubernetes.io" || strings.HasSuffix(namespace, ".kubernetes.io") {
		return true
	}
	if namespace == "k8s.io" || strings.HasSuffix(namespace, ".k8s.io") {
		return true
	}
	return false
}

func getLabelNamespace(key string) string {
	if parts := strings.SplitN(key, "/", 2); len(parts) == 2 {
		return parts[0]
	}
	return ""
}

// NewKubeletConfiguration 通过版本化 v1beta1 Scheme 生成默认值，再转换为内部配置类型。
func NewKubeletConfiguration() (*kubeletconfigapi.KubeletConfiguration, error) {
	scheme, _, err := kubeletscheme.NewSchemeAndCodecs()
	if err != nil {
		return nil, err
	}
	// 先对外部版本执行默认值策略，再通过 Scheme 转为内部类型，保持 API 默认值规则集中管理。
	versioned := &v1beta1.KubeletConfiguration{}
	scheme.Default(versioned)
	config := &kubeletconfigapi.KubeletConfiguration{}
	if err := scheme.Convert(versioned, config, nil); err != nil {
		return nil, err
	}
	applyLegacyDefaults(config)
	return config, nil
}

// applyLegacyDefaults 补齐历史命令行 API 的默认语义，确保新配置体系升级时保持兼容。
// 这些值用于首轮参数解析前的基线配置。
func applyLegacyDefaults(kc *kubeletconfigapi.KubeletConfiguration) {
	// --anonymous-auth
	kc.Authentication.Anonymous.Enabled = true
	// --authentication-token-webhook
	kc.Authentication.Webhook.Enabled = false
	// --authorization-mode
	kc.Authorization.Mode = kubeletconfigapi.KubeletAuthorizationModeAlwaysAllow
	// --read-only-port
	kc.ReadOnlyPort = ports.KubeletReadOnlyPort
}

// KubeletServer 将节点本地启动参数与可版本化的组件配置合并为最终服务配置。
// 两种配置均可通过命令行参数或直接赋值设置。
type KubeletServer struct {
	KubeletFlags
	kubeletconfigapi.KubeletConfiguration
}

// NewKubeletServer 创建带默认值的 KubeletServer，是程序化启动 Kubelet 的默认配置入口。
func NewKubeletServer() (*KubeletServer, error) {
	config, err := NewKubeletConfiguration()
	if err != nil {
		return nil, err
	}
	return &KubeletServer{
		KubeletFlags:         *NewKubeletFlags(),
		KubeletConfiguration: *config,
	}, nil
}

// ValidateKubeletServer 依次校验组件配置与节点级 Flag；组件字段规则集中在专用 validation 包中。
func ValidateKubeletServer(s *KubeletServer) error {
	// 新增 KubeletConfiguration 校验规则时应放入 kubeletconfigvalidation.ValidateKubeletConfiguration。
	if err := kubeletconfigvalidation.ValidateKubeletConfiguration(&s.KubeletConfiguration, utilfeature.DefaultFeatureGate); err != nil {
		return err
	}
	if err := ValidateKubeletFlags(&s.KubeletFlags); err != nil {
		return err
	}
	return nil
}

// AddFlags 将节点级参数和 KubeletConfiguration 参数注册到指定 FlagSet。
func (s *KubeletServer) AddFlags(fs *pflag.FlagSet) {
	s.KubeletFlags.AddFlags(fs)
	AddKubeletConfigFlags(fs, &s.KubeletConfiguration)
}

// AddFlags 注册节点本地参数，并在最后统一合并到调用方传入的 FlagSet。
func (f *KubeletFlags) AddFlags(mainfs *pflag.FlagSet) {
	fs := pflag.NewFlagSet("", pflag.ExitOnError)
	defer func() {
		// 弃用参数仍在帮助信息中展示，避免用户在不知情的情况下失去迁移线索。
		// 这些参数原本可能被隐藏；直接静默删除即使不影响功能，也会破坏使用者的升级预期。
		fs.VisitAll(func(f *pflag.Flag) {
			if len(f.Deprecated) > 0 {
				f.Hidden = false
			}
		})
		mainfs.AddFlagSet(fs)
	}()

	addContainerRuntimeFlags(fs, &f.ContainerRuntimeOptions)
	f.addOSFlags(fs)

	fs.StringVar(&f.KubeletConfigFile, "config", f.KubeletConfigFile, "The Kubelet will load its initial configuration from this file. The path may be absolute or relative; relative paths start at the Kubelet's current working directory. Omit this flag to use the built-in default configuration values. Command-line flags override configuration from this file.")
	fs.StringVar(&f.KubeletDropinConfigDirectory, "config-dir", "", "Path to a directory containing drop-in configuration files that override settings from defaults and the --config file. The path may be absolute or relative; relative paths start at the Kubelet's current working directory. Drop-in files must have a '.conf' suffix (e.g., '99-kubelet-address.conf') and are processed in lexical order. Only '.conf' files are loaded; all other files are ignored. All .conf files in the directory and its subdirectories are processed in lexical order. [default='']")
	fs.StringVar(&f.KubeConfig, "kubeconfig", f.KubeConfig, "Path to a kubeconfig file, specifying how to connect to the API server. Providing --kubeconfig enables API server mode, omitting --kubeconfig enables standalone mode.")

	fs.StringVar(&f.BootstrapKubeconfig, "bootstrap-kubeconfig", f.BootstrapKubeconfig, "Path to a kubeconfig file that will be used to get client certificate for kubelet. "+
		"If the file specified by --kubeconfig does not exist, the bootstrap kubeconfig is used to request a client certificate from the API server. "+
		"On success, a kubeconfig file referencing the generated client certificate and key is written to the path specified by --kubeconfig. "+
		"The client certificate and key file will be stored in the directory pointed by --cert-dir.")

	fs.StringVar(&f.HostnameOverride, "hostname-override", f.HostnameOverride, "If non-empty, will use this string as identification instead of the actual hostname.")

	fs.StringVar(&f.NodeIP, "node-ip", f.NodeIP, "IP address (or comma-separated dual-stack IP addresses) of the node. If unset, kubelet will use the node's default IPv4 address, if any, or its default IPv6 address if it has no IPv4 addresses. You can pass '::' to make it prefer the default IPv6 address rather than the default IPv4 address. If cloud-provider is set to external, this flag will help to bootstrap the node with the corresponding IP.")

	fs.StringVar(&f.CertDirectory, "cert-dir", f.CertDirectory, "The directory where the TLS certs are located. "+
		"If --tls-cert-file and --tls-private-key-file are provided, this flag will be ignored.")

	fs.StringVar(&f.CloudProvider, "cloud-provider", f.CloudProvider, "The provider for cloud services. Set to empty string for running with no cloud provider. Set to 'external' for running with an external cloud provider.")

	fs.StringVar(&f.RootDirectory, "root-dir", f.RootDirectory, "Directory path for managing kubelet files (volume mounts,etc).")
	fs.BoolVar(&f.SeccompDefault, "seccomp-default", f.SeccompDefault, "Enable the use of `RuntimeDefault` as the default seccomp profile for all workloads.")

	bindableNodeLabels := cliflag.ConfigurationMap(f.NodeLabels)
	fs.Var(&bindableNodeLabels, "node-labels", fmt.Sprintf("Labels to add when registering the node in the cluster.  Labels must be key=value pairs separated by ','. Labels in the 'kubernetes.io' namespace must begin with an allowed prefix (%s) or be in the specifically allowed set (%s)", strings.Join(kubeletapis.KubeletLabelNamespaces(), ", "), strings.Join(kubeletapis.KubeletLabels(), ", ")))

	// 实验性参数。
	fs.StringVar(&f.LockFilePath, "lock-file", f.LockFilePath, "<Warning: Alpha feature> The path to file for kubelet to use as a lock file.")
	fs.BoolVar(&f.ExitOnLockContention, "exit-on-lock-contention", f.ExitOnLockContention, "Whether kubelet should exit upon lock-file contention.")

	// 已弃用参数。
	fs.DurationVar(&f.MinimumGCAge.Duration, "minimum-container-ttl-duration", f.MinimumGCAge.Duration, "Minimum age for a finished container before it is garbage collected.  Examples: '300ms', '10s' or '2h45m'")
	fs.MarkDeprecated("minimum-container-ttl-duration", "Use --eviction-hard or --eviction-soft instead. Will be removed in a future version.")
	fs.Int32Var(&f.MaxPerPodContainerCount, "maximum-dead-containers-per-container", f.MaxPerPodContainerCount, "Maximum number of old instances to retain per container.  Each container takes up some disk space.")
	fs.MarkDeprecated("maximum-dead-containers-per-container", "Use --eviction-hard or --eviction-soft instead. Will be removed in a future version.")
	fs.Int32Var(&f.MaxContainerCount, "maximum-dead-containers", f.MaxContainerCount, "Maximum number of old instances of containers to retain globally.  Each container takes up some disk space. To disable, set to a negative number.")
	fs.MarkDeprecated("maximum-dead-containers", "Use --eviction-hard or --eviction-soft instead. Will be removed in a future version.")
	fs.StringVar(&f.ExperimentalMounterPath, "experimental-mounter-path", f.ExperimentalMounterPath, "[Experimental] Path of mounter binary. Leave empty to use the default mount.")
	fs.MarkDeprecated("experimental-mounter-path", "will be removed in 1.25 or later. in favor of using CSI.")
	fs.BoolVar(&f.ExperimentalNodeAllocatableIgnoreEvictionThreshold, "experimental-allocatable-ignore-eviction", f.ExperimentalNodeAllocatableIgnoreEvictionThreshold, "When set to 'true', Hard Eviction Thresholds will be ignored while calculating Node Allocatable. See https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/ for more details. [default=false]")
	fs.MarkDeprecated("experimental-allocatable-ignore-eviction", "will be removed in 1.25 or later.")
}

// addContainerRuntimeFlags 根据 ContainerRuntimeOptions 注册容器运行时参数。
func addContainerRuntimeFlags(fs *pflag.FlagSet, o *kubeletconfig.ContainerRuntimeOptions) {
	// 常规设置。
	fs.StringVar(&o.RuntimeCgroups, "runtime-cgroups", o.RuntimeCgroups, "Optional absolute name of cgroups to create and run the runtime in.")

	// 镜像凭据提供程序设置。
	fs.StringVar(&o.ImageCredentialProviderConfigPath, "image-credential-provider-config", o.ImageCredentialProviderConfigPath, "Path to a credential provider plugin config file (JSON/YAML/YML) or a directory of such files (merged in lexicographical order; non-recursive search).")
	fs.StringVar(&o.ImageCredentialProviderBinDir, "image-credential-provider-bin-dir", o.ImageCredentialProviderBinDir, "The path to the directory where credential provider plugin binaries are located.")
}

// AddKubeletConfigFlags 将 KubeletConfiguration 参数注册到指定 FlagSet。
func AddKubeletConfigFlags(mainfs *pflag.FlagSet, c *kubeletconfigapi.KubeletConfiguration) {
	fs := pflag.NewFlagSet("", pflag.ExitOnError)
	defer func() {
		// 指向 KubeletConfiguration 的参数均已弃用，新增参数从引入时即弃用。
		// 删除参数前必须确认它已在整个弃用期内作为配置字段存在；后加入的参数可能尚未到期。
		// 例如，若参数在弃用流程之后才添加，即使其他参数已到期，它也可能仍在有效期内。
		deprecated := "This parameter should be set via the config file specified by the Kubelet's --config flag. See https://kubernetes.io/docs/tasks/administer-cluster/kubelet-config-file/ for more information."
		// 部分参数拟长期保留。Kubernetes 1.23 将它们纳入 LoggingConfiguration，
		// 但当时认为立即弃用对应 Flag 为时过早。
		notDeprecated := map[string]bool{
			"v":                   true,
			"vmodule":             true,
			"log-flush-frequency": true,
			"provider-id":         true,
			"fail-cgroupv1":       true,
		}
		fs.VisitAll(func(f *pflag.Flag) {
			if notDeprecated[f.Name] {
				return
			}
			f.Deprecated = deprecated
		})
		mainfs.AddFlagSet(fs)
	}()

	fs.BoolVar(&c.EnableServer, "enable-server", c.EnableServer, "Enable the Kubelet's server")

	fs.BoolVar(&c.FailSwapOn, "fail-swap-on", c.FailSwapOn, "Makes the Kubelet fail to start if swap is enabled on the node. ")
	fs.StringVar(&c.StaticPodPath, "pod-manifest-path", c.StaticPodPath, "Path to the directory containing static pod files to run, or the path to a single static pod file. Files starting with dots will be ignored.")
	fs.DurationVar(&c.SyncFrequency.Duration, "sync-frequency", c.SyncFrequency.Duration, "Max period between synchronizing running containers and config")
	fs.DurationVar(&c.FileCheckFrequency.Duration, "file-check-frequency", c.FileCheckFrequency.Duration, "Duration between checking config files for new data")
	fs.DurationVar(&c.HTTPCheckFrequency.Duration, "http-check-frequency", c.HTTPCheckFrequency.Duration, "Duration between checking http for new data")
	fs.StringVar(&c.StaticPodURL, "manifest-url", c.StaticPodURL, "URL for accessing additional Pod specifications to run")
	fs.Var(cliflag.NewColonSeparatedMultimapStringString(&c.StaticPodURLHeader), "manifest-url-header", "Comma-separated list of HTTP headers to use when accessing the url provided to --manifest-url. Multiple headers with the same name will be added in the same order provided. This flag can be repeatedly invoked. For example: --manifest-url-header 'a:hello,b:again,c:world' --manifest-url-header 'b:beautiful'")
	fs.Var(&utilflag.IPVar{Val: &c.Address}, "address", "The IP address for the Kubelet to serve on (set to '0.0.0.0' or '::' for listening on all interfaces and IP address families)")
	fs.Int32Var(&c.Port, "port", c.Port, "The port for the Kubelet to serve on.")
	fs.Int32Var(&c.ReadOnlyPort, "read-only-port", c.ReadOnlyPort, "The read-only port for the Kubelet to serve on with no authentication/authorization (set to 0 to disable)")
	fs.BoolVar(&c.FailCgroupV1, "fail-cgroupv1", c.FailCgroupV1, "Prevent the kubelet from starting on the host using cgroup v1.")

	// 运行时参数。
	fs.StringVar(&c.ContainerRuntimeEndpoint, "container-runtime-endpoint", c.ContainerRuntimeEndpoint, "The endpoint of container runtime service. Unix Domain Sockets are supported on Linux, while npipe and tcp endpoints are supported on Windows. Examples:'unix:///path/to/runtime.sock', 'npipe:////./pipe/runtime'")
	fs.StringVar(&c.ImageServiceEndpoint, "image-service-endpoint", c.ImageServiceEndpoint, "The endpoint of container image service. If not specified, it will be the same with --container-runtime-endpoint by default. Unix Domain Socket are supported on Linux, while npipe and tcp endpoints are supported on Windows. Examples:'unix:///path/to/runtime.sock', 'npipe:////./pipe/runtime'")

	// 认证。
	fs.BoolVar(&c.Authentication.Anonymous.Enabled, "anonymous-auth", c.Authentication.Anonymous.Enabled, ""+
		"Enables anonymous requests to the Kubelet server. Requests that are not rejected by another "+
		"authentication method are treated as anonymous requests. Anonymous requests have a username "+
		"of system:anonymous, and a group name of system:unauthenticated.")
	fs.BoolVar(&c.Authentication.Webhook.Enabled, "authentication-token-webhook", c.Authentication.Webhook.Enabled, ""+
		"Use the TokenReview API to determine authentication for bearer tokens.")
	fs.DurationVar(&c.Authentication.Webhook.CacheTTL.Duration, "authentication-token-webhook-cache-ttl", c.Authentication.Webhook.CacheTTL.Duration, ""+
		"The duration to cache responses from the webhook token authenticator.")
	fs.StringVar(&c.Authentication.X509.ClientCAFile, "client-ca-file", c.Authentication.X509.ClientCAFile, ""+
		"If set, any request presenting a client certificate signed by one of the authorities in the client-ca-file "+
		"is authenticated with an identity corresponding to the CommonName of the client certificate.")

	// 授权。
	fs.StringVar((*string)(&c.Authorization.Mode), "authorization-mode", string(c.Authorization.Mode), ""+
		"Authorization mode for Kubelet server. Valid options are AlwaysAllow or Webhook. "+
		"Webhook mode uses the SubjectAccessReview API to determine authorization.")
	fs.DurationVar(&c.Authorization.Webhook.CacheAuthorizedTTL.Duration, "authorization-webhook-cache-authorized-ttl", c.Authorization.Webhook.CacheAuthorizedTTL.Duration, ""+
		"The duration to cache 'authorized' responses from the webhook authorizer.")
	fs.DurationVar(&c.Authorization.Webhook.CacheUnauthorizedTTL.Duration, "authorization-webhook-cache-unauthorized-ttl", c.Authorization.Webhook.CacheUnauthorizedTTL.Duration, ""+
		"The duration to cache 'unauthorized' responses from the webhook authorizer.")

	fs.StringVar(&c.TLSCertFile, "tls-cert-file", c.TLSCertFile, ""+
		"File containing x509 Certificate used for serving HTTPS (with intermediate certs, if any, concatenated after server cert). "+
		"If --tls-cert-file and --tls-private-key-file are not provided, a self-signed certificate and key "+
		"are generated for the public address and saved to the directory passed to --cert-dir.")
	fs.StringVar(&c.TLSPrivateKeyFile, "tls-private-key-file", c.TLSPrivateKeyFile, "File containing x509 private key matching --tls-cert-file.")
	fs.BoolVar(&c.ServerTLSBootstrap, "rotate-server-certificates", c.ServerTLSBootstrap, "Auto-request and rotate the kubelet serving certificates by requesting new certificates from the kube-apiserver when the certificate expiration approaches. Requires the RotateKubeletServerCertificate feature gate to be enabled, and approval of the submitted CertificateSigningRequest objects.")

	tlsCipherPreferredValues := cliflag.PreferredTLSCipherNames()
	tlsCipherInsecureValues := cliflag.InsecureTLSCipherNames()
	fs.StringSliceVar(&c.TLSCipherSuites, "tls-cipher-suites", c.TLSCipherSuites,
		"Comma-separated list of cipher suites for the server. "+
			"If omitted, the default Go cipher suites will be used. \n"+
			"Preferred values: "+strings.Join(tlsCipherPreferredValues, ", ")+". \n"+
			"Insecure values: "+strings.Join(tlsCipherInsecureValues, ", ")+".")
	tlsPossibleVersions := cliflag.TLSPossibleVersions()
	fs.StringVar(&c.TLSMinVersion, "tls-min-version", c.TLSMinVersion,
		"Minimum TLS version supported. "+
			"Possible values: "+strings.Join(tlsPossibleVersions, ", "))
	fs.BoolVar(&c.RotateCertificates, "rotate-certificates", c.RotateCertificates, "Auto rotate the kubelet client certificates by requesting new certificates from the kube-apiserver when the certificate expiration approaches.")

	fs.Int32Var(&c.RegistryPullQPS, "registry-qps", c.RegistryPullQPS, "If > 0, limit registry pull QPS to this value.  If 0, unlimited.")
	fs.Int32Var(&c.RegistryBurst, "registry-burst", c.RegistryBurst, "Maximum size of a bursty pulls, temporarily allows pulls to burst to this number, while still not exceeding registry-qps. Only used if --registry-qps > 0")
	fs.Int32Var(&c.EventRecordQPS, "event-qps", c.EventRecordQPS, "QPS to limit event creations. The number must be >= 0. If 0 there is no limit enforced. Note: if not set, kubelet configuration default applies, currently 50.")
	fs.Int32Var(&c.EventBurst, "event-burst", c.EventBurst, "Maximum size of a bursty event records, temporarily allows event records to burst to this number, while still not exceeding event-qps. The number must be >= 0. If 0 will use DefaultBurst: 100.")

	fs.BoolVar(&c.EnableDebuggingHandlers, "enable-debugging-handlers", c.EnableDebuggingHandlers, "Enables server endpoints for log collection and local running of containers and commands")
	fs.BoolVar(&c.EnableContentionProfiling, "contention-profiling", c.EnableContentionProfiling, "Enable block profiling, if profiling is enabled")
	fs.Int32Var(&c.HealthzPort, "healthz-port", c.HealthzPort, "The port of the localhost healthz endpoint (set to 0 to disable)")
	fs.Var(&utilflag.IPVar{Val: &c.HealthzBindAddress}, "healthz-bind-address", "The IP address for the healthz server to serve on (set to '0.0.0.0' or '::' for listening on all interfaces and IP address families)")
	fs.Int32Var(&c.OOMScoreAdj, "oom-score-adj", c.OOMScoreAdj, "The oom-score-adj value for kubelet process. Values must be within the range [-1000, 1000]")
	fs.StringVar(&c.ClusterDomain, "cluster-domain", c.ClusterDomain, "Domain for this cluster.  If set, kubelet will configure all containers to search this domain in addition to the host's search domains")

	fs.StringVar(&c.VolumePluginDir, "volume-plugin-dir", c.VolumePluginDir, "The full path of the directory in which to search for additional third party volume plugins")
	fs.StringSliceVar(&c.ClusterDNS, "cluster-dns", c.ClusterDNS, "Comma-separated list of DNS server IP address.  This value is used for containers DNS server in case of Pods with \"dnsPolicy=ClusterFirst\". Note: all DNS servers appearing in the list MUST serve the same set of records otherwise name resolution within the cluster may not work correctly. There is no guarantee as to which DNS server may be contacted for name resolution.")
	fs.DurationVar(&c.StreamingConnectionIdleTimeout.Duration, "streaming-connection-idle-timeout", c.StreamingConnectionIdleTimeout.Duration, "Maximum time a streaming connection can be idle before the connection is automatically closed. 0 indicates no timeout. Example: '5m'. Note: All connections to the kubelet server have a maximum duration of 4 hours.")
	fs.DurationVar(&c.NodeStatusUpdateFrequency.Duration, "node-status-update-frequency", c.NodeStatusUpdateFrequency.Duration, "Specifies how often kubelet posts node status to master. Note: be cautious when changing the constant, it must work with nodeMonitorGracePeriod in nodecontroller.")
	fs.DurationVar(&c.ImageMinimumGCAge.Duration, "minimum-image-ttl-duration", c.ImageMinimumGCAge.Duration, "Minimum age for an unused image before it is garbage collected.  Examples: '300ms', '10s' or '2h45m'.")
	fs.Int32Var(&c.ImageGCHighThresholdPercent, "image-gc-high-threshold", c.ImageGCHighThresholdPercent, "The percent of disk usage after which image garbage collection is always run. Values must be within the range [0, 100], To disable image garbage collection, set to 100. ")
	fs.Int32Var(&c.ImageGCLowThresholdPercent, "image-gc-low-threshold", c.ImageGCLowThresholdPercent, "The percent of disk usage before which image garbage collection is never run. Lowest disk usage to garbage collect to. Values must be within the range [0, 100] and must be less than that of --image-gc-high-threshold.")
	fs.DurationVar(&c.VolumeStatsAggPeriod.Duration, "volume-stats-agg-period", c.VolumeStatsAggPeriod.Duration, "Specifies interval for kubelet to calculate and cache the volume disk usage for all pods and volumes.  To disable volume calculations, set to a negative number.")
	fs.Var(cliflag.NewMapStringBool(&c.FeatureGates), "feature-gates", "A set of key=value pairs that describe feature gates for alpha/experimental features. "+
		"Options are:\n"+strings.Join(utilfeature.DefaultFeatureGate.KnownFeatures(), "\n"))
	fs.StringVar(&c.KubeletCgroups, "kubelet-cgroups", c.KubeletCgroups, "Optional absolute name of cgroups to create and run the Kubelet in.")
	fs.StringVar(&c.SystemCgroups, "system-cgroups", c.SystemCgroups, "Optional absolute name of cgroups in which to place all non-kernel processes that are not already inside a cgroup under '/'. Empty for no container. Rolling back the flag requires a reboot.")

	fs.StringVar(&c.ProviderID, "provider-id", c.ProviderID, "Unique identifier for identifying the node in a machine database, i.e cloudprovider")

	fs.BoolVar(&c.CgroupsPerQOS, "cgroups-per-qos", c.CgroupsPerQOS, "Enable creation of QoS cgroup hierarchy, if true top level QoS and pod cgroups are created.")
	fs.StringVar(&c.CgroupDriver, "cgroup-driver", c.CgroupDriver, "Driver that the kubelet uses to manipulate cgroups on the host.  Possible values: 'cgroupfs', 'systemd'")
	fs.StringVar(&c.CgroupRoot, "cgroup-root", c.CgroupRoot, "Optional root cgroup to use for pods. This is handled by the container runtime on a best effort basis. Default: '', which means use the container runtime default.")
	fs.StringVar(&c.CPUManagerPolicy, "cpu-manager-policy", c.CPUManagerPolicy, "CPU Manager policy to use. Possible values: 'none', 'static'.")
	fs.Var(cliflag.NewMapStringStringNoSplit(&c.CPUManagerPolicyOptions), "cpu-manager-policy-options", "A set of key=value CPU Manager policy options to use, to fine tune their behaviour. If not supplied, keep the default behaviour.")
	fs.DurationVar(&c.CPUManagerReconcilePeriod.Duration, "cpu-manager-reconcile-period", c.CPUManagerReconcilePeriod.Duration, "<Warning: Alpha feature> CPU Manager reconciliation period. Examples: '10s', or '1m'. If not supplied, defaults to 'NodeStatusUpdateFrequency'")
	fs.Var(cliflag.NewMapStringString(&c.QOSReserved), "qos-reserved", "<Warning: Alpha feature> A set of ResourceName=Percentage (e.g. memory=50%) pairs that describe how pod resource requests are reserved at the QoS level. Currently only memory is supported. Requires the QOSReserved feature gate to be enabled.")
	fs.StringVar(&c.TopologyManagerPolicy, "topology-manager-policy", c.TopologyManagerPolicy, "Topology Manager policy to use. Possible values: 'none', 'best-effort', 'restricted', 'single-numa-node'.")
	fs.DurationVar(&c.RuntimeRequestTimeout.Duration, "runtime-request-timeout", c.RuntimeRequestTimeout.Duration, "Timeout of all runtime requests except long running request - pull, logs, exec and attach. When timeout exceeded, kubelet will cancel the request, throw out an error and retry later.")
	fs.StringVar(&c.HairpinMode, "hairpin-mode", c.HairpinMode, "How should the kubelet setup hairpin NAT. This allows endpoints of a Service to loadbalance back to themselves if they should try to access their own Service. Valid values are \"promiscuous-bridge\", \"hairpin-veth\" and \"none\".")
	fs.Int32Var(&c.MaxPods, "max-pods", c.MaxPods, "Number of Pods that can run on this Kubelet.")

	fs.StringVar(&c.PodCIDR, "pod-cidr", c.PodCIDR, "The CIDR to use for pod IP addresses, only used in standalone mode.  In cluster mode, this is obtained from the master. For IPv6, the maximum number of IP's allocated is 65536")
	fs.Int64Var(&c.PodPidsLimit, "pod-max-pids", c.PodPidsLimit, "Set the maximum number of processes per pod.  If -1, the kubelet defaults to the node allocatable pid capacity.")

	fs.StringVar(&c.ResolverConfig, "resolv-conf", c.ResolverConfig, "Resolver configuration file used as the basis for the container DNS resolution configuration.")

	fs.BoolVar(&c.RunOnce, "runonce", c.RunOnce, "If true, exit after spawning pods from static pod files or remote urls. Exclusive with --enable-server")

	fs.BoolVar(&c.CPUCFSQuota, "cpu-cfs-quota", c.CPUCFSQuota, "Enable CPU CFS quota enforcement for containers that specify CPU limits")
	fs.DurationVar(&c.CPUCFSQuotaPeriod.Duration, "cpu-cfs-quota-period", c.CPUCFSQuotaPeriod.Duration, "Sets CPU CFS quota period value (cpu.cfs_period_us). Defaults to Linux Kernel default. "+"Requires the CustomCPUCFSQuotaPeriod feature gate to set non-default values.")

	fs.BoolVar(&c.EnableControllerAttachDetach, "enable-controller-attach-detach", c.EnableControllerAttachDetach, "Enables the Attach/Detach controller to manage attachment/detachment of volumes scheduled to this node, and disables kubelet from executing any attach/detach operations")
	fs.BoolVar(&c.MakeIPTablesUtilChains, "make-iptables-util-chains", c.MakeIPTablesUtilChains, "If true, kubelet will ensure iptables utility rules are present on host.")
	fs.StringVar(&c.ContainerLogMaxSize, "container-log-max-size", c.ContainerLogMaxSize, "<Warning: Beta feature> Set the maximum size (e.g. 10Mi) of container log file before it is rotated.")
	fs.Int32Var(&c.ContainerLogMaxFiles, "container-log-max-files", c.ContainerLogMaxFiles, "<Warning: Beta feature> Set the maximum number of container log files that can be present for a container. The number must be >= 2.")
	fs.StringSliceVar(&c.AllowedUnsafeSysctls, "allowed-unsafe-sysctls", c.AllowedUnsafeSysctls, "Comma-separated whitelist of unsafe sysctls or unsafe sysctl patterns (ending in *). Use these at your own risk.")

	fs.Int32Var(&c.NodeStatusMaxImages, "node-status-max-images", c.NodeStatusMaxImages, "The maximum number of images to report in Node.Status.Images. If -1 is specified, no cap will be applied.")
	fs.BoolVar(&c.KernelMemcgNotification, "kernel-memcg-notification", c.KernelMemcgNotification, "If enabled, the kubelet will integrate with the kernel memcg notification to determine if memory eviction thresholds are crossed rather than polling.")
	fs.BoolVar(&c.LocalStorageCapacityIsolation, "local-storage-capacity-isolation", c.LocalStorageCapacityIsolation, "If true, local ephemeral storage isolation is enabled. Otherwise, local storage isolation feature will be disabled")

	// 仅供测试使用，不建议在生产环境启用。
	fs.Int64Var(&c.MaxOpenFiles, "max-open-files", c.MaxOpenFiles, "Number of files that can be opened by Kubelet process.")

	fs.StringVar(&c.ContentType, "kube-api-content-type", c.ContentType, "Content type of requests sent to apiserver.")
	fs.Int32Var(&c.KubeAPIQPS, "kube-api-qps", c.KubeAPIQPS, "QPS to use while talking with kubernetes apiserver. The number must be >= 0. If 0 will use DefaultQPS: 50. Doesn't cover events and node heartbeat apis which rate limiting is controlled by a different set of flags")
	fs.Int32Var(&c.KubeAPIBurst, "kube-api-burst", c.KubeAPIBurst, "Burst to use while talking with kubernetes apiserver. The number must be >= 0. If 0 will use DefaultBurst: 100. Doesn't cover events and node heartbeat apis which rate limiting is controlled by a different set of flags")
	fs.BoolVar(&c.SerializeImagePulls, "serialize-image-pulls", c.SerializeImagePulls, "Pull images one at a time. We recommend *not* changing the default value on nodes that run docker daemon with version < 1.9 or an Aufs storage backend. Issue #10959 has more details.")

	fs.Var(cliflag.NewLangleSeparatedMapStringString(&c.EvictionHard), "eviction-hard", "A set of eviction thresholds (e.g. memory.available<1Gi) that if met would trigger a pod eviction.")
	fs.Var(cliflag.NewLangleSeparatedMapStringString(&c.EvictionSoft), "eviction-soft", "A set of eviction thresholds (e.g. memory.available<1.5Gi) that if met over a corresponding grace period would trigger a pod eviction.")
	fs.Var(cliflag.NewMapStringString(&c.EvictionSoftGracePeriod), "eviction-soft-grace-period", "A set of eviction grace periods (e.g. memory.available=1m30s) that correspond to how long a soft eviction threshold must hold before triggering a pod eviction.")
	fs.DurationVar(&c.EvictionPressureTransitionPeriod.Duration, "eviction-pressure-transition-period", c.EvictionPressureTransitionPeriod.Duration, "Duration for which the kubelet has to wait before transitioning out of an eviction pressure condition.")
	fs.Int32Var(&c.EvictionMaxPodGracePeriod, "eviction-max-pod-grace-period", c.EvictionMaxPodGracePeriod, "Maximum allowed grace period (in seconds) to use when terminating pods in response to a soft eviction threshold being met.  If negative, defer to pod specified value.")
	fs.Var(cliflag.NewMapStringString(&c.EvictionMinimumReclaim), "eviction-minimum-reclaim", "A set of minimum reclaims (e.g. imagefs.available=2Gi) that describes the minimum amount of resource the kubelet will reclaim when performing a pod eviction if that resource is under pressure.")
	fs.Int32Var(&c.PodsPerCore, "pods-per-core", c.PodsPerCore, "Number of Pods per core that can run on this Kubelet. The total number of Pods on this Kubelet cannot exceed max-pods, so max-pods will be used if this calculation results in a larger number of Pods allowed on the Kubelet. A value of 0 disables this limit.")
	fs.BoolVar(&c.ProtectKernelDefaults, "protect-kernel-defaults", c.ProtectKernelDefaults, "Default kubelet behaviour for kernel tuning. If set, kubelet errors if any of kernel tunables is different than kubelet defaults.")
	fs.StringVar(&c.ReservedSystemCPUs, "reserved-cpus", c.ReservedSystemCPUs, "A comma-separated list of CPUs or CPU ranges that are reserved for system and kubernetes usage. This specific list will supersede cpu counts in --system-reserved and --kube-reserved.")
	fs.StringVar(&c.TopologyManagerScope, "topology-manager-scope", c.TopologyManagerScope, "Scope to which topology hints applied. Topology Manager collects hints from Hint Providers and applies them to defined scope to ensure the pod admission. Possible values: 'container', 'pod'.")
	fs.Var(cliflag.NewMapStringString(&c.TopologyManagerPolicyOptions), "topology-manager-policy-options", "A set of key=value Topology Manager policy options to use, to fine tune their behaviour. If not supplied, keep the default behaviour.")
	// Node Allocatable 参数。
	fs.Var(cliflag.NewMapStringString(&c.SystemReserved), "system-reserved", "A set of ResourceName=ResourceQuantity (e.g. cpu=200m,memory=500Mi,ephemeral-storage=1Gi,pid=1000) pairs that describe resources reserved for non-kubernetes components. Currently only cpu, memory, pid and local ephemeral storage for root file system are supported. See https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/ for more detail. [default=none]")
	fs.Var(cliflag.NewMapStringString(&c.KubeReserved), "kube-reserved", "A set of ResourceName=ResourceQuantity (e.g. cpu=200m,memory=500Mi,ephemeral-storage=1Gi,pid=1000) pairs that describe resources reserved for kubernetes system components. Currently only cpu, memory, pid and local ephemeral storage for root file system are supported. See https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/ for more detail. [default=none]")
	fs.StringSliceVar(&c.EnforceNodeAllocatable, "enforce-node-allocatable", c.EnforceNodeAllocatable, "A comma separated list of levels of node allocatable enforcement to be enforced by kubelet. Acceptable options are 'none', 'pods', 'system-reserved', 'system-reserved-compressible', 'kube-reserved' and 'kube-reserved-compressible'. If any of the latter four options are specified, '--system-reserved-cgroup' and '--kube-reserved-cgroup' must also be set, respectively. If 'none' is specified, no additional options should be set. See https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/ for more details.")
	fs.StringVar(&c.SystemReservedCgroup, "system-reserved-cgroup", c.SystemReservedCgroup, "Absolute name of the top level cgroup that is used to manage non-kubernetes components for which compute resources were reserved via '--system-reserved' flag. Ex. '/system-reserved'. [default='']")
	fs.StringVar(&c.KubeReservedCgroup, "kube-reserved-cgroup", c.KubeReservedCgroup, "Absolute name of the top level cgroup that is used to manage kubernetes components for which compute resources were reserved via '--kube-reserved' flag. Ex. '/kube-reserved'. [default='']")
	logsapi.AddFlags(&c.Logging, fs)

	// Memory Manager 参数。
	fs.StringVar(&c.MemoryManagerPolicy, "memory-manager-policy", c.MemoryManagerPolicy, "Memory Manager policy to use. Possible values: 'None', 'Static'.")
	fs.Var(&utilflag.ReservedMemoryVar{Value: &c.ReservedMemory}, "reserved-memory", "A comma separated list of memory reservations for NUMA nodes. (e.g. --reserved-memory 0:memory=1Gi,hugepages-1M=2Gi --reserved-memory 1:memory=2Gi). The total sum for each memory type should be equal to the sum of kube-reserved, system-reserved and eviction-threshold. See https://kubernetes.io/docs/tasks/administer-cluster/memory-manager/#reserved-memory-flag for more details.")

	fs.BoolVar(&c.RegisterNode, "register-node", c.RegisterNode, "Register the node with the apiserver. If --kubeconfig is not provided, this flag is irrelevant, as the Kubelet won't have an apiserver to register with.")

	fs.Var(&utilflag.RegisterWithTaintsVar{Value: &c.RegisterWithTaints}, "register-with-taints", "Register the node with the given list of taints (comma separated \"<key>=<value>:<effect>\"). No-op if register-node is false.")
}
