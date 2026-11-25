/*
Copyright 2014 The Kubernetes Authors.

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

package clientcmd

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"unicode"

	restclient "k8s.io/client-go/rest"
	clientauth "k8s.io/client-go/tools/auth"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"
)

const (
	// clusterExtensionKey 是集群扩展列表中为 exec 插件配置保留的键。
	clusterExtensionKey = "client.authentication.k8s.io/exec"
)

var (
	// ClusterDefaults 与旧的 EnvVar 和 DefaultCluster 字段行为相同。
	// 已弃用，将来会被替换
	ClusterDefaults = clientcmdapi.Cluster{Server: getDefaultServer()}
	// DefaultClientConfig 表示本包在默认化方面的旧有行为。
	// 已弃用，将来会被替换
	DefaultClientConfig = DirectClientConfig{*clientcmdapi.NewConfig(), "", &ConfigOverrides{
		ClusterDefaults: ClusterDefaults,
	}, nil, NewDefaultClientConfigLoadingRules(), promptedCredentials{}}
)

// getDefaultServer 返回 DefaultClientConfig 的默认设置。
// 已弃用。
// 保留 KUBERNETES_MASTER 环境变量的读取，是为了兼容早期 kubectl/组件的使用方式；
// 新代码应显式提供 kubeconfig 或 server 地址。
func getDefaultServer() string {
	if server := os.Getenv("KUBERNETES_MASTER"); len(server) > 0 {
		return server
	}
	return "http://localhost:8080"
}

// ClientConfig 用于方便地获取 API Server 客户端。
// 它把“kubeconfig + 命令行覆盖项”解析为可直接用的 rest.Config；
// Namespace 的第二个返回值表示命名空间是否来自显式覆盖。
type ClientConfig interface {
	// RawConfig 返回所有覆盖项合并后的结果
	RawConfig() (clientcmdapi.Config, error)
	// ClientConfig 返回完整的客户端配置
	ClientConfig() (*restclient.Config, error)
	// Namespace 返回所有覆盖项合并后的命名空间，
	// 并用布尔值指示它是否被覆盖
	Namespace() (string, bool, error)
	// ConfigAccess 返回加载/持久化配置所使用的规则。
	ConfigAccess() ConfigAccess
}

// OverridingClientConfig 允许对原始 KubeConfig 进行覆盖。
type OverridingClientConfig interface {
	ClientConfig
	// MergedRawConfig 返回与所有覆盖项合并后的 RawConfig。
	MergedRawConfig() (clientcmdapi.Config, error)
}

// PersistAuthProviderConfigForUser 为指定用户构造凭据持久化器；返回 nil 表示不持久化。
type PersistAuthProviderConfigForUser func(user string) restclient.AuthProviderConfigPersister

// promptedCredentials 缓存交互式输入的凭据，避免同一配置对象被多次调用时重复提示。
type promptedCredentials struct {
	username string
	password string `datapolicy:"password"`
}

// DirectClientConfig 是一个由 clientcmdapi.Config、选项覆盖项以及可选的认证信息
// fallbackReader 支撑的 ClientConfig 接口实现。
type DirectClientConfig struct {
	config         clientcmdapi.Config
	contextName    string
	overrides      *ConfigOverrides
	fallbackReader io.Reader
	configAccess   ConfigAccess
	// promptedCredentials 保存用户输入的凭据
	promptedCredentials promptedCredentials
}

// NewDefaultClientConfig 创建一个以 config.CurrentContext 为上下文名的 DirectClientConfig。
func NewDefaultClientConfig(config clientcmdapi.Config, overrides *ConfigOverrides) OverridingClientConfig {
	return &DirectClientConfig{config, config.CurrentContext, overrides, nil, NewDefaultClientConfigLoadingRules(), promptedCredentials{}}
}

// NewNonInteractiveClientConfig 创建一个使用传入上下文名、且没有认证信息回退读取器的 DirectClientConfig。
func NewNonInteractiveClientConfig(config clientcmdapi.Config, contextName string, overrides *ConfigOverrides, configAccess ConfigAccess) OverridingClientConfig {
	return &DirectClientConfig{config, contextName, overrides, nil, configAccess, promptedCredentials{}}
}

// NewInteractiveClientConfig 创建一个 DirectClientConfig：使用传入的上下文名，
// 并在文件与命令行都未提供认证信息时使用读取器。
// fallbackReader 非 nil 时，缺失凭据会触发交互式提示；服务器端组件应使用 NonInteractive 版本。
func NewInteractiveClientConfig(config clientcmdapi.Config, contextName string, overrides *ConfigOverrides, fallbackReader io.Reader, configAccess ConfigAccess) OverridingClientConfig {
	return &DirectClientConfig{config, contextName, overrides, fallbackReader, configAccess, promptedCredentials{}}
}

// NewClientConfigFromBytes 接收 kubeconfig 并返回一个 ClientConfig。
func NewClientConfigFromBytes(configBytes []byte) (OverridingClientConfig, error) {
	config, err := Load(configBytes)
	if err != nil {
		return nil, err
	}

	return &DirectClientConfig{*config, "", &ConfigOverrides{}, nil, nil, promptedCredentials{}}, nil
}

// RESTConfigFromKubeConfig 是一个便捷方法，从 kubeconfig 字节直接返回 restconfig。
// 对于程序化访问，80% 的情况下你想要的就是它。
func RESTConfigFromKubeConfig(configBytes []byte) (*restclient.Config, error) {
	clientConfig, err := NewClientConfigFromBytes(configBytes)
	if err != nil {
		return nil, err
	}
	return clientConfig.ClientConfig()
}

func (config *DirectClientConfig) RawConfig() (clientcmdapi.Config, error) {
	return config.config, nil
}

// MergedRawConfig 返回与覆盖项合并后的原始 kube 配置。
// 与 RawConfig 不同，这里返回的是覆盖项生效后的视图，且当前 context 会被改写为合并后的名字，
// 便于调用方把完整配置原样输出或持久化。
func (config *DirectClientConfig) MergedRawConfig() (clientcmdapi.Config, error) {
	if err := config.ConfirmUsable(); err != nil {
		return clientcmdapi.Config{}, err
	}
	merged := config.config.DeepCopy()

	// 将合并了覆盖项的 AuthInfo 写入合并后的配置
	mergedAuthInfo, err := config.getAuthInfo()
	if err != nil {
		return clientcmdapi.Config{}, err
	}
	mergedAuthInfoName, _ := config.getAuthInfoName()
	merged.AuthInfos[mergedAuthInfoName] = &mergedAuthInfo

	// 将合并了覆盖项的 Context 写入合并后的配置
	mergedContext, err := config.getContext()
	if err != nil {
		return clientcmdapi.Config{}, err
	}
	mergedContextName, _ := config.getContextName()
	merged.Contexts[mergedContextName] = &mergedContext
	merged.CurrentContext = mergedContextName

	// 将合并了覆盖项的 Cluster 写入合并后的配置
	configClusterInfo, err := config.getCluster()
	if err != nil {
		return clientcmdapi.Config{}, err
	}
	configClusterName, _ := config.getClusterName()
	merged.Clusters[configClusterName] = &configClusterInfo
	return *merged, nil
}

// ClientConfig 实现 ClientConfig 接口。
func (config *DirectClientConfig) ClientConfig() (*restclient.Config, error) {
	// 先确认 getAuthInfo、getContext、getCluster 不返回错误。
	// 在使用户定义的 AuthInfo、Context 或 Cluster 配置找不到时，
	// 先于“当前配置是否可用”的检查，以便用户能立即看到错误的直接原因
	configAuthInfo, err := config.getAuthInfo()
	if err != nil {
		return nil, err
	}

	_, err = config.getContext()
	if err != nil {
		return nil, err
	}

	configClusterInfo, err := config.getCluster()
	if err != nil {
		return nil, err
	}

	if err := config.ConfirmUsable(); err != nil {
		return nil, err
	}

	clientConfig := &restclient.Config{}
	clientConfig.Host = configClusterInfo.Server
	if configClusterInfo.ProxyURL != "" {
		u, err := parseProxyURL(configClusterInfo.ProxyURL)
		if err != nil {
			return nil, err
		}
		clientConfig.Proxy = http.ProxyURL(u)
	}

	// 压缩会改变线上传输形式，必须与集群配置保持一致。
	clientConfig.DisableCompression = configClusterInfo.DisableCompression

	if config.overrides != nil && len(config.overrides.Timeout) > 0 {
		timeout, err := ParseTimeout(config.overrides.Timeout)
		if err != nil {
			return nil, err
		}
		clientConfig.Timeout = timeout
	}

	// 去掉 host 上的查询串与 fragment：它们属于单次请求，不能固化成 base URL 的一部分。
	if u, err := url.ParseRequestURI(clientConfig.Host); err == nil && u.Opaque == "" && len(u.Path) > 1 {
		u.RawQuery = ""
		u.Fragment = ""
		clientConfig.Host = u.String()
	}
	if len(configAuthInfo.Impersonate) > 0 {
		clientConfig.Impersonate = restclient.ImpersonationConfig{
			UserName: configAuthInfo.Impersonate,
			UID:      configAuthInfo.ImpersonateUID,
			Groups:   configAuthInfo.ImpersonateGroups,
			Extra:    configAuthInfo.ImpersonateUserExtra,
		}
	}

	// 仅在连接安全时才尝试读取认证信息。
	// 明文 HTTP 上发送凭据既不安全也无意义，因此只在 TLS 生效时收集认证信息。
	if restclient.IsConfigTransportTLS(*clientConfig) {
		var err error
		var persister restclient.AuthProviderConfigPersister
		if config.configAccess != nil {
			authInfoName, _ := config.getAuthInfoName()
			persister = PersisterForUser(config.configAccess, authInfoName)
		}
		// 用户侧与服务器侧配置的合并顺序不同，因此分两步 merge：
		// 先合并用户身份，再合并服务器身份，后者可覆盖 host 级别的 TLS 设置。
		userAuthPartialConfig, err := config.getUserIdentificationPartialConfig(configAuthInfo, config.fallbackReader, persister, configClusterInfo)
		if err != nil {
			return nil, err
		}
		if err := merge(clientConfig, userAuthPartialConfig); err != nil {
			return nil, err
		}

		serverAuthPartialConfig := getServerIdentificationPartialConfig(configClusterInfo)
		if err := merge(clientConfig, serverAuthPartialConfig); err != nil {
			return nil, err
		}
	}

	return clientConfig, nil
}

// clientauth.Info 对象同时包含用户标识与服务端标识。二者需要不同的优先级顺序，
// 因此必须把对象拆开、分别合并。

// getServerIdentificationPartialConfig 从 configClusterInfo（命令行 flag 与合并后的
// .kubeconfig 文件的最终结果）中提取服务端标识信息。
func getServerIdentificationPartialConfig(configClusterInfo clientcmdapi.Cluster) *restclient.Config {
	configClientConfig := &restclient.Config{}
	configClientConfig.CAFile = configClusterInfo.CertificateAuthority
	configClientConfig.CAData = configClusterInfo.CertificateAuthorityData
	configClientConfig.Insecure = configClusterInfo.InsecureSkipTLSVerify
	configClientConfig.ServerName = configClusterInfo.TLSServerName

	return configClientConfig
}

// getUserIdentificationPartialConfig 从 configAuthInfo（命令行 flag 与合并后的
// .kubeconfig 文件的最终结果）中提取用户标识信息；若信息不足，则在可能时
// 交互式提示补充。
func (config *DirectClientConfig) getUserIdentificationPartialConfig(configAuthInfo clientcmdapi.AuthInfo, fallbackReader io.Reader, persistAuthConfig restclient.AuthProviderConfigPersister, configClusterInfo clientcmdapi.Cluster) (*restclient.Config, error) {
	mergedConfig := &restclient.Config{}

	// 按优先级直接覆盖已有值。
	// 显式 token 优先于 token 文件；同时保留 TokenFile 让 rest 层能自行轮询刷新。
	if len(configAuthInfo.Token) > 0 {
		mergedConfig.BearerToken = configAuthInfo.Token
		mergedConfig.BearerTokenFile = configAuthInfo.TokenFile
	} else if len(configAuthInfo.TokenFile) > 0 {
		tokenBytes, err := os.ReadFile(configAuthInfo.TokenFile)
		if err != nil {
			return nil, err
		}
		mergedConfig.BearerToken = string(tokenBytes)
		mergedConfig.BearerTokenFile = configAuthInfo.TokenFile
	}
	if len(configAuthInfo.Impersonate) > 0 {
		mergedConfig.Impersonate = restclient.ImpersonationConfig{
			UserName: configAuthInfo.Impersonate,
			UID:      configAuthInfo.ImpersonateUID,
			Groups:   configAuthInfo.ImpersonateGroups,
			Extra:    configAuthInfo.ImpersonateUserExtra,
		}
	}
	// 只有证书与私钥同时存在才启用 mTLS；否则保持未设置，让校验逻辑报错。
	if len(configAuthInfo.ClientCertificate) > 0 || len(configAuthInfo.ClientCertificateData) > 0 {
		mergedConfig.CertFile = configAuthInfo.ClientCertificate
		mergedConfig.CertData = configAuthInfo.ClientCertificateData
		mergedConfig.KeyFile = configAuthInfo.ClientKey
		mergedConfig.KeyData = configAuthInfo.ClientKeyData
	}
	if len(configAuthInfo.Username) > 0 || len(configAuthInfo.Password) > 0 {
		mergedConfig.Username = configAuthInfo.Username
		mergedConfig.Password = configAuthInfo.Password
	}
	if configAuthInfo.AuthProvider != nil {
		mergedConfig.AuthProvider = configAuthInfo.AuthProvider
		mergedConfig.AuthConfigPersister = persistAuthConfig
	}
	if configAuthInfo.Exec != nil {
		mergedConfig.ExecProvider = configAuthInfo.Exec
		// 安装提示会在名称未找到时打印到终端，去掉 ANSI 转义避免破坏用户终端状态。
		mergedConfig.ExecProvider.InstallHint = cleanANSIEscapeCodes(mergedConfig.ExecProvider.InstallHint)
		// exec 插件可以从集群扩展中读取额外的调用参数，这是该扩展键的唯一用途。
		mergedConfig.ExecProvider.Config = configClusterInfo.Extensions[clusterExtensionKey]
	}

	// 若信息仍不足以认证用户，则尝试交互式提示
	if !canIdentifyUser(*mergedConfig) && (fallbackReader != nil) {
		if len(config.promptedCredentials.username) > 0 && len(config.promptedCredentials.password) > 0 {
			mergedConfig.Username = config.promptedCredentials.username
			mergedConfig.Password = config.promptedCredentials.password
			return mergedConfig, nil
		}
		prompter := NewPromptingAuthLoader(fallbackReader)
		promptedAuthInfo, err := prompter.Prompt()
		if err != nil {
			return nil, err
		}
		// 先合并提示得到的凭据，再合并已有配置：已有配置优先，提示值只用于填补空缺。
		promptedConfig := makeUserIdentificationConfig(*promptedAuthInfo)
		previouslyMergedConfig := mergedConfig
		mergedConfig = &restclient.Config{}
		if err := merge(mergedConfig, promptedConfig); err != nil {
			return nil, err
		}
		if err := merge(mergedConfig, previouslyMergedConfig); err != nil {
			return nil, err
		}
		config.promptedCredentials.username = mergedConfig.Username
		config.promptedCredentials.password = mergedConfig.Password
	}

	return mergedConfig, nil
}

// makeUserIdentificationConfig 返回仅包含用户标识信息、可被合并的 client.Config。
func makeUserIdentificationConfig(info clientauth.Info) *restclient.Config {
	config := &restclient.Config{}
	config.Username = info.User
	config.Password = info.Password
	config.CertFile = info.CertFile
	config.KeyFile = info.KeyFile
	config.BearerToken = info.BearerToken
	return config
}

func canIdentifyUser(config restclient.Config) bool {
	return len(config.Username) > 0 ||
		(len(config.CertFile) > 0 || len(config.CertData) > 0) ||
		len(config.BearerToken) > 0 ||
		config.AuthProvider != nil ||
		config.ExecProvider != nil
}

// cleanANSIEscapeCodes 接收任意字符串，确保其中不含可能使终端进入异常状态的
// ANSI 转义序列（例如 "\e[1m" 会把文本加粗）。
func cleanANSIEscapeCodes(s string) string {
	// spaceControlCharacters 包含制表符、换行符、垂直制表符、换页符和回车符。
	// 它们属于 unicode.Cc 类别，但该类别还包含我们不想要的 ESC（U+001B）。
	spaceControlCharacters := unicode.RangeTable{
		R16: []unicode.Range16{
			{Lo: 0x0009, Hi: 0x000D, Stride: 1},
		},
	}

	// 为什么不采用“仅拒绝”（而非“仅允许”）？因为 unicode.C
	// 包含我们想要的换行符与制表符。
	// 采用“白名单”而非“仅剔除 ESC”：控制字符类别中还包含需要保留的换行与制表符，
	// 逐类允许更不容易误伤。不认识的字符会被转义为 \uXXXX 形式输出。
	allowedRanges := []*unicode.RangeTable{
		unicode.L,
		unicode.M,
		unicode.N,
		unicode.P,
		unicode.S,
		unicode.Z,
		&spaceControlCharacters,
	}
	builder := strings.Builder{}
	for _, roon := range s {
		if unicode.IsOneOf(allowedRanges, roon) {
			builder.WriteRune(roon) // 按 go 文档，返回的 error 恒为 nil
		} else {
			fmt.Fprintf(&builder, "%U", roon)
		}
	}
	return builder.String()
}

// Namespace 实现 ClientConfig 接口。
func (config *DirectClientConfig) Namespace() (string, bool, error) {
	if config.overrides != nil && config.overrides.Context.Namespace != "" {
		// 若配置为空但存在命名空间覆盖项，应返回该覆盖项，
		// 而不是让 config.ConfirmUsable() 报错。这样 in-cluster 客户端执行
		// `kubectl get pods --namespace=foo` 时，--namespace 才能生效而不被忽略。
		// 注意这里跳过了 ConfirmUsable：没有 kubeconfig 时也应尊重显式的命名空间覆盖。
		return config.overrides.Context.Namespace, true, nil
	}

	if err := config.ConfirmUsable(); err != nil {
		return "", false, err
	}

	configContext, err := config.getContext()
	if err != nil {
		return "", false, err
	}

	if len(configContext.Namespace) == 0 {
		return "default", false, nil
	}

	return configContext.Namespace, false, nil
}

// ConfigAccess 实现 ClientConfig 接口。
func (config *DirectClientConfig) ConfigAccess() ConfigAccess {
	return config.configAccess
}

// ConfirmUsable 检查特定上下文，判断配置的相应部分是否可用。配置中可能仍有错误，
// 但所请求或引用的部分没有错误。它不提前返回，以便尽可能多地找出错误。
// 特意收集全部错误再一次性返回，让用户可以同时看到多处配置问题，而不是修一个冒一个。
func (config *DirectClientConfig) ConfirmUsable() error {
	validationErrors := make([]error, 0)

	var contextName string
	if len(config.contextName) != 0 {
		contextName = config.contextName
	} else {
		contextName = config.config.CurrentContext
	}

	if len(contextName) > 0 {
		_, exists := config.config.Contexts[contextName]
		if !exists {
			validationErrors = append(validationErrors, &errContextNotFound{contextName})
		}
	}

	authInfoName, _ := config.getAuthInfoName()
	authInfo, _ := config.getAuthInfo()
	validationErrors = append(validationErrors, validateAuthInfo(authInfoName, authInfo)...)
	clusterName, _ := config.getClusterName()
	cluster, _ := config.getCluster()
	validationErrors = append(validationErrors, validateClusterInfo(clusterName, cluster)...)
	// 当指定了 direct client config 且唯一的错误是未定义 server 时，
	// 应返回标准的 "no config" 错误。
	// 只缺 server 时统一报告 ErrEmptyConfig，避免调用方看到令人困惑的“空集群”细节。
	if len(validationErrors) == 1 && validationErrors[0] == ErrEmptyCluster {
		return newErrConfigurationInvalid([]error{ErrEmptyConfig})
	}
	return newErrConfigurationInvalid(validationErrors)
}

// getContextName 返回默认或用户设置的上下文名，以及一个布尔值，
// 指示默认上下文名是否被用户设置的 flag 覆盖。
func (config *DirectClientConfig) getContextName() (string, bool) {
	if config.overrides != nil && len(config.overrides.CurrentContext) != 0 {
		return config.overrides.CurrentContext, true
	}
	if len(config.contextName) != 0 {
		return config.contextName, false
	}

	return config.config.CurrentContext, false
}

// getAuthInfoName 返回当前上下文对应的 authinfo 名称，以及一个布尔值，
// 指示默认 authInfo 名称是否被用户设置的 flag 覆盖。
func (config *DirectClientConfig) getAuthInfoName() (string, bool) {
	if config.overrides != nil && len(config.overrides.Context.AuthInfo) != 0 {
		return config.overrides.Context.AuthInfo, true
	}
	context, _ := config.getContext()
	return context.AuthInfo, false
}

// getClusterName 返回默认或用户设置的集群名，以及一个布尔值，
// 指示默认 clusterName 是否被用户设置的 flag 覆盖。
func (config *DirectClientConfig) getClusterName() (string, bool) {
	if config.overrides != nil && len(config.overrides.Context.Cluster) != 0 {
		return config.overrides.Context.Cluster, true
	}
	context, _ := config.getContext()
	return context.Cluster, false
}

// getContext 返回 clientcmdapi.Context；若所需的 context 找不到则返回错误。
// 合并顺序固定为“默认值 → kubeconfig 中的 context → 命令行覆盖”，后者的非空字段生效。
func (config *DirectClientConfig) getContext() (clientcmdapi.Context, error) {
	contexts := config.config.Contexts
	contextName, required := config.getContextName()

	mergedContext := clientcmdapi.NewContext()
	if configContext, exists := contexts[contextName]; exists {
		if err := merge(mergedContext, configContext); err != nil {
			return clientcmdapi.Context{}, err
		}
	} else if required {
		return clientcmdapi.Context{}, fmt.Errorf("context %q does not exist", contextName)
	}
	if config.overrides != nil {
		if err := merge(mergedContext, &config.overrides.Context); err != nil {
			return clientcmdapi.Context{}, err
		}
	}

	return *mergedContext, nil
}

// getAuthInfo 返回 clientcmdapi.AuthInfo；若所需的 auth info 找不到则返回错误。
// required 为 true 表示名字来自用户显式指定，此时找不到必须报错；否则允许得到默认空对象。
func (config *DirectClientConfig) getAuthInfo() (clientcmdapi.AuthInfo, error) {
	authInfos := config.config.AuthInfos
	authInfoName, required := config.getAuthInfoName()

	mergedAuthInfo := clientcmdapi.NewAuthInfo()
	if configAuthInfo, exists := authInfos[authInfoName]; exists {
		if err := merge(mergedAuthInfo, configAuthInfo); err != nil {
			return clientcmdapi.AuthInfo{}, err
		}
	} else if required {
		return clientcmdapi.AuthInfo{}, fmt.Errorf("auth info %q does not exist", authInfoName)
	}
	if config.overrides != nil {
		if err := merge(mergedAuthInfo, &config.overrides.AuthInfo); err != nil {
			return clientcmdapi.AuthInfo{}, err
		}

		// 处理 ClientKey/ClientKeyData 冲突：若覆盖项设置了 ClientKey，就同时使用
		// 覆盖项的 ClientKeyData；否则若原配置设置了 ClientKeyData，
		// 校验会返回 "client-key-data and client-key are both specified <user-name>" 错误。
		// 覆盖项的 file 与 data 必须成对替换，否则会残留另一半导致校验冲突。
		if len(config.overrides.AuthInfo.ClientKey) > 0 || len(config.overrides.AuthInfo.ClientKeyData) > 0 {
			mergedAuthInfo.ClientKey = config.overrides.AuthInfo.ClientKey
			mergedAuthInfo.ClientKeyData = config.overrides.AuthInfo.ClientKeyData
		}
		// 处理 ClientCertificate/ClientCertificateData 冲突：若覆盖项设置了
		// ClientCertificate，就同时使用覆盖项的 ClientCertificateData；否则若原配置
		// 设置了 ClientCertificateData，校验会返回
		// "client-cert-data and client-cert are both specified <user-name>" 错误。
		// 与 key 同理，证书的 file/data 也必须成对覆盖。
		if len(config.overrides.AuthInfo.ClientCertificate) > 0 || len(config.overrides.AuthInfo.ClientCertificateData) > 0 {
			mergedAuthInfo.ClientCertificate = config.overrides.AuthInfo.ClientCertificate
			mergedAuthInfo.ClientCertificateData = config.overrides.AuthInfo.ClientCertificateData
		}
	}

	return *mergedAuthInfo, nil
}

// getCluster 返回 clientcmdapi.Cluster；若所需的 cluster 找不到则返回错误。
// ClusterDefaults 最先合并，随后是 kubeconfig 中的集群，最后是显式覆盖。
func (config *DirectClientConfig) getCluster() (clientcmdapi.Cluster, error) {
	clusterInfos := config.config.Clusters
	clusterInfoName, required := config.getClusterName()

	mergedClusterInfo := clientcmdapi.NewCluster()
	if config.overrides != nil {
		if err := merge(mergedClusterInfo, &config.overrides.ClusterDefaults); err != nil {
			return clientcmdapi.Cluster{}, err
		}
	}
	if configClusterInfo, exists := clusterInfos[clusterInfoName]; exists {
		if err := merge(mergedClusterInfo, configClusterInfo); err != nil {
			return clientcmdapi.Cluster{}, err
		}
	} else if required {
		return clientcmdapi.Cluster{}, fmt.Errorf("cluster %q does not exist", clusterInfoName)
	}
	if config.overrides != nil {
		if err := merge(mergedClusterInfo, &config.overrides.ClusterInfo); err != nil {
			return clientcmdapi.Cluster{}, err
		}
	}

	// * 若覆盖项设置了 --insecure-skip-tls-verify=true 且没有附带 CA/CA 数据，
	//   应清除已设置的 CA/CA 数据；否则含 CA 引用的 kubeconfig 会报
	//   "CA and insecure-skip-tls-verify couldn't both be set" 错误。
	// * 覆盖项设置了 --certificate-authority 时，也应覆盖 TLS 跳过设置与 CA 数据，
	//   否则已有 CA 数据将优先。
	// 这三种覆盖必须整体替换 CA 相关三个字段，避免出现“既跳过校验又带 CA”的非法组合。
	if config.overrides != nil {
		caLen := len(config.overrides.ClusterInfo.CertificateAuthority)
		caDataLen := len(config.overrides.ClusterInfo.CertificateAuthorityData)
		if config.overrides.ClusterInfo.InsecureSkipTLSVerify || caLen > 0 || caDataLen > 0 {
			mergedClusterInfo.InsecureSkipTLSVerify = config.overrides.ClusterInfo.InsecureSkipTLSVerify
			mergedClusterInfo.CertificateAuthority = config.overrides.ClusterInfo.CertificateAuthority
			mergedClusterInfo.CertificateAuthorityData = config.overrides.ClusterInfo.CertificateAuthorityData
		}

		// 若覆盖项设置了 --tls-server-name，就用该值。
		// 若覆盖项设置了 --server，同样使用命令行上指定的 --tls-server-name 值。
		// 这保证了：设置 --server 时，若命令行指定了 tls-server-name，
		// KUBECONFIG 中的 tls-server-name 会被有效清除——这通常正是期望行为。
		if config.overrides.ClusterInfo.TLSServerName != "" || config.overrides.ClusterInfo.Server != "" {
			mergedClusterInfo.TLSServerName = config.overrides.ClusterInfo.TLSServerName
		}
	}

	return *mergedClusterInfo, nil
}

// inClusterClientConfig 构造在 Kubernetes 集群容器环境内可用的配置。
// 可以接收针对容器内命令显式提供的 flag 的覆盖选项。
type inClusterClientConfig struct {
	overrides               *ConfigOverrides
	inClusterConfigProvider func() (*restclient.Config, error)
}

// 编译期断言：in-cluster 配置同样满足 ClientConfig 接口。
var _ ClientConfig = &inClusterClientConfig{}

func (config *inClusterClientConfig) RawConfig() (clientcmdapi.Config, error) {
	return clientcmdapi.Config{}, fmt.Errorf("inCluster environment config doesn't support multiple clusters")
}

func (config *inClusterClientConfig) ClientConfig() (*restclient.Config, error) {
	// provider 可注入，便于测试时不依赖真实 ServiceAccount 挂载。
	inClusterConfigProvider := config.inClusterConfigProvider
	if inClusterConfigProvider == nil {
		inClusterConfigProvider = restclient.InClusterConfig
	}

	icc, err := inClusterConfigProvider()
	if err != nil {
		return nil, err
	}

	// in-cluster 配置只接受 host、token 或 CA 文件。
	// 只要其中某项被单独提供，就覆盖其他来源的值。
	if config.overrides != nil {
		if server := config.overrides.ClusterInfo.Server; len(server) > 0 {
			icc.Host = server
		}
		if len(config.overrides.AuthInfo.Token) > 0 || len(config.overrides.AuthInfo.TokenFile) > 0 {
			icc.BearerToken = config.overrides.AuthInfo.Token
			icc.BearerTokenFile = config.overrides.AuthInfo.TokenFile
		}
		if certificateAuthorityFile := config.overrides.ClusterInfo.CertificateAuthority; len(certificateAuthorityFile) > 0 {
			icc.TLSClientConfig.CAFile = certificateAuthorityFile
		}
	}

	return icc, nil
}

func (config *inClusterClientConfig) Namespace() (string, bool, error) {
	// 这种方式假设你已通过 downward API 设置了 POD_NAMESPACE 环境变量。
	// 为与 InClusterConfig 的原始初始化方式保持向后兼容，该检查必须最先执行。
	// 优先读取环境变量：downward API 显式注入的值比 ServiceAccount 挂载文件更可信。
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns, false, nil
	}

	// 否则回退到与 service account token 关联的命名空间（如可用）
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns, false, nil
		}
	}

	return "default", false, nil
}

func (config *inClusterClientConfig) ConfigAccess() ConfigAccess {
	return NewDefaultClientConfigLoadingRules()
}

// Possible 在可以加载集群内配置时返回 true。
func (config *inClusterClientConfig) Possible() bool {
	fi, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token")
	return os.Getenv("KUBERNETES_SERVICE_HOST") != "" &&
		os.Getenv("KUBERNETES_SERVICE_PORT") != "" &&
		err == nil && !fi.IsDir()
}

// BuildConfigFromFlags 是一个辅助函数，从 master url 或 kubeconfig 文件路径构建配置。
// 这些值以命令行 flag 的形式传给集群组件，警告信息也应体现这种用法。
// 若 masterUrl 和 kubeconfigPath 都未提供，则回退到 inClusterConfig；
// 若 inClusterConfig 也失败，则回退到默认配置。
// 三条路径的优先级：显式参数 → in-cluster → 默认加载规则（含 $HOME/.kube/config 等），
// 这是集群组件常见的渐进式配置体验。
func BuildConfigFromFlags(masterUrl, kubeconfigPath string) (*restclient.Config, error) {
	if kubeconfigPath == "" && masterUrl == "" {
		//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
		klog.Warning("Neither --kubeconfig nor --master was specified.  Using the inClusterConfig.  This might not work.")
		kubeconfig, err := restclient.InClusterConfig()
		if err == nil {
			return kubeconfig, nil
		}
		//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
		klog.Warning("error creating inClusterConfig, falling back to default config: ", err)
	}
	return NewNonInteractiveDeferredLoadingClientConfig(
		&ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
		&ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterUrl}}).ClientConfig()
}

// BuildConfigFromKubeconfigGetter 是一个辅助函数，从 master url 和
// kubeconfigGetter 构建配置。
func BuildConfigFromKubeconfigGetter(masterUrl string, kubeconfigGetter KubeconfigGetter) (*restclient.Config, error) {
	// TODO：这里不需要 DeferredLoader。重构代码，看看这里能否直接使用 DirectClientConfig。
	cc := NewNonInteractiveDeferredLoadingClientConfig(
		&ClientConfigGetter{kubeconfigGetter: kubeconfigGetter},
		&ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterUrl}})
	return cc.ClientConfig()
}
