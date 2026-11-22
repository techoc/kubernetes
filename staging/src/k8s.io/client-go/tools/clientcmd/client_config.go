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
	// clusterExtensionKey is reserved in the cluster extensions list for exec plugin config.
	clusterExtensionKey = "client.authentication.k8s.io/exec"
)

var (
	// ClusterDefaults has the same behavior as the old EnvVar and DefaultCluster fields
	// DEPRECATED will be replaced
	ClusterDefaults = clientcmdapi.Cluster{Server: getDefaultServer()}
	// DefaultClientConfig represents the legacy behavior of this package for defaulting
	// DEPRECATED will be replace
	DefaultClientConfig = DirectClientConfig{*clientcmdapi.NewConfig(), "", &ConfigOverrides{
		ClusterDefaults: ClusterDefaults,
	}, nil, NewDefaultClientConfigLoadingRules(), promptedCredentials{}}
)

// getDefaultServer returns a default setting for DefaultClientConfig
// DEPRECATED
//
// 保留 KUBERNETES_MASTER 环境变量的读取，是为了兼容早期 kubectl/组件的使用方式；
// 新代码应显式提供 kubeconfig 或 server 地址。
func getDefaultServer() string {
	if server := os.Getenv("KUBERNETES_MASTER"); len(server) > 0 {
		return server
	}
	return "http://localhost:8080"
}

// ClientConfig is used to make it easy to get an api server client
//
// 它把“kubeconfig + 命令行覆盖项”解析为可直接用的 rest.Config；
// Namespace 的第二个返回值表示命名空间是否来自显式覆盖。
type ClientConfig interface {
	// RawConfig returns the merged result of all overrides
	RawConfig() (clientcmdapi.Config, error)
	// ClientConfig returns a complete client config
	ClientConfig() (*restclient.Config, error)
	// Namespace returns the namespace resulting from the merged
	// result of all overrides and a boolean indicating if it was
	// overridden
	Namespace() (string, bool, error)
	// ConfigAccess returns the rules for loading/persisting the config.
	ConfigAccess() ConfigAccess
}

// OverridingClientConfig is used to enable overrriding the raw KubeConfig
type OverridingClientConfig interface {
	ClientConfig
	// MergedRawConfig return the RawConfig merged with all overrides.
	MergedRawConfig() (clientcmdapi.Config, error)
}

// PersistAuthProviderConfigForUser 为指定用户构造凭据持久化器；返回 nil 表示不持久化。
type PersistAuthProviderConfigForUser func(user string) restclient.AuthProviderConfigPersister

// promptedCredentials 缓存交互式输入的凭据，避免同一配置对象被多次调用时重复提示。
type promptedCredentials struct {
	username string
	password string `datapolicy:"password"`
}

// DirectClientConfig is a ClientConfig interface that is backed by a clientcmdapi.Config, options overrides, and an optional fallbackReader for auth information
type DirectClientConfig struct {
	config         clientcmdapi.Config
	contextName    string
	overrides      *ConfigOverrides
	fallbackReader io.Reader
	configAccess   ConfigAccess
	// promptedCredentials store the credentials input by the user
	promptedCredentials promptedCredentials
}

// NewDefaultClientConfig creates a DirectClientConfig using the config.CurrentContext as the context name
func NewDefaultClientConfig(config clientcmdapi.Config, overrides *ConfigOverrides) OverridingClientConfig {
	return &DirectClientConfig{config, config.CurrentContext, overrides, nil, NewDefaultClientConfigLoadingRules(), promptedCredentials{}}
}

// NewNonInteractiveClientConfig creates a DirectClientConfig using the passed context name and does not have a fallback reader for auth information
func NewNonInteractiveClientConfig(config clientcmdapi.Config, contextName string, overrides *ConfigOverrides, configAccess ConfigAccess) OverridingClientConfig {
	return &DirectClientConfig{config, contextName, overrides, nil, configAccess, promptedCredentials{}}
}

// NewInteractiveClientConfig creates a DirectClientConfig using the passed context name and a reader in case auth information is not provided via files or flags
//
// fallbackReader 非 nil 时，缺失凭据会触发交互式提示；服务器端组件应使用 NonInteractive 版本。
func NewInteractiveClientConfig(config clientcmdapi.Config, contextName string, overrides *ConfigOverrides, fallbackReader io.Reader, configAccess ConfigAccess) OverridingClientConfig {
	return &DirectClientConfig{config, contextName, overrides, fallbackReader, configAccess, promptedCredentials{}}
}

// NewClientConfigFromBytes takes your kubeconfig and gives you back a ClientConfig
func NewClientConfigFromBytes(configBytes []byte) (OverridingClientConfig, error) {
	config, err := Load(configBytes)
	if err != nil {
		return nil, err
	}

	return &DirectClientConfig{*config, "", &ConfigOverrides{}, nil, nil, promptedCredentials{}}, nil
}

// RESTConfigFromKubeConfig is a convenience method to give back a restconfig from your kubeconfig bytes.
// For programmatic access, this is what you want 80% of the time
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

// MergedRawConfig returns the raw kube config merged with the overrides
//
// 与 RawConfig 不同，这里返回的是覆盖项生效后的视图，且当前 context 会被改写为合并后的名字，
// 便于调用方把完整配置原样输出或持久化。
func (config *DirectClientConfig) MergedRawConfig() (clientcmdapi.Config, error) {
	if err := config.ConfirmUsable(); err != nil {
		return clientcmdapi.Config{}, err
	}
	merged := config.config.DeepCopy()

	// set the AuthInfo merged with overrides in the merged config
	mergedAuthInfo, err := config.getAuthInfo()
	if err != nil {
		return clientcmdapi.Config{}, err
	}
	mergedAuthInfoName, _ := config.getAuthInfoName()
	merged.AuthInfos[mergedAuthInfoName] = &mergedAuthInfo

	// set the Context merged with overrides in the merged config
	mergedContext, err := config.getContext()
	if err != nil {
		return clientcmdapi.Config{}, err
	}
	mergedContextName, _ := config.getContextName()
	merged.Contexts[mergedContextName] = &mergedContext
	merged.CurrentContext = mergedContextName

	// set the Cluster merged with overrides in the merged config
	configClusterInfo, err := config.getCluster()
	if err != nil {
		return clientcmdapi.Config{}, err
	}
	configClusterName, _ := config.getClusterName()
	merged.Clusters[configClusterName] = &configClusterInfo
	return *merged, nil
}

// ClientConfig implements ClientConfig
func (config *DirectClientConfig) ClientConfig() (*restclient.Config, error) {
	// check that getAuthInfo, getContext, and getCluster do not return an error.
	// Do this before checking if the current config is usable in the event that an
	// AuthInfo, Context, or Cluster config with user-defined names are not found.
	// This provides a user with the immediate cause for error if one is found
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

	// only try to read the auth information if we are secure
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

// clientauth.Info object contain both user identification and server identification.  We want different precedence orders for
// both, so we have to split the objects and merge them separately.

// getServerIdentificationPartialConfig extracts server identification information from configClusterInfo
// (the final result of command line flags and merged .kubeconfig files).
func getServerIdentificationPartialConfig(configClusterInfo clientcmdapi.Cluster) *restclient.Config {
	configClientConfig := &restclient.Config{}
	configClientConfig.CAFile = configClusterInfo.CertificateAuthority
	configClientConfig.CAData = configClusterInfo.CertificateAuthorityData
	configClientConfig.Insecure = configClusterInfo.InsecureSkipTLSVerify
	configClientConfig.ServerName = configClusterInfo.TLSServerName

	return configClientConfig
}

// getUserIdentificationPartialConfig extracts user identification information from configAuthInfo
// (the final result of command line flags and merged .kubeconfig files);
// if the information available there is insufficient, it prompts (if possible) for additional information.
func (config *DirectClientConfig) getUserIdentificationPartialConfig(configAuthInfo clientcmdapi.AuthInfo, fallbackReader io.Reader, persistAuthConfig restclient.AuthProviderConfigPersister, configClusterInfo clientcmdapi.Cluster) (*restclient.Config, error) {
	mergedConfig := &restclient.Config{}

	// blindly overwrite existing values based on precedence
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

	// if there still isn't enough information to authenticate the user, try prompting
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

// makeUserIdentificationFieldsConfig returns a client.Config capable of being merged for only user identification information
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

// cleanANSIEscapeCodes takes an arbitrary string and ensures that there are no
// ANSI escape sequences that could put the terminal in a weird state (e.g.,
// "\e[1m" bolds text)
func cleanANSIEscapeCodes(s string) string {
	// spaceControlCharacters includes tab, new line, vertical tab, new page, and
	// carriage return. These are in the unicode.Cc category, but that category also
	// contains ESC (U+001B) which we don't want.
	spaceControlCharacters := unicode.RangeTable{
		R16: []unicode.Range16{
			{Lo: 0x0009, Hi: 0x000D, Stride: 1},
		},
	}

	// Why not make this deny-only (instead of allow-only)? Because unicode.C
	// contains newline and tab characters that we want.
	//
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
			builder.WriteRune(roon) // returns nil error, per go doc
		} else {
			fmt.Fprintf(&builder, "%U", roon)
		}
	}
	return builder.String()
}

// Namespace implements ClientConfig
func (config *DirectClientConfig) Namespace() (string, bool, error) {
	if config.overrides != nil && config.overrides.Context.Namespace != "" {
		// In the event we have an empty config but we do have a namespace override, we should return
		// the namespace override instead of having config.ConfirmUsable() return an error. This allows
		// things like in-cluster clients to execute `kubectl get pods --namespace=foo` and have the
		// --namespace flag honored instead of being ignored.
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

// ConfigAccess implements ClientConfig
func (config *DirectClientConfig) ConfigAccess() ConfigAccess {
	return config.configAccess
}

// ConfirmUsable looks a particular context and determines if that particular part of the config is useable.  There might still be errors in the config,
// but no errors in the sections requested or referenced.  It does not return early so that it can find as many errors as possible.
//
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
	// when direct client config is specified, and our only error is that no server is defined, we should
	// return a standard "no config" error
	// 只缺 server 时统一报告 ErrEmptyConfig，避免调用方看到令人困惑的“空集群”细节。
	if len(validationErrors) == 1 && validationErrors[0] == ErrEmptyCluster {
		return newErrConfigurationInvalid([]error{ErrEmptyConfig})
	}
	return newErrConfigurationInvalid(validationErrors)
}

// getContextName returns the default, or user-set context name, and a boolean that indicates
// whether the default context name has been overwritten by a user-set flag, or left as its default value
func (config *DirectClientConfig) getContextName() (string, bool) {
	if config.overrides != nil && len(config.overrides.CurrentContext) != 0 {
		return config.overrides.CurrentContext, true
	}
	if len(config.contextName) != 0 {
		return config.contextName, false
	}

	return config.config.CurrentContext, false
}

// getAuthInfoName returns a string containing the current authinfo name for the current context,
// and a boolean indicating  whether the default authInfo name is overwritten by a user-set flag, or
// left as its default value
func (config *DirectClientConfig) getAuthInfoName() (string, bool) {
	if config.overrides != nil && len(config.overrides.Context.AuthInfo) != 0 {
		return config.overrides.Context.AuthInfo, true
	}
	context, _ := config.getContext()
	return context.AuthInfo, false
}

// getClusterName returns a string containing the default, or user-set cluster name, and a boolean
// indicating whether the default clusterName has been overwritten by a user-set flag, or left as
// its default value
func (config *DirectClientConfig) getClusterName() (string, bool) {
	if config.overrides != nil && len(config.overrides.Context.Cluster) != 0 {
		return config.overrides.Context.Cluster, true
	}
	context, _ := config.getContext()
	return context.Cluster, false
}

// getContext returns the clientcmdapi.Context, or an error if a required context is not found.
//
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

// getAuthInfo returns the clientcmdapi.AuthInfo, or an error if a required auth info is not found.
//
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

		// Handle ClientKey/ClientKeyData conflict: if override sets ClientKey, also use override's ClientKeyData
		// otherwise if original config has ClientKeyData set,
		// validation returns error "client-key-data and client-key are both specified <user-name>"
		// 覆盖项的 file 与 data 必须成对替换，否则会残留另一半导致校验冲突。
		if len(config.overrides.AuthInfo.ClientKey) > 0 || len(config.overrides.AuthInfo.ClientKeyData) > 0 {
			mergedAuthInfo.ClientKey = config.overrides.AuthInfo.ClientKey
			mergedAuthInfo.ClientKeyData = config.overrides.AuthInfo.ClientKeyData
		}
		// Handle ClientCertificate/ClientCertificateData conflict, if override sets ClientCertificate, also use override's ClientCertificateData
		// otherwise if original config has ClientCertificateData set,
		// validation returns error "client-cert-data and client-cert are both specified <user-name>"
		// 与 key 同理，证书的 file/data 也必须成对覆盖。
		if len(config.overrides.AuthInfo.ClientCertificate) > 0 || len(config.overrides.AuthInfo.ClientCertificateData) > 0 {
			mergedAuthInfo.ClientCertificate = config.overrides.AuthInfo.ClientCertificate
			mergedAuthInfo.ClientCertificateData = config.overrides.AuthInfo.ClientCertificateData
		}
	}

	return *mergedAuthInfo, nil
}

// getCluster returns the clientcmdapi.Cluster, or an error if a required cluster is not found.
//
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

	// * An override of --insecure-skip-tls-verify=true and no accompanying CA/CA data should clear already-set CA/CA data
	// otherwise, a kubeconfig containing a CA reference would return an error that "CA and insecure-skip-tls-verify couldn't both be set".
	// * An override of --certificate-authority should also override TLS skip settings and CA data, otherwise existing CA data will take precedence.
	// 这三种覆盖必须整体替换 CA 相关三个字段，避免出现“既跳过校验又带 CA”的非法组合。
	if config.overrides != nil {
		caLen := len(config.overrides.ClusterInfo.CertificateAuthority)
		caDataLen := len(config.overrides.ClusterInfo.CertificateAuthorityData)
		if config.overrides.ClusterInfo.InsecureSkipTLSVerify || caLen > 0 || caDataLen > 0 {
			mergedClusterInfo.InsecureSkipTLSVerify = config.overrides.ClusterInfo.InsecureSkipTLSVerify
			mergedClusterInfo.CertificateAuthority = config.overrides.ClusterInfo.CertificateAuthority
			mergedClusterInfo.CertificateAuthorityData = config.overrides.ClusterInfo.CertificateAuthorityData
		}

		// if the --tls-server-name has been set in overrides, use that value.
		// if the --server has been set in overrides, then use the value of --tls-server-name specified on the CLI too.  This gives the property
		// that setting a --server will effectively clear the KUBECONFIG value of tls-server-name if it is specified on the command line which is
		// usually correct.
		if config.overrides.ClusterInfo.TLSServerName != "" || config.overrides.ClusterInfo.Server != "" {
			mergedClusterInfo.TLSServerName = config.overrides.ClusterInfo.TLSServerName
		}
	}

	return *mergedClusterInfo, nil
}

// inClusterClientConfig makes a config that will work from within a kubernetes cluster container environment.
// Can take options overrides for flags explicitly provided to the command inside the cluster container.
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

	// in-cluster configs only takes a host, token, or CA file
	// if any of them were individually provided, overwrite anything else
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
	// This way assumes you've set the POD_NAMESPACE environment variable using the downward API.
	// This check has to be done first for backwards compatibility with the way InClusterConfig was originally set up
	// 优先读取环境变量：downward API 显式注入的值比 ServiceAccount 挂载文件更可信。
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns, false, nil
	}

	// Fall back to the namespace associated with the service account token, if available
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

// Possible returns true if loading an inside-kubernetes-cluster is possible.
func (config *inClusterClientConfig) Possible() bool {
	fi, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token")
	return os.Getenv("KUBERNETES_SERVICE_HOST") != "" &&
		os.Getenv("KUBERNETES_SERVICE_PORT") != "" &&
		err == nil && !fi.IsDir()
}

// BuildConfigFromFlags is a helper function that builds configs from a master
// url or a kubeconfig filepath. These are passed in as command line flags for cluster
// components. Warnings should reflect this usage. If neither masterUrl or kubeconfigPath
// are passed in we fallback to inClusterConfig. If inClusterConfig fails, we fallback
// to the default config.
//
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

// BuildConfigFromKubeconfigGetter is a helper function that builds configs from a master
// url and a kubeconfigGetter.
func BuildConfigFromKubeconfigGetter(masterUrl string, kubeconfigGetter KubeconfigGetter) (*restclient.Config, error) {
	// TODO: We do not need a DeferredLoader here. Refactor code and see if we can use DirectClientConfig here.
	cc := NewNonInteractiveDeferredLoadingClientConfig(
		&ClientConfigGetter{kubeconfigGetter: kubeconfigGetter},
		&ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterUrl}})
	return cc.ClientConfig()
}
