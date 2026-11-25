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
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// ConfigOverrides 中的值会覆盖从实际 Config 对象读取到的信息。不能直接使用 Config 对象，
// 因为 Config 内部是 map，而覆盖值被限制为“至多一个”：命令行只能指定单个
// cluster/user/context，因此用扁平结构而非 map 表示覆盖值。非零字段会覆盖
// kubeconfig 中加载出的对应信息。
type ConfigOverrides struct {
	// AuthInfo 覆盖认证信息（证书、token、身份伪装等）。
	AuthInfo clientcmdapi.AuthInfo
	// ClusterDefaults 会在加载所配置的 cluster 信息之前应用：作为 cluster 的初始值，
	// 随后再被 kubeconfig 中的值覆盖，优先级最低。
	ClusterDefaults clientcmdapi.Cluster
	// ClusterInfo 覆盖集群连接信息（server、CA 等）。
	ClusterInfo clientcmdapi.Cluster
	// Context 覆盖 context 中的 cluster/user/namespace 引用。
	Context clientcmdapi.Context
	// CurrentContext 覆盖要使用的 context 名称。
	CurrentContext string
	// Timeout 是单个请求的超时字符串（如 "5s"），空串表示不覆盖。
	Timeout string
}

// ConfigOverrideFlags 保存用于绑定命令行 flag 的 flag 名称。注意该结构与 ConfigOverrides
// 严格对应，字段一一对应，便于扩展者自定义 flag 名称。
type ConfigOverrideFlags struct {
	AuthOverrideFlags    AuthOverrideFlags
	ClusterOverrideFlags ClusterOverrideFlags
	ContextOverrideFlags ContextOverrideFlags
	CurrentContext       FlagInfo
	Timeout              FlagInfo
}

// AuthOverrideFlags 保存为 AuthInfo 对象绑定命令行 flag 时使用的 flag 名称
type AuthOverrideFlags struct {
	ClientCertificate FlagInfo
	ClientKey         FlagInfo
	Token             FlagInfo
	Impersonate       FlagInfo
	ImpersonateUID    FlagInfo
	ImpersonateGroups FlagInfo
	Username          FlagInfo
	Password          FlagInfo
}

// ContextOverrideFlags 保存为 Cluster 对象绑定命令行 flag 时使用的 flag 名称。
// 注意，尽管注释写的是 Cluster，实际绑定的是 Context 字段。
type ContextOverrideFlags struct {
	ClusterName  FlagInfo
	AuthInfoName FlagInfo
	Namespace    FlagInfo
}

// ClusterOverride 保存为 Cluster 对象绑定命令行 flag 时使用的 flag 名称
type ClusterOverrideFlags struct {
	APIServer             FlagInfo
	APIVersion            FlagInfo
	CertificateAuthority  FlagInfo
	InsecureSkipTLSVerify FlagInfo
	TLSServerName         FlagInfo
	ProxyURL              FlagInfo
	DisableCompression    FlagInfo
}

// FlagInfo 包含如何注册 flag 的信息。如果想向扩展者提供一组推荐的 flag 名称、描述和默认值，
// 同时又允许其自定义，这个结构体很有用：既能保证扩展的一致性，又不做硬性规定。
type FlagInfo struct {
	// LongName 是 flag 的长名称。为空时不会绑定该 flag
	LongName string
	// ShortName 是 flag 的单字符短名称。为空时不提供短 flag
	ShortName string
	// Default 是 flag 的默认值
	Default string
	// Description 是 flag 的描述
	Description string
}

// AddSecretAnnotation 将敏感 flag 加入 Annotation。
// 标记该 flag 含敏感值（证书私钥、token、密码等），使日志或诊断输出时
// 能据此脱敏。返回自身以便链式调用。
func (f FlagInfo) AddSecretAnnotation(flags *pflag.FlagSet) FlagInfo {
	flags.SetAnnotation(f.LongName, "classified", []string{"true"})
	return f
}

// BindStringFlag 根据提供的信息绑定 flag。若 LongName == ""，则不注册任何内容
func (f FlagInfo) BindStringFlag(flags *pflag.FlagSet, target *string) FlagInfo {
	// 没有长名称就无法注册 flag，直接跳过，便于扩展者通过置空禁用某个 flag。
	if len(f.LongName) > 0 {
		flags.StringVarP(target, f.LongName, f.ShortName, f.Default, f.Description)
	}
	return f
}

// BindTransformingStringFlag 根据提供的信息绑定 flag。若 LongName == ""，则不注册任何内容。
// 绑定前对输入值应用 transformer，例如从 "ns/foo" 中去掉命名空间前缀。
func (f FlagInfo) BindTransformingStringFlag(flags *pflag.FlagSet, target *string, transformer func(string) (string, error)) FlagInfo {
	// 没有长名称就无法注册 flag
	if len(f.LongName) > 0 {
		flags.VarP(newTransformingStringValue(f.Default, target, transformer), f.LongName, f.ShortName, f.Description)
	}
	return f
}

// BindStringSliceFlag 根据提供的信息绑定 flag。若 LongName == ""，则不注册任何内容。
// 实际使用 StringArray（重复 flag 累积），而非逗号分隔的 StringSlice。
func (f FlagInfo) BindStringArrayFlag(flags *pflag.FlagSet, target *[]string) FlagInfo {
	// 没有长名称就无法注册 flag
	if len(f.LongName) > 0 {
		// Default 是单个字符串，若有默认值则作为数组的唯一初始元素。
		sliceVal := []string{}
		if len(f.Default) > 0 {
			sliceVal = []string{f.Default}
		}
		flags.StringArrayVarP(target, f.LongName, f.ShortName, sliceVal, f.Description)
	}
	return f
}

// BindBoolFlag 根据提供的信息绑定 flag。若 LongName == ""，则不注册任何内容
func (f FlagInfo) BindBoolFlag(flags *pflag.FlagSet, target *bool) FlagInfo {
	// 没有长名称就无法注册 flag
	if len(f.LongName) > 0 {
		// 尝试将 Default 解析为 bool，失败则视为 false：默认值以字符串存储，
		// 解析失败时退化为 false，而不是让程序启动失败。
		boolVal, err := strconv.ParseBool(f.Default)
		if err != nil {
			boolVal = false
		}

		flags.BoolVarP(target, f.LongName, f.ShortName, boolVal, f.Description)
	}
	return f
}

// 推荐的 flag 名称常量；对外暴露，便于扩展者复用同一命名。
const (
	FlagClusterName        = "cluster"
	FlagAuthInfoName       = "user"
	FlagContext            = "context"
	FlagNamespace          = "namespace"
	FlagAPIServer          = "server"
	FlagTLSServerName      = "tls-server-name"
	FlagInsecure           = "insecure-skip-tls-verify"
	FlagCertFile           = "client-certificate"
	FlagKeyFile            = "client-key"
	FlagCAFile             = "certificate-authority"
	FlagEmbedCerts         = "embed-certs"
	FlagBearerToken        = "token"
	FlagImpersonate        = "as"
	FlagImpersonateUID     = "as-uid"
	FlagImpersonateGroup   = "as-group"
	FlagUsername           = "username"
	FlagPassword           = "password"
	FlagTimeout            = "request-timeout"
	FlagProxyURL           = "proxy-url"
	FlagDisableCompression = "disable-compression"
)

// RecommendedConfigOverrideFlags 是一个便捷方法，返回带有自定义前缀的推荐 flag 名称：
// 即一套可直接使用的 flag 定义，全部带 prefix，方便与其它组件的 flag 共存。
func RecommendedConfigOverrideFlags(prefix string) ConfigOverrideFlags {
	return ConfigOverrideFlags{
		AuthOverrideFlags:    RecommendedAuthOverrideFlags(prefix),
		ClusterOverrideFlags: RecommendedClusterOverrideFlags(prefix),
		ContextOverrideFlags: RecommendedContextOverrideFlags(prefix),

		CurrentContext: FlagInfo{prefix + FlagContext, "", "", "The name of the kubeconfig context to use"},
		// 默认 "0" 表示不设超时；非零值必须带时间单位。
		Timeout: FlagInfo{prefix + FlagTimeout, "", "0", "The length of time to wait before giving up on a single server request. Non-zero values should contain a corresponding time unit (e.g. 1s, 2m, 3h). A value of zero means don't timeout requests."},
	}
}

// RecommendedAuthOverrideFlags 是一个便捷方法，返回带有自定义前缀的推荐 flag 名称
func RecommendedAuthOverrideFlags(prefix string) AuthOverrideFlags {
	return AuthOverrideFlags{
		ClientCertificate: FlagInfo{prefix + FlagCertFile, "", "", "Path to a client certificate file for TLS"},
		ClientKey:         FlagInfo{prefix + FlagKeyFile, "", "", "Path to a client key file for TLS"},
		Token:             FlagInfo{prefix + FlagBearerToken, "", "", "Bearer token for authentication to the API server"},
		Impersonate:       FlagInfo{prefix + FlagImpersonate, "", "", "Username to impersonate for the operation"},
		ImpersonateUID:    FlagInfo{prefix + FlagImpersonateUID, "", "", "UID to impersonate for the operation"},
		ImpersonateGroups: FlagInfo{prefix + FlagImpersonateGroup, "", "", "Group to impersonate for the operation, this flag can be repeated to specify multiple groups."},
		Username:          FlagInfo{prefix + FlagUsername, "", "", "Username for basic authentication to the API server"},
		Password:          FlagInfo{prefix + FlagPassword, "", "", "Password for basic authentication to the API server"},
	}
}

// RecommendedClusterOverrideFlags 是一个便捷方法，返回带有自定义前缀的推荐 flag 名称
func RecommendedClusterOverrideFlags(prefix string) ClusterOverrideFlags {
	return ClusterOverrideFlags{
		APIServer:             FlagInfo{prefix + FlagAPIServer, "", "", "The address and port of the Kubernetes API server"},
		CertificateAuthority:  FlagInfo{prefix + FlagCAFile, "", "", "Path to a cert file for the certificate authority"},
		InsecureSkipTLSVerify: FlagInfo{prefix + FlagInsecure, "", "false", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure"},
		TLSServerName:         FlagInfo{prefix + FlagTLSServerName, "", "", "If provided, this name will be used to validate server certificate. If this is not provided, hostname used to contact the server is used."},
		ProxyURL:              FlagInfo{prefix + FlagProxyURL, "", "", "If provided, this URL will be used to connect via proxy"},
		DisableCompression:    FlagInfo{prefix + FlagDisableCompression, "", "", "If true, opt-out of response compression for all requests to the server"},
	}
}

// RecommendedContextOverrideFlags 是一个便捷方法，返回带有自定义前缀的推荐 flag 名称
func RecommendedContextOverrideFlags(prefix string) ContextOverrideFlags {
	return ContextOverrideFlags{
		ClusterName:  FlagInfo{prefix + FlagClusterName, "", "", "The name of the kubeconfig cluster to use"},
		AuthInfoName: FlagInfo{prefix + FlagAuthInfoName, "", "", "The name of the kubeconfig user to use"},
		// namespace 提供 -n 短选项，是 kubectl 风格命令行的惯例。
		Namespace: FlagInfo{prefix + FlagNamespace, "n", "", "If present, the namespace scope for this CLI request"},
	}
}

// BindOverrideFlags 是一个便捷方法，将指定的 flag 绑定到对应的变量上：
// 一次性把认证、集群、context 及超时相关的 flag 绑定到 overrides 上。
func BindOverrideFlags(overrides *ConfigOverrides, flags *pflag.FlagSet, flagNames ConfigOverrideFlags) {
	BindAuthInfoFlags(&overrides.AuthInfo, flags, flagNames.AuthOverrideFlags)
	BindClusterFlags(&overrides.ClusterInfo, flags, flagNames.ClusterOverrideFlags)
	BindContextFlags(&overrides.Context, flags, flagNames.ContextOverrideFlags)
	flagNames.CurrentContext.BindStringFlag(flags, &overrides.CurrentContext)
	flagNames.Timeout.BindStringFlag(flags, &overrides.Timeout)
}

// BindAuthInfoFlags 是一个便捷方法，将指定的 flag 绑定到对应的变量上。
// 所有认证 flag 都标记为敏感信息，供后续日志脱敏使用。
func BindAuthInfoFlags(authInfo *clientcmdapi.AuthInfo, flags *pflag.FlagSet, flagNames AuthOverrideFlags) {
	flagNames.ClientCertificate.BindStringFlag(flags, &authInfo.ClientCertificate).AddSecretAnnotation(flags)
	flagNames.ClientKey.BindStringFlag(flags, &authInfo.ClientKey).AddSecretAnnotation(flags)
	flagNames.Token.BindStringFlag(flags, &authInfo.Token).AddSecretAnnotation(flags)
	flagNames.Impersonate.BindStringFlag(flags, &authInfo.Impersonate).AddSecretAnnotation(flags)
	flagNames.ImpersonateUID.BindStringFlag(flags, &authInfo.ImpersonateUID).AddSecretAnnotation(flags)
	flagNames.ImpersonateGroups.BindStringArrayFlag(flags, &authInfo.ImpersonateGroups).AddSecretAnnotation(flags)
	flagNames.Username.BindStringFlag(flags, &authInfo.Username).AddSecretAnnotation(flags)
	flagNames.Password.BindStringFlag(flags, &authInfo.Password).AddSecretAnnotation(flags)
}

// BindClusterFlags 是一个便捷方法，将指定的 flag 绑定到对应的变量上
func BindClusterFlags(clusterInfo *clientcmdapi.Cluster, flags *pflag.FlagSet, flagNames ClusterOverrideFlags) {
	flagNames.APIServer.BindStringFlag(flags, &clusterInfo.Server)
	flagNames.CertificateAuthority.BindStringFlag(flags, &clusterInfo.CertificateAuthority)
	flagNames.InsecureSkipTLSVerify.BindBoolFlag(flags, &clusterInfo.InsecureSkipTLSVerify)
	flagNames.TLSServerName.BindStringFlag(flags, &clusterInfo.TLSServerName)
	flagNames.ProxyURL.BindStringFlag(flags, &clusterInfo.ProxyURL)
	flagNames.DisableCompression.BindBoolFlag(flags, &clusterInfo.DisableCompression)
}

// BindFlags 是一个便捷方法，将指定的 flag 绑定到对应的变量上。
// 注释中的函数名已过时（实际为 BindContextFlags），保留原文以免丢失历史信息。
// namespace flag 使用转换绑定，以兼容用户传入 "ns/foo" 这类写法。
func BindContextFlags(contextInfo *clientcmdapi.Context, flags *pflag.FlagSet, flagNames ContextOverrideFlags) {
	flagNames.ClusterName.BindStringFlag(flags, &contextInfo.Cluster)
	flagNames.AuthInfoName.BindStringFlag(flags, &contextInfo.AuthInfo)
	flagNames.Namespace.BindTransformingStringFlag(flags, &contextInfo.Namespace, RemoveNamespacesPrefix)
}

// RemoveNamespacesPrefix 是一个 transformer，不区分大小写地去除 "ns/"、"namespace/" 和 "namespaces/" 前缀。
// 兼容 kubectl 用户习惯——复制资源路径前缀后作为 namespace 参数。
// 只匹配“前缀后仍有内容”的情况，避免把单独的 "ns/" 变成空字符串。
func RemoveNamespacesPrefix(value string) (string, error) {
	for _, prefix := range []string{"namespaces/", "namespace/", "ns/"} {
		if len(value) > len(prefix) && strings.EqualFold(value[0:len(prefix)], prefix) {
			value = value[len(prefix):]
			break
		}
	}
	return value, nil
}
