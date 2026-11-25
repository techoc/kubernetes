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

package api

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

// 尽可能让 json 标签与命令行参数名保持一致。
// 顶层配置对象以及正常运行所必需的字段都不加 "omitempty"；真正可选的配置项才允许省略。

// Config 保存了以某个用户身份连接远程 Kubernetes 集群所需的信息。
// 重要：如果向该结构体添加字段，请同步更新 IsConfigEmpty()。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
//
// 这是 kubeconfig 的内部（非版本化）表示：加载多个文件后合并为 Config，再由
// DefaultClientConfig 按 CurrentContext 组装成 rest.Config。
type Config struct {
	// pkg/api/types.go 中 TypeMeta 的历史遗留字段。
	// TODO(jlowdermilk): 在消除下游依赖后移除。
	// +k8s:conversion-gen=false
	// +optional
	Kind string `json:"kind,omitempty"`
	// pkg/api/types.go 中 TypeMeta 的历史遗留字段。
	// TODO(jlowdermilk): 在消除下游依赖后移除。
	// +k8s:conversion-gen=false
	// +optional
	APIVersion string `json:"apiVersion,omitempty"`
	// Preferences 保存供命令行交互使用的一般信息。
	// Deprecated: 该字段自 v1.34 起已弃用，不再被任何 Kubernetes 组件使用。
	Preferences Preferences `json:"preferences,omitzero"`
	// Clusters 是集群名到集群配置的映射
	Clusters map[string]*Cluster `json:"clusters"`
	// AuthInfos 是用户配置名到用户配置的映射
	AuthInfos map[string]*AuthInfo `json:"users"`
	// Contexts 是上下文名到上下文配置的映射
	Contexts map[string]*Context `json:"contexts"`
	// CurrentContext 是默认使用的上下文的名称
	CurrentContext string `json:"current-context"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions map[string]runtime.Object `json:"extensions,omitempty"`
}

// 重要：如果向该结构体添加字段，请同步更新 IsConfigEmpty()。
// Deprecated: 该结构体自 v1.34 起已弃用，不再被任何 Kubernetes 组件使用；
// 保留仅为 kubeconfig 往返兼容。
type Preferences struct {
	// +optional
	Colors bool `json:"colors,omitempty"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions map[string]runtime.Object `json:"extensions,omitempty"`
}

// Cluster 包含与 Kubernetes 集群通信所需的信息
type Cluster struct {
	// LocationOfOrigin 指示该对象来自哪个文件。用于合并后配置的回写（round trip），
	// 但从不序列化。
	// 合并后写回时据此定位原始文件，确保只改动该条目所在的 kubeconfig。
	// +k8s:conversion-gen=false
	LocationOfOrigin string `json:"-"`
	// Server 是 Kubernetes 集群的地址（https://hostname:port）。
	Server string `json:"server"`
	// TLSServerName 用于校验服务端证书。为空时使用联系服务端时的主机名。
	// +optional
	TLSServerName string `json:"tls-server-name,omitempty"`
	// InsecureSkipTLSVerify 跳过对服务端证书的合法性校验。这会让你的 HTTPS 连接不再安全。
	// +optional
	InsecureSkipTLSVerify bool `json:"insecure-skip-tls-verify,omitempty"`
	// CertificateAuthority 是证书颁发机构 cert 文件的路径。
	// +optional
	CertificateAuthority string `json:"certificate-authority,omitempty"`
	// CertificateAuthorityData 包含 PEM 编码的证书颁发机构证书。优先于 CertificateAuthority。
	// 内联数据优先于路径；两者同时存在时忽略 CertificateAuthority。
	// +optional
	CertificateAuthorityData []byte `json:"certificate-authority-data,omitempty"`
	// ProxyURL 是该客户端发出的所有请求所使用的代理 URL。支持 "http"、"https"
	// 和 "socks5" 方案。若未提供该配置或为空字符串，客户端会尝试从
	// http_proxy 和 https_proxy 环境变量构造代理配置；若这些环境变量也未设置，
	// 客户端不会尝试代理请求。
	//
	// socks5 代理目前不支持 spdy 流式端点（exec、attach、port forward）。
	// +optional
	ProxyURL string `json:"proxy-url,omitempty"`
	// DisableCompression 允许客户端选择退出对所有服务端请求的响应压缩。
	// 当客户端与服务端之间带宽充足时，这可以节省服务端压缩、客户端解压的时间，
	// 从而加速请求（尤其是 list）：https://github.com/kubernetes/kubernetes/issues/112296。
	// +optional
	DisableCompression bool `json:"disable-compression,omitempty"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions map[string]runtime.Object `json:"extensions,omitempty"`
}

// AuthInfo 包含描述身份信息的内容，用于告知 Kubernetes 集群"你是谁"。
type AuthInfo struct {
	// LocationOfOrigin 指示该对象来自哪个文件。用于合并后配置的回写（round trip），
	// 但从不序列化。
	// +k8s:conversion-gen=false
	LocationOfOrigin string `json:"-"`
	// ClientCertificate 是 TLS 客户端证书文件的路径。
	// +optional
	ClientCertificate string `json:"client-certificate,omitempty"`
	// ClientCertificateData 包含 TLS 客户端证书文件的 PEM 编码数据。优先于 ClientCertificate。
	// +optional
	ClientCertificateData []byte `json:"client-certificate-data,omitempty"`
	// ClientKey 是 TLS 客户端密钥文件的路径。
	// +optional
	ClientKey string `json:"client-key,omitempty"`
	// ClientKeyData 包含 TLS 客户端密钥文件的 PEM 编码数据。优先于 ClientKey。
	// +optional
	ClientKeyData []byte `json:"client-key-data,omitempty" datapolicy:"security-key"`
	// Token 是向 Kubernetes 集群认证时使用的 bearer token。
	// +optional
	Token string `json:"token,omitempty" datapolicy:"token"`
	// TokenFile 指向包含 bearer token（同上）的文件。若 Token 与 TokenFile 同时存在，
	// TokenFile 会被周期性读取，最近一次成功读取的值优先于 Token。
	// +optional
	TokenFile string `json:"tokenFile,omitempty"`
	// Impersonate 是要扮演的用户名。
	// +optional
	Impersonate string `json:"act-as,omitempty"`
	// ImpersonateUID 是要扮演的 uid。
	// +optional
	ImpersonateUID string `json:"act-as-uid,omitempty"`
	// ImpersonateGroups 是要扮演的用户组。
	// +optional
	ImpersonateGroups []string `json:"act-as-groups,omitempty"`
	// ImpersonateUserExtra 包含被扮演用户的附加信息。
	// +optional
	ImpersonateUserExtra map[string][]string `json:"act-as-user-extra,omitempty"`
	// Username 是向 Kubernetes 集群做基本认证的用户名。
	// +optional
	Username string `json:"username,omitempty"`
	// Password 是向 Kubernetes 集群做基本认证的密码。
	// +optional
	Password string `json:"password,omitempty" datapolicy:"password"`
	// AuthProvider 指定 Kubernetes 集群的自定义认证插件。
	// 旧的进程外认证方式，逐步被 Exec 取代；同一条目不应同时配置两者。
	// +optional
	AuthProvider *AuthProviderConfig `json:"auth-provider,omitempty"`
	// Exec 指定 Kubernetes 集群的基于 exec 的自定义认证插件。
	// +optional
	Exec *ExecConfig `json:"exec,omitempty"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions map[string]runtime.Object `json:"extensions,omitempty"`
}

// Context 是一组引用：集群（如何与 Kubernetes 集群通信）、用户（如何表明身份）
// 与命名空间（想操作哪一部分资源）。
type Context struct {
	// LocationOfOrigin 指示该对象来自哪个文件。用于合并后配置的回写（round trip），
	// 但从不序列化。
	// +k8s:conversion-gen=false
	LocationOfOrigin string `json:"-"`
	// Cluster 是该上下文引用的集群名称
	Cluster string `json:"cluster"`
	// AuthInfo 是该上下文引用的用户配置名称
	AuthInfo string `json:"user"`
	// Namespace 是请求未指定命名空间时默认使用的命名空间
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions map[string]runtime.Object `json:"extensions,omitempty"`
}

// AuthProviderConfig 保存指定认证插件的配置。
type AuthProviderConfig struct {
	Name string `json:"name"`
	// Config 可能包含 token、refresh token 等机密，禁止直接打印。
	// +optional
	Config map[string]string `json:"config,omitempty"`
}

var _ fmt.Stringer = new(AuthProviderConfig)
var _ fmt.GoStringer = new(AuthProviderConfig)

// GoString 实现 fmt.GoStringer，并对 AuthProviderConfig 的敏感字段做脱敏，
// 防止通过日志意外泄露。
func (c AuthProviderConfig) GoString() string {
	return c.String()
}

// String 实现 fmt.Stringer，并对 AuthProviderConfig 的敏感字段做脱敏，
// 防止通过日志意外泄露。
func (c AuthProviderConfig) String() string {
	cfg := "<nil>"
	if c.Config != nil {
		cfg = "--- REDACTED ---"
	}
	return fmt.Sprintf("api.AuthProviderConfig{Name: %q, Config: map[string]string{%s}}", c.Name, cfg)
}

// ExecConfig 指定一个用于提供客户端凭据的命令。该命令会被 exec 执行，
// 并在标准输出上打印结构化的凭据。
//
// 具体的输入输出格式规范见 client.authentication.k8s.io API 组。
type ExecConfig struct {
	// 要执行的命令。
	Command string `json:"command"`
	// 执行命令时传入的参数。
	// +optional
	Args []string `json:"args"`
	// Env 定义暴露给该进程的附加环境变量。这些变量会与宿主环境以及
	// client-go 用来向插件传递参数的变量取并集。
	// +optional
	Env []ExecEnvVar `json:"env"`

	// ExecInfo 首选的输入版本。返回的 ExecCredentials 必须使用与输入相同的编码版本。
	APIVersion string `json:"apiVersion,omitempty"`

	// 当可执行文件似乎不存在时展示给用户的提示文本。
	// 例如，`brew install foo-cli` 可能是 foo-cli 在 macOS 系统上合适的 InstallHint。
	InstallHint string `json:"installHint,omitempty"`

	// ProvideClusterInfo 决定是否在 KUBERNETES_EXEC_INFO 环境变量中向该 exec 插件
	// 提供集群信息（其中可能包含非常大的 CA 数据）。默认为 false。
	// 包 k8s.io/client-go/tools/auth/exec 提供了读取该环境变量的辅助方法。
	ProvideClusterInfo bool `json:"provideClusterInfo"`

	// Config 保存针对所认证集群的、exec 插件特有的附加配置数据。
	//
	// 这些数据来源于 clientcmd Cluster 对象的 extensions[exec] 字段：
	//
	// clusters:
	// - name: my-cluster
	//   cluster:
	//     ...
	//     extensions:
	//     - name: client.authentication.k8s.io/exec  # reserved extension name for per cluster exec config
	//       extension:
	//         audience: 06e3fbd18de8  # arbitrary config
	//
	// 在某些环境下，用户的配置在多个集群之间可能完全相同（即调用同一个 exec 插件），
	// 只差一些每个集群特有的细节，例如 audience。该字段允许随集群信息直接指定
	// 按集群区分的配置。不建议用该字段存储机密数据，因为 exec 插件的主要好处之一
	// 就是无需把机密直接存储在 kubeconfig 中。
	// +k8s:conversion-gen=false
	Config runtime.Object `json:"-"`

	// InteractiveMode 决定该插件与标准输入的关系。合法取值为 "Never"（该 exec 插件
	// 永不使用标准输入）、"IfAvailable"（若标准输入可用则该插件希望使用标准输入）
	// 或 "Always"（该 exec 插件必须依赖标准输入才能工作）。
	// 更多细节见 ExecInteractiveMode 的取值说明。
	//
	// 若 APIVersion 是 client.authentication.k8s.io/v1alpha1 或
	// client.authentication.k8s.io/v1beta1，则该字段可选，未设置时默认为
	// "IfAvailable"；否则该字段必填。
	// +optional
	InteractiveMode ExecInteractiveMode `json:"interactiveMode,omitempty"`

	// StdinUnavailable 表示 exec 认证器能否把标准输入透传给该 exec 插件。
	// 例如，上层实体可能正在使用标准输入做别的事情，此时让 exec 插件使用标准输入
	// 并不安全。把它放在这里是为了让 exec 配置保持完整，但它从不被序列化。
	// +k8s:conversion-gen=false
	StdinUnavailable bool `json:"-"`

	// StdinUnavailableMessage 是可选消息：当 exec 认证器需要标准输入、但
	// StdinUnavailable 为 true 而无法运行该 exec 插件时展示此消息。
	// 例如，一个已经用标准输入读取用户指令的进程可以把该消息设为
	// "used by my-program to read user instructions"。
	// +k8s:conversion-gen=false
	StdinUnavailableMessage string `json:"-"`

	// PluginPolicy 是决定已配置的 `Command` 能否运行的策略。
	// +k8s:conversion-gen=false
	PluginPolicy PluginPolicy `json:"-"`
}

// AllowlistEntry 是允许列表（allowlist）中的一项。每个允许列表条目至少要有一个
// 字段非空，所有字段都为空的结构体视为配置错误。每个字段都是一条执行条件；
// 指定多个字段时，所有指定字段的条件必须同时满足——即单个条目的结果是
// 条目内所有指定字段对应检查的逻辑与。
type AllowlistEntry struct {
	// 命令匹配的方式：先用 `exec.LookPath` 分别解析插件与允许列表条目中名称的
	// 绝对路径，两者调用结果必须相等。若任一次 `exec.LookPath` 调用出错，
	// `Command` 检查即视为失败。
	Command string `json:"-"`
}

// PluginPolicy 描述 client-go 凭据插件的策略类型及（如有）允许列表。
type PluginPolicy struct {
	// PolicyType 指定决定哪些 client-go 凭据插件可以执行的策略，
	// 取值必须是 { "", "AllowAll", "DenyAll", "Allowlist" } 之一。
	// 为空时回退为 "AllowAll"（为保证向后兼容必须如此）。DenyAll 表示任何凭据
	// 插件都不得运行。Allowlist 表示只有满足 `credentialPluginAllowlist` 字段
	// 所指定条件的插件才能运行。若策略不是 `Allowlist` 却提供了允许列表，
	// 视为配置错误。
	PolicyType PolicyType `json:"-"`

	// Allowlist 是允许列表条目的切片；任一条目匹配即可允许相应可执行文件运行，
	// 即结果是所有条目的逻辑或。当策略不是 "Allowlist" 时，该列表必须为 nil。
	Allowlist []AllowlistEntry `json:"-"`
}

// PolicyType 是凭据插件执行策略的枚举值：
// AllowAll 不限制；DenyAll 禁止所有凭据插件；Allowlist 仅允许 allowlist 中匹配的插件。
type PolicyType string

const (
	PluginPolicyAllowAll  PolicyType = "AllowAll"
	PluginPolicyDenyAll   PolicyType = "DenyAll"
	PluginPolicyAllowlist PolicyType = "Allowlist"
)

var _ fmt.Stringer = new(ExecConfig)
var _ fmt.GoStringer = new(ExecConfig)

// GoString 实现 fmt.GoStringer，并对 ExecConfig 的敏感字段做脱敏，
// 防止通过日志意外泄露。
func (c ExecConfig) GoString() string {
	return c.String()
}

// String 实现 fmt.Stringer，并对 ExecConfig 的敏感字段做脱敏，
// 防止通过日志意外泄露。
func (c ExecConfig) String() string {
	// Args 与 Env 里可能夹带 token，Config 也可能含机密，统一打码。
	var args []string
	if len(c.Args) > 0 {
		args = []string{"--- REDACTED ---"}
	}
	env := "[]ExecEnvVar(nil)"
	if len(c.Env) > 0 {
		env = "[]ExecEnvVar{--- REDACTED ---}"
	}
	config := "runtime.Object(nil)"
	if c.Config != nil {
		config = "runtime.Object(--- REDACTED ---)"
	}
	return fmt.Sprintf("api.ExecConfig{Command: %q, Args: %#v, Env: %s, APIVersion: %q, ProvideClusterInfo: %t, Config: %s, StdinUnavailable: %t}", c.Command, args, env, c.APIVersion, c.ProvideClusterInfo, config, c.StdinUnavailable)
}

// ExecEnvVar 用于在执行基于 exec 的凭据插件时设置环境变量。
type ExecEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ExecInteractiveMode 是一个字符串，描述 exec 插件与标准输入的关系。
type ExecInteractiveMode string

const (
	// NeverExecInteractiveMode 声明该 exec 插件永不使用标准输入，
	// 因此无论标准输入是否可用于用户输入，该 exec 插件都会运行。
	NeverExecInteractiveMode ExecInteractiveMode = "Never"
	// IfAvailableExecInteractiveMode 声明该 exec 插件在标准输入可用时希望使用它，
	// 但标准输入不可用时也能工作。因此无论 stdin 是否可用于用户输入，该 exec
	// 插件都会运行；若标准输入可用于用户输入，则会提供给该 exec 插件。
	IfAvailableExecInteractiveMode ExecInteractiveMode = "IfAvailable"
	// AlwaysExecInteractiveMode 声明该 exec 插件必须使用标准输入才能运行，
	// 因此只有标准输入可用于用户输入时该 exec 插件才会运行。若标准输入不可用，
	// 该 exec 插件不会运行，exec 插件运行器会返回一个错误。
	AlwaysExecInteractiveMode ExecInteractiveMode = "Always"
)

// NewConfig 是一个便捷函数，返回 map 字段均非 nil 的新 Config 对象。
// 非 nil 的 map 让调用方可以直接赋值，避免到处判空。
func NewConfig() *Config {
	return &Config{
		Clusters:   make(map[string]*Cluster),
		AuthInfos:  make(map[string]*AuthInfo),
		Contexts:   make(map[string]*Context),
		Extensions: make(map[string]runtime.Object),
	}
}

// NewContext 是一个便捷函数，返回 map 字段均非 nil 的新 Context 对象。
func NewContext() *Context {
	return &Context{Extensions: make(map[string]runtime.Object)}
}

// NewCluster 是一个便捷函数，返回 map 字段均非 nil 的新 Cluster 对象。
func NewCluster() *Cluster {
	return &Cluster{Extensions: make(map[string]runtime.Object)}
}

// NewAuthInfo 是一个便捷函数，返回 map 字段均非 nil 的新 AuthInfo 对象。
func NewAuthInfo() *AuthInfo {
	return &AuthInfo{
		Extensions:           make(map[string]runtime.Object),
		ImpersonateUserExtra: make(map[string][]string),
	}
}

// NewPreferences 是一个便捷函数，返回 map 字段均非 nil 的新 Preferences 对象。
// Deprecated: 该方法自 v1.34 起已弃用，不再被任何 Kubernetes 组件使用。
func NewPreferences() *Preferences {
	return &Preferences{Extensions: make(map[string]runtime.Object)}
}
