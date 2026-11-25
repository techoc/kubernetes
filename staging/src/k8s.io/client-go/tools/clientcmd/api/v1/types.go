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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// 尽可能让 json 标签与命令行参数名保持一致。
// 顶层配置对象以及正常运行所必需的字段都不加 "omitempty"；真正可选的配置项才允许省略。
// 这些类型对外是稳定的 kubeconfig v1 文件格式，字段增删需保持向后兼容；
// 内部表示见 tools/clientcmd/api，转换由 conversion 完成。

// Config 保存了以某个用户身份连接远程 Kubernetes 集群所需的信息
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
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
	// Deprecated: 该字段自 v1.34 起已弃用，不再被任何 Kubernetes 组件使用；
	// 仅为兼容旧版 kubeconfig 而保留。
	Preferences Preferences `json:"preferences,omitzero"`
	// Clusters 是集群名到集群配置的列表
	Clusters []NamedCluster `json:"clusters"`
	// AuthInfos 是用户配置名到用户配置的列表。
	// 注意 json tag 为 "users"，与文件格式保持一致。
	AuthInfos []NamedAuthInfo `json:"users"`
	// Contexts 是上下文名到上下文配置的列表
	Contexts []NamedContext `json:"contexts"`
	// CurrentContext 是默认使用的上下文的名称
	CurrentContext string `json:"current-context"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty"`
}

// Deprecated: 该结构体自 v1.34 起已弃用，不再被任何 Kubernetes 组件使用；
// 仅为文件兼容保留。
type Preferences struct {
	// +optional
	Colors bool `json:"colors,omitempty"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty"`
}

// Cluster 包含与 Kubernetes 集群通信所需的信息
type Cluster struct {
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
	Extensions []NamedExtension `json:"extensions,omitempty"`
}

// AuthInfo 包含描述身份信息的内容，用于告知 Kubernetes 集群"你是谁"。
// 证书/密钥/Token 同时存在时，*Data 字段优先于文件路径；TokenFile 会周期性重读。
type AuthInfo struct {
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
	// Impersonate 是要扮演的用户名。名称与命令行 flag 一致。
	// +optional
	Impersonate string `json:"as,omitempty"`
	// ImpersonateUID 是要扮演的 uid。
	// +optional
	ImpersonateUID string `json:"as-uid,omitempty"`
	// ImpersonateGroups 是要扮演的用户组。
	// +optional
	ImpersonateGroups []string `json:"as-groups,omitempty"`
	// ImpersonateUserExtra 包含被扮演用户的附加信息。
	// +optional
	ImpersonateUserExtra map[string][]string `json:"as-user-extra,omitempty"`
	// Username 是向 Kubernetes 集群做基本认证的用户名。
	// +optional
	Username string `json:"username,omitempty"`
	// Password 是向 Kubernetes 集群做基本认证的密码。
	// +optional
	Password string `json:"password,omitempty" datapolicy:"password"`
	// AuthProvider 指定 Kubernetes 集群的自定义认证插件。
	// +optional
	AuthProvider *AuthProviderConfig `json:"auth-provider,omitempty"`
	// Exec 指定 Kubernetes 集群的基于 exec 的自定义认证插件。
	// +optional
	Exec *ExecConfig `json:"exec,omitempty"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty"`
}

// Context 是一组引用：集群（如何与 Kubernetes 集群通信）、用户（如何表明身份）
// 与命名空间（想操作哪一部分资源）。
type Context struct {
	// Cluster 是该上下文引用的集群名称
	Cluster string `json:"cluster"`
	// AuthInfo 是该上下文引用的用户配置名称
	AuthInfo string `json:"user"`
	// Namespace 是请求未指定命名空间时默认使用的命名空间
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Extensions 保存附加信息。这方便扩展组件读写时不会覆盖未知字段
	// +optional
	Extensions []NamedExtension `json:"extensions,omitempty"`
}

// NamedCluster 将昵称与集群信息关联。
// kubeconfig 用列表 + name 表示 map，便于 YAML 保序与人类阅读。
type NamedCluster struct {
	// Name 是该 Cluster 的昵称
	Name string `json:"name"`
	// Cluster 保存集群信息
	Cluster Cluster `json:"cluster"`
}

// NamedContext 将昵称与上下文信息关联。
// 同 NamedCluster，列表形式在转换时若出现重名会报错。
type NamedContext struct {
	// Name 是该 Context 的昵称
	Name string `json:"name"`
	// Context 保存上下文信息
	Context Context `json:"context"`
}

// NamedAuthInfo 将昵称与认证信息关联。
// json 中表现为 users 列表中的一项。
type NamedAuthInfo struct {
	// Name 是该 AuthInfo 的昵称
	Name string `json:"name"`
	// AuthInfo 保存认证信息
	AuthInfo AuthInfo `json:"user"`
}

// NamedExtension 将昵称与扩展信息关联。
// 扩展用于第三方工具持久化附加配置，未知字段得以在读写间保留。
type NamedExtension struct {
	// Name 是该 Extension 的昵称
	Name string `json:"name"`
	// Extension 保存扩展信息
	Extension runtime.RawExtension `json:"extension"`
}

// AuthProviderConfig 保存指定认证插件的配置。
// 多数内置 auth provider 已移除，当前主要用于 exec 插件之外的兼容配置。
type AuthProviderConfig struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

// ExecConfig 指定一个用于提供客户端凭据的命令。该命令会被 exec 执行，
// 并在标准输出上打印结构化的凭据。
// 具体的输入输出格式规范见 client.authentication.k8s.io API 组。
// Exec 插件认证，输入经 KUBERNETES_EXEC_INFO 传递，输出为 ExecCredential。
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

	// InteractiveMode 决定该插件与标准输入的关系。合法取值为 "Never"（该 exec 插件
	// 永不使用标准输入）、"IfAvailable"（若标准输入可用则该插件希望使用标准输入）
	// 或 "Always"（该 exec 插件必须依赖标准输入才能工作）。
	// 更多细节见 ExecInteractiveMode 的取值说明。
	//
	// 若 APIVersion 是 client.authentication.k8s.io/v1alpha1 或
	// client.authentication.k8s.io/v1beta1，则该字段可选，未设置时默认为
	// "IfAvailable"；否则该字段必填。
	//+optional
	InteractiveMode ExecInteractiveMode `json:"interactiveMode,omitempty"`
}

// ExecEnvVar 用于在执行基于 exec 的凭据插件时设置环境变量。
// 与宿主环境合并，同名时以插件配置为准。
type ExecEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ExecInteractiveMode 是一个字符串，描述 exec 插件与标准输入的关系。
// 仅在 v1 及更高版本要求显式设置；旧版本默认 IfAvailable。
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
