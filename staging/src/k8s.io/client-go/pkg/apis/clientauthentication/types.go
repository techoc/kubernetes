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

package clientauthentication

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExecCredential 是 exec 插件与 HTTP transport 之间交换凭据的内部（非版本化）类型；
// 各版本 API 会转换到该类型后再被客户端消费。
type ExecCredential struct {
	metav1.TypeMeta

	// Spec 携带 transport 传给插件的请求级、运行时信息，例如本次会话是否处于交互模式。
	Spec ExecCredentialSpec

	// Status 由插件填写，包含 transport 访问 API 时应使用的凭据。
	// +optional
	Status *ExecCredentialStatus
}

// ExecCredentialSpec 保存 transport 提供的请求级和运行时信息。
type ExecCredentialSpec struct {
	// Interactive 为 true 表示 transport 检测到命令由交互式终端发起（stdin 已接入此 exec 插件）。
	// +optional
	Interactive bool

	// Cluster 提供 exec 插件与所认证集群通信所需的信息。
	// 仅当 exec provider 配置中 provideClusterInfo 为 true 时（即 ExecConfig.ProvideClusterInfo）才非空。
	// +optional
	Cluster *Cluster
}

// ExecCredentialStatus 保存供 transport 使用的凭据。
type ExecCredentialStatus struct {
	// ExpirationTimestamp 为所提供凭据的过期时间；为空表示不过期或无法预知。
	// +optional
	ExpirationTimestamp *metav1.Time
	// Token 是客户端请求认证使用的 bearer token。
	// +optional
	Token string `datapolicy:"token"`
	// ClientCertificateData 为 PEM 编码的客户端 TLS 证书。
	// +optional
	ClientCertificateData string
	// ClientKeyData 为 PEM 编码的客户端 TLS 私钥。
	// +optional
	ClientKeyData string `datapolicy:"secret-key"`
}

// Cluster 提供 exec 插件与所认证集群通信所需的信息。
//
// 为确保该结构像一份 kubeconfig 那样包含连接集群所需的全部内容，其字段与
// "k8s.io/client-go/tools/clientcmd/api/v1".Cluster 保持一致，仅 CertificateAuthority
// 例外：CA 数据始终以字节形式传给插件。
// 这样插件仅凭该结构即可连接目标集群。
type Cluster struct {
	// Server 为集群地址，形如 https://hostname:port。
	Server string
	// TLSServerName 传给服务端用于 SNI，客户端也用它校验服务端证书；
	// 为空时退化为连接该服务端时使用的主机名。
	// +optional
	TLSServerName string
	// InsecureSkipTLSVerify 跳过服务端证书有效性校验，会使 HTTPS 连接不安全，仅应在受控测试环境使用。
	// +optional
	InsecureSkipTLSVerify bool
	// CAData 为 PEM 编码的证书颁发机构证书；为空则使用系统根证书。
	// +listType=atomic
	// +optional
	CertificateAuthorityData []byte
	// ProxyURL 是访问该集群所有请求所使用的代理 URL。
	// +optional
	ProxyURL string
	// DisableCompression 允许客户端对所有发往服务端的请求关闭响应压缩。当客户端与服务端
	// 之间带宽充足时，这能加快请求（尤其是 list 操作）：用带宽换取服务端压缩和客户端解压
	// 的耗时。https://github.com/kubernetes/kubernetes/issues/112296
	// +optional
	DisableCompression bool
	// Config 保存针对所认证集群、供 exec 插件使用的额外配置。
	//
	// 该数据来自 clientcmd Cluster 对象的 extensions[client.authentication.k8s.io/exec] 字段：
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
	// 某些环境下插件在各集群间的使用方式基本相同，只有 audience 之类的集群专属参数不同；
	// 该字段允许把这类按集群配置直接写在集群信息中。
	// 不建议用该字段存放机密数据：exec 插件的主要优势之一就是无需在 kubeconfig 中直接保存密钥。
	// +optional
	Config runtime.Object
}
