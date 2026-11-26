/*
Copyright 2021 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExecCredential 用于 exec 插件向 HTTP transport 传递凭据。
//
// ExecCredential 是 exec 插件与 transport 之间的私密数据交换格式：transport 写入
// Spec（可选地附带集群信息），插件在 Status 中回填凭据。它只存在于内存/管道中，
// 不会持久化到 API server。
type ExecCredential struct {
	metav1.TypeMeta `json:""`

	// Spec 存放 transport 传给插件的信息，插件据此决定使用哪个集群上下文去获取凭据。
	Spec ExecCredentialSpec `json:"spec,omitempty"`

	// Status 由插件填写，存放 transport 联系 API 时应使用的凭据；
	// transport 只读取这里返回的凭据，不信任其他来源。
	// +optional
	Status *ExecCredentialStatus `json:"status,omitempty"`
}

// ExecCredentialSpec 保存 transport 提供给插件的请求与运行时信息。
//
// 描述本次 exec 调用的上下文：是否交互式、以及（按需）目标集群的连接信息。
type ExecCredentialSpec struct {
	// Cluster 提供 exec 插件与所认证集群通信所需的信息。
	// 仅当 exec provider 配置中 provideClusterInfo 为 true 时（即 ExecConfig.ProvideClusterInfo）才非 nil；
	// 插件不能假设它总是存在。
	// +optional
	Cluster *Cluster `json:"cluster,omitempty"`

	// Interactive 声明是否已把 stdin 传给此 exec 插件。
	// 插件可据此决定是否弹出浏览器/设备登录等交互流程；为 false 时必须非交互完成。
	Interactive bool `json:"interactive"`
}

// ExecCredentialStatus 保存供 transport 使用的凭据。
//
// Token 与 ClientKeyData 是敏感字段，这些数据只应在客户端与 exec 插件进程之间
// 通过内存传递；插件二进制本身至少应通过文件权限加以保护。
//
// token 与私钥属于敏感信息，仅允许在父子进程间通过管道传递；插件二进制本身也应
// 至少用文件权限保护，防止其被执行或替换。
type ExecCredentialStatus struct {
	// ExpirationTimestamp 标明所提供凭据的过期时间；
	// 为空表示不过期；transport 会在该时刻前触发一次重新执行插件。
	// +optional
	ExpirationTimestamp *metav1.Time `json:"expirationTimestamp,omitempty"`
	// Token 是客户端用于请求认证的 bearer token。
	Token string `json:"token,omitempty" datapolicy:"token"`
	// ClientCertificateData 是 PEM 编码的客户端 TLS 证书（如有中间证书也一并包含）。
	// 与 ClientKeyData 必须成对出现，否则 transport 无法建立 mTLS。
	ClientCertificateData string `json:"clientCertificateData,omitempty"`
	// 上述证书的 PEM 编码私钥。
	ClientKeyData string `json:"clientKeyData,omitempty" datapolicy:"security-key"`
}

// Cluster 包含让 exec 插件与所认证 Kubernetes 集群通信所需的信息。
//
// 为确保该结构体涵盖与集群通信所需的全部信息（就像通过 kubeconfig 一样），字段
// 应与 "k8s.io/client-go/tools/clientcmd/api/v1".Cluster 保持一致，唯独
// CertificateAuthority 例外——CA 数据总是以字节形式传给插件。
//
// 字段刻意与 kubeconfig 的 Cluster 保持一致，使插件拿到的信息足以自行构造客户端；
// 唯一例外是 CA 始终以字节形式提供，而不是文件路径。
type Cluster struct {
	// Server is the address of the kubernetes cluster (https://hostname:port).
	Server string `json:"server"`
	// TLSServerName 传给服务端用于 SNI，客户端也用它校验证书。为空时使用联系服务端
	// 的主机名。
	// +optional
	TLSServerName string `json:"tls-server-name,omitempty"`
	// InsecureSkipTLSVerify 跳过对服务端证书的合法性校验，会使 HTTPS 连接不再安全。
	// +optional
	InsecureSkipTLSVerify bool `json:"insecure-skip-tls-verify,omitempty"`
	// CAData 存放 PEM 编码的证书颁发机构证书。
	// 为空时回退到系统根证书，而不是拒绝连接。
	// +listType=atomic
	// +optional
	CertificateAuthorityData []byte `json:"certificate-authority-data,omitempty"`
	// ProxyURL 是发往该集群的所有请求使用的代理 URL。
	// +optional
	ProxyURL string `json:"proxy-url,omitempty"`
	// DisableCompression 允许客户端关闭对该服务端所有请求的响应压缩。当客户端与
	// 服务端之间带宽充足时，省去压缩时间可加速请求（尤其是 List）。
	// +optional
	DisableCompression bool `json:"disable-compression,omitempty"`
	// Config 保存 exec 插件针对所认证集群的附加配置数据，来源于 clientcmd Cluster
	// 对象的 extensions[client.authentication.k8s.io/exec] 字段：
	// extensions[client.authentication.k8s.io/exec] field:
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
	// 某些环境中，多个集群的用户配置完全相同，只有 audience 等少数字段不同。该字段
	// 允许直接把每个集群的配置随集群信息一起给出。不建议用它存密钥——exec 插件的主要
	// 优势正是无需在 kubeconfig 中直接存放机密。
	//
	// 来源是 kubeconfig 中保留扩展名 client.authentication.k8s.io/exec，用于存放
	// 按集群区分的非敏感上下文（如 audience）。exec 插件的主要收益就是不落盘密钥，
	// 因此不要把秘密放进这里。
	// +optional
	Config runtime.RawExtension `json:"config,omitempty"`
}
