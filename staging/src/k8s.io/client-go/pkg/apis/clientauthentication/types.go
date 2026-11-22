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

// ExecCredential is used by exec-based plugins to communicate credentials to
// HTTP transports.
// ExecCredential 是 exec 插件与 HTTP transport 之间交换凭据的内部（非版本化）类型；
// 各版本 API 会转换到该类型后再被客户端消费。
type ExecCredential struct {
	metav1.TypeMeta

	// Spec holds information passed to the plugin by the transport. This contains
	// request and runtime specific information, such as if the session is interactive.
	// Spec 携带 transport 传给插件的请求级/运行时信息。
	Spec ExecCredentialSpec

	// Status is filled in by the plugin and holds the credentials that the transport
	// should use to contact the API.
	// Status 由插件填写，包含 transport 访问 API 时应使用的凭据。
	// +optional
	Status *ExecCredentialStatus
}

// ExecCredentialSpec holds request and runtime specific information provided by
// the transport.
// ExecCredentialSpec 保存 transport 提供的请求级和运行时信息。
type ExecCredentialSpec struct {
	// Interactive is true when the transport detects the command is being called from an
	// interactive prompt, i.e., when stdin has been passed to this exec plugin.
	// Interactive 为 true 表示该插件运行在交互式终端中（stdin 已接入插件）。
	// +optional
	Interactive bool

	// Cluster contains information to allow an exec plugin to communicate with the
	// kubernetes cluster being authenticated to. Note that Cluster is non-nil only
	// when provideClusterInfo is set to true in the exec provider config (i.e.,
	// ExecConfig.ProvideClusterInfo).
	// Cluster 仅在 ExecConfig.ProvideClusterInfo 为 true 时非空。
	// +optional
	Cluster *Cluster
}

// ExecCredentialStatus holds credentials for the transport to use.
// ExecCredentialStatus 保存 transport 使用的凭据。
type ExecCredentialStatus struct {
	// ExpirationTimestamp indicates a time when the provided credentials expire.
	// 凭据过期时间；为空表示不过期或无法预知。
	// +optional
	ExpirationTimestamp *metav1.Time
	// Token is a bearer token used by the client for request authentication.
	// Token 是请求认证使用的 bearer token。
	// +optional
	Token string `datapolicy:"token"`
	// PEM-encoded client TLS certificate.
	// PEM 编码的客户端 TLS 证书。
	// +optional
	ClientCertificateData string
	// PEM-encoded client TLS private key.
	// PEM 编码的客户端 TLS 私钥。
	// +optional
	ClientKeyData string `datapolicy:"secret-key"`
}

// Cluster contains information to allow an exec plugin to communicate
// with the kubernetes cluster being authenticated to.
//
// To ensure that this struct contains everything someone would need to communicate
// with a kubernetes cluster (just like they would via a kubeconfig), the fields
// should shadow "k8s.io/client-go/tools/clientcmd/api/v1".Cluster, with the exception
// of CertificateAuthority, since CA data will always be passed to the plugin as bytes.
// Cluster 的字段与 kubeconfig 中的 Cluster 保持一致（CA 证书以字节形式直接提供），
// 以便插件仅凭该结构即可连接目标集群。
type Cluster struct {
	// Server is the address of the kubernetes cluster (https://hostname:port).
	// 集群地址，形如 https://hostname:port。
	Server string
	// TLSServerName is passed to the server for SNI and is used in the client to
	// check server certificates against. If ServerName is empty, the hostname
	// used to contact the server is used.
	// TLSServerName 同时用于 SNI 与证书校验；为空时退化为连接时使用的主机名。
	// +optional
	TLSServerName string
	// InsecureSkipTLSVerify skips the validity check for the server's certificate.
	// This will make your HTTPS connections insecure.
	// 跳过服务端证书校验，会降低安全性，仅应在受控测试环境使用。
	// +optional
	InsecureSkipTLSVerify bool
	// CAData contains PEM-encoded certificate authority certificates.
	// If empty, system roots should be used.
	// CAData 为 PEM 编码的 CA 证书；为空则使用系统根证书。
	// +listType=atomic
	// +optional
	CertificateAuthorityData []byte
	// ProxyURL is the URL to the proxy to be used for all requests to this
	// cluster.
	// 访问该集群时使用的代理 URL。
	// +optional
	ProxyURL string
	// DisableCompression allows client to opt-out of response compression for all requests to the server. This is useful
	// to speed up requests (specifically lists) when client-server network bandwidth is ample, by saving time on
	// compression (server-side) and decompression (client-side): https://github.com/kubernetes/kubernetes/issues/112296.
	// 在带宽充足时可关闭压缩，用带宽换 CPU（避免服务端压缩、客户端解压的开销）。
	// +optional
	DisableCompression bool
	// Config holds additional config data that is specific to the exec
	// plugin with regards to the cluster being authenticated to.
	// Config 保存针对该集群、供 exec 插件使用的额外配置。
	//
	// This data is sourced from the clientcmd Cluster object's
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
	// In some environments, the user config may be exactly the same across many clusters
	// (i.e. call this exec plugin) minus some details that are specific to each cluster
	// such as the audience.  This field allows the per cluster config to be directly
	// specified with the cluster info.  Using this field to store secret data is not
	// recommended as one of the prime benefits of exec plugins is that no secrets need
	// to be stored directly in the kubeconfig.
	// 该数据来自 kubeconfig 的 extensions[client.authentication.k8s.io/exec]。
	// 官方不建议在此存放密钥：exec 插件的一大优势正是 kubeconfig 中无需保存机密。
	// +optional
	Config runtime.Object
}
