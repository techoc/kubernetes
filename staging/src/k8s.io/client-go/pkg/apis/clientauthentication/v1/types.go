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

// ExecCredential is used by exec-based plugins to communicate credentials to
// HTTP transports.
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

// ExecCredentialSpec holds request and runtime specific information provided by
// the transport.
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

// ExecCredentialStatus holds credentials for the transport to use.
//
// Token and ClientKeyData are sensitive fields. This data should only be
// transmitted in-memory between client and exec plugin process. Exec plugin
// itself should at least be protected via file permissions.
//
// token 与私钥属于敏感信息，仅允许在父子进程间通过管道传递；插件二进制本身也应
// 至少用文件权限保护，防止其被执行或替换。
type ExecCredentialStatus struct {
	// ExpirationTimestamp 标明所提供凭据的过期时间；
	// 为空表示不过期；transport 会在该时刻前触发一次重新执行插件。
	// +optional
	ExpirationTimestamp *metav1.Time `json:"expirationTimestamp,omitempty"`
	// Token is a bearer token used by the client for request authentication.
	Token string `json:"token,omitempty" datapolicy:"token"`
	// ClientCertificateData 是 PEM 编码的客户端 TLS 证书（如有中间证书也一并包含）。
	// 与 ClientKeyData 必须成对出现，否则 transport 无法建立 mTLS。
	ClientCertificateData string `json:"clientCertificateData,omitempty"`
	// PEM-encoded private key for the above certificate.
	ClientKeyData string `json:"clientKeyData,omitempty" datapolicy:"security-key"`
}

// Cluster contains information to allow an exec plugin to communicate
// with the kubernetes cluster being authenticated to.
//
// To ensure that this struct contains everything someone would need to communicate
// with a kubernetes cluster (just like they would via a kubeconfig), the fields
// should shadow "k8s.io/client-go/tools/clientcmd/api/v1".Cluster, with the exception
// of CertificateAuthority, since CA data will always be passed to the plugin as bytes.
//
// 字段刻意与 kubeconfig 的 Cluster 保持一致，使插件拿到的信息足以自行构造客户端；
// 唯一例外是 CA 始终以字节形式提供，而不是文件路径。
type Cluster struct {
	// Server is the address of the kubernetes cluster (https://hostname:port).
	Server string `json:"server"`
	// TLSServerName is passed to the server for SNI and is used in the client to
	// check server certificates against. If ServerName is empty, the hostname
	// used to contact the server is used.
	// +optional
	TLSServerName string `json:"tls-server-name,omitempty"`
	// InsecureSkipTLSVerify skips the validity check for the server's certificate.
	// This will make your HTTPS connections insecure.
	// +optional
	InsecureSkipTLSVerify bool `json:"insecure-skip-tls-verify,omitempty"`
	// CAData 存放 PEM 编码的证书颁发机构证书。
	// 为空时回退到系统根证书，而不是拒绝连接。
	// +listType=atomic
	// +optional
	CertificateAuthorityData []byte `json:"certificate-authority-data,omitempty"`
	// ProxyURL is the URL to the proxy to be used for all requests to this
	// cluster.
	// +optional
	ProxyURL string `json:"proxy-url,omitempty"`
	// DisableCompression allows client to opt-out of response compression for all requests to the server. This is useful
	// to speed up requests (specifically lists) when client-server network bandwidth is ample, by saving time on
	// compression (server-side) and decompression (client-side): https://github.com/kubernetes/kubernetes/issues/112296.
	// +optional
	DisableCompression bool `json:"disable-compression,omitempty"`
	// Config holds additional config data that is specific to the exec
	// plugin with regards to the cluster being authenticated to.
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
	//
	// 来源是 kubeconfig 中保留扩展名 client.authentication.k8s.io/exec，用于存放
	// 按集群区分的非敏感上下文（如 audience）。exec 插件的主要收益就是不落盘密钥，
	// 因此不要把秘密放进这里。
	// +optional
	Config runtime.RawExtension `json:"config,omitempty"`
}
