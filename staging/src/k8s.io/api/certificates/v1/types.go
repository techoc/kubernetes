/*
Copyright 2020 The Kubernetes Authors.

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

// Package v1 提供 certificates.k8s.io/v1 API 组的类型定义。
package v1

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:method=UpdateApproval,verb=update,subresource=approval,input=k8s.io/api/certificates/v1.CertificateSigningRequest,result=k8s.io/api/certificates/v1.CertificateSigningRequest
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// CertificateSigningRequest 对象通过提交证书签名请求并异步审批、签发来获取 x509 证书。
//
// Kubelet 使用该 API 获取：
//  1. 用于向 kube-apiserver 认证身份的客户端证书（signerName 为 "kubernetes.io/kube-apiserver-client-kubelet"）。
//  2. 用于 TLS 端点、供 kube-apiserver 安全连接的服务端证书（signerName 为 "kubernetes.io/kubelet-serving"）。
//
// 该 API 也可用于申请向 kube-apiserver 认证的客户端证书（signerName 为
// "kubernetes.io/kube-apiserver-client"），或从 Kubernetes 之外的自定义签名者获取证书。
// +k8s:supportsSubresource="/status"
// +k8s:supportsSubresource="/approval"
type CertificateSigningRequest struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含证书请求，创建后不可变。创建时只能设置 request、signerName、
	// expirationSeconds 和 usages 字段，其余字段由 Kubernetes 派生，用户不可修改。
	Spec CertificateSigningRequestSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 包含请求是否被批准或拒绝的信息，以及签名者签发的证书或表示签发失败的状况。
	// +optional
	Status CertificateSigningRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// CertificateSigningRequestSpec 包含证书请求。
type CertificateSigningRequestSpec struct {
	// request 包含以 "CERTIFICATE REQUEST" PEM 块编码的 x509 证书签名请求。
	// 序列化为 JSON 或 YAML 时，数据会额外做 base64 编码。
	Request []byte `json:"request" protobuf:"bytes,1,opt,name=request"`

	// signerName 表示请求的签名者，是带限定名的名称。
	//
	// 对 CertificateSigningRequest 的 list/watch 请求可用 "spec.signerName=NAME" 字段选择器按此字段过滤。
	//
	// Kubernetes 内置的知名签名者有：
	//  1. "kubernetes.io/kube-apiserver-client"：签发可用于向 kube-apiserver 认证的客户端证书。
	//   此类请求绝不会被 kube-controller-manager 自动批准，可由 kube-controller-manager 的 "csrsigning" 控制器签发。
	//  2. "kubernetes.io/kube-apiserver-client-kubelet"：签发 kubelet 用于向 kube-apiserver 认证的客户端证书。
	//   此类请求可由 kube-controller-manager 的 "csrapproving" 控制器自动批准，由 "csrsigning" 控制器签发。
	//  3. "kubernetes.io/kubelet-serving"：签发 kubelet 用于提供 TLS 端点的服务端证书，kube-apiserver 可安全连接。
	//   此类请求绝不会被 kube-controller-manager 自动批准，可由 "csrsigning" 控制器签发。
	//
	// 更多细节见 https://k8s.io/docs/reference/access-authn-authz/certificate-signing-requests/#kubernetes-signers
	//
	// 也可指定自定义的 signerName。签名者定义：
	//  1. 信任分发方式（CA bundle 如何分发）。
	//  2. 允许的主体，以及请求不允许主体时的行为。
	//  3. 请求中必需、允许或禁止的 x509 扩展（包括是否允许 subjectAltNames、允许的类型与取值限制），
	//     以及请求不允许扩展时的行为。
	//  4. 必需、允许或禁止的密钥用法 / 扩展密钥用法。
	//  5. 过期时间/证书有效期：由签名者固定，还是可由管理员配置。
	//  6. 是否允许申请 CA 证书。
	SignerName string `json:"signerName" protobuf:"bytes,7,opt,name=signerName"`

	// expirationSeconds 是请求签发的证书的有效期（秒）。签名者可能签发有效期不同的证书，
	// 因此客户端必须比较所签发证书中 notBefore 与 notAfter 的差值来确定实际有效期。
	//
	// v1.22 及以后内置的知名 Kubernetes 签名者实现会遵守该字段，前提是请求的时长不超过
	// Kubernetes controller manager 的 --cluster-signing-duration 命令行参数设定的最大时长。
	//
	// 签名者可能因以下原因不遵守该字段：
	//
	//   1. 旧签名者不认识该字段（例如 v1.22 之前的内置实现）
	//   2. 签名者配置的最大有效期短于请求时长
	//   3. 签名者配置的最小有效期长于请求时长
	//
	// expirationSeconds 的最小合法值为 600，即 10 分钟。
	//
	// +optional
	ExpirationSeconds *int32 `json:"expirationSeconds,omitempty" protobuf:"varint,8,opt,name=expirationSeconds"`

	// usages 指定所签发证书中请求的一组密钥用法。
	//
	// TLS 客户端证书请求通常要求："digital signature"、"key encipherment"、"client auth"。
	//
	// TLS 服务端证书请求通常要求："key encipherment"、"digital signature"、"server auth"。
	//
	// 合法取值为：
	//  "signing", "digital signature", "content commitment",
	//  "key encipherment", "key agreement", "data encipherment",
	//  "cert sign", "crl sign", "encipher only", "decipher only", "any",
	//  "server auth", "client auth",
	//  "code signing", "email protection", "s/mime",
	//  "ipsec end system", "ipsec tunnel", "ipsec user",
	//  "timestamping", "ocsp signing", "microsoft sgc", "netscape sgc"
	// +listType=atomic
	Usages []KeyUsage `json:"usages,omitempty" protobuf:"bytes,5,opt,name=usages"`

	// username 是创建该 CertificateSigningRequest 的用户名。
	// 由 API Server 在创建时填充，不可修改。
	// +optional
	Username string `json:"username,omitempty" protobuf:"bytes,2,opt,name=username"`
	// uid 是创建该 CertificateSigningRequest 的用户的 UID。
	// 由 API Server 在创建时填充，不可修改。
	// +optional
	UID string `json:"uid,omitempty" protobuf:"bytes,3,opt,name=uid"`
	// groups 是创建该 CertificateSigningRequest 的用户所属的组。
	// 由 API Server 在创建时填充，不可修改。
	// +listType=atomic
	// +optional
	Groups []string `json:"groups,omitempty" protobuf:"bytes,4,rep,name=groups"`
	// extra 是创建该 CertificateSigningRequest 的用户的附加属性。
	// 由 API Server 在创建时填充，不可修改。
	// +optional
	Extra map[string]ExtraValue `json:"extra,omitempty" protobuf:"bytes,6,rep,name=extra"`
}

// kube-controller-manager 支持的内置 signerName 取值。
const (
	// "kubernetes.io/kube-apiserver-client" 签名者签发可用于向 kube-apiserver 认证的客户端证书。
	// 绝不会被 kube-controller-manager 自动批准。
	// 可由 kube-controller-manager 的 "csrsigning" 控制器签发。
	KubeAPIServerClientSignerName = "kubernetes.io/kube-apiserver-client"

	// "kubernetes.io/kube-apiserver-client-kubelet" 签发 kubelet 用于向 kube-apiserver 认证的客户端证书。
	// 可由 kube-controller-manager 的 "csrapproving" 控制器自动批准。
	// 可由 kube-controller-manager 的 "csrsigning" 控制器签发。
	KubeAPIServerClientKubeletSignerName = "kubernetes.io/kube-apiserver-client-kubelet"

	// "kubernetes.io/kubelet-serving" 签发 kubelet 用于提供 TLS 端点的服务端证书，
	// kube-apiserver 可安全连接该端点。
	// 绝不会被 kube-controller-manager 自动批准。
	// 可由 kube-controller-manager 的 "csrsigning" 控制器签发。
	KubeletServingSignerName = "kubernetes.io/kubelet-serving"
)

// ExtraValue 对值做一层包装，使 protobuf 能够生成相应的可空表示。
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type ExtraValue []string

// String 以字符串形式返回 ExtraValue 的内容。
func (t ExtraValue) String() string {
	return fmt.Sprintf("%v", []string(t))
}

// CertificateSigningRequestStatus 包含表示请求已批准/拒绝/失败的状况，以及签发的证书。
type CertificateSigningRequestStatus struct {
	// 应用在请求上的状况。已知状况有 "Approved"、"Denied" 和 "Failed"。
	// +listType=map
	// +listMapKey=type
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:listType=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=type
	// +k8s:beta(since: "1.37")=+k8s:customUnique
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:item(type: "Approved")=+k8s:zeroOrOneOfMember
	// +k8s:beta(since: "1.37")=+k8s:item(type: "Denied")=+k8s:zeroOrOneOfMember
	Conditions []CertificateSigningRequestCondition `json:"conditions,omitempty" protobuf:"bytes,1,rep,name=conditions"`

	// certificate 由签名者在出现 Approved 状况后填入签发的证书。
	// 该字段通过 /status 子资源设置，填充后不可修改。
	//
	// 若证书签名请求被拒绝，会添加类型为 "Denied" 的状况，该字段保持为空。
	// 若签名者无法签发证书，会添加类型为 "Failed" 的状况，该字段同样保持为空。
	//
	// 校验要求：
	//  1. certificate 必须包含一个或多个 PEM 块。
	//  2. 所有 PEM 块必须带有 "CERTIFICATE" 标签、不含头部，且编码数据是 RFC5280 第 4 节描述的
	//     BER 编码 ASN.1 Certificate 结构。
	//  3. "CERTIFICATE" PEM 块前后可以有非 PEM 内容，这些内容不做校验，
	//     以支持 RFC7468 第 5.2 节所述的说明性文本。
	//
	// 如果存在多个 PEM 块，且请求的 spec.signerName 定义未另行说明，则第一个块是签发的证书，
	// 后续块应视为中间证书并在 TLS 握手中提供。
	//
	// 证书采用 PEM 格式编码。
	//
	// 序列化为 JSON 或 YAML 时，数据会额外做 base64 编码，形式如下：
	//
	//     base64(
	//     -----BEGIN CERTIFICATE-----
	//     ...
	//     -----END CERTIFICATE-----
	//     )
	//
	// +optional
	Certificate []byte `json:"certificate,omitempty" protobuf:"bytes,2,opt,name=certificate"`
}

// RequestConditionType 表示 CertificateSigningRequestCondition 的类型。
type RequestConditionType string

// 证书请求的知名状况类型。
const (
	// Approved 表示请求已获批准，签名者应当签发证书。
	CertificateApproved RequestConditionType = "Approved"
	// Denied 表示请求被拒绝，签名者不应签发证书。
	CertificateDenied RequestConditionType = "Denied"
	// Failed 表示签名者签发证书失败。
	CertificateFailed RequestConditionType = "Failed"
)

// CertificateSigningRequestCondition 描述 CertificateSigningRequest 对象的一个状况。
type CertificateSigningRequestCondition struct {
	// 状况类型。已知状况有 "Approved"、"Denied" 和 "Failed"。
	//
	// "Approved" 状况通过 /approval 子资源添加，表示请求已获批准，签名者应当签发证书。
	//
	// "Denied" 状况通过 /approval 子资源添加，表示请求被拒绝，签名者不应签发证书。
	//
	// "Failed" 状况通过 /status 子资源添加，表示签名者签发证书失败。
	//
	// Approved 与 Denied 状况互斥。
	// Approved、Denied 和 Failed 状况一经添加便不可移除。
	//
	// 每种类型的状况最多允许一个。
	Type RequestConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=RequestConditionType"`
	// 状况的状态，取值为 True、False 或 Unknown。
	// Approved、Denied 和 Failed 状况不可为 "False" 或 "Unknown"。
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,6,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// reason 简要说明请求当前状态的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,2,opt,name=reason"`
	// message 是人可读的消息，说明请求状态的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// lastUpdateTime 是该状况最近一次更新的时间。
	// +optional
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,4,opt,name=lastUpdateTime"`
	// lastTransitionTime 是状况最近一次发生状态转换的时间。
	// 未设置时，新增状况类型或变更现有状况状态时，服务端会默认设为当前时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,5,opt,name=lastTransitionTime"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// CertificateSigningRequestList 是 CertificateSigningRequest 对象的集合。
type CertificateSigningRequestList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CertificateSigningRequest 对象的集合。
	Items []CertificateSigningRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// KeyUsage 指定密钥的合法使用场景。
// 参见：
//
//	https://tools.ietf.org/html/rfc5280#section-4.2.1.3
//	https://tools.ietf.org/html/rfc5280#section-4.2.1.12
//
// +enum
type KeyUsage string

// 合法的密钥用法。
const (
	// UsageSigning 表示用于签名。
	UsageSigning KeyUsage = "signing"
	// UsageDigitalSignature 表示用于数字签名。
	UsageDigitalSignature KeyUsage = "digital signature"
	// UsageContentCommitment 表示用于内容承诺。
	UsageContentCommitment KeyUsage = "content commitment"
	// UsageKeyEncipherment 表示用于密钥加密。
	UsageKeyEncipherment KeyUsage = "key encipherment"
	// UsageKeyAgreement 表示用于密钥协商。
	UsageKeyAgreement KeyUsage = "key agreement"
	// UsageDataEncipherment 表示用于数据加密。
	UsageDataEncipherment KeyUsage = "data encipherment"
	// UsageCertSign 表示用于证书签名。
	UsageCertSign KeyUsage = "cert sign"
	// UsageCRLSign 表示用于 CRL 签名。
	UsageCRLSign KeyUsage = "crl sign"
	// UsageEncipherOnly 表示仅用于加密。
	UsageEncipherOnly KeyUsage = "encipher only"
	// UsageDecipherOnly 表示仅用于解密。
	UsageDecipherOnly KeyUsage = "decipher only"
	// UsageAny 表示可用于任意场景。
	UsageAny KeyUsage = "any"
	// UsageServerAuth 表示用于服务端认证。
	UsageServerAuth KeyUsage = "server auth"
	// UsageClientAuth 表示用于客户端认证。
	UsageClientAuth KeyUsage = "client auth"
	// UsageCodeSigning 表示用于代码签名。
	UsageCodeSigning KeyUsage = "code signing"
	// UsageEmailProtection 表示用于电子邮件保护。
	UsageEmailProtection KeyUsage = "email protection"
	// UsageSMIME 表示用于 S/MIME。
	UsageSMIME KeyUsage = "s/mime"
	// UsageIPsecEndSystem 表示用于 IPsec 终端系统。
	UsageIPsecEndSystem KeyUsage = "ipsec end system"
	// UsageIPsecTunnel 表示用于 IPsec 隧道。
	UsageIPsecTunnel KeyUsage = "ipsec tunnel"
	// UsageIPsecUser 表示用于 IPsec 用户。
	UsageIPsecUser KeyUsage = "ipsec user"
	// UsageTimestamping 表示用于时间戳。
	UsageTimestamping KeyUsage = "timestamping"
	// UsageOCSPSigning 表示用于 OCSP 签名。
	UsageOCSPSigning KeyUsage = "ocsp signing"
	// UsageMicrosoftSGC 表示用于 Microsoft SGC。
	UsageMicrosoftSGC KeyUsage = "microsoft sgc"
	// UsageNetscapeSGC 表示用于 Netscape SGC。
	UsageNetscapeSGC KeyUsage = "netscape sgc"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// ClusterTrustBundle 是集群级容器，用于存放 X.509 信任锚（根证书）。
//
// 集群中任何已认证用户都被视为可以读取 ClusterTrustBundle 对象，因为 Pod 可以通过
// `clusterTrustBundle` 投影挂载它们。所有 ServiceAccount 默认具有读取权限。
// 仅有命名空间级权限的用户可以通过模拟其可用的 ServiceAccount 来读取 ClusterTrustBundle。
//
// 该对象可选择与某个签名者关联，此时它包含该签名者的一组有效信任锚。
// 一个签名者可以关联多个 ClusterTrustBundle，各自包含该签名者独立的一组信任锚。
// 准入控制用于保证只有对该签名者有权限的用户才能创建或修改相应的 bundle。
type ClusterTrustBundle struct {
	metav1.TypeMeta `json:""`

	// metadata 包含对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含签名者（如有）与信任锚。
	Spec ClusterTrustBundleSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// ClusterTrustBundleSpec 包含签名者与信任锚。
type ClusterTrustBundleSpec struct {
	// signerName 表示关联的签名者（如有）。
	//
	// 要创建或更新设置了 signerName 的 ClusterTrustBundle，必须具有如下集群级权限：
	// group=certificates.k8s.io resource=signers resourceName=<签名者名称>
	// verb=attest。
	//
	// signerName 非空时，ClusterTrustBundle 对象的名称必须以签名者名称为前缀（斜杠转为冒号）。
	// 例如签名者名称为 `example.com/foo` 时，合法的对象名包括 `example.com:foo:abc`
	// 和 `example.com:foo:v1`。
	//
	// signerName 为空时，对象名称不得带有这样的前缀。
	//
	// 对 ClusterTrustBundle 的 list/watch 请求可用 `spec.signerName=NAME` 字段选择器按此字段过滤。
	//
	// +optional
	// +k8s:alpha(since:"1.37")=+k8s:optional
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	SignerName string `json:"signerName,omitempty" protobuf:"bytes,1,opt,name=signerName"`

	// trustBundle 包含该 bundle 中各个 X.509 信任锚，
	// 以 PEM 包裹的 DER 格式 X.509 证书组成的 PEM bundle 形式提供。
	//
	// 数据必须仅由可解析为合法 X.509 证书的 PEM 证书块组成。每个证书都必须包含设置了 CA 位的
	// basic constraints 扩展。API Server 会拒绝包含重复证书或使用 PEM 块头部的对象。
	//
	// ClusterTrustBundle 的使用者（包括 Kubelet）可按自身逻辑对这些证书块重新排序和去重，
	// 也可丢弃 PEM 块头部和块间数据。
	TrustBundle string `json:"trustBundle" protobuf:"bytes,2,opt,name=trustBundle"`
}

// +k8s:prerelease-lifecycle-gen:introduced=1.37
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTrustBundleList 是 ClusterTrustBundle 对象的集合。
type ClusterTrustBundleList struct {
	metav1.TypeMeta `json:""`

	// metadata 包含列表元数据。
	//
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 ClusterTrustBundle 对象的集合。
	Items []ClusterTrustBundle `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:prerelease-lifecycle-gen:introduced=1.37
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodCertificateRequest 表示 Pod 请求指定签名者签发证书。
//
// Kubelet 使用该 API 实现 podCertificate 投影卷。
// +k8s:supportsSubresource="/status"
type PodCertificateRequest struct {
	metav1.TypeMeta `json:""`

	// metadata 包含对象元数据。
	//
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含所请求证书的详情。
	// +required
	Spec PodCertificateRequestSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 包含签发的证书和一组标准状况。
	// +optional
	Status PodCertificateRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PodCertificateRequestSpec 描述证书请求，所有字段创建后不可修改。
type PodCertificateRequestSpec struct {
	// signerName 表示请求的签名者。
	//
	// 所有以 `kubernetes.io` 开头的签名者名称都保留给 Kubernetes 项目使用。
	// 目前 Kubernetes 项目记录的知名签名者只有一个：
	// `kubernetes.io/kube-apiserver-client-pod`，它将签发 kube-apiserver 可识别的客户端证书，
	// 但当前尚未实现。
	//
	// +required
	SignerName string `json:"signerName" protobuf:"bytes,1,opt,name=signerName"`

	// podName 是证书将被挂载到的 Pod 的名称。
	//
	// +required
	PodName string `json:"podName" protobuf:"bytes,2,opt,name=podName"`
	// podUID 是证书将被挂载到的 Pod 的 UID。
	//
	// +required
	PodUID types.UID `json:"podUID" protobuf:"bytes,3,opt,name=podUID"`

	// serviceAccountName 是运行该 Pod 的 ServiceAccount 名称。
	//
	// +required
	ServiceAccountName string `json:"serviceAccountName" protobuf:"bytes,4,opt,name=serviceAccountName"`
	// serviceAccountUID 是运行该 Pod 的 ServiceAccount 的 UID。
	//
	// +required
	ServiceAccountUID types.UID `json:"serviceAccountUID" protobuf:"bytes,5,opt,name=serviceAccountUID"`

	// nodeName 是 Pod 被调度到的节点名称。
	//
	// +required
	NodeName types.NodeName `json:"nodeName" protobuf:"bytes,6,opt,name=nodeName"`
	// nodeUID 是 Pod 被调度到的节点的 UID。
	//
	// +required
	NodeUID types.UID `json:"nodeUID" protobuf:"bytes,7,opt,name=nodeUID"`

	// maxExpirationSeconds 是证书允许的最大有效期。
	//
	// 省略时 kube-apiserver 会将其设为 86400（24 小时）。kube-apiserver 会拒绝小于 3600（1 小时）的值，
	// 允许的最大值为 7862400（91 天）。
	//
	// 签名者实现可签发有效期短于 MaxExpirationSeconds 的任意证书，但不得短于 3600 秒（1 小时）。
	// 该约束由 kube-apiserver 强制。`kubernetes.io` 的签名者绝不会签发有效期超过 24 小时的证书。
	//
	// +optional
	// +default=86400
	MaxExpirationSeconds *int32 `json:"maxExpirationSeconds,omitempty" protobuf:"varint,8,opt,name=maxExpirationSeconds"`

	// StubPKCS10Request 是 Kubelet 使用主体私钥生成的 PKCS#10 证书签名请求（DER 序列化）。
	//
	// 多数签名者实现会忽略 CSR 的内容，只提取主体公钥。API Server 已在准入阶段自动校验 CSR 签名，
	// 因此签名者无需重复校验。Kubelet 生成的 CSR 内容完全为空。
	//
	// 主体公钥必须是 RSA3072、RSA4096、ECDSAP256、ECDSAP384、ECDSAP521 或 ED25519 之一。
	// 注意，该列表未来可能扩展。
	//
	// 签名者实现无需支持 kube-apiserver 和 kubelet 支持的所有密钥类型。若某个签名者不支持
	// 给定 PodCertificateRequest 使用的密钥类型，必须拒绝该请求：在 status.conditions 中设置
	// 类型为 "Denied"、原因为 "UnsupportedKeyType" 的条目，并可在 message 字段建议其支持的密钥类型。
	//
	// +required
	StubPKCS10Request []byte `json:"stubPKCS10Request" protobuf:"bytes,12,opt,name=stubPKCS10Request"`

	// unverifiedUserAnnotations 允许 Pod 作者向签名者实现传递附加信息。
	// Kubernetes 不会以任何方式限制或校验这些元数据。
	//
	// 条目遵循与对象元数据注解相同的校验规则，但所有键必须带域名前缀。除整个字段的总大小限制外，
	// 对值没有限制。
	//
	// 签名者应记录自己支持的键和值，并拒绝含有无法识别键的请求。
	//
	// +optional
	UnverifiedUserAnnotations map[string]string `json:"unverifiedUserAnnotations,omitempty" protobuf:"bytes,11,opt,name=unverifiedUserAnnotations"`
}

// PodCertificateRequestStatus 描述请求状态，并在请求获签发时保存证书数据。
type PodCertificateRequestStatus struct {
	// 应用在请求上的状况。
	//
	// "Issued"、"Denied" 和 "Failed" 类型有特殊处理：这些状况最多出现一个，
	// 且状态必须为 "True"。
	//
	// 若请求以 `Reason=UnsupportedKeyType` 被拒绝，签名者可在 message 字段建议使用可用的密钥类型。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// certificateChain 由签名者填入签发的证书。
	// 该字段通过 /status 子资源设置，填充后不可修改。
	//
	// 若证书签名请求被拒绝，会添加类型为 "Denied" 的状况，该字段保持为空。
	// 若签名者无法签发证书，会添加类型为 "Failed" 的状况，该字段同样保持为空。
	//
	// 校验要求：
	//  1. certificateChain 必须由一个或多个 PEM 格式的证书组成。
	//  2. 每一项必须是 RFC5280 第 4 节描述的、合法 PEM 包裹的 DER 编码 ASN.1 证书。
	//
	// 如果存在多个块，且请求的 spec.signerName 定义未另行说明，则第一个块是签发的证书，
	// 后续块应视为中间证书并在 TLS 握手中提供。将证书链投影到 Pod 卷时，
	// kubelet 会丢弃 PEM 块之间的数据以及任何 PEM 块头部。
	//
	// +optional
	CertificateChain string `json:"certificateChain,omitempty" protobuf:"bytes,2,opt,name=certificateChain"`

	// notBefore 是证书生效的时间，其值必须与 certificateChain 中叶子证书的 notBefore 一致。
	// 该字段通过 /status 子资源设置，填充后不可修改，且签名者必须将其与 certificateChain 同时设置。
	//
	// +optional
	NotBefore *metav1.Time `json:"notBefore,omitempty" protobuf:"bytes,4,opt,name=notBefore"`

	// beginRefreshAt 是 kubelet 应开始尝试刷新证书的时间。该字段通过 /status 子资源设置，
	// 必须与 certificateChain 同时设置，填充后不可修改。
	//
	// 该字段仅作提示，必要时 kubelet 可早于或晚于该时间开始刷新。
	//
	// +optional
	BeginRefreshAt *metav1.Time `json:"beginRefreshAt,omitempty" protobuf:"bytes,5,opt,name=beginRefreshAt"`

	// notAfter 是证书过期的时间，其值必须与 certificateChain 中叶子证书的 notAfter 一致。
	// 该字段通过 /status 子资源设置，填充后不可修改，且签名者必须将其与 certificateChain 同时设置。
	//
	// +optional
	NotAfter *metav1.Time `json:"notAfter,omitempty" protobuf:"bytes,6,opt,name=notAfter"`
}

// PodCertificateRequest 的知名状况类型。
const (
	// Denied 表示请求被签名者拒绝。
	PodCertificateRequestConditionTypeDenied string = "Denied"
	// Failed 表示签名者签发证书失败。
	PodCertificateRequestConditionTypeFailed string = "Failed"
	// Issued 表示证书已签发。
	PodCertificateRequestConditionTypeIssued string = "Issued"
)

// PodCertificateRequest 的知名状况原因。
const (
	// UnsupportedKeyType 应在签名者不支持 publicKey 的密钥类型时，
	// 设置到 "Denied" 状况上。
	PodCertificateRequestConditionUnsupportedKeyType string = "UnsupportedKeyType"

	// InvalidUnverifiedUserAnnotations 应在签名者不识别 userConfig 中的某个键，
	// 或认为请求的 userConfig 无效时，设置到 "Denied" 状况上。
	PodCertificateRequestConditionInvalidUserConfig string = "InvalidUnverifiedUserAnnotations"
)

// +k8s:prerelease-lifecycle-gen:introduced=1.37
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodCertificateRequestList 是 PodCertificateRequest 对象的集合。
type PodCertificateRequestList struct {
	metav1.TypeMeta `json:""`

	// metadata 包含列表元数据。
	//
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 PodCertificateRequest 对象的集合。
	Items []PodCertificateRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}
