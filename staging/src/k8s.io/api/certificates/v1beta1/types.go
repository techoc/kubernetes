/*
Copyright 2016 The Kubernetes Authors.

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

package v1beta1

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.12
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=certificates.k8s.io,v1,CertificateSigningRequest

// 描述一个证书签名请求。
// +k8s:supportsSubresource="/status"
// +k8s:supportsSubresource="/approval"
type CertificateSigningRequest struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含证书请求，创建后不可变。
	// 创建时只能设置 request、signerName、expirationSeconds 和 usages 字段。
	// 其他字段由 Kubernetes 推导，用户不能修改。
	Spec CertificateSigningRequestSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// 关于请求的派生信息。
	// +optional
	Status CertificateSigningRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// CertificateSigningRequestSpec 包含证书请求。
type CertificateSigningRequestSpec struct {
	// Base64 编码的 PKCS#10 CSR 数据。
	Request []byte `json:"request" protobuf:"bytes,1,opt,name=request"`

	// 请求指定的签名者，格式为限定名 `scope-hostname.io/name`。
	// 若为空，会被默认赋值：
	//  1. 若是 kubelet 客户端证书，赋值为 "kubernetes.io/kube-apiserver-client-kubelet"。
	//  2. 若是 kubelet 服务端证书，赋值为 "kubernetes.io/kubelet-serving"。
	//  3. 其他情况赋值为 "kubernetes.io/legacy-unknown"。
	// 签名者的信任分发在带外完成。
	// 可以通过 `spec.signerName` 对此字段进行选择。
	// +optional
	SignerName *string `json:"signerName,omitempty" protobuf:"bytes,7,opt,name=signerName"`

	// expirationSeconds 是请求签发的证书的有效期。
	// 签名者可能签发不同有效期的证书，因此客户端必须检查所签发证书中 notBefore 和
	// notAfter 字段之差，以确定实际有效期。
	//
	// v1.22+ tree 内知名 Kubernetes 签名者的实现会遵守此字段，
	// 前提是请求的时长不超过其根据 Kubernetes controller manager 的
	// --cluster-signing-duration CLI 标志所允许的最大时长。
	//
	// 签名者可能因以下原因不遵守此字段：
	//
	//   1. 不知道该字段的旧签名者（如 v1.22 之前的 tree 内实现）
	//   2. 配置的最大值短于请求时长的签名者
	//   3. 配置的最小值长于请求时长的签名者
	//
	// expirationSeconds 的最小有效值为 600，即 10 分钟。
	//
	// +optional
	ExpirationSeconds *int32 `json:"expirationSeconds,omitempty" protobuf:"varint,8,opt,name=expirationSeconds"`

	// allowedUsages 指定密钥有效的一组使用场景。
	// 参见：
	//	https://tools.ietf.org/html/rfc5280#section-4.2.1.3
	//	https://tools.ietf.org/html/rfc5280#section-4.2.1.12
	//
	// 有效值为：
	//  "signing",
	//  "digital signature",
	//  "content commitment",
	//  "key encipherment",
	//  "key agreement",
	//  "data encipherment",
	//  "cert sign",
	//  "crl sign",
	//  "encipher only",
	//  "decipher only",
	//  "any",
	//  "server auth",
	//  "client auth",
	//  "code signing",
	//  "email protection",
	//  "s/mime",
	//  "ipsec end system",
	//  "ipsec tunnel",
	//  "ipsec user",
	//  "timestamping",
	//  "ocsp signing",
	//  "microsoft sgc",
	//  "netscape sgc"
	// +listType=atomic
	Usages []KeyUsage `json:"usages,omitempty" protobuf:"bytes,5,opt,name=usages"`

	// 关于请求用户的信息。详见 user.Info 接口。
	// +optional
	Username string `json:"username,omitempty" protobuf:"bytes,2,opt,name=username"`
	// 关于请求用户的 UID 信息。详见 user.Info 接口。
	// +optional
	UID string `json:"uid,omitempty" protobuf:"bytes,3,opt,name=uid"`
	// 关于请求用户的组信息。详见 user.Info 接口。
	// +listType=atomic
	// +optional
	Groups []string `json:"groups,omitempty" protobuf:"bytes,4,rep,name=groups"`
	// 关于请求用户的额外信息。详见 user.Info 接口。
	// +optional
	Extra map[string]ExtraValue `json:"extra,omitempty" protobuf:"bytes,6,rep,name=extra"`
}

// kube-controller-manager 认可的内置 signerName 值。
// 这些用途均与 ServiceAccount token secret 的 `.data[ca.crt]` 无关。
const (
	// 签发会被 kube-apiserver 认可为客户端证书的证书。
	// 从不会被 kube-controller-manager 自动批准。
	KubeAPIServerClientSignerName = "kubernetes.io/kube-apiserver-client"

	// 为 kubelet 签发会被 kube-apiserver 认可为客户端证书的客户端证书。
	// 可能被 kube-controller-manager 自动批准。
	KubeAPIServerClientKubeletSignerName = "kubernetes.io/kube-apiserver-client-kubelet"

	// 签发会被 kube-apiserver 认可为有效 kubelet 服务端证书的服务端证书，但无其他保证。
	KubeletServingSignerName = "kubernetes.io/kubelet-serving"

	// 完全不保证信任。某些发行版可能将其作为客户端证书认可，
	// 但该行为不是标准 Kubernetes 行为。
	LegacyUnknownSignerName = "kubernetes.io/legacy-unknown"
)

// ExtraValue 对值做一层包装，使 protobuf 能够生成代码。
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type ExtraValue []string

// String 返回 ExtraValue 的字符串表示。
func (t ExtraValue) String() string {
	return fmt.Sprintf("%v", []string(t))
}

// CertificateSigningRequestStatus 保存证书签名请求的状态。
type CertificateSigningRequestStatus struct {
	// 应用于请求的条件，例如批准或拒绝。
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

	// 若请求被批准，控制器会将签发的证书放在这里。
	// +optional
	Certificate []byte `json:"certificate,omitempty" protobuf:"bytes,2,opt,name=certificate"`
}

// RequestConditionType 是证书请求条件的类型。
type RequestConditionType string

// 证书请求可能具有的条件。
const (
	// CertificateApproved 表示请求已被批准。
	CertificateApproved RequestConditionType = "Approved"
	// CertificateDenied 表示请求已被拒绝。
	CertificateDenied RequestConditionType = "Denied"
	// CertificateFailed 表示请求签发失败。
	CertificateFailed RequestConditionType = "Failed"
)

// CertificateSigningRequestCondition 描述证书签名请求的一个条件。
type CertificateSigningRequestCondition struct {
	// 条件的类型。已知条件包括 "Approved"、"Denied" 和 "Failed"。
	Type RequestConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=RequestConditionType"`
	// 条件的状态，取值为 True、False 或 Unknown 之一。
	// Approved、Denied 和 Failed 条件不能为 "False" 或 "Unknown"。
	// 默认为 "True"。若未设置，应视为 "True"。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,6,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 请求状态的简要原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,2,opt,name=reason"`
	// 包含请求状态细节的人类可读消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// 本条件最后一次更新的时间戳。
	// +optional
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,4,opt,name=lastUpdateTime"`
	// lastTransitionTime 是条件上次从一种状态转变为另一种状态的时间。
	// 若未设置，当新增条件类型或改变现有条件状态时，服务端会将其默认设为当前时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,5,opt,name=lastTransitionTime"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.12
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=certificates.k8s.io,v1,CertificateSigningRequestList

// CertificateSigningRequestList 是 CertificateSigningRequest 对象的列表。
type CertificateSigningRequestList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是证书签名请求的列表。
	Items []CertificateSigningRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// KeyUsage 指定密钥的有效使用场景。
// 参见：
//
//	https://tools.ietf.org/html/rfc5280#section-4.2.1.3
//	https://tools.ietf.org/html/rfc5280#section-4.2.1.12
type KeyUsage string

// 密钥使用场景的取值。
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
	// UsageAny 表示任意用途。
	UsageAny KeyUsage = "any"
	// UsageServerAuth 表示用于服务端认证。
	UsageServerAuth KeyUsage = "server auth"
	// UsageClientAuth 表示用于客户端认证。
	UsageClientAuth KeyUsage = "client auth"
	// UsageCodeSigning 表示用于代码签名。
	UsageCodeSigning KeyUsage = "code signing"
	// UsageEmailProtection 表示用于邮件保护。
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
// +k8s:prerelease-lifecycle-gen:introduced=1.33
// +k8s:prerelease-lifecycle-gen:deprecated=1.37
// +k8s:prerelease-lifecycle-gen:replacement=certificates.k8s.io,v1,ClusterTrustBundle
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTrustBundle 是集群级容器，用于存放 X.509 信任锚（根证书）。
//
// ClusterTrustBundle 对象被认为可由集群中任何已认证用户读取，因为它们可以通过
// `clusterTrustBundle` 投影挂载到 pod 中。所有 service account 默认对
// ClusterTrustBundle 具有读权限。仅有命名空间级权限的用户可通过模拟其有权访问的
// service account 来读取 ClusterTrustBundle。
//
// 它可以选择性地与某个特定签名者关联，此时包含该签名者的一组有效信任锚。
// 一个签名者可以有多个关联的 ClusterTrustBundle，每个都是该签名者独立的一组信任锚。
// 准入控制用于确保只有对该签名者有权限的用户才能创建或修改相应的 bundle。
type ClusterTrustBundle struct {
	metav1.TypeMeta `json:""`

	// metadata 包含对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含签名者（如有）和信任锚。
	Spec ClusterTrustBundleSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// ClusterTrustBundleSpec 包含签名者和信任锚。
type ClusterTrustBundleSpec struct {
	// signerName 表示关联的签名者（如有）。
	//
	// 要创建或更新设置了 signerName 的 ClusterTrustBundle，必须具有如下集群级权限：
	// group=certificates.k8s.io resource=signers resourceName=<签名者名称>
	// verb=attest。
	//
	// 若 signerName 非空，ClusterTrustBundle 对象的名称必须以签名者名称为前缀
	// （将斜杠替换为冒号）。例如，签名者名称 `example.com/foo` 对应的合法
	// ClusterTrustBundle 对象名包括 `example.com:foo:abc` 和 `example.com:foo:v1`。
	//
	// 若 signerName 为空，ClusterTrustBundle 对象的名称不得带有此类前缀。
	//
	// 对 ClusterTrustBundle 的 list/watch 请求可以通过 `spec.signerName=NAME`
	// 字段选择器对此字段进行过滤。
	//
	// +optional
	// +k8s:alpha(since:"1.37")=+k8s:optional
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	SignerName string `json:"signerName,omitempty" protobuf:"bytes,1,opt,name=signerName"`

	// trustBundle 包含此 bundle 的各个 X.509 信任锚，
	// 以 PEM 封装的 DER 格式 X.509 证书组成的 PEM bundle 形式保存。
	//
	// 数据必须仅由能解析为有效 X.509 证书的 PEM 证书块组成。
	// 每个证书必须包含设置了 CA 位的 basic constraints 扩展。
	// API server 会拒绝包含重复证书或使用 PEM 块头的对象。
	//
	// ClusterTrustBundle 的使用者（包括 Kubelet）可以按照自己的逻辑对此文件中的
	// 证书块重新排序和去重，也可以丢弃 PEM 块头和块间数据。
	TrustBundle string `json:"trustBundle" protobuf:"bytes,2,opt,name=trustBundle"`
}

// +k8s:prerelease-lifecycle-gen:introduced=1.33
// +k8s:prerelease-lifecycle-gen:deprecated=1.37
// +k8s:prerelease-lifecycle-gen:replacement=certificates.k8s.io,v1,ClusterTrustBundleList
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
// +k8s:prerelease-lifecycle-gen:introduced=1.35
// +k8s:prerelease-lifecycle-gen:deprecated=1.37
// +k8s:prerelease-lifecycle-gen:replacement=certificates.k8s.io,v1,PodCertificateRequest
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodCertificateRequest 表示一个 pod 向给定签名者请求证书。
//
// Kubelet 使用此 API 实现 podCertificate 投影卷。
// +k8s:supportsSubresource="/status"
type PodCertificateRequest struct {
	metav1.TypeMeta `json:""`

	// metadata 包含对象元数据。
	//
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含所请求证书的详细信息。
	// +required
	Spec PodCertificateRequestSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 包含签发的证书和一组标准条件。
	// +optional
	Status PodCertificateRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PodCertificateRequestSpec 描述证书请求。所有字段创建后不可变。
type PodCertificateRequestSpec struct {
	// signerName 表示请求的签名者。
	//
	// 所有以 `kubernetes.io` 开头的签名者名称都保留给 Kubernetes 项目使用。
	// 目前 Kubernetes 项目记录了一个知名签名者
	// `kubernetes.io/kube-apiserver-client-pod`，它将签发 kube-apiserver
	// 可识别的客户端证书。该签名者目前尚未实现。
	//
	// +required
	SignerName string `json:"signerName" protobuf:"bytes,1,opt,name=signerName"`

	// podName 是证书将被挂载到的 pod 的名称。
	//
	// +required
	PodName string `json:"podName" protobuf:"bytes,2,opt,name=podName"`
	// podUID 是证书将被挂载到的 pod 的 UID。
	//
	// +required
	PodUID types.UID `json:"podUID" protobuf:"bytes,3,opt,name=podUID"`

	// serviceAccountName 是 pod 运行时使用的 service account 名称。
	//
	// +required
	ServiceAccountName string `json:"serviceAccountName" protobuf:"bytes,4,opt,name=serviceAccountName"`
	// serviceAccountUID 是 pod 运行时使用的 service account 的 UID。
	//
	// +required
	ServiceAccountUID types.UID `json:"serviceAccountUID" protobuf:"bytes,5,opt,name=serviceAccountUID"`

	// nodeName 是 pod 被调度到的节点名称。
	//
	// +required
	NodeName types.NodeName `json:"nodeName" protobuf:"bytes,6,opt,name=nodeName"`
	// nodeUID 是 pod 被调度到的节点的 UID。
	//
	// +required
	NodeUID types.UID `json:"nodeUID" protobuf:"bytes,7,opt,name=nodeUID"`

	// maxExpirationSeconds 是证书允许的最大有效期。
	//
	// 若省略，kube-apiserver 会将其设为 86400（24 小时）。kube-apiserver
	// 会拒绝短于 3600（1 小时）的值。允许的最大值为 7862400（91 天）。
	//
	// 签名者实现可以自由签发有效期*短于* MaxExpirationSeconds 的证书，
	// 但不得短于 3600 秒（1 小时）。此约束由 kube-apiserver 强制。
	// `kubernetes.io` 签名者永远不会签发有效期超过 24 小时的证书。
	//
	// +optional
	// +default=86400
	MaxExpirationSeconds *int32 `json:"maxExpirationSeconds,omitempty" protobuf:"varint,8,opt,name=maxExpirationSeconds"`

	// 签名者将为其签发证书的 PKIX 序列化公钥。
	//
	// 密钥必须是 RSA3072、RSA4096、ECDSAP256、ECDSAP384、ECDSAP521 或
	// ED25519 之一。注意此列表将来可能扩展。
	//
	// 签名者实现不需要支持 kube-apiserver 和 kubelet 支持的所有密钥类型。
	// 若签名者不支持某 PodCertificateRequest 使用的密钥类型，必须拒绝该请求，
	// 方法是设置一条 type 为 "Denied"、reason 为 "UnsupportedKeyType" 的
	// status.conditions 记录。它也可以在 message 字段中建议自己支持的密钥类型。
	//
	// 已废弃：此字段由 StubPKCS10Request 取代。若设置了 StubPKCS10Request，
	// 此字段必须为空。签名者实现应从 StubPKCS10Request 字段中提取公钥。
	//
	// +optional
	PKIXPublicKey []byte `json:"pkixPublicKey" protobuf:"bytes,9,opt,name=pkixPublicKey"`

	// 证明发起请求的 kubelet 持有与 pkixPublicKey 对应的私钥。
	//
	// 它通过对 pod UID 的 ASCII 字节用 `pkixPublicKey` 签名构造而成。
	//
	// kube-apiserver 在创建 PodCertificateRequest 时会验证持有证明。
	//
	// 若密钥是 RSA 密钥，签名是对 pod UID 的 ASCII 字节使用 RFC 8017 中的
	// RSASSA-PSS（由 golang 函数 crypto/rsa.SignPSS 以 nil options 实现）。
	//
	// 若密钥是 ECDSA 密钥，签名如 [SEC 1, Version 2.0]
	// (https://www.secg.org/sec1-v2.pdf) 所述（由 golang 库函数
	// crypto/ecdsa.SignASN1 实现）。
	//
	// 若密钥是 ED25519 密钥, 签名如 [ED25519 Specification]
	// (https://ed25519.cr.yp.to/) 所述（由 golang 库 crypto/ed25519.Sign 实现）。
	//
	// 已废弃：此字段由 StubPKCS10Request 取代。若设置了 StubPKCS10Request，
	// 此字段必须为空。
	//
	// +optional
	ProofOfPossession []byte `json:"proofOfPossession" protobuf:"bytes,10,opt,name=proofOfPossession"`

	// 由 Kubelet 使用主体私钥生成的 PKCS#10 证书签名请求（DER 序列化）。
	//
	// 大多数签名者实现会忽略 CSR 的内容，只提取主体公钥。API server 会在准入时
	// 自动验证 CSR 签名，因此签名者无需重复验证。kubelet 生成的 CSR 完全为空。
	//
	// 主体公钥必须是 RSA3072、RSA4096、ECDSAP256、ECDSAP384、ECDSAP521 或
	// ED25519 之一。注意此列表将来可能扩展。
	//
	// 签名者实现不需要支持 kube-apiserver 和 kubelet 支持的所有密钥类型。
	// 若签名者不支持某 PodCertificateRequest 使用的密钥类型，必须拒绝该请求，
	// 方法是设置一条 type 为 "Denied"、reason 为 "UnsupportedKeyType" 的
	// status.conditions 记录。它也可以在 message 字段中建议自己支持的密钥类型。
	StubPKCS10Request []byte `json:"stubPKCS10Request" protobuf:"bytes,12,opt,name=stubPKCS10Request"`

	// unverifiedUserAnnotations 允许 pod 作者向签名者实现传递额外信息。
	// Kubernetes 不以任何方式限制或校验这些元数据。
	//
	// 这些条目遵循与对象元数据注解相同的校验规则，此外所有键必须带域名前缀。
	// 除整个字段的总大小限制外，对值没有限制。
	//
	// 签名者应记录其支持的键和值。签名者应拒绝含有不认识的键的请求。
	UnverifiedUserAnnotations map[string]string `json:"unverifiedUserAnnotations,omitempty" protobuf:"bytes,11,opt,name=unverifiedUserAnnotations"`
}

// PodCertificateRequestStatus 描述请求的状态，并在请求被签发时保存证书数据。
type PodCertificateRequestStatus struct {
	// 应用于请求的条件。
	//
	// 类型 "Issued"、"Denied" 和 "Failed" 有特殊处理。这些条件最多只能出现一个，
	// 且其 status 必须为 "True"。
	//
	// 若请求因 `Reason=UnsupportedKeyType` 被拒绝，签名者可以在 message 字段中
	// 建议使用某种可用的密钥类型。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// certificateChain 由签名者填入签发的证书。
	// 此字段通过 /status 子资源设置。填充后不可变。
	//
	// 若证书签名请求被拒绝，会添加一条 type 为 "Denied" 的条件，此字段保持为空。
	// 若签名者无法签发证书，会添加一条 type 为 "Failed" 的条件，此字段保持为空。
	//
	// 校验要求：
	//  1. certificateChain 必须由一个或多个 PEM 格式证书组成。
	//  2. 每一项都必须是有效的 PEM 封装、DER 编码的 ASN.1 证书，
	//     如 RFC5280 第 4 节所述。
	//
	// 若存在多个块，且所请求的 spec.signerName 定义未另行说明，则第一个块是
	// 签发的证书，后续块应视为中间证书并在 TLS 握手中呈现。将链投影到 pod 卷时，
	// kubelet 会丢弃 PEM 块之间的任何数据以及所有 PEM 块头。
	//
	// +optional
	CertificateChain string `json:"certificateChain,omitempty" protobuf:"bytes,2,opt,name=certificateChain"`

	// notBefore 是证书开始生效的时间。其值必须与 certificateChain 中叶子证书的
	// notBefore 值相同。此字段通过 /status 子资源设置，填充后不可变。
	// 签名者必须在设置 certificateChain 的同时设置此字段。
	//
	// +optional
	NotBefore *metav1.Time `json:"notBefore,omitempty" protobuf:"bytes,4,opt,name=notBefore"`

	// beginRefreshAt 是 kubelet 应开始尝试刷新证书的时间。
	// 此字段通过 /status 子资源设置，必须与 certificateChain 同时设置。
	// 填充后不可变。
	//
	// 此字段仅是提示。如有必要，kubelet 可以在该时间之前或之后开始刷新。
	//
	// +optional
	BeginRefreshAt *metav1.Time `json:"beginRefreshAt,omitempty" protobuf:"bytes,5,opt,name=beginRefreshAt"`

	// notAfter 是证书过期的时间。其值必须与 certificateChain 中叶子证书的
	// notAfter 值相同。此字段通过 /status 子资源设置，填充后不可变。
	// 签名者必须在设置 certificateChain 的同时设置此字段。
	//
	// +optional
	NotAfter *metav1.Time `json:"notAfter,omitempty" protobuf:"bytes,6,opt,name=notAfter"`
}

// PodCertificateRequest 的知名条件类型。
const (
	// Denied 表示请求被签名者拒绝。
	PodCertificateRequestConditionTypeDenied string = "Denied"
	// Failed 表示签名者签发证书失败。
	PodCertificateRequestConditionTypeFailed string = "Failed"
	// Issued 表示证书已签发。
	PodCertificateRequestConditionTypeIssued string = "Issued"
)

// PodCertificateRequest 的知名条件原因。
const (
	// 当签名者不支持 publicKey 的密钥类型时，应在 "Denied" 条件上设置 UnsupportedKeyType。
	PodCertificateRequestConditionUnsupportedKeyType string = "UnsupportedKeyType"

	// 当签名者不认识 userConfig 中传入的某个键，或认为请求的 userConfig 无效时，
	// 应在 "Denied" 条件上设置 InvalidUnverifiedUserAnnotations。
	PodCertificateRequestConditionInvalidUserConfig string = "InvalidUnverifiedUserAnnotations"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.35
// +k8s:prerelease-lifecycle-gen:deprecated=1.37
// +k8s:prerelease-lifecycle-gen:replacement=certificates.k8s.io,v1,PodCertificateRequestList

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
