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

package certificates

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// 描述一个证书签名请求
type CertificateSigningRequest struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// 证书请求本身及任何附加信息。
	// +optional
	Spec CertificateSigningRequestSpec

	// 关于该请求的派生信息。
	// +optional
	Status CertificateSigningRequestStatus
}

// 请求创建后这些信息不可变。创建时只能设置 Request 和 Usages
// 字段，其余字段由 Kubernetes 派生生成，用户无法修改。
type CertificateSigningRequestSpec struct {
	// Base64 编码的 PKCS#10 CSR 数据
	Request []byte

	// signerName 指示所请求的签名者（signer），是一个限定名称。
	//
	// 可以使用 "spec.signerName=NAME" fieldSelector 按该字段过滤
	// CertificateSigningRequest 的 list/watch 请求。
	//
	// 众所周知的 Kubernetes 签名者有：
	//  1. "kubernetes.io/kube-apiserver-client"：签发可用于向 kube-apiserver
	//   认证的客户端证书。该签名者的请求永远不会被 kube-controller-manager
	//   自动批准，可由 kube-controller-manager 中的 "csrsigning" 控制器签发。
	//  2. "kubernetes.io/kube-apiserver-client-kubelet"：签发 kubelet 用于向
	//   kube-apiserver 认证的客户端证书。该签名者的请求可以被 kube-controller-manager
	//   中的 "csrapproving" 控制器自动批准，也可由 "csrsigning" 控制器签发。
	//  3. "kubernetes.io/kubelet-serving"：签发 kubelet 用于提供 TLS 端点、
	//   供 kube-apiserver 安全连接的服务端证书。该签名者的请求永远不会被
	//   kube-controller-manager 自动批准，可由 kube-controller-manager 中的
	//   "csrsigning" 控制器签发。
	//
	// 更多细节参见 https://k8s.io/docs/reference/access-authn-authz/certificate-signing-requests/#kubernetes-signers
	//
	// 也可以指定自定义 signerName。签名者需定义：
	//  1. 信任分发方式：如何分发信任（CA bundle）。
	//  2. 允许的 subject：以及请求不被允许的 subject 时的行为。
	//  3. 请求中必需、允许或禁止的 x509 扩展（包括是否允许 subjectAltName、
	//   允许哪些类型、对允许值的限制），以及请求不被允许的扩展时的行为。
	//  4. 必需、允许或禁止的密钥用途/增强密钥用途。
	//  5. 过期时间/证书有效期：由签名者固定还是可由管理员配置。
	//  6. 是否允许签发 CA 证书的请求。
	SignerName string

	// expirationSeconds 是所签发证书有效期的请求时长。证书签名者签发的
	// 证书有效期可能与此不同，因此客户端必须检查所签发证书中 notBefore
	// 与 notAfter 字段之间的差值，以确定实际有效期。
	//
	// 只要请求的时长不超过 Kubernetes controller manager 的
	// --cluster-signing-duration CLI 旗标所规定的、各实现愿意接受的最大时长，
	// v1.22+ 的内置实现（in-tree）就会遵循该字段。
	//
	// 证书签名者可能因以下各种原因不遵循该字段：
	//
	//   1. 旧签名者不了解该字段（如 v1.22 之前的内置实现）
	//   2. 签名者配置的最大值短于请求的时长
	//   3. 签名者配置的最小值长于请求的时长
	//
	// expirationSeconds 的最小合法值为 600，即 10 分钟。
	//
	// +optional
	ExpirationSeconds *int32

	// usages 指定密钥有效的用途上下文集合。
	// 参见:
	//	https://tools.ietf.org/html/rfc5280#section-4.2.1.3
	//	https://tools.ietf.org/html/rfc5280#section-4.2.1.12
	Usages []KeyUsage

	// Username 是发起请求的用户名信息。
	// 详见 user.Info 接口。
	// +optional
	Username string
	// UID 是发起请求用户的 UID 信息。
	// 详见 user.Info 接口。
	// +optional
	UID string
	// Groups 是发起请求用户的组信息。
	// 详见 user.Info 接口。
	// +optional
	Groups []string
	// Extra 是发起请求用户的附加（extra）信息。
	// 详见 user.Info 接口。
	// +optional
	Extra map[string]ExtraValue
}

// kube-controller-manager 内置支持的 signerName 取值。
// 这些用途均与 ServiceAccount token secret 的
// `.data[ca.crt]` 无任何关联。
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

// ExtraValue 对值做掩码，使 protobuf 能够生成
type ExtraValue []string

type CertificateSigningRequestStatus struct {
	// 应用于请求的条件，例如批准或拒绝。
	// +optional
	Conditions []CertificateSigningRequestCondition

	// 若请求被批准，控制器会将签发的证书放在这里。
	// +optional
	Certificate []byte
}

type RequestConditionType string

// 证书请求可能具有的条件。
const (
	CertificateApproved RequestConditionType = "Approved"
	CertificateDenied   RequestConditionType = "Denied"
	CertificateFailed   RequestConditionType = "Failed"
)

type CertificateSigningRequestCondition struct {
	// 条件的类型。已知条件包括 "Approved"、"Denied" 和 "Failed"。
	Type RequestConditionType
	// 条件的状态，取值为 True、False 或 Unknown。
	// Approved、Denied 和 Failed 条件的状态不能为 "False" 或 "Unknown"。
	// 若未设置，应视为 "True"。
	// +optional
	Status api.ConditionStatus
	// 请求状态的简要原因。
	// +optional
	Reason string
	// 关于请求状态详情的可读消息。
	// +optional
	Message string
	// 该条件最近一次更新的时间戳。
	// +optional
	LastUpdateTime metav1.Time
	// lastTransitionTime 是该条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type CertificateSigningRequestList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// +optional
	Items []CertificateSigningRequest
}

// KeyUsage 指定密钥的有效使用场景。
// 参见:
//
//	https://tools.ietf.org/html/rfc5280#section-4.2.1.3
//	https://tools.ietf.org/html/rfc5280#section-4.2.1.12
type KeyUsage string

const (
	UsageSigning           KeyUsage = "signing"
	UsageDigitalSignature  KeyUsage = "digital signature"
	UsageContentCommitment KeyUsage = "content commitment"
	UsageKeyEncipherment   KeyUsage = "key encipherment"
	UsageKeyAgreement      KeyUsage = "key agreement"
	UsageDataEncipherment  KeyUsage = "data encipherment"
	UsageCertSign          KeyUsage = "cert sign"
	UsageCRLSign           KeyUsage = "crl sign"
	UsageEncipherOnly      KeyUsage = "encipher only"
	UsageDecipherOnly      KeyUsage = "decipher only"
	UsageAny               KeyUsage = "any"
	UsageServerAuth        KeyUsage = "server auth"
	UsageClientAuth        KeyUsage = "client auth"
	UsageCodeSigning       KeyUsage = "code signing"
	UsageEmailProtection   KeyUsage = "email protection"
	UsageSMIME             KeyUsage = "s/mime"
	UsageIPsecEndSystem    KeyUsage = "ipsec end system"
	UsageIPsecTunnel       KeyUsage = "ipsec tunnel"
	UsageIPsecUser         KeyUsage = "ipsec user"
	UsageTimestamping      KeyUsage = "timestamping"
	UsageOCSPSigning       KeyUsage = "ocsp signing"
	UsageMicrosoftSGC      KeyUsage = "microsoft sgc"
	UsageNetscapeSGC       KeyUsage = "netscape sgc"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTrustBundle 是一个集群级别的容器，用于存放 X.509 信任锚
// （根证书）。
//
// 集群中任何通过认证的用户都被认为可以读取 ClusterTrustBundle 对象。
//
// 它可以可选地与特定签名者关联；此时它包含该签名者的一组有效信任锚。
// 一个签名者可以关联多个 ClusterTrustBundle；每个都是该签名者独立的一组
// 信任锚。
type ClusterTrustBundle struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// spec 包含签名者（如有）和信任锚。
	// +optional
	Spec ClusterTrustBundleSpec
}

// ClusterTrustBundleSpec 包含签名者和信任锚。
type ClusterTrustBundleSpec struct {
	// SignerName 指示关联的签名者（如有）。
	SignerName string

	// TrustBundle 包含此 bundle 的各个 X.509 信任锚，
	// 形式为 PEM bundle，其中每项是 PEM 包裹、DER 编码的 X.509 证书。
	//
	// 数据必须仅由可解析为合法 X.509 证书的 PEM 证书块组成。
	// 每个证书必须包含一个 basic constraints 扩展且 CA 位已置位。
	// 若对象包含重复证书，或使用了 PEM 块头部，API server 将拒绝该对象。
	//
	// ClusterTrustBundle 的使用者（包括 Kubelet）可以按自己的逻辑
	// 对文件中的证书块重新排序和去重，也可以丢弃 PEM 块头部及块间数据。
	TrustBundle string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTrustBundleList 是 ClusterTrustBundle 对象的集合
type ClusterTrustBundleList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// items 是 ClusterTrustBundle 对象的集合。
	Items []ClusterTrustBundle
}

// MaxTrustBundleSize 是单个 trust bundle 字段的最大允许大小。
const MaxTrustBundleSize = 1 * 1024 * 1024

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodCertificateRequest 编码了某个 Pod 向指定 signer 请求证书的信息。
//
// Kubelet 使用该 API 实现 podCertificate 投影卷。
type PodCertificateRequest struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// spec 包含所请求证书的详细信息。
	Spec PodCertificateRequestSpec

	// status 包含签发的证书和一组标准状况。
	// +optional
	Status PodCertificateRequestStatus
}

// PodCertificateRequestSpec 描述该证书请求。所有字段在创建后均不可变。
type PodCertificateRequestSpec struct {
	// SignerName 指示所请求的签名者。
	//
	// 所有以 `kubernetes.io` 开头的 signer 名称均保留给 Kubernetes 项目使用。
	// 目前 Kubernetes 项目记录了一个广为人知的签名者
	// `kubernetes.io/kube-apiserver-client-pod`，它将签发可被 kube-apiserver
	// 识别的客户端证书。该签名者目前尚未实现。
	SignerName string

	// PodName 是将挂载该证书的 Pod 的名称。
	PodName string
	// PodUID 是将挂载该证书的 Pod 的 UID。
	PodUID types.UID

	// ServiceAccountname 是该 Pod 运行时所使用的服务账号的名称。
	ServiceAccountName string
	// ServiceAccountUID 是该 Pod 运行时所使用的服务账号的 UID。
	ServiceAccountUID types.UID

	// NodeName 是该 Pod 被分配到的节点名称。
	NodeName types.NodeName
	// NodeUID 是该 Pod 被分配到的节点的 UID。
	NodeUID types.UID

	// maxExpirationSeconds 是证书允许的最大有效期。
	// 若省略，kube-apiserver 会将其设为 86400（24 小时）。kube-apiserver
	// 会拒绝短于 3600（1 小时）的值。允许的最大值为 7862400（91 天）。
	// 签名者实现可以自由签发有效期*短于* MaxExpirationSeconds 的证书，
	// 但不得短于 3600 秒（1 小时）。此约束由 kube-apiserver 强制。
	MaxExpirationSeconds *int32

	// PKIXPublicKey 是 signer 签发证书时对应的 PKIX 序列化公钥。
	//
	// 密钥必须是 RSA3072、RSA4096、ECDSAP256、ECDSAP384、ECDSAP521 或
	// ED25519 之一。注意此列表将来可能扩展。
	//
	// Signer 实现无需支持 kube-apiserver 和 kubelet 支持的所有密钥类型。
	// 若 signer 不支持某个 PodCertificateRequest 使用的密钥类型，
	// 它必须通过以下方式拒绝该请求：
	// 设置 status.conditions 条目，type 为 "Denied"、reason 为
	// "UnsupportedKeyType"。它也可以在 message 字段中建议自己支持的密钥类型。
	//
	// Deprecated: 该字段由 StubPKCS10Request 取代。若 StubPKCS10Request 已设置，
	// 则此字段必须为空。Signer 实现应从 StubPKCS10Request 字段中提取公钥。
	//
	// +optional
	PKIXPublicKey []byte

	// ProofOfPossession 证明请求方 kubelet 持有与 pkixPublicKey 相对应的私钥。
	//
	// 其构造方式是使用 `pkixPublicKey` 对 Pod UID 的 ASCII 字节进行签名。
	//
	// kube-apiserver 会在创建 PodCertificateRequest 时校验该持有证明。
	//
	// 若密钥是 RSA 密钥，则签名对象为 Pod UID 的 ASCII 字节，
	// 使用 RFC 8017 的 RSASSA-PSS（即 Go 标准库函数 crypto/rsa.SignPSS
	// 在 options 为 nil 时的实现）。
	//
	// 若密钥是 ECDSA 密钥，签名方式如
	// [SEC 1, Version 2.0](https://www.secg.org/sec1-v2.pdf) 所述
	// （即 Go 标准库 crypto/ecdsa.SignASN1 的实现）。
	//
	// 若密钥是 ED25519 密钥，签名方式如
	// [ED25519 Specification](https://ed25519.cr.yp.to/) 所述
	// （即 Go 标准库 crypto/ed25519.Sign 的实现）。
	//
	// Deprecated: 该字段由 StubPKCS10Request 取代。若 StubPKCS10Request 已设置，
	// 则此字段必须为空。
	//
	// +optional
	ProofOfPossession []byte

	// StubPKCS10Request 是由 Kubelet 使用 subject 私钥生成的
	// PKCS#10 证书签名请求（DER 序列化）。
	//
	// 多数 signer 实现会忽略 CSR 的内容，仅从中提取 subject 公钥。
	// API server 会在准入阶段自动校验 CSR 签名，因此 signer 无需重复校验。
	// kubelet 生成的 CSR 内容完全为空。
	//
	// subject 公钥必须是 RSA3072、RSA4096、ECDSAP256、ECDSAP384、ECDSAP521
	// 或 ED25519 之一。注意此列表将来可能扩展。
	//
	// Signer 实现无需支持 kube-apiserver 和 kubelet 支持的所有密钥类型。
	// 若 signer 不支持某个 PodCertificateRequest 使用的密钥类型，
	// 它必须通过以下方式拒绝该请求：
	// 设置 status.conditions 条目，type 为 "Denied"、reason 为
	// "UnsupportedKeyType"。它也可以在 message 字段中建议自己支持的密钥类型。
	StubPKCS10Request []byte

	// unverifiedUserAnnotations 允许 Pod 作者向签名者实现传递附加信息。
	// Kubernetes 不会以任何方式限制或校验这些元数据。
	// 条目遵循与对象元数据注解相同的校验规则，但所有键必须带域名前缀。除整个字段的总大小限制外，
	// 对值没有限制。
	// 签名者应记录自己支持的键和值，并拒绝含有无法识别键的请求。
	UnverifiedUserAnnotations map[string]string
}

type PodCertificateRequestStatus struct {
	// 应用在请求上的状况。
	// "Issued"、"Denied" 和 "Failed" 类型有特殊处理：这些状况最多出现一个，
	// 且状态必须为 "True"。
	// 若请求以 `Reason=UnsupportedKeyType` 被拒绝，签名者可在 message 字段建议使用可用的密钥类型。
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition

	// certificateChain 由签名者填入签发的证书。
	// 该字段通过 /status 子资源设置，填充后不可修改。
	// 若证书签名请求被拒绝，会添加类型为 "Denied" 的状况，该字段保持为空。
	// 若签名者无法签发证书，会添加类型为 "Failed" 的状况，该字段同样保持为空。
	// 校验要求：
	// 1. certificateChain 必须由一个或多个 PEM 格式的证书组成。
	// 2. 每一项必须是 RFC5280 第 4 节描述的、合法 PEM 包裹的 DER 编码 ASN.1 证书。
	// 如果存在多个块，且请求的 spec.signerName 定义未另行说明，则第一个块是签发的证书，
	// 后续块应视为中间证书并在 TLS 握手中提供。将证书链投影到 Pod 卷时，
	// kubelet 会丢弃 PEM 块之间的数据以及任何 PEM 块头部。
	// +optional
	CertificateChain string

	// notBefore 是证书开始生效的时间。该字段通过 /status 子资源设置，
	// 填充后不可修改。签名者必须在设置 certificateChain 的同时设置该字段。
	//
	// +optional
	NotBefore *metav1.Time

	// beginRefreshAt 是 kubelet 应开始尝试刷新证书的时间。该字段通过 /status 子资源设置，
	// 必须与 certificateChain 同时设置，填充后不可修改。
	// 该字段仅作提示，必要时 kubelet 可早于或晚于该时间开始刷新。
	// +optional
	BeginRefreshAt *metav1.Time

	// notAfter 是证书过期的时间。该字段通过 /status 子资源设置，
	// 填充后不可修改。签名者必须在设置 certificateChain 的同时设置该字段。
	//
	// +optional
	NotAfter *metav1.Time
}

// PodCertificateRequest 的已知条件类型
const (
	// Denied 表示请求被签名者拒绝。
	PodCertificateRequestConditionTypeDenied string = "Denied"
	// Failed 表示签名者签发证书失败。
	PodCertificateRequestConditionTypeFailed string = "Failed"
	// Issued 表示证书已签发。
	PodCertificateRequestConditionTypeIssued string = "Issued"
)

// PodCertificateRequest 的已知条件原因
const (
	// UnsupportedKeyType 应在签名者不支持 publicKey 的密钥类型时，
	// 设置到 "Denied" 状况上。
	PodCertificateRequestConditionUnsupportedKeyType string = "UnsupportedKeyType"

	// InvalidUnverifiedUserAnnotations 应在签名者不识别 userConfig 中的某个键，
	// 或认为请求的 userConfig 无效时，设置到 "Denied" 状况上。
	PodCertificateRequestConditionInvalidUserConfig string = "InvalidUnverifiedUserAnnotations"
)

const (
	// MaxPKIXPublicKeySize 是 PKIXPublicKey 字段允许的最大大小。
	// 该大小基于 RSA 4096 密钥的大小并留有一定余量。
	MaxPKIXPublicKeySize = 10 * 1024
	// MaxProofOfPossessionSize 是 ProofOfPossession 字段允许的最大大小。
	MaxProofOfPossessionSize = 10 * 1024
	// MaxStubPKCS10RequestSize 是 UnverifiedPKCS10Request 字段允许的最大大小。
	MaxStubPKCS10RequestSize = 10 * 1024
	// MaxCertificateChainSize 是 CertificateChain 字段允许的最大大小。
	//
	// 该大小应足以存储 10 张各自嵌入大量扩展的 RSA 4096 证书。
	MaxCertificateChainSize = 100 * 1024
	// MinMaxExpirationSeconds 是 MaxExpirationSeconds 字段允许的最小值。
	MinMaxExpirationSeconds = 60 * 60
	// MaxMaxExpirationSeconds 是非 Kubernetes 签名者的
	// MaxExpirationSeconds 字段允许的最大值。
	MaxMaxExpirationSeconds = 91 * 24 * 60 * 60
	// KubernetesMaxMaxExpirationSeconds 是 Kubernetes 签名者的
	// MaxExpirationSeconds 字段允许的最大值。
	KubernetesMaxMaxExpirationSeconds = 24 * 60 * 60
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodCertificateRequestList 是 PodCertificateRequest 对象的集合。
type PodCertificateRequestList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// items 是 PodCertificateRequest 对象的集合。
	Items []PodCertificateRequest
}
