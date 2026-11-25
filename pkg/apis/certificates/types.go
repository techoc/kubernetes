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

// Describes a certificate signing request
type CertificateSigningRequest struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// The certificate request itself and any additional information.
	// +optional
	Spec CertificateSigningRequestSpec

	// Derived information about the request.
	// +optional
	Status CertificateSigningRequestStatus
}

// This information is immutable after the request is created. Only the Request
// and Usages fields can be set on creation, other fields are derived by
// Kubernetes and cannot be modified by users.
type CertificateSigningRequestSpec struct {
	// Base64-encoded PKCS#10 CSR data
	Request []byte

	// signerName indicates the requested signer, and is a qualified name.
	//
	// List/watch requests for CertificateSigningRequests can filter on this field using a "spec.signerName=NAME" fieldSelector.
	//
	// Well-known Kubernetes signers are:
	//  1. "kubernetes.io/kube-apiserver-client": issues client certificates that can be used to authenticate to kube-apiserver.
	//   Requests for this signer are never auto-approved by kube-controller-manager, can be issued by the "csrsigning" controller in kube-controller-manager.
	//  2. "kubernetes.io/kube-apiserver-client-kubelet": issues client certificates that kubelets use to authenticate to kube-apiserver.
	//   Requests for this signer can be auto-approved by the "csrapproving" controller in kube-controller-manager, and can be issued by the "csrsigning" controller in kube-controller-manager.
	//  3. "kubernetes.io/kubelet-serving" issues serving certificates that kubelets use to serve TLS endpoints, which kube-apiserver can connect to securely.
	//   Requests for this signer are never auto-approved by kube-controller-manager, and can be issued by the "csrsigning" controller in kube-controller-manager.
	//
	// More details are available at https://k8s.io/docs/reference/access-authn-authz/certificate-signing-requests/#kubernetes-signers
	//
	// Custom signerNames can also be specified. The signer defines:
	//  1. Trust distribution: how trust (CA bundles) are distributed.
	//  2. Permitted subjects: and behavior when a disallowed subject is requested.
	//  3. Required, permitted, or forbidden x509 extensions in the request (including whether subjectAltNames are allowed, which types, restrictions on allowed values) and behavior when a disallowed extension is requested.
	//  4. Required, permitted, or forbidden key usages / extended key usages.
	//  5. Expiration/certificate lifetime: whether it is fixed by the signer, configurable by the admin.
	//  6. Whether or not requests for CA certificates are allowed.
	SignerName string

	// expirationSeconds is the requested duration of validity of the issued
	// certificate. The certificate signer may issue a certificate with a different
	// validity duration so a client must check the delta between the notBefore and
	// and notAfter fields in the issued certificate to determine the actual duration.
	//
	// The v1.22+ in-tree implementations of the well-known Kubernetes signers will
	// honor this field as long as the requested duration is not greater than the
	// maximum duration they will honor per the --cluster-signing-duration CLI
	// flag to the Kubernetes controller manager.
	//
	// Certificate signers may not honor this field for various reasons:
	//
	//   1. Old signer that is unaware of the field (such as the in-tree
	//      implementations prior to v1.22)
	//   2. Signer whose configured maximum is shorter than the requested duration
	//   3. Signer whose configured minimum is longer than the requested duration
	//
	// The minimum valid value for expirationSeconds is 600, i.e. 10 minutes.
	//
	// +optional
	ExpirationSeconds *int32

	// usages specifies a set of usage contexts the key will be
	// valid for.
	// See:
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

// Built in signerName values that are honoured by kube-controller-manager.
// None of these usages are related to ServiceAccount token secrets
// `.data[ca.crt]` in any way.
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
	// brief reason for the request state
	// +optional
	Reason string
	// human readable message with details about the request state
	// +optional
	Message string
	// timestamp for the last update to this condition
	// +optional
	LastUpdateTime metav1.Time
	// lastTransitionTime is the time the condition last transitioned from one status to another.
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

// ClusterTrustBundle is a cluster-scoped container for X.509 trust anchors
// (root certificates).
//
// ClusterTrustBundle objects are considered to be readable by any authenticated
// user in the cluster.
//
// It can be optionally associated with a particular signer, in which case it
// contains one valid set of trust anchors for that signer. Signers may have
// multiple associated ClusterTrustBundles; each is an independent set of trust
// anchors for that signer.
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
	// SignerName indicates the associated signer, if any.
	SignerName string

	// TrustBundle contains the individual X.509 trust anchors for this
	// bundle, as PEM bundle of PEM-wrapped, DER-formatted X.509 certificates.
	//
	// The data must consist only of PEM certificate blocks that parse as valid
	// X.509 certificates.  Each certificate must include a basic constraints
	// extension with the CA bit set.  The API server will reject objects that
	// contain duplicate certificates, or that use PEM block headers.
	//
	// Users of ClusterTrustBundles, including Kubelet, are free to reorder and
	// deduplicate certificate blocks in this file according to their own logic,
	// as well as to drop PEM block headers and inter-block data.
	TrustBundle string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTrustBundleList is a collection of ClusterTrustBundle objects
type ClusterTrustBundleList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// items 是 ClusterTrustBundle 对象的集合。
	Items []ClusterTrustBundle
}

// MaxTrustBundleSize is the maximimum size of a single trust bundle field.
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

// PodCertificateRequestSpec describes the certificate request.  All fields are
// immutable after creation.
type PodCertificateRequestSpec struct {
	// SignerName indicates the requested signer.
	//
	// All signer names beginning with `kubernetes.io` are reserved for use by
	// the Kubernetes project.  There is currently one well-known signer
	// documented by the Kubernetes project,
	// `kubernetes.io/kube-apiserver-client-pod`, which will issue client
	// certificates understood by kube-apiserver.  It is currently
	// unimplemented.
	SignerName string

	// PodName is the name of the pod into which the certificate will be mounted.
	PodName string
	// PodUID is the UID of the pod into which the certificate will be mounted.
	PodUID types.UID

	// ServiceAccountname is the name of the service account the pod is running as.
	ServiceAccountName string
	// ServiceAccountUID is the UID of the service account the pod is running as.
	ServiceAccountUID types.UID

	// NodeName is the name of the node the pod is assigned to.
	NodeName types.NodeName
	// NodeUID is the UID of the node the pod is assigned to.
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

	// notBefore is the time at which the certificate becomes valid.  This field
	// is set via the /status subresource.  Once populated, it is immutable.
	// The signer must set this field at the same time it sets certificateChain.
	//
	// +optional
	NotBefore *metav1.Time

	// beginRefreshAt 是 kubelet 应开始尝试刷新证书的时间。该字段通过 /status 子资源设置，
	// 必须与 certificateChain 同时设置，填充后不可修改。
	// 该字段仅作提示，必要时 kubelet 可早于或晚于该时间开始刷新。
	// +optional
	BeginRefreshAt *metav1.Time

	// notAfter is the time at which the certificate expires.  This field is set
	// via the /status subresource.  Once populated, it is immutable.  The
	// signer must set this field at the same time it sets certificateChain.
	//
	// +optional
	NotAfter *metav1.Time
}

// Well-known condition types for PodCertificateRequests
const (
	// Denied 表示请求被签名者拒绝。
	PodCertificateRequestConditionTypeDenied string = "Denied"
	// Failed 表示签名者签发证书失败。
	PodCertificateRequestConditionTypeFailed string = "Failed"
	// Issued 表示证书已签发。
	PodCertificateRequestConditionTypeIssued string = "Issued"
)

// Well-known condition reasons for PodCertificateRequests
const (
	// UnsupportedKeyType 应在签名者不支持 publicKey 的密钥类型时，
	// 设置到 "Denied" 状况上。
	PodCertificateRequestConditionUnsupportedKeyType string = "UnsupportedKeyType"

	// InvalidUnverifiedUserAnnotations 应在签名者不识别 userConfig 中的某个键，
	// 或认为请求的 userConfig 无效时，设置到 "Denied" 状况上。
	PodCertificateRequestConditionInvalidUserConfig string = "InvalidUnverifiedUserAnnotations"
)

const (
	// MaxPKIXPublicKeySize is the maximimum size permitted for the
	// PKIXPublicKey field.  Size is chosen based on the size of an RSA 4096 key
	// plus some margin.
	MaxPKIXPublicKeySize = 10 * 1024
	// MaxProofOfPossessionSize is the maximum size permitted for the
	// ProofOfPossession field.
	MaxProofOfPossessionSize = 10 * 1024
	// MaxStubPKCS10RequestSize is the maximum size permitted for the
	// UnverifiedPKCS10Request field.
	MaxStubPKCS10RequestSize = 10 * 1024
	// MaxCertificateChainSize is the maximum size permitted for the
	// CertificateChain field.
	//
	// Size should be more than sufficient to store 10 RSA 4096 certificates,
	// each with a bunch of embedded extensions.
	MaxCertificateChainSize = 100 * 1024
	// MinMaxExpirationSeconds is the minimum value permitted for the MaxExpirationSeconds field.
	MinMaxExpirationSeconds = 60 * 60
	// MaxMaxExpirationSeconds is the maximum value permitted for the
	// MaxExpirationSeconds field for non-Kubernetes signers.
	MaxMaxExpirationSeconds = 91 * 24 * 60 * 60
	// KubernetesMaxMaxExpirationSeconds is the maximum value permitted for the
	// MaxExpirationSeconds field for Kubernetes signers.
	KubernetesMaxMaxExpirationSeconds = 24 * 60 * 60
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodCertificateRequestList is a collection of PodCertificateRequest objects.
type PodCertificateRequestList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// items 是 PodCertificateRequest 对象的集合。
	Items []PodCertificateRequest
}
