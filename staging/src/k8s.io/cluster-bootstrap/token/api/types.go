/*
Copyright 2017 The Kubernetes Authors.

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
	"k8s.io/api/core/v1"
)

const (
	// BootstrapTokenSecretPrefix is the prefix for bootstrap token names.
	// Bootstrap tokens secrets must be named in the form
	// `bootstrap-token-<token-id>`.  This is the prefix to be used before the
	// token ID.
	//
	// Secret 名称采用 `bootstrap-token-<token-id>` 的形式，ID 可公开，
	// 因此可以安全地出现在对象名称中；Secret 本体则只存于 Data。
	BootstrapTokenSecretPrefix = "bootstrap-token-"

	// SecretTypeBootstrapToken is used during the automated bootstrap process (first
	// implemented by kubeadm). It stores tokens that are used to sign well known
	// ConfigMaps. They may also eventually be used for authentication.
	//
	// 使用专用 Secret 类型而非普通 Opaque，便于控制器按类型批量过滤、
	// 回收，并让 RBAC 与审计能明确识别这类高价值凭据。
	SecretTypeBootstrapToken v1.SecretType = "bootstrap.kubernetes.io/token"

	// BootstrapTokenIDKey is the id of this token. This can be transmitted in the
	// clear and encoded in the name of the secret. It must be a random 6 character
	// string that matches the regexp `^([a-z0-9]{6})$`. Required.
	//
	// ID 是 token 的公开部分：它不能单独用于认证，只用于定位和标识 Secret。
	BootstrapTokenIDKey = "token-id"

	// BootstrapTokenSecretKey is the actual secret. It must be a random 16 character
	// string that matches the regexp `^([a-z0-9]{16})$`. Required.
	//
	// Secret 是 token 的保密部分，必须始终被当作密码对待：不可写入日志、
	// 不可放在 Secret 名称或注解中。16 个字符（约 80 bit）在短期有效期
	// 内足以抵御暴力猜测。
	BootstrapTokenSecretKey = "token-secret"

	// BootstrapTokenExpirationKey is when this token should be expired and no
	// longer used. A controller will delete this resource after this time. This
	// is an absolute UTC time using RFC3339. If this cannot be parsed, the token
	// should be considered invalid. Optional.
	//
	// 解析失败时按“已过期”处理，而不是“永久有效”，是安全上的失效关闭
	// （fail-closed）选择：畸形时间不能意外延长凭据寿命。
	BootstrapTokenExpirationKey = "expiration"

	// BootstrapTokenDescriptionKey is a description in human-readable format that
	// describes what the bootstrap token is used for. Optional.
	//
	// 仅供人工运维识别用途，认证与签名逻辑不读取该字段。
	BootstrapTokenDescriptionKey = "description"

	// BootstrapTokenExtraGroupsKey is a comma-separated list of group names.
	// The  bootstrap token will authenticate as these groups in addition to the
	// "system:bootstrappers" group.
	//
	// 附加组必须匹配 BootstrapGroupPattern，避免 token 冒充高权限内置组；
	// 每个名称都会通过 ValidateBootstrapGroupName 校验后才生效。
	BootstrapTokenExtraGroupsKey = "auth-extra-groups"

	// BootstrapTokenUsagePrefix is the prefix for the other usage constants that specifies different
	// functions of a bootstrap token
	//
	// 出于向前兼容，未知的 usage-bootstrap-* 键会被忽略而不是报错，
	// 这样新增用途的集群仍可被旧组件读取。
	BootstrapTokenUsagePrefix = "usage-bootstrap-"

	// BootstrapTokenUsageSigningKey signals that this token should be used to
	// sign configs as part of the bootstrap process. Value must be "true". Any
	// other value is assumed to be false. Optional.
	//
	// 签名用途只允许控制面用 token 为 cluster-info 生成 JWS 签名，避免
	// 把所有 token 都隐式赋予签名能力。
	BootstrapTokenUsageSigningKey = "usage-bootstrap-signing"

	// BootstrapTokenUsageAuthentication signals that this token should be used
	// as a bearer token to authenticate against the Kubernetes API. The bearer
	// token takes the form "<token-id>.<token-secret>" and authenticates as the
	// user "system:bootstrap:<token-id>" in the "system:bootstrappers" group
	// as well as any groups specified using BootstrapTokenExtraGroupsKey.
	// Value must be "true". Any other value is assumed to be false. Optional.
	//
	// 只有显式开启该用途，token 才能作为 Bearer token 通过认证；签名用途
	// 与认证用途相互独立，便于最小权限授予。
	BootstrapTokenUsageAuthentication = "usage-bootstrap-authentication"

	// ConfigMapClusterInfo defines the name for the ConfigMap where the information how to connect and trust the cluster exist
	//
	// 该 ConfigMap 位于 kube-public 命名空间，内容包含集群 CA 与 API
	// 端点；其 kubeconfig 条目用 bootstrap token 的 HMAC 签名，正是防止
	// 未认证的中间人篡改引导信息。
	ConfigMapClusterInfo = "cluster-info"

	// KubeConfigKey defines at which key in the Data object of the ConfigMap the KubeConfig object is stored
	KubeConfigKey = "kubeconfig"

	// JWSSignatureKeyPrefix defines what key prefix the JWS-signed tokens have
	JWSSignatureKeyPrefix = "jws-kubeconfig-"

	// BootstrapUserPrefix is the username prefix bootstrapping bearer tokens
	// authenticate as. The full username given is "system:bootstrap:<token-id>".
	//
	// 用户名只由公开的 token ID 组成，不含 Secret，因此可以安全地出现在
	// 审计日志与 RBAC 绑定中。
	BootstrapUserPrefix = "system:bootstrap:"

	// BootstrapDefaultGroup is the default group for bootstrapping bearer
	// tokens (in addition to any groups from BootstrapTokenExtraGroupsKey).
	//
	// 所有 bootstrap token 默认属于该组；配合 RBAC 通常只授予
	// CSR 创建与相关读取权限，而不授予任意工作负载权限。
	BootstrapDefaultGroup = "system:bootstrappers"

	// BootstrapGroupPattern is the valid regex pattern that all groups
	// assigned to a bootstrap token by BootstrapTokenExtraGroupsKey must match.
	// See also util.ValidateBootstrapGroupName()
	//
	// 限定只能落在 system:bootstrappers 前缀下，且字符集受限，防止 token
	// 被用来加入 system:masters 或其他特权组。
	BootstrapGroupPattern = `\Asystem:bootstrappers:[a-z0-9:-]{0,255}[a-z0-9]\z`

	// BootstrapTokenPattern defines the {id}.{secret} regular expression pattern
	//
	// 完整 token 的形态为 "<id>.<secret>"，即 kubeadm 输出给用户的字符串；
	// 用 \A...\z 锚定整串，避免前缀或后缀篡改通过校验。
	BootstrapTokenPattern = `\A([a-z0-9]{6})\.([a-z0-9]{16})\z`

	// BootstrapTokenIDPattern defines token's id regular expression pattern
	BootstrapTokenIDPattern = `\A([a-z0-9]{6})\z`

	// BootstrapTokenIDBytes defines the number of bytes used for the Bootstrap Token's ID field
	// ID 固定 6 字节，与 Secret 名称 `bootstrap-token-<id>` 的长度约束保持一致。
	BootstrapTokenIDBytes = 6

	// BootstrapTokenSecretBytes defines the number of bytes used the Bootstrap Token's Secret field
	// Secret 固定 16 字节，是判断输入长度的唯一依据；不要把它当作可选值。
	BootstrapTokenSecretBytes = 16
)

// KnownTokenUsages specifies the known functions a token will get.
//
// 这是 ValidateUsages 的唯一白名单来源；新增用途需同时在这里登记，
// 否则写入 Secret 时会被拒绝。
var KnownTokenUsages = []string{"signing", "authentication"}
