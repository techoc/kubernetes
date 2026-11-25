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
	// BootstrapTokenSecretPrefix 是 bootstrap token Secret 名称的前缀。
	// 名称必须为 `bootstrap-token-<token-id>`；ID 可公开，Secret 本体只存入 Data。
	BootstrapTokenSecretPrefix = "bootstrap-token-"

	// SecretTypeBootstrapToken 是自动化引导流程使用的专用 Secret 类型。
	// 专用类型便于控制器筛选和回收，也让 RBAC 与审计能识别这类凭据。
	SecretTypeBootstrapToken v1.SecretType = "bootstrap.kubernetes.io/token"

	// BootstrapTokenIDKey 是 Secret 中存放公开 token ID 的必需键。
	// ID 必须是随机的 6 位小写字母或数字，可放入 Secret 名称，但不能单独用于认证。
	BootstrapTokenIDKey = "token-id"

	// BootstrapTokenSecretKey 是 Secret 中存放保密部分的必需键。
	// 值必须是随机的 16 位小写字母或数字，不得写入日志、对象名称或注解。
	BootstrapTokenSecretKey = "token-secret"

	// BootstrapTokenExpirationKey 是可选的绝对过期时间，采用 RFC3339 UTC 格式。
	// 控制器在到期后删除 Secret；无法解析时必须按已过期处理，不能视为永久有效。
	BootstrapTokenExpirationKey = "expiration"

	// BootstrapTokenDescriptionKey 是可选的人工可读用途说明。
	// 认证与签名逻辑不读取该字段。
	BootstrapTokenDescriptionKey = "description"

	// BootstrapTokenExtraGroupsKey 是逗号分隔的附加组名称，认证时叠加到默认组。
	// 每个名称都必须匹配 BootstrapGroupPattern，防止 token 加入特权组。
	BootstrapTokenExtraGroupsKey = "auth-extra-groups"

	// BootstrapTokenUsagePrefix 是 token 用途键的前缀。
	// 未知的 usage-bootstrap-* 键会被忽略，以兼容由更新组件写入的 Secret。
	BootstrapTokenUsagePrefix = "usage-bootstrap-"

	// BootstrapTokenUsageSigningKey 是可选签名用途开关，仅值为 "true" 时启用。
	// 该用途允许控制面为 cluster-info 生成 JWS 签名；其他值均按 false 处理。
	BootstrapTokenUsageSigningKey = "usage-bootstrap-signing"

	// BootstrapTokenUsageAuthentication 是可选 Bearer token 认证用途开关，仅值为 "true" 时启用。
	// token 形如 "<token-id>.<token-secret>"，用户名为 "system:bootstrap:<token-id>"，
	// 所属组为 "system:bootstrappers" 及 BootstrapTokenExtraGroupsKey 指定的附加组。
	// 签名与认证用途相互独立；其他值均按 false 处理。
	BootstrapTokenUsageAuthentication = "usage-bootstrap-authentication"

	// ConfigMapClusterInfo 是 kube-public 命名空间中保存集群连接与信任信息的 ConfigMap 名称。
	// 其中 kubeconfig 条目带有 bootstrap token 的 HMAC 签名，用于检测中间人篡改。
	ConfigMapClusterInfo = "cluster-info"

	// KubeConfigKey 是 ConfigMapClusterInfo 的 Data 中存放 kubeconfig 的键。
	KubeConfigKey = "kubeconfig"

	// JWSSignatureKeyPrefix 是 ConfigMapClusterInfo 中 detached JWS 签名条目的键前缀。
	JWSSignatureKeyPrefix = "jws-kubeconfig-"

	// BootstrapUserPrefix 是 bootstrap Bearer token 认证用户名的前缀。
	// 完整用户名为 "system:bootstrap:<token-id>"，不含 Secret，可安全用于审计和 RBAC。
	BootstrapUserPrefix = "system:bootstrap:"

	// BootstrapDefaultGroup 是 bootstrap Bearer token 的默认组，附加组在此之上叠加。
	// 该组本身不代表管理员权限，实际权限由 RBAC 绑定决定。
	BootstrapDefaultGroup = "system:bootstrappers"

	// BootstrapGroupPattern 限制 BootstrapTokenExtraGroupsKey 中所有组名的格式。
	// 名称只能位于 system:bootstrappers 命名空间且字符集受限，防止加入 system:masters 等特权组。
	BootstrapGroupPattern = `\Asystem:bootstrappers:[a-z0-9:-]{0,255}[a-z0-9]\z`

	// BootstrapTokenPattern 匹配完整的 "<id>.<secret>" token。
	// \A 与 \z 锚定整串，拒绝前后附加内容；6 位 ID 和 16 位 Secret 均限于小写字母与数字。
	BootstrapTokenPattern = `\A([a-z0-9]{6})\.([a-z0-9]{16})\z`

	// BootstrapTokenIDPattern 匹配完整的 6 位公开 token ID。
	BootstrapTokenIDPattern = `\A([a-z0-9]{6})\z`

	// BootstrapTokenIDBytes 是生成 token ID 时使用的随机字节数。
	// 固定为 6，与 Secret 名称中的 ID 长度约束一致。
	BootstrapTokenIDBytes = 6

	// BootstrapTokenSecretBytes 是生成或校验 token Secret 时使用的固定字节数。
	BootstrapTokenSecretBytes = 16
)

// KnownTokenUsages 是 bootstrap token 支持的用途白名单。
// ValidateUsages 据此拒绝未知用途；新增用途必须同步登记。
var KnownTokenUsages = []string{"signing", "authentication"}
