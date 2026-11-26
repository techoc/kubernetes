/*
Copyright 2022 The Kubernetes Authors.

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
)

// ServiceAccountTokenCacheType 是使用 service account token 时缓存插件返回凭据的键类型。
// 缓存粒度决定“哪些 Pod 可以共享拉取凭据”：粒度太粗会串用凭据，太细则每次拉取都要重新执行插件。
type ServiceAccountTokenCacheType string

const (
	// TokenServiceAccountTokenCacheType 表示 kubelet 按 token 缓存返回的凭据。
	// 当凭据有效期受限于输入 token 的有效期时应使用此类型。
	// 例如直接把输入的 service account token 作为拉取凭据返回时必须用它。
	// 透传 token 时必须按 token 缓存，否则 token 过期后旧凭据还会被命中。
	TokenServiceAccountTokenCacheType ServiceAccountTokenCacheType = "Token"
	// ServiceAccountServiceAccountTokenCacheType 表示 kubelet 按 service account 缓存凭据。
	// 当插件的凭据获取逻辑只依赖 service account、与 Pod 特有声明无关时应使用。
	// 返回的凭据对该 service account 下所有 Pod 都有效时使用。
	// 插件凭据与 Pod 声明无关时才可用此粒度，用 Pod 特有声明换来的凭据不能这样共享。
	ServiceAccountServiceAccountTokenCacheType ServiceAccountTokenCacheType = "ServiceAccount"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderConfig 包含各 exec 凭据插件的配置。
// kubelet 从磁盘读取该配置，并按 CredentialProvider 类型
// 启用其中每个 provider。
// 配置文件从磁盘读取，插件列表的顺序会影响凭据优先级，不能随意重排。
type CredentialProviderConfig struct {
	metav1.TypeMeta `json:""`

	// providers 是 kubelet 要启用的凭据插件列表。
	// 同一镜像可能匹配多个 provider，此时它们的凭据都会返回给 kubelet。
	// 若为一个镜像调用了多个 provider，结果会合并；若返回的 auth key 重叠，
	// 列表中较靠前的 provider 的值优先尝试。
	// 多个插件命中同一镜像时结果会合并，靠前的插件优先尝试，因此顺序即优先级。
	Providers []CredentialProvider `json:"providers"`
}

// CredentialProvider 表示 kubelet 调用的 exec 插件。只有当被拉取镜像
// 匹配该插件处理的镜像时（见 matchImages）才会调用。
// 仅在镜像匹配时执行插件，避免每次拉取都启动外部进程。
type CredentialProvider struct {
	// name 是凭据插件的必填名字，必须与 kubelet 看到的插件可执行文件名一致，
	// 可执行文件必须位于 kubelet 的 bin 目录（由 --image-credential-provider-bin-dir 设置）。
	// 在所有 provider 中必须唯一。
	// 必须与 kubelet bin 目录下的可执行文件名一致，且全局唯一，否则无法定位插件。
	Name string `json:"name"`

	// matchImages 是必填的匹配串列表，用于判断是否为某镜像调用该插件。
	// 任一字符串匹配 kubelet 请求的镜像时，插件就会被调用并提供凭据。
	// 镜像名应包含 registry 域名与 URL 路径。
	// matchImages 每项都是可含端口与路径的模式串。
	// 通配符只用于域名，不用于端口和路径；支持子域
	// （如 '*.k8s.io'、'k8s.*.io'）与顶级域（如 'k8s.*'）。
	// 也支持部分子域匹配（如 'app*.k8s.io'）。每个通配符只匹配一段子域，
	// 因此 *.io 不匹配 *.k8s.io。
	// 镜像与 matchImage 匹配的条件是：
	// - 域名段数相同且逐段匹配；
	// - imageMatch 的 URL 路径必须是目标镜像路径的前缀；
	// - imageMatch 若含端口，镜像中也必须一致。
	// matchImages 示例：
	// - 123456789.dkr.ecr.us-east-1.amazonaws.com
	// - *.azurecr.io
	// - gcr.io
	// - *.*.registry.io
	// - registry.io:8080/path
	// 通配符只覆盖域名段，不给端口和路径用，就是为了避免匹配范围失控导致凭据泄露。
	MatchImages []string `json:"matchImages"`

	// defaultCacheDuration 是插件未返回缓存时长时，kubelet 默认在内存中缓存凭据的时长，必填。
	// （插件响应中未提供缓存时长时）。该字段必填。
	// 作为插件未声明 TTL 时的兜底，防止插件每次拉取都被执行。
	DefaultCacheDuration *metav1.Duration `json:"defaultCacheDuration"`

	// exec CredentialProviderRequest 要求的输入版本。返回的 CredentialProviderResponse
	// 必须使用与输入相同的编码版本。当前支持：
	// - credentialprovider.kubelet.k8s.io/v1
	// 输入输出版本必须一致，插件才能确定怎样编解码请求体。
	APIVersion string `json:"apiVersion"`

	// 执行命令时传入的参数。
	// +optional
	Args []string `json:"args,omitempty"`

	// Env 定义暴露给进程环境变量，与宿主环境及 client-go 传给插件的变量合并。
	// 与宿主环境合并而非替换，插件才能继承 PATH、证书位置等运行时变量。
	// +optional
	Env []ExecEnvVar `json:"env,omitempty"`

	// tokenAttributes 是传给插件的 service account token 配置。
	// 插件通过设置该字段选择用 service account token 拉取镜像。
	// 设置后 kubelet 会生成绑定待拉取镜像 Pod 的 service account token，
	// 连同插件要求的其它属性一起放入 CredentialProviderRequest 传给插件。
	// service account 元数据与 token 属性会作为 kubelet 缓存凭据的维度。
	// 缓存键由 service account 元数据（namespace、name、UID，以及
	// token 中的 Pod 元数据（namespace、name、UID）不作为缓存维度，
	// 因此共用同一 service account 的工作负载可能共用拉取凭据。不想如此的插件，
	// 或透传模式（原样返回 service account token）的插件，
	// 可把 response 的 cacheDuration 设为 0，
	// 这样每次拉取都会调用插件，但会带来 token 生成开销；
	// 这是确保凭据不在 Pod 间共享（即使同一 service account）的唯一方式。
	// 缓存只按服务账号维度，不按 Pod 维度：同账号的多个 Pod 会共享凭据，
	// 需要严格隔离的插件应把 cacheDuration 设为 0 或按 token 缓存。
	// +optional
	TokenAttributes *ServiceAccountTokenAttributes `json:"tokenAttributes,omitempty"`
}

// ServiceAccountTokenAttributes 是传给插件的 service account token 配置。
// 让插件按需索取服务账号信息，而不是默认把 token 交给所有插件。
type ServiceAccountTokenAttributes struct {
	// serviceAccountTokenAudience 是投射 token 的目标 audience。
	// audience 限定 token 只能给目标插件使用，降低 token 被挪作他用的风险。
	// +required
	ServiceAccountTokenAudience string `json:"serviceAccountTokenAudience"`

	// cacheType 指定使用 service account token 时缓存插件返回凭据的键类型。
	// 最保守的选择是 "Token"：kubelet 按 token 缓存返回凭据，
	// 适用于凭据有效期受 token 有效期限制的情况。
	// 若插件凭据逻辑只依赖 service account，则可设为 "ServiceAccount"，
	// 此时 kubelet 按 service account 缓存凭据；
	// 适用于返回凭据对该账号下所有 Pod 都有效的场景。
	// 拿不准插件凭据的生命周期时选 "Token" 更稳妥，代价是每个 token 单独缓存。
	// +required
	CacheType ServiceAccountTokenCacheType `json:"cacheType"`

	// requireServiceAccount 表示插件是否要求 Pod 必须有 service account。
	// 为 true 时，kubelet 只对有 service account 的 Pod 调用插件。
	// 为 false 时，即使 Pod 没有 service account 也会调用插件，
	// 此时请求中不带 token。适用于为无 service account 的 Pod（如静态 Pod）拉镜像的插件。
	// 静态 Pod 没有服务账号，设为 false 才能让插件仍被调用，只是不带 token。
	// +required
	RequireServiceAccount *bool `json:"requireServiceAccount"`

	// requiredServiceAccountAnnotationKeys 是插件关心且必须存在于 service account 上的注解键列表。
	// 列表中的键会从对应 service account 提取并随 CredentialProviderRequest 传给插件。
	// 若任一键不存在，kubelet 不调用插件并返回错误。
	// 该字段可选、可为空，插件可用它提取获取凭据所需的额外信息，
	// 或让工作负载选择启用 service account token 拉镜像。
	// 非空时 requireServiceAccount 必须为 true。
	// 列表中的键必须唯一。
	// 该列表必须与 optionalServiceAccountAnnotationKeys 互斥。
	// 必需的注解缺失就不调用插件，相当于让服务账号自己声明是否启用该插件；
	// 与可选注解列表互斥，避免同一键被两种语义同时解释。
	// +optional
	// +listType=set
	RequiredServiceAccountAnnotationKeys []string `json:"requiredServiceAccountAnnotationKeys,omitempty"`

	// optionalServiceAccountAnnotationKeys 是插件关心、但在 service account 上可选存在的注解键列表。
	// 列表中的键会提取并随请求传给插件；注解是否存在及其值由插件自行校验。
	// 该字段可选、可为空，插件可用它提取获取凭据所需的额外信息。
	// 列表中的键必须唯一。
	// 可选注解交由插件自己校验：缺失不会阻止调用，适合插件自行决定宽松程度的场景。
	// +optional
	// +listType=set
	OptionalServiceAccountAnnotationKeys []string `json:"optionalServiceAccountAnnotationKeys,omitempty"`
}

// ExecEnvVar 用于给基于 exec 的凭据插件设置环境变量。
// 只描述需要注入的变量，值可为字面量；敏感信息应交给插件从安全来源读取。
type ExecEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
