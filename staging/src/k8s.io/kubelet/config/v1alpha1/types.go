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

// 本包是 kubelet 配置组（kubelet.config.k8s.io）的 alpha 版本，
// 保留旧版配置的解码能力；字段语义与 v1 保持一致，升级时通过 scheme 转换迁移。
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderConfig 包含各 exec 凭据插件的配置。
// kubelet 从磁盘读取该配置，
// 并按 CredentialProvider 类型启用其中每个 provider。
// CredentialProviderConfig 是 kubelet 从磁盘读取的镜像凭证插件配置。
// 配置本身不参与 API server 存储，只在 kubelet 启动时加载，
// 因此使用 TypeMeta 而非 ObjectMeta。
type CredentialProviderConfig struct {
	metav1.TypeMeta `json:""`

	// providers 是 kubelet 要启用的凭据插件列表。
	// 同一镜像可能匹配多个 provider，此时它们的凭据都返回给 kubelet；
	// 若为一个镜像调用多个 provider，结果会合并；凭据键重叠时，
	// 列表中较靠前的 provider 优先尝试。
	Providers []CredentialProvider `json:"providers"`
}

// CredentialProvider 表示 kubelet 调用的 exec 插件。只有当被拉取镜像
// 匹配该插件处理的镜像时（见 matchImages）才会调用。
// CredentialProvider 描述一个可被 kubelet 调用的 exec 插件；
// 是否调用由 MatchImages 决定，避免每个镜像都启动外部进程。
type CredentialProvider struct {
	// name 是凭据插件的必填名字，必须与 kubelet 看到的插件可执行文件名一致，
	// 可执行文件必须位于 kubelet 的 bin 目录（由 --image-credential-provider-bin-dir 设置）。
	// 在所有 provider 中必须唯一。
	Name string `json:"name"`

	// matchImages 是必填的匹配串列表，用于判断是否为某镜像调用该插件。
	// 任一字符串匹配 kubelet 请求的镜像时，插件就会被调用并提供凭据。
	// 镜像名应包含 registry 域名与 URL 路径。
	// matchImages 每项都是可含端口与路径的模式串。
	// 通配符只用于域名，不用于端口和路径；支持子域
	// （`*.k8s.io`、`k8s.*.io`）与顶级域（`k8s.*`）。
	// 也支持部分子域匹配（`app*.k8s.io`）。每个通配符只匹配一段子域，
	// 因此 `*.io` 不匹配 `*.k8s.io`。
	// 镜像与 matchImage 匹配的条件是：
	// - 域名段数相同且逐段匹配；
	// - imageMatch 的 URL 路径必须是目标镜像路径的前缀；
	// - imageMatch 若含端口，镜像中也必须一致。
	// matchImages 示例值：
	// - `123456789.dkr.ecr.us-east-1.amazonaws.com`
	// - `*.azurecr.io`
	// - `gcr.io`
	// - `*.*.registry.io`
	// - `registry.io:8080/path`
	MatchImages []string `json:"matchImages"`

	// defaultCacheDuration 是插件未返回缓存时长时，kubelet 默认在内存中缓存凭据的时长，
	// 该字段必填。
	DefaultCacheDuration *metav1.Duration `json:"defaultCacheDuration"`

	// exec CredentialProviderRequest 要求的输入版本。返回的 CredentialProviderResponse
	// 必须使用与输入相同的编码版本。当前支持：
	// - credentialprovider.kubelet.k8s.io/v1alpha1
	APIVersion string `json:"apiVersion"`

	// 执行命令时传入的参数。
	// +optional
	Args []string `json:"args,omitempty"`

	// Env 定义暴露给进程的环境变量，与宿主环境及 client-go 传给插件的变量合并。
	// +optional
	Env []ExecEnvVar `json:"env,omitempty"`
}

// ExecEnvVar 用于给基于 exec 的凭据插件设置环境变量。
// ExecEnvVar 用 Name/Value 而非 KEY=VALUE 字符串，
// 便于 kubelet 安全地合并环境变量并检测重名。
type ExecEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ImagePullIntent 记录 kubelet 尝试拉取镜像的一次行为。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// ImagePullIntent 在发起拉取前落盘，记录 kubelet “打算拉哪个镜像”；
// 崩溃恢复时据此和 ImagePulledRecord 对比，判断拉取是否已完成。
type ImagePullIntent struct {
	metav1.TypeMeta `json:""`

	// Image 是容器 `image` 字段中的镜像名。
	// 文件名是该值的 SHA-256 哈希，以避免 ':'、'/' 等文件名不安全字符。
	//
	Image string `json:"image"`
}

// ImagePullRecord 记录 kubelet 拉取过一个镜像。
// 若 `kubernetesSecrets` 中没有记录，且 `nodeWideCredentials` 与
// `anonymous` 都为 `false`，下次请求该记录对应的镜像时必须重新校验凭据。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// ImagePulledRecord 保存一次成功拉取所用的凭证索引，
// 让后续拉取在凭证失效前无需再次调用外部插件。
type ImagePulledRecord struct {
	metav1.TypeMeta `json:""`

	// LastUpdatedTime 是该记录最后一次更新的时间。
	LastUpdatedTime metav1.Time `json:"lastUpdatedTime"`

	// ImageRef 是文件中记录的镜像引用（来自 CRI）。
	// 文件名是该值的 SHA-256 哈希，以避免 ':'、'/' 等文件名不安全字符。
	//
	ImageRef string `json:"imageRef"`

	// CredentialMapping 把 `image` 映射到之前拉取它时使用的凭据集合。
	// 这里的 `image` 是 Pod 容器 `image` 字段去掉 tag/digest 后的内容。
	// 示例：
	// 容器请求 `hello-world:latest@sha256:91fb4b041da273d5a3273b6d587d62d518300a6ad268b28628f74997b93171b2` 镜像：
	// "credentialMapping": {
	// "hello-world": { "nodePodsAccessible": true }
	//     }
	CredentialMapping map[string]ImagePullCredentials `json:"credentialMapping,omitempty"`
}

// ImagePullCredentials 描述可用于拉取镜像的凭据。
// ImagePullCredentials 只存凭证的坐标与哈希，不存明文凭证：
// 文件落盘后可安全读取，同时靠哈希判断 Secret 内容是否变化、缓存是否失效。
type ImagePullCredentials struct {
	// KuberneteSecretCoordinates 是拉取该镜像时使用过的所有 kubernetes secret 的坐标索引。
	// +optional
	// +listType=set
	KubernetesSecrets []ImagePullSecret `json:"kubernetesSecrets,omitempty"`

	// KubernetesServiceAccounts 是拉取该镜像时使用过的所有 kubernetes service account 的坐标索引。
	// +optional
	// +listType=set
	KubernetesServiceAccounts []ImagePullServiceAccount `json:"kubernetesServiceAccounts,omitempty"`

	// NodePodsAccessible 标志表示拉取凭据可被节点上所有 Pod 使用，或拉取不需要凭据。
	// 为 true 时与 `kubernetesSecrets` 字段互斥。
	// +optional
	NodePodsAccessible bool `json:"nodePodsAccessible,omitempty"`
}

// ImagePullSecret 表示 Kubernetes secret 的坐标，以及该 secret 中拉取凭据的哈希。
// ImagePullSecret 用 UID + Namespace + Name 唯一定位 Secret；
// 加上 CredentialHash 是为了在 Secret 被更新后让缓存的凭证自动失效。
type ImagePullSecret struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`

	// CredentialHash 是对 UID/Namespace/Name 指定 secret 中的拉取凭据内容做 SHA-256 得到的哈希。
	//
	CredentialHash string `json:"credentialHash"`
}

// ImagePullServiceAccount 表示 Kubernetes service account 的坐标：kubelet 曾为拉取凭据向凭据插件发送该账号的 token。
// ImagePullServiceAccount 记录曾把哪个 ServiceAccount 的 token 交给插件，
// 便于 token 轮换后按对象坐标重新获取，而不是盲目重试所有账号。
type ImagePullServiceAccount struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
