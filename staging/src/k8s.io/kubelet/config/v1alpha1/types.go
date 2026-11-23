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

// CredentialProviderConfig is the configuration containing information about
// each exec credential provider. Kubelet reads this configuration from disk and enables
// each provider as specified by the CredentialProvider type.
//
// CredentialProviderConfig 是 kubelet 从磁盘读取的镜像凭证插件配置。
// 配置本身不参与 API server 存储，只在 kubelet 启动时加载，
// 因此使用 TypeMeta 而非 ObjectMeta。
type CredentialProviderConfig struct {
	metav1.TypeMeta `json:""`

	// providers is a list of credential provider plugins that will be enabled by the kubelet.
	// Multiple providers may match against a single image, in which case credentials
	// from all providers will be returned to the kubelet. If multiple providers are called
	// for a single image, the results are combined. If providers return overlapping
	// auth keys, the value from the provider earlier in this list is attempted first.
	Providers []CredentialProvider `json:"providers"`
}

// CredentialProvider represents an exec plugin to be invoked by the kubelet. The plugin is only
// invoked when an image being pulled matches the images handled by the plugin (see matchImages).
//
// CredentialProvider 描述一个可被 kubelet 调用的 exec 插件；
// 是否调用由 MatchImages 决定，避免每个镜像都启动外部进程。
type CredentialProvider struct {
	// name is the required name of the credential provider. It must match the name of the
	// provider executable as seen by the kubelet. The executable must be in the kubelet's
	// bin directory (set by the --image-credential-provider-bin-dir flag).
	// Required to be unique across all providers.
	Name string `json:"name"`

	// matchImages is a required list of strings used to match against images in order to
	// determine if this provider should be invoked. If one of the strings matches the
	// requested image from the kubelet, the plugin will be invoked and given a chance
	// to provide credentials. Images are expected to contain the registry domain
	// and URL path.
	//
	// Each entry in matchImages is a pattern which can optionally contain a port and a path.
	// Globs can be used in the domain, but not in the port or the path. Globs are supported
	// as subdomains like `*.k8s.io` or `k8s.*.io`, and top-level-domains such as `k8s.*`.
	// Matching partial subdomains like `app*.k8s.io` is also supported. Each glob can only match
	// a single subdomain segment, so `*.io` does not match `*.k8s.io`.
	//
	// A match exists between an image and a matchImage when all of the below are true:
	// - Both contain the same number of domain parts and each part matches.
	// - The URL path of an imageMatch must be a prefix of the target image URL path.
	// - If the imageMatch contains a port, then the port must match in the image as well.
	//
	// Example values of matchImages:
	//   - `123456789.dkr.ecr.us-east-1.amazonaws.com`
	//   - `*.azurecr.io`
	//   - `gcr.io`
	//   - `*.*.registry.io`
	//   - `registry.io:8080/path`
	MatchImages []string `json:"matchImages"`

	// defaultCacheDuration is the default duration the plugin will cache credentials in-memory
	// if a cache duration is not provided in the plugin response. This field is required.
	DefaultCacheDuration *metav1.Duration `json:"defaultCacheDuration"`

	// Required input version of the exec CredentialProviderRequest. The returned CredentialProviderResponse
	// MUST use the same encoding version as the input. Current supported values are:
	// - credentialprovider.kubelet.k8s.io/v1alpha1
	APIVersion string `json:"apiVersion"`

	// Arguments to pass to the command when executing it.
	// +optional
	Args []string `json:"args,omitempty"`

	// Env defines additional environment variables to expose to the process. These
	// are unioned with the host's environment, as well as variables client-go uses
	// to pass argument to the plugin.
	// +optional
	Env []ExecEnvVar `json:"env,omitempty"`
}

// ExecEnvVar is used for setting environment variables when executing an exec-based
// credential plugin.
//
// ExecEnvVar 用 Name/Value 而非 KEY=VALUE 字符串，
// 便于 kubelet 安全地合并环境变量并检测重名。
type ExecEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ImagePullIntent is a record of the kubelet attempting to pull an image.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
//
// ImagePullIntent 在发起拉取前落盘，记录 kubelet “打算拉哪个镜像”；
// 崩溃恢复时据此和 ImagePulledRecord 对比，判断拉取是否已完成。
type ImagePullIntent struct {
	metav1.TypeMeta `json:""`

	// Image is the image spec from a Container's `image` field.
	// The filename is a SHA-256 hash of this value. This is to avoid filename-unsafe
	// characters like ':' and '/'.
	Image string `json:"image"`
}

// ImagePullRecord is a record of an image that was pulled by the kubelet.
//
// If there are no records in the `kubernetesSecrets` field and both `nodeWideCredentials`
// and `anonymous` are `false`, credentials must be re-checked the next time an
// image represented by this record is being requested.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
//
// ImagePulledRecord 保存一次成功拉取所用的凭证索引，
// 让后续拉取在凭证失效前无需再次调用外部插件。
type ImagePulledRecord struct {
	metav1.TypeMeta `json:""`

	// LastUpdatedTime is the time of the last update to this record
	LastUpdatedTime metav1.Time `json:"lastUpdatedTime"`

	// ImageRef is a reference to the image represented by this file as received
	// from the CRI.
	// The filename is a SHA-256 hash of this value. This is to avoid filename-unsafe
	// characters like ':' and '/'.
	ImageRef string `json:"imageRef"`

	// CredentialMapping maps `image` to the set of credentials that it was
	// previously pulled with.
	// `image` in this case is the content of a pod's container `image` field that's
	// got its tag/digest removed.
	//
	// Example:
	//   Container requests the `hello-world:latest@sha256:91fb4b041da273d5a3273b6d587d62d518300a6ad268b28628f74997b93171b2` image:
	//     "credentialMapping": {
	//       "hello-world": { "nodePodsAccessible": true }
	//     }
	CredentialMapping map[string]ImagePullCredentials `json:"credentialMapping,omitempty"`
}

// ImagePullCredentials describe credentials that can be used to pull an image.
//
// ImagePullCredentials 只存凭证的坐标与哈希，不存明文凭证：
// 文件落盘后可安全读取，同时靠哈希判断 Secret 内容是否变化、缓存是否失效。
type ImagePullCredentials struct {
	// KuberneteSecretCoordinates is an index of coordinates of all the kubernetes
	// secrets that were used to pull the image.
	// +optional
	// +listType=set
	KubernetesSecrets []ImagePullSecret `json:"kubernetesSecrets,omitempty"`

	// KubernetesServiceAccounts is an index of coordinates of all the kubernetes
	// service accounts that were used to pull the image.
	// +optional
	// +listType=set
	KubernetesServiceAccounts []ImagePullServiceAccount `json:"kubernetesServiceAccounts,omitempty"`

	// NodePodsAccessible is a flag denoting the pull credentials are accessible
	// by all the pods on the node, or that no credentials are needed for the pull.
	//
	// If true, it is mutually exclusive with the `kubernetesSecrets` field.
	// +optional
	NodePodsAccessible bool `json:"nodePodsAccessible,omitempty"`
}

// ImagePullSecret is a representation of a Kubernetes secret object coordinates along
// with a credential hash of the pull secret credentials this object contains.
//
// ImagePullSecret 用 UID + Namespace + Name 唯一定位 Secret；
// 加上 CredentialHash 是为了在 Secret 被更新后让缓存的凭证自动失效。
type ImagePullSecret struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`

	// CredentialHash is a SHA-256 retrieved by hashing the image pull credentials
	// content of the secret specified by the UID/Namespace/Name coordinates.
	CredentialHash string `json:"credentialHash"`
}

// ImagePullServiceAccount is a representation of a Kubernetes service account object coordinates
// for which the kubelet sent service account token to the credential provider plugin for image pull credentials.
//
// ImagePullServiceAccount 记录曾把哪个 ServiceAccount 的 token 交给插件，
// 便于 token 轮换后按对象坐标重新获取，而不是盲目重试所有账号。
type ImagePullServiceAccount struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
