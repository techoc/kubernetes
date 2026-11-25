/*
Copyright The Kubernetes Authors.

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

package v1alpha1

import (
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DeviceMetadata 包含分配给某个 ResourceClaim 的设备的元数据。它会被序列化为带版本的 JSON 文件，
// 这些文件可以被挂载到容器中。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DeviceMetadata struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// PodClaimName 是在 pod spec 中引用该 claim 时使用的名称（即 pod.spec.resourceClaims[].name 中的条目）。
	// 对于由 ResourceClaimTemplate 创建的 claim，它与真实的 ResourceClaim 名称不同，
	// 因此需要该字段来解析两者之间的映射关系。格式约束为 k8s-short-name（合法的短名称）。
	// +k8s:optional
	// +k8s:format=k8s-short-name
	PodClaimName *string `json:"podClaimName,omitempty"`

	// Requests 包含 ResourceClaim 中每个 request 的设备分配信息。
	// +k8s:optional
	Requests []DeviceMetadataRequest `json:"requests,omitempty"`
}

// DeviceMetadataRequest 包含 ResourceClaim 中单个 request 的元数据。
type DeviceMetadataRequest struct {
	// Name 是 ResourceClaim spec 中的 request 名称。对于使用 FirstAvailable 且带有子请求的
	// request，该字段会以 "<request>/<subrequest>" 的格式包含子请求名称，
	// 与 DeviceRequestAllocationResult.Request 中使用的格式一致。该字段必填。
	// +k8s:required
	Name string `json:"name"`

	// Devices 包含分配给该 request 的每个设备的元数据。
	// +k8s:optional
	Devices []Device `json:"devices,omitempty"`
}

// Device 包含单个已分配设备的元数据。
type Device struct {
	// Driver 是管理该设备的 DRA 驱动名称，必须是长度不超过 63 个字符的 DNS 子域。
	// 由下面的标记强制校验：必填、k8s-long-name-caseless 格式、最大长度 63。
	// +k8s:required
	// +k8s:format=k8s-long-name-caseless
	// +k8s:maxLength=63
	Driver string `json:"driver"`

	// Pool 是该设备所属资源池的名称，必须是由斜杠分隔的一个或多个 DNS 子域，
	// 且总长度不能超过 253 个字符（k8s-resource-pool-name 格式，必填）。
	// +k8s:required
	// +k8s:format=k8s-resource-pool-name
	Pool string `json:"pool"`

	// Name 是该设备在资源池内的名称，必须是一个 DNS label（k8s-short-name 格式，必填）。
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name"`

	// Attributes 包含与 ResourceSlice 相同格式的设备属性。
	// 键是限定名形式的属性名（例如 "model"、"resource.k8s.io/pciBusID"），最多 32 个属性。
	// 下方标记含义：可选、最多 32 个键、使用自定义校验（ValidateCustom_Device_Attributes），
	// 每个值按 opaqueType 处理（由自定义校验负责逐值检查）。
	// +k8s:optional
	// +k8s:maxProperties=32
	// +k8s:customValidation
	// +k8s:eachVal=+k8s:opaqueType
	Attributes map[resourceapi.QualifiedName]resourceapi.DeviceAttribute `json:"attributes,omitempty"`

	// NetworkData 包含网络设备专属的数据（例如接口名、地址、硬件地址）。
	// 它只对网络设备填充，通常在 CRI 的 RunPodSandbox RPC 期间写入，
	// 即容器启动之前、网络命名空间创建之后。该字段可选，并使用自定义校验
	// （ValidateCustom_Device_NetworkData），按 opaqueType 处理。
	// +k8s:optional
	// +k8s:customValidation
	// +k8s:opaqueType
	NetworkData *resourceapi.NetworkDeviceData `json:"networkData,omitempty"`
}
