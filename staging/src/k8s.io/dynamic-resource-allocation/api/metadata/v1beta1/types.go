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

package v1beta1

import (
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DeviceMetadata contains metadata about devices allocated to a ResourceClaim.
// It is serialized to versioned JSON files that can be mounted into containers.
//
// DeviceMetadata 描述分配给某个 ResourceClaim 的设备元数据；它会序列化为带版本的
// JSON 文件，供容器挂载使用。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DeviceMetadata struct {
	// TypeMeta 携带 apiVersion/kind，用于反序列化时识别对象类型。
	// ObjectMeta 携带 name、namespace 等对象元信息。
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// PodClaimName is the name used to reference this claim in the pod spec
	// (i.e., the entry in pod.spec.resourceClaims[].name). For claims
	// created from a ResourceClaimTemplate, this differs from the actual
	// ResourceClaim name and is needed to resolve the mapping.
	//
	// PodClaimName 是 Pod spec 中引用该 claim 的名称，即
	// pod.spec.resourceClaims[].name。由 ResourceClaimTemplate 创建的 claim
	// 该名称与实际 ResourceClaim 名称不同，需要借助它完成映射解析。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-short-name
	PodClaimName *string `json:"podClaimName,omitempty"`

	// Requests contains the device allocation information for each request
	// in the ResourceClaim.
	//
	// Requests 按 ResourceClaim 中的每个请求记录对应的设备分配信息。
	//
	// +optional
	// +k8s:optional
	Requests []DeviceMetadataRequest `json:"requests,omitempty"`
}

// DeviceMetadataRequest contains metadata for a single request within a ResourceClaim.
//
// DeviceMetadataRequest 保存 ResourceClaim 中单个请求对应的设备元数据。
type DeviceMetadataRequest struct {
	// Name is the request name from the ResourceClaim spec. For requests
	// using FirstAvailable with subrequests, this includes the subrequest
	// name in the format "<request>/<subrequest>", matching the format
	// used in DeviceRequestAllocationResult.Request
	//
	// Name 是 ResourceClaim spec 中的请求名。使用 FirstAvailable 子请求时，
	// 格式为 "<request>/<subrequest>"，与 DeviceRequestAllocationResult.Request
	// 中的格式保持一致。
	//
	// +required
	// +k8s:required
	Name string `json:"name"`

	// Devices contains metadata for each device allocated to this request.
	//
	// Devices 列出分配给该请求的所有设备及其元数据。
	//
	// +optional
	// +k8s:optional
	Devices []Device `json:"devices,omitempty"`
}

// Device contains metadata about a single allocated device.
//
// Device 描述单个已分配设备的元数据。
type Device struct {
	// Driver is the name of the DRA driver that manages this device.
	// It must be a DNS subdomain no longer than 63 characters.
	//
	// Driver 是管理该设备的 DRA 驱动名称，必须是长度不超过 63 个字符的 DNS 子域。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-long-name-caseless
	// +k8s:maxLength=63
	Driver string `json:"driver"`

	// Pool is the name of the resource pool this device belongs to.
	// It must be one or more DNS subdomains separated by slashes and must
	// not be longer than 253 characters.
	//
	// Pool 是设备所属资源池的名称，由若干 DNS 子域以斜杠分隔，长度不超过 253 个字符。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-resource-pool-name
	Pool string `json:"pool"`

	// Name is the name of the device within the pool.
	// It must be a DNS label.
	//
	// Name 是设备在资源池内的名称，必须是 DNS label（短名称）。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name"`

	// Attributes contains device attributes in the same format as a ResourceSlice.
	// Keys are qualified attribute names (e.g., "model", "resource.k8s.io/pciBusID").
	// There can be at most 32 attributes.
	//
	// Attributes 保存设备属性，格式与 ResourceSlice 中的属性一致：键为限定名，
	// 例如 "model" 或 "resource.k8s.io/pciBusID"，最多允许 32 个属性。
	//
	// +optional
	// +k8s:optional
	// +k8s:maxProperties=32
	// +k8s:customValidation
	// +k8s:eachVal=+k8s:opaqueType
	Attributes map[resourceapi.QualifiedName]resourceapi.DeviceAttribute `json:"attributes,omitempty"`

	// NetworkData contains network-specific device data (e.g., interface name,
	// addresses, hardware address). This is populated for network devices,
	// typically during the CRI RPC RunPodSandbox, before the containers are
	// started and after the network namespace is created.
	//
	// NetworkData 保存网络设备的专有数据（接口名、地址、硬件地址等）；它通常在
	// CRI 的 RunPodSandbox 调用期间填充，即网络命名空间创建后、容器启动前。
	//
	// +optional
	// +k8s:optional
	// +k8s:customValidation
	// +k8s:opaqueType
	NetworkData *resourceapi.NetworkDeviceData `json:"networkData,omitempty"`
}
