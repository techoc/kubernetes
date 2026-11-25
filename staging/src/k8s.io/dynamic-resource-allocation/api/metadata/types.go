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

package metadata

import (
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 以下常量定义了设备元数据文件在容器内的目录布局。它们属于面向消费者的 API 的一部分：
// 修改其中任何一个值，对于读取元数据文件的应用来说都是破坏性变更。
const (
	// ContainerDir 是容器内设备元数据文件挂载的约定目录（well-known directory）。
	ContainerDir = "/var/run/kubernetes.io/dra-device-attributes"

	// ResourceClaimsSubDir 是 ContainerDir 下用于存放直接引用的 ResourceClaim 的子目录
	// （即在 pod.spec.resourceClaims[].resourceClaimName 中按名称列出的那些 claim）。
	// claim 名称会作为下一级路径段。
	ResourceClaimsSubDir = "resourceclaims"

	// ResourceClaimTemplatesSubDir 是 ContainerDir 下用于存放由 ResourceClaimTemplate
	// 创建出来的 claim 的子目录。下一级路径段使用 podClaimName，
	// 即 pod.spec.resourceClaims[].name（Pod 本地的 claim 名称）。
	ResourceClaimTemplatesSubDir = "resourceclaimtemplates"

	// MetadataFileSuffix 会追加到驱动名称之后，构成每个 request 目录下的元数据文件名。
	// 完整文件名为 "{driverName}-metadata.json"；使用驱动名称作为前缀可以避免
	// 多个驱动为同一个 request 服务时发生文件名冲突。
	MetadataFileSuffix = "-metadata.json"
)

// DeviceMetadata 包含分配给某个 ResourceClaim 的设备的元数据。它会被序列化为带版本的 JSON 文件，
// 并挂载到 [ContainerDir] 下的容器中。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DeviceMetadata struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// PodClaimName 是在 pod spec 中引用该 claim 时使用的名称（即 pod.spec.resourceClaims[].name 中的条目）。
	// 对于由 ResourceClaimTemplate 创建的 claim 会设置该字段，否则为 nil。设置时元数据文件放在
	// [ResourceClaimTemplatesSubDir] 下，否则放在 [ResourceClaimsSubDir] 下。
	PodClaimName *string

	// Requests 包含 ResourceClaim 中每个 request 的设备分配信息。
	Requests []DeviceMetadataRequest
}

// DeviceMetadataRequest 包含 ResourceClaim 中单个 request 的元数据。
type DeviceMetadataRequest struct {
	// Name 是 ResourceClaim spec 中的 request 名称。对于使用 FirstAvailable 且带有子请求的
	// request，该字段会以 "<request>/<subrequest>" 的格式包含子请求名称，
	// 与 DeviceRequestAllocationResult.Request 中使用的格式一致。
	Name string

	// Devices 包含分配给该 request 的每个设备的元数据。
	Devices []Device
}

// Device 包含单个已分配设备的元数据。
type Device struct {
	// Driver 是管理该设备的 DRA 驱动的名称。
	Driver string

	// Pool 是该设备所属资源池的名称。
	Pool string

	// Name 是该设备在资源池内的名称。
	Name string

	// Attributes 包含与 ResourceSlice 相同格式的设备属性。
	// 键是限定名（qualified name）形式的属性名，例如 "model"、"resource.k8s.io/pciBusID"。
	Attributes map[resourceapi.QualifiedName]resourceapi.DeviceAttribute

	// NetworkData 包含网络设备专属的数据（例如接口名、地址、硬件地址）。
	// 该字段仅对网络设备填充，通常在 CRI 的 RunPodSandbox RPC 期间、
	// 容器启动之前且网络命名空间创建之后写入。
	NetworkData *resourceapi.NetworkDeviceData
}
