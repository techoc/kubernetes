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

// +k8s:deepcopy-gen=package

// Package metadata contains internal (unversioned) types for DRA device
// metadata. This metadata consists of device attributes that are similar, but
// not necessarily identical to the ones found in ResourceSlices. DRA drivers
// publish this information for consumption inside the containers which use
// devices.
//
// These types are the canonical in-memory representation that Go
// consumers program against. Versioned types (e.g. v1alpha1) are converted
// to/from these internal types via the scheme.
//
// # In-container metadata API
//
// Device metadata files are mounted read-only into every container that
// references one or more ResourceClaims, provided that the DRA drivers which
// allocate the devices support the feature.
//
// The directory layout distinguishes between two kinds of claims because
// consumers may have to be configured at the time when the Pod spec is
// created, i.e. without access to the Pod status where the mapping from
// ResourceClaimTemplate to ResourceClaim is stored at runtime:
//
//   - Directly referenced claims (pod.spec.resourceClaims[].resourceClaimName
//     is set) are stored under the [ResourceClaimsSubDir] subdirectory,
//     keyed by the ResourceClaim name, which the consumer can read from
//     the pod spec:
//
//     /var/run/kubernetes.io/dra-device-attributes/resourceclaims/<claimName>/<requestName>/<driverName>-metadata.json
//
//   - Template-generated claims (pod.spec.resourceClaims[].resourceClaimTemplateName
//     was set) are stored under the [ResourceClaimTemplatesSubDir] subdirectory,
//     keyed by the pod-local claim name (pod.spec.resourceClaims[].name).
//     The actual ResourceClaim name is generated and not predictable from
//     the pod spec, so the pod-local name is used instead:
//
//     /var/run/kubernetes.io/dra-device-attributes/resourceclaimtemplates/<podClaimName>/<requestName>/<driverName>-metadata.json
//
// DRA drivers can derive these file names from the information which is available
// to them through the ResourceClaims, without relying on the Pod.
//
// The <driverName> in the file name is the DRA driver name (e.g.,
// "gpu.example.com"). Each driver writes its own file, so multiple drivers
// serving the same request do not collide.
//
// The <requestName> segment corresponds to an entry in the claim's
// spec.devices.requests[] list. For requests that use FirstAvailable with
// subrequests, the subrequest name will be removed and only request name will be used
//
// # Accessing metadata files
//
// Individual metadata files are bind-mounted read-only. Consumers should
// construct the exact file paths from known claim names and request names
// rather than enumerating directories.
//
// # File format
//
// Each metadata.json file is a concatenated JSON stream containing the same
// [DeviceMetadata] object encoded once per supported API version, newest
// first. This ensures forward and backward compatibility: older consumers
// skip versions they do not recognise and newer consumers can always find a
// version they support. Standard JSON decoders can read the objects one by
// one; see the k8s.io/dynamic-resource-allocation/devicemetadata package
// for a ready-made decoder.
//
// 本包包含 DRA（动态资源分配）设备元数据的内部（无版本）类型。这些元数据由设备属性组成，
// 与 ResourceSlice 中的属性相似但不完全相同。DRA 驱动发布这些信息，供使用设备的容器内部消费。
//
// 这些类型是 Go 消费者编程所面向的规范内存表示。版本化类型（例如 v1alpha1）通过 scheme
// 与这些内部类型相互转换。
//
// # 容器内元数据 API
//
// 只要分配设备的 DRA 驱动支持该功能，设备元数据文件就会以只读方式挂载到每一个引用了
// ResourceClaim 的容器中。
//
// 目录布局之所以区分两类 claim，是因为消费者可能需要在创建 Pod spec 时就完成配置，
// 而此时还无法访问 Pod status —— ResourceClaimTemplate 到 ResourceClaim 的运行时映射就存在那里：
//
//   - 直接引用的 claim（设置了 pod.spec.resourceClaims[].resourceClaimName）存放在
//     [ResourceClaimsSubDir] 子目录下，以 ResourceClaim 名称作为键，消费者可以直接从 pod spec 中读到该名称：
//
//     /var/run/kubernetes.io/dra-device-attributes/resourceclaims/<claimName>/<requestName>/<driverName>-metadata.json
//
//   - 模板生成的 claim（设置的是 pod.spec.resourceClaims[].resourceClaimTemplateName）存放在
//     [ResourceClaimTemplatesSubDir] 子目录下，以 Pod 本地的 claim 名称（pod.spec.resourceClaims[].name）作为键。
//     真实的 ResourceClaim 名称是运行时生成的，无法从 pod spec 预测，因此改用 Pod 本地名称：
//
//     /var/run/kubernetes.io/dra-device-attributes/resourceclaimtemplates/<podClaimName>/<requestName>/<driverName>-metadata.json
//
// DRA 驱动可以根据 ResourceClaim 中可用的信息推导出这些文件名，无需依赖 Pod。
//
// 文件名中的 <driverName> 是 DRA 驱动名称（例如 "gpu.example.com"）。每个驱动写自己的文件，
// 因此为同一个 request 服务的多个驱动之间不会发生冲突。
//
// <requestName> 段对应 claim 的 spec.devices.requests[] 列表中的一项。对于使用 FirstAvailable
// 并带有子请求的 request，子请求名称会被去掉，只保留 request 名称。
//
// # 访问元数据文件
//
// 单个元数据文件以只读 bind mount 方式挂载。消费者应当根据已知的 claim 名称和 request 名称
// 构造精确的文件路径，而不是去枚举目录。
//
// # 文件格式
//
// 每个 metadata.json 文件都是一个拼接（concatenated）的 JSON 流，其中同一个 [DeviceMetadata]
// 对象按受支持的每个 API 版本各编码一次，最新的版本排在最前面。这样既保证向前兼容也保证向后兼容：
// 较旧的消费者会跳过自己不认识的版本，较新的消费者总能找到自己支持的版本。标准 JSON 解码器可以
// 逐个读取这些对象；k8s.io/dynamic-resource-allocation/devicemetadata 包提供了一个现成的解码器。
package metadata
