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

package endpointslicemirroring

const (
	// FailedToListEndpointSlices 表示控制器列举 EndpointSlice 失败。
	// 该事件用于提示同步无法获取期望状态，需排查 API 连通性或权限。
	FailedToListEndpointSlices = "FailedToListEndpointSlices"
	// FailedToUpdateEndpointSlices 表示控制器更新 EndpointSlice 失败。
	// 出现该事件时镜像数据可能落后于 Endpoints，需关注写入冲突或配额限制。
	FailedToUpdateEndpointSlices = "FailedToUpdateEndpointSlices"
	// InvalidIPAddress 表示 Endpoints 资源中发现的 IP 地址无效。
	// 无效地址不会被镜像，事件用于提醒用户修正 Endpoints 内容。
	InvalidIPAddress = "InvalidIPAddress"
	// TooManyAddressesToMirror 表示部分地址未被镜像。
	// 原因是某个 EndpointSubset 中待镜像的地址数超过 MaxEndpointsPerSubset 上限，
	// 属于截断行为，需提示用户调整 Endpoints 规模或配置。
	TooManyAddressesToMirror = "TooManyAddressesToMirror"
)
