/*
Copyright 2024 The Kubernetes Authors.

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

// 本包定义 DRA（动态资源分配）插件 gRPC API 的稳定版本常量。
// 这里的字符串是插件注册握手时的版本标识，写错会导致插件与 kubelet 无法配对。
package v1

// DRA 插件通过 DRAPluginService 声明自己实现的 gRPC 服务版本，
// kubelet 据此在多个版本中选择双方都支持的协议。
const (
	// DRAPluginService needs to be listed in the "supported versions"
	// array during plugin registration by a DRA plugin which provides
	// an implementation of the v1 DRAPlugin service.
	//
	// DRAPluginService 必须出现在插件的 supportedVersions 列表中，
	// 否则 kubelet 会认为该插件不支持 v1 而跳过握手。
	DRAPluginService = "v1.DRAPlugin"
)
