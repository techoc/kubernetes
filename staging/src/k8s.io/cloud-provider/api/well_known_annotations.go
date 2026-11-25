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

const (
	// AnnotationAlphaProvidedIPAddr 是由 "external" 云提供商设置的节点 IP 注解。
	// kubelet 以 "external" 云提供商启动时，会把 --node-ip 指定的地址写入该节点注解，
	// 表示这是一个来自命令行的 IP 地址；cloud-controller-manager 据此向云厂商校验该
	// IP 是否有效，再决定是否接受该地址。
	// 名称中的 alpha 前缀是历史兼容约定，不能随意更名，否则会破坏 kubelet 与 CCM 的对接。
	AnnotationAlphaProvidedIPAddr = "alpha.kubernetes.io/provided-node-ip"
)
