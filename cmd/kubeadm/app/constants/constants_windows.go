//go:build windows

/*
Copyright 2019 The Kubernetes Authors.

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

package constants

const (
	// CRISocketContainerd 是 containerd 的 CRI 端点。
	CRISocketContainerd = "npipe:////./pipe/containerd-containerd"
	// CRISocketCRIO 是 cri-o 的 CRI 端点。
	// 注意：由于 CRI-O 不支持 Windows，此值仅为占位符。
	CRISocketCRIO = "npipe:////./pipe/cri-o"
	// CRISocketDocker 是 cri-dockerd 的 CRI 端点。
	CRISocketDocker = "npipe:////./pipe/cri-dockerd"

	// DefaultCRISocket 定义默认 CRI 套接字。
	DefaultCRISocket = CRISocketContainerd
)
