//go:build !windows

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
	CRISocketContainerd = "unix:///var/run/containerd/containerd.sock"
	// CRISocketCRIO 是 cri-o 的 CRI 端点。
	CRISocketCRIO = "unix:///var/run/crio/crio.sock"
	// CRISocketDocker 是 cri-dockerd 的 CRI 端点。
	CRISocketDocker = "unix:///var/run/cri-dockerd.sock"

	// DefaultCRISocket 定义默认 CRI 套接字。
	DefaultCRISocket = CRISocketContainerd
)
