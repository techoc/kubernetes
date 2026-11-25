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

package framework

// NOTE: 本文件中的常量复制自 pkg/cluster/ports/ports.go

const (
	// KubeletPort 是每台主机上 kubelet server 的默认端口。
	// 启动时可通过 flag 覆盖。
	KubeletPort = 10250
	// KubeControllerManagerPort 是 controller manager status server 的默认端口。
	// 启动时可通过 flag 覆盖。
	KubeControllerManagerPort = 10257
)
