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

package apps

import (
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// NOTE(claudiub): 这些常量不应作为 Pod 的容器镜像使用。
const (
	AgnhostImageName = "agnhost"
)

var (
	// PrevAgnhostImage 是 AgnhostPrev 镜像的完整限定 URI
	PrevAgnhostImage = imageutils.GetE2EImage(imageutils.AgnhostPrev)

	// AgnhostImage 是 Agnhost 镜像的完整限定 URI
	AgnhostImage = imageutils.GetE2EImage(imageutils.Agnhost)
)
