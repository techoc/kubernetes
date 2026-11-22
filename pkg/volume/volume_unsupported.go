//go:build !linux

/*
Copyright 2016 The Kubernetes Authors.

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

package volume

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// NewVolumeOwnership 返回卷属组修改器；非 Linux 平台不支持递归改权限，这里返回空实现。
// 调用方无需感知平台差异，因此统一提供同样的接口。
func NewVolumeOwnership(mounter Mounter, dir string, fsGroup *int64, fsGroupChangePolicy *v1.PodFSGroupChangePolicy, completeFunc func(types.CompleteFuncParam)) VolumeOwnershipChanger {
	return &VolumeOwnership{}
}

// AddProgressNotifier 在非 Linux 平台上是空操作，保持与 Linux 实现相同的调用方式。
func (vo *VolumeOwnership) AddProgressNotifier(pod *v1.Pod, recorder record.EventRecorder) VolumeOwnershipChanger {
	return vo
}

// ChangePermissions 在非 Linux 平台上直接返回成功，不修改任何文件。
func (vo *VolumeOwnership) ChangePermissions() error {
	return nil
}
