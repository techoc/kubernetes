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

package flexvolume

import (
	"fmt"
	"os"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"

	"k8s.io/kubernetes/pkg/volume"
)

// flexVolumeUnmounter 表示由该插件负责清理的磁盘卷。
type flexVolumeUnmounter struct {
	*flexVolume
	// 用于卸载卷的命令执行器。
	runner exec.Interface
}

// 编译时断言 flexVolumeUnmounter 实现 volume.Unmounter 接口。
var _ volume.Unmounter = &flexVolumeUnmounter{}

// TearDown 卸载插件挂载目录下的卷。
func (f *flexVolumeUnmounter) TearDown() error {
	path := f.GetPath()
	return f.TearDownAt(path)
}

// TearDownAt 调用驱动的 unmount 命令卸载指定目录，并在驱动未删除目录时清理它。
// 目录不存在视为已卸载，直接返回成功（卸载操作需可重复执行）。
func (f *flexVolumeUnmounter) TearDownAt(dir string) error {
	pathExists, pathErr := mount.PathExists(dir)
	if pathErr != nil {
		// 检查路径失败仅记录警告：驱动自身也需要处理错误，
		// 此处不阻断卸载尝试，交由驱动报告实际问题。
		klog.Warningf("Error checking path: %v", pathErr)
	} else {
		if !pathExists {
			klog.Warningf("Unmount skipped because path does not exist: %v", dir)
			return nil
		}
	}

	call := f.plugin.NewDriverCall(unmountCmd)
	call.Append(dir)
	_, err := call.Run()
	if isCmdNotSupportedErr(err) {
		err = (*unmounterDefaults)(f).TearDownAt(dir)
	}
	if err != nil {
		return err
	}

	// 驱动可能已自行删除目录，此时无需再清理。
	if pathExists, pathErr := mount.PathExists(dir); pathErr != nil {
		return fmt.Errorf("error checking if path exists: %w", pathErr)
	} else if !pathExists {
		return nil
	}
	return os.Remove(dir)
}
