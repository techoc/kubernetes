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
	"os"
	"strconv"

	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/exec"
)

// flexVolumeMounter 是该插件对外暴露的磁盘挂载器。
type flexVolumeMounter struct {
	*flexVolume
	// runner 用于执行驱动命令以配置卷。
	runner exec.Interface
	// spec 是待挂载卷的规格。
	spec     *volume.Spec
	readOnly bool
}

var _ volume.Mounter = &flexVolumeMounter{}

// 实现 volume.Mounter 接口。

// SetUp 在卷的默认路径上挂载卷。
func (f *flexVolumeMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return f.SetUpAt(f.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录上挂载卷。
func (f *flexVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	// 避免重复挂载同一目录。
	alreadyMounted, err := prepareForMount(f.mounter, dir)
	if err != nil {
		return err
	}
	if alreadyMounted {
		return nil
	}

	call := f.plugin.NewDriverCall(mountCmd)

	// 接口参数。
	call.Append(dir)

	extraOptions := make(map[string]string)

	// Pod 元数据。
	extraOptions[optionKeyPodName] = f.podName
	extraOptions[optionKeyPodNamespace] = f.podNamespace
	extraOptions[optionKeyPodUID] = string(f.podUID)
	// ServiceAccount 元数据。
	extraOptions[optionKeyServiceAccountName] = f.podServiceAccountName

	// 提取 secret 并作为选项传给驱动。
	if err := addSecretsToOptions(extraOptions, f.spec, f.podNamespace, f.driverName, f.plugin.host); err != nil {
		os.Remove(dir)
		return err
	}

	// 隐式参数。
	if mounterArgs.FsGroup != nil {
		extraOptions[optionFSGroup] = strconv.FormatInt(int64(*mounterArgs.FsGroup), 10)
	}

	call.AppendSpec(f.spec, f.plugin.host, extraOptions)

	_, err = call.Run()
	if isCmdNotSupportedErr(err) {
		err = (*mounterDefaults)(f).SetUpAt(dir, mounterArgs)
	}

	if err != nil {
		os.Remove(dir)
		return err
	}

	if !f.readOnly {
		if f.plugin.capabilities.FSGroup {
			// 完整插件名有助于区分 flexvolume 插件与不同驱动。
			ownershipChanger := volume.NewVolumeOwnership(f, dir, mounterArgs.FsGroup, mounterArgs.FSGroupChangePolicy, util.FSGroupCompleteHook(f.plugin, f.spec))
			_ = ownershipChanger.ChangePermissions()
		}
	}

	return nil
}

// GetAttributes 获取 flexvolume 的属性。待回调语法确定后，
// 这些属性将通过插件回调查询。
func (f *flexVolumeMounter) GetAttributes() volume.Attributes {
	return (*mounterDefaults)(f).GetAttributes()
}
