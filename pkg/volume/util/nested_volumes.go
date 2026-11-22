/*
Copyright 2018 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
)

// getNestedMountpoints 返回为指定卷需要创建的挂载点目录列表。
// 注意：返回的列表相对于 baseDir。
func getNestedMountpoints(name, baseDir string, pod v1.Pod) ([]string, error) {
	var retval []string
	checkContainer := func(container *v1.Container) error {
		var allMountPoints []string // 该容器内所有挂载点
		var myMountPoints []string  // 与 name 匹配的挂载点
		for _, vol := range container.VolumeMounts {
			cleaned := filepath.Clean(vol.MountPath)
			allMountPoints = append(allMountPoints, cleaned)
			if vol.Name == name {
				myMountPoints = append(myMountPoints, cleaned)
			}
		}
		sort.Strings(allMountPoints)
		parentPrefix := ".." + string(os.PathSeparator)
		// 检查该卷的每一处挂载位置
		for _, myMountPoint := range myMountPoints {
			if strings.HasPrefix(myMountPoint, parentPrefix) {
				// 防止容器借相对路径诱使我们在 rootfs 之外创建目录
				return fmt.Errorf("invalid container mount point %v", myMountPoint)
			}
			myMountPointWithSlash := myMountPoint + string(os.PathSeparator)
			// 记录此前已发现的嵌套挂载点。
			// 注意：不能只依赖 sort.Strings 让挂载点自然分组。例如以下字符串恰好按此顺序排列：
			// /dir/nested, /dir/nested-vol, /dir/nested.vol, /dir/nested/double, /dir/nested2
			// Windows 路径更麻烦，因为反斜杠的排序值大于正斜杠：
			// \dir\nested, \dir\nested-vol, \dir\nested.vol, \dir\nested2, \dir\nested\double
			// 因此需要用列表记录所有已发现的挂载点，而不是只记一个。
			prevNestedMPs := []string{}
			// 找出嵌套在该卷挂载点下的其他挂载点，但跳过双重嵌套者。
			// 例如本卷挂载在 /dir，其他卷挂载在 /dir/nested 和 /dir/nested/other，
			// 则只需创建 /dir/nested。
			for _, mp := range allMountPoints {
				if !strings.HasPrefix(mp, myMountPointWithSlash) {
					continue // 跳过：不在 myMountPoint 之下
				}

				isNested := false
				for _, prevNestedMP := range prevNestedMPs {
					if strings.HasPrefix(mp, prevNestedMP) {
						isNested = true
						break
					}
				}
				if isNested {
					continue // 跳过：在 myMountPoint 下双重嵌套
				}
				// 记住这个嵌套挂载点，以便检查后续挂载点是否嵌套在它之下
				prevNestedMPs = append(prevNestedMPs, mp+string(os.PathSeparator))
				retval = append(retval, mp[len(myMountPointWithSlash):])
			}
		}
		return nil
	}

	var retErr error
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *v1.Container, containerType podutil.ContainerType) bool {
		retErr = checkContainer(c)
		return retErr == nil
	})
	if retErr != nil {
		return nil, retErr
	}

	return retval, nil
}

// MakeNestedMountpoints 在 baseDir 下为挂载在 name 卷之内的其他卷创建挂载点。
func MakeNestedMountpoints(name, baseDir string, pod v1.Pod) error {
	dirs, err := getNestedMountpoints(name, baseDir, pod)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(baseDir, dir), 0755)
		if err != nil {
			return fmt.Errorf("unable to create nested volume mountpoints: %v", err)
		}
	}
	return nil
}
