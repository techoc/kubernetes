//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package emptydir

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/volume"
)

// realMountDetector 通过系统调用实现 mountDetector。
type realMountDetector struct {
	mounter mount.Interface
}

// getPageSize 从卷挂载选项的 pagesize 中读取页大小。
func getPageSize(path string, mounter mount.Interface) (*resource.Quantity, error) {
	// 先获取该路径的挂载点信息
	mountPoints, err := mounter.List()
	if err != nil {
		return nil, fmt.Errorf("error listing mount points: %v", err)
	}
	// 从中找出对应该路径的挂载点
	mountPoint, err := func(mps []mount.MountPoint, mpPath string) (*mount.MountPoint, error) {
		for _, mp := range mps {
			if mp.Path == mpPath {
				return &mp, nil
			}
		}
		return nil, fmt.Errorf("mount point for %s not found", mpPath)
	}(mountPoints, path)
	if err != nil {
		return nil, err
	}
	// 从 pagesize 选项中提取页大小
	for _, opt := range mountPoint.Opts {
		opt = strings.TrimSpace(opt)
		prefix := "pagesize="
		if strings.HasPrefix(opt, prefix) {
			// NOTE: Adding suffix 'i' as result should be comparable with a medium size.
			// pagesize 挂载选项不带后缀，
			// e.g. pagesize=2M or pagesize=1024M for x86 CPUs
			trimmedOpt := strings.TrimPrefix(opt, prefix)
			if !strings.HasSuffix(trimmedOpt, "i") {
				trimmedOpt = trimmedOpt + "i"
			}
			pageSize, err := resource.ParseQuantity(trimmedOpt)
			if err != nil {
				return nil, fmt.Errorf("error getting page size from '%s' mount option: %v", opt, err)
			}
			return &pageSize, nil
		}
	}
	return nil, fmt.Errorf("no pagesize option specified for %s mount", mountPoint.Path)
}

// GetMountMedium 检测给定路径的挂载介质类型、是否已挂载及巨页大小。
func (m *realMountDetector) GetMountMedium(path string, requestedMedium v1.StorageMedium) (v1.StorageMedium, bool, *resource.Quantity, error) {
	klog.V(5).Infof("Determining mount medium of %v", path)
	notMnt, err := m.mounter.IsLikelyNotMountPoint(path)
	if err != nil {
		return v1.StorageMediumDefault, false, nil, fmt.Errorf("IsLikelyNotMountPoint(%q): %v", path, err)
	}

	buf := unix.Statfs_t{}
	if err := unix.Statfs(path, &buf); err != nil {
		return v1.StorageMediumDefault, false, nil, fmt.Errorf("statfs(%q): %v", path, err)
	}

	klog.V(3).Infof("Statfs_t of %v: %+v", path, buf)

	if buf.Type == unix.TMPFS_MAGIC {
		return v1.StorageMediumMemory, !notMnt, nil, nil
	} else if int64(buf.Type) == unix.HUGETLBFS_MAGIC {
		// 请求的介质未指定大小时，无需探测页大小
		if requestedMedium == v1.StorageMediumHugePages {
			return v1.StorageMediumHugePages, !notMnt, nil, nil
		}
		// 探测卷挂载使用的页大小
		pageSize, err := getPageSize(path, m.mounter)
		if err != nil {
			return v1.StorageMediumHugePages, !notMnt, nil, err
		}
		return v1.StorageMediumHugePages, !notMnt, pageSize, nil
	}
	return v1.StorageMediumDefault, !notMnt, nil, nil
}

// ResizeEphemeralVolume 在节点上调整临时卷大小。
func (plugin *emptyDirPlugin) ResizeEphemeralVolume(spec *volume.Spec, pod *v1.Pod, newSize *resource.Quantity) error {
	if spec.Volume == nil || spec.Volume.EmptyDir == nil {
		return fmt.Errorf("spec does not reference an emptyDir volume type")
	}
	if spec.Volume.EmptyDir.Medium != v1.StorageMediumMemory {
		return fmt.Errorf("only memory-backed emptyDir volumes support direct resize")
	}
	if newSize == nil || newSize.Value() == 0 {
		return fmt.Errorf("addition or removal of size limit is not supported")
	}

	dir := getPath(pod.UID, spec.Name(), plugin.host)
	mounter := plugin.host.GetMounter()

	var isNotMnt bool
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		isNotMnt = true
	} else {
		var err error
		isNotMnt, err = mounter.IsLikelyNotMountPoint(dir)
		if err != nil {
			return err
		}
	}

	if isNotMnt {
		return fmt.Errorf("volume %s is not yet mounted; deferring resize", spec.Name())
	}

	options := []string{"remount", fmt.Sprintf("size=%d", newSize.Value())}

	klog.V(2).InfoS("Resizing emptyDir volume", "pod", klog.KObj(pod), "volume", spec.Name(), "newSize", newSize)
	return mounter.MountSensitiveWithoutSystemd("tmpfs", dir, "tmpfs", options, nil)
}
