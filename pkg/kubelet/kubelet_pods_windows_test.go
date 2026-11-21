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

package kubelet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestMakeMountsWindows 验证 Windows 下容器挂载路径的规范化与 etc-hosts 挂载生成。
func TestMakeMountsWindows(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// TODO: remove skip once the failing test has been fixed.
	// 已知失败用例，先跳过以免阻塞 CI。
	t.Skip("Skip failing test on Windows.")
	// 构造覆盖正斜杠、反斜杠、盘符与命名管道路径的挂载点。
	container := v1.Container{
		VolumeMounts: []v1.VolumeMount{
			{
				MountPath: "c:/etc/hosts",
				Name:      "disk",
				ReadOnly:  false,
			},
			{
				MountPath: "c:/mnt/path3",
				Name:      "disk",
				ReadOnly:  true,
			},
			{
				MountPath: "c:/mnt/path4",
				Name:      "disk4",
				ReadOnly:  false,
			},
			{
				MountPath: "c:/mnt/path5",
				Name:      "disk5",
				ReadOnly:  false,
			},
			{
				MountPath: `\mnt\path6`,
				Name:      "disk6",
				ReadOnly:  false,
			},
			{
				MountPath: `/mnt/path7`,
				Name:      "disk7",
				ReadOnly:  false,
			},
			{
				MountPath: `\\.\pipe\pipe1`,
				Name:      "pipe1",
				ReadOnly:  false,
			},
		},
	}

	// 为每个挂载点提供对应的卷挂载器。
	podVolumes := kubecontainer.VolumeMap{
		"disk":  kubecontainer.VolumeInfo{Mounter: &stubVolume{path: "c:/mnt/disk"}},
		"disk4": kubecontainer.VolumeInfo{Mounter: &stubVolume{path: "c:/mnt/host"}},
		"disk5": kubecontainer.VolumeInfo{Mounter: &stubVolume{path: "c:/var/lib/kubelet/podID/volumes/empty/disk5"}},
		"disk6": kubecontainer.VolumeInfo{Mounter: &stubVolume{path: `/mnt/disk6`}},
		"disk7": kubecontainer.VolumeInfo{Mounter: &stubVolume{path: `\mnt\disk7`}},
		"pipe1": kubecontainer.VolumeInfo{Mounter: &stubVolume{path: `\\.\pipe\pipe1`}},
	}

	pod := v1.Pod{
		Spec: v1.PodSpec{
			HostNetwork: true,
		},
	}

	// 使用 fake 的 hostutil 与 subpath，避免测试依赖真实文件系统。
	fhu := hostutil.NewFakeHostUtil(nil)
	fsp := &subpath.FakeSubpath{}
	// 创建临时 Pod 目录，用于生成 etc-hosts 文件。
	podDir, err := os.MkdirTemp("", "test-rotate-logs")
	require.NoError(t, err)
	defer os.RemoveAll(podDir)
	// 调用被测函数生成挂载列表。
	mounts, _, err := makeMounts(logger, &pod, podDir, &container, "fakepodname", "", []string{""}, podVolumes, fhu, fsp, nil, false, nil)
	require.NoError(t, err)

	// 期望路径会被转换为 Windows 风格，同时应自动追加 etc-hosts 挂载。
	expectedMounts := []kubecontainer.Mount{
		{
			Name:           "disk",
			ContainerPath:  "c:/etc/hosts",
			HostPath:       "c:/mnt/disk",
			ReadOnly:       false,
			SELinuxRelabel: false,
		},
		{
			Name:           "disk",
			ContainerPath:  "c:/mnt/path3",
			HostPath:       "c:/mnt/disk",
			ReadOnly:       true,
			SELinuxRelabel: false,
		},
		{
			Name:           "disk4",
			ContainerPath:  "c:/mnt/path4",
			HostPath:       "c:/mnt/host",
			ReadOnly:       false,
			SELinuxRelabel: false,
		},
		{
			Name:           "disk5",
			ContainerPath:  "c:/mnt/path5",
			HostPath:       "c:/var/lib/kubelet/podID/volumes/empty/disk5",
			ReadOnly:       false,
			SELinuxRelabel: false,
		},
		{
			Name:           "disk6",
			ContainerPath:  `c:\mnt\path6`,
			HostPath:       `c:/mnt/disk6`,
			ReadOnly:       false,
			SELinuxRelabel: false,
		},
		{
			Name:           "disk7",
			ContainerPath:  `c:/mnt/path7`,
			HostPath:       `c:\mnt\disk7`,
			ReadOnly:       false,
			SELinuxRelabel: false,
		},
		{
			Name:           "pipe1",
			ContainerPath:  `\\.\pipe\pipe1`,
			HostPath:       `\\.\pipe\pipe1`,
			ReadOnly:       false,
			SELinuxRelabel: false,
		},
		{
			Name:           "k8s-managed-etc-hosts",
			ContainerPath:  `C:\Windows\System32\drivers\etc\hosts`,
			HostPath:       filepath.Join(podDir, "etc-hosts"),
			ReadOnly:       false,
			SELinuxRelabel: true,
		},
	}
	// 整体比较，任何路径规范化差异都会暴露。
	assert.Equal(t, expectedMounts, mounts, "mounts of container %+v", container)
}
