//go:build linux

/*
Copyright 2021 The Kubernetes Authors.

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

package cadvisor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/google/cadvisor/lib/container/crio"
	cadvisorfs "github.com/google/cadvisor/lib/fs"
	"github.com/opencontainers/cgroups"
	"k8s.io/klog/v2"
)

// TestIsPsiEnabled 验证 PSI 是否可用取决于 cgroup pressure 文件是否存在。
func TestIsPsiEnabled(t *testing.T) {
	testcases := []struct {
		description   string
		createPSIFile bool
		expected      bool
	}{{
		description:   "PSI enabled when cgroup pressure file exists",
		createPSIFile: true,
		expected:      true,
	}, {
		description:   "PSI disabled when cgroup pressure file does not exist",
		createPSIFile: false,
		expected:      false,
	}}

	// 开启 cgroups 测试模式，避免访问真实 cgroup 文件系统。
	cgroups.TestMode = true
	defer func() { cgroups.TestMode = false }()

	for _, tc := range testcases {
		t.Run(tc.description, func(t *testing.T) {
			// 每个子测试使用独立临时目录。
			tmpDir := t.TempDir()

			// 按用例决定是否创建 pressure 文件。
			if tc.createPSIFile {
				err := os.WriteFile(filepath.Join(tmpDir, "cpu.pressure"), []byte("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"), 0644)
				require.NoError(t, err)
			}

			result := isPsiEnabled(klog.Background(), tmpDir, "cpu.pressure")
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestImageFsInfoLabel 验证不同容器运行时下镜像文件系统标签的解析结果。
func TestImageFsInfoLabel(t *testing.T) {
	testcases := []struct {
		description     string
		runtime         string
		runtimeEndpoint string
		expectedLabel   string
		expectedError   error
	}{{
		description:     "LabelCrioImages should be returned",
		runtimeEndpoint: crio.CrioSocket,
		expectedLabel:   cadvisorfs.LabelCrioImages,
		expectedError:   nil,
	}, {
		description:     "Cannot find valid imagefs label",
		runtimeEndpoint: "",
		expectedLabel:   "",
		expectedError:   fmt.Errorf("no imagefs label for configured runtime"),
	}}

	for _, tc := range testcases {
		t.Run(tc.description, func(t *testing.T) {
			// 根据运行时 endpoint 构造文件系统信息提供者。
			infoProvider := NewImageFsInfoProvider(tc.runtimeEndpoint)
			label, err := infoProvider.ImageFsInfoLabel()
			assert.Equal(t, tc.expectedLabel, label)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}

// TestContainerFsInfoLabel 验证不同容器运行时下容器文件系统标签的解析结果。
func TestContainerFsInfoLabel(t *testing.T) {
	testcases := []struct {
		description     string
		runtime         string
		runtimeEndpoint string
		expectedLabel   string
		expectedError   error
	}{{
		description:     "LabelCrioWriteableImages should be returned",
		runtimeEndpoint: crio.CrioSocket,
		expectedLabel:   cadvisorfs.LabelCrioContainers,
		expectedError:   nil,
	}, {
		description:     "Cannot find valid imagefs label",
		runtimeEndpoint: "",
		expectedLabel:   "",
		expectedError:   fmt.Errorf("no containerfs label for configured runtime"),
	}}

	for _, tc := range testcases {
		t.Run(tc.description, func(t *testing.T) {
			// 根据运行时 endpoint 构造文件系统信息提供者。
			infoProvider := NewImageFsInfoProvider(tc.runtimeEndpoint)
			label, err := infoProvider.ContainerFsInfoLabel()
			assert.Equal(t, tc.expectedLabel, label)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}
