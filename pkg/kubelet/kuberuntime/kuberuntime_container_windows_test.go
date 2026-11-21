//go:build windows

/*
Copyright 2022 The Kubernetes Authors.

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

package kuberuntime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestApplyPlatformSpecificContainerConfig 验证 Windows 容器配置中的资源与安全上下文生成。
func TestApplyPlatformSpecificContainerConfig(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, fakeRuntimeSvc, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	// 构造空容器配置作为被测输入。
	containerConfig := &runtimeapi.ContainerConfig{}

	resources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("128Mi"),
			v1.ResourceCPU:    resource.MustParse("1"),
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("256Mi"),
			v1.ResourceCPU:    resource.MustParse("3"),
		},
	}

	// 构造 GMSA 凭据、运行用户与 HostProcess 配置。
	gmsaCredSpecName := "gmsa spec name"
	gmsaCredSpec := "credential spec"
	username := "ContainerAdministrator"
	asHostProcess := true
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "foo",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					Command:         []string{"testCommand"},
					WorkingDir:      "testWorkingDir",
					Resources:       resources,
					SecurityContext: &v1.SecurityContext{
						WindowsOptions: &v1.WindowsSecurityContextOptions{
							GMSACredentialSpecName: &gmsaCredSpecName,
							GMSACredentialSpec:     &gmsaCredSpec,
							RunAsUserName:          &username,
							HostProcess:            &asHostProcess,
						},
					},
				},
			},
		},
	}

	// 调用被测方法生成 Windows 平台配置。
	err = fakeRuntimeSvc.applyPlatformSpecificContainerConfig(tCtx, containerConfig, &pod.Spec.Containers[0], pod, new(int64), "foo", nil)
	require.NoError(t, err)

	limit := int64(3000)
	expectedCpuMax := 10 * limit / int64(winstats.ProcessorCount())
	// 上面把限制设为 3 个 CPU，但 CPU 最大值不能超过 100%
	// （例如只有 2 个 CPU 时不能达到 150%）。
	// CPU 最大值上限为 10000，即 100%。
	if expectedCpuMax > 10000 {
		expectedCpuMax = 10000
	}
	expectedWindowsConfig := &runtimeapi.WindowsContainerConfig{
		Resources: &runtimeapi.WindowsContainerResources{
			CpuMaximum:         expectedCpuMax,
			MemoryLimitInBytes: 256 * 1024 * 1024,
		},
		SecurityContext: &runtimeapi.WindowsContainerSecurityContext{
			CredentialSpec: gmsaCredSpec,
			RunAsUsername:  "ContainerAdministrator",
			HostProcess:    true,
		},
	}
	assert.Equal(t, expectedWindowsConfig, containerConfig.Windows)
}

// TestCalculateCPUMaximum 验证 CPU 限制到 Windows CPU 最大值的换算与范围裁剪。
func TestCalculateCPUMaximum(t *testing.T) {
	tests := []struct {
		name     string
		cpuLimit resource.Quantity
		cpuCount int64
		want     int64
	}{
		{
			name:     "max range when same amount",
			cpuLimit: resource.MustParse("1"),
			cpuCount: 1,
			want:     10000,
		},
		{
			name:     "percentage calculation is working as intended",
			cpuLimit: resource.MustParse("94"),
			cpuCount: 96,
			want:     9791,
		},
		{
			name:     "half range when half amount",
			cpuLimit: resource.MustParse("1"),
			cpuCount: 2,
			want:     5000,
		},
		{
			name:     "max range when more requested than available",
			cpuLimit: resource.MustParse("2"),
			cpuCount: 1,
			want:     10000,
		},
		{
			name:     "min range when less than minimum",
			cpuLimit: resource.MustParse("1m"),
			cpuCount: 100,
			want:     1,
		},
	}
	// 逐用例比较换算结果。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, calculateCPUMaximum(&tt.cpuLimit, tt.cpuCount))
		})
	}
}

// TestCalculateWindowsResources 验证 Windows 容器资源（CPU 最大值与内存限制）的计算。
func TestCalculateWindowsResources(t *testing.T) {
	// TODO: remove skip once the failing test has been fixed.
	// 已知失败用例，先跳过以免阻塞 CI。
	t.Skip("Skip failing test on Windows.")

	tCtx := ktesting.Init(t)
	_, _, fakeRuntimeSvc, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	// 用例覆盖正常限制、无内存限制与零 CPU 限制。
	tests := []struct {
		name     string
		cpuLim   resource.Quantity
		memLim   resource.Quantity
		expected *runtimeapi.WindowsContainerResources
	}{
		{
			name:   "Request128MBLimit256MB",
			cpuLim: resource.MustParse("2"),
			memLim: resource.MustParse("128Mi"),
			expected: &runtimeapi.WindowsContainerResources{
				CpuMaximum:         2500,
				MemoryLimitInBytes: 134217728,
			},
		},
		{
			name:   "RequestNoMemory",
			cpuLim: resource.MustParse("8"),
			memLim: resource.MustParse("0"),
			expected: &runtimeapi.WindowsContainerResources{
				CpuMaximum:         10000,
				MemoryLimitInBytes: 0,
			},
		},
		{
			name:   "RequestZeroCPU",
			cpuLim: resource.MustParse("0"),
			memLim: resource.MustParse("128Mi"),
			expected: &runtimeapi.WindowsContainerResources{
				CpuMaximum:         1,
				MemoryLimitInBytes: 134217728,
			},
		},
	}
	for _, test := range tests {
		// 逐用例比较生成的 Windows 容器资源。
		windowsContainerResources := fakeRuntimeSvc.calculateWindowsResources(tCtx, &test.cpuLim, &test.memLim)
		assert.Equal(t, test.expected, windowsContainerResources)
	}
}
