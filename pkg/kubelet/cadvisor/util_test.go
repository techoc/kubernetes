//go:build cgo && linux

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

package cadvisor

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/cadvisor/lib/container/crio"
	info "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestCapacityFromMachineInfoWithHugePagesEnable 验证启用 huge pages 时资源容量的计算。
func TestCapacityFromMachineInfoWithHugePagesEnable(t *testing.T) {
	machineInfo := &info.MachineInfo{
		NumCores:       2,
		MemoryCapacity: 2048,
		HugePages: []info.HugePagesInfo{
			{
				PageSize: 5,
				NumPages: 10,
			},
		},
	}

	expected := v1.ResourceList{
		v1.ResourceCPU:    *resource.NewMilliQuantity(int64(2000), resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(int64(2048), resource.BinarySI),
		"hugepages-5Ki":   *resource.NewQuantity(int64(51200), resource.BinarySI),
	}
	// 2 核换算为 2000m；hugepage 容量为页大小乘页数。
	actual := CapacityFromMachineInfo(machineInfo)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("when set hugepages true, got resource list %v, want %v", actual, expected)
	}
}

// TestEphemeralStorageCapacityFromFsInfo 验证容量使用 DecimalSI 格式。
// setters.go 中通过 DeepCopy().Sub() 计算 allocatable，因此 allocatable 会继承容量格式。
// DecimalSI 对任意字节数都能输出带单位的整洁值，而 BinarySI 在非 1024 对齐时
// 会退化为裸整数。参见 #133927。
func TestEphemeralStorageCapacityFromFsInfo(t *testing.T) {
	// 使用真实文件系统中常见的非 1024 对齐容量。
	fsInfo := info.FsInfo{Capacity: 97842800000}
	capacity := EphemeralStorageCapacityFromFsInfo(fsInfo)
	ephemeralCapacity := capacity[v1.ResourceEphemeralStorage]

	if ephemeralCapacity.Format != resource.DecimalSI {
		t.Errorf("capacity format = %v, want DecimalSI", ephemeralCapacity.Format)
	}

	// 模拟 allocatable 的推导（setters.go）：allocatable = capacity - reservation。
	// DeepCopy 保留格式，因此 allocatable 会继承容量的 DecimalSI。
	allocatable := ephemeralCapacity.DeepCopy()
	reservation := resource.MustParse("1Gi")
	allocatable.Sub(reservation)

	if allocatable.Format != resource.DecimalSI {
		t.Errorf("allocatable format = %v, want DecimalSI (inherited from capacity)", allocatable.Format)
	}
}

// TestCrioSocket 验证本包的 CRI-O socket 后缀与上游 cadvisor 保持一致。
func TestCrioSocket(t *testing.T) {
	assert.True(t, strings.HasSuffix(crio.CrioSocket, CrioSocketSuffix), "CrioSocketSuffix in this package must be a suffix of the one in github.com/google/cadvisor/container/crio/client.go")
}
