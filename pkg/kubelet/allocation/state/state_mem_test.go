/*
Copyright The Kubernetes Authors.

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

package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// TestStateMemory_EmptyDirVolumeLimits 验证内存 state 中 EmptyDir 卷大小限制的读写语义：
// 隐式初始化 Pod、返回深拷贝、以及多卷互不影响。
func TestStateMemory_EmptyDirVolumeLimits(t *testing.T) {
	logger := klog.TODO()
	state := NewStateMemory(logger, PodResourceInfoMap{})

	podUID := types.UID("pod-1")
	volName := "volume-1"

	// 对不存在的 Pod 读取应返回 (nil, false)。
	qty, exists := state.GetEmptyDirVolumeLimit(podUID, volName)
	assert.Nil(t, qty)
	assert.False(t, exists)

	// 对不存在的 Pod 设置限制应隐式初始化该 Pod 并插入。
	targetLimit := resource.MustParse("256Mi")
	err := state.SetEmptyDirVolumeLimit(podUID, volName, &targetLimit)
	require.NoError(t, err)

	// 读取应返回已设置的值与 true。
	qty, exists = state.GetEmptyDirVolumeLimit(podUID, volName)
	require.True(t, exists)
	require.NotNil(t, qty)
	assert.True(t, targetLimit.Equal(*qty))

	// 已存在 Pod 上不存在的卷应返回 (nil, false)。
	qtyNonexistent, existsNonexistent := state.GetEmptyDirVolumeLimit(podUID, "nonexistent-volume")
	assert.Nil(t, qtyNonexistent)
	assert.False(t, existsNonexistent)

	// 返回的 Quantity 必须是深拷贝，修改它不能影响内部状态。
	qty.Set(1024 * 1024 * 512) // 将返回的 Quantity 修改为 512Mi
	refreshedQty, exists := state.GetEmptyDirVolumeLimit(podUID, volName)
	require.True(t, exists)
	assert.True(t, targetLimit.Equal(*refreshedQty), "Modifying the returned Quantity pointer should not alter Kubelet's internal memory state")

	// 为同一 Pod 设置另一个卷时，已有卷的限制应保持不变。
	anotherVolName := "volume-2"
	anotherLimit := resource.MustParse("128Mi")
	err = state.SetEmptyDirVolumeLimit(podUID, anotherVolName, &anotherLimit)
	require.NoError(t, err)

	// 确认两个卷的限制都存在且正确。
	qty1, exists1 := state.GetEmptyDirVolumeLimit(podUID, volName)
	assert.True(t, exists1)
	assert.True(t, targetLimit.Equal(*qty1))

	qty2, exists2 := state.GetEmptyDirVolumeLimit(podUID, anotherVolName)
	assert.True(t, exists2)
	assert.True(t, anotherLimit.Equal(*qty2))
}
