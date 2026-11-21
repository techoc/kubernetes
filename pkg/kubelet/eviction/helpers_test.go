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

package eviction

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"k8s.io/kubernetes/pkg/features"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

// quantityMustParse 把资源量字符串解析成指针形式，便于在 ThresholdValue 里复用。
// resource.Quantity 的零值不能直接取地址，测试统一走这个辅助函数。
func quantityMustParse(value string) *resource.Quantity {
	q := resource.MustParse(value)
	return &q
}

// TestGetReclaimableThreshold 验证按驱逐优先级排序后，getReclaimableThreshold
// 会挑出最优先且仍被触发的阈值，并给出其对应的资源名称。
func TestGetReclaimableThreshold(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := map[string]struct {
		thresholds                   []evictionapi.Threshold
		expectedThreshold            evictionapi.Threshold
		expectedResourceName         v1.ResourceName
		expectedReclaimableToBeFound bool
	}{
		"": {
			thresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalAllocatableMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalContainerFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
			},
			expectedThreshold: evictionapi.Threshold{
				Signal:   evictionapi.Signal("memory.available"),
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("150Mi"),
				},
			},
			expectedResourceName:         v1.ResourceName("memory"),
			expectedReclaimableToBeFound: true,
		},
		"no thresholds": {
			thresholds:           []evictionapi.Threshold{},
			expectedThreshold:    evictionapi.Threshold{},
			expectedResourceName: v1.ResourceName(""),
		},
		"threshold was crossed but reclaim not implemented (invalid signal)": {
			thresholds: []evictionapi.Threshold{
				{
					Signal: "mem.available",
				},
			},
			expectedThreshold:    evictionapi.Threshold{},
			expectedResourceName: v1.ResourceName(""),
		},
	}
	for _, testCase := range testCases {
		// 先排序，保证"第一个可回收"阈值就是优先级最高的那个信号。
		sort.Sort(byEvictionPriority(testCase.thresholds))
		threshold, ressourceName, found := getReclaimableThreshold(logger, testCase.thresholds)
		assert.Equal(t, testCase.expectedReclaimableToBeFound, found)
		assert.Equal(t, testCase.expectedResourceName, ressourceName)
		assert.Equal(t, testCase.expectedThreshold.Signal, threshold.Signal)
		assert.Equal(t, testCase.expectedThreshold.Operator, threshold.Operator)
		assert.Equal(t, testCase.expectedThreshold.Value.Quantity.String(), threshold.Value.Quantity.String())
		assert.Equal(t, testCase.expectedThreshold.GracePeriod, threshold.GracePeriod)
	}
}

// TestParseThresholdConfig 覆盖 hard/soft 阈值、宽限期和最小回收量的解析，
// 既覆盖量与百分比两种写法，也覆盖各类非法输入必须报错的分支。
func TestParseThresholdConfig(t *testing.T) {
	gracePeriod, _ := time.ParseDuration("30s")
	testCases := map[string]struct {
		allocatableConfig       []string
		evictionHard            map[string]string
		evictionSoft            map[string]string
		evictionSoftGracePeriod map[string]string
		evictionMinReclaim      map[string]string
		expectErr               bool
		expectThresholds        []evictionapi.Threshold
	}{
		"no values": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               false,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"all memory eviction values": {
			allocatableConfig:       []string{kubetypes.NodeAllocatableEnforcementKey},
			evictionHard:            map[string]string{"memory.available": "150Mi"},
			evictionSoft:            map[string]string{"memory.available": "300Mi"},
			evictionSoftGracePeriod: map[string]string{"memory.available": "30s"},
			evictionMinReclaim:      map[string]string{"memory.available": "0"},
			expectErr:               false,
			expectThresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalAllocatableMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
			},
		},
		"all memory eviction values in percentages": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"memory.available": "10%"},
			evictionSoft:            map[string]string{"memory.available": "30%"},
			evictionSoftGracePeriod: map[string]string{"memory.available": "30s"},
			evictionMinReclaim:      map[string]string{"memory.available": "5%"},
			expectErr:               false,
			expectThresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Percentage: 0.1,
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Percentage: 0.05,
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Percentage: 0.3,
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Percentage: 0.05,
					},
				},
			},
		},
		"disk eviction values": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"imagefs.available": "150Mi", "nodefs.available": "100Mi"},
			evictionSoft:            map[string]string{"imagefs.available": "300Mi", "nodefs.available": "200Mi"},
			evictionSoftGracePeriod: map[string]string{"imagefs.available": "30s", "nodefs.available": "30s"},
			evictionMinReclaim:      map[string]string{"imagefs.available": "2Gi", "nodefs.available": "1Gi"},
			expectErr:               false,
			expectThresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
			},
		},
		"disk eviction values in percentages": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"imagefs.available": "15%", "nodefs.available": "10.5%"},
			evictionSoft:            map[string]string{"imagefs.available": "30%", "nodefs.available": "20.5%"},
			evictionSoftGracePeriod: map[string]string{"imagefs.available": "30s", "nodefs.available": "30s"},
			evictionMinReclaim:      map[string]string{"imagefs.available": "10%", "nodefs.available": "5%"},
			expectErr:               false,
			expectThresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Percentage: 0.15,
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Percentage: 0.1,
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Percentage: 0.105,
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Percentage: 0.05,
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Percentage: 0.3,
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Percentage: 0.1,
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Percentage: 0.205,
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Percentage: 0.05,
					},
				},
			},
		},
		"inode eviction values": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"imagefs.inodesFree": "150Mi", "nodefs.inodesFree": "100Mi"},
			evictionSoft:            map[string]string{"imagefs.inodesFree": "300Mi", "nodefs.inodesFree": "200Mi"},
			evictionSoftGracePeriod: map[string]string{"imagefs.inodesFree": "30s", "nodefs.inodesFree": "30s"},
			evictionMinReclaim:      map[string]string{"imagefs.inodesFree": "2Gi", "nodefs.inodesFree": "1Gi"},
			expectErr:               false,
			expectThresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
			},
		},
		"disable via 0%": {
			allocatableConfig: []string{},
			evictionHard:      map[string]string{"memory.available": "0%"},
			evictionSoft:      map[string]string{"memory.available": "0%"},
			expectErr:         false,
			expectThresholds:  []evictionapi.Threshold{},
		},
		"disable via 100%": {
			allocatableConfig: []string{},
			evictionHard:      map[string]string{"memory.available": "100%"},
			evictionSoft:      map[string]string{"memory.available": "100%"},
			expectErr:         false,
			expectThresholds:  []evictionapi.Threshold{},
		},
		"invalid-signal": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"mem.available": "150Mi"},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"hard-signal-negative": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"memory.available": "-150Mi"},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"hard-signal-negative-percentage": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"memory.available": "-15%"},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"hard-signal-percentage-greater-than-100%": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"memory.available": "150%"},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"soft-signal-negative": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{"memory.available": "-150Mi"},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"valid-and-invalid-signal": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"memory.available": "150Mi", "invalid.foo": "150Mi"},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"soft-no-grace-period": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{"memory.available": "150Mi"},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"soft-negative-grace-period": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{"memory.available": "150Mi"},
			evictionSoftGracePeriod: map[string]string{"memory.available": "-30s"},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"negative-reclaim": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{"memory.available": "-300Mi"},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"min-reclaim-invalid-signal": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{"mem.available": "300Mi"},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"min-reclaim-empty-value": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{"memory.available": ""},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"min-reclaim-negative-percentage": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{"memory.available": "-15%"},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"min-reclaim-invalid-percentage": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{"memory.available": "10..5%"},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"hard-signal-empty-eviction-value": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"memory.available": ""},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"hard-signal-invalid-float-percentage": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{"memory.available": "10..5%"},
			evictionSoft:            map[string]string{},
			evictionSoftGracePeriod: map[string]string{},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"soft-grace-period-invalid-signal": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{"memory.available": "150Mi"},
			evictionSoftGracePeriod: map[string]string{"mem.available": "30s"},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
		"soft-invalid-grace-period": {
			allocatableConfig:       []string{},
			evictionHard:            map[string]string{},
			evictionSoft:            map[string]string{"memory.available": "150Mi"},
			evictionSoftGracePeriod: map[string]string{"memory.available": "30mins"},
			evictionMinReclaim:      map[string]string{},
			expectErr:               true,
			expectThresholds:        []evictionapi.Threshold{},
		},
	}
	for testName, testCase := range testCases {
		// 用 thresholdsEqual 做无序比较：map 遍历顺序会影响阈值生成顺序，不能依赖切片顺序。
		thresholds, err := ParseThresholdConfig(testCase.allocatableConfig, testCase.evictionHard, testCase.evictionSoft, testCase.evictionSoftGracePeriod, testCase.evictionMinReclaim)
		if testCase.expectErr != (err != nil) {
			t.Errorf("Err not as expected, test: %v, error expected: %v, actual: %v", testName, testCase.expectErr, err)
		}
		if !thresholdsEqual(testCase.expectThresholds, thresholds) {
			t.Errorf("thresholds not as expected, test: %v, expected: %v, actual: %v", testName, testCase.expectThresholds, thresholds)
		}
	}
}

// TestAddAllocatableThresholds 验证 addAllocatableThresholds 只为无宽限期的
// memory.available 阈值追加对应的 allocatableMemory.available 阈值。
func TestAddAllocatableThresholds(t *testing.T) {
	// 对每个 Signal 为 memory.available 且 GracePeriod 为 0 的阈值，额外追加一份
	// Signal 改为 allocatableMemory.available 的副本；其余阈值保持不动。
	testCases := map[string]struct {
		thresholds []evictionapi.Threshold
		expected   []evictionapi.Threshold
	}{
		"non-memory-signal": {
			thresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
			},
			expected: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
			},
		},
		"memory-signal-with-grace": {
			thresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 10,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
			},
			expected: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 10,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
			},
		},
		"memory-signal-without-grace": {
			thresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
			},
			expected: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalAllocatableMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
			},
		},
		"memory-signal-without-grace-two-thresholds": {
			thresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
			},
			expected: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalAllocatableMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("0"),
					},
				},
				{
					Signal:   evictionapi.SignalAllocatableMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalMemoryAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: 0,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
			},
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			// 结果可能是"原阈值 + 追加副本"，顺序不确定，因此用无序比较。
			if !thresholdsEqual(testCase.expected, addAllocatableThresholds(testCase.thresholds)) {
				t.Errorf("Err not as expected, test: %v, Unexpected data: %s", testName, cmp.Diff(testCase.expected, addAllocatableThresholds(testCase.thresholds)))
			}
		})
	}
}

// TestFallbackResourcesUsage 验证 stats 字段缺失时各 usage 函数返回非零兜底值，
// 否则百分比阈值会除以 0，或把"没有数据"误判成资源耗尽。
func TestFallbackResourcesUsage(t *testing.T) {
	for _, test := range []struct {
		description   string
		usageFuncName string
		usageFunc     func() int64
	}{
		{
			description:   "disk usage, fallback value",
			usageFuncName: "diskUsage",
			usageFunc: func() int64 {
				return diskUsage(&statsapi.FsStats{}).Value()
			},
		},
		{
			description:   "inode usage, fallback value",
			usageFuncName: "inodeUsage",
			usageFunc: func() int64 {
				return inodeUsage(&statsapi.FsStats{}).Value()
			},
		},
		{
			description:   "memory usage, fallback value",
			usageFuncName: "memoryUsage",
			usageFunc: func() int64 {
				return memoryUsage(&statsapi.MemoryStats{}).Value()
			},
		},
		{
			description:   "process usage, fallback value",
			usageFuncName: "processUsage",
			usageFunc: func() int64 {
				return int64(processUsage(&statsapi.ProcessStats{}))
			},
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			assert.NotEqualf(t, 0, test.usageFunc(), "%s: unexpected fallback value", test.usageFuncName)
		})
	}
}

// thresholdsEqual 以顺序无关的方式比较两组阈值，双向各查一次，
// 避免某一侧存在重复元素时被长度相等掩盖。
func thresholdsEqual(expected []evictionapi.Threshold, actual []evictionapi.Threshold) bool {
	if len(expected) != len(actual) {
		return false
	}
	for _, aThreshold := range expected {
		equal := false
		for _, bThreshold := range actual {
			if thresholdEqual(aThreshold, bThreshold) {
				equal = true
			}
		}
		if !equal {
			return false
		}
	}
	for _, aThreshold := range actual {
		equal := false
		for _, bThreshold := range expected {
			if thresholdEqual(aThreshold, bThreshold) {
				equal = true
			}
		}
		if !equal {
			return false
		}
	}
	return true
}

// thresholdEqual 逐字段比较阈值；数量与百分比会在同一量纲上统一折算后再比较。
func thresholdEqual(a evictionapi.Threshold, b evictionapi.Threshold) bool {
	return a.GracePeriod == b.GracePeriod &&
		a.Operator == b.Operator &&
		a.Signal == b.Signal &&
		compareThresholdValue(*a.MinReclaim, *b.MinReclaim) &&
		compareThresholdValue(a.Value, b.Value)
}

// TestOrderedByExceedsRequestMemory 验证"超出内存请求"的比较器会把超额使用的 Pod 排在前面。
func TestOrderedByExceedsRequestMemory(t *testing.T) {
	below := newPod("below-requests", -1, []v1.Container{
		newContainer("below-requests", newResourceList("", "200Mi", ""), newResourceList("", "", "")),
	}, nil)
	exceeds := newPod("exceeds-requests", 1, []v1.Container{
		newContainer("exceeds-requests", newResourceList("", "100Mi", ""), newResourceList("", "", "")),
	}, nil)
	// below 比请求少 1Mi（-1），exceeds 比请求多 1Mi（+1）；超出者应优先被驱逐。
	stats := map[*v1.Pod]statsapi.PodStats{
		below:   newPodMemoryStats(below, resource.MustParse("199Mi")),   // -1 relative to request
		exceeds: newPodMemoryStats(exceeds, resource.MustParse("101Mi")), // 1 relative to request
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{below, exceeds}
	orderedBy(exceedMemoryRequests(statsFn)).Sort(pods)

	expected := []*v1.Pod{exceeds, below}
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod: %s, but got: %s", expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedByExceedsRequestDisk 验证按临时存储请求计算超额使用时，超出者排在前面。
func TestOrderedByExceedsRequestDisk(t *testing.T) {
	below := newPod("below-requests", -1, []v1.Container{
		newContainer("below-requests", v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("200Mi")}, newResourceList("", "", "")),
	}, nil)
	exceeds := newPod("exceeds-requests", 1, []v1.Container{
		newContainer("exceeds-requests", v1.ResourceList{v1.ResourceEphemeralStorage: resource.MustParse("100Mi")}, newResourceList("", "", "")),
	}, nil)
	stats := map[*v1.Pod]statsapi.PodStats{
		below:   newPodDiskStats(below, resource.MustParse("100Mi"), resource.MustParse("99Mi"), resource.MustParse("0Mi")),  // -1 relative to request
		exceeds: newPodDiskStats(exceeds, resource.MustParse("90Mi"), resource.MustParse("11Mi"), resource.MustParse("0Mi")), // 1 relative to request
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{below, exceeds}
	orderedBy(exceedDiskRequests(statsFn, []fsStatsType{fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, v1.ResourceEphemeralStorage)).Sort(pods)

	expected := []*v1.Pod{exceeds, below}
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod: %s, but got: %s", expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedByPriority 验证优先级升序排序：低优先级的 Pod 先被驱逐。
func TestOrderedByPriority(t *testing.T) {
	low := newPod("low-priority", -134, []v1.Container{
		newContainer("low-priority", newResourceList("", "", ""), newResourceList("", "", "")),
	}, nil)
	medium := newPod("medium-priority", 1, []v1.Container{
		newContainer("medium-priority", newResourceList("100m", "100Mi", ""), newResourceList("200m", "200Mi", "")),
	}, nil)
	high := newPod("high-priority", 12534, []v1.Container{
		newContainer("high-priority", newResourceList("200m", "200Mi", ""), newResourceList("200m", "200Mi", "")),
	}, nil)

	// 输入顺序故意打乱，排序结果应为 low -> medium -> high。
	pods := []*v1.Pod{high, medium, low}
	orderedBy(priority).Sort(pods)

	expected := []*v1.Pod{low, medium, high}
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod: %s, but got: %s", expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedbyDisk 验证磁盘比较器按 Pod 用量从大到小排序；
// best-effort Pod 请求为 0，相对用量就是绝对用量。
func TestOrderedbyDisk(t *testing.T) {
	pod1 := newPod("best-effort-high", defaultPriority, []v1.Container{
		newContainer("best-effort-high", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod2 := newPod("best-effort-low", defaultPriority, []v1.Container{
		newContainer("best-effort-low", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod3 := newPod("burstable-high", defaultPriority, []v1.Container{
		newContainer("burstable-high", newResourceList("", "", "100Mi"), newResourceList("", "", "400Mi")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod4 := newPod("burstable-low", defaultPriority, []v1.Container{
		newContainer("burstable-low", newResourceList("", "", "100Mi"), newResourceList("", "", "400Mi")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod5 := newPod("guaranteed-high", defaultPriority, []v1.Container{
		newContainer("guaranteed-high", newResourceList("", "", "400Mi"), newResourceList("", "", "400Mi")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod6 := newPod("guaranteed-low", defaultPriority, []v1.Container{
		newContainer("guaranteed-low", newResourceList("", "", "400Mi"), newResourceList("", "", "400Mi")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	stats := map[*v1.Pod]statsapi.PodStats{
		pod1: newPodDiskStats(pod1, resource.MustParse("50Mi"), resource.MustParse("100Mi"), resource.MustParse("150Mi")), // 300Mi - 0 = 300Mi
		pod2: newPodDiskStats(pod2, resource.MustParse("25Mi"), resource.MustParse("25Mi"), resource.MustParse("50Mi")),   // 100Mi - 0 = 100Mi
		pod3: newPodDiskStats(pod3, resource.MustParse("150Mi"), resource.MustParse("150Mi"), resource.MustParse("50Mi")), // 350Mi - 100Mi = 250Mi
		pod4: newPodDiskStats(pod4, resource.MustParse("25Mi"), resource.MustParse("35Mi"), resource.MustParse("50Mi")),   // 110Mi - 100Mi = 10Mi
		pod5: newPodDiskStats(pod5, resource.MustParse("225Mi"), resource.MustParse("100Mi"), resource.MustParse("50Mi")), // 375Mi - 400Mi = -25Mi
		pod6: newPodDiskStats(pod6, resource.MustParse("25Mi"), resource.MustParse("45Mi"), resource.MustParse("50Mi")),   // 120Mi - 400Mi = -280Mi
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{pod1, pod2, pod3, pod4, pod5, pod6}
	orderedBy(disk(statsFn, []fsStatsType{fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, v1.ResourceEphemeralStorage)).Sort(pods)
	expected := []*v1.Pod{pod1, pod3, pod2, pod4, pod5, pod6}
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod[%d]: %s, but got: %s", i, expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedbyInodes 验证 inode 比较器按 Pod 的 inode 用量从大到小排序。
func TestOrderedbyInodes(t *testing.T) {
	low := newPod("low", defaultPriority, []v1.Container{
		newContainer("low", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	medium := newPod("medium", defaultPriority, []v1.Container{
		newContainer("medium", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	high := newPod("high", defaultPriority, []v1.Container{
		newContainer("high", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	stats := map[*v1.Pod]statsapi.PodStats{
		low:    newPodInodeStats(low, resource.MustParse("50000"), resource.MustParse("100000"), resource.MustParse("50000")),     // 200000
		medium: newPodInodeStats(medium, resource.MustParse("100000"), resource.MustParse("150000"), resource.MustParse("50000")), // 300000
		high:   newPodInodeStats(high, resource.MustParse("200000"), resource.MustParse("150000"), resource.MustParse("50000")),   // 400000
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{low, medium, high}
	orderedBy(disk(statsFn, []fsStatsType{fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, resourceInodes)).Sort(pods)
	expected := []*v1.Pod{high, medium, low}
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod[%d]: %s, but got: %s", i, expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedByPriorityDisk 验证综合排序：先按优先级分组，同优先级内再按磁盘超额用量降序。
func TestOrderedByPriorityDisk(t *testing.T) {
	pod1 := newPod("above-requests-low-priority-high-usage", lowPriority, []v1.Container{
		newContainer("above-requests-low-priority-high-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod2 := newPod("above-requests-low-priority-low-usage", lowPriority, []v1.Container{
		newContainer("above-requests-low-priority-low-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod3 := newPod("above-requests-high-priority-high-usage", highPriority, []v1.Container{
		newContainer("above-requests-high-priority-high-usage", newResourceList("", "", "100Mi"), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod4 := newPod("above-requests-high-priority-low-usage", highPriority, []v1.Container{
		newContainer("above-requests-high-priority-low-usage", newResourceList("", "", "100Mi"), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod5 := newPod("below-requests-low-priority-high-usage", lowPriority, []v1.Container{
		newContainer("below-requests-low-priority-high-usage", newResourceList("", "", "1Gi"), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod6 := newPod("below-requests-low-priority-low-usage", lowPriority, []v1.Container{
		newContainer("below-requests-low-priority-low-usage", newResourceList("", "", "1Gi"), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod7 := newPod("below-requests-high-priority-high-usage", highPriority, []v1.Container{
		newContainer("below-requests-high-priority-high-usage", newResourceList("", "", "1Gi"), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod8 := newPod("below-requests-high-priority-low-usage", highPriority, []v1.Container{
		newContainer("below-requests-high-priority-low-usage", newResourceList("", "", "1Gi"), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	stats := map[*v1.Pod]statsapi.PodStats{
		pod1: newPodDiskStats(pod1, resource.MustParse("200Mi"), resource.MustParse("100Mi"), resource.MustParse("200Mi")), // 500 relative to request
		pod2: newPodDiskStats(pod2, resource.MustParse("10Mi"), resource.MustParse("10Mi"), resource.MustParse("30Mi")),    // 50 relative to request
		pod3: newPodDiskStats(pod3, resource.MustParse("200Mi"), resource.MustParse("150Mi"), resource.MustParse("250Mi")), // 500 relative to request
		pod4: newPodDiskStats(pod4, resource.MustParse("90Mi"), resource.MustParse("50Mi"), resource.MustParse("10Mi")),    // 50 relative to request
		pod5: newPodDiskStats(pod5, resource.MustParse("500Mi"), resource.MustParse("200Mi"), resource.MustParse("100Mi")), // -200 relative to request
		pod6: newPodDiskStats(pod6, resource.MustParse("50Mi"), resource.MustParse("100Mi"), resource.MustParse("50Mi")),   // -800 relative to request
		pod7: newPodDiskStats(pod7, resource.MustParse("250Mi"), resource.MustParse("500Mi"), resource.MustParse("50Mi")),  // -200 relative to request
		pod8: newPodDiskStats(pod8, resource.MustParse("100Mi"), resource.MustParse("60Mi"), resource.MustParse("40Mi")),   // -800 relative to request
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{pod8, pod7, pod6, pod5, pod4, pod3, pod2, pod1}
	expected := []*v1.Pod{pod1, pod2, pod3, pod4, pod5, pod6, pod7, pod8}
	fsStatsToMeasure := []fsStatsType{fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}
	orderedBy(exceedDiskRequests(statsFn, fsStatsToMeasure, v1.ResourceEphemeralStorage), priority, disk(statsFn, fsStatsToMeasure, v1.ResourceEphemeralStorage)).Sort(pods)
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod[%d]: %s, but got: %s", i, expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedByPriorityInodes 验证综合排序：先按优先级分组，同优先级内再按 inode 用量降序。
func TestOrderedByPriorityInodes(t *testing.T) {
	pod1 := newPod("low-priority-high-usage", lowPriority, []v1.Container{
		newContainer("low-priority-high-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod2 := newPod("low-priority-low-usage", lowPriority, []v1.Container{
		newContainer("low-priority-low-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod3 := newPod("high-priority-high-usage", highPriority, []v1.Container{
		newContainer("high-priority-high-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	pod4 := newPod("high-priority-low-usage", highPriority, []v1.Container{
		newContainer("high-priority-low-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, []v1.Volume{
		newVolume("local-volume", v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}),
	})
	stats := map[*v1.Pod]statsapi.PodStats{
		pod1: newPodInodeStats(pod1, resource.MustParse("50000"), resource.MustParse("100000"), resource.MustParse("250000")), // 400000
		pod2: newPodInodeStats(pod2, resource.MustParse("60000"), resource.MustParse("30000"), resource.MustParse("10000")),   // 100000
		pod3: newPodInodeStats(pod3, resource.MustParse("150000"), resource.MustParse("150000"), resource.MustParse("50000")), // 350000
		pod4: newPodInodeStats(pod4, resource.MustParse("10000"), resource.MustParse("40000"), resource.MustParse("100000")),  // 150000
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{pod4, pod3, pod2, pod1}
	orderedBy(priority, disk(statsFn, []fsStatsType{fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, resourceInodes)).Sort(pods)
	expected := []*v1.Pod{pod1, pod2, pod3, pod4}
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod[%d]: %s, but got: %s", i, expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedByMemory 验证按相对内存请求的超出量从大到小排序。
func TestOrderedByMemory(t *testing.T) {
	pod1 := newPod("best-effort-high", defaultPriority, []v1.Container{
		newContainer("best-effort-high", newResourceList("", "", ""), newResourceList("", "", "")),
	}, nil)
	pod2 := newPod("best-effort-low", defaultPriority, []v1.Container{
		newContainer("best-effort-low", newResourceList("", "", ""), newResourceList("", "", "")),
	}, nil)
	pod3 := newPod("burstable-high", defaultPriority, []v1.Container{
		newContainer("burstable-high", newResourceList("", "100Mi", ""), newResourceList("", "1Gi", "")),
	}, nil)
	pod4 := newPod("burstable-low", defaultPriority, []v1.Container{
		newContainer("burstable-low", newResourceList("", "100Mi", ""), newResourceList("", "1Gi", "")),
	}, nil)
	pod5 := newPod("guaranteed-high", defaultPriority, []v1.Container{
		newContainer("guaranteed-high", newResourceList("", "1Gi", ""), newResourceList("", "1Gi", "")),
	}, nil)
	pod6 := newPod("guaranteed-low", defaultPriority, []v1.Container{
		newContainer("guaranteed-low", newResourceList("", "1Gi", ""), newResourceList("", "1Gi", "")),
	}, nil)
	stats := map[*v1.Pod]statsapi.PodStats{
		pod1: newPodMemoryStats(pod1, resource.MustParse("500Mi")), // 500 relative to request
		pod2: newPodMemoryStats(pod2, resource.MustParse("300Mi")), // 300 relative to request
		pod3: newPodMemoryStats(pod3, resource.MustParse("800Mi")), // 700 relative to request
		pod4: newPodMemoryStats(pod4, resource.MustParse("300Mi")), // 200 relative to request
		pod5: newPodMemoryStats(pod5, resource.MustParse("800Mi")), // -200 relative to request
		pod6: newPodMemoryStats(pod6, resource.MustParse("200Mi")), // -800 relative to request
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{pod1, pod2, pod3, pod4, pod5, pod6}
	orderedBy(memory(statsFn)).Sort(pods)
	expected := []*v1.Pod{pod3, pod1, pod2, pod4, pod5, pod6}
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod[%d]: %s, but got: %s", i, expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedByPriorityMemory 验证先按优先级分组，再按相对内存请求的超出量降序排序。
func TestOrderedByPriorityMemory(t *testing.T) {
	pod1 := newPod("above-requests-low-priority-high-usage", lowPriority, []v1.Container{
		newContainer("above-requests-low-priority-high-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, nil)
	pod2 := newPod("above-requests-low-priority-low-usage", lowPriority, []v1.Container{
		newContainer("above-requests-low-priority-low-usage", newResourceList("", "", ""), newResourceList("", "", "")),
	}, nil)
	pod3 := newPod("above-requests-high-priority-high-usage", highPriority, []v1.Container{
		newContainer("above-requests-high-priority-high-usage", newResourceList("", "100Mi", ""), newResourceList("", "", "")),
	}, nil)
	pod4 := newPod("above-requests-high-priority-low-usage", highPriority, []v1.Container{
		newContainer("above-requests-high-priority-low-usage", newResourceList("", "100Mi", ""), newResourceList("", "", "")),
	}, nil)
	pod5 := newPod("below-requests-low-priority-high-usage", lowPriority, []v1.Container{
		newContainer("below-requests-low-priority-high-usage", newResourceList("", "1Gi", ""), newResourceList("", "", "")),
	}, nil)
	pod6 := newPod("below-requests-low-priority-low-usage", lowPriority, []v1.Container{
		newContainer("below-requests-low-priority-low-usage", newResourceList("", "1Gi", ""), newResourceList("", "", "")),
	}, nil)
	pod7 := newPod("below-requests-high-priority-high-usage", highPriority, []v1.Container{
		newContainer("below-requests-high-priority-high-usage", newResourceList("", "1Gi", ""), newResourceList("", "", "")),
	}, nil)
	pod8 := newPod("below-requests-high-priority-low-usage", highPriority, []v1.Container{
		newContainer("below-requests-high-priority-low-usage", newResourceList("", "1Gi", ""), newResourceList("", "", "")),
	}, nil)
	stats := map[*v1.Pod]statsapi.PodStats{
		pod1: newPodMemoryStats(pod1, resource.MustParse("500Mi")), // 500 relative to request
		pod2: newPodMemoryStats(pod2, resource.MustParse("50Mi")),  // 50 relative to request
		pod3: newPodMemoryStats(pod3, resource.MustParse("600Mi")), // 500 relative to request
		pod4: newPodMemoryStats(pod4, resource.MustParse("150Mi")), // 50 relative to request
		pod5: newPodMemoryStats(pod5, resource.MustParse("800Mi")), // -200 relative to request
		pod6: newPodMemoryStats(pod6, resource.MustParse("200Mi")), // -800 relative to request
		pod7: newPodMemoryStats(pod7, resource.MustParse("800Mi")), // -200 relative to request
		pod8: newPodMemoryStats(pod8, resource.MustParse("200Mi")), // -800 relative to request
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{pod8, pod7, pod6, pod5, pod4, pod3, pod2, pod1}
	expected := []*v1.Pod{pod1, pod2, pod3, pod4, pod5, pod6, pod7, pod8}
	orderedBy(exceedMemoryRequests(statsFn), priority, memory(statsFn)).Sort(pods)
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod[%d]: %s, but got: %s", i, expected[i].Name, pods[i].Name)
		}
	}
}

// TestOrderedByPriorityProcess 验证先按优先级分组，再按进程数从多到少排序。
func TestOrderedByPriorityProcess(t *testing.T) {
	pod1 := newPod("low-priority-high-usage", lowPriority, nil, nil)
	pod2 := newPod("low-priority-low-usage", lowPriority, nil, nil)
	pod3 := newPod("high-priority-high-usage", highPriority, nil, nil)
	pod4 := newPod("high-priority-low-usage", highPriority, nil, nil)
	stats := map[*v1.Pod]statsapi.PodStats{
		pod1: newPodProcessStats(pod1, 20),
		pod2: newPodProcessStats(pod2, 6),
		pod3: newPodProcessStats(pod3, 20),
		pod4: newPodProcessStats(pod4, 5),
	}
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		result, found := stats[pod]
		return result, found
	}
	pods := []*v1.Pod{pod4, pod3, pod2, pod1}
	expected := []*v1.Pod{pod1, pod2, pod3, pod4}
	orderedBy(priority, process(statsFn)).Sort(pods)
	for i := range expected {
		if pods[i] != expected[i] {
			t.Errorf("Expected pod[%d]: %s, but got: %s", i, expected[i].Name, pods[i].Name)
		}
	}
}

// TestSortByEvictionPriority 验证阈值按信号优先级排序：内存类信号排在磁盘、PID 信号之前，
// 因为内存压力恢复最慢、对节点影响最大。
func TestSortByEvictionPriority(t *testing.T) {
	for _, tc := range []struct {
		name       string
		thresholds []evictionapi.Threshold
		expected   []evictionapi.Threshold
	}{
		{
			name:       "empty threshold list",
			thresholds: []evictionapi.Threshold{},
			expected:   []evictionapi.Threshold{},
		},
		{
			name: "memory first",
			thresholds: []evictionapi.Threshold{
				{
					Signal: evictionapi.SignalNodeFsAvailable,
				},
				{
					Signal: evictionapi.SignalPIDAvailable,
				},
				{
					Signal: evictionapi.SignalMemoryAvailable,
				},
			},
			expected: []evictionapi.Threshold{
				{
					Signal: evictionapi.SignalMemoryAvailable,
				},
				{
					Signal: evictionapi.SignalNodeFsAvailable,
				},
				{
					Signal: evictionapi.SignalPIDAvailable,
				},
			},
		},
		{
			name: "allocatable memory first",
			thresholds: []evictionapi.Threshold{
				{
					Signal: evictionapi.SignalNodeFsAvailable,
				},
				{
					Signal: evictionapi.SignalPIDAvailable,
				},
				{
					Signal: evictionapi.SignalAllocatableMemoryAvailable,
				},
			},
			expected: []evictionapi.Threshold{
				{
					Signal: evictionapi.SignalAllocatableMemoryAvailable,
				},
				{
					Signal: evictionapi.SignalNodeFsAvailable,
				},
				{
					Signal: evictionapi.SignalPIDAvailable,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sort.Sort(byEvictionPriority(tc.thresholds))
			for i := range tc.expected {
				if tc.thresholds[i].Signal != tc.expected[i].Signal {
					t.Errorf("At index %d, expected threshold with signal %s, but got %s", i, tc.expected[i].Signal, tc.thresholds[i].Signal)
				}
			}

		})
	}
}

// fakeSummaryProvider 始终返回同一份 Summary，用于把统计采集逻辑从被测逻辑中隔离出去。
type fakeSummaryProvider struct {
	result *statsapi.Summary
}

// Get 实现 summaryProvider 接口，忽略 updateStats，直接返回预置结果。
func (f *fakeSummaryProvider) Get(ctx context.Context, updateStats bool) (*statsapi.Summary, error) {
	return f.result, nil
}

// GetCPUAndMemoryStats 让假实现同时满足只需 CPU/内存统计的调用方。
func (f *fakeSummaryProvider) GetCPUAndMemoryStats(ctx context.Context) (*statsapi.Summary, error) {
	return f.result, nil
}

// newPodStats 构造只含工作集内存统计的 PodStats。
// Pod 必须有 Name、UID、Namespace，statsFunc 才能按 PodRef 正确匹配。
func newPodStats(pod *v1.Pod, podWorkingSetBytes uint64) statsapi.PodStats {
	return statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			UID:       string(pod.UID),
		},
		Memory: &statsapi.MemoryStats{
			WorkingSetBytes: &podWorkingSetBytes,
		},
	}
}

// TestMakeSignalObservations 校验从 Summary 各字段提取信号观测值：可用量与容量都要正确，
// 同时覆盖 Linux（pods 系统容器）与 Windows（GlobalCommitMemory）两种内存来源。
func TestMakeSignalObservations(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	podMaker := func(name, namespace, uid string, numContainers int) *v1.Pod {
		pod := &v1.Pod{}
		pod.Name = name
		pod.Namespace = namespace
		pod.UID = types.UID(uid)
		pod.Spec = v1.PodSpec{}
		for i := 0; i < numContainers; i++ {
			pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
				Name: fmt.Sprintf("ctr%v", i),
			})
		}
		return pod
	}
	nodeAvailableBytes := uint64(1024 * 1024 * 1024)
	nodeWorkingSetBytes := uint64(1024 * 1024 * 1024)
	allocatableMemoryCapacity := uint64(5 * 1024 * 1024 * 1024)
	imageFsAvailableBytes := uint64(1024 * 1024)
	imageFsCapacityBytes := uint64(1024 * 1024 * 2)
	nodeFsAvailableBytes := uint64(1024)
	nodeFsCapacityBytes := uint64(1024 * 2)
	imageFsInodesFree := uint64(1024)
	imageFsInodes := uint64(1024 * 1024)
	nodeFsInodesFree := uint64(1024)
	nodeFsInodes := uint64(1024 * 1024)
	containerFsAvailableBytes := uint64(1024 * 1024 * 2)
	containerFsCapacityBytes := uint64(1024 * 1024 * 8)
	containerFsInodesFree := uint64(1024 * 2)
	containerFsInodes := uint64(1024 * 2)
	maxPID := int64(255816)
	numberOfRunningProcesses := int64(1000)
	fakeStats := &statsapi.Summary{
		Node: statsapi.NodeStats{
			Memory: &statsapi.MemoryStats{
				AvailableBytes:  &nodeAvailableBytes,
				WorkingSetBytes: &nodeWorkingSetBytes,
			},
			Runtime: &statsapi.RuntimeStats{
				ImageFs: &statsapi.FsStats{
					AvailableBytes: &imageFsAvailableBytes,
					CapacityBytes:  &imageFsCapacityBytes,
					InodesFree:     &imageFsInodesFree,
					Inodes:         &imageFsInodes,
				},
				ContainerFs: &statsapi.FsStats{
					AvailableBytes: &containerFsAvailableBytes,
					CapacityBytes:  &containerFsCapacityBytes,
					InodesFree:     &containerFsInodesFree,
					Inodes:         &containerFsInodes,
				},
			},
			Rlimit: &statsapi.RlimitStats{
				MaxPID:                &maxPID,
				NumOfRunningProcesses: &numberOfRunningProcesses,
			},
			Fs: &statsapi.FsStats{
				AvailableBytes: &nodeFsAvailableBytes,
				CapacityBytes:  &nodeFsCapacityBytes,
				InodesFree:     &nodeFsInodesFree,
				Inodes:         &nodeFsInodes,
			},
			SystemContainers: []statsapi.ContainerStats{
				// Used for memory signal observations on linux
				{
					Name: statsapi.SystemContainerPods,
					Memory: &statsapi.MemoryStats{
						AvailableBytes:  &nodeAvailableBytes,
						WorkingSetBytes: &nodeWorkingSetBytes,
					},
				},
				// Used for memory signal observations on windows
				{
					Name: statsapi.SystemContainerWindowsGlobalCommitMemory,
					Memory: &statsapi.MemoryStats{
						AvailableBytes: &nodeAvailableBytes,
						UsageBytes:     &nodeWorkingSetBytes,
					},
				},
			},
		},
		Pods: []statsapi.PodStats{},
	}
	pods := []*v1.Pod{
		podMaker("pod1", "ns1", "uuid1", 1),
		podMaker("pod1", "ns2", "uuid2", 1),
		podMaker("pod3", "ns3", "uuid3", 1),
	}
	podWorkingSetBytes := uint64(1024 * 1024 * 1024)
	for _, pod := range pods {
		fakeStats.Pods = append(fakeStats.Pods, newPodStats(pod, podWorkingSetBytes))
	}
	res := quantityMustParse("5Gi")
	// 可分配内存的阈值恒为 100%，因此其容量必须等于 allocatable 容量。
	// Allocatable thresholds are always 100%.  Verify that Threshold == Capacity.
	if res.CmpInt64(int64(allocatableMemoryCapacity)) != 0 {
		t.Errorf("Expected Threshold %v to be equal to value %v", res.Value(), allocatableMemoryCapacity)
	}
	actualObservations, statsFunc := makeSignalObservations(logger, fakeStats)
	allocatableMemQuantity, found := actualObservations[evictionapi.SignalAllocatableMemoryAvailable]
	if !found {
		t.Errorf("Expected allocatable memory observation, but didn't find one")
	}
	if expectedBytes := int64(nodeAvailableBytes); allocatableMemQuantity.available.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, allocatableMemQuantity.available.Value())
	}
	if expectedBytes := int64(nodeWorkingSetBytes + nodeAvailableBytes); allocatableMemQuantity.capacity.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, allocatableMemQuantity.capacity.Value())
	}
	memQuantity, found := actualObservations[evictionapi.SignalMemoryAvailable]
	if !found {
		t.Error("Expected available memory observation")
	}
	if expectedBytes := int64(nodeAvailableBytes); memQuantity.available.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, memQuantity.available.Value())
	}
	if expectedBytes := int64(nodeWorkingSetBytes + nodeAvailableBytes); memQuantity.capacity.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, memQuantity.capacity.Value())
	}
	nodeFsQuantity, found := actualObservations[evictionapi.SignalNodeFsAvailable]
	if !found {
		t.Error("Expected available nodefs observation")
	}
	if expectedBytes := int64(nodeFsAvailableBytes); nodeFsQuantity.available.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, nodeFsQuantity.available.Value())
	}
	if expectedBytes := int64(nodeFsCapacityBytes); nodeFsQuantity.capacity.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, nodeFsQuantity.capacity.Value())
	}
	nodeFsInodesQuantity, found := actualObservations[evictionapi.SignalNodeFsInodesFree]
	if !found {
		t.Error("Expected inodes free nodefs observation")
	}
	if expected := int64(nodeFsInodesFree); nodeFsInodesQuantity.available.Value() != expected {
		t.Errorf("Expected %v, actual: %v", expected, nodeFsInodesQuantity.available.Value())
	}
	if expected := int64(nodeFsInodes); nodeFsInodesQuantity.capacity.Value() != expected {
		t.Errorf("Expected %v, actual: %v", expected, nodeFsInodesQuantity.capacity.Value())
	}
	imageFsQuantity, found := actualObservations[evictionapi.SignalImageFsAvailable]
	if !found {
		t.Error("Expected available imagefs observation")
	}
	if expectedBytes := int64(imageFsAvailableBytes); imageFsQuantity.available.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, imageFsQuantity.available.Value())
	}
	if expectedBytes := int64(imageFsCapacityBytes); imageFsQuantity.capacity.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, imageFsQuantity.capacity.Value())
	}
	containerFsQuantity, found := actualObservations[evictionapi.SignalContainerFsAvailable]
	if !found {
		t.Error("Expected available containerfs observation")
	}
	if expectedBytes := int64(containerFsAvailableBytes); containerFsQuantity.available.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, containerFsQuantity.available.Value())
	}
	if expectedBytes := int64(containerFsCapacityBytes); containerFsQuantity.capacity.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, containerFsQuantity.capacity.Value())
	}
	imageFsInodesQuantity, found := actualObservations[evictionapi.SignalImageFsInodesFree]
	if !found {
		t.Error("Expected inodes free imagefs observation")
	}
	if expected := int64(imageFsInodesFree); imageFsInodesQuantity.available.Value() != expected {
		t.Errorf("Expected %v, actual: %v", expected, imageFsInodesQuantity.available.Value())
	}
	if expected := int64(imageFsInodes); imageFsInodesQuantity.capacity.Value() != expected {
		t.Errorf("Expected %v, actual: %v", expected, imageFsInodesQuantity.capacity.Value())
	}
	containerFsInodesQuantity, found := actualObservations[evictionapi.SignalContainerFsInodesFree]
	if !found {
		t.Error("Expected indoes free containerfs observation")
	}
	if expected := int64(containerFsInodesFree); containerFsInodesQuantity.available.Value() != expected {
		t.Errorf("Expected %v, actual: %v", expected, containerFsInodesQuantity.available.Value())
	}
	if expected := int64(containerFsInodes); containerFsInodesQuantity.capacity.Value() != expected {
		t.Errorf("Expected %v, actual: %v", expected, containerFsInodesQuantity.capacity.Value())
	}

	pidQuantity, found := actualObservations[evictionapi.SignalPIDAvailable]
	if !found {
		t.Error("Expected available memory observation")
	}
	if expectedBytes := int64(maxPID); pidQuantity.capacity.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, pidQuantity.capacity.Value())
	}
	if expectedBytes := int64(maxPID - numberOfRunningProcesses); pidQuantity.available.Value() != expectedBytes {
		t.Errorf("Expected %v, actual: %v", expectedBytes, pidQuantity.available.Value())
	}
	// 返回的 statsFunc 必须能按 Pod 取回各自的统计，供后续按 Pod 排序使用。
	for _, pod := range pods {
		podStats, found := statsFunc(pod)
		if !found {
			t.Errorf("Pod stats were not found for pod %v", pod.UID)
		}
		if *podStats.Memory.WorkingSetBytes != podWorkingSetBytes {
			t.Errorf("Pod working set expected %v, actual: %v", podWorkingSetBytes, *podStats.Memory.WorkingSetBytes)
		}
	}
}

// TestThresholdsMet 验证阈值是否被触发；开启 enforceMinReclaim 后，
// 还要求当前可回收量满足 MinReclaim 才算触发。
func TestThresholdsMet(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	hardThreshold := evictionapi.Threshold{
		Signal:   evictionapi.SignalMemoryAvailable,
		Operator: evictionapi.OpLessThan,
		Value: evictionapi.ThresholdValue{
			Quantity: quantityMustParse("1Gi"),
		},
		MinReclaim: &evictionapi.ThresholdValue{
			Quantity: quantityMustParse("500Mi"),
		},
	}
	testCases := map[string]struct {
		enforceMinReclaim bool
		thresholds        []evictionapi.Threshold
		observations      signalObservations
		result            []evictionapi.Threshold
	}{
		"empty": {
			enforceMinReclaim: false,
			thresholds:        []evictionapi.Threshold{},
			observations:      signalObservations{},
			result:            []evictionapi.Threshold{},
		},
		"threshold-met-memory": {
			enforceMinReclaim: false,
			thresholds:        []evictionapi.Threshold{hardThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("500Mi"),
				},
			},
			result: []evictionapi.Threshold{hardThreshold},
		},
		"threshold-not-met": {
			enforceMinReclaim: false,
			thresholds:        []evictionapi.Threshold{hardThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("2Gi"),
				},
			},
			result: []evictionapi.Threshold{},
		},
		"threshold-met-with-min-reclaim": {
			enforceMinReclaim: true,
			thresholds:        []evictionapi.Threshold{hardThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("1.05Gi"),
				},
			},
			result: []evictionapi.Threshold{hardThreshold},
		},
		"threshold-not-met-with-min-reclaim": {
			enforceMinReclaim: true,
			thresholds:        []evictionapi.Threshold{hardThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("2Gi"),
				},
			},
			result: []evictionapi.Threshold{},
		},
	}
	for testName, testCase := range testCases {
		actual := thresholdsMet(logger, testCase.thresholds, testCase.observations, testCase.enforceMinReclaim)
		if !thresholdList(actual).Equal(thresholdList(testCase.result)) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestThresholdsUpdatedStats 验证只保留观测时间晚于上次的阈值：
// 同一时刻或时间倒退的观测视为过期，避免重复触发驱逐。
func TestThresholdsUpdatedStats(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	updatedThreshold := evictionapi.Threshold{
		Signal: evictionapi.SignalMemoryAvailable,
	}
	locationUTC, err := time.LoadLocation("UTC")
	if err != nil {
		t.Error(err)
		return
	}
	testCases := map[string]struct {
		thresholds   []evictionapi.Threshold
		observations signalObservations
		last         signalObservations
		result       []evictionapi.Threshold
	}{
		"empty": {
			thresholds:   []evictionapi.Threshold{},
			observations: signalObservations{},
			last:         signalObservations{},
			result:       []evictionapi.Threshold{},
		},
		"no-time": {
			thresholds: []evictionapi.Threshold{updatedThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{},
			},
			last:   signalObservations{},
			result: []evictionapi.Threshold{updatedThreshold},
		},
		"no-last-observation": {
			thresholds: []evictionapi.Threshold{updatedThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					time: metav1.Date(2016, 1, 1, 0, 0, 0, 0, locationUTC),
				},
			},
			last:   signalObservations{},
			result: []evictionapi.Threshold{updatedThreshold},
		},
		"time-machine": {
			thresholds: []evictionapi.Threshold{updatedThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					time: metav1.Date(2016, 1, 1, 0, 0, 0, 0, locationUTC),
				},
			},
			last: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					time: metav1.Date(2016, 1, 1, 0, 1, 0, 0, locationUTC),
				},
			},
			result: []evictionapi.Threshold{},
		},
		"same-observation": {
			thresholds: []evictionapi.Threshold{updatedThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					time: metav1.Date(2016, 1, 1, 0, 0, 0, 0, locationUTC),
				},
			},
			last: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					time: metav1.Date(2016, 1, 1, 0, 0, 0, 0, locationUTC),
				},
			},
			result: []evictionapi.Threshold{},
		},
		"new-observation": {
			thresholds: []evictionapi.Threshold{updatedThreshold},
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					time: metav1.Date(2016, 1, 1, 0, 1, 0, 0, locationUTC),
				},
			},
			last: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					time: metav1.Date(2016, 1, 1, 0, 0, 0, 0, locationUTC),
				},
			},
			result: []evictionapi.Threshold{updatedThreshold},
		},
	}
	for testName, testCase := range testCases {
		actual := thresholdsUpdatedStats(logger, testCase.thresholds, testCase.observations, testCase.last)
		if !thresholdList(actual).Equal(thresholdList(testCase.result)) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestPercentageThresholdsMet 验证百分比阈值按 available/capacity 计算，
// 且 MinReclaim 为百分比时同样按容量折算。
func TestPercentageThresholdsMet(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	specificThresholds := []evictionapi.Threshold{
		{
			Signal:   evictionapi.SignalMemoryAvailable,
			Operator: evictionapi.OpLessThan,
			Value: evictionapi.ThresholdValue{
				Percentage: 0.2,
			},
			MinReclaim: &evictionapi.ThresholdValue{
				Percentage: 0.05,
			},
		},
		{
			Signal:   evictionapi.SignalNodeFsAvailable,
			Operator: evictionapi.OpLessThan,
			Value: evictionapi.ThresholdValue{
				Percentage: 0.3,
			},
		},
	}

	testCases := map[string]struct {
		enforceMinRelaim bool
		thresholds       []evictionapi.Threshold
		observations     signalObservations
		result           []evictionapi.Threshold
	}{
		"BothMet": {
			enforceMinRelaim: false,
			thresholds:       specificThresholds,
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("100Mi"),
					capacity:  quantityMustParse("1000Mi"),
				},
				evictionapi.SignalNodeFsAvailable: signalObservation{
					available: quantityMustParse("100Gi"),
					capacity:  quantityMustParse("1000Gi"),
				},
			},
			result: specificThresholds,
		},
		"NoneMet": {
			enforceMinRelaim: false,
			thresholds:       specificThresholds,
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("300Mi"),
					capacity:  quantityMustParse("1000Mi"),
				},
				evictionapi.SignalNodeFsAvailable: signalObservation{
					available: quantityMustParse("400Gi"),
					capacity:  quantityMustParse("1000Gi"),
				},
			},
			result: []evictionapi.Threshold{},
		},
		"DiskMet": {
			enforceMinRelaim: false,
			thresholds:       specificThresholds,
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("300Mi"),
					capacity:  quantityMustParse("1000Mi"),
				},
				evictionapi.SignalNodeFsAvailable: signalObservation{
					available: quantityMustParse("100Gi"),
					capacity:  quantityMustParse("1000Gi"),
				},
			},
			result: []evictionapi.Threshold{specificThresholds[1]},
		},
		"MemoryMet": {
			enforceMinRelaim: false,
			thresholds:       specificThresholds,
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("100Mi"),
					capacity:  quantityMustParse("1000Mi"),
				},
				evictionapi.SignalNodeFsAvailable: signalObservation{
					available: quantityMustParse("400Gi"),
					capacity:  quantityMustParse("1000Gi"),
				},
			},
			result: []evictionapi.Threshold{specificThresholds[0]},
		},
		"MemoryMetWithMinReclaim": {
			enforceMinRelaim: true,
			thresholds:       specificThresholds,
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("225Mi"),
					capacity:  quantityMustParse("1000Mi"),
				},
			},
			result: []evictionapi.Threshold{specificThresholds[0]},
		},
		"MemoryNotMetWithMinReclaim": {
			enforceMinRelaim: true,
			thresholds:       specificThresholds,
			observations: signalObservations{
				evictionapi.SignalMemoryAvailable: signalObservation{
					available: quantityMustParse("300Mi"),
					capacity:  quantityMustParse("1000Mi"),
				},
			},
			result: []evictionapi.Threshold{},
		},
	}
	for testName, testCase := range testCases {
		actual := thresholdsMet(logger, testCase.thresholds, testCase.observations, testCase.enforceMinRelaim)
		if !thresholdList(actual).Equal(thresholdList(testCase.result)) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestThresholdsFirstObservedAt 验证首次观测时间只记录一次：
// 已有记录时保留旧时间，宽限期从这一刻开始累计。
func TestThresholdsFirstObservedAt(t *testing.T) {
	hardThreshold := evictionapi.Threshold{
		Signal:   evictionapi.SignalMemoryAvailable,
		Operator: evictionapi.OpLessThan,
		Value: evictionapi.ThresholdValue{
			Quantity: quantityMustParse("1Gi"),
		},
	}
	now := metav1.Now()
	oldTime := metav1.NewTime(now.Time.Add(-1 * time.Minute))
	testCases := map[string]struct {
		thresholds     []evictionapi.Threshold
		lastObservedAt thresholdsObservedAt
		now            time.Time
		result         thresholdsObservedAt
	}{
		"empty": {
			thresholds:     []evictionapi.Threshold{},
			lastObservedAt: thresholdsObservedAt{},
			now:            now.Time,
			result:         thresholdsObservedAt{},
		},
		"no-previous-observation": {
			thresholds:     []evictionapi.Threshold{hardThreshold},
			lastObservedAt: thresholdsObservedAt{},
			now:            now.Time,
			result: thresholdsObservedAt{
				hardThreshold: now.Time,
			},
		},
		"previous-observation": {
			thresholds: []evictionapi.Threshold{hardThreshold},
			lastObservedAt: thresholdsObservedAt{
				hardThreshold: oldTime.Time,
			},
			now: now.Time,
			result: thresholdsObservedAt{
				hardThreshold: oldTime.Time,
			},
		},
	}
	for testName, testCase := range testCases {
		actual := thresholdsFirstObservedAt(testCase.thresholds, testCase.lastObservedAt, testCase.now)
		if !reflect.DeepEqual(actual, testCase.result) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestThresholdsMetGracePeriod 验证硬阈值立即生效，软阈值必须持续超过 GracePeriod 才生效。
func TestThresholdsMetGracePeriod(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	now := metav1.Now()
	hardThreshold := evictionapi.Threshold{
		Signal:   evictionapi.SignalMemoryAvailable,
		Operator: evictionapi.OpLessThan,
		Value: evictionapi.ThresholdValue{
			Quantity: quantityMustParse("1Gi"),
		},
	}
	softThreshold := evictionapi.Threshold{
		Signal:   evictionapi.SignalMemoryAvailable,
		Operator: evictionapi.OpLessThan,
		Value: evictionapi.ThresholdValue{
			Quantity: quantityMustParse("2Gi"),
		},
		GracePeriod: 1 * time.Minute,
	}
	oldTime := metav1.NewTime(now.Time.Add(-2 * time.Minute))
	testCases := map[string]struct {
		observedAt thresholdsObservedAt
		now        time.Time
		result     []evictionapi.Threshold
	}{
		"empty": {
			observedAt: thresholdsObservedAt{},
			now:        now.Time,
			result:     []evictionapi.Threshold{},
		},
		"hard-threshold-met": {
			observedAt: thresholdsObservedAt{
				hardThreshold: now.Time,
			},
			now:    now.Time,
			result: []evictionapi.Threshold{hardThreshold},
		},
		"soft-threshold-not-met": {
			observedAt: thresholdsObservedAt{
				softThreshold: now.Time,
			},
			now:    now.Time,
			result: []evictionapi.Threshold{},
		},
		"soft-threshold-met": {
			observedAt: thresholdsObservedAt{
				softThreshold: oldTime.Time,
			},
			now:    now.Time,
			result: []evictionapi.Threshold{softThreshold},
		},
	}
	for testName, testCase := range testCases {
		actual := thresholdsMetGracePeriod(logger, testCase.observedAt, now.Time)
		if !thresholdList(actual).Equal(thresholdList(testCase.result)) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestNodeConditions 验证阈值信号到 NodeCondition 的映射，例如 memory.available 对应 MemoryPressure。
func TestNodeConditions(t *testing.T) {
	testCases := map[string]struct {
		inputs []evictionapi.Threshold
		result []v1.NodeConditionType
	}{
		"empty-list": {
			inputs: []evictionapi.Threshold{},
			result: []v1.NodeConditionType{},
		},
		"memory.available": {
			inputs: []evictionapi.Threshold{
				{Signal: evictionapi.SignalMemoryAvailable},
			},
			result: []v1.NodeConditionType{v1.NodeMemoryPressure},
		},
	}
	for testName, testCase := range testCases {
		actual := nodeConditions(testCase.inputs)
		if !nodeConditionList(actual).Equal(nodeConditionList(testCase.result)) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestNodeConditionsLastObservedAt 验证条件仍存在时刷新观测时间，条件消失时保留旧时间，
// 供条件退出时的宽限判断使用。
func TestNodeConditionsLastObservedAt(t *testing.T) {
	now := metav1.Now()
	oldTime := metav1.NewTime(now.Time.Add(-1 * time.Minute))
	testCases := map[string]struct {
		nodeConditions []v1.NodeConditionType
		lastObservedAt nodeConditionsObservedAt
		now            time.Time
		result         nodeConditionsObservedAt
	}{
		"no-previous-observation": {
			nodeConditions: []v1.NodeConditionType{v1.NodeMemoryPressure},
			lastObservedAt: nodeConditionsObservedAt{},
			now:            now.Time,
			result: nodeConditionsObservedAt{
				v1.NodeMemoryPressure: now.Time,
			},
		},
		"previous-observation": {
			nodeConditions: []v1.NodeConditionType{v1.NodeMemoryPressure},
			lastObservedAt: nodeConditionsObservedAt{
				v1.NodeMemoryPressure: oldTime.Time,
			},
			now: now.Time,
			result: nodeConditionsObservedAt{
				v1.NodeMemoryPressure: now.Time,
			},
		},
		"old-observation": {
			nodeConditions: []v1.NodeConditionType{},
			lastObservedAt: nodeConditionsObservedAt{
				v1.NodeMemoryPressure: oldTime.Time,
			},
			now: now.Time,
			result: nodeConditionsObservedAt{
				v1.NodeMemoryPressure: oldTime.Time,
			},
		},
	}
	for testName, testCase := range testCases {
		actual := nodeConditionsLastObservedAt(testCase.nodeConditions, testCase.lastObservedAt, testCase.now)
		if !reflect.DeepEqual(actual, testCase.result) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestNodeConditionsObservedSince 验证只返回在给定周期内被观测到的条件。
func TestNodeConditionsObservedSince(t *testing.T) {
	now := metav1.Now()
	observedTime := metav1.NewTime(now.Time.Add(-1 * time.Minute))
	testCases := map[string]struct {
		observedAt nodeConditionsObservedAt
		period     time.Duration
		now        time.Time
		result     []v1.NodeConditionType
	}{
		"in-period": {
			observedAt: nodeConditionsObservedAt{
				v1.NodeMemoryPressure: observedTime.Time,
			},
			period: 2 * time.Minute,
			now:    now.Time,
			result: []v1.NodeConditionType{v1.NodeMemoryPressure},
		},
		"out-of-period": {
			observedAt: nodeConditionsObservedAt{
				v1.NodeMemoryPressure: observedTime.Time,
			},
			period: 30 * time.Second,
			now:    now.Time,
			result: []v1.NodeConditionType{},
		},
	}
	for testName, testCase := range testCases {
		actual := nodeConditionsObservedSince(testCase.observedAt, testCase.period, testCase.now)
		if !nodeConditionList(actual).Equal(nodeConditionList(testCase.result)) {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestHasNodeConditions 验证节点条件列表的成员查找。
func TestHasNodeConditions(t *testing.T) {
	testCases := map[string]struct {
		inputs []v1.NodeConditionType
		item   v1.NodeConditionType
		result bool
	}{
		"has-condition": {
			inputs: []v1.NodeConditionType{v1.NodeReady, v1.NodeDiskPressure, v1.NodeMemoryPressure},
			item:   v1.NodeMemoryPressure,
			result: true,
		},
		"does-not-have-condition": {
			inputs: []v1.NodeConditionType{v1.NodeReady, v1.NodeDiskPressure},
			item:   v1.NodeMemoryPressure,
			result: false,
		},
	}
	for testName, testCase := range testCases {
		if actual := hasNodeCondition(testCase.inputs, testCase.item); actual != testCase.result {
			t.Errorf("Test case: %s, expected: %v, actual: %v", testName, testCase.result, actual)
		}
	}
}

// TestParsePercentage 验证 parsePercentage 只接受"数字%"形式，其余输入一律报错。
func TestParsePercentage(t *testing.T) {
	testCases := map[string]struct {
		hasError bool
		value    float32
	}{
		"blah": {
			hasError: true,
		},
		"25.5%": {
			value: 0.255,
		},
		"foo%": {
			hasError: true,
		},
		"12%345": {
			hasError: true,
		},
	}
	for input, expected := range testCases {
		value, err := parsePercentage(input)
		if (err != nil) != expected.hasError {
			t.Errorf("Test case: %s, expected: %v, actual: %v", input, expected.hasError, err != nil)
		}
		if value != expected.value {
			t.Errorf("Test case: %s, expected: %v, actual: %v", input, expected.value, value)
		}
	}
}

// TestCompareThresholdValue 验证阈值比较只认同一种表示形式：
// 数量与百分比即使折算后相等也算不同，且比较必须对称。
func TestCompareThresholdValue(t *testing.T) {
	testCases := []struct {
		a, b  evictionapi.ThresholdValue
		equal bool
	}{
		{
			a: evictionapi.ThresholdValue{
				Quantity: resource.NewQuantity(123, resource.BinarySI),
			},
			b: evictionapi.ThresholdValue{
				Quantity: resource.NewQuantity(123, resource.BinarySI),
			},
			equal: true,
		},
		{
			a: evictionapi.ThresholdValue{
				Quantity: resource.NewQuantity(123, resource.BinarySI),
			},
			b: evictionapi.ThresholdValue{
				Quantity: resource.NewQuantity(456, resource.BinarySI),
			},
			equal: false,
		},
		{
			a: evictionapi.ThresholdValue{
				Quantity: resource.NewQuantity(123, resource.BinarySI),
			},
			b: evictionapi.ThresholdValue{
				Percentage: 0.1,
			},
			equal: false,
		},
		{
			a: evictionapi.ThresholdValue{
				Percentage: 0.1,
			},
			b: evictionapi.ThresholdValue{
				Percentage: 0.1,
			},
			equal: true,
		},
		{
			a: evictionapi.ThresholdValue{
				Percentage: 0.2,
			},
			b: evictionapi.ThresholdValue{
				Percentage: 0.1,
			},
			equal: false,
		},
	}

	for i, testCase := range testCases {
		if compareThresholdValue(testCase.a, testCase.b) != testCase.equal ||
			compareThresholdValue(testCase.b, testCase.a) != testCase.equal {
			t.Errorf("Test case: %v failed", i)
		}
	}
}

// TestAddContainerFsThresholds 验证 UpdateContainerFsThresholds 根据 imagefs/containerfs
// 是否独立决定是否生成 containerfs 阈值；已存在 containerfs 阈值时再次添加应报错。
func TestAddContainerFsThresholds(t *testing.T) {
	gracePeriod := time.Duration(1)
	testCases := []struct {
		description                   string
		imageFs                       bool
		containerFs                   bool
		expectedContainerFsHard       evictionapi.Threshold
		expectedContainerFsSoft       evictionapi.Threshold
		expectedContainerFsINodesHard evictionapi.Threshold
		expectedContainerFsINodesSoft evictionapi.Threshold
		expectErr                     bool
		thresholdList                 []evictionapi.Threshold
	}{
		{
			description: "single filesystem",
			imageFs:     false,
			containerFs: false,
			expectedContainerFsHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				GracePeriod: gracePeriod,
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: gracePeriod,
			},

			thresholdList: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},

				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
			},
		},
		{
			description: "image filesystem",
			imageFs:     true,
			containerFs: false,
			expectedContainerFsHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("150Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1.5Gi"),
				},
			},
			expectedContainerFsSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("300Mi"),
				},
				GracePeriod: gracePeriod,
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("3Gi"),
				},
			},
			expectedContainerFsINodesHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("300Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
			},
			expectedContainerFsINodesSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("150Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: gracePeriod,
			},
			thresholdList: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1.5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1.5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("3Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
			},
		},
		{
			description: "container and image are separate",
			imageFs:     true,
			containerFs: true,
			expectedContainerFsHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				GracePeriod: gracePeriod,
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: gracePeriod,
			},
			thresholdList: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1.5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("3Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
			},
		},
		{
			description: "single filesystem; existing containerfsstats",
			imageFs:     false,
			containerFs: false,
			expectErr:   true,
			expectedContainerFsHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				GracePeriod: gracePeriod,
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: gracePeriod,
			},

			thresholdList: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},

				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalContainerFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},

				{
					Signal:   evictionapi.SignalContainerFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalContainerFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalContainerFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},
			},
		},
		{
			description: "image filesystem; expect error",
			imageFs:     true,
			containerFs: false,
			expectErr:   true,
			expectedContainerFsHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("150Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1.5Gi"),
				},
			},
			expectedContainerFsSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("300Mi"),
				},
				GracePeriod: gracePeriod,
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("3Gi"),
				},
			},
			expectedContainerFsINodesHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("300Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
			},
			expectedContainerFsINodesSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("150Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: gracePeriod,
			},
			thresholdList: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1.5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1.5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("3Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalContainerFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},

				{
					Signal:   evictionapi.SignalContainerFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalContainerFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalContainerFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},
			},
		},
		{
			description: "container and image are separate; expect error",
			imageFs:     true,
			containerFs: true,
			expectErr:   true,
			expectedContainerFsHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				GracePeriod: gracePeriod,
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesHard: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("100Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedContainerFsINodesSoft: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsInodesFree,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("200Mi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: gracePeriod,
			},
			thresholdList: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("200Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("100Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1.5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("3Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("150Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("300Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalContainerFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},

				{
					Signal:   evictionapi.SignalContainerFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					GracePeriod: gracePeriod,
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalContainerFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
					GracePeriod: gracePeriod,
				},
				{
					Signal:   evictionapi.SignalContainerFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("500Mi"),
					},
					MinReclaim: &evictionapi.ThresholdValue{
						Quantity: quantityMustParse("5Gi"),
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			expected, err := UpdateContainerFsThresholds(testCase.thresholdList, testCase.imageFs, testCase.containerFs)
			if err != nil && !testCase.expectErr {
				t.Fatalf("got error but did not expect any")
			}
			// 逐个记录四类 containerfs 阈值的下标，-1 表示缺失；最后统一断言四者都能找到。
			hardContainerFsMatch := -1
			softContainerFsMatch := -1
			hardContainerFsINodesMatch := -1
			softContainerFsINodesMatch := -1
			for idx, val := range expected {
				if val.Signal == evictionapi.SignalContainerFsAvailable && isHardEvictionThreshold(val) {
					if !reflect.DeepEqual(val, testCase.expectedContainerFsHard) {
						t.Fatalf("want %v got %v", testCase.expectedContainerFsHard, val)
					}
					hardContainerFsMatch = idx
				}
				if val.Signal == evictionapi.SignalContainerFsAvailable && !isHardEvictionThreshold(val) {
					if !reflect.DeepEqual(val, testCase.expectedContainerFsSoft) {
						t.Fatalf("want %v got %v", testCase.expectedContainerFsSoft, val)
					}
					softContainerFsMatch = idx
				}
				if val.Signal == evictionapi.SignalContainerFsInodesFree && isHardEvictionThreshold(val) {
					if !reflect.DeepEqual(val, testCase.expectedContainerFsINodesHard) {
						t.Fatalf("want %v got %v", testCase.expectedContainerFsINodesHard, val)
					}
					hardContainerFsINodesMatch = idx
				}
				if val.Signal == evictionapi.SignalContainerFsInodesFree && !isHardEvictionThreshold(val) {
					if !reflect.DeepEqual(val, testCase.expectedContainerFsINodesSoft) {
						t.Fatalf("want %v got %v", testCase.expectedContainerFsINodesSoft, val)
					}
					softContainerFsINodesMatch = idx
				}
			}
			if hardContainerFsMatch == -1 {
				t.Fatalf("did not find hard containerfs.available")
			}
			if softContainerFsMatch == -1 {
				t.Fatalf("did not find soft containerfs.available")
			}
			if hardContainerFsINodesMatch == -1 {
				t.Fatalf("did not find hard containerfs.inodesfree")
			}
			if softContainerFsINodesMatch == -1 {
				t.Fatalf("did not find soft containerfs.inodesfree")
			}
		})
	}
}

// newPodInodeStats 按给定用量构造 inode 统计：每个容器都带 rootfs 与 logs，
// 每个本地卷带一份卷统计，便于比较器累加。
func newPodInodeStats(pod *v1.Pod, rootFsInodesUsed, logsInodesUsed, perLocalVolumeInodesUsed resource.Quantity) statsapi.PodStats {
	result := statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID),
		},
	}
	rootFsUsed := uint64(rootFsInodesUsed.Value())
	logsUsed := uint64(logsInodesUsed.Value())
	for range pod.Spec.Containers {
		result.Containers = append(result.Containers, statsapi.ContainerStats{
			Rootfs: &statsapi.FsStats{
				InodesUsed: &rootFsUsed,
			},
			Logs: &statsapi.FsStats{
				InodesUsed: &logsUsed,
			},
		})
	}

	perLocalVolumeUsed := uint64(perLocalVolumeInodesUsed.Value())
	for _, volumeName := range localVolumeNames(pod) {
		result.VolumeStats = append(result.VolumeStats, statsapi.VolumeStats{
			Name: volumeName,
			FsStats: statsapi.FsStats{
				InodesUsed: &perLocalVolumeUsed,
			},
		})
	}
	return result
}

// newPodDiskStats 按给定用量构造磁盘统计：每个容器都带 rootfs 与 logs，
// 每个本地卷带一份卷统计，便于比较器累加。
func newPodDiskStats(pod *v1.Pod, rootFsUsed, logsUsed, perLocalVolumeUsed resource.Quantity) statsapi.PodStats {
	result := statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID),
		},
	}

	rootFsUsedBytes := uint64(rootFsUsed.Value())
	logsUsedBytes := uint64(logsUsed.Value())
	for _, container := range pod.Spec.Containers {
		result.Containers = append(result.Containers, statsapi.ContainerStats{
			Name: container.Name,
			Rootfs: &statsapi.FsStats{
				UsedBytes: &rootFsUsedBytes,
			},
			Logs: &statsapi.FsStats{
				UsedBytes: &logsUsedBytes,
			},
		})
	}

	perLocalVolumeUsedBytes := uint64(perLocalVolumeUsed.Value())
	for _, volumeName := range localVolumeNames(pod) {
		result.VolumeStats = append(result.VolumeStats, statsapi.VolumeStats{
			Name: volumeName,
			FsStats: statsapi.FsStats{
				UsedBytes: &perLocalVolumeUsedBytes,
			},
		})
	}

	return result
}

// newPodMemoryStats 构造内存统计；同时填上卷、容器和临时存储字段，
// 让同一份统计也能被磁盘类比较器使用。
func newPodMemoryStats(pod *v1.Pod, workingSet resource.Quantity) statsapi.PodStats {
	workingSetBytes := uint64(workingSet.Value())
	return statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID),
		},
		Memory: &statsapi.MemoryStats{
			WorkingSetBytes: &workingSetBytes,
		},
		VolumeStats: []statsapi.VolumeStats{
			{
				FsStats: statsapi.FsStats{
					UsedBytes: &workingSetBytes,
				},
				Name: "local-volume",
			},
		},
		Containers: []statsapi.ContainerStats{
			{
				Name: pod.Name,
				Logs: &statsapi.FsStats{
					UsedBytes: &workingSetBytes,
				},
				Rootfs: &statsapi.FsStats{UsedBytes: &workingSetBytes},
			},
		},
		EphemeralStorage: &statsapi.FsStats{UsedBytes: &workingSetBytes},
	}
}

// newPodProcessStats 构造只含进程数的统计，用于进程数比较器。
func newPodProcessStats(pod *v1.Pod, num uint64) statsapi.PodStats {
	return statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID),
		},
		ProcessStats: &statsapi.ProcessStats{
			ProcessCount: &num,
		},
	}
}

// newResourceList 按非空参数拼出 ResourceList，空字符串表示该资源不设置。
func newResourceList(cpu, memory, disk string) v1.ResourceList {
	res := v1.ResourceList{}
	if cpu != "" {
		res[v1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		res[v1.ResourceMemory] = resource.MustParse(memory)
	}
	if disk != "" {
		res[v1.ResourceEphemeralStorage] = resource.MustParse(disk)
	}
	return res
}

// newResourceRequirements 把请求与限制组装成 ResourceRequirements。
func newResourceRequirements(requests, limits v1.ResourceList) v1.ResourceRequirements {
	res := v1.ResourceRequirements{}
	res.Requests = requests
	res.Limits = limits
	return res
}

// newContainer 构造带资源声明的容器。
func newContainer(name string, requests v1.ResourceList, limits v1.ResourceList) v1.Container {
	return v1.Container{
		Name:      name,
		Resources: newResourceRequirements(requests, limits),
	}
}

// newRestartableInitContainer 构造可重启 init 容器（RestartPolicy=Always），
// 这类容器会参与常规容器的资源计算。
func newRestartableInitContainer(name string, requests v1.ResourceList, limits v1.ResourceList) v1.Container {
	restartAlways := v1.ContainerRestartPolicyAlways
	container := newContainer(name, requests, limits)
	container.RestartPolicy = &restartAlways
	return container
}

// newVolume 构造卷声明。
func newVolume(name string, volumeSource v1.VolumeSource) v1.Volume {
	return v1.Volume{
		Name:         name,
		VolumeSource: volumeSource,
	}
}

// newPod 用名字充当 UID，因此测试中名字必须唯一；否则 statsFunc 的 map 查找会串到别的 Pod。
func newPod(name string, priority int32, containers []v1.Container, volumes []v1.Volume) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID(name),
		},
		Spec: v1.PodSpec{
			Containers: containers,
			Volumes:    volumes,
			Priority:   &priority,
		},
	}
}

// newPodWithInitContainers 在 newPod 基础上补充 init 容器和 pod 级资源。
func newPodWithInitContainers(name string, priority int32, resources v1.ResourceRequirements, initContainers []v1.Container, containers []v1.Container, volumes []v1.Volume) *v1.Pod {
	pod := newPod(name, priority, containers, volumes)

	pod.Spec.InitContainers = initContainers
	pod.Spec.Resources = &resources

	return pod
}

// nodeConditionList 是节点条件切片的别名，提供与顺序无关的相等判断。
type nodeConditionList []v1.NodeConditionType

// Equal 以不区分顺序的方式比较两组节点条件。
func (s1 nodeConditionList) Equal(s2 nodeConditionList) bool {
	if len(s1) != len(s2) {
		return false
	}
	for _, item := range s1 {
		if !hasNodeCondition(s2, item) {
			return false
		}
	}
	return true
}

// thresholdList 是阈值切片的别名，提供与顺序无关的相等判断。
type thresholdList []evictionapi.Threshold

// Equal 以不区分顺序的方式比较两组阈值。
func (s1 thresholdList) Equal(s2 thresholdList) bool {
	if len(s1) != len(s2) {
		return false
	}
	for _, item := range s1 {
		if !hasThreshold(s2, item) {
			return false
		}
	}
	return true
}

// TestStatsNotFoundForPod 验证 statsFunc 查不到 Pod 统计时，所有比较器都返回 0，
// 保证排序稳定且不会 panic。
func TestStatsNotFoundForPod(t *testing.T) {
	pod1 := newPod("fake-pod1", defaultPriority, []v1.Container{
		newContainer("fake-container1", newResourceList("", "", ""), newResourceList("", "", "")),
	}, nil)
	pod2 := newPod("fake-pod2", defaultPriority, []v1.Container{
		newContainer("fake-container2", newResourceList("", "", ""), newResourceList("", "", "")),
	}, nil)
	statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
		return statsapi.PodStats{}, false
	}

	for _, test := range []struct {
		description string
		compFunc    func(stats statsFunc) cmpFunc
	}{
		{
			description: "process",
			compFunc:    process,
		},
		{
			description: "memory",
			compFunc:    memory,
		},
		{
			description: "exceedMemoryRequests",
			compFunc:    exceedMemoryRequests,
		},
		{
			description: "exceedDiskRequests",
			compFunc: func(stats statsFunc) cmpFunc {
				return exceedDiskRequests(stats, nil, "")
			},
		},
		{
			description: "disk",
			compFunc: func(stats statsFunc) cmpFunc {
				return disk(stats, nil, "")
			},
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			assert.Equal(t, 0, test.compFunc(statsFn)(pod1, pod2), "unexpected default result")
		})
	}
}

// TestEvictionMessage 验证驱逐消息和注解只列出超出请求的容器（普通容器、可重启 init 容器）
// 或 pod 级资源，并带上对应请求值与实际用量。
func TestEvictionMessage(t *testing.T) {
	type containerMemoryStat struct {
		name  string
		usage string
	}
	type memoryExceededContainer struct {
		name    string
		request string
		usage   string
	}
	// 按被测代码的格式拼出期望消息，避免在测试里硬编码整段字符串。
	memoryExceededEvictionMessage := func(podMessage string, containers []memoryExceededContainer) string {
		resourceToReclaim := v1.ResourceMemory
		containersMessage := ""
		for _, container := range containers {
			containersMessage += fmt.Sprintf(containerMessageFmt, container.name, container.usage, container.request, resourceToReclaim)
		}
		return fmt.Sprintf(nodeLowMessageFmt, resourceToReclaim) + podMessage + containersMessage
	}

	const (
		podName          = "pod-eviction-message"
		init1            = "init1"
		init2            = "init2"
		init3            = "init3"
		restartableInit1 = "restartable-init1"
		restartableInit2 = "restartable-init2"
		restartableInit3 = "restartable-init3"
		regular1         = "regular1"
		regular2         = "regular2"
		regular3         = "regular3"
		ephemeral1       = "ephemeral1"
	)

	testcase := []struct {
		name                     string
		podResources             v1.ResourceRequirements
		initContainers           []v1.Container
		containers               []v1.Container
		ephemeralContainers      []v1.EphemeralContainer
		podMemoryStats           string
		containerMemoryStats     []containerMemoryStat
		expectedContainerMessage []memoryExceededContainer
		expectedAnnotations      map[string]string
		podLevelResourcesEnabled bool
	}{
		{
			name: "No container exceeds memory usage",
			initContainers: []v1.Container{
				newContainer(init1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			containers: []v1.Container{
				newContainer(regular1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			// Terminated init containers aren't included in the containerMemoryStats
			containerMemoryStats: []containerMemoryStat{
				{name: restartableInit1, usage: "150Mi"},
				{name: regular1, usage: "100Mi"},
			},
			expectedContainerMessage: nil,
		},
		{
			name: "Init container exceeds memory usage",
			initContainers: []v1.Container{
				newContainer(init1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
				newContainer(init2, newResourceList("", "150Mi", ""), newResourceList("", "", "")),
			},
			containers: []v1.Container{
				newContainer(regular1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			// An eviction occurred while the init container was running
			containerMemoryStats: []containerMemoryStat{
				{name: init1, usage: "150Mi"},
			},
			expectedContainerMessage: []memoryExceededContainer{
				{name: init1, request: "100Mi", usage: "150Mi"},
			},
			expectedAnnotations: map[string]string{
				OffendingContainersKey:      init1,
				OffendingContainersUsageKey: "150Mi",
			},
		},
		{
			name: "Restartable init container exceeds memory usage",
			initContainers: []v1.Container{
				newContainer(init1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit2, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			containers: []v1.Container{
				newContainer(regular1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			// Terminated init containers aren't included in the containerMemoryStats
			containerMemoryStats: []containerMemoryStat{
				{name: restartableInit1, usage: "250Mi"},
				{name: restartableInit2, usage: "150Mi"},
				{name: regular1, usage: "200Mi"},
			},
			expectedContainerMessage: []memoryExceededContainer{
				{name: restartableInit1, request: "200Mi", usage: "250Mi"},
			},
			expectedAnnotations: map[string]string{
				OffendingContainersKey:      restartableInit1,
				OffendingContainersUsageKey: "250Mi",
			},
		},
		{
			name: "Regular container exceeds memory usage",
			initContainers: []v1.Container{
				newContainer(init1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			containers: []v1.Container{
				newContainer(regular1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
				newContainer(regular2, newResourceList("", "300Mi", ""), newResourceList("", "", "")),
			},
			// Terminated init containers aren't included in the containerMemoryStats
			containerMemoryStats: []containerMemoryStat{
				{name: restartableInit1, usage: "200Mi"},
				{name: regular1, usage: "250Mi"},
				{name: regular2, usage: "250Mi"},
			},
			expectedContainerMessage: []memoryExceededContainer{
				{name: regular1, request: "200Mi", usage: "250Mi"},
			},
			expectedAnnotations: map[string]string{
				OffendingContainersKey:      regular1,
				OffendingContainersUsageKey: "250Mi",
			},
		},
		{
			name: "Ephemeral container exceeds memory usage",
			initContainers: []v1.Container{
				newContainer(init1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			containers: []v1.Container{
				newContainer(regular1, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
			},
			ephemeralContainers: []v1.EphemeralContainer{
				{TargetContainerName: regular1, EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: ephemeral1}},
			},
			// Terminated init containers aren't included in the containerMemoryStats
			containerMemoryStats: []containerMemoryStat{
				{name: restartableInit1, usage: "200Mi"},
				{name: regular1, usage: "150Mi"},
				{name: ephemeral1, usage: "250Mi"},
			},
			expectedContainerMessage: nil,
		},
		{
			name: "Both regular and restartable init containers exceed memory usage due to missing memory requests",
			initContainers: []v1.Container{
				newContainer(init1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit1, newResourceList("", "", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit2, newResourceList("", "300Mi", ""), newResourceList("", "", "")),
			},
			containers: []v1.Container{
				newContainer("regular1", newResourceList("", "", ""), newResourceList("", "", "")),
			},
			// Terminated init containers aren't included in the containerMemoryStats
			containerMemoryStats: []containerMemoryStat{
				{name: restartableInit1, usage: "250Mi"},
				{name: restartableInit2, usage: "250Mi"},
				{name: regular1, usage: "200Mi"},
			},
			expectedContainerMessage: []memoryExceededContainer{
				{name: restartableInit1, request: "0", usage: "250Mi"},
				{name: regular1, request: "0", usage: "200Mi"},
			},
			expectedAnnotations: map[string]string{
				OffendingContainersKey:      strings.Join([]string{restartableInit1, regular1}, ","),
				OffendingContainersUsageKey: "250Mi,200Mi",
			},
		},
		{
			name: "Multiple regular and restartable init containers exceed memory usage",
			initContainers: []v1.Container{
				newContainer(init1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newContainer(init2, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newContainer(init3, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit1, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit2, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
				newRestartableInitContainer(restartableInit3, newResourceList("", "300Mi", ""), newResourceList("", "", "")),
			},
			containers: []v1.Container{
				newContainer(regular1, newResourceList("", "300Mi", ""), newResourceList("", "", "")),
				newContainer(regular2, newResourceList("", "200Mi", ""), newResourceList("", "", "")),
				newContainer(regular3, newResourceList("", "100Mi", ""), newResourceList("", "", "")),
			},
			// Terminated init containers aren't included in the containerMemoryStats
			containerMemoryStats: []containerMemoryStat{
				{name: restartableInit1, usage: "150Mi"},
				{name: restartableInit2, usage: "250Mi"},
				{name: restartableInit3, usage: "250Mi"},
				{name: regular1, usage: "50Mi"},
				{name: regular2, usage: "250Mi"},
				{name: regular3, usage: "400Mi"},
			},
			expectedContainerMessage: []memoryExceededContainer{
				{name: restartableInit1, request: "100Mi", usage: "150Mi"},
				{name: restartableInit2, request: "200Mi", usage: "250Mi"},
				{name: regular2, request: "200Mi", usage: "250Mi"},
				{name: regular3, request: "100Mi", usage: "400Mi"},
			},
			expectedAnnotations: map[string]string{
				OffendingContainersKey:      strings.Join([]string{restartableInit1, restartableInit2, regular2, regular3}, ","),
				OffendingContainersUsageKey: "150Mi,250Mi,250Mi,400Mi",
			},
		},
		{
			name: "Regular pod with pod level resources exceeds memory usage",
			podResources: v1.ResourceRequirements{
				Requests: newResourceList("", "250Mi", ""),
				Limits:   newResourceList("", "1000Mi", ""),
			},
			podMemoryStats: "600Mi",
			containers: []v1.Container{
				newContainer(regular1, newResourceList("", "", ""), newResourceList("", "", "")),
				newContainer(regular2, newResourceList("", "", ""), newResourceList("", "", "")),
			},
			containerMemoryStats: []containerMemoryStat{
				{name: regular1, usage: "250Mi"},
				{name: regular2, usage: "350Mi"},
			},
			expectedContainerMessage: []memoryExceededContainer{
				{name: regular1, request: "0", usage: "250Mi"},
				{name: regular2, request: "0", usage: "350Mi"},
			},
			expectedAnnotations: map[string]string{
				OffendingPodKey:             podName,
				OffendingPodUsageKey:        "600Mi",
				OffendingContainersKey:      strings.Join([]string{regular1, regular2}, ","),
				OffendingContainersUsageKey: "250Mi,350Mi",
			},
			podLevelResourcesEnabled: true,
		},
	}

	threshold := []evictionapi.Threshold{}
	observations := signalObservations{}
	for _, tc := range testcase {
		// 自定义写法的容器列表里包含可重启 init 容器，这里保留其 RestartPolicy 供被测逻辑区分。
		pod := newPodWithInitContainers(podName, 1, tc.podResources, tc.initContainers, tc.containers, nil)
		if len(tc.ephemeralContainers) > 0 {
			pod.Spec.EphemeralContainers = tc.ephemeralContainers
		}
		// Create PodMemoryStats with dummy container memory
		// and override container stats with the provided values.
		dummyContainerMemory := resource.MustParse("1Mi")
		if tc.podLevelResourcesEnabled {
			dummyContainerMemory = resource.MustParse(tc.podMemoryStats)
		}
		podStats := newPodMemoryStats(pod, dummyContainerMemory)
		podStats.Containers = make([]statsapi.ContainerStats, len(tc.containerMemoryStats))
		for _, stat := range tc.containerMemoryStats {
			memoryStat := resource.MustParse(stat.usage)
			memoryBytes := uint64(memoryStat.Value())
			podStats.Containers = append(podStats.Containers, statsapi.ContainerStats{
				Name: stat.name,
				Memory: &statsapi.MemoryStats{
					WorkingSetBytes: &memoryBytes,
				},
			})
		}
		stats := map[*v1.Pod]statsapi.PodStats{
			pod: podStats,
		}
		statsFn := func(pod *v1.Pod) (statsapi.PodStats, bool) {
			result, found := stats[pod]
			return result, found
		}

		t.Run(tc.name, func(t *testing.T) {
			podMessage := ""
			// 默认只校验容器维度的两个注解；启用 pod 级资源后再追加 pod 维度注解。
			annotationsKeys := []string{OffendingContainersKey, OffendingContainersUsageKey}
			if tc.podLevelResourcesEnabled {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, true)
				podMessage = fmt.Sprintf(podMessageFmt, pod.Name, tc.podMemoryStats, tc.podResources.Requests.Memory(), v1.ResourceMemory)
				annotationsKeys = append(annotationsKeys, OffendingPodKey, OffendingPodUsageKey)
			}

			msg, annotations := evictionMessage(v1.ResourceMemory, pod, statsFn, threshold, observations)

			if msg != memoryExceededEvictionMessage(podMessage, tc.expectedContainerMessage) {
				t.Errorf("Unexpected memory exceeded eviction message found, got: %s, want : %s", msg, tc.expectedContainerMessage)
			}
			for _, key := range annotationsKeys {
				val, ok := annotations[key]
				if !ok {
					t.Errorf("Expected annotation %s not found", key)
				}
				if val != tc.expectedAnnotations[key] {
					t.Errorf("Unexpected annotation value for %s key found, got: %s, want: %s", key, val, tc.expectedAnnotations[key])
				}
			}
		})
	}
}
