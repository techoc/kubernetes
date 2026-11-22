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

// Package nestedpendingoperations 测试卷操作的嵌套挂起逻辑。
package nestedpendingoperations

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	// testTimeout 是等待 goroutine 结束的超时时间。理论上只是“上下文切换”，几毫秒足矣；
	// 但 Clayton 说过：“曾经有测试以为睡 15 秒就够，结果因此出现偶发失败。”故留足一分钟。
	testTimeout time.Duration = 1 * time.Minute

	// initialOperationWaitTimeShort 是等待操作完成的最初时长；每次失败后按指数退避递增。
	initialOperationWaitTimeShort time.Duration = 20 * time.Millisecond

	// initialOperationWaitTimeLong 是等待操作完成的最初时长；每次失败后按指数退避递增。
	// 用于退避本身耗时较长的场景。
	initialOperationWaitTimeLong time.Duration = 500 * time.Millisecond
)

// Test_NestedPendingOperations_Positive_SingleOp 验证单个操作可正常启动并执行。
func Test_NestedPendingOperations_Positive_SingleOp(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")

	// 执行
	err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: noopFunc})

	// 断言
	if err != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err)
	}
}

// Test_NestedPendingOperations_Positive_TwoOps 验证不同卷上的两个操作可并行执行。
func Test_NestedPendingOperations_Positive_TwoOps(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume2Name := v1.UniqueVolumeName("volume2-name")

	// 执行
	err1 := grm.Run(volume1Name, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: noopFunc})
	err2 := grm.Run(volume2Name, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: noopFunc})

	// 断言
	if err1 != nil {
		t.Fatalf("NestedPendingOperations %q failed. Expected: <no error> Actual: <%v>", volume1Name, err1)
	}

	if err2 != nil {
		t.Fatalf("NestedPendingOperations %q failed. Expected: <no error> Actual: <%v>", volume2Name, err2)
	}
}

// Test_NestedPendingOperations_Positive_TwoSubOps 验证同一卷下不同 Pod 的两个子操作可并行执行。
func Test_NestedPendingOperations_Positive_TwoSubOps(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1PodName := volumetypes.UniquePodName("operation1-podname")
	operation2PodName := volumetypes.UniquePodName("operation2-podname")

	// 执行
	err1 := grm.Run(volumeName, operation1PodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: noopFunc})
	err2 := grm.Run(volumeName, operation2PodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: noopFunc})

	// 断言
	if err1 != nil {
		t.Fatalf("NestedPendingOperations %q failed. Expected: <no error> Actual: <%v>", operation1PodName, err1)
	}

	if err2 != nil {
		t.Fatalf("NestedPendingOperations %q failed. Expected: <no error> Actual: <%v>", operation2PodName, err2)
	}
}

// Test_NestedPendingOperations_Positive_SingleOpWithExpBackoff 验证开启出错退避时单个操作仍可正常执行。
func Test_NestedPendingOperations_Positive_SingleOpWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")

	// 执行
	err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: noopFunc})

	// 断言
	if err != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err)
	}
}

// Test_NestedPendingOperations_Positive_SecondOpAfterFirstCompletes 验证第一个操作结束后，同键的第二个操作可以启动。
func Test_NestedPendingOperations_Positive_SecondOpAfterFirstCompletes(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateCallbackFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc
	<-operation1DoneCh // 放行 operation1，使其结束

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})
			if err != nil {
				t.Logf("Warning: NestedPendingOperations failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NestedPendingOperations_Positive_SecondOpAfterFirstCompletesWithExpBackoff 验证开启出错退避时，第一个操作结束后同键的第二个操作可以启动。
func Test_NestedPendingOperations_Positive_SecondOpAfterFirstCompletesWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateCallbackFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc
	<-operation1DoneCh // 放行 operation1，使其结束

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})
			if err != nil {
				t.Logf("Warning: NestedPendingOperations failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NestedPendingOperations_Positive_SecondOpAfterFirstPanics 验证第一个操作 panic 后，同键的第二个操作仍可启动。
func Test_NestedPendingOperations_Positive_SecondOpAfterFirstPanics(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1 := panicFunc
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})
			if err != nil {
				t.Logf("Warning: NestedPendingOperations failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NestedPendingOperations_Positive_SecondOpAfterFirstPanicsWithExpBackoff 验证开启出错退避时，第一个操作 panic 后同键的第二个操作仍可启动。
func Test_NestedPendingOperations_Positive_SecondOpAfterFirstPanicsWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1 := panicFunc
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeLong), // 退避需要时间，故放宽初始等待时长
		func() (bool, error) {
			err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})
			if err != nil {
				t.Logf("Warning: NestedPendingOperations failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NestedPendingOperations_Negative_SecondOpBeforeFirstCompletes 验证第一个操作未结束时，同键的第二个操作会被拒绝。
func Test_NestedPendingOperations_Negative_SecondOpBeforeFirstCompletes(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 == nil {
		t.Fatalf("NestedPendingOperations did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", volumeName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NestedPendingOperations did not return alreadyExistsError, got: %v", err2)
	}
}

// Test_NestedPendingOperations_Negative_SecondThirdOpWithDifferentNames 验证不同操作名的操作不受退避影响；相同操作名在退避期内仍被拒绝。
func Test_NestedPendingOperations_Negative_SecondThirdOpWithDifferentNames(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	op1Name := "mount_volume"
	operation1 := errorFunc
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1, OperationName: op1Name})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	// 等待时间短于退避周期，以便第二次操作触发退避错误。
	operation2 := errorFunc
	err2 := retryWithExponentialBackOff(
		initialOperationWaitTimeShort,
		func() (bool, error) {
			err := grm.Run(volumeName,
				EmptyUniquePodName,
				EmptyNodeName,
				volumetypes.GeneratedOperations{OperationFunc: operation2, OperationName: op1Name})

			if exponentialbackoff.IsExponentialBackoff(err) {
				return true, nil
			}
			return false, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("Expected NestedPendingOperations to fail with exponential backoff for operationKey : %s and operationName : %s", volumeName, op1Name)
	}

	operation3 := noopFunc
	op3Name := "unmount_volume"
	// 执行
	err3 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation3, OperationName: op3Name})
	if err3 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected <no error> Actual: <%v>", err3)
	}
}

// Test_NestedPendingOperations_Negative_SecondSubOpBeforeFirstCompletes2 验证同一卷同一 Pod 的第一个子操作未结束时，第二个子操作被拒绝。
func Test_NestedPendingOperations_Negative_SecondSubOpBeforeFirstCompletes2(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operationPodName := volumetypes.UniquePodName("operation-podname")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, operationPodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := grm.Run(volumeName, operationPodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 == nil {
		t.Fatalf("NestedPendingOperations did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", volumeName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NestedPendingOperations did not return alreadyExistsError, got: %v", err2)
	}
}

// Test_NestedPendingOperations_Negative_SecondSubOpBeforeFirstCompletes 验证同一卷同一 Pod 的第一个子操作未结束时，第二个子操作被拒绝。
func Test_NestedPendingOperations_Negative_SecondSubOpBeforeFirstCompletes(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operationPodName := volumetypes.UniquePodName("operation-podname")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, operationPodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := grm.Run(volumeName, operationPodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 == nil {
		t.Fatalf("NestedPendingOperations did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", volumeName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NestedPendingOperations did not return alreadyExistsError, got: %v", err2)
	}
}

// Test_NestedPendingOperations_Negative_SecondOpBeforeFirstCompletesWithExpBackoff 验证开启出错退避时，第一个操作未结束时同键的第二个操作仍被拒绝。
func Test_NestedPendingOperations_Negative_SecondOpBeforeFirstCompletesWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 == nil {
		t.Fatalf("NestedPendingOperations did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", volumeName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NestedPendingOperations did not return alreadyExistsError, got: %v", err2)
	}
}

// Test_NestedPendingOperations_Positive_ThirdOpAfterFirstCompletes 验证第二个操作被拒绝后，第一个操作结束则第三个操作可启动。
func Test_NestedPendingOperations_Positive_ThirdOpAfterFirstCompletes(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc
	operation3 := noopFunc

	// 执行
	err2 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 == nil {
		t.Fatalf("NestedPendingOperations did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", volumeName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NestedPendingOperations did not return alreadyExistsError, got: %v", err2)
	}

	// 执行
	operation1DoneCh <- true // 放行 operation1，使其结束
	err3 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation3})
			if err != nil {
				t.Logf("Warning: NestedPendingOperations failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err3 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err3)
	}
}

// Test_NestedPendingOperations_Positive_ThirdOpAfterFirstCompletesWithExpBackoff 验证开启出错退避时，第二个操作被拒绝后第一个操作结束则第三个操作可启动。
func Test_NestedPendingOperations_Positive_ThirdOpAfterFirstCompletesWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc
	operation3 := noopFunc

	// 执行
	err2 := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 == nil {
		t.Fatalf("NestedPendingOperations did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", volumeName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NestedPendingOperations did not return alreadyExistsError, got: %v", err2)
	}

	// 执行
	operation1DoneCh <- true // 放行 operation1，使其结束
	err3 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation3})
			if err != nil {
				t.Logf("Warning: NestedPendingOperations failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err3 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err3)
	}
}

// Test_NestedPendingOperations_Positive_WaitEmpty 验证空操作集合上的 Wait() 不阻塞。
func Test_NestedPendingOperations_Positive_WaitEmpty(t *testing.T) {
	// 验证空 GoRoutineMap 上的 Wait() 总能立即返回、不阻塞
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 断言
	err := waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Errorf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// Test_NestedPendingOperations_Positive_WaitEmptyWithExpBackoff 验证开启出错退避时，空操作集合上的 Wait() 不阻塞。
func Test_NestedPendingOperations_Positive_WaitEmptyWithExpBackoff(t *testing.T) {
	// 验证空 GoRoutineMap 上的 Wait() 总能立即返回、不阻塞
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 断言
	err := waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Errorf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// Test_NestedPendingOperations_Positive_Wait 验证 Wait() 会阻塞到最后一个操作完成。
func Test_NestedPendingOperations_Positive_Wait(t *testing.T) {
	// 验证 Wait() 会一直阻塞到最后一个操作成功
	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 结束该操作
	operation1DoneCh <- true

	// 断言
	err = waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Fatalf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// Test_NestedPendingOperations_Positive_WaitWithExpBackoff 验证开启出错退避时，Wait() 会阻塞到最后一个操作完成。
func Test_NestedPendingOperations_Positive_WaitWithExpBackoff(t *testing.T) {
	// 验证 Wait() 会一直阻塞到最后一个操作成功
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("volume-name")
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err := grm.Run(volumeName, EmptyUniquePodName, EmptyNodeName, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 结束该操作
	operation1DoneCh <- true

	// 断言
	err = waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Fatalf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

/* 并发操作测试 */

// “None”表示卷名、Pod 名和节点名都为空。
// “Volume”表示只设置卷名，Pod 名和节点名为空。
// “Volume Pod”表示设置卷名和 Pod 名，节点名为空。
// “Volume Node”表示设置卷名和节点名，Pod 名为空。

// 非空时，各用例使用相同的卷名、Pod 名和节点名。

// 覆盖的用例：
// 第一个操作 | 第二个操作 | 结果
// None        | None        | 通过
// None        | Volume      | 通过
// None        | Volume Pod  | 通过
// None        | Volume Node | 通过
// Volume      | None        | 通过
// Volume      | Volume      | 失败（上面的 Test_NestedPendingOperations_Negative_SecondOpBeforeFirstCompletes 已覆盖）
// Volume      | Volume Pod  | 失败
// Volume      | Volume Node | 失败
// Volume Pod  | None        | 通过
// Volume Pod  | Volume      | 失败
// Volume Pod  | Volume Pod  | 失败（上面的 Test_NestedPendingOperations_Negative_SecondSubOpBeforeFirstCompletes 已覆盖）
// Volume Node | None        | 通过
// Volume Node | Volume      | 失败
// Volume Node | Volume Node | 失败

// 以下组合不会出现在同一个二进制中，故无需覆盖，哪种结果都可以。
// Volume Pod  | Volume Node
// Volume Node | Volume Pod

// Test_NestedPendingOperations_SecondOpBeforeFirstCompletes 枚举各种键形态组合，
// 验证第一个操作未结束时第二个操作能否创建。
func Test_NestedPendingOperations_SecondOpBeforeFirstCompletes(t *testing.T) {
	const (
		keyNone = iota
		keyVolume
		keyVolumePod
		keyVolumeNode
	)

	type testCase struct {
		testID     int
		keyTypes   []int // 只支持 2 个元素
		expectPass bool
	}

	tests := []testCase{
		{testID: 1, keyTypes: []int{keyNone, keyNone}, expectPass: true},
		{testID: 2, keyTypes: []int{keyNone, keyVolume}, expectPass: true},
		{testID: 3, keyTypes: []int{keyNone, keyVolumePod}, expectPass: true},
		{testID: 4, keyTypes: []int{keyNone, keyVolumeNode}, expectPass: true},
		{testID: 5, keyTypes: []int{keyVolume, keyNone}, expectPass: true},
		{testID: 6, keyTypes: []int{keyVolume, keyVolumePod}, expectPass: false},
		{testID: 7, keyTypes: []int{keyVolume, keyVolumeNode}, expectPass: false},
		{testID: 8, keyTypes: []int{keyVolumePod, keyNone}, expectPass: true},
		{testID: 9, keyTypes: []int{keyVolumePod, keyVolume}, expectPass: false},
		{testID: 10, keyTypes: []int{keyVolumeNode, keyNone}, expectPass: true},
		{testID: 11, keyTypes: []int{keyVolumeNode, keyVolume}, expectPass: false},
		{testID: 12, keyTypes: []int{keyVolumeNode, keyVolumeNode}, expectPass: false},
	}

	for _, test := range tests {
		var (
			volumeNames []v1.UniqueVolumeName
			podNames    []volumetypes.UniquePodName
			nodeNames   []types.NodeName
		)
		for _, keyType := range test.keyTypes {
			var (
				v v1.UniqueVolumeName
				p volumetypes.UniquePodName
				n types.NodeName
			)
			switch keyType {
			case keyNone:
				v = EmptyUniqueVolumeName
				p = EmptyUniquePodName
				n = EmptyNodeName
			case keyVolume:
				v = v1.UniqueVolumeName("volume-name")
				p = EmptyUniquePodName
				n = EmptyNodeName
			case keyVolumePod:
				v = v1.UniqueVolumeName("volume-name")
				p = volumetypes.UniquePodName("operation-podname")
				n = EmptyNodeName
			case keyVolumeNode:
				v = v1.UniqueVolumeName("volume-name")
				p = EmptyUniquePodName
				n = types.NodeName("operation-nodename")
			}
			volumeNames = append(volumeNames, v)
			podNames = append(podNames, p)
			nodeNames = append(nodeNames, n)
		}

		t.Run(fmt.Sprintf("Test %d", test.testID), func(t *testing.T) {
			if test.expectPass {
				testConcurrentOperationsPositive(t,
					volumeNames[0], podNames[0], nodeNames[0],
					volumeNames[1], podNames[1], nodeNames[1],
				)
			} else {
				testConcurrentOperationsNegative(t,
					volumeNames[0], podNames[0], nodeNames[0],
					volumeNames[1], podNames[1], nodeNames[1],
				)
			}
		})

	}

}

// Test_NestedPendingOperations_Positive_Issue_88355 复现 kubernetes/kubernetes issue #88355
// 中疑似触发问题的操作时序，验证退避后的操作能正常重试并成功。
// 详情见 https://github.com/kubernetes/kubernetes/issues/88355。
func Test_NestedPendingOperations_Positive_Issue_88355(t *testing.T) {
	// vx 表示卷名，nx 表示节点名。

	// 操作时序：
	// opZ(v0) 启动（操作的是另一个卷，与其他操作无关）
	// op1(v1, n1) 启动
	// op2(v1, n2) 启动
	// opZ(v0) 成功结束
	// op2(v1, n2) 失败结束（应触发指数退避）
	// op1(v1, n1) 成功结束
	// op3(v1, n2) 启动（遇退避错误则不断重试）
	// op3(v1, n2) 成功结束
	// op4(v1, n2) 启动
	// op4(v1, n2) 成功结束

	const (
		mainVolumeName = "main-volume"
		opZVolumeName  = "other-volume"
		node1          = "node1"
		node2          = "node2"

		// 发出结束信号后留一点延迟，确保操作真的结束后再执行下一个。
		delay = 50 * time.Millisecond

		// 模拟 AttachDetachController 默认的对账周期。
		reconcilerPeriod = 100 * time.Millisecond
	)

	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	opZContinueCh := make(chan interface{})
	op1ContinueCh := make(chan interface{})
	op2ContinueCh := make(chan interface{})
	operationZ := generateWaitFunc(opZContinueCh)
	operation1 := generateWaitFunc(op1ContinueCh)
	operation2 := generateWaitWithErrorFunc(op2ContinueCh)
	operation3 := noopFunc
	operation4 := noopFunc

	errZ := grm.Run(opZVolumeName, "" /* podName：空 */, "" /* nodeName：空 */, volumetypes.GeneratedOperations{OperationFunc: operationZ})
	if errZ != nil {
		t.Fatalf("NestedPendingOperations failed for operationZ. Expected: <no error> Actual: <%v>", errZ)
	}

	err1 := grm.Run(mainVolumeName, "" /* podName：空 */, node1, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Fatalf("NestedPendingOperations failed for operation1. Expected: <no error> Actual: <%v>", err1)
	}

	err2 := grm.Run(mainVolumeName, "" /* podName：空 */, node2, volumetypes.GeneratedOperations{OperationFunc: operation2})
	if err2 != nil {
		t.Fatalf("NestedPendingOperations failed for operation2. Expected: <no error> Actual: <%v>", err2)
	}

	opZContinueCh <- true
	time.Sleep(delay)
	op2ContinueCh <- true
	time.Sleep(delay)
	op1ContinueCh <- true
	time.Sleep(delay)

	for {
		err3 := grm.Run(mainVolumeName, "" /* podName：空 */, node2, volumetypes.GeneratedOperations{OperationFunc: operation3})
		if err3 == nil {
			break
		} else if !exponentialbackoff.IsExponentialBackoff(err3) {
			t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err3)
		}
		time.Sleep(reconcilerPeriod)
	}

	time.Sleep(delay)

	err4 := grm.Run(mainVolumeName, "" /* podName：空 */, node2, volumetypes.GeneratedOperations{OperationFunc: operation4})
	if err4 != nil {
		t.Fatalf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err4)
	}
}

// testConcurrentOperationsPositive 验证两个给定键的操作能并行执行，否则失败。
func testConcurrentOperationsPositive(
	t *testing.T,
	volumeName1 v1.UniqueVolumeName,
	podName1 volumetypes.UniquePodName,
	nodeName1 types.NodeName,
	volumeName2 v1.UniqueVolumeName,
	podName2 volumetypes.UniquePodName,
	nodeName2 types.NodeName) {

	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName1, podName1, nodeName1 /* nodeName：同变量 */, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := grm.Run(volumeName2, podName2, nodeName2, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// testConcurrentOperationsNegative 验证第二个操作的创建返回 alreadyExists 错误，否则失败。
func testConcurrentOperationsNegative(
	t *testing.T,
	volumeName1 v1.UniqueVolumeName,
	podName1 volumetypes.UniquePodName,
	nodeName1 types.NodeName,
	volumeName2 v1.UniqueVolumeName,
	podName2 volumetypes.UniquePodName,
	nodeName2 types.NodeName) {

	// 准备
	grm := NewNestedPendingOperations(false /* exponentialBackOffOnError：是否出错退避 */)
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(volumeName1, podName1, nodeName1 /* nodeName：同变量 */, volumetypes.GeneratedOperations{OperationFunc: operation1})
	if err1 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := noopFunc

	// 执行
	err2 := grm.Run(volumeName2, podName2, nodeName2, volumetypes.GeneratedOperations{OperationFunc: operation2})

	// 断言
	if err2 == nil {
		t.Errorf("NestedPendingOperations did not fail. Expected an operation to already exist")
	}
	if !IsAlreadyExists(err2) {
		t.Errorf("NestedPendingOperations did not return alreadyExistsError, got: %v", err2)
	}
}

/* 并发操作测试结束 */

// generateCallbackFunc 生成操作函数：收到结束信号后返回成功上下文。
func generateCallbackFunc(done chan<- interface{}) func() volumetypes.OperationContext {
	return func() volumetypes.OperationContext {
		done <- true
		return volumetypes.NewOperationContext(nil, nil, false)
	}
}

// generateWaitFunc 生成操作函数：阻塞等待结束信号后返回成功上下文。
func generateWaitFunc(done <-chan interface{}) func() volumetypes.OperationContext {
	return func() volumetypes.OperationContext {
		<-done
		return volumetypes.NewOperationContext(nil, nil, false)
	}
}

// panicFunc 操作函数：直接 panic，用于测试异常恢复。
func panicFunc() volumetypes.OperationContext {
	panic("testing panic")
}

// errorFunc 操作函数：返回带占位错误的上下文，用于模拟操作失败。
func errorFunc() volumetypes.OperationContext {
	return volumetypes.NewOperationContext(fmt.Errorf("placeholder1"), fmt.Errorf("placeholder2"), false)
}

// generateWaitWithErrorFunc 生成操作函数：阻塞等待结束信号后返回带错误的上下文。
func generateWaitWithErrorFunc(done <-chan interface{}) func() volumetypes.OperationContext {
	return func() volumetypes.OperationContext {
		<-done
		return volumetypes.NewOperationContext(fmt.Errorf("placeholder1"), fmt.Errorf("placeholder2"), false)
	}
}

// noopFunc 空操作函数：直接返回成功上下文。
func noopFunc() volumetypes.OperationContext {
	return volumetypes.NewOperationContext(nil, nil, false)
}

// retryWithExponentialBackOff 按固定因子 3、最多 4 步的指数退避重复调用 fn，直到成功或耗尽步数。
func retryWithExponentialBackOff(initialDuration time.Duration, fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   3,
		Jitter:   0,
		Steps:    4,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// waitChannelWithTimeout 在超时时间内等待通道收到值，超时则返回错误。
func waitChannelWithTimeout(ch <-chan interface{}, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ch:
		// 成功
		return nil
	case <-timer.C:
		return fmt.Errorf("timeout after %v", timeout)
	}
}

// Test_NestedPendingOperations_OperationExists_PendingFirst 验证退避中的操作优先于同键其他操作：仍有挂起操作时，空 Pod 名的操作被拒绝。
func Test_NestedPendingOperations_OperationExists_PendingFirst(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("test-volume")
	podName1 := volumetypes.UniquePodName("pod1")
	podName2 := volumetypes.UniquePodName("pod2")
	podName3 := volumetypes.UniquePodName("pod3")
	podName4 := EmptyUniquePodName
	nodeName := EmptyNodeName

	// 发出结束信号后留一点延迟，确保操作真的结束后再执行下一个。
	delay := 50 * time.Millisecond

	// pod1 的操作1：模拟失败
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitWithErrorFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, podName1, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation1, OperationName: "umount"})
	if err1 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}

	// pod2 的操作2：模拟失败
	operation2DoneCh := make(chan interface{})
	operation2 := generateWaitWithErrorFunc(operation2DoneCh)
	err2 := grm.Run(volumeName, podName2, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation2, OperationName: "umount"})
	if err2 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err2)
	}

	// pod3 的操作3：保持挂起不结束
	operation3DoneCh := make(chan interface{})
	operation3 := generateWaitFunc(operation3DoneCh)
	defer func() {
		close(operation3DoneCh)
	}()
	err3 := grm.Run(volumeName, podName3, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation3, OperationName: "umount"})
	if err3 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err3)
	}

	operation1DoneCh <- true
	operation2DoneCh <- true
	time.Sleep(delay)

	// 操作3 仍挂起，空 Pod 名的操作4 应被拒绝
	operation4DoneCh := make(chan interface{})
	operation4 := generateWaitFunc(operation4DoneCh)
	defer func() {
		close(operation4DoneCh)
	}()
	err4 := grm.Run(volumeName, podName4, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation4, OperationName: "mount"})

	// 断言
	if err4 == nil {
		t.Errorf("NestedPendingOperations did not fail. Expected an operation to already exist")
	}
	if !IsAlreadyExists(err4) {
		t.Errorf("NestedPendingOperations did not return alreadyExistsError, got: %v", err4)
	}
}

// Test_NestedPendingOperations_OperationExists_ExactMatchFirstNoPending 验证精确匹配的操作全部失败后，新的同键操作可启动并替换旧记录。
func Test_NestedPendingOperations_OperationExists_ExactMatchFirstNoPending(t *testing.T) {
	// 准备
	grm := NewNestedPendingOperations(true /* exponentialBackOffOnError：是否出错退避 */)
	volumeName := v1.UniqueVolumeName("test-volume")
	podName1 := volumetypes.UniquePodName("pod1")
	podName2 := volumetypes.UniquePodName("pod2")
	podName3 := volumetypes.UniquePodName("pod3")
	podName4 := EmptyUniquePodName
	nodeName := EmptyNodeName

	// 发出结束信号后留一点延迟，确保操作真的结束后再执行下一个。
	delay := 50 * time.Millisecond
	backoffDelay := 500 * time.Millisecond

	// fake operation1 for pod1 fails
	operation1DoneCh := make(chan interface{})
	operation1 := generateWaitWithErrorFunc(operation1DoneCh)
	err1 := grm.Run(volumeName, podName1, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation1, OperationName: "umount"})
	if err1 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err1)
	}

	// pod2 的操作2：模拟失败
	operation2DoneCh := make(chan interface{})
	operation2 := generateWaitWithErrorFunc(operation2DoneCh)
	err2 := grm.Run(volumeName, podName2, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation2, OperationName: "umount"})
	if err2 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err2)
	}

	// pod3 的操作3：模拟失败
	operation3DoneCh := make(chan interface{})
	operation3 := generateWaitWithErrorFunc(operation3DoneCh)
	err3 := grm.Run(volumeName, podName3, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation3, OperationName: "umount"})
	if err3 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err3)
	}

	operation1DoneCh <- true
	operation2DoneCh <- true
	operation3DoneCh <- true
	time.Sleep(delay)

	// 空 Pod 名的操作4：模拟失败
	operation4DoneCh := make(chan interface{})
	operation4 := generateWaitWithErrorFunc(operation4DoneCh)
	err4 := grm.Run(volumeName, podName4, nodeName /* nodeName：空节点名 */, volumetypes.GeneratedOperations{OperationFunc: operation4, OperationName: "mount"})
	if err4 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err4)
	}

	operation4DoneCh <- true

	// 退避结束后重试 pod2 的操作
	time.Sleep(backoffDelay)
	operation5 := noopFunc
	err5 := grm.Run(volumeName, podName2, nodeName, volumetypes.GeneratedOperations{OperationFunc: operation5, OperationName: "umount"})
	if err5 != nil {
		t.Errorf("NestedPendingOperations failed. Expected: <no error> Actual: <%v>", err5)
	}
	time.Sleep(delay)

	// 断言
	// 操作5 覆盖了操作2；pod2 的卸载已成功，该记录应从 operations 中移除
	grm.(*nestedPendingOperations).lock.Lock()
	defer grm.(*nestedPendingOperations).lock.Unlock()
	for _, op := range grm.(*nestedPendingOperations).operations {
		if op.key.podName == podName2 {
			t.Errorf("NestedPendingOperations failed. Operation for pod2 should be removed")
		}
	}

}
