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

package oom

import (
	"os"

	"github.com/opencontainers/cgroups"
	"testing"

	"github.com/stretchr/testify/assert"
)

// sequenceToPidLister 把 PID 列表序列转换成一个 PID 列举函数。
// 第 i 次调用返回 pidListSequence[i]；若调用次数超出序列长度，
// 则返回最后一个元素（认为序列已经稳定）。
func sequenceToPidLister(pidListSequence [][]int) func(string) ([]int, error) {
	var numCalls int
	return func(cgroupName string) ([]int, error) {
		numCalls++
		if len(pidListSequence) == 0 {
			return []int{}, nil
		} else if numCalls > len(pidListSequence) {
			return pidListSequence[len(pidListSequence)-1], nil
		}
		return pidListSequence[numCalls-1], nil
	}
}

// applyOOMScoreAdjContainerTester 验证 applyOOMScoreAdjContainer 是否正确地为相关进程设置 OOM 分数，
// 或者在预期场景下返回正确的错误。
func applyOOMScoreAdjContainerTester(pidListSequence [][]int, maxTries int, appliedPids []int, expectedError bool, t *testing.T) {
	pidOOMs := make(map[int]bool)

	// 用 mock 替换 ApplyOOMScoreAdj 和 pidLister，记录实际被调整的 PID。
	oomAdjuster := NewOOMAdjuster()
	oomAdjuster.ApplyOOMScoreAdj = func(pid int, oomScoreAdj int) error {
		pidOOMs[pid] = true
		return nil
	}
	oomAdjuster.pidLister = sequenceToPidLister(pidListSequence)
	err := oomAdjuster.ApplyOOMScoreAdjContainer("", 100, maxTries)

	// 校验错误返回值是否符合预期。
	if expectedError && err == nil {
		t.Errorf("Expected error %+v when running ApplyOOMScoreAdjContainer but got no error", expectedError)
		return
	} else if !expectedError && err != nil {
		t.Errorf("Expected no error but got error %+v when running ApplyOOMScoreAdjContainer", err)
		return
	} else if err != nil {
		return
	}
	// 校验 OOM 分数确实应用到了预期的进程集合。
	if len(appliedPids) != len(pidOOMs) {
		t.Errorf("Applied OOM scores to incorrect number of processes - %+v vs %v", appliedPids, pidOOMs)
		return
	}
	for _, pid := range appliedPids {
		if !pidOOMs[pid] {
			t.Errorf("Failed to apply OOM scores to process %d", pid)
		}
	}
}

// TestOOMScoreAdjContainer 覆盖进程列表为空、包含进程以及列表延迟稳定的场景，
// 验证 applyOOMScoreAdjContainer 的重试和错误行为。
func TestOOMScoreAdjContainer(t *testing.T) {
	pidListSequenceEmpty := [][]int{}
	applyOOMScoreAdjContainerTester(pidListSequenceEmpty, 3, nil, true, t)

	pidListSequence1 := [][]int{
		{1, 2},
	}
	applyOOMScoreAdjContainerTester(pidListSequence1, 1, []int{1, 2}, false, t)

	pidListSequenceLag := [][]int{
		{},
		{},
		{},
		{1, 2, 4},
	}
	for i := 1; i < 4; i++ {
		applyOOMScoreAdjContainerTester(pidListSequenceLag, i, nil, true, t)
	}
	applyOOMScoreAdjContainerTester(pidListSequenceLag, 4, []int{1, 2, 4}, false, t)
}

// TestPidListerFailure 验证 cgroup 路径不存在时，getPids 返回“不存在”错误。
func TestPidListerFailure(t *testing.T) {
	_, err := getPids("/does/not/exist")
	assert.True(t, cgroups.IsNotFound(err) || os.IsNotExist(err), "expected getPids to return not exists error. Got %v", err)
}
