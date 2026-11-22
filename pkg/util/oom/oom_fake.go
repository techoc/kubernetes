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

// FakeOOMAdjuster 是 OOMAdjuster 的空占位类型；实际使用的是
// NewFakeOOMAdjuster 返回的、注入了空操作的 OOMAdjuster。
type FakeOOMAdjuster struct{}

// NewFakeOOMAdjuster 返回一个不会触碰真实 /proc 或 cgroup 的 OOMAdjuster，
// 供 hollow kubelet 和测试在无需特权环境时使用。
func NewFakeOOMAdjuster() *OOMAdjuster {
	return &OOMAdjuster{
		pidLister:                 func(cgroupName string) ([]int, error) { return make([]int, 0), nil },
		ApplyOOMScoreAdj:          fakeApplyOOMScoreAdj,
		ApplyOOMScoreAdjContainer: fakeApplyOOMScoreAdjContainer,
	}
}

func fakeApplyOOMScoreAdj(pid int, oomScoreAdj int) error {
	return nil
}

func fakeApplyOOMScoreAdjContainer(cgroupName string, oomScoreAdj, maxTries int) error {
	return nil
}
