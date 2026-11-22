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

// FakeVolumeBinder 和 FakeVolumeBinderConfig 为测试提供可控的卷绑定插件替身。
package volumebinding

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// FakeVolumeBinderConfig 保存假卷绑定器的行为配置。
// 每个字段对应一个接口方法的返回值，让测试能直接制造成功或失败的场景。
type FakeVolumeBinderConfig struct {
	AllBound    bool
	FindReasons ConflictReasons
	FindErr     error
	AssumeErr   error
	BindErr     error
}

// NewFakeVolumeBinder 创建假卷绑定器。
// 真正的绑定器会维护拓扑感知决策所需的缓存，这里用配置直接给出结果，无需这些缓存。
func NewFakeVolumeBinder(config *FakeVolumeBinderConfig) *FakeVolumeBinder {
	return &FakeVolumeBinder{
		config: config,
	}
}

// FakeVolumeBinder 是测试用的假卷绑定器。
// AssumeCalled 与 BindCalled 记录方法是否被调用，便于断言调度流程走到了哪一步。
type FakeVolumeBinder struct {
	config       *FakeVolumeBinderConfig
	AssumeCalled bool
	BindCalled   bool
}

var _ SchedulerVolumeBinder = &FakeVolumeBinder{}

// GetPodVolumeClaims 实现 SchedulerVolumeBinder.GetPodVolumes。
// 测试用实现不收集真实 PVC，直接返回空结构。
func (b *FakeVolumeBinder) GetPodVolumeClaims(_ klog.Logger, pod *v1.Pod) (podVolumeClaims *PodVolumeClaims, err error) {
	return &PodVolumeClaims{}, nil
}

// FindPodVolumes 实现 SchedulerVolumeBinder.FindPodVolumes。
// 返回值完全由配置决定，不检查节点与卷的实际亲和性。
func (b *FakeVolumeBinder) FindPodVolumes(_ klog.Logger, pod *v1.Pod, _ *PodVolumeClaims, node *v1.Node) (podVolumes *PodVolumes, reasons ConflictReasons, err error) {
	return nil, b.config.FindReasons, b.config.FindErr
}

// AssumePodVolumes 实现 SchedulerVolumeBinder.AssumePodVolumes。
// 记录调用发生，并返回配置中预设的绑定状态与错误。
func (b *FakeVolumeBinder) AssumePodVolumes(_ klog.Logger, assumedPod *v1.Pod, nodeName string, podVolumes *PodVolumes) (bool, error) {
	b.AssumeCalled = true
	return b.config.AllBound, b.config.AssumeErr
}

// RevertAssumedPodVolumes 实现 SchedulerVolumeBinder.RevertAssumedPodVolumes。
// 假绑定器没有内部状态需要回滚。
func (b *FakeVolumeBinder) RevertAssumedPodVolumes(_ *PodVolumes) {}

// BindPodVolumes 实现 SchedulerVolumeBinder.BindPodVolumes。
// 只记录调用并返回配置中的错误，不执行真正的卷绑定。
func (b *FakeVolumeBinder) BindPodVolumes(ctx context.Context, assumedPod *v1.Pod, podVolumes *PodVolumes) error {
	b.BindCalled = true
	return b.config.BindErr
}
