/*
Copyright 2020 The Kubernetes Authors.

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

package queuesort

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// Name 是该插件在插件注册表和配置文件中使用的名称。
const Name = names.PrioritySort

// PrioritySort 实现按优先级排序的队列排序插件。
type PrioritySort struct{}

var _ fwk.QueueSortPlugin = &PrioritySort{}

// Name 返回插件名称。
func (pl *PrioritySort) Name() string {
	return Name
}

// Less 供 activeQ 堆算法比较实体使用：高优先级排前面，
// 优先级相同则先入队的实体排前面，以保证队列内的公平性。
func (pl *PrioritySort) Less(entity1, entity2 fwk.QueuedEntityInfo) bool {
	p1 := entity1.GetPriority()
	p2 := entity2.GetPriority()
	return (p1 > p2) || (p1 == p2 && entity1.GetTimestamp().Before(entity2.GetTimestamp()))
}

// New 创建并返回一个 PrioritySort 插件实例。
func New(_ context.Context, _ runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	return &PrioritySort{}, nil
}
