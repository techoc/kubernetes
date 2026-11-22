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

package defaultbinder

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// Name 是插件在注册表和配置中使用的名字。
const Name = names.DefaultBinder

// DefaultBinder 通过 k8s 客户端把 Pod 绑定到节点。
type DefaultBinder struct {
	handle fwk.Handle
}

var _ fwk.BindPlugin = &DefaultBinder{}

// New 创建一个 DefaultBinder。
func New(_ context.Context, _ runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	return &DefaultBinder{handle: handle}, nil
}

// Name 返回插件名。
func (b DefaultBinder) Name() string {
	return Name
}

// Bind 通过 k8s 客户端把 Pod 绑定到指定节点。
func (b DefaultBinder) Bind(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) *fwk.Status {
	logger := klog.FromContext(ctx)
	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID},
		Target:     v1.ObjectReference{Kind: "Node", Name: nodeName},
	}
	if b.handle.APICacher() != nil {
		// 有 API cacher 时走它的绑定路径，避免直接打到 apiserver。
		onFinish, err := b.handle.APICacher().BindPod(binding)
		if err != nil {
			return fwk.AsStatus(err)
		}
		// 绑定是异步提交的，必须等到真正完成才算成功。
		err = b.handle.APICacher().WaitOnFinish(ctx, onFinish)
		if err != nil {
			return fwk.AsStatus(err)
		}
		return nil
	}
	logger.V(3).Info("Attempting to bind pod to node", "pod", klog.KObj(p), "node", klog.KRef("", nodeName))
	err := util.BindPod(ctx, b.handle.ClientSet(), binding)
	if err != nil {
		return fwk.AsStatus(err)
	}
	return nil
}
