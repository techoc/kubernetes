/*
Copyright 2025 The Kubernetes Authors.

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

package apicalls

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// PodBindingCall 承载一次 Pod 绑定调用所需的 Binding 对象。
type PodBindingCall struct {
	binding *v1.Binding
}

// NewPodBindingCall 用给定的 Binding 构造一个绑定调用。
func NewPodBindingCall(binding *v1.Binding) *PodBindingCall {
	return &PodBindingCall{
		binding: binding,
	}
}

// CallType 返回调用类型，即 PodBinding。
func (pbc *PodBindingCall) CallType() fwk.APICallType {
	return PodBinding
}

// UID 返回待绑定 Pod 的 UID，供调用去重和追踪使用。
func (pbc *PodBindingCall) UID() types.UID {
	return pbc.binding.UID
}

// Execute 向 apiserver 提交 Binding，把 Pod 绑定到指定节点。
func (pbc *PodBindingCall) Execute(ctx context.Context, client clientset.Interface) error {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Attempting to bind pod to node", "pod", klog.KObj(&pbc.binding.ObjectMeta), "node", pbc.binding.Target.Name)

	return util.BindPod(ctx, client, pbc.binding)
}

// Sync 无需缓存或更新对象，原样返回即可。
func (pbc *PodBindingCall) Sync(obj metav1.Object) (metav1.Object, error) {
	// 绑定是一次性副作用，没有可供同步的本地状态。
	return obj, nil
}

// Merge 直接用新调用覆盖旧调用。
func (pbc *PodBindingCall) Merge(oldCall fwk.APICall) error {
	// 同一 Pod 的多次绑定请求只需保留最新一次，无需合并内容。
	return nil
}

// IsNoOp 返回该调用是否为空操作；绑定总是会产生副作用。
func (pbc *PodBindingCall) IsNoOp() bool {
	return false
}
