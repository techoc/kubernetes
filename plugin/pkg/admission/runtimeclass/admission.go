/*
Copyright 2019 The Kubernetes Authors.

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

// Package runtimeclass 包含一个准入控制器，用于根据 RuntimeClass 修改和校验新建的 Pod。
// 对于声明了运行 Pod 额外开销的 RuntimeClass，该控制器会相应地设置 pod.Spec.Overhead。
// 该字段只允许由此控制器设置，因此校验阶段会确保 Pod 上的值与对应 RuntimeClass 中的定义一致。
package runtimeclass

import (
	"context"
	"fmt"
	"io"

	nodev1 "k8s.io/api/node/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitailizer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	nodev1client "k8s.io/client-go/kubernetes/typed/node/v1"
	nodev1listers "k8s.io/client-go/listers/node/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
	node "k8s.io/kubernetes/pkg/apis/node"
	apinodev1 "k8s.io/kubernetes/pkg/apis/node/v1"
	"k8s.io/kubernetes/pkg/util/tolerations"
)

// PluginName 表示该准入插件的名称。
const PluginName = "RuntimeClass"

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewRuntimeClass(), nil
	})
}

// RuntimeClass 是 admission.Interface 的一个实现。
// 它检查所有新建 Pod，若指定的 RuntimeClass 定义了开销，则设置 pod.Spec.Overhead。
// 若 Pod 已设置 Overhead 但指定的 RuntimeClass 未定义匹配的开销，则拒绝该 Pod。
type RuntimeClass struct {
	*admission.Handler
	runtimeClassLister nodev1listers.RuntimeClassLister
	runtimeClassClient nodev1client.RuntimeClassInterface
}

var _ admission.MutationInterface = &RuntimeClass{}
var _ admission.ValidationInterface = &RuntimeClass{}

var _ genericadmissioninitailizer.WantsExternalKubeInformerFactory = &RuntimeClass{}
var _ genericadmissioninitailizer.WantsExternalKubeClientSet = &RuntimeClass{}

// SetExternalKubeClientSet 设置插件使用的客户端。
func (r *RuntimeClass) SetExternalKubeClientSet(client kubernetes.Interface) {
	r.runtimeClassClient = client.NodeV1().RuntimeClasses()
}

// SetExternalKubeInformerFactory 实现 WantsExternalKubeInformerFactory 接口。
func (r *RuntimeClass) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	runtimeClassInformer := f.Node().V1().RuntimeClasses()
	r.SetReadyFunc(runtimeClassInformer.Informer().HasSynced)
	r.runtimeClassLister = runtimeClassInformer.Lister()
}

// ValidateInitialization 校验初始化结果，确保 lister 与 client 均已注入。
func (r *RuntimeClass) ValidateInitialization() error {
	if r.runtimeClassLister == nil {
		return fmt.Errorf("missing RuntimeClass lister")
	}
	if r.runtimeClassClient == nil {
		return fmt.Errorf("missing RuntimeClass client")
	}
	return nil
}

// Admit 根据请求属性作出准入决策，将 RuntimeClass 中的开销和调度约束写入 Pod。
func (r *RuntimeClass) Admit(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) error {
	// 只处理 Pod 主资源，子资源无需注入字段。
	if shouldIgnore(attributes) {
		return nil
	}

	pod, runtimeClass, err := r.prepareObjects(ctx, attributes)
	if err != nil {
		return err
	}
	if err := setOverhead(attributes, pod, runtimeClass); err != nil {
		return err
	}

	if err := setScheduling(attributes, pod, runtimeClass); err != nil {
		return err
	}

	return nil
}

// Validate 确保 Pod 的 Overhead 与 RuntimeClass 的定义一致。
func (r *RuntimeClass) Validate(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) error {
	// 只处理 Pod 主资源，子资源无需注入字段。
	if shouldIgnore(attributes) {
		return nil
	}

	pod, runtimeClass, err := r.prepareObjects(ctx, attributes)
	if err != nil {
		return err
	}
	if err := validateOverhead(attributes, pod, runtimeClass); err != nil {
		return err
	}

	return nil
}

// NewRuntimeClass 创建一个新的 RuntimeClass 准入处理器。
func NewRuntimeClass() *RuntimeClass {
	return &RuntimeClass{
		Handler: admission.NewHandler(admission.Create),
	}
}

// prepareObjects 从准入属性中提取 Pod，并按其指定的 RuntimeClass 名称查找对应对象。
func (r *RuntimeClass) prepareObjects(ctx context.Context, attributes admission.Attributes) (pod *api.Pod, runtimeClass *nodev1.RuntimeClass, err error) {
	pod, ok := attributes.GetObject().(*api.Pod)
	if !ok {
		return nil, nil, apierrors.NewBadRequest("Resource was marked with kind Pod but was unable to be converted")
	}

	if pod.Spec.RuntimeClassName == nil {
		return pod, nil, nil
	}

	// 从缓存读取 RuntimeClass。
	runtimeClass, err = r.runtimeClassLister.Get(*pod.Spec.RuntimeClassName)
	if apierrors.IsNotFound(err) {
		// 缓存可能滞后，回退到实时查询以避免误拒。
		runtimeClass, err = r.runtimeClassClient.Get(ctx, *pod.Spec.RuntimeClassName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return pod, nil, admission.NewForbidden(attributes, fmt.Errorf("pod rejected: RuntimeClass %q not found", *pod.Spec.RuntimeClassName))
		}
	}

	// 返回解析出的 Pod 与 RuntimeClass。
	return pod, runtimeClass, err
}

// setOverhead 将 RuntimeClass 中定义的 PodFixed 开销写入 Pod，并拒绝用户自行设置的不一致开销。
func setOverhead(a admission.Attributes, pod *api.Pod, runtimeClass *nodev1.RuntimeClass) (err error) {
	if runtimeClass == nil || runtimeClass.Overhead == nil {
		return nil
	}

	// 先转换到内部类型再进行比较和赋值。
	nodeOverhead := &node.Overhead{}
	if err = apinodev1.Convert_v1_Overhead_To_node_Overhead(runtimeClass.Overhead, nodeOverhead, nil); err != nil {
		return err
	}

	// 用户显式设置的 Overhead 必须与 RuntimeClass 定义一致，防止绕过管控。
	if len(pod.Spec.Overhead) > 0 && !apiequality.Semantic.DeepEqual(nodeOverhead.PodFixed, pod.Spec.Overhead) {
		return admission.NewForbidden(a, fmt.Errorf("pod rejected: Pod's Overhead doesn't match RuntimeClass's defined Overhead"))
	}

	pod.Spec.Overhead = nodeOverhead.PodFixed

	return nil
}

// setScheduling 将 RuntimeClass 的 nodeSelector 与 tolerations 合并到 Pod，
// 节点选择冲突时拒绝，以保证 Pod 只被调度到该运行时可用的节点。
func setScheduling(a admission.Attributes, pod *api.Pod, runtimeClass *nodev1.RuntimeClass) (err error) {
	if runtimeClass == nil || runtimeClass.Scheduling == nil {
		return nil
	}

	// 转换到内部类型后再合并到 Pod，确保比较时使用统一的表示。
	nodeScheduling := &node.Scheduling{}
	if err = apinodev1.Convert_v1_Scheduling_To_node_Scheduling(runtimeClass.Scheduling, nodeScheduling, nil); err != nil {
		return err
	}

	runtimeNodeSelector := nodeScheduling.NodeSelector
	newNodeSelector := pod.Spec.NodeSelector
	if newNodeSelector == nil {
		newNodeSelector = runtimeNodeSelector
	} else {
		for key, runtimeClassValue := range runtimeNodeSelector {
			// 同一 key 上不允许用户值与运行时要求冲突。
			if podValue, ok := newNodeSelector[key]; ok && podValue != runtimeClassValue {
				return admission.NewForbidden(a, fmt.Errorf("conflict: runtimeClass.scheduling.nodeSelector[%s] = %s; pod.spec.nodeSelector[%s] = %s", key, runtimeClassValue, key, podValue))
			}
			newNodeSelector[key] = runtimeClassValue
		}
	}

	newTolerations := tolerations.MergeTolerations(pod.Spec.Tolerations, nodeScheduling.Tolerations)

	pod.Spec.NodeSelector = newNodeSelector
	pod.Spec.Tolerations = newTolerations

	return nil
}

// validateOverhead 校验 Pod 的 Overhead 与 RuntimeClass 定义一致，
// 并拒绝没有对应 RuntimeClass 定义却自行设置 Overhead 的 Pod。
func validateOverhead(a admission.Attributes, pod *api.Pod, runtimeClass *nodev1.RuntimeClass) (err error) {
	if runtimeClass != nil && runtimeClass.Overhead != nil {
		// 开销不匹配则拒绝，避免用户绕过 admission 自行设置。
		nodeOverhead := &node.Overhead{}
		if err := apinodev1.Convert_v1_Overhead_To_node_Overhead(runtimeClass.Overhead, nodeOverhead, nil); err != nil {
			return err
		}
		if !apiequality.Semantic.DeepEqual(nodeOverhead.PodFixed, pod.Spec.Overhead) {
			return admission.NewForbidden(a, fmt.Errorf("pod rejected: Pod's Overhead doesn't match RuntimeClass's defined Overhead"))
		}
	} else {
		// 没有对应的开销定义却设置了开销，视为非法。
		if pod.Spec.Overhead != nil {
			return admission.NewForbidden(a, fmt.Errorf("pod rejected: Pod Overhead set without corresponding RuntimeClass defined Overhead"))
		}
	}

	return nil
}

// shouldIgnore 判断请求是否无需处理：仅 Pod 主资源需要关注。
func shouldIgnore(attributes admission.Attributes) bool {
	// 子资源或非 Pod 资源不注入也不校验相关字段。
	if len(attributes.GetSubresource()) != 0 || attributes.GetResource().GroupResource() != api.Resource("pods") {
		return true
	}

	return false
}
