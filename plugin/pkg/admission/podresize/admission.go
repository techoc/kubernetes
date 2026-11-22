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

package podresize

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/apis/core"
	v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// PluginName 是本准入控制器插件的名称。
	PluginName = "PodResizeValidator"

	// ReasonNodeCapacity 表示节点可分配资源不足。
	ReasonNodeCapacity metav1.CauseType = "NodeCapacity"
	// ReasonUnsupportedPlatform 表示当前平台不支持 Pod 原地调整。
	ReasonUnsupportedPlatform metav1.CauseType = "UnsupportedPlatform"
)

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin()
	})
}

// Plugin 是根据节点能力校验 Pod 调整请求的准入控制器。
type Plugin struct {
	*admission.Handler
	nodeLister                       corev1listers.NodeLister
	inPlacePodVerticalScalingEnabled bool
}

var _ admission.ValidationInterface = &Plugin{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&Plugin{})

// NewPlugin 创建一个新的 ResizeValidator 准入插件。
func NewPlugin() (*Plugin, error) {
	return &Plugin{
		Handler: admission.NewHandler(admission.Update),
	}, nil
}

// SetExternalKubeInformerFactory 为插件设置 informer 工厂。
func (p *Plugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	nodeInformer := f.Core().V1().Nodes()
	p.nodeLister = nodeInformer.Lister()
	// 缓存未同步前无法可靠查询节点，故等待同步完成再就绪。
	p.SetReadyFunc(nodeInformer.Informer().HasSynced)
}

// InspectFeatureGates 读取插件关心的特性开关。
func (p *Plugin) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	p.inPlacePodVerticalScalingEnabled = featureGates.Enabled(features.InPlacePodVerticalScaling)
}

// ValidateInitialization 确保插件已正确初始化。
func (p *Plugin) ValidateInitialization() error {
	if p.nodeLister == nil {
		return fmt.Errorf("missing node lister for %s", PluginName)
	}

	return nil
}

// Validate 是准入控制器的核心逻辑。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	// 特性未开启时不介入。
	if !p.inPlacePodVerticalScalingEnabled {
		return nil
	}

	if a.GetOperation() != admission.Update {
		return nil
	}

	// 只关心 Pod 更新。
	if a.GetResource().GroupResource() != core.Resource("pods") {
		return nil
	}

	// 只校验自定义的 "resize" 子资源更新。
	if a.GetSubresource() != "resize" {
		return nil
	}
	pod, ok := a.GetObject().(*core.Pod)
	if !ok {
		return errors.NewBadRequest(fmt.Sprintf("expected a pod but got a %T", a.GetObject()))
	}

	// 未绑定到节点的 Pod 无从校验节点能力，直接放行。
	if len(pod.Spec.NodeName) == 0 {
		return nil
	}

	oldPod, ok := a.GetOldObject().(*core.Pod)
	if !ok {
		return errors.NewBadRequest(fmt.Sprintf("expected an old pod but got a %T", a.GetOldObject()))
	}

	// generation 仅在 spec 变化时递增，未变化则无需校验。
	if oldPod.Generation == pod.Generation {
		return nil
	}

	if !p.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	return p.validatePodResize(pod, a)
}

// validatePodResize 校验目标节点是否支持并容纳本次调整。
func (p *Plugin) validatePodResize(pod *core.Pod, a admission.Attributes) error {
	node, err := p.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		if errors.IsNotFound(err) {
			return admission.NewForbidden(a, fmt.Errorf("node %q not found", pod.Spec.NodeName))
		}
		return admission.NewForbidden(a, fmt.Errorf("failed to get node %q: %w", pod.Spec.NodeName, err))
	}

	// 非 Linux 节点不支持原地调整。
	if err := validateLinuxNode(node); err != nil {
		return resizeValidationError(a, err, ReasonUnsupportedPlatform)
	}

	// 新请求量超过节点可分配资源时拒绝。
	if err := validateNodeAllocatable(pod, node); err != nil {
		return resizeValidationError(a, err, ReasonNodeCapacity)
	}

	return nil
}

// validateNodeAllocatable 在 Pod 请求量不超过节点可分配资源时返回 nil，否则返回错误。
func validateNodeAllocatable(pod *core.Pod, node *corev1.Node) error {
	// 辅助库只接受外部版本类型，需先做内部到外部的转换。
	podV1 := &corev1.Pod{}
	if err := v1.Convert_core_Pod_To_v1_Pod(pod, podV1, nil); err != nil {
		return errors.NewBadRequest(fmt.Sprintf("failed to convert pod: %v", err))
	}
	return validateRequestsWithinAllocatable(podV1, node.Status.Allocatable)
}

// validateLinuxNode 在节点为 Linux 时返回 nil，否则返回错误。
func validateLinuxNode(node *corev1.Node) error {
	// 该标签正常情况下总会被填充；缺失时按 Linux 处理。
	val, ok := node.Labels[corev1.LabelOSStable]
	if !ok || val == "linux" {
		return nil
	}
	return fmt.Errorf("pod resize is only supported on linux nodes, node %q is %q", node.Name, val)
}

// resizeValidationError 构造带指定原因的禁止错误，便于客户端定位拒绝原因。
func resizeValidationError(a admission.Attributes, err error, reason metav1.CauseType) error {
	forbiddenErr := admission.NewForbidden(a, err)
	var statusErr *errors.StatusError
	if stderrors.As(forbiddenErr, &statusErr) {
		statusErr.ErrStatus.Details.Causes = append(statusErr.ErrStatus.Details.Causes, metav1.StatusCause{
			Type: reason,
		})
		return statusErr
	}
	return forbiddenErr
}

// validateRequestsWithinAllocatable 在 Pod 请求量超过可分配容量时返回错误。
func validateRequestsWithinAllocatable(pod *corev1.Pod, allocatable corev1.ResourceList) error {
	cpuAllocatable := allocatable.Cpu()
	memAllocatable := allocatable.Memory()

	requests := resource.PodRequests(pod, resource.PodResourcesOptions{})
	cpuRequests := requests[corev1.ResourceCPU]
	memRequests := requests[corev1.ResourceMemory]

	var msg []string
	if cpuRequests.Cmp(*cpuAllocatable) > 0 {
		msg = append(msg, fmt.Sprintf("cpu, requested: %d, allocatable: %d", cpuRequests.MilliValue(), cpuAllocatable.MilliValue()))
	}
	if memRequests.Cmp(*memAllocatable) > 0 {
		msg = append(msg, fmt.Sprintf("memory, requested: %d, allocatable: %d", memRequests.Value(), memAllocatable.Value()))
	}
	if len(msg) > 0 {
		return fmt.Errorf("node didn't have enough allocatable resources: %s", strings.Join(msg, "; "))
	}
	return nil
}
