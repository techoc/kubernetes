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

package priority

import (
	"context"
	"fmt"
	"io"

	apiv1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializers "k8s.io/apiserver/pkg/admission/initializer"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	schedulingv1listers "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// PluginName 表示该准入插件的名称。
	PluginName = "Priority"
)

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin(), nil
	})
}

// Plugin 是 admission.Interface 的一个实现。
type Plugin struct {
	*admission.Handler
	client kubernetes.Interface
	lister schedulingv1listers.PriorityClassLister
}

var _ admission.MutationInterface = &Plugin{}
var _ admission.ValidationInterface = &Plugin{}
var _ = genericadmissioninitializers.WantsExternalKubeInformerFactory(&Plugin{})
var _ = genericadmissioninitializers.WantsExternalKubeClientSet(&Plugin{})

// NewPlugin 创建一个新的优先级准入插件。
func NewPlugin() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update, admission.Delete),
	}
}

// ValidateInitialization 实现 InitializationValidator 接口，校验 client 与 lister 已注入。
func (p *Plugin) ValidateInitialization() error {
	if p.client == nil {
		return fmt.Errorf("%s requires a client", PluginName)
	}
	if p.lister == nil {
		return fmt.Errorf("%s requires a lister", PluginName)
	}
	return nil
}

// SetExternalKubeClientSet 实现 WantsExternalKubeClientSet 接口。
func (p *Plugin) SetExternalKubeClientSet(client kubernetes.Interface) {
	p.client = client
}

// SetExternalKubeInformerFactory 实现 WantsExternalKubeInformerFactory 接口。
func (p *Plugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	priorityInformer := f.Scheduling().V1().PriorityClasses()
	p.lister = priorityInformer.Lister()
	p.SetReadyFunc(priorityInformer.Informer().HasSynced)
}

var (
	podResource               = core.Resource("pods")
	podGroupResource          = scheduling.Resource("podgroups")
	compositePodGroupResource = scheduling.Resource("compositepodgroups")
	priorityClassResource     = scheduling.Resource("priorityclasses")
)

// Admit 检查 Pod、PodGroup 和 CompositePodGroup 并决定准入或拒绝，
// 同时根据各自的 PriorityClass 解析出其优先级。
func (p *Plugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	operation := a.GetOperation()
	// 子资源不携带优先级字段，无需处理。
	if len(a.GetSubresource()) != 0 {
		return nil
	}
	switch a.GetResource().GroupResource() {
	case podResource:
		if operation == admission.Create || operation == admission.Update {
			return p.admitPod(a)
		}
		return nil
	case podGroupResource:
		if operation == admission.Create {
			return p.admitPodGroup(a)
		}
		return nil
	case compositePodGroupResource:
		if operation == admission.Create {
			return p.admitCompositePodGroup(a)
		}
		return nil
	default:
		return nil
	}
}

// Validate 检查 PriorityClass 并决定准入或拒绝。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	operation := a.GetOperation()
	// 子资源不携带优先级字段，无需处理。
	if len(a.GetSubresource()) != 0 {
		return nil
	}

	switch a.GetResource().GroupResource() {
	case priorityClassResource:
		if operation == admission.Create || operation == admission.Update {
			return p.validatePriorityClass(a)
		}
		return nil

	default:
		return nil
	}
}

// admitPod 确保新 Pod 不自行设置 spec.Priority，校验指定的 PriorityClassName 存在，
// 并据此解析 Pod 的优先级。
func (p *Plugin) admitPod(a admission.Attributes) error {
	operation := a.GetOperation()
	pod, ok := a.GetObject().(*core.Pod)
	if !ok {
		return errors.NewBadRequest("resource was marked with kind Pod but was unable to be converted")
	}

	if operation == admission.Update {
		oldPod, ok := a.GetOldObject().(*core.Pod)
		if !ok {
			return errors.NewBadRequest("resource was marked with kind Pod but was unable to be converted")
		}

		// 本插件在创建时写入了 Priority；更新时要保留这些值，
		// 避免丢失。API 校验本身禁止修改 Priority 和 PriorityClassName，
		// 其他改动会在更新校验阶段被拒绝，不会持久化。
		if pod.Spec.Priority == nil && oldPod.Spec.Priority != nil {
			pod.Spec.Priority = oldPod.Spec.Priority
		}
		if pod.Spec.PreemptionPolicy == nil && oldPod.Spec.PreemptionPolicy != nil {
			pod.Spec.PreemptionPolicy = oldPod.Spec.PreemptionPolicy
		}
		return nil
	}

	if operation == admission.Create {
		var priority int32
		var preemptionPolicy *apiv1.PreemptionPolicy
		pcName, priority, preemptionPolicy, err := p.establishPriority(a, &pod.Spec.PriorityClassName)
		if err != nil {
			return err
		}
		pod.Spec.PriorityClassName = pcName
		// 用户提供的整数优先级必须与 PriorityClass 计算出的值一致，否则拒绝。
		if pod.Spec.Priority != nil && *pod.Spec.Priority != priority {
			return admission.NewForbidden(a, fmt.Errorf("the integer value of priority (%d) must not be provided in pod spec; priority admission controller computed %d from the given PriorityClass name", *pod.Spec.Priority, priority))
		}
		pod.Spec.Priority = &priority

		var corePolicy core.PreemptionPolicy
		if preemptionPolicy != nil {
			corePolicy = core.PreemptionPolicy(*preemptionPolicy)
			if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy != corePolicy {
				return admission.NewForbidden(a, fmt.Errorf("the string value of PreemptionPolicy (%s) must not be provided in pod spec; priority admission controller computed %s from the given PriorityClass name", *pod.Spec.PreemptionPolicy, corePolicy))
			}
			pod.Spec.PreemptionPolicy = &corePolicy
		}
	}
	return nil
}

// admitPodGroup 确保新 PodGroup 不自行设置 spec.Priority，校验指定的 PriorityClassName 存在，
// 并据此解析 PodGroup 的优先级。
func (p *Plugin) admitPodGroup(attributes admission.Attributes) error {
	if !utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		return nil
	}

	pg, ok := attributes.GetObject().(*scheduling.PodGroup)
	if !ok {
		return errors.NewBadRequest("resource was marked with kind PodGroup but was unable to be converted")
	}

	priorityClassName, priority, preemptionPolicy, err := p.establishPriority(attributes, &pg.Spec.PriorityClassName)
	if err != nil {
		return err
	}
	// 用户提供的优先级必须与 PriorityClass 计算值一致，防止绕过管控。
	if pg.Spec.Priority != nil && *pg.Spec.Priority != priority {
		return admission.NewForbidden(attributes, fmt.Errorf("priority set in the pod group (%d) must match the priority computed (%d) based on the priority class set in the spec", *pg.Spec.Priority, priority))
	}
	pg.Spec.Priority = &priority
	pg.Spec.PriorityClassName = priorityClassName

	var schedulingPreemptionPolicy scheduling.PreemptionPolicy
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) && preemptionPolicy != nil {
		switch *preemptionPolicy {
		case apiv1.PreemptLowerPriority:
			schedulingPreemptionPolicy = scheduling.PreemptLowerPriority
		case apiv1.PreemptNever:
			schedulingPreemptionPolicy = scheduling.PreemptNever
		default:
			return admission.NewForbidden(attributes, fmt.Errorf("preemptionPolicy set in the PriorityClass object (%v) must match one of the allowed values of PreemptionPolicy type in pod group", *preemptionPolicy))
		}
		if pg.Spec.PreemptionPolicy != nil && *pg.Spec.PreemptionPolicy != schedulingPreemptionPolicy {
			return admission.NewForbidden(attributes, fmt.Errorf("the string value of PreemptionPolicy (%s) must not be provided in pod group spec; priority admission controller computed %s from the given PriorityClass name", *pg.Spec.PreemptionPolicy, schedulingPreemptionPolicy))
		}
		pg.Spec.PreemptionPolicy = &schedulingPreemptionPolicy
	}
	return nil
}

// admitCompositePodGroup 确保新 CompositePodGroup 不自行设置 spec.Priority，
// 校验指定的 PriorityClassName 存在，并据此解析其优先级。
func (p *Plugin) admitCompositePodGroup(attributes admission.Attributes) error {
	if !utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		return nil
	}

	cpg, ok := attributes.GetObject().(*scheduling.CompositePodGroup)
	if !ok {
		return errors.NewBadRequest("resource was marked with kind CompositePodGroup but was unable to be converted")
	}

	priorityClassName, priority, preemptionPolicy, err := p.establishPriority(attributes, &cpg.Spec.PriorityClassName)
	if err != nil {
		return err
	}
	// 用户提供的优先级必须与 PriorityClass 计算值一致，防止绕过管控。
	if cpg.Spec.Priority != nil && *cpg.Spec.Priority != priority {
		return admission.NewForbidden(attributes, fmt.Errorf("priority set in the composite pod group (%d) must match the priority computed (%d) based on the priority class set in the spec", *cpg.Spec.Priority, priority))
	}
	cpg.Spec.Priority = &priority
	cpg.Spec.PriorityClassName = priorityClassName

	var schedulingPreemptionPolicy scheduling.PreemptionPolicy
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) && preemptionPolicy != nil {
		switch *preemptionPolicy {
		case apiv1.PreemptLowerPriority:
			schedulingPreemptionPolicy = scheduling.PreemptLowerPriority
		case apiv1.PreemptNever:
			schedulingPreemptionPolicy = scheduling.PreemptNever
		default:
			return admission.NewForbidden(attributes, fmt.Errorf("preemptionPolicy set in the PriorityClass object (%v) must match one of the allowed values of PreemptionPolicy type in composite pod group", *preemptionPolicy))
		}
		if cpg.Spec.PreemptionPolicy != nil && *cpg.Spec.PreemptionPolicy != schedulingPreemptionPolicy {
			return admission.NewForbidden(attributes, fmt.Errorf("the string value of PreemptionPolicy (%s) must not be provided in composite pod group spec; priority admission controller computed %s from the given PriorityClass name", *cpg.Spec.PreemptionPolicy, schedulingPreemptionPolicy))
		}
		cpg.Spec.PreemptionPolicy = &schedulingPreemptionPolicy
	}
	return nil
}

// validatePriorityClass 确保全局默认 PriorityClass 至多只有一个；
// 若已有其他默认类，再次创建或将其设为默认都会被拒绝。
func (p *Plugin) validatePriorityClass(a admission.Attributes) error {
	operation := a.GetOperation()
	pc, ok := a.GetObject().(*scheduling.PriorityClass)
	if !ok {
		return errors.NewBadRequest("resource was marked with kind PriorityClass but was unable to be converted")
	}
	// 新类若声称是默认类，必须先确认没有别的默认类。
	if pc.GlobalDefault {
		dpc, err := p.getDefaultPriorityClass()
		if err != nil {
			return fmt.Errorf("failed to get default priority class: %v", err)
		}
		if dpc != nil {
			// 创建第二个默认类，或把另一个类改为默认类，都会破坏唯一性约束。
			if operation == admission.Create || (operation == admission.Update && dpc.GetName() != pc.GetName()) {
				return admission.NewForbidden(a, fmt.Errorf("PriorityClass %v is already marked as default. Only one default can exist", dpc.GetName()))
			}
		}
	}
	return nil
}

// establishPriority 根据给定的 PriorityClass 名称计算优先级相关字段。
// 名称为空时回退到默认 PriorityClass；名称非空时按名称解析对应类并返回其中的信息。
func (p *Plugin) establishPriority(attributes admission.Attributes, priorityClassName *string) (string, int32, *apiv1.PreemptionPolicy, error) {
	if priorityClassName == nil || *priorityClassName == "" {
		pcName, priority, preemptionPolicy, err := p.getDefaultPriority()
		if err != nil {
			return "", 0, nil, fmt.Errorf("error occurred while retrieving default priority class: %w", err)
		}
		return pcName, priority, preemptionPolicy, nil
	}
	// 按名称解析 PriorityClass，找不到时由调用方转为拒绝。
	pc, err := p.resolvePriorityClass(attributes, *priorityClassName)
	if err != nil {
		return "", 0, nil, err
	}
	return *priorityClassName, pc.Value, pc.PreemptionPolicy, nil
}

// getDefaultPriorityClass 从缓存中找出全局默认 PriorityClass。
func (p *Plugin) getDefaultPriorityClass() (*schedulingv1.PriorityClass, error) {
	list, err := p.lister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	// 竞态可能产生多个默认类，此时取优先级值最小的一个，保证结果确定。
	var defaultPC *schedulingv1.PriorityClass
	for _, pci := range list {
		if pci.GlobalDefault {
			if defaultPC == nil || defaultPC.Value > pci.Value {
				defaultPC = pci
			}
		}
	}
	return defaultPC, nil
}

// getDefaultPriority 返回默认类的名称、优先级和抢占策略；
// 不存在默认类时回退到内置默认值。
func (p *Plugin) getDefaultPriority() (string, int32, *apiv1.PreemptionPolicy, error) {
	dpc, err := p.getDefaultPriorityClass()
	if err != nil {
		return "", 0, nil, err
	}
	if dpc != nil {
		return dpc.Name, dpc.Value, dpc.PreemptionPolicy, nil
	}
	preemptLowerPriority := apiv1.PreemptLowerPriority
	return "", int32(scheduling.DefaultPriorityWhenNoDefaultClassExists), &preemptLowerPriority, nil
}

// resolvePriorityClass 按名称查找 PriorityClass，不存在时返回拒绝错误。
func (p *Plugin) resolvePriorityClass(attributes admission.Attributes, priorityClassName string) (*schedulingv1.PriorityClass, error) {
	priorityClass, err := p.lister.Get(priorityClassName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, admission.NewForbidden(attributes, fmt.Errorf("no PriorityClass with name %v was found", priorityClassName))
		}
		return nil, fmt.Errorf("failed to resolve PriorityClass with name %s: %w", priorityClassName, err)
	}
	return priorityClass, nil
}
