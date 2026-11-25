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

package app

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"
)

// 本文件定义用于包装下游控制器的类型和函数。包装器实现 Controller 接口，
// 并由 ControllerDescriptor 保存管理器所需的静态元数据。

// Controller 是所有控制器包装器必须实现的基础接口。
type Controller interface {
	// Name 返回控制器的规范名称。
	Name() string

	// Run 运行控制器循环，有工作处理时持续阻塞直到上下文取消。
	// Run 返回前必须确保所有协程均已终止。
	Run(context.Context)
}

// ControllerConstructor 创建控制器；返回 nil Controller 表示禁用对应控制器。
type ControllerConstructor func(ctx context.Context, controllerContext ControllerContext, controllerName string) (Controller, error)

// ControllerDescriptor 保存控制器的名称、构造器及启用条件等静态元数据。
type ControllerDescriptor struct {
	name                    string
	constructor             ControllerConstructor
	requiredFeatureGates    []featuregate.Feature
	aliases                 []string
	isDisabledByDefault     bool
	requiresSpecialHandling bool
}

// Name 返回控制器规范名称；管理器据此匹配配置、日志和启动顺序。
func (r *ControllerDescriptor) Name() string {
	return r.name
}

// GetControllerConstructor 返回延迟创建控制器的函数，调用前不会分配控制器资源。
func (r *ControllerDescriptor) GetControllerConstructor() ControllerConstructor {
	return r.constructor
}

// GetRequiredFeatureGates 返回必需特性门控的副本，调用方修改结果不会影响描述符。
func (r *ControllerDescriptor) GetRequiredFeatureGates() []featuregate.Feature {
	return append([]featuregate.Feature(nil), r.requiredFeatureGates...)
}

// GetAliases 返回用于向后兼容的别名，已有别名绝不能删除。
// 仅允许在规范名称变更时新增别名，详见控制器名称的变更策略。
func (r *ControllerDescriptor) GetAliases() []string {
	return append([]string(nil), r.aliases...)
}

// IsDisabledByDefault 报告控制器是否必须显式启用，避免新控制器随升级意外启动。
func (r *ControllerDescriptor) IsDisabledByDefault() bool {
	return r.isDisabledByDefault
}

// RequiresSpecialHandling 仅对 ServiceAccountTokenController 等需要特殊启动流程的控制器返回 true。
func (r *ControllerDescriptor) RequiresSpecialHandling() bool {
	return r.requiresSpecialHandling
}

// BuildController 检查必需特性门控后调用构造器创建控制器，返回值约定与构造器一致。
func (r *ControllerDescriptor) BuildController(ctx context.Context, controllerCtx ControllerContext) (Controller, error) {
	logger := klog.FromContext(ctx)
	controllerName := r.Name()

	for _, featureGate := range r.GetRequiredFeatureGates() {
		if !utilfeature.DefaultFeatureGate.Enabled(featureGate) {
			logger.Info("Controller is disabled by a feature gate",
				"controller", controllerName,
				"requiredFeatureGates", r.GetRequiredFeatureGates())
			return nil, nil
		}
	}

	ctx = klog.NewContext(ctx, klog.LoggerWithName(logger, controllerName))
	return r.GetControllerConstructor()(ctx, controllerCtx, controllerName)
}

// KnownControllers 返回所有已知控制器的规范名称。
func KnownControllers() []string {
	return sets.StringKeySet(NewControllerDescriptors()).List()
}

// ControllerAliases 返回别名到规范控制器名称的映射。
func ControllerAliases() map[string]string {
	aliases := map[string]string{}
	for name, c := range NewControllerDescriptors() {
		for _, alias := range c.GetAliases() {
			aliases[alias] = name
		}
	}
	return aliases
}

// ControllersDisabledByDefault 返回默认禁用的控制器名称，结果按名称排序。
func ControllersDisabledByDefault() []string {
	var controllersDisabledByDefault []string

	for name, c := range NewControllerDescriptors() {
		if c.IsDisabledByDefault() {
			controllersDisabledByDefault = append(controllersDisabledByDefault, name)
		}
	}

	sort.Strings(controllersDisabledByDefault)

	return controllersDisabledByDefault
}

// NewControllerDescriptors 返回控制器组名称到描述符的公开映射，供下游结构化组合和细分。
// 每组包含对应控制器的构造器，可在初始化函数中启动多个控制器。
func NewControllerDescriptors() map[string]*ControllerDescriptor {
	controllers := map[string]*ControllerDescriptor{}
	aliases := sets.NewString()

	// 所有控制器必须满足共同约束，否则注册过程将失败。
	register := func(controllerDesc *ControllerDescriptor) {
		if controllerDesc == nil {
			panic("received nil controller for a registration")
		}
		name := controllerDesc.Name()
		if len(name) == 0 {
			panic("received controller without a name for a registration")
		}
		if _, found := controllers[name]; found {
			panic(fmt.Sprintf("controller name %q was registered twice", name))
		}
		if controllerDesc.GetControllerConstructor() == nil {
			panic(fmt.Sprintf("controller %q does not have a constructor specified", name))
		}

		for _, alias := range controllerDesc.GetAliases() {
			if aliases.Has(alias) {
				panic(fmt.Sprintf("controller %q has a duplicate alias %q", name, alias))
			}
			aliases.Insert(alias)
		}

		controllers[name] = controllerDesc
	}

	// 先注册不能按常规流程初始化的特殊控制器；此处仅登记元数据并检测重复。
	// 这类描述符的 RequiresSpecialHandling 必须返回 true。
	// 目前唯一的特殊控制器是 ServiceAccountTokenController，它必须先启动，
	// 确保后续控制器所需的服务账号令牌存在。新增特殊控制器前务必审慎评估。
	register(newServiceAccountTokenControllerDescriptor(nil))

	register(newEndpointsControllerDescriptor())
	register(newEndpointSliceControllerDescriptor())
	register(newEndpointSliceMirroringControllerDescriptor())
	register(newReplicationControllerDescriptor())
	register(newPodGarbageCollectorControllerDescriptor())
	register(newResourceQuotaControllerDescriptor())
	register(newNamespaceControllerDescriptor())
	register(newServiceAccountControllerDescriptor())
	register(newGarbageCollectorControllerDescriptor())
	register(newDaemonSetControllerDescriptor())
	register(newJobControllerDescriptor())
	register(newPodGroupProtectionControllerDescriptor())
	register(newDeploymentControllerDescriptor())
	register(newReplicaSetControllerDescriptor())
	register(newHorizontalPodAutoscalerControllerDescriptor())
	register(newDisruptionControllerDescriptor())
	register(newStatefulSetControllerDescriptor())
	register(newCronJobControllerDescriptor())
	register(newCertificateSigningRequestSigningControllerDescriptor())
	register(newCertificateSigningRequestApprovingControllerDescriptor())
	register(newCertificateSigningRequestCleanerControllerDescriptor())
	register(newPodCertificateRequestCleanerControllerDescriptor())
	register(newTTLControllerDescriptor())
	register(newBootstrapSignerControllerDescriptor())
	register(newTokenCleanerControllerDescriptor())
	register(newNodeIpamControllerDescriptor())
	register(newNodeLifecycleControllerDescriptor())
	register(newPersistentVolumeBinderControllerDescriptor())
	register(newPersistentVolumeAttachDetachControllerDescriptor())
	register(newPersistentVolumeExpanderControllerDescriptor())
	register(newClusterRoleAggregrationControllerDescriptor())
	register(newPersistentVolumeClaimProtectionControllerDescriptor())
	register(newPersistentVolumeProtectionControllerDescriptor())
	register(newVolumeAttributesClassProtectionControllerDescriptor())
	register(newTTLAfterFinishedControllerDescriptor())
	register(newRootCACertificatePublisherControllerDescriptor())
	register(newKubeAPIServerSignerClusterTrustBundledPublisherDescriptor())
	register(newEphemeralVolumeControllerDescriptor())

	// 受特性门控保护的控制器
	register(newStorageVersionGarbageCollectorControllerDescriptor())
	register(newResourceClaimControllerDescriptor())
	register(newResourcePoolStatusRequestControllerDescriptor())
	register(newDeviceTaintEvictionControllerDescriptor())
	register(newLegacyServiceAccountTokenCleanerControllerDescriptor())
	register(newValidatingAdmissionPolicyStatusControllerDescriptor())
	register(newTaintEvictionControllerDescriptor())
	register(newServiceCIDRsControllerDescriptor())
	register(newStorageVersionMigratorControllerDescriptor())
	register(newSELinuxWarningControllerDescriptor())

	for _, alias := range aliases.UnsortedList() {
		if _, ok := controllers[alias]; ok {
			panic(fmt.Sprintf("alias %q conflicts with a controller name", alias))
		}
	}

	return controllers
}
