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

// 本文件负责装配 APF API Group 的 REST Storage，并在 API Server 启动后协调默认配置。
package rest

import (
	"context"
	"fmt"
	"time"

	flowcontrolv1 "k8s.io/api/flowcontrol/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	flowcontrolbootstrap "k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/client-go/informers"
	flowcontrolclient "k8s.io/client-go/kubernetes/typed/flowcontrol/v1"
	flowcontrollisters "k8s.io/client-go/listers/flowcontrol/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	flowcontrolapisv1 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1"
	"k8s.io/kubernetes/pkg/registry/flowcontrol/ensurer"
	flowschemastore "k8s.io/kubernetes/pkg/registry/flowcontrol/flowschema/storage"
	prioritylevelconfigurationstore "k8s.io/kubernetes/pkg/registry/flowcontrol/prioritylevelconfiguration/storage"
)

var _ genericapiserver.PostStartHookProvider = RESTStorageProvider{}

// RESTStorageProvider 负责装配 flowcontrol.apiserver.k8s.io API Group 的 REST 存储，并提供启动后配置协调钩子。
type RESTStorageProvider struct {
	InformerFactory informers.SharedInformerFactory
}

// PostStartHookName 是 APF 默认配置协调器注册到 API Server 的 PostStart Hook 名称。
const PostStartHookName = "priority-and-fairness-config-producer"

// NewRESTStorage 创建 FlowControl API Group，并注册当前 API 版本中已启用的资源存储。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(flowcontrol.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)

	if storageMap, err := p.storage(apiResourceConfigSource, restOptionsGetter, flowcontrolapisv1.SchemeGroupVersion); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[flowcontrolapisv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// storage 按指定 GroupVersion 构建资源映射；只注册当前版本存在且由 APIResourceConfig 启用的资源。
func (p RESTStorageProvider) storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter, groupVersion schema.GroupVersion) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册 FlowSchema 主资源及其 /status 子资源。
	if resource := "flowschemas"; apiResourceConfigSource.ResourceEnabled(groupVersion.WithResource(resource)) {
		flowSchemaStorage, flowSchemaStatusStorage, err := flowschemastore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = flowSchemaStorage
		storage[resource+"/status"] = flowSchemaStatusStorage
	}

	// 注册 PriorityLevelConfiguration 主资源及其 /status 子资源。
	if resource := "prioritylevelconfigurations"; apiResourceConfigSource.ResourceEnabled(groupVersion.WithResource(resource)) {
		priorityLevelConfigurationStorage, priorityLevelConfigurationStatusStorage, err := prioritylevelconfigurationstore.NewREST(restOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = priorityLevelConfigurationStorage
		storage[resource+"/status"] = priorityLevelConfigurationStatusStorage
	}

	return storage, nil
}

// GroupName 返回 FlowControl API Group 的名称。
func (p RESTStorageProvider) GroupName() string {
	return flowcontrol.GroupName
}

// PostStartHook 返回 API Server 启动后执行 APF 引导配置协调的钩子函数。
func (p RESTStorageProvider) PostStartHook() (string, genericapiserver.PostStartHookFunc, error) {
	bce := &bootstrapConfigurationEnsurer{
		informersSynced: []cache.InformerSynced{
			p.InformerFactory.Flowcontrol().V1().PriorityLevelConfigurations().Informer().HasSynced,
			p.InformerFactory.Flowcontrol().V1().FlowSchemas().Informer().HasSynced,
		},
		fsLister:  p.InformerFactory.Flowcontrol().V1().FlowSchemas().Lister(),
		plcLister: p.InformerFactory.Flowcontrol().V1().PriorityLevelConfigurations().Lister(),
	}
	return PostStartHookName, bce.ensureAPFBootstrapConfiguration, nil
}

type bootstrapConfigurationEnsurer struct {
	informersSynced []cache.InformerSynced
	fsLister        flowcontrollisters.FlowSchemaLister
	plcLister       flowcontrollisters.PriorityLevelConfigurationLister
}

// ensureAPFBootstrapConfiguration 启动后协调默认 FlowSchema 和 PriorityLevelConfiguration；通过 informer/lister 读取当前对象，再创建或修复必需配置。
func (bce *bootstrapConfigurationEnsurer) ensureAPFBootstrapConfiguration(hookContext genericapiserver.PostStartHookContext) error {
	clientset, err := flowcontrolclient.NewForConfig(hookContext.LoopbackClientConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize clientset for APF - %w", err)
	}

	err = func() error {
		// 创建派生上下文：最多等待 5 分钟；如果 API Server 的停止通道先关闭，则立即取消。
		ctx, cancel := contextFromChannelAndMaxWaitDuration(hookContext.Done(), 5*time.Minute)
		defer cancel()

		if !cache.WaitForCacheSync(ctx.Done(), bce.informersSynced...) {
			return fmt.Errorf("APF bootstrap ensurer timed out waiting for cache sync")
		}

		err = wait.PollImmediateUntilWithContext(
			ctx,
			time.Second,
			func(context.Context) (bool, error) {
				if err := ensure(ctx, clientset, bce.fsLister, bce.plcLister); err != nil {
					klog.ErrorS(err, "APF bootstrap ensurer ran into error, will retry later")
					return false, nil
				}
				return true, nil
			})
		if err != nil {
			return fmt.Errorf("unable to initialize APF bootstrap configuration: %w", err)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	// 首次引导配置已经成功初始化；随后启动后台 goroutine，定期重新协调引导配置。
	go func() {
		wait.PollImmediateUntil(
			time.Minute,
			func() (bool, error) {
				if err := ensure(hookContext, clientset, bce.fsLister, bce.plcLister); err != nil {
					klog.ErrorS(err, "APF bootstrap ensurer ran into error, will retry later")
				}
				// 每个周期都重新检查建议配置和强制配置；具体是否覆盖 Spec 由各自的维护策略决定。
				return false, nil
			}, hookContext.Done())
		klog.Info("APF bootstrap ensurer is exiting")
	}()

	return nil
}

// ensure 按顺序执行建议配置、强制配置的确保逻辑，并清理不再需要的引导对象。
func ensure(ctx context.Context, clientset flowcontrolclient.FlowcontrolV1Interface, fsLister flowcontrollisters.FlowSchemaLister, plcLister flowcontrollisters.PriorityLevelConfigurationLister) error {

	if err := ensureSuggestedConfiguration(ctx, clientset, fsLister, plcLister); err != nil {
		// 建议配置协调失败时，不继续创建强制配置。此处的错误通常意味着停止通道已关闭，协调流程应当终止。
		return fmt.Errorf("failed ensuring suggested settings - %w", err)
	}

	if err := ensureMandatoryConfiguration(ctx, clientset, fsLister, plcLister); err != nil {
		return fmt.Errorf("failed ensuring mandatory settings - %w", err)
	}

	if err := removeDanglingBootstrapConfiguration(ctx, clientset, fsLister, plcLister); err != nil {
		return fmt.Errorf("failed to delete removed settings - %w", err)
	}

	return nil
}

// ensureSuggestedConfiguration 确保建议级别的默认 FlowSchema/PriorityLevelConfiguration 存在；这些配置可由管理员按集群需求调整。
func ensureSuggestedConfiguration(ctx context.Context, clientset flowcontrolclient.FlowcontrolV1Interface, fsLister flowcontrollisters.FlowSchemaLister, plcLister flowcontrollisters.PriorityLevelConfigurationLister) error {
	plcOps := ensurer.NewPriorityLevelConfigurationOps(clientset.PriorityLevelConfigurations(), plcLister)
	if err := ensurer.EnsureConfigurations(ctx, plcOps, flowcontrolbootstrap.SuggestedPriorityLevelConfigurations, ensurer.NewSuggestedEnsureStrategy[*flowcontrolv1.PriorityLevelConfiguration]()); err != nil {
		return err
	}

	fsOps := ensurer.NewFlowSchemaOps(clientset.FlowSchemas(), fsLister)
	return ensurer.EnsureConfigurations(ctx, fsOps, flowcontrolbootstrap.SuggestedFlowSchemas, ensurer.NewSuggestedEnsureStrategy[*flowcontrolv1.FlowSchema]())
}

// ensureMandatoryConfiguration 确保系统正常运行所必需的 APF 配置存在，避免关键 API 请求缺少匹配或优先级处理规则。
func ensureMandatoryConfiguration(ctx context.Context, clientset flowcontrolclient.FlowcontrolV1Interface, fsLister flowcontrollisters.FlowSchemaLister, plcLister flowcontrollisters.PriorityLevelConfigurationLister) error {
	plcOps := ensurer.NewPriorityLevelConfigurationOps(clientset.PriorityLevelConfigurations(), plcLister)
	if err := ensurer.EnsureConfigurations(ctx, plcOps, flowcontrolbootstrap.MandatoryPriorityLevelConfigurations, ensurer.NewMandatoryEnsureStrategy[*flowcontrolv1.PriorityLevelConfiguration]()); err != nil {
		return err
	}

	fsOps := ensurer.NewFlowSchemaOps(clientset.FlowSchemas(), fsLister)
	return ensurer.EnsureConfigurations(ctx, fsOps, flowcontrolbootstrap.MandatoryFlowSchemas, ensurer.NewMandatoryEnsureStrategy[*flowcontrolv1.FlowSchema]())
}

// removeDanglingBootstrapConfiguration 清理不再被当前引导配置引用的旧对象，避免遗留 FlowSchema 或 PriorityLevelConfiguration 持续参与请求分类。
func removeDanglingBootstrapConfiguration(ctx context.Context, clientset flowcontrolclient.FlowcontrolV1Interface, fsLister flowcontrollisters.FlowSchemaLister, plcLister flowcontrollisters.PriorityLevelConfigurationLister) error {
	if err := removeDanglingBootstrapFlowSchema(ctx, clientset, fsLister); err != nil {
		return err
	}

	return removeDanglingBootstrapPriorityLevel(ctx, clientset, plcLister)
}

// removeDanglingBootstrapFlowSchema 删除不再属于当前引导集合的 FlowSchema，同时避免误删用户自定义配置。
func removeDanglingBootstrapFlowSchema(ctx context.Context, clientset flowcontrolclient.FlowcontrolV1Interface, fsLister flowcontrollisters.FlowSchemaLister) error {
	bootstrap := append(flowcontrolbootstrap.MandatoryFlowSchemas, flowcontrolbootstrap.SuggestedFlowSchemas...)
	fsOps := ensurer.NewFlowSchemaOps(clientset.FlowSchemas(), fsLister)
	return ensurer.RemoveUnwantedObjects(ctx, fsOps, bootstrap)
}

// removeDanglingBootstrapPriorityLevel 删除不再被引导 FlowSchema 引用的 PriorityLevelConfiguration，保持配置引用关系一致。
func removeDanglingBootstrapPriorityLevel(ctx context.Context, clientset flowcontrolclient.FlowcontrolV1Interface, plcLister flowcontrollisters.PriorityLevelConfigurationLister) error {
	bootstrap := append(flowcontrolbootstrap.MandatoryPriorityLevelConfigurations, flowcontrolbootstrap.SuggestedPriorityLevelConfigurations...)
	plcOps := ensurer.NewPriorityLevelConfigurationOps(clientset.PriorityLevelConfigurations(), plcLister)
	return ensurer.RemoveUnwantedObjects(ctx, plcOps, bootstrap)
}

// contextFromChannelAndMaxWaitDuration 创建一个同时受停止通道和最长等待时长约束的上下文。
// stopCh 关闭或 maxWait 到期时，派生上下文都会被取消，以先发生者为准。
// 调用方必须始终调用返回的 CancelFunc，否则可能造成上下文及相关资源泄漏。
func contextFromChannelAndMaxWaitDuration(stopCh <-chan struct{}, maxWait time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()

		select {
		case <-stopCh:
		case <-time.After(maxWait):

		// 调用方主动取消上下文也表示应立即退出该 goroutine。
		// 这里可能再次调用 cancel；CancelFunc 是幂等的，重复调用不会产生额外副作用。
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
