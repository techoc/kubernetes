/*
Copyright 2024 The Kubernetes Authors.

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

package apiserver

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	genericfeatures "k8s.io/apiserver/pkg/features"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilpeerproxy "k8s.io/apiserver/pkg/util/peerproxy"
	kubeexternalinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	v1helper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	apiregistrationclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
	informers "k8s.io/kube-aggregator/pkg/client/informers/externalversions/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers/autoregister"

	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/controlplane/controller/crdregistration"
)

// CreateAggregatorConfig 构造聚合服务器的配置，复用 kube-apiserver 配置为基础并做针对性调整。
func CreateAggregatorConfig(
	kubeAPIServerConfig genericapiserver.Config,
	commandOptions options.CompletedOptions,
	externalInformers kubeexternalinformers.SharedInformerFactory,
	serviceResolver aggregatorapiserver.ServiceResolver,
	proxyTransport *http.Transport,
	peerProxy utilpeerproxy.Interface,
	pluginInitializers []admission.PluginInitializer,
) (*aggregatorapiserver.Config, error) {
	// 浅拷贝配置：大部分配置保持不变，仅需调整与聚合服务器相关的少数项。
	genericConfig := kubeAPIServerConfig
	// 清空启动后钩子与 RESTOptionsGetter，避免与其他服务器重复安装。
	genericConfig.PostStartHooks = map[string]genericapiserver.PostStartHookConfigEntry{}
	genericConfig.RESTOptionsGetter = nil
	// 聚合服务器有自己定制的 OpenAPI handler，需阻止通用服务器重复安装。
	genericConfig.SkipOpenAPIInstallation = true

	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.StorageVersionAPI) &&
		utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerIdentity) {
		// StorageVersionPrecondition 会阻塞对内置资源的写请求，直到目标资源的存储版本更新完毕。
		genericConfig.BuildHandlerChainFunc = genericapiserver.BuildHandlerChainWithStorageVersionPrecondition
	}

	if peerProxy != nil {
		originalHandlerChainBuilder := genericConfig.BuildHandlerChainFunc
		genericConfig.BuildHandlerChainFunc = func(apiHandler http.Handler, c *genericapiserver.Config) http.Handler {
			// 先包裹对等节点代理处理器，使其位于处理器链的最外层。
			apiHandler = peerProxy.WrapHandler(apiHandler)
			return originalHandlerChainBuilder(apiHandler, c)
		}
	}

	// 复制 etcd 选项以避免修改原对象；假定其已完成，仅改动 StorageConfig 相关字段。
	etcdOptions := *commandOptions.Etcd
	etcdOptions.StorageConfig.Codec = aggregatorscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion, v1beta1.SchemeGroupVersion)
	etcdOptions.StorageConfig.EncodeVersioner = runtime.NewMultiGroupVersioner(v1.SchemeGroupVersion, schema.GroupKind{Group: v1beta1.GroupName})
	etcdOptions.SkipHealthEndpoints = true // 避免重复接入健康检查
	if err := etcdOptions.ApplyTo(&genericConfig); err != nil {
		return nil, err
	}

	// 用聚合服务器的默认资源配置与 scheme 覆盖 MergedResourceConfig。
	if err := commandOptions.APIEnablement.ApplyTo(
		&genericConfig,
		aggregatorapiserver.DefaultAPIResourceConfigSource(),
		aggregatorscheme.Scheme); err != nil {
		return nil, err
	}

	aggregatorConfig := &aggregatorapiserver.Config{
		GenericConfig: &genericapiserver.RecommendedConfig{
			Config:                genericConfig,
			SharedInformerFactory: externalInformers,
		},
		ExtraConfig: aggregatorapiserver.ExtraConfig{
			ProxyClientCertFile:       commandOptions.ProxyClientCertFile,
			ProxyClientKeyFile:        commandOptions.ProxyClientKeyFile,
			PeerAdvertiseAddress:      commandOptions.PeerAdvertiseAddress,
			ServiceResolver:           serviceResolver,
			ProxyTransport:            proxyTransport,
			RejectForwardingRedirects: commandOptions.AggregatorRejectForwardingRedirects,
			PeerProxy:                 peerProxy,
		},
	}

	// 清空启动后钩子，避免所有服务器重复添加导致失败。
	aggregatorConfig.GenericConfig.PostStartHooks = map[string]genericapiserver.PostStartHookConfigEntry{}

	return aggregatorConfig, nil
}

// CreateAggregatorServer 创建聚合服务器，注册 peer proxy 的 informer handler 并启动自动注册控制器。
func CreateAggregatorServer(aggregatorConfig aggregatorapiserver.CompletedConfig, delegateAPIServer genericapiserver.DelegationTarget, crds apiextensionsinformers.CustomResourceDefinitionInformer, crdAPIEnabled bool, apiVersionPriorities map[schema.GroupVersion]APIServicePriority) (*aggregatorapiserver.APIAggregator, error) {
	aggregatorServer, err := aggregatorConfig.NewWithDelegate(delegateAPIServer)
	if err != nil {
		return nil, err
	}

	// 为 peer proxy 注册 informer handler，将 CRD 与 APIService 的 GV 排除在 peer 代理与聚合发现之外。
	if aggregatorConfig.ExtraConfig.PeerProxy != nil {
		if err := aggregatorConfig.ExtraConfig.PeerProxy.RegisterCRDInformerHandlers(crds.Informer(), crdExtractor); err != nil {
			return nil, err
		}
		if err := aggregatorConfig.ExtraConfig.PeerProxy.RegisterAPIServiceInformerHandlers(
			aggregatorServer.APIRegistrationInformers.Apiregistration().V1().APIServices().Informer(),
			apiServiceExtractor,
		); err != nil {
			return nil, err
		}
	}

	// 创建用于自动注册的控制器。
	apiRegistrationClient, err := apiregistrationclient.NewForConfig(aggregatorConfig.GenericConfig.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	autoRegistrationController := autoregister.NewAutoRegisterController(aggregatorServer.APIRegistrationInformers.Apiregistration().V1().APIServices(), apiRegistrationClient)
	apiServices := apiServicesToRegister(delegateAPIServer, autoRegistrationController, apiVersionPriorities)

	type controller interface {
		Run(workers int, stopCh <-chan struct{})
		WaitForInitialSync()
	}
	var crdRegistrationController controller
	if crdAPIEnabled {
		crdRegistrationController = crdregistration.NewCRDRegistrationController(
			crds,
			autoRegistrationController)
	}

	// 将内置组的优先级写入聚合发现，使其顺序与本地发现一致。
	if aggregatorConfig.GenericConfig.AggregatedDiscoveryGroupManager != nil {
		for gv, entry := range apiVersionPriorities {
			aggregatorConfig.GenericConfig.AggregatedDiscoveryGroupManager.SetGroupVersionPriority(metav1.GroupVersion(gv), int(entry.Group), int(entry.Version))
		}
	}

	err = aggregatorServer.GenericAPIServer.AddPostStartHook("kube-apiserver-autoregistration", func(context genericapiserver.PostStartHookContext) error {
		if crdAPIEnabled {
			go crdRegistrationController.Run(5, context.Done())
		}
		go func() {
			// 先让 CRD 控制器处理初始 CRD 集合，再启动自动注册控制器，
			// 防止自动注册的初始同步误删仍存在的 CRD 所对应的 APIService。
			// 此处无法依赖发现接口，因为本服务器就是发现的来源。
			if crdAPIEnabled {
				klog.Infof("waiting for initial CRD sync...")
				crdRegistrationController.WaitForInitialSync()
				klog.Infof("initial CRD sync complete...")
			} else {
				klog.Infof("CRD API not enabled, starting APIService registration without waiting for initial CRD sync")
			}
			autoRegistrationController.Run(5, context.Done())
		}()
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = aggregatorServer.GenericAPIServer.AddBootSequenceHealthChecks(
		makeAPIServiceAvailableHealthCheck(
			"autoregister-completion",
			apiServices,
			aggregatorServer.APIRegistrationInformers.Apiregistration().V1().APIServices(),
		),
	)
	if err != nil {
		return nil, err
	}

	return aggregatorServer, nil
}

// crdExtractor 从 CustomResourceDefinition 对象中提取组版本，供 peer proxy 排除过滤器使用。
func crdExtractor(obj interface{}) []schema.GroupVersion {
	crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		klog.Errorf("Expected CustomResourceDefinition but got %T", obj)
		return nil
	}
	var gvs []schema.GroupVersion
	for _, v := range crd.Spec.Versions {
		gvs = append(gvs, schema.GroupVersion{Group: crd.Spec.Group, Version: v.Name})
	}
	return gvs
}

// apiServiceExtractor 从 APIService 对象中提取组版本，供 peer proxy 排除过滤器使用；仅排除指向 Service 的聚合 API。
func apiServiceExtractor(obj interface{}) []schema.GroupVersion {
	apiService, ok := obj.(*v1.APIService)
	if !ok {
		klog.Errorf("Expected APIService but got %T", obj)
		return nil
	}
	// 只有指向 Service 的 APIService 才是聚合 API，才需要排除。
	if apiService.Spec.Service == nil {
		return nil
	}
	gv := schema.GroupVersion{Group: apiService.Spec.Group, Version: apiService.Spec.Version}
	return []schema.GroupVersion{gv}
}

// makeAPIService 为给定的组版本构造 APIService；若版本未配置优先级则返回 nil。
func makeAPIService(gv schema.GroupVersion, apiVersionPriorities map[schema.GroupVersion]APIServicePriority) *v1.APIService {
	apiServicePriority, ok := apiVersionPriorities[gv]
	if !ok {
		// 未配置优先级时不应注册，否则 CRD 的组版本可能永久滞留在 APIServices 列表中。
		klog.Infof("Skipping APIService creation for %v", gv)
		return nil
	}
	return &v1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: gv.Version + "." + gv.Group},
		Spec: v1.APIServiceSpec{
			Group:                gv.Group,
			Version:              gv.Version,
			GroupPriorityMinimum: apiServicePriority.Group,
			VersionPriority:      apiServicePriority.Version,
		},
	}
}

// makeAPIServiceAvailableHealthCheck 返回一个 healthz 检查：当所有指定服务至少被观测到一次可用时即健康。
func makeAPIServiceAvailableHealthCheck(name string, apiServices []*v1.APIService, apiServiceInformer informers.APIServiceInformer) healthz.HealthChecker {
	// 记录尚未观测到可用的自动注册 API 服务。
	pendingServiceNamesLock := &sync.RWMutex{}
	pendingServiceNames := sets.NewString()
	for _, service := range apiServices {
		pendingServiceNames.Insert(service.Name)
	}

	// APIService 变为可用时，将其从待处理集合中移除。
	handleAPIServiceChange := func(service *v1.APIService) {
		pendingServiceNamesLock.Lock()
		defer pendingServiceNamesLock.Unlock()
		if !pendingServiceNames.Has(service.Name) {
			return
		}
		if v1helper.IsAPIServiceConditionTrue(service, v1.Available) {
			pendingServiceNames.Delete(service.Name)
		}
	}

	// 监听 APIService 的新增与更新事件。
	apiServiceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck // 该回调无法返回错误
		AddFunc:    func(obj interface{}) { handleAPIServiceChange(obj.(*v1.APIService)) },
		UpdateFunc: func(old, new interface{}) { handleAPIServiceChange(new.(*v1.APIService)) },
	})

	// 待处理集合未清空前不报告健康。
	return healthz.NamedCheck(name, func(r *http.Request) error {
		pendingServiceNamesLock.RLock()
		defer pendingServiceNamesLock.RUnlock()
		if pendingServiceNames.Len() > 0 {
			return fmt.Errorf("missing APIService: %v", pendingServiceNames.List())
		}
		return nil
	})
}

// APIServicePriority 定义发现时使用的组优先级，决定 kubectl 输出中组的位置。
type APIServicePriority struct {
	// Group 表示该组相对于其他组的顺序。
	Group int32
	// Version 表示该版本在其组内的相对顺序。
	Version int32
}

// DefaultGenericAPIServicePriorities 返回通用 API 的 APIService 优先级。
func DefaultGenericAPIServicePriorities() map[schema.GroupVersion]APIServicePriority {
	// 理想做法是重构 DelegationTarget 让其携带已安装 API 的优先级，但影响面过大，
	// 因此这里直接硬编码优先级列表，而非在构建存储时动态生成。
	return map[schema.GroupVersion]APIServicePriority{
		{Group: "", Version: "v1"}: {Group: 18000, Version: 1},
		// 据目前所知，以下各组的优先级不会冲突。
		{Group: "events.k8s.io", Version: "v1"}:                      {Group: 17750, Version: 15},
		{Group: "events.k8s.io", Version: "v1beta1"}:                 {Group: 17750, Version: 5},
		{Group: "authentication.k8s.io", Version: "v1"}:              {Group: 17700, Version: 15},
		{Group: "authentication.k8s.io", Version: "v1beta1"}:         {Group: 17700, Version: 9},
		{Group: "authentication.k8s.io", Version: "v1alpha1"}:        {Group: 17700, Version: 1},
		{Group: "authorization.k8s.io", Version: "v1"}:               {Group: 17600, Version: 15},
		{Group: "certificates.k8s.io", Version: "v1"}:                {Group: 17300, Version: 15},
		{Group: "certificates.k8s.io", Version: "v1beta1"}:           {Group: 17300, Version: 9},
		{Group: "certificates.k8s.io", Version: "v1alpha1"}:          {Group: 17300, Version: 1},
		{Group: "rbac.authorization.k8s.io", Version: "v1"}:          {Group: 17000, Version: 15},
		{Group: "apiextensions.k8s.io", Version: "v1"}:               {Group: 16700, Version: 15},
		{Group: "admissionregistration.k8s.io", Version: "v1"}:       {Group: 16700, Version: 15},
		{Group: "admissionregistration.k8s.io", Version: "v1beta1"}:  {Group: 16700, Version: 12},
		{Group: "admissionregistration.k8s.io", Version: "v1alpha1"}: {Group: 16700, Version: 9},
		{Group: "coordination.k8s.io", Version: "v1"}:                {Group: 16500, Version: 15},
		{Group: "coordination.k8s.io", Version: "v1beta1"}:           {Group: 16500, Version: 13},
		{Group: "coordination.k8s.io", Version: "v1alpha2"}:          {Group: 16500, Version: 12},
		{Group: "discovery.k8s.io", Version: "v1"}:                   {Group: 16200, Version: 15},
		{Group: "discovery.k8s.io", Version: "v1beta1"}:              {Group: 16200, Version: 12},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1"}:       {Group: 16100, Version: 21},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta3"}:  {Group: 16100, Version: 18},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta2"}:  {Group: 16100, Version: 15},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1beta1"}:  {Group: 16100, Version: 12},
		{Group: "flowcontrol.apiserver.k8s.io", Version: "v1alpha1"}: {Group: 16100, Version: 9},
		{Group: "internal.apiserver.k8s.io", Version: "v1alpha1"}:    {Group: 16000, Version: 9},
		{Group: "resource.k8s.io", Version: "v1alpha3"}:              {Group: 15900, Version: 9},
		{Group: "storagemigration.k8s.io", Version: "v1"}:            {Group: 15800, Version: 15},
		{Group: "storagemigration.k8s.io", Version: "v1beta1"}:       {Group: 15800, Version: 9},
		// 不确定时把新组追加到列表末尾，组优先级可取现有最小值减 100，版本优先级设为 9 以留出空间。
	}
}

// apiServicesToRegister 遍历委托服务器暴露的路径，生成需要自动注册的 APIService 列表。
func apiServicesToRegister(delegateAPIServer genericapiserver.DelegationTarget, registration autoregister.AutoAPIServiceRegistration, apiVersionPriorities map[schema.GroupVersion]APIServicePriority) []*v1.APIService {
	apiServices := []*v1.APIService{}

	for _, curr := range delegateAPIServer.ListedPaths() {
		if curr == "/api/v1" {
			apiService := makeAPIService(schema.GroupVersion{Group: "", Version: "v1"}, apiVersionPriorities)
			registration.AddAPIServiceToSyncOnStart(apiService)
			apiServices = append(apiServices, apiService)
			continue
		}

		if !strings.HasPrefix(curr, "/apis/") {
			continue
		}
		// 路径形如 /apis/rbac.authorization.k8s.io/v1alpha1，按 / 切分后取组与版本。
		tokens := strings.Split(curr, "/")
		if len(tokens) != 4 {
			continue
		}

		apiService := makeAPIService(schema.GroupVersion{Group: tokens[2], Version: tokens[3]}, apiVersionPriorities)
		if apiService == nil {
			continue
		}
		registration.AddAPIServiceToSyncOnStart(apiService)
		apiServices = append(apiServices, apiService)
	}

	return apiServices
}
