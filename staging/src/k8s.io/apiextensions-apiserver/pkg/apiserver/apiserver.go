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

// Package apiserver 实现 CRD API Server 的核心逻辑。
// 负责：
//   - 注册 CRD 的 REST Storage 和 API 组
//   - 创建 CRD 请求处理器（动态安装/卸载 CR 的 HTTP 路由）
//   - 启动所有控制器（establishing、status、finalizer、OpenAPI 等）
//   - 管理 discovery 信息（/apis 端点动态反映已注册的 CR）
package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	externalinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	"k8s.io/apiextensions-apiserver/pkg/controller/apiapproval"
	"k8s.io/apiextensions-apiserver/pkg/controller/establish"
	"k8s.io/apiextensions-apiserver/pkg/controller/finalizer"
	"k8s.io/apiextensions-apiserver/pkg/controller/nonstructuralschema"
	openapicontroller "k8s.io/apiextensions-apiserver/pkg/controller/openapi"
	openapiv3controller "k8s.io/apiextensions-apiserver/pkg/controller/openapiv3"
	"k8s.io/apiextensions-apiserver/pkg/controller/status"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresourcedefinition"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/klog/v2"
)

var (
	// Scheme 包含 CRD 的所有版本类型，用于序列化和反序列化。
	Scheme = runtime.NewScheme()
	// Codecs 提供基于 Scheme 的编解码器工厂。
	Codecs = serializer.NewCodecFactory(Scheme)

	// 改动此处必须同步更新 crEncoder，两者对无版本对象的处理要一致。
	unversionedVersion = schema.GroupVersion{Group: "", Version: "v1"}
	// unversionedTypes 是无版本类型，用于 Watch 事件、错误状态等跨版本场景。
	unversionedTypes = []runtime.Object{
		&metav1.Status{},
		&metav1.WatchEvent{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	}
)

// init 初始化 Scheme，注册 CRD 的所有版本类型和无版本类型。
func init() {
	install.Install(Scheme)

	// 将 metav1 类型添加到空组 v1，用于错误响应等场景
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Group: "", Version: "v1"})

	Scheme.AddUnversionedTypes(unversionedVersion, unversionedTypes...)
}

// ExtraConfig 包含 CRD API Server 特有的配置项。
type ExtraConfig struct {
	// CRDRESTOptionsGetter 提供 CR 的存储选项（etcd 前缀、编解码器等）。
	CRDRESTOptionsGetter genericregistry.RESTOptionsGetter

	// MasterCount 用于检测集群是否为 HA 模式。
	// 在 HA 模式下，CRD 的建立（Establishing）会延迟 5 秒，
	// 避免多个 API Server 实例同时注册同一 CRD 导致竞争。
	MasterCount int

	// ServiceResolver 用于 CR 的 conversion webhook 解析 Service 名称为实际地址。
	ServiceResolver webhook.ServiceResolver
	// AuthResolverWrapper 用于 CR 的 webhook 认证信息解析。
	AuthResolverWrapper webhook.AuthenticationInfoResolverWrapper
}

// Config 是 CRD API Server 的完整配置，包含通用配置和 CRD 特有配置。
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

type CompletedConfig struct {
	// 内嵌私有指针，使外部包无法自行构造 CompletedConfig，只能走完整配置流程。
	*completedConfig
}

// CustomResourceDefinitions 是 CRD API Server 的主结构体。
// 封装了通用 API Server、CRD informer 和所有控制器。
type CustomResourceDefinitions struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	// provided for easier embedding
	Informers externalinformers.SharedInformerFactory
}

// Complete 完成配置，禁用内置 discovery（使用自定义 discovery 控制器）。
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	c.GenericConfig.EnableDiscovery = false

	return CompletedConfig{&c}
}

// New 从配置创建 CRD API Server 实例。
// 核心流程：
// 1. 创建通用 API Server
// 2. 注册 CRD 的 REST Storage（/apis/apiextensions.k8s.io/v1/customresourcedefinitions）
// 3. 创建 CRD 请求处理器（动态安装/卸载 CR 的 HTTP 路由）
// 4. 启动所有控制器（establishing、status、finalizer、OpenAPI、discovery 等）
// 5. 等待 CRD informer 同步完成后才报告健康
func (c completedConfig) New(delegationTarget genericapiserver.DelegationTarget) (*CustomResourceDefinitions, error) {
	validation.RegisterMetrics()
	genericServer, err := c.GenericConfig.New("apiextensions-apiserver", delegationTarget)
	if err != nil {
		return nil, err
	}

	// CRD informer 完成同步后关闭 hasCRDInformerSyncedSignal；
	// 这样在全部 HTTP 路由安装完成前请求自定义资源端点会得到 503 而非 404。
	hasCRDInformerSyncedSignal := make(chan struct{})
	if err := genericServer.RegisterMuxAndDiscoveryCompleteSignal("CRDInformerHasNotSynced", hasCRDInformerSyncedSignal); err != nil {
		return nil, err
	}

	s := &CustomResourceDefinitions{
		GenericAPIServer: genericServer,
	}

	apiResourceConfig := c.GenericConfig.MergedResourceConfig
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(apiextensions.GroupName, Scheme, metav1.ParameterCodec, Codecs)
	storage := map[string]rest.Storage{}
	// customresourcedefinitions
	if resource := "customresourcedefinitions"; apiResourceConfig.ResourceEnabled(v1.SchemeGroupVersion.WithResource(resource)) {
		customResourceDefinitionStorage, err := customresourcedefinition.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
		if err != nil {
			return nil, err
		}
		storage[resource] = customResourceDefinitionStorage
		storage[resource+"/status"] = customresourcedefinition.NewStatusREST(Scheme, customResourceDefinitionStorage)
	}
	if len(storage) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[v1.SchemeGroupVersion.Version] = storage
	}

	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	crdClient, err := clientset.NewForConfig(s.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		// 这个错误往外泄漏并不理想，但测试问题没修好之前只能先这样，
		// 否则无法继续推进。
		return nil, fmt.Errorf("failed to create clientset: %v", err)
	}
	s.Informers = externalinformers.NewSharedInformerFactory(crdClient, 5*time.Minute)

	delegateHandler := delegationTarget.UnprotectedHandler()
	if delegateHandler == nil {
		delegateHandler = http.NotFoundHandler()
	}

	versionDiscoveryHandler := &versionDiscoveryHandler{
		discovery: map[schema.GroupVersion]*discovery.APIVersionHandler{},
		delegate:  delegateHandler,
	}
	groupDiscoveryHandler := &groupDiscoveryHandler{
		discovery: map[string]*discovery.APIGroupHandler{},
		delegate:  delegateHandler,
	}
	establishingController := establish.NewEstablishingController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	crdHandler, err := NewCustomResourceDefinitionHandler(
		versionDiscoveryHandler,
		groupDiscoveryHandler,
		s.Informers.Apiextensions().V1().CustomResourceDefinitions(),
		delegateHandler,
		c.ExtraConfig.CRDRESTOptionsGetter,
		c.GenericConfig.AdmissionControl,
		establishingController,
		c.ExtraConfig.ServiceResolver,
		c.ExtraConfig.AuthResolverWrapper,
		c.ExtraConfig.MasterCount,
		s.GenericAPIServer.Authorizer,
		c.GenericConfig.RequestTimeout,
		time.Duration(c.GenericConfig.MinRequestTimeout)*time.Second,
		apiGroupInfo.StaticOpenAPISpec,
		c.GenericConfig.MaxRequestBodyBytes,
	)
	if err != nil {
		return nil, err
	}
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/apis", crdHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.HandlePrefix("/apis/", crdHandler)
	s.GenericAPIServer.RegisterDestroyFunc(crdHandler.destroy)

	aggregatedDiscoveryManager := genericServer.AggregatedDiscoveryGroupManager
	if aggregatedDiscoveryManager != nil {
		aggregatedDiscoveryManager = aggregatedDiscoveryManager.WithSource(aggregated.CRDSource)
	}
	discoveryController := NewDiscoveryController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), versionDiscoveryHandler, groupDiscoveryHandler, aggregatedDiscoveryManager)
	namingController := status.NewNamingConditionController(klog.TODO() /* for contextual logging */, s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	nonStructuralSchemaController := nonstructuralschema.NewConditionController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	apiApprovalController := apiapproval.NewKubernetesAPIApprovalPolicyConformantConditionController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	finalizingController := finalizer.NewCRDFinalizer(
		s.Informers.Apiextensions().V1().CustomResourceDefinitions(),
		crdClient.ApiextensionsV1(),
		crdHandler,
	)

	s.GenericAPIServer.AddPostStartHookOrDie("start-apiextensions-informers", func(hookContext genericapiserver.PostStartHookContext) error {
		s.Informers.Start(hookContext.Done())
		return nil
	})
	s.GenericAPIServer.AddPostStartHookOrDie("start-apiextensions-controllers", func(hookContext genericapiserver.PostStartHookContext) error {
		// OpenAPIVersionedService 与 StaticOpenAPISpec 由 generic apiserver 的 PrepareRun() 填充。
		// 二者共同提供 /openapi/v2 端点。若 generic apiserver 未启用 OpenAPI
		// （openAPIConfig 为 nil），则 OpenAPIVersionedService 与 StaticOpenAPISpec 都为 nil，
		// 此时就不启动 CRD OpenAPI 控制器。
		if s.GenericAPIServer.StaticOpenAPISpec != nil {
			if s.GenericAPIServer.OpenAPIVersionedService != nil {
				openapiController := openapicontroller.NewController(s.Informers.Apiextensions().V1().CustomResourceDefinitions())
				go openapiController.Run(s.GenericAPIServer.StaticOpenAPISpec, s.GenericAPIServer.OpenAPIVersionedService, hookContext.Done())
			}

			if s.GenericAPIServer.OpenAPIV3VersionedService != nil {
				openapiv3Controller := openapiv3controller.NewController(s.Informers.Apiextensions().V1().CustomResourceDefinitions())
				go openapiv3Controller.Run(s.GenericAPIServer.OpenAPIV3VersionedService, hookContext.Done())
			}
		}

		go namingController.RunWithContext(hookContext)
		go establishingController.RunWithContext(hookContext)
		go nonStructuralSchemaController.RunWithContext(5, hookContext)
		go apiApprovalController.RunWithContext(5, hookContext)
		go finalizingController.RunWithContext(5, hookContext)

		discoverySyncedCh := make(chan struct{})
		go discoveryController.Run(hookContext.Done(), discoverySyncedCh)
		select {
		case <-hookContext.Done():
		case <-discoverySyncedCh:
		}

		return nil
	})
	// 只有能处理所有已注册的 CRD 才算健康；等 informer 同步完成，
	// 是为了保证 lister 可用。启动后新增的 CRD 仍可能有竞争，
	// 但至少要等已存在的 CRD 都能处理后才上报健康。
	s.GenericAPIServer.AddPostStartHookOrDie("crd-informer-synced", func(ctx genericapiserver.PostStartHookContext) error {
		return wait.PollUntilContextCancel(ctx.Context, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
			if s.Informers.Apiextensions().V1().CustomResourceDefinitions().Informer().HasSynced() {
				close(hasCRDInformerSyncedSignal)
				return true, nil
			}
			return false, nil
		})
	})

	return s, nil
}

// DefaultAPIResourceConfigSource 返回默认启用的 API 资源配置。
// v1 和 v1beta1 版本的 CRD API 默认启用。
func DefaultAPIResourceConfigSource() *serverstorage.ResourceConfig {
	ret := serverstorage.NewResourceConfig()
	// 注意：这里列出的 GroupVersion 默认启用，不要把 alpha 版本放进来。
	ret.EnableVersions(
		v1beta1.SchemeGroupVersion,
		v1.SchemeGroupVersion,
	)

	return ret
}
