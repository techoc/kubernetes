/*
Copyright 2014 The Kubernetes Authors.

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

package controlplane

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	admissionregistrationv1alpha1 "k8s.io/api/admissionregistration/v1alpha1"
	admissionregistrationv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	apiserverinternalv1alpha1 "k8s.io/api/apiserverinternal/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authenticationv1alpha1 "k8s.io/api/authentication/v1alpha1"
	authenticationv1beta1 "k8s.io/api/authentication/v1beta1"
	authorizationapiv1 "k8s.io/api/authorization/v1"
	autoscalingapiv1 "k8s.io/api/autoscaling/v1"
	autoscalingapiv2 "k8s.io/api/autoscaling/v2"
	batchapiv1 "k8s.io/api/batch/v1"
	certificatesapiv1 "k8s.io/api/certificates/v1"
	certificatesv1alpha1 "k8s.io/api/certificates/v1alpha1"
	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	coordinationapiv1 "k8s.io/api/coordination/v1"
	coordinationv1alpha2 "k8s.io/api/coordination/v1alpha2"
	coordinationv1beta1 "k8s.io/api/coordination/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	eventsv1 "k8s.io/api/events/v1"
	lifecyclev1alpha1 "k8s.io/api/lifecycle/v1alpha1"
	networkingapiv1 "k8s.io/api/networking/v1"
	networkingapiv1beta1 "k8s.io/api/networking/v1beta1"
	nodev1 "k8s.io/api/node/v1"
	policyapiv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	resourcev1 "k8s.io/api/resource/v1"
	resourcev1alpha3 "k8s.io/api/resource/v1alpha3"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	resourcev1beta2 "k8s.io/api/resource/v1beta2"
	schedulingapiv1 "k8s.io/api/scheduling/v1"
	schedulingapiv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingapiv1beta1 "k8s.io/api/scheduling/v1beta1"
	storageapiv1 "k8s.io/api/storage/v1"
	storageapiv1alpha1 "k8s.io/api/storage/v1alpha1"
	storageapiv1beta1 "k8s.io/api/storage/v1beta1"
	svmv1 "k8s.io/api/storagemigration/v1"
	svmv1beta1 "k8s.io/api/storagemigration/v1beta1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	discoveryclient "k8s.io/client-go/kubernetes/typed/discovery/v1"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	flowcontrolv1 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1"
	flowcontrolv1beta1 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1beta1"
	flowcontrolv1beta2 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1beta2"
	flowcontrolv1beta3 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1beta3"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/controlplane/controller/defaultservicecidr"
	"k8s.io/kubernetes/pkg/controlplane/controller/kubernetesservice"
	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
	"k8s.io/kubernetes/pkg/features"
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
	kubeletclient "k8s.io/kubernetes/pkg/kubelet/client"

	// RESTStorage 安装器（副作用导入）。
	admissionregistrationrest "k8s.io/kubernetes/pkg/registry/admissionregistration/rest"
	apiserverinternalrest "k8s.io/kubernetes/pkg/registry/apiserverinternal/rest"
	appsrest "k8s.io/kubernetes/pkg/registry/apps/rest"
	authenticationrest "k8s.io/kubernetes/pkg/registry/authentication/rest"
	authorizationrest "k8s.io/kubernetes/pkg/registry/authorization/rest"
	autoscalingrest "k8s.io/kubernetes/pkg/registry/autoscaling/rest"
	batchrest "k8s.io/kubernetes/pkg/registry/batch/rest"
	certificatesrest "k8s.io/kubernetes/pkg/registry/certificates/rest"
	coordinationrest "k8s.io/kubernetes/pkg/registry/coordination/rest"
	corerest "k8s.io/kubernetes/pkg/registry/core/rest"
	discoveryrest "k8s.io/kubernetes/pkg/registry/discovery/rest"
	eventsrest "k8s.io/kubernetes/pkg/registry/events/rest"
	flowcontrolrest "k8s.io/kubernetes/pkg/registry/flowcontrol/rest"
	lifecyclerest "k8s.io/kubernetes/pkg/registry/lifecycle/rest"
	networkingrest "k8s.io/kubernetes/pkg/registry/networking/rest"
	noderest "k8s.io/kubernetes/pkg/registry/node/rest"
	policyrest "k8s.io/kubernetes/pkg/registry/policy/rest"
	rbacrest "k8s.io/kubernetes/pkg/registry/rbac/rest"
	resourcerest "k8s.io/kubernetes/pkg/registry/resource/rest"
	schedulingrest "k8s.io/kubernetes/pkg/registry/scheduling/rest"
	storagerest "k8s.io/kubernetes/pkg/registry/storage/rest"
	svmrest "k8s.io/kubernetes/pkg/registry/storagemigration/rest"
)

const (
	// DefaultEndpointReconcilerInterval 是 Kubernetes Service 端点的默认协调间隔。
	DefaultEndpointReconcilerInterval = 10 * time.Second
	// DefaultEndpointReconcilerTTL 是存储层的默认 TTL。
	DefaultEndpointReconcilerTTL = 15 * time.Second
	// IdentityLeaseComponentLabelKey 用于给 identity 租约打组件标签，表明：
	//   1. 该租约是 identity 租约（不同于 leader election 租约）
	//   2. 该租约归属哪个组件
	// TODO(sttts): 移除此间接引用。
	IdentityLeaseComponentLabelKey = controlplaneapiserver.IdentityLeaseComponentLabelKey
	// repairLoopInterval 是 Service ClusterIP 与 NodePort 修复循环的执行间隔。
	repairLoopInterval = 3 * time.Minute
)

var (
	// AdditionalStorageProvidersForTests 供集成测试注入额外的测试用 API 组。
	AdditionalStorageProvidersForTests func(client *kubernetes.Clientset) []controlplaneapiserver.RESTStorageProvider
)

// Extra 定义 kube-apiserver 的额外配置。
type Extra struct {
	EndpointReconcilerConfig EndpointReconcilerConfig
	KubeletClientConfig      kubeletclient.KubeletClientConfig

	// 以下值用于构造发现接口使用的 IP 地址：ServiceIPRange 是 ClusterIP 及以上类型 Service 的 IP 范围。
	ServiceIPRange net.IPNet
	// GenericAPIServer Service 的 IP（必须在 ServiceIPRange 内）。
	APIServerServiceIP net.IP

	// 双栈场景下 SecondaryServiceIPRange 是备用 IP 范围，地址族必须与主范围不同。
	SecondaryServiceIPRange net.IPNet
	// GenericAPIServer Service 的备用 IP（必须在 SecondaryServiceIPRange 内）。
	SecondaryAPIServerServiceIP net.IP

	// apiserver Service 的端口。
	APIServerServicePort int

	// TODO: 可将 Service 相关字段归入子结构，便于配置。

	// NodePort 及以上类型 Service 的端口范围。
	ServiceNodePortRange utilnet.PortRange
	// 非零时 "kubernetes" Service 使用该端口作为 NodePort。
	KubernetesServiceNodePort int

	// 运行的 master 数量；所有 master 必须配置相同值（大于 1 目前未测试）。
	MasterCount int

	// MasterEndpointReconcileTTL 是每个 master 记录的端点 TTL（秒），检查间隔为其 2/3，默认 15s。
	// 超大集群可调大该值，降低因 etcd 负载导致租约过期、master 端点频繁进出的概率；不建议低于 15s。
	MasterEndpointReconcileTTL time.Duration

	// 选择使用的 endpoint reconciler。
	EndpointReconcilerType reconcilers.Type

	// RepairServicesInterval 是 Service NodePort 与 ClusterIP 修复循环的间隔。
	RepairServicesInterval time.Duration
}

// Config 定义 master 的配置。
type Config struct {
	ControlPlane controlplaneapiserver.Config
	Extra
}

// completedConfig 保存完成后的内部配置。
type completedConfig struct {
	ControlPlane controlplaneapiserver.CompletedConfig
	*Extra
}

// CompletedConfig 内嵌私有指针，防止本包之外实例化。
type CompletedConfig struct {
	*completedConfig
}

// EndpointReconcilerConfig 保存 master 使用的 endpoint reconciler 及其协调间隔。
type EndpointReconcilerConfig struct {
	Reconciler reconcilers.EndpointReconciler
	Interval   time.Duration
}

// Instance 保存 Kubernetes 集群 apiserver 实例的状态。
type Instance struct {
	ControlPlane *controlplaneapiserver.Server
}

// createMasterCountReconciler 创建基于 master 数量的 endpoint reconciler。
func (c *Config) createMasterCountReconciler() reconcilers.EndpointReconciler {
	endpointClient := corev1client.NewForConfigOrDie(c.ControlPlane.Generic.LoopbackClientConfig)
	endpointSliceClient := discoveryclient.NewForConfigOrDie(c.ControlPlane.Generic.LoopbackClientConfig)
	endpointsAdapter := reconcilers.NewEndpointsAdapter(endpointClient, endpointSliceClient)

	return reconcilers.NewMasterCountEndpointReconciler(c.Extra.MasterCount, endpointsAdapter)
}

// createNoneReconciler 创建空操作 endpoint reconciler。
func (c *Config) createNoneReconciler() reconcilers.EndpointReconciler {
	return reconcilers.NewNoneEndpointReconciler()
}

// createLeaseReconciler 创建基于租约的 endpoint reconciler。
func (c *Config) createLeaseReconciler() reconcilers.EndpointReconciler {
	endpointClient := corev1client.NewForConfigOrDie(c.ControlPlane.Generic.LoopbackClientConfig)
	endpointSliceClient := discoveryclient.NewForConfigOrDie(c.ControlPlane.Generic.LoopbackClientConfig)
	endpointsAdapter := reconcilers.NewEndpointsAdapter(endpointClient, endpointSliceClient)

	ttl := c.Extra.MasterEndpointReconcileTTL
	config, err := c.ControlPlane.StorageFactory.NewConfig(api.Resource("apiServerIPInfo"), &api.Endpoints{})
	if err != nil {
		klog.Fatalf("Error creating storage factory config: %v", err)
	}
	masterLeases, err := reconcilers.NewLeases(config, "/masterleases/", ttl)
	if err != nil {
		klog.Fatalf("Error creating leases: %v", err)
	}

	return reconcilers.NewLeaseEndpointReconciler(endpointsAdapter, masterLeases)
}

// createEndpointReconciler 按配置类型创建对应的 endpoint reconciler。
func (c *Config) createEndpointReconciler() reconcilers.EndpointReconciler {
	klog.Infof("Using reconciler: %v", c.Extra.EndpointReconcilerType)
	switch c.Extra.EndpointReconcilerType {
	// 许多测试依赖默认的 master-count 控制器。
	case reconcilers.MasterCountReconcilerType:
		return c.createMasterCountReconciler()
	case "", reconcilers.LeaseEndpointReconcilerType:
		return c.createLeaseReconciler()
	case reconcilers.NoneEndpointReconcilerType:
		return c.createNoneReconciler()
	default:
		klog.Fatalf("Reconciler not implemented: %v", c.Extra.EndpointReconcilerType)
	}
	return nil
}

// Complete 补全未设置的必填字段，会修改接收者。
func (c *Config) Complete() CompletedConfig {
	if c.ControlPlane.PeerEndpointReconcileInterval == 0 && c.EndpointReconcilerConfig.Interval != 0 {
		// 在通用控制面完成配置前，先用 endpoint reconciler 的间隔作为默认值。
		c.ControlPlane.PeerEndpointReconcileInterval = c.EndpointReconcilerConfig.Interval
	}

	cfg := completedConfig{
		c.ControlPlane.Complete(),
		&c.Extra,
	}

	serviceIPRange, apiServerServiceIP, err := options.ServiceIPRange(cfg.Extra.ServiceIPRange)
	if err != nil {
		klog.Fatalf("Error determining service IP ranges: %v", err)
	}
	if cfg.Extra.ServiceIPRange.IP == nil {
		cfg.Extra.ServiceIPRange = serviceIPRange
	}
	if cfg.Extra.APIServerServiceIP == nil {
		cfg.Extra.APIServerServiceIP = apiServerServiceIP
	}

	// 覆盖通用控制面的默认发现地址，加入 Service IP 支持。
	discoveryAddresses := discovery.DefaultAddresses{DefaultAddress: cfg.ControlPlane.Generic.ExternalAddress}
	discoveryAddresses.CIDRRules = append(discoveryAddresses.CIDRRules,
		discovery.CIDRRule{IPRange: cfg.Extra.ServiceIPRange, Address: net.JoinHostPort(cfg.Extra.APIServerServiceIP.String(), strconv.Itoa(cfg.Extra.APIServerServicePort))})
	cfg.ControlPlane.Generic.DiscoveryAddresses = discoveryAddresses

	if cfg.Extra.ServiceNodePortRange.Size == 0 {
		// TODO: 目前无法指定空范围。对不需要 NodePort 做负载均衡的云（如 GCE）或许应允许，
		// 但会破坏 ServiceType 的严格嵌套关系，v1 后再审视。
		cfg.Extra.ServiceNodePortRange = kubeoptions.DefaultServiceNodePortRange
		klog.Infof("Node port range unspecified. Defaulting to %v.", cfg.Extra.ServiceNodePortRange)
	}

	if cfg.Extra.EndpointReconcilerConfig.Interval == 0 {
		cfg.Extra.EndpointReconcilerConfig.Interval = DefaultEndpointReconcilerInterval
	}

	if cfg.Extra.MasterEndpointReconcileTTL == 0 {
		cfg.Extra.MasterEndpointReconcileTTL = DefaultEndpointReconcilerTTL
	}

	if cfg.Extra.EndpointReconcilerConfig.Reconciler == nil {
		cfg.Extra.EndpointReconcilerConfig.Reconciler = c.createEndpointReconciler()
	}

	if cfg.Extra.RepairServicesInterval == 0 {
		cfg.Extra.RepairServicesInterval = repairLoopInterval
	}

	return CompletedConfig{&cfg}
}

// New 根据给定配置创建 Master 实例。部分未设置字段会被赋予默认值，KubeletClientConfig 必须显式指定。
func (c CompletedConfig) New(delegationTarget genericapiserver.DelegationTarget) (*Instance, error) {
	if reflect.DeepEqual(c.Extra.KubeletClientConfig, kubeletclient.KubeletClientConfig{}) {
		return nil, fmt.Errorf("Master.New() called with empty config.KubeletClientConfig")
	}

	cp, err := c.ControlPlane.New(controlplaneapiserver.KubeAPIServer, delegationTarget)
	if err != nil {
		return nil, err
	}

	s := &Instance{
		ControlPlane: cp,
	}

	client, err := kubernetes.NewForConfig(c.ControlPlane.Generic.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}

	restStorageProviders, err := c.StorageProviders(client)
	if err != nil {
		return nil, err
	}

	if err := s.ControlPlane.InstallAPIs(restStorageProviders...); err != nil {
		return nil, err
	}

	_, publicServicePort, err := c.ControlPlane.Generic.SecureServing.HostPort()
	if err != nil {
		return nil, fmt.Errorf("failed to get listener address: %w", err)
	}

	if err := c.Extra.EndpointReconcilerConfig.Reconciler.ValidateIP(c.ControlPlane.Generic.PublicAddress); err != nil {
		return nil, fmt.Errorf("cannot use public IP %s with endpoint reconciler: %w", c.ControlPlane.Generic.PublicAddress.String(), err)
	}

	kubernetesServiceCtrl := kubernetesservice.New(kubernetesservice.Config{
		PublicIP: c.ControlPlane.Generic.PublicAddress,

		EndpointReconciler: c.Extra.EndpointReconcilerConfig.Reconciler,
		EndpointInterval:   c.Extra.EndpointReconcilerConfig.Interval,

		ServiceIP:                 c.Extra.APIServerServiceIP,
		ServicePort:               c.Extra.APIServerServicePort,
		PublicServicePort:         publicServicePort,
		KubernetesServiceNodePort: c.Extra.KubernetesServiceNodePort,
	}, client, c.ControlPlane.Extra.VersionedInformers.Core().V1().Services())
	s.ControlPlane.GenericAPIServer.AddPostStartHookOrDie("bootstrap-controller", func(hookContext genericapiserver.PostStartHookContext) error {
		kubernetesServiceCtrl.Start(hookContext.Done())
		return nil
	})
	s.ControlPlane.GenericAPIServer.AddPreShutdownHookOrDie("stop-kubernetes-service-controller", func() error {
		kubernetesServiceCtrl.Stop()
		return nil
	})

	if utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
		s.ControlPlane.GenericAPIServer.AddPostStartHookOrDie("start-kubernetes-service-cidr-controller", func(hookContext genericapiserver.PostStartHookContext) error {
			controller := defaultservicecidr.NewController(
				c.Extra.ServiceIPRange,
				c.Extra.SecondaryServiceIPRange,
				client,
			)
			// 默认 ServiceCIDR 必须在 apiserver 健康前就绪，否则 Service 分配器无法工作。
			controller.Start(hookContext)
			return nil
		})
	}

	return s, nil

}

// StorageProviders 返回 kube-apiserver 的全部 REST 存储提供者。
func (c CompletedConfig) StorageProviders(client *kubernetes.Clientset) ([]controlplaneapiserver.RESTStorageProvider, error) {
	legacyRESTStorageProvider, err := corerest.New(corerest.Config{
		GenericConfig: *c.ControlPlane.NewCoreGenericConfig(),
		Proxy: corerest.ProxyConfig{
			Transport:           c.ControlPlane.Extra.ProxyTransport,
			KubeletClientConfig: c.Extra.KubeletClientConfig,
		},
		Services: corerest.ServicesConfig{
			ClusterIPRange:          c.Extra.ServiceIPRange,
			SecondaryClusterIPRange: c.Extra.SecondaryServiceIPRange,
			NodePortRange:           c.Extra.ServiceNodePortRange,
			IPRepairInterval:        c.Extra.RepairServicesInterval,
		},
		EndpointSliceGetter: c.ControlPlane.Extra.EndpointSliceGetter,
		Authorizer:          c.ControlPlane.Generic.Authorization.Authorizer,
	}, c.ControlPlane.Generic.Authorization.Authorizer)
	if err != nil {
		return nil, err
	}

	// 列表顺序会被发现接口保留：多个组存在同名资源时，顺序决定非限定资源名优先归属的组。
	// 本地发现使用该顺序，聚合时由 aggregator.go 的特定优先级覆盖。
	// TODO: 将优先级下沉到 RESTStorageProviders 并贯穿各发现处理器。
	providers := []controlplaneapiserver.RESTStorageProvider{
		legacyRESTStorageProvider,
		apiserverinternalrest.StorageProvider{},
		authenticationrest.RESTStorageProvider{Authenticator: c.ControlPlane.Generic.Authentication.Authenticator, APIAudiences: c.ControlPlane.Generic.Authentication.APIAudiences},
		authorizationrest.RESTStorageProvider{Authorizer: c.ControlPlane.Generic.Authorization.Authorizer, RuleResolver: c.ControlPlane.Generic.RuleResolver},
		autoscalingrest.RESTStorageProvider{},
		batchrest.RESTStorageProvider{},
		certificatesrest.RESTStorageProvider{Authorizer: c.ControlPlane.Generic.Authorization.Authorizer},
		coordinationrest.RESTStorageProvider{},
		discoveryrest.StorageProvider{},
		lifecyclerest.RESTStorageProvider{},
		networkingrest.RESTStorageProvider{},
		noderest.RESTStorageProvider{},
		policyrest.RESTStorageProvider{},
		rbacrest.RESTStorageProvider{Authorizer: c.ControlPlane.Generic.Authorization.Authorizer},
		schedulingrest.RESTStorageProvider{},
		storagerest.RESTStorageProvider{},
		svmrest.RESTStorageProvider{},
		flowcontrolrest.RESTStorageProvider{InformerFactory: c.ControlPlane.Generic.SharedInformerFactory},
		// apps 放在 extensions 之后，使旧客户端优先解析共享资源名的 extensions 版本（见 issue #42392）。
		appsrest.StorageProvider{},
		admissionregistrationrest.RESTStorageProvider{Authorizer: c.ControlPlane.Generic.Authorization.Authorizer, DiscoveryClient: client.Discovery()},
		eventsrest.RESTStorageProvider{TTL: c.ControlPlane.EventTTL},
		resourcerest.RESTStorageProvider{
			NamespaceClient: client.CoreV1().Namespaces(),
			Authorizer:      c.ControlPlane.Generic.Authorization.Authorizer,
		},
	}

	if AdditionalStorageProvidersForTests != nil {
		providers = append(providers, AdditionalStorageProvidersForTests(client)...)
	}

	return providers, nil
}

var (
	// genericStableAPIGroupVersionsEnabledByDefault 是通用存储提供者所提供 API 组的默认启用稳定版本。
	genericStableAPIGroupVersionsEnabledByDefault = []schema.GroupVersion{
		admissionregistrationv1.SchemeGroupVersion,
		apiv1.SchemeGroupVersion,
		authenticationv1.SchemeGroupVersion,
		authorizationapiv1.SchemeGroupVersion,
		certificatesapiv1.SchemeGroupVersion,
		coordinationapiv1.SchemeGroupVersion,
		eventsv1.SchemeGroupVersion,
		rbacv1.SchemeGroupVersion,
		flowcontrolv1.SchemeGroupVersion,
	}
	// stableAPIGroupVersionsEnabledByDefault 是仅 kube-apiserver 提供的额外 API 组的默认启用稳定版本。
	stableAPIGroupVersionsEnabledByDefault = []schema.GroupVersion{
		appsv1.SchemeGroupVersion,
		autoscalingapiv1.SchemeGroupVersion,
		autoscalingapiv2.SchemeGroupVersion,
		batchapiv1.SchemeGroupVersion,
		discoveryv1.SchemeGroupVersion,
		networkingapiv1.SchemeGroupVersion,
		nodev1.SchemeGroupVersion,
		policyapiv1.SchemeGroupVersion,
		resourcev1.SchemeGroupVersion,
		storageapiv1.SchemeGroupVersion,
		schedulingapiv1.SchemeGroupVersion,
		svmv1.SchemeGroupVersion,
	}

	// genericBetaAPIGroupVersionsDisabledByDefault 是通用 API 组中所有默认禁用的 beta 版本。
	genericBetaAPIGroupVersionsDisabledByDefault = []schema.GroupVersion{
		admissionregistrationv1beta1.SchemeGroupVersion,
		authenticationv1beta1.SchemeGroupVersion,
		certificatesv1beta1.SchemeGroupVersion,
		coordinationv1beta1.SchemeGroupVersion,
		flowcontrolv1beta1.SchemeGroupVersion,
		flowcontrolv1beta2.SchemeGroupVersion,
		flowcontrolv1beta3.SchemeGroupVersion,
		svmv1beta1.SchemeGroupVersion,
	}
	// betaAPIGroupVersionsDisabledByDefault 是 kube-apiserver 额外 API 组中默认禁用的 beta 版本。
	betaAPIGroupVersionsDisabledByDefault = []schema.GroupVersion{
		storageapiv1beta1.SchemeGroupVersion,
		networkingapiv1beta1.SchemeGroupVersion,
		resourcev1beta1.SchemeGroupVersion,
		resourcev1beta2.SchemeGroupVersion,
		schedulingapiv1beta1.SchemeGroupVersion,
	}

	// genericAlphaAPIGroupVersionsDisabledByDefault 是通用 API 组中默认禁用的 alpha 版本。
	genericAlphaAPIGroupVersionsDisabledByDefault = []schema.GroupVersion{
		admissionregistrationv1alpha1.SchemeGroupVersion,
		apiserverinternalv1alpha1.SchemeGroupVersion,
		authenticationv1alpha1.SchemeGroupVersion,
		apiserverinternalv1alpha1.SchemeGroupVersion,
		coordinationv1alpha2.SchemeGroupVersion,
		certificatesv1alpha1.SchemeGroupVersion,
	}
	// alphaAPIGroupVersionsDisabledByDefault 是 kube-apiserver 额外 API 组中默认禁用的 alpha 版本。
	alphaAPIGroupVersionsDisabledByDefault = []schema.GroupVersion{
		lifecyclev1alpha1.SchemeGroupVersion,
		resourcev1alpha3.SchemeGroupVersion,
		schedulingapiv1alpha3.SchemeGroupVersion,
		storageapiv1alpha1.SchemeGroupVersion,
	}
)

// DefaultGenericAPIResourceConfigSource 返回通用存储提供者所提供资源的默认配置。
func DefaultGenericAPIResourceConfigSource() *serverstorage.ResourceConfig {
	ret := serverstorage.NewResourceConfig()
	// 此处列出的版本默认启用，勿将 alpha/beta 放入；显式禁用 alpha/beta 以完整覆盖所有可服务版本。
	ret.EnableVersions(genericStableAPIGroupVersionsEnabledByDefault...)
	ret.DisableVersions(genericBetaAPIGroupVersionsDisabledByDefault...)
	ret.DisableVersions(genericAlphaAPIGroupVersionsDisabledByDefault...)
	return ret
}

// DefaultAPIResourceConfigSource 返回 kube-apiserver 所提供资源的默认配置。
func DefaultAPIResourceConfigSource() *serverstorage.ResourceConfig {
	// 以通用配置为基础。此处列出的版本默认启用，勿将 alpha/beta 放入；显式禁用 alpha/beta 以完整覆盖。
	ret := DefaultGenericAPIResourceConfigSource()
	ret.EnableVersions(stableAPIGroupVersionsEnabledByDefault...)
	ret.DisableVersions(betaAPIGroupVersionsDisabledByDefault...)
	ret.DisableVersions(alphaAPIGroupVersionsDisabledByDefault...)
	return ret
}
