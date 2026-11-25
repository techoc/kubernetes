/*
Copyright 2016 The Kubernetes Authors.

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

package rest

import (
	"crypto/tls"
	goerrors "errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/client-go/kubernetes"
	admissionregistrationv1client "k8s.io/client-go/kubernetes/typed/admissionregistration/v1"
	networkingv1client "k8s.io/client-go/kubernetes/typed/networking/v1"
	policyclient "k8s.io/client-go/kubernetes/typed/policy/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"k8s.io/kubernetes/pkg/features"
	kubeletclient "k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/registry/core/componentstatus"
	endpointsstore "k8s.io/kubernetes/pkg/registry/core/endpoint/storage"
	limitrangestore "k8s.io/kubernetes/pkg/registry/core/limitrange/storage"
	nodestore "k8s.io/kubernetes/pkg/registry/core/node/storage"
	pvstore "k8s.io/kubernetes/pkg/registry/core/persistentvolume/storage"
	pvcstore "k8s.io/kubernetes/pkg/registry/core/persistentvolumeclaim/storage"
	podstore "k8s.io/kubernetes/pkg/registry/core/pod/storage"
	podtemplatestore "k8s.io/kubernetes/pkg/registry/core/podtemplate/storage"
	"k8s.io/kubernetes/pkg/registry/core/rangeallocation"
	controllerstore "k8s.io/kubernetes/pkg/registry/core/replicationcontroller/storage"
	"k8s.io/kubernetes/pkg/registry/core/service/allocator"
	serviceallocator "k8s.io/kubernetes/pkg/registry/core/service/allocator/storage"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	serviceipallocatorcontroller "k8s.io/kubernetes/pkg/registry/core/service/ipallocator/controller"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
	portallocatorcontroller "k8s.io/kubernetes/pkg/registry/core/service/portallocator/controller"
	servicestore "k8s.io/kubernetes/pkg/registry/core/service/storage"
	serviceaccountstore "k8s.io/kubernetes/pkg/registry/core/serviceaccount/storage"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/util/async"
	netutils "k8s.io/utils/net"
)

// Config 提供构建 Core API RESTStorage 所需的配置信息。
// Config 汇总 legacy core API Group 的存储后端、Service 网络范围、事件 TTL 和启动后修复控制器所需依赖。
type Config struct {
	GenericConfig

	Proxy    ProxyConfig
	Services ServicesConfig

	EndpointSliceGetter proxy.EndpointSliceGetter
	Authorizer          authorizer.Authorizer
}

type ProxyConfig struct {
	Transport           http.RoundTripper
	KubeletClientConfig kubeletclient.KubeletClientConfig
}

type ServicesConfig struct {
	// Service ClusterIP 地址范围。
	ClusterIPRange          net.IPNet
	SecondaryClusterIPRange net.IPNet
	NodePortRange           utilnet.PortRange

	IPRepairInterval time.Duration
}

type rangeRegistries struct {
	clusterIP          rangeallocation.RangeRegistry
	secondaryClusterIP rangeallocation.RangeRegistry
	nodePort           rangeallocation.RangeRegistry
}

type legacyProvider struct {
	Config

	primaryServiceClusterIPAllocator ipallocator.Interface
	serviceClusterIPAllocators       map[api.IPFamily]ipallocator.Interface
	serviceNodePortAllocator         *portallocator.PortAllocator
	authorizer                       authorizer.UnconditionalAuthorizer

	startServiceNodePortsRepair, startServiceClusterIPRepair func(onFirstSuccess func(), stopCh chan struct{})
}

// New 初始化旧 Core API 的资源 Store 与 IP/NodePort 分配器，并把存储资源组装成 legacyProvider。
func New(c Config, authorizer authorizer.UnconditionalAuthorizer) (*legacyProvider, error) {
	rangeRegistries, serviceClusterIPAllocator, serviceIPAllocators, serviceNodePortAllocator, err := c.newServiceIPAllocators()
	if err != nil {
		return nil, err
	}

	p := &legacyProvider{
		Config: c,

		primaryServiceClusterIPAllocator: serviceClusterIPAllocator,
		serviceClusterIPAllocators:       serviceIPAllocators,
		serviceNodePortAllocator:         serviceNodePortAllocator,
		authorizer:                       authorizer,
	}

	// 创建 Service NodePort 分配修复控制器。
	client, err := kubernetes.NewForConfig(c.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	p.startServiceNodePortsRepair = portallocatorcontroller.NewRepair(c.Services.IPRepairInterval, client.CoreV1(), client.EventsV1(), c.Services.NodePortRange, rangeRegistries.nodePort).RunUntil

	// 创建 Service ClusterIP 分配修复控制器。
	if !utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
		p.startServiceClusterIPRepair = serviceipallocatorcontroller.NewRepair(
			c.Services.IPRepairInterval,
			client.CoreV1(),
			client.EventsV1(),
			&c.Services.ClusterIPRange,
			rangeRegistries.clusterIP,
			&c.Services.SecondaryClusterIPRange,
			rangeRegistries.secondaryClusterIP,
		).RunUntil
	} else {
		p.startServiceClusterIPRepair = serviceipallocatorcontroller.NewRepairIPAddress(
			c.Services.IPRepairInterval,
			client,
			c.Informers.Core().V1().Services(),
			c.Informers.Networking().V1().ServiceCIDRs(),
			c.Informers.Networking().V1().IPAddresses(),
		).RunUntil
	}

	return p, nil
}

// NewRESTStorage 按资源启用开关注册 Core v1 REST Handler；部分资源还注册 status 子资源，并把分配器/修复控制器注入 Service Storage。
func (p *legacyProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo, err := p.GenericConfig.NewRESTStorage(apiResourceConfigSource, restOptionsGetter)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	podDisruptionClient, err := policyclient.NewForConfig(p.LoopbackClientConfig)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	podTemplateStorage, err := podtemplatestore.NewREST(restOptionsGetter)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	limitRangeStorage, err := limitrangestore.NewREST(restOptionsGetter)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	persistentVolumeStorage, persistentVolumeStatusStorage, err := pvstore.NewREST(restOptionsGetter)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}
	persistentVolumeClaimStorage, persistentVolumeClaimStatusStorage, err := pvcstore.NewREST(restOptionsGetter)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	endpointsStorage, err := endpointsstore.NewREST(restOptionsGetter)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	nodeStorage, err := nodestore.NewStorage(restOptionsGetter, p.Proxy.KubeletClientConfig, p.Proxy.Transport)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	podStorage, err := podstore.NewStorage(
		restOptionsGetter,
		nodeStorage.KubeletConnectionInfo,
		p.Proxy.Transport,
		podDisruptionClient,
		p.authorizer,
	)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	serviceRESTStorage, serviceStatusStorage, serviceRESTProxy, err := servicestore.NewREST(
		restOptionsGetter,
		p.primaryServiceClusterIPAllocator.IPFamily(),
		p.serviceClusterIPAllocators,
		p.serviceNodePortAllocator,
		p.EndpointSliceGetter,
		podStorage.Pod,
		p.Proxy.Transport)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	storage := apiGroupInfo.VersionedResourcesStorageMap["v1"]
	if storage == nil {
		storage = map[string]rest.Storage{}
	}

	// 创建客户端，用于读取 ServiceAccount Token 所绑定的对象。
	whClient, err := admissionregistrationv1client.NewForConfig(p.LoopbackClientConfig)
	if err != nil {
		return genericapiserver.APIGroupInfo{}, err
	}

	// 如需支持 Pod 绑定信息，则使用专用实现替换通用 ServiceAccount Storage。
	var serviceAccountStorage *serviceaccountstore.REST
	if p.ServiceAccountIssuer != nil {
		var nodeGetter rest.Getter
		if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenNodeBinding) ||
			utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenPodNodeInfo) {
			nodeGetter = nodeStorage.Node.Store
		}
		serviceAccountStorage, err = serviceaccountstore.NewREST(restOptionsGetter, p.ServiceAccountIssuer, p.APIAudiences, p.Authorizer, p.ServiceAccountMaxExpiration, podStorage.Pod.Store, storage["secrets"].(rest.Getter), nodeGetter,
			whClient.ValidatingWebhookConfigurations(), whClient.MutatingWebhookConfigurations(), p.ExtendExpiration, p.MaxExtendedExpiration)
		if err != nil {
			return genericapiserver.APIGroupInfo{}, err
		}
	}

	if resource := "pods"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = podStorage.Pod
		storage[resource+"/attach"] = podStorage.Attach
		storage[resource+"/status"] = podStorage.Status
		storage[resource+"/log"] = podStorage.Log
		storage[resource+"/exec"] = podStorage.Exec
		storage[resource+"/portforward"] = podStorage.PortForward
		storage[resource+"/proxy"] = podStorage.Proxy
		storage[resource+"/binding"] = podStorage.Binding
		if podStorage.Eviction != nil {
			storage[resource+"/eviction"] = podStorage.Eviction
		}
		storage[resource+"/ephemeralcontainers"] = podStorage.EphemeralContainers
		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
			storage[resource+"/resize"] = podStorage.Resize
		}
	}
	if resource := "bindings"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = podStorage.LegacyBinding
	}

	if resource := "podtemplates"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = podTemplateStorage
	}

	if resource := "replicationcontrollers"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		controllerStorage, err := controllerstore.NewStorage(restOptionsGetter)
		if err != nil {
			return genericapiserver.APIGroupInfo{}, err
		}

		storage[resource] = controllerStorage.Controller
		storage[resource+"/status"] = controllerStorage.Status
		if legacyscheme.Scheme.IsVersionRegistered(schema.GroupVersion{Group: "autoscaling", Version: "v1"}) {
			storage[resource+"/scale"] = controllerStorage.Scale
		}
	}

	// 如需支持 Pod 绑定信息，则替换通用 ServiceAccount Storage。
	if resource := "serviceaccounts"; serviceAccountStorage != nil && apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		// 销毁被替换的 Storage，避免其后台 Goroutine 泄漏。
		storage[resource].Destroy()
		if storage[resource+"/token"] != nil {
			storage[resource+"/token"].Destroy()
		}

		storage[resource] = serviceAccountStorage
		if serviceAccountStorage.Token != nil {
			storage[resource+"/token"] = serviceAccountStorage.Token
		}
	}

	if resource := "services"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = serviceRESTStorage
		storage[resource+"/proxy"] = serviceRESTProxy
		storage[resource+"/status"] = serviceStatusStorage
	}

	if resource := "endpoints"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = endpointsStorage
	}

	if resource := "nodes"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = nodeStorage.Node
		storage[resource+"/proxy"] = nodeStorage.Proxy
		storage[resource+"/status"] = nodeStorage.Status
	}

	if resource := "limitranges"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = limitRangeStorage
	}

	if resource := "persistentvolumes"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = persistentVolumeStorage
		storage[resource+"/status"] = persistentVolumeStatusStorage
	}

	if resource := "persistentvolumeclaims"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = persistentVolumeClaimStorage
		storage[resource+"/status"] = persistentVolumeClaimStatusStorage
	}

	if resource := "componentstatuses"; apiResourceConfigSource.ResourceEnabled(corev1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = componentstatus.NewStorage(componentStatusStorage{p.StorageFactory}.serversToValidate)
	}

	if len(storage) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap["v1"] = storage
	}

	return apiGroupInfo, nil
}

// newServiceIPAllocators 根据 ServiceCIDR、双栈配置和 Feature Gate 创建 ClusterIP/NodePort 池，并准备旧位图与新 IPAddress 分配器的迁移组合。
func (c *Config) newServiceIPAllocators() (registries rangeRegistries, primaryClusterIPAllocator ipallocator.Interface, clusterIPAllocators map[api.IPFamily]ipallocator.Interface, nodePortAllocator *portallocator.PortAllocator, err error) {
	clusterIPAllocators = map[api.IPFamily]ipallocator.Interface{}

	serviceStorageConfig, err := c.StorageFactory.NewConfig(api.Resource("services"), &api.Service{})
	if err != nil {
		return rangeRegistries{}, nil, nil, nil, err
	}

	serviceClusterIPRange := c.Services.ClusterIPRange
	if serviceClusterIPRange.IP == nil {
		return rangeRegistries{}, nil, nil, nil, fmt.Errorf("service clusterIPRange is missing")
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
		primaryClusterIPAllocator, err = ipallocator.New(&serviceClusterIPRange, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
			var mem allocator.Snapshottable
			mem = allocator.NewAllocationMapWithOffset(max, rangeSpec, offset)
			// TODO：让 etcdallocator 包通过 storageFactory 返回统一的 Storage 接口。
			etcd, err := serviceallocator.NewEtcd(mem, "/ranges/serviceips", serviceStorageConfig.ForResource(api.Resource("serviceipallocations")))
			if err != nil {
				return nil, err
			}
			registries.clusterIP = etcd
			return etcd, nil
		})
		if err != nil {
			return rangeRegistries{}, nil, nil, nil, fmt.Errorf("cannot create cluster IP allocator: %v", err)
		}
	} else {
		networkingv1Client, err := networkingv1client.NewForConfig(c.LoopbackClientConfig)
		if err != nil {
			return rangeRegistries{}, nil, nil, nil, err
		}
		var bitmapAllocator ipallocator.Interface
		if !utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
			bitmapAllocator, err = ipallocator.New(&serviceClusterIPRange, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
				mem := allocator.NewAllocationMapWithOffset(max, rangeSpec, offset)
				etcd, err := serviceallocator.NewEtcd(mem, "/ranges/serviceips", serviceStorageConfig.ForResource(api.Resource("serviceipallocations")))
				if err != nil {
					return nil, err
				}
				// 新环境可能直接启用新分配器和双写模式启动 API Server。
				// 此时必须先初始化位图；否则不透明 API 对象的 ResourceVersion 为零，位图分配器将无法分配 IP 地址。
				rangeRegistry, err := etcd.Get()
				if err != nil {
					return nil, err
				}
				rangeRegistry.Range = serviceClusterIPRange.String()
				if len(rangeRegistry.ResourceVersion) == 0 {
					klog.Infof("kube-apiserver started with IP allocator and dual write enabled but bitmap allocator does not exist, recreating it ...")
					err := etcd.CreateOrUpdate(rangeRegistry)
					if err != nil {
						return nil, err
					}
				}
				registries.clusterIP = etcd
				return etcd, nil
			})
			if err != nil {
				return rangeRegistries{}, nil, nil, nil, fmt.Errorf("cannot create cluster IP allocator: %w", err)
			}

		}
		// TODO(aojea)：重新审视分配器的初始化方式。
		// 当前初始化依赖 service-cidr 启动参数，并据此设置默认 IPFamily；
		// 该默认值可能与集群中已有的默认 ServiceCIDR 不一致。
		primaryClusterIPAllocator, err = ipallocator.NewMetaAllocator(
			networkingv1Client,
			c.Informers.Networking().V1().ServiceCIDRs(),
			c.Informers.Networking().V1().IPAddresses(),
			netutils.IsIPv6CIDR(&serviceClusterIPRange),
			bitmapAllocator,
		)
		if err != nil {
			return rangeRegistries{}, nil, nil, nil, fmt.Errorf("cannot create cluster IP allocator: %v", err)
		}
	}
	primaryClusterIPAllocator.EnableMetrics()
	clusterIPAllocators[primaryClusterIPAllocator.IPFamily()] = primaryClusterIPAllocator

	var secondaryClusterIPAllocator ipallocator.Interface
	if c.Services.SecondaryClusterIPRange.IP != nil {
		if !utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
			var err error
			secondaryClusterIPAllocator, err = ipallocator.New(&c.Services.SecondaryClusterIPRange, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
				var mem allocator.Snapshottable
				mem = allocator.NewAllocationMapWithOffset(max, rangeSpec, offset)
				// TODO: 让 etcdallocator 包通过 storageFactory 返回 storage 接口
				etcd, err := serviceallocator.NewEtcd(mem, "/ranges/secondaryserviceips", serviceStorageConfig.ForResource(api.Resource("serviceipallocations")))
				if err != nil {
					return nil, err
				}
				registries.secondaryClusterIP = etcd
				return etcd, nil
			})
			if err != nil {
				return rangeRegistries{}, nil, nil, nil, fmt.Errorf("cannot create cluster secondary IP allocator: %v", err)
			}
		} else {
			networkingv1Client, err := networkingv1client.NewForConfig(c.LoopbackClientConfig)
			if err != nil {
				return rangeRegistries{}, nil, nil, nil, err
			}
			var bitmapAllocator ipallocator.Interface
			if !utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
				bitmapAllocator, err = ipallocator.New(&c.Services.SecondaryClusterIPRange, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
					mem := allocator.NewAllocationMapWithOffset(max, rangeSpec, offset)
					// TODO：让 etcdallocator 包通过 storageFactory 返回统一的 Storage 接口。
					etcd, err := serviceallocator.NewEtcd(mem, "/ranges/secondaryserviceips", serviceStorageConfig.ForResource(api.Resource("serviceipallocations")))
					if err != nil {
						return nil, err
					}
					// 新环境可能直接启用新分配器和双写模式启动 API Server。
					// 此时必须先初始化位图；否则不透明 API 对象的 ResourceVersion 为零，位图分配器将无法分配 IP 地址。
					rangeRegistry, err := etcd.Get()
					if err != nil {
						return nil, err
					}
					rangeRegistry.Range = c.Services.SecondaryClusterIPRange.String()
					if len(rangeRegistry.ResourceVersion) == 0 {
						klog.Infof("kube-apiserver started with IP allocator and dual write enabled but bitmap allocator does not exist, recreating it ...")
						err := etcd.CreateOrUpdate(rangeRegistry)
						if err != nil {
							return nil, err
						}
					}
					registries.secondaryClusterIP = etcd
					return etcd, nil
				})
				if err != nil {
					return rangeRegistries{}, nil, nil, nil, fmt.Errorf("cannot create cluster secondary IP allocator: %w", err)
				}
			}
			// TODO(aojea)：重新审视分配器的初始化方式。
			// 当前初始化依赖 service-cidr 启动参数，并据此设置默认 IPFamily；
			// 该默认值可能与集群中已有的默认 ServiceCIDR 不一致。
			secondaryClusterIPAllocator, err = ipallocator.NewMetaAllocator(
				networkingv1Client,
				c.Informers.Networking().V1().ServiceCIDRs(),
				c.Informers.Networking().V1().IPAddresses(),
				netutils.IsIPv6CIDR(&c.Services.SecondaryClusterIPRange),
				bitmapAllocator,
			)
			if err != nil {
				return rangeRegistries{}, nil, nil, nil, fmt.Errorf("cannot create cluster secondary IP allocator: %v", err)
			}
		}
		secondaryClusterIPAllocator.EnableMetrics()
		clusterIPAllocators[secondaryClusterIPAllocator.IPFamily()] = secondaryClusterIPAllocator
	}

	nodePortAllocator, err = portallocator.New(c.Services.NodePortRange, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
		mem := allocator.NewAllocationMapWithOffset(max, rangeSpec, offset)
		// TODO: 让 etcdallocator 包通过 storageFactory 返回 storage 接口
		etcd, err := serviceallocator.NewEtcd(mem, "/ranges/servicenodeports", serviceStorageConfig.ForResource(api.Resource("servicenodeportallocations")))
		if err != nil {
			return nil, err
		}
		registries.nodePort = etcd
		return etcd, nil
	})
	if err != nil {
		return rangeRegistries{}, nil, nil, nil, fmt.Errorf("cannot create cluster port allocator: %v", err)
	}
	nodePortAllocator.EnableMetrics()

	return
}

var _ genericapiserver.PostStartHookProvider = &legacyProvider{}

// PostStartHook 在 API Server 启动后运行 ClusterIP 与 NodePort Repair Controller；首次修复完成前阻止 Ready，超过 1 分钟则启动失败。
func (p *legacyProvider) PostStartHook() (string, genericapiserver.PostStartHookFunc, error) {
	return "start-service-ip-repair-controllers", func(context genericapiserver.PostStartHookContext) error {
		// 同时启动 ClusterIP 和 NodePort 两个修复循环，并通过 RunUntil 接口运行。
		// 在两个循环首次成功完成前，API Server 的存活/就绪检查必须保持失败，因此向 RunUntil 传入相应回调。
		// 为兼容旧行为，首次修复等待时间最多为 1 分钟；超时后整个 API Server 启动失败。
		wg := sync.WaitGroup{}
		wg.Add(2)
		runner := async.NewRunner(
			func(stopCh chan struct{}) { p.startServiceClusterIPRepair(wg.Done, stopCh) },
			func(stopCh chan struct{}) { p.startServiceNodePortsRepair(wg.Done, stopCh) },
		)
		runner.Start()
		go func() {
			defer runner.Stop()
			<-context.Done()
		}()

		// 为保持向后兼容，如果 ClusterIP 或 NodePort 始终无法完成修复，
		// 不仅要让存活/就绪检查失败，还要显式返回错误，使 API Server 启动失败。
		done := make(chan struct{})
		go func() {
			defer close(done)
			wg.Wait()
		}()
		select {
		case <-done:

		case <-context.Done():
			return goerrors.New("unable to perform initial IP and Port allocation check (context cancelled)")

		case <-time.After(time.Minute):
			return goerrors.New("unable to perform initial IP and Port allocation check (timeout)")
		}

		return nil
	}, nil
}

func (p *legacyProvider) GroupName() string {
	return api.GroupName
}

type componentStatusStorage struct {
	storageFactory serverstorage.StorageFactory
}

// serversToValidate 构建 ComponentStatus 的探测目标：本机 controller-manager、scheduler 以及每个 etcd 后端实例。
func (s componentStatusStorage) serversToValidate() map[string]componentstatus.Server {
	// 当前实现依赖组件使用默认端口，存在脆弱性。
	// TODO：待这些组件移除不安全服务能力后，改为探测安全端口。
	serversToValidate := map[string]componentstatus.Server{
		"controller-manager": &componentstatus.HttpServer{EnableHTTPS: true, TLSConfig: &tls.Config{InsecureSkipVerify: true}, Addr: "127.0.0.1", Port: ports.KubeControllerManagerPort, Path: "/healthz"},
		"scheduler":          &componentstatus.HttpServer{EnableHTTPS: true, TLSConfig: &tls.Config{InsecureSkipVerify: true}, Addr: "127.0.0.1", Port: kubeschedulerconfig.DefaultKubeSchedulerPort, Path: "/healthz"},
	}

	for ix, cfg := range s.storageFactory.Configs() {
		serversToValidate[fmt.Sprintf("etcd-%d", ix)] = &componentstatus.EtcdServer{Config: cfg}
	}
	return serversToValidate
}
