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

package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	genericfeatures "k8s.io/apiserver/pkg/features"
	peerreconcilers "k8s.io/apiserver/pkg/reconcilers"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/egressselector"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilpeerproxy "k8s.io/apiserver/pkg/util/peerproxy"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/transport"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/tracing"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	v1helper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	informers "k8s.io/kube-aggregator/pkg/client/informers/externalversions"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	openapicontroller "k8s.io/kube-aggregator/pkg/controllers/openapi"
	openapiaggregator "k8s.io/kube-aggregator/pkg/controllers/openapi/aggregator"
	openapiv3controller "k8s.io/kube-aggregator/pkg/controllers/openapiv3"
	openapiv3aggregator "k8s.io/kube-aggregator/pkg/controllers/openapiv3/aggregator"
	localavailability "k8s.io/kube-aggregator/pkg/controllers/status/local"
	availabilitymetrics "k8s.io/kube-aggregator/pkg/controllers/status/metrics"
	remoteavailability "k8s.io/kube-aggregator/pkg/controllers/status/remote"
	apiservicerest "k8s.io/kube-aggregator/pkg/registry/apiservice/rest"
	openapicommon "k8s.io/kube-openapi/pkg/common"
)

// making sure we only register metrics once into legacy registry
// 中文补充：一个进程里可以创建多个 APIAggregator（比如测试里），
// 但 legacy registry 的指标只能注册一次，否则会 panic；用 Once 兜住这个约束。
var registerIntoLegacyRegistryOnce sync.Once

func init() {
	// we need to add the options (like ListOptions) to empty v1
	metav1.AddToGroupVersion(aggregatorscheme.Scheme, schema.GroupVersion{Group: "", Version: "v1"})

	// 中文补充：发现端点返回的是 Status/APIGroupList 等核心元类型，
	// 它们属于无组的 v1，必须显式注册才能在协商编码时被识别。
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	aggregatorscheme.Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

const (
	// legacyAPIServiceName is the fixed name of the only non-groupified API version
	legacyAPIServiceName = "v1."
	// StorageVersionPostStartHookName is the name of the storage version updater post start hook.
	StorageVersionPostStartHookName = "built-in-resources-storage-version-updater"
)

// ExtraConfig represents APIServices-specific configuration
//
// 中文补充：这些是聚合层独有的配置，跟 generic apiserver 的通用配置分开，
// 便于把 kube-aggregator 作为库嵌入到其他 control plane 时按需替换。
type ExtraConfig struct {
	// PeerAdvertiseAddress is the IP for this kube-apiserver which is used by peer apiservers to route a request
	// to this apiserver. This happens in cases where the peer is not able to serve the request due to
	// version skew. If unset, AdvertiseAddress/BindAddress will be used.
	PeerAdvertiseAddress peerreconcilers.PeerAdvertiseAddress

	// ProxyClientCert/Key are the client cert used to identify this proxy. Backing APIServices use
	// this to confirm the proxy's identity
	ProxyClientCertFile string
	ProxyClientKeyFile  string

	// If present, the Dial method will be used for dialing out to delegate
	// apiservers.
	ProxyTransport *http.Transport

	// Mechanism by which the Aggregator will resolve services. Required.
	ServiceResolver ServiceResolver

	RejectForwardingRedirects bool

	// DisableRemoteAvailableConditionController disables the controller that updates the Available conditions for
	// remote APIServices via querying endpoints of the referenced services. In generic controlplane use-cases,
	// the concept of services and endpoints might differ, and might require another implementation of this
	// controller. Local APIService are reconciled nevertheless.
	DisableRemoteAvailableConditionController bool

	// PeerProxy, if not nil, sets proxy transport between kube-apiserver peers for requests
	// that can not be served locally
	PeerProxy utilpeerproxy.Interface
}

// Config represents the configuration needed to create an APIAggregator.
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig same as Config, just to swap private object.
//
// 中文补充：把私有类型藏在导出结构里，外部包只能拿到实例、无法自行构造，
// 从而保证 CompletedConfig 一定经过 Complete 的校验和补全。
type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

type runnable interface {
	RunWithContext(ctx context.Context) error
}

// preparedGenericAPIServer is a private wrapper that enforces a call of PrepareRun() before Run can be invoked.
//
// 中文补充：用类型系统把"先 PrepareRun 再 Run"的顺序约束编码进 API，
// 因为直接暴露 Run 的话调用方很容易漏掉 OpenAPI/handler 的准备步骤。
type preparedAPIAggregator struct {
	*APIAggregator
	runnable runnable
}

// APIAggregator contains state for a Kubernetes cluster master/api server.
type APIAggregator struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	// provided for easier embedding
	APIRegistrationInformers informers.SharedInformerFactory

	delegateHandler http.Handler

	// proxyCurrentCertKeyContent holds he client cert used to identify this proxy. Backing APIServices use this to confirm the proxy's identity
	proxyCurrentCertKeyContent certKeyFunc
	proxyTransportDial         *transport.DialHolder

	// proxyHandlers are the proxy handlers that are currently registered, keyed by apiservice.name
	proxyHandlers map[string]*proxyHandler
	// handledGroupVersions contain the groups that already have routes. The key is the name of the group and the value
	// is the versions for the group.
	handledGroupVersions map[string]sets.Set[string]

	// lister is used to add group handling for /apis/<group> aggregator lookups based on
	// controller state
	// 中文补充：发现请求直接读 lister 缓存，不查 API，避免 /apis 的延迟
	// 受 APIService 存储可用性影响，也防止发现流量放大到后端。
	lister listers.APIServiceLister

	// Information needed to determine routing for the aggregator
	serviceResolver ServiceResolver

	// Enable swagger and/or OpenAPI if these configs are non-nil.
	openAPIConfig *openapicommon.Config

	// Enable OpenAPI V3 if these configs are non-nil
	openAPIV3Config *openapicommon.OpenAPIV3Config

	// openAPIAggregationController downloads and merges OpenAPI v2 specs.
	openAPIAggregationController *openapicontroller.AggregationController

	// openAPIV3AggregationController downloads and caches OpenAPI v3 specs.
	openAPIV3AggregationController *openapiv3controller.AggregationController

	// discoveryAggregationController downloads and caches discovery documents
	// from all aggregated apiservices so they are available from /apis endpoint
	// when discovery with resources are requested
	discoveryAggregationController DiscoveryAggregationController

	// rejectForwardingRedirects is whether to allow to forward redirect response
	rejectForwardingRedirects bool

	// tracerProvider is used to wrap the proxy transport and handler with tracing
	tracerProvider tracing.TracerProvider
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	// the kube aggregator wires its own discovery mechanism
	// TODO eventually collapse this by extracting all of the discovery out
	// 中文补充：generic server 自带的发现只会列它自己注册的资源，
	// 而聚合层要按 APIService 动态拼装发现文档，两者会互相覆盖，所以必须关掉前者。
	c.GenericConfig.EnableDiscovery = false

	return CompletedConfig{&c}
}

// NewWithDelegate returns a new instance of APIAggregator from the given config.
func (c completedConfig) NewWithDelegate(delegationTarget genericapiserver.DelegationTarget) (*APIAggregator, error) {
	genericServer, err := c.GenericConfig.New("kube-aggregator", delegationTarget)
	if err != nil {
		return nil, err
	}

	apiregistrationClient, err := clientset.NewForConfig(c.GenericConfig.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	informerFactory := informers.NewSharedInformerFactory(
		apiregistrationClient,
		5*time.Minute, // this is effectively used as a refresh interval right now.  Might want to do something nicer later on.
		// 中文补充：这里的值被当作 resync 周期使用；APIService 变动很少，
		// 且注册控制器自己关心事件，5 分钟只作为兜底重放。
	)

	// apiServiceRegistrationControllerInitiated is closed when APIServiceRegistrationController has finished "installing" all known APIServices.
	// At this point we know that the proxy handler knows about APIServices and can handle client requests.
	// Before it might have resulted in a 404 response which could have serious consequences for some controllers like  GC and NS
	//
	// Note that the APIServiceRegistrationController waits for APIServiceInformer to synced before doing its work.
	// 中文补充：这个 channel 同时是"路由已就绪"的信号，
	// 注册到 mux completion 机制后，健康检查和发现请求会等它关闭再放行。
	// 否则启动瞬间一个 /apis/foo 请求可能撞上尚未注册的路由，得到 404，
	// 对 GC、namespace 控制器这类把 404 当"对象消失"的调用方后果很严重。
	apiServiceRegistrationControllerInitiated := make(chan struct{})
	if err := genericServer.RegisterMuxAndDiscoveryCompleteSignal("APIServiceRegistrationControllerInitiated", apiServiceRegistrationControllerInitiated); err != nil {
		return nil, err
	}

	// 中文补充：拨号器有两个来源，优先级是 egress selector > 显式传入的 ProxyTransport。
	// 前者是集群级的出网管控，后者是嵌入场景下的定制入口；
	// 都为空时走 transport 默认拨号，不需要 DialHolder。
	var proxyTransportDial *transport.DialHolder
	if c.GenericConfig.EgressSelector != nil {
		egressDialer, err := c.GenericConfig.EgressSelector.Lookup(egressselector.Cluster.AsNetworkContext())
		if err != nil {
			return nil, err
		}
		if egressDialer != nil {
			proxyTransportDial = &transport.DialHolder{Dial: egressDialer}
		}
	} else if c.ExtraConfig.ProxyTransport != nil && c.ExtraConfig.ProxyTransport.DialContext != nil {
		proxyTransportDial = &transport.DialHolder{Dial: c.ExtraConfig.ProxyTransport.DialContext}
	}

	s := &APIAggregator{
		GenericAPIServer:           genericServer,
		delegateHandler:            delegationTarget.UnprotectedHandler(),
		proxyTransportDial:         proxyTransportDial,
		proxyHandlers:              map[string]*proxyHandler{},
		handledGroupVersions:       map[string]sets.Set[string]{},
		lister:                     informerFactory.Apiregistration().V1().APIServices().Lister(),
		APIRegistrationInformers:   informerFactory,
		serviceResolver:            c.ExtraConfig.ServiceResolver,
		openAPIConfig:              c.GenericConfig.OpenAPIConfig,
		openAPIV3Config:            c.GenericConfig.OpenAPIV3Config,
		proxyCurrentCertKeyContent: func() (bytes []byte, bytes2 []byte) { return nil, nil },
		rejectForwardingRedirects:  c.ExtraConfig.RejectForwardingRedirects,
		tracerProvider:             c.GenericConfig.TracerProvider,
	}

	apiGroupInfo := apiservicerest.NewRESTStorage(c.GenericConfig.MergedResourceConfig, c.GenericConfig.RESTOptionsGetter, false)
	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	// 中文补充：从实际装进来的 storage 反推启用的版本，而不是读配置常量，
	// 这样发现文档只包含真正可服务的版本；配置关掉了某版本就不会对外宣称支持。
	enabledVersions := sets.NewString()
	for v := range apiGroupInfo.VersionedResourcesStorageMap {
		enabledVersions.Insert(v)
	}
	if !enabledVersions.Has(v1.SchemeGroupVersion.Version) {
		return nil, fmt.Errorf("API group/version %s must be enabled", v1.SchemeGroupVersion.String())
	}

	codecs := aggregatorscheme.Codecs
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		// 中文补充：CBOR 是可选的线格式，开关打开时单独造一个 codec factory，
		// 不改全局的 aggregatorscheme.Codecs，避免影响其他未适配 CBOR 的路径。
		codecs = serializer.NewCodecFactory(aggregatorscheme.Scheme, serializer.WithSerializer(cbor.NewSerializerInfo))
	}

	apisHandler := &apisHandler{
		codecs:         codecs,
		lister:         s.lister,
		discoveryGroup: discoveryGroup(enabledVersions),
	}

	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.UnknownVersionInteroperabilityProxy) && c.ExtraConfig.PeerProxy != nil {
		s.GenericAPIServer.PeerAggregatedDiscoveryManager = aggregated.NewPeerAggregatedDiscoveryHandler(s.GenericAPIServer.APIServerID, s.GenericAPIServer.AggregatedDiscoveryGroupManager, c.ExtraConfig.PeerProxy, "apis")

		// Register cache invalidation callback to the peer proxy handler
		if s.GenericAPIServer.PeerAggregatedDiscoveryManager != nil {
			c.ExtraConfig.PeerProxy.RegisterCacheInvalidationCallback(s.GenericAPIServer.PeerAggregatedDiscoveryManager.InvalidateCache)
		}
	}

	apisHandlerWithAggregationSupport := aggregated.WrapAggregatedDiscoveryToHandler(apisHandler, s.GenericAPIServer.AggregatedDiscoveryGroupManager, s.GenericAPIServer.PeerAggregatedDiscoveryManager)
	// 中文补充：/apis 精确匹配走带聚合（含资源）的发现处理器，
	// /apis/ 前缀和其他方法走原始处理器；两者分开注册避免相互覆盖。
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/apis", apisHandlerWithAggregationSupport)
	s.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandle("/apis/", apisHandler)

	apiserviceRegistrationController := NewAPIServiceRegistrationController(informerFactory.Apiregistration().V1().APIServices(), s)
	if len(c.ExtraConfig.ProxyClientCertFile) > 0 && len(c.ExtraConfig.ProxyClientKeyFile) > 0 {
		aggregatorProxyCerts, err := dynamiccertificates.NewDynamicServingContentFromFiles("aggregator-proxy-cert", c.ExtraConfig.ProxyClientCertFile, c.ExtraConfig.ProxyClientKeyFile)
		if err != nil {
			return nil, err
		}
		// We are passing the context to ProxyCerts.RunOnce as it needs to implement RunOnce(ctx) however the
		// context is not used at all. So passing a empty context shouldn't be a problem
		if err := aggregatorProxyCerts.RunOnce(context.Background()); err != nil {
			return nil, err
		}
		// 中文补充：后端 APIService 靠这份客户端证书识别代理身份，
		// 证书轮换时必须通知注册控制器刷新 proxy handler，否则会继续用旧证书。
		aggregatorProxyCerts.AddListener(apiserviceRegistrationController)
		s.proxyCurrentCertKeyContent = aggregatorProxyCerts.CurrentCertKeyContent

		s.GenericAPIServer.AddPostStartHookOrDie("aggregator-reload-proxy-client-cert", func(postStartHookContext genericapiserver.PostStartHookContext) error {
			go aggregatorProxyCerts.Run(postStartHookContext, 1)
			return nil
		})
	}

	s.GenericAPIServer.AddPostStartHookOrDie("start-kube-aggregator-informers", func(context genericapiserver.PostStartHookContext) error {
		informerFactory.Start(context.Done())
		c.GenericConfig.SharedInformerFactory.Start(context.Done())
		return nil
	})

	// create shared (remote and local) availability metrics
	// TODO: decouple from legacyregistry
	// 中文补充：local 和 remote 两个控制器共用同一组指标，避免同名指标被注册两次；
	// Once 还顺带处理了一个进程里创建多个 aggregator 的情况。
	metrics := availabilitymetrics.New()
	registerIntoLegacyRegistryOnce.Do(func() { err = metrics.Register(legacyregistry.Register, legacyregistry.CustomRegister) })
	if err != nil {
		return nil, err
	}

	// always run local availability controller
	// 中文补充：local 的判活不依赖 Service/EndpointSlice，任何部署形态都能跑，
	// 所以即使 remote 控制器被禁用，它也必须启动。
	local, err := localavailability.New(
		informerFactory.Apiregistration().V1().APIServices(),
		apiregistrationClient.ApiregistrationV1(),
		metrics,
	)
	if err != nil {
		return nil, err
	}
	s.GenericAPIServer.AddPostStartHookOrDie("apiservice-status-local-available-controller", func(context genericapiserver.PostStartHookContext) error {
		// if we end up blocking for long periods of time, we may need to increase workers.
		go local.Run(5, context.Done())
		return nil
	})

	// conditionally run remote availability controller. This could be replaced in certain
	// generic controlplane use-cases where there is another concept of services and/or endpoints.
	if !c.ExtraConfig.DisableRemoteAvailableConditionController {
		remote, err := remoteavailability.New(
			informerFactory.Apiregistration().V1().APIServices(),
			c.GenericConfig.SharedInformerFactory.Core().V1().Services(),
			c.GenericConfig.SharedInformerFactory.Discovery().V1().EndpointSlices(),
			apiregistrationClient.ApiregistrationV1(),
			proxyTransportDial,
			(func() ([]byte, []byte))(s.proxyCurrentCertKeyContent),
			s.serviceResolver,
			metrics,
		)
		if err != nil {
			return nil, err
		}
		s.GenericAPIServer.AddPostStartHookOrDie("apiservice-status-remote-available-controller", func(context genericapiserver.PostStartHookContext) error {
			// if we end up blocking for long periods of time, we may need to increase workers.
			go remote.Run(5, context.Done())
			return nil
		})
	}

	s.GenericAPIServer.AddPostStartHookOrDie("apiservice-registration-controller", func(context genericapiserver.PostStartHookContext) error {
		go apiserviceRegistrationController.Run(context.Done(), apiServiceRegistrationControllerInitiated)
		// 中文补充：post start hook 故意阻塞到路由注册完成，
		// 这样 /healthz 和 mux  ready 信号才会等到 APIService 全部就位后才放行。
		select {
		case <-context.Done():
		case <-apiServiceRegistrationControllerInitiated:
		}

		return nil
	})

	s.discoveryAggregationController = NewDiscoveryManager(
		// Use aggregator as the source name to avoid overwriting native/CRD
		// groups
		// 中文补充：发现文档按来源分区合并，用独立的 AggregatorSource 才能
		// 保证聚合层写入的组不会覆盖 native/CRD 自己上报的同名组。
		s.GenericAPIServer.AggregatedDiscoveryGroupManager.WithSource(aggregated.AggregatorSource),
	)

	// Setup discovery endpoint
	s.GenericAPIServer.AddPostStartHookOrDie("apiservice-discovery-controller", func(context genericapiserver.PostStartHookContext) error {
		// Discovery aggregation depends on the apiservice registration controller
		// having the full list of APIServices already synced
		select {
		case <-context.Done():
			return nil
		// Context cancelled, should abort/clean goroutines
		case <-apiServiceRegistrationControllerInitiated:
		}

		// Run discovery manager's worker to watch for new/removed/updated
		// APIServices to the discovery document can be updated at runtime
		// When discovery is ready, all APIServices will be present, with APIServices
		// that have not successfully synced discovery to be present but marked as Stale.
		discoverySyncedCh := make(chan struct{})
		go s.discoveryAggregationController.Run(context.Done(), discoverySyncedCh)

		select {
		case <-context.Done():
			return nil
		// Context cancelled, should abort/clean goroutines
		case <-discoverySyncedCh:
			// API services successfully sync
		}
		return nil
	})

	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.StorageVersionAPI) &&
		utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerIdentity) {
		// Spawn a goroutine in aggregator apiserver to update storage version for
		// all built-in resources
		s.GenericAPIServer.AddPostStartHookOrDie(StorageVersionPostStartHookName, func(hookContext genericapiserver.PostStartHookContext) error {
			// Wait for apiserver-identity to exist first before updating storage
			// versions, to avoid storage version GC accidentally garbage-collecting
			// storage versions.
			// 中文补充：StorageVersion GC 用 Lease 判断实例是否存活，
			// 自己的 Lease 还没建好就去写 StorageVersion，容易被当成孤儿清掉。
			kubeClient, err := kubernetes.NewForConfig(hookContext.LoopbackClientConfig)
			if err != nil {
				return err
			}
			if err := wait.PollImmediateUntil(100*time.Millisecond, func() (bool, error) {
				_, err := kubeClient.CoordinationV1().Leases(metav1.NamespaceSystem).Get(
					context.TODO(), s.GenericAPIServer.APIServerID, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				return true, nil
			}, hookContext.Done()); err != nil {
				return fmt.Errorf("failed to wait for apiserver-identity lease %s to be created: %v",
					s.GenericAPIServer.APIServerID, err)
			}
			// Technically an apiserver only needs to update storage version once during bootstrap.
			// Reconcile StorageVersion objects every 10 minutes will help in the case that the
			// StorageVersion objects get accidentally modified/deleted by a different agent. In that
			// case, the reconciliation ensures future storage migration still works. If nothing gets
			// changed, the reconciliation update is a noop and gets short-circuited by the apiserver,
			// therefore won't change the resource version and trigger storage migration.
			// 中文补充：这里刻意做成"反复写但内容相同"的调和循环，
			// 依赖 apiserver 对 no-op 更新短路（不改 resourceVersion）来保证安全；
			// 真被误删时又能自愈，不需要额外的修复机制。
			go wait.PollImmediateUntil(10*time.Minute, func() (bool, error) {
				// All apiservers (aggregator-apiserver, kube-apiserver, apiextensions-apiserver)
				// share the same generic apiserver config. The same StorageVersion manager is used
				// to register all built-in resources when the generic apiservers install APIs.
				s.GenericAPIServer.StorageVersionManager.UpdateStorageVersions(hookContext.LoopbackClientConfig, s.GenericAPIServer.APIServerID)
				return false, nil
			}, hookContext.Done())
			// Once the storage version updater finishes the first round of update,
			// the PostStartHook will return to unblock /healthz. The handler chain
			// won't block write requests anymore. Check every second since it's not
			// expensive.
			wait.PollImmediateUntil(1*time.Second, func() (bool, error) {
				return s.GenericAPIServer.StorageVersionManager.Completed(), nil
			}, hookContext.Done())
			return nil
		})
	}

	return s, nil
}

// PrepareRun prepares the aggregator to run, by setting up the OpenAPI spec &
// aggregated discovery document and calling the generic PrepareRun.
//
// 中文补充：PrepareRun 与 NewWithDelegate 分两段，是因为 OpenAPI 聚合器
// 依赖 delegate 先注册完自己的 handler；构造期拿不到这些信息，
// 只能等 PrepareRun 时再搭建，同时仍需赶在 /healthz 安装前挂好 post start hook。
func (s *APIAggregator) PrepareRun() (preparedAPIAggregator, error) {
	// add post start hook before generic PrepareRun in order to be before /healthz installation
	// 中文补充：/healthz 是在 generic PrepareRun 里装的，hook 必须排在它前面，
	// 否则 OpenAPI 聚合 controller 启动前健康检查就已经通过了。
	if s.openAPIConfig != nil {
		s.GenericAPIServer.AddPostStartHookOrDie("apiservice-openapi-controller", func(context genericapiserver.PostStartHookContext) error {
			go s.openAPIAggregationController.Run(context.Done())
			return nil
		})
	}

	if s.openAPIV3Config != nil {
		s.GenericAPIServer.AddPostStartHookOrDie("apiservice-openapiv3-controller", func(context genericapiserver.PostStartHookContext) error {
			go s.openAPIV3AggregationController.Run(context.Done())
			return nil
		})
	}

	prepared := s.GenericAPIServer.PrepareRun()

	// delay OpenAPI setup until the delegate had a chance to setup their OpenAPI handlers
	// 中文补充：BuildAndRegisterAggregator 需要读取 GoRestfulContainer 里已注册的
	// WebServices，delegate 若还没准备好，得到的规范会缺内容，所以顺序不能提前。
	if s.openAPIConfig != nil {
		specDownloader := openapiaggregator.NewDownloader()
		openAPIAggregator, err := openapiaggregator.BuildAndRegisterAggregator(
			&specDownloader,
			s.GenericAPIServer.NextDelegate(),
			s.GenericAPIServer.Handler.GoRestfulContainer.RegisteredWebServices(),
			s.openAPIConfig,
			s.GenericAPIServer.Handler.NonGoRestfulMux)
		if err != nil {
			return preparedAPIAggregator{}, err
		}
		s.openAPIAggregationController = openapicontroller.NewAggregationController(&specDownloader, openAPIAggregator)
	}

	if s.openAPIV3Config != nil {
		specDownloaderV3 := openapiv3aggregator.NewDownloader()
		openAPIV3Aggregator, err := openapiv3aggregator.BuildAndRegisterAggregator(
			specDownloaderV3,
			s.GenericAPIServer.NextDelegate(),
			s.GenericAPIServer.Handler.GoRestfulContainer,
			s.openAPIV3Config,
			s.GenericAPIServer.Handler.NonGoRestfulMux)
		if err != nil {
			return preparedAPIAggregator{}, err
		}
		s.openAPIV3AggregationController = openapiv3controller.NewAggregationController(openAPIV3Aggregator)
	}

	return preparedAPIAggregator{APIAggregator: s, runnable: prepared}, nil
}

func (s preparedAPIAggregator) Run(ctx context.Context) error {
	// 中文补充：只能拿到 preparedAPIAggregator 的实例，也就保证了 PrepareRun
	// 一定先于 Run 被调用；这里只是把调用转发给 generic server。
	return s.runnable.RunWithContext(ctx)
}

// AddAPIService adds an API service.  It is not thread-safe, so only call it on one thread at a time please.
// It's a slow moving API, so its ok to run the controller on a single thread
//
// 中文补充：整个函数就是一次"注册路由"：APIService 新增时挂上 proxy handler，
// 更新时刷新已有 handler。APIService 变动是很慢的事件，单线程跑控制器即可，
// 这样就不用给 proxyHandlers 和 handledGroupVersions 加锁。
func (s *APIAggregator) AddAPIService(apiService *v1.APIService) error {
	// if the proxyHandler already exists, it needs to be updated. The aggregation bits do not
	// since they are wired against listers because they require multiple resources to respond
	// 中文补充：OpenAPI/发现聚合走 lister 读全量对象，不依赖单个 handler 的引用，
	// 所以只在首次注册时把它们接进来；更新靠 lister 里的新对象自然生效。
	if proxyHandler, exists := s.proxyHandlers[apiService.Name]; exists {
		proxyHandler.updateAPIService(apiService)
		if s.openAPIAggregationController != nil {
			s.openAPIAggregationController.UpdateAPIService(proxyHandler, apiService)
		}
		if s.openAPIV3AggregationController != nil {
			s.openAPIV3AggregationController.UpdateAPIService(proxyHandler, apiService)
		}
		// Forward calls to discovery manager to update discovery document
		if s.discoveryAggregationController != nil {
			// 中文补充：发现文档只在 APIService 可用时才宣称支持其版本，
			// 但真正用来代理请求的 handler 不应被改动，所以用副本标记可用状态。
			handlerCopy := *proxyHandler
			handlerCopy.setServiceAvailable()
			s.discoveryAggregationController.AddAPIService(apiService, &handlerCopy)
		}
		return nil
	}

	proxyPath := "/apis/" + apiService.Spec.Group + "/" + apiService.Spec.Version
	// v1. is a special case for the legacy API.  It proxies to a wider set of endpoints.
	if apiService.Name == legacyAPIServiceName {
		proxyPath = "/api"
	}

	// register the proxy handler
	proxyHandler := &proxyHandler{
		localDelegate:              s.delegateHandler,
		proxyCurrentCertKeyContent: s.proxyCurrentCertKeyContent,
		proxyTransportDial:         s.proxyTransportDial,
		serviceResolver:            s.serviceResolver,
		rejectForwardingRedirects:  s.rejectForwardingRedirects,
		tracerProvider:             s.tracerProvider,
	}
	proxyHandler.updateAPIService(apiService)
	if s.openAPIAggregationController != nil {
		s.openAPIAggregationController.AddAPIService(proxyHandler, apiService)
	}
	if s.openAPIV3AggregationController != nil {
		s.openAPIV3AggregationController.AddAPIService(proxyHandler, apiService)
	}
	if s.discoveryAggregationController != nil {
		s.discoveryAggregationController.AddAPIService(apiService, proxyHandler)
	}

	s.proxyHandlers[apiService.Name] = proxyHandler
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle(proxyPath, proxyHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandlePrefix(proxyPath+"/", proxyHandler)

	// if we're dealing with the legacy group, we're done here
	if apiService.Name == legacyAPIServiceName {
		return nil
	}

	// if we've already registered the path with the handler, we don't want to do it again.
	// 中文补充：组级发现端点一个组只需注册一次；handledGroupVersions 顺带记录
	// 该组下已出现过的版本，删除最后一个版本时才知道何时该把这个端点摘掉。
	versions, exist := s.handledGroupVersions[apiService.Spec.Group]
	if exist {
		versions.Insert(apiService.Spec.Version)
		return nil
	}

	codecs := aggregatorscheme.Codecs
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		// 中文补充：组级发现处理器也要和 /apis 处理器用同一套 codec，
		// 否则客户端在 CBOR 模式下访问组端点会拿到不可解析的响应。
		codecs = serializer.NewCodecFactory(aggregatorscheme.Scheme, serializer.WithSerializer(cbor.NewSerializerInfo))
	}

	// it's time to register the group aggregation endpoint
	groupPath := "/apis/" + apiService.Spec.Group
	groupDiscoveryHandler := &apiGroupHandler{
		codecs:    codecs,
		groupName: apiService.Spec.Group,
		lister:    s.lister,
		delegate:  s.delegateHandler,
	}
	// aggregation is protected
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle(groupPath, groupDiscoveryHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandle(groupPath+"/", groupDiscoveryHandler)
	s.handledGroupVersions[apiService.Spec.Group] = sets.New[string](apiService.Spec.Version)
	return nil
}

// RemoveAPIService removes the APIService from being handled.  It is not thread-safe, so only call it on one thread at a time please.
// It's a slow moving API, so it's ok to run the controller on a single thread.
//
// 中文补充：这是 AddAPIService 的逆过程，但多了一步组级端点的引用计数：
// 同组还有其他版本在跑时，不能因为删掉一个版本就撤销组发现路径。
func (s *APIAggregator) RemoveAPIService(apiServiceName string) {
	// Forward calls to discovery manager to update discovery document
	if s.discoveryAggregationController != nil {
		s.discoveryAggregationController.RemoveAPIService(apiServiceName)
	}

	version := v1helper.APIServiceNameToGroupVersion(apiServiceName)

	proxyPath := "/apis/" + version.Group + "/" + version.Version
	// v1. is a special case for the legacy API.  It proxies to a wider set of endpoints.
	if apiServiceName == legacyAPIServiceName {
		proxyPath = "/api"
	}
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(proxyPath)
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(proxyPath + "/")
	if s.openAPIAggregationController != nil {
		s.openAPIAggregationController.RemoveAPIService(apiServiceName)
	}
	if s.openAPIV3AggregationController != nil {
		s.openAPIV3AggregationController.RemoveAPIService(apiServiceName)
	}
	delete(s.proxyHandlers, apiServiceName)

	// 中文补充：同组其他版本还在，组级发现路径就必须保留；
	// 只有最后一个版本删掉后才真正 Unregister，避免误伤其他版本。
	versions, exist := s.handledGroupVersions[version.Group]
	if !exist {
		return
	}
	versions.Delete(version.Version)
	if versions.Len() > 0 {
		return
	}
	delete(s.handledGroupVersions, version.Group)
	groupPath := "/apis/" + version.Group
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(groupPath)
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(groupPath + "/")
}

// DefaultAPIResourceConfigSource returns default configuration for an APIResource.
func DefaultAPIResourceConfigSource() *serverstorage.ResourceConfig {
	ret := serverstorage.NewResourceConfig()
	// NOTE: GroupVersions listed here will be enabled by default. Don't put alpha versions in the list.
	// 中文补充：alpha 版本默认不开，是为了避免升级后 API 行为随默认值漂移；
	// 想要 alpha 的人显式配置启用，风险自负。
	ret.EnableVersions(
		v1.SchemeGroupVersion,
		v1beta1.SchemeGroupVersion,
	)

	return ret
}
