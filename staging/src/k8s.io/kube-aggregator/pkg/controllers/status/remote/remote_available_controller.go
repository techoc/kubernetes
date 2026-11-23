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

package remote

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/proxy"
	v1informers "k8s.io/client-go/informers/core/v1"
	discoveryv1informers "k8s.io/client-go/informers/discovery/v1"
	v1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/transport"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationv1apihelper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	apiregistrationclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
	informers "k8s.io/kube-aggregator/pkg/client/informers/externalversions/apiregistration/v1"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers"
	availabilitymetrics "k8s.io/kube-aggregator/pkg/controllers/status/metrics"
)

// certKeyFunc 延迟读取代理客户端证书/私钥，而不是在构造控制器时固化。
// 证书会轮换，每次同步时取最新内容，才能保证聚合层用当前有效的证书访问聚合服务。
type certKeyFunc func() ([]byte, []byte)

// ServiceResolver knows how to convert a service reference into an actual location.
// 与 apiserver 包中的同名接口相互独立：这里的用途是把 Service 解析成健康检查的目标地址，
// 只依赖接口便于在测试中注入假解析器，无需真实的 Service/EndpointSlice。
type ServiceResolver interface {
	ResolveEndpoint(namespace, name string, port int32) (*url.URL, error)
}

// AvailableConditionController handles checking the availability of registered API services.
// 该控制器只负责远端 APIService 的健康检查：先看 Service/EndpointSlice 是否可用，
// 再实际请求发现端点，最后把结果写回 APIService 的 Available condition。
// 本地 APIService 由另一个控制器处理，因此在 sync 开头就跳过。
type AvailableConditionController struct {
	apiServiceClient apiregistrationclient.APIServicesGetter

	apiServiceLister listers.APIServiceLister
	apiServiceSynced cache.InformerSynced

	// serviceLister is used to get the IP to create the transport for
	serviceLister  v1listers.ServiceLister
	servicesSynced cache.InformerSynced

	endpointSliceGetter  proxy.EndpointSliceGetter
	endpointSlicesSynced cache.InformerSynced

	// proxyTransportDial specifies the dial function for creating unencrypted TCP connections.
	proxyTransportDial         *transport.DialHolder
	proxyCurrentCertKeyContent certKeyFunc
	serviceResolver            ServiceResolver

	// To allow injection for testing.
	// 把同步逻辑抽成函数字段，测试可直接替换，无需构造完整的 informer。
	syncFn func(key string) error

	// 限速队列：合并同一 APIService 的重复事件，失败时快速重试并逐步退避到 30 秒。
	queue workqueue.TypedRateLimitingInterface[string]
	// map from service-namespace -> service-name -> apiservice names
	// 反向索引：Service/EndpointSlice 事件到达时，通过它快速找到受影响的 APIService 入队，
	// 而不必每次都遍历全部 APIService。
	cache map[string]map[string][]string
	// this lock protects operations on the above cache
	cacheLock sync.RWMutex

	// metrics registered into legacy registry
	metrics *availabilitymetrics.Metrics
}

// New returns a new remote APIService AvailableConditionController.
// 同时监听 APIService、Service、EndpointSlice 三类资源：前者决定“检查谁”，
// 后两者决定“什么时候需要重新检查”；构造时把三者的 HasSynced 都纳入等待，
// 保证队列在真正开始前已积累完整事件。
func New(
	apiServiceInformer informers.APIServiceInformer,
	serviceInformer v1informers.ServiceInformer,
	endpointSliceInformer discoveryv1informers.EndpointSliceInformer,
	apiServiceClient apiregistrationclient.APIServicesGetter,
	proxyTransportDial *transport.DialHolder,
	proxyCurrentCertKeyContent certKeyFunc,
	serviceResolver ServiceResolver,
	metrics *availabilitymetrics.Metrics,
) (*AvailableConditionController, error) {

	endpointSliceGetter, err := proxy.NewEndpointSliceIndexerGetter(endpointSliceInformer)
	if err != nil {
		return nil, err
	}

	c := &AvailableConditionController{
		apiServiceClient:    apiServiceClient,
		apiServiceLister:    apiServiceInformer.Lister(),
		serviceLister:       serviceInformer.Lister(),
		endpointSliceGetter: endpointSliceGetter,
		serviceResolver:     serviceResolver,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			// We want a fairly tight requeue time.  The controller listens to the API, but because it relies on the routability of the
			// service network, it is possible for an external, non-watchable factor to affect availability.  This keeps
			// the maximum disruption time to a minimum, but it does prevent hot loops.
			// 初始重试间隔很小（5ms）是为了尽快恢复短暂抖动；上限 30 秒防止持续失败时刷屏。
			// 网络可达性不是资源事件能反映的，必须靠快速重试来兜底。
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](5*time.Millisecond, 30*time.Second),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "RemoteAvailabilityController"},
		),
		proxyTransportDial:         proxyTransportDial,
		proxyCurrentCertKeyContent: proxyCurrentCertKeyContent,
		metrics:                    metrics,
	}

	// resync on this one because it is low cardinality and rechecking the actual discovery
	// allows us to detect health in a more timely fashion when network connectivity to
	// nodes is snipped, but the network still attempts to route there.  See
	// https://github.com/openshift/origin/issues/17159#issuecomment-341798063
	// APIService 数量少，30 秒全量 resync 代价可控；网络被切断但路由仍存在时，
	// 没有资源事件可依赖，只能靠周期性主动探测及时发现不健康状态。
	apiServiceHandler, _ := apiServiceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.addAPIService,
			UpdateFunc: c.updateAPIService,
			DeleteFunc: c.deleteAPIService,
		},
		30*time.Second)
	c.apiServiceSynced = apiServiceHandler.HasSynced

	serviceHandler, _ := serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addService,
		UpdateFunc: c.updateService,
		DeleteFunc: c.deleteService,
	})
	c.servicesSynced = serviceHandler.HasSynced

	endpointSliceHandler, _ := endpointSliceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addEndpointSlice,
		UpdateFunc: c.updateEndpointSlice,
		DeleteFunc: c.deleteEndpointSlice,
	})
	c.endpointSlicesSynced = endpointSliceHandler.HasSynced

	c.syncFn = c.sync

	return c, nil
}

// BuildTransportConfig builds a transport.Config for an APIService.
// It ignores TLS verification if InsecureSkipTLSVerify is true.
// ServerName 用 Service 的 DNS 名而非 IP，保证 TLS SNI 与证书中的名称一致；
// 客户端证书/CA 来自参数或 APIService 的 CABundle，配合可选的自定义 dial 函数
// 支持在特殊网络环境下建立连接。
func BuildTransportConfig(
	proxyTransportDial *transport.DialHolder,
	proxyClientCert, proxyClientKey []byte,
	apiService *apiregistrationv1.APIService,
) *transport.Config {
	return &transport.Config{
		TLS: transport.TLSConfig{
			Insecure:   apiService.Spec.InsecureSkipTLSVerify,
			ServerName: apiService.Spec.Service.Name + "." + apiService.Spec.Service.Namespace + ".svc",
			CertData:   proxyClientCert,
			KeyData:    proxyClientKey,
			CAData:     apiService.Spec.CABundle,
		},
		DialHolder: proxyTransportDial,
	}
}

// sync 执行一次完整的健康检查：先做本地可判定的结构性检查（Service、端口、Endpoint），
// 再实际访问发现端点，最后把结果写回 status。
// 对 originalAPIService 做 DeepCopy 再修改，是因为 lister 返回的是共享缓存对象，
// 直接改会污染 informer 缓存并造成数据竞争。
func (c *AvailableConditionController) sync(key string) error {
	originalAPIService, err := c.apiServiceLister.Get(key)
	if apierrors.IsNotFound(err) {
		c.metrics.ForgetAPIService(key)
		return nil
	}
	if err != nil {
		return err
	}

	if originalAPIService.Spec.Service == nil {
		// handled by the local APIService controller
		// 本地 APIService 不需要走 Service 网络，其可用性由另一个控制器维护。
		return nil
	}

	apiService := originalAPIService.DeepCopy()

	availableCondition := apiregistrationv1.APIServiceCondition{
		Type:               apiregistrationv1.Available,
		Status:             apiregistrationv1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}

	service, err := c.serviceLister.Services(apiService.Spec.Service.Namespace).Get(apiService.Spec.Service.Name)
	if apierrors.IsNotFound(err) {
		availableCondition.Status = apiregistrationv1.ConditionFalse
		availableCondition.Reason = "ServiceNotFound"
		availableCondition.Message = fmt.Sprintf("service/%s in %q is not present", apiService.Spec.Service.Name, apiService.Spec.Service.Namespace)
		apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
		_, err := c.updateAPIServiceStatus(originalAPIService, apiService)
		return err
	} else if err != nil {
		availableCondition.Status = apiregistrationv1.ConditionUnknown
		availableCondition.Reason = "ServiceAccessError"
		availableCondition.Message = fmt.Sprintf("service/%s in %q cannot be checked due to: %v", apiService.Spec.Service.Name, apiService.Spec.Service.Namespace, err)
		apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
		_, err := c.updateAPIServiceStatus(originalAPIService, apiService)
		return err
	}

	if service.Spec.Type == v1.ServiceTypeClusterIP {
		// if we have a cluster IP service, it must be listening on configured port and we can check that
		// ClusterIP 类型可以直接判断端口与后端 EndpointSlice 是否就绪，
		// 这些结构性检查比实际发请求便宜且能给出更具体的失败原因。

		servicePort := apiService.Spec.Service.Port
		portName := ""
		foundPort := false
		for _, port := range service.Spec.Ports {
			if port.Port == *servicePort {
				foundPort = true
				portName = port.Name
				break
			}
		}
		if !foundPort {
			availableCondition.Status = apiregistrationv1.ConditionFalse
			availableCondition.Reason = "ServicePortError"
			availableCondition.Message = fmt.Sprintf("service/%s in %q is not listening on port %d", apiService.Spec.Service.Name, apiService.Spec.Service.Namespace, *apiService.Spec.Service.Port)
			apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
			_, err := c.updateAPIServiceStatus(originalAPIService, apiService)
			return err
		}

		endpointSlices, err := c.endpointSliceGetter.GetEndpointSlices(apiService.Spec.Service.Namespace, apiService.Spec.Service.Name)
		if err != nil {
			availableCondition.Status = apiregistrationv1.ConditionUnknown
			availableCondition.Reason = "EndpointsAccessError"
			availableCondition.Message = fmt.Sprintf("service/%s in %q cannot be checked due to: %v", apiService.Spec.Service.Name, apiService.Spec.Service.Namespace, err)
			apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
			_, err := c.updateAPIServiceStatus(originalAPIService, apiService)
			return err
		} else if len(endpointSlices) == 0 {
			availableCondition.Status = apiregistrationv1.ConditionFalse
			availableCondition.Reason = "EndpointsNotFound"
			availableCondition.Message = fmt.Sprintf("cannot find endpointslices for service/%s in %q", apiService.Spec.Service.Name, apiService.Spec.Service.Namespace)
			apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
			_, err := c.updateAPIServiceStatus(originalAPIService, apiService)
			return err
		}
		hasActiveEndpoints := hasAvailableEndpoint(portName, endpointSlices...)
		if !hasActiveEndpoints {
			availableCondition.Status = apiregistrationv1.ConditionFalse
			availableCondition.Reason = "MissingEndpoints"
			availableCondition.Message = fmt.Sprintf("endpointslices for service/%s in %q have no addresses with port name %q", apiService.Spec.Service.Name, apiService.Spec.Service.Namespace, portName)
			apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
			_, err := c.updateAPIServiceStatus(originalAPIService, apiService)
			return err
		}
	}
	// actually try to hit the discovery endpoint when it isn't local and when we're routing as a service.
	// 结构性检查通过后，再真正发请求验证发现端点可达：前面的检查只能证明“配置上有后端”，
	// 无法证明网络实际通畅，因此这两层缺一不可。
	if apiService.Spec.Service != nil && c.serviceResolver != nil {
		// if a particular transport was specified, use that otherwise build one
		var proxyClientCert, proxyClientKey []byte
		if c.proxyCurrentCertKeyContent != nil {
			proxyClientCert, proxyClientKey = c.proxyCurrentCertKeyContent()
		}

		transportConfig := BuildTransportConfig(c.proxyTransportDial, proxyClientCert, proxyClientKey, apiService)
		restTransport, err := transport.New(transportConfig)
		if err != nil {
			return err
		}
		discoveryClient := &http.Client{
			Transport: restTransport,
			// the request should happen quickly.
			// 单次健康检查必须有超时，否则一个无响应的后端会拖住 worker；
			// 下面还额外用 select 做了 6 秒兜底，覆盖 DNS/拨号卡住的情况。
			Timeout: 5 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}

		// 并发发 5 次探测：Service 可能有多个后端，单个后端抖动不应影响整体健康判定。
		// 带缓冲的结果通道保证每个 goroutine 写完就能退出，即使主协程提前返回也不泄漏。
		attempts := 5
		results := make(chan error, attempts)
		for range attempts {
			go func() {
				discoveryURL, err := c.serviceResolver.ResolveEndpoint(apiService.Spec.Service.Namespace, apiService.Spec.Service.Name, *apiService.Spec.Service.Port)
				if err != nil {
					results <- err
					return
				}
				// render legacyAPIService health check path when it is delegated to a service
				// v1. 是核心 API（无 group）的占位 APIService，路径用 /api/<version>；
				// 其他聚合服务走 /apis/<group>/<version>。
				if apiService.Name == "v1." {
					discoveryURL.Path = "/api/" + apiService.Spec.Version
				} else {
					discoveryURL.Path = "/apis/" + apiService.Spec.Group + "/" + apiService.Spec.Version
				}

				errCh := make(chan error, 1)
				go func() {
					// be sure to check a URL that the aggregated API server is required to serve
					newReq, err := http.NewRequest("GET", discoveryURL.String(), nil)
					if err != nil {
						errCh <- err
						return
					}

					// setting the system-masters identity ensures that we will always have access rights
					// 以 system:masters 身份探测，避免聚合服务因鉴权拒绝而返回非 2xx，
					// 被误判为不可用；这里检查的是可达性，不是权限模型。
					transport.SetAuthProxyHeaders(newReq, "system:kube-aggregator", "", []string{"system:masters"}, nil)
					resp, err := discoveryClient.Do(newReq)
					if resp != nil {
						resp.Body.Close()
						// we should always been in the 200s or 300s
						if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
							errCh <- fmt.Errorf("bad status from %v: %d", discoveryURL, resp.StatusCode)
							return
						}
					}

					errCh <- err
				}()

				select {
				case err = <-errCh:
					if err != nil {
						utilnet.CloseIdleConnectionsFor(restTransport)
						results <- fmt.Errorf("failing or missing response from %v: %w", discoveryURL, err)
						return
					}

					// we had trouble with slow dial and DNS responses causing us to wait too long.
					// we added this as insurance
					// http.Client.Timeout 对某些 DNS/拨号卡住的情况覆盖不到，
					// 这里用外层 select 做 6 秒硬超时兜底，并主动关闭空闲连接。
				case <-time.After(6 * time.Second):
					utilnet.CloseIdleConnectionsFor(restTransport)
					results <- fmt.Errorf("timed out waiting for %v", discoveryURL)
					return
				}

				results <- nil
			}()
		}

		var lastError error
		for range attempts {
			lastError = <-results
			// if we had at least one success, we are successful overall and we can return now
			// 只要有一次成功就认为服务整体可用；循环读取剩余结果是为了让所有 goroutine
			// 都能写入已带缓冲的通道并顺利退出，而不是卡在发送上。
			if lastError == nil {
				break
			}
		}

		if lastError != nil {
			availableCondition.Status = apiregistrationv1.ConditionFalse
			availableCondition.Reason = "FailedDiscoveryCheck"
			availableCondition.Message = lastError.Error()
			apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
			_, updateErr := c.updateAPIServiceStatus(originalAPIService, apiService)
			if updateErr != nil {
				return updateErr
			}
			// force a requeue to make it very obvious that this will be retried at some point in the future
			// along with other requeues done via service change, endpoint change, and resync
			return lastError
		}
	}

	availableCondition.Reason = "Passed"
	availableCondition.Message = "all checks passed"
	apiregistrationv1apihelper.SetAPIServiceCondition(apiService, availableCondition)
	_, err = c.updateAPIServiceStatus(originalAPIService, apiService)
	return err
}

// hasAvailableEndpoint 判断是否存在这样一个 EndpointSlice：其中至少有一个 ready 的端点，
// 且暴露了与目标端口名匹配的命名端口。
// Ready 为 nil 按 ready 处理，兼容尚未填充该字段的旧 EndpointSlice。
func hasAvailableEndpoint(portName string, es ...*discoveryv1.EndpointSlice) bool {
	return slices.ContainsFunc(es, func(s *discoveryv1.EndpointSlice) bool {
		if !slices.ContainsFunc(s.Endpoints, func(e discoveryv1.Endpoint) bool {
			return e.Conditions.Ready == nil || *e.Conditions.Ready
		}) {
			return false
		}

		return slices.ContainsFunc(s.Ports, func(p discoveryv1.EndpointPort) bool {
			return p.Name != nil && *p.Name == portName && p.Port != nil
		})
	})
}

// updateAPIServiceStatus only issues an update if a change is detected.  We have a tight resync loop to quickly detect dead
// apiservices. Doing that means we don't want to quickly issue no-op updates.
// 每次同步都会刷新不可用 gauge（反映最新观测），但只有 status 真正变化时才发 UpdateStatus，
// 避免在 30 秒 resync 下对 apiserver 产生大量无效写入。
func (c *AvailableConditionController) updateAPIServiceStatus(originalAPIService, newAPIService *apiregistrationv1.APIService) (*apiregistrationv1.APIService, error) {
	// update this metric on every sync operation to reflect the actual state
	c.metrics.SetUnavailableGauge(newAPIService)

	if equality.Semantic.DeepEqual(originalAPIService.Status, newAPIService.Status) {
		// 状态没变就不写 apiserver：resync 很频繁，无脑更新会制造大量无意义的写请求，
		// 还会不断刷新 resourceVersion 触发更多 watch 事件。
		return newAPIService, nil
	}

	orig := apiregistrationv1apihelper.GetAPIServiceConditionByType(originalAPIService, apiregistrationv1.Available)
	now := apiregistrationv1apihelper.GetAPIServiceConditionByType(newAPIService, apiregistrationv1.Available)
	unknown := apiregistrationv1.APIServiceCondition{
		Type:   apiregistrationv1.Available,
		Status: apiregistrationv1.ConditionUnknown,
	}
	if orig == nil {
		orig = &unknown
	}
	if now == nil {
		now = &unknown
	}
	if *orig != *now {
		klog.V(2).InfoS("changing APIService availability", "name", newAPIService.Name, "oldStatus", orig.Status, "newStatus", now.Status, "message", now.Message, "reason", now.Reason)
	}

	newAPIService, err := c.apiServiceClient.APIServices().UpdateStatus(context.TODO(), newAPIService, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	// 只有成功写入后才累加不可用次数计数器，
	// 避免把失败的重试也算成一次真实的状态翻转。
	c.metrics.SetUnavailableCounter(originalAPIService, newAPIService)
	return newAPIService, nil
}

// Run starts the AvailableConditionController loop which manages the availability condition of API services.
// workers 大于 1 时用多 worker 并发处理不同 key；同一个 key 由 workqueue 保证串行处理。
func (c *AvailableConditionController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting RemoteAvailability controller")
	defer klog.Info("Shutting down RemoteAvailability controller")

	// This waits not just for the informers to sync, but for our handlers
	// to be called; since the handlers are three different ways of
	// enqueueing the same thing, waiting for this permits the queue to
	// maximally de-duplicate the entries.
	// 等 handler 都被调用过，意味着初始事件已全部入队；此时再启动 worker，
	// workqueue 能把这些重复 key 合并，避免启动时对同一 APIService 做多次相同检查。
	if !controllers.WaitForCacheSync("RemoteAvailability", stopCh, c.apiServiceSynced, c.servicesSynced, c.endpointSlicesSynced) {
		return
	}

	for range workers {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *AvailableConditionController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
// 成功就 Forget 清掉失败计数；失败则重新入队按退避重试，
// 保证网络抖动导致的偶发失败能被自动恢复。
func (c *AvailableConditionController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncFn(key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	c.queue.AddRateLimited(key)

	return true
}

// addAPIService 处理 APIService 新增事件。
// Service 引用变化会影响反向索引，因此只有带 Service 的对象才重建缓存；
// 无论哪种情况都要把对象入队做一次检查。
func (c *AvailableConditionController) addAPIService(obj interface{}) {
	castObj := obj.(*apiregistrationv1.APIService)
	klog.V(4).Infof("Adding %s", castObj.Name)
	if castObj.Spec.Service != nil {
		c.rebuildAPIServiceCache()
	}
	c.queue.Add(castObj.Name)
}

// updateAPIService 处理 APIService 更新事件。
// 只有 Spec.Service 变化才需要重建反向索引；其他字段（如 status）变化
// 不该影响索引，避免无谓的全量重建。
func (c *AvailableConditionController) updateAPIService(oldObj, newObj any) {
	castObj := newObj.(*apiregistrationv1.APIService)
	oldCastObj := oldObj.(*apiregistrationv1.APIService)
	klog.V(4).Infof("Updating %s", oldCastObj.Name)
	if !reflect.DeepEqual(castObj.Spec.Service, oldCastObj.Spec.Service) {
		c.rebuildAPIServiceCache()
	}
	c.queue.Add(oldCastObj.Name)
}

// deleteAPIService 处理删除事件，并从 tombstone 中兼容恢复对象。
// 删除后需要重建反向索引，因为被删的 Service 引用不应再指向任何 APIService。
func (c *AvailableConditionController) deleteAPIService(obj interface{}) {
	castObj, ok := obj.(*apiregistrationv1.APIService)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*apiregistrationv1.APIService)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	klog.V(4).Infof("Deleting %q", castObj.Name)
	if castObj.Spec.Service != nil {
		c.rebuildAPIServiceCache()
	}
	c.queue.Add(castObj.Name)
}

// getAPIServicesFor 从反向索引取出引用了某个 Service 的所有 APIService 名字。
// 返回的切片直接来自 map，调用方不应修改它；这里只是读取并按名字入队。
func (c *AvailableConditionController) getAPIServicesFor(serviceNamespace, serviceName string) []string {
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()
	return c.cache[serviceNamespace][serviceName]
}

// if the service/endpointslice handler wins the race against the cache rebuilding, it may queue a no-longer-relevant apiservice
// (which will get processed an extra time - this doesn't matter),
// and miss a newly relevant apiservice (which will get queued by the apiservice handler)
// 从 APIService 列表整体重建“Service -> APIService 列表”的反向索引。
// 不做增量更新而全量重建，是因为 APIService 数量少，整体重建更简单；
// 竞态下漏掉的新引用会由 APIService 自己的事件补上，多入队的旧引用只是多检查一次，无碍正确性。
func (c *AvailableConditionController) rebuildAPIServiceCache() {
	apiServiceList, _ := c.apiServiceLister.List(labels.Everything())
	newCache := map[string]map[string][]string{}
	for _, apiService := range apiServiceList {
		if apiService.Spec.Service == nil {
			continue
		}
		if newCache[apiService.Spec.Service.Namespace] == nil {
			newCache[apiService.Spec.Service.Namespace] = map[string][]string{}
		}
		newCache[apiService.Spec.Service.Namespace][apiService.Spec.Service.Name] = append(newCache[apiService.Spec.Service.Namespace][apiService.Spec.Service.Name], apiService.Name)
	}

	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	c.cache = newCache
}

// TODO, think of a way to avoid checking on every service manipulation
// 下面三个 Service handler 不在回调里直接检查，而是把引用它的 APIService 入队，
// 由 worker 统一执行；这样多个事件能合并，检查逻辑也只有一份。

func (c *AvailableConditionController) addService(obj interface{}) {
	service := obj.(*v1.Service)
	for _, apiService := range c.getAPIServicesFor(service.Namespace, service.Name) {
		c.queue.Add(apiService)
	}
}

func (c *AvailableConditionController) updateService(obj, _ interface{}) {
	service := obj.(*v1.Service)
	for _, apiService := range c.getAPIServicesFor(service.Namespace, service.Name) {
		c.queue.Add(apiService)
	}
}

func (c *AvailableConditionController) deleteService(obj interface{}) {
	castObj, ok := obj.(*v1.Service)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*v1.Service)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	for _, apiService := range c.getAPIServicesFor(castObj.Namespace, castObj.Name) {
		c.queue.Add(apiService)
	}
}

// addEndpointSlice 处理 EndpointSlice 新增事件。
// EndpointSlice 通过 label kubernetes.io/service-name 关联到 Service，
// 若无该 label 说明它不属于任何 Service，直接忽略；否则把依赖该 Service 的 APIService 入队。
func (c *AvailableConditionController) addEndpointSlice(obj interface{}) {
	slice := obj.(*discoveryv1.EndpointSlice)
	serviceName := slice.Labels[discoveryv1.LabelServiceName]
	if serviceName == "" {
		return
	}
	for _, apiService := range c.getAPIServicesFor(slice.Namespace, serviceName) {
		c.queue.Add(apiService)
	}
}

func (c *AvailableConditionController) updateEndpointSlice(obj, _ interface{}) {
	slice := obj.(*discoveryv1.EndpointSlice)
	serviceName := slice.Labels[discoveryv1.LabelServiceName]
	if serviceName == "" {
		return
	}
	for _, apiService := range c.getAPIServicesFor(slice.Namespace, serviceName) {
		c.queue.Add(apiService)
	}
}

func (c *AvailableConditionController) deleteEndpointSlice(obj interface{}) {
	castObj, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*discoveryv1.EndpointSlice)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	serviceName := castObj.Labels[discoveryv1.LabelServiceName]
	if serviceName == "" {
		return
	}
	for _, apiService := range c.getAPIServicesFor(castObj.Namespace, serviceName) {
		c.queue.Add(apiService)
	}
}
