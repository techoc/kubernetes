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

package autoregister

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/klog/v2"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
	informers "k8s.io/kube-aggregator/pkg/client/informers/externalversions/apiregistration/v1"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers"
)

// 自治注册通过标签而不是内存状态来标记归属：APIService 重建后控制器重启也能
// 从对象本身认出哪些是自己创建的，不会把别的组件管理的服务对象误删。
const (
	// AutoRegisterManagedLabel 标注在 APIService 上，表明它希望以何种方式被同步。
	AutoRegisterManagedLabel = "kube-aggregator.kubernetes.io/automanaged"

	// manageOnStart 表示 APIService 只需在控制器启动时同步一次。
	manageOnStart = "onstart"
	// manageContinuously 表示 APIService 需要持续同步。
	manageContinuously = "true"
)

// AutoAPIServiceRegistration 是一个接口，调用方可以在本地重新声明它并安全地断言转换，
// 用于添加和删除 APIService。
// 让调用方在本地重声明这个接口，是为了让各组件只依赖自己需要的注册方法，
// 不必引入整个控制器的实现类型；编译期仍由 Go 的鸭子类型保证兼容。
type AutoAPIServiceRegistration interface {
	// AddAPIServiceToSyncOnStart 添加一个启动时同步一次的 APIService。
	AddAPIServiceToSyncOnStart(in *v1.APIService)
	// AddAPIServiceToSync 添加一个持续同步的 APIService。
	AddAPIServiceToSync(in *v1.APIService)
	// RemoveAPIServiceToSync 移除一个待自动注册的 APIService。
	RemoveAPIServiceToSync(name string)
}

// autoRegisterController 保证一组特定的 APIService 始终存在于 API 中，
// 适用于自动注册 TPR 类 API 或 kube-apiserver 核心组的情况。
type autoRegisterController struct {
	apiServiceLister listers.APIServiceLister
	apiServiceSynced cache.InformerSynced
	apiServiceClient apiregistrationclient.APIServicesGetter

	// 期望状态由注册方写入（Add/Remove），协调循环只读取；分离锁避免协调时
	// 阻塞注册方，也让快照语义清晰：取出的对象已经被 DeepCopy，调用方改不到这里。
	apiServicesToSyncLock sync.RWMutex
	apiServicesToSync     map[string]*v1.APIService

	syncHandler func(apiServiceName string) error

	// 记录已同步过的 Service。
	syncedSuccessfullyLock *sync.RWMutex
	syncedSuccessfully     map[string]bool

	// 记住启动时就已存在的 service 名称。
	// onstart 语义只对启动时就存在的对象生效：后来才出现的同名对象可能是别的组件创建的，
	// 不能因为打了个历史标签就把人家的对象接管或删掉。
	apiServicesAtStart map[string]bool

	// 工作先入队去重，出错时也便于做限速重试。
	queue workqueue.TypedRateLimitingInterface[string]
}

// NewAutoRegisterController 创建一个新的 autoRegisterController。
func NewAutoRegisterController(apiServiceInformer informers.APIServiceInformer, apiServiceClient apiregistrationclient.APIServicesGetter) *autoRegisterController {
	c := &autoRegisterController{
		apiServiceLister:  apiServiceInformer.Lister(),
		apiServiceSynced:  apiServiceInformer.Informer().HasSynced,
		apiServiceClient:  apiServiceClient,
		apiServicesToSync: map[string]*v1.APIService{},

		apiServicesAtStart: map[string]bool{},

		syncedSuccessfullyLock: &sync.RWMutex{},
		syncedSuccessfully:     map[string]bool{},

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "autoregister"},
		),
	}
	c.syncHandler = c.checkAPIService

	// 所有变更都只入队名字：informer 事件可能重复且并发到达，工作队列负责去重，
	// 具体状态在协调时再从 lister 读取，避免拿着事件里的旧对象做判断。
	apiServiceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cast := obj.(*v1.APIService)
			c.queue.Add(cast.Name)
		},
		UpdateFunc: func(_, obj interface{}) {
			cast := obj.(*v1.APIService)
			c.queue.Add(cast.Name)
		},
		DeleteFunc: func(obj interface{}) {
			cast, ok := obj.(*v1.APIService)
			// 删除事件可能以 tombstone 形式送达，因为 informer 无法保证
			// 缓存中还有原对象；不解析出来就会漏掉一次本应触发的协调。
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.V(2).Infof("Couldn't get object from tombstone %#v", obj)
					return
				}
				cast, ok = tombstone.Obj.(*v1.APIService)
				if !ok {
					klog.V(2).Infof("Tombstone contained unexpected object: %#v", obj)
					return
				}
			}
			c.queue.Add(cast.Name)
		},
	})

	return c
}

// Run 启动 autoregister 控制器的循环，持续同步 APIService 直到 stopCh 关闭。
func (c *autoRegisterController) Run(workers int, stopCh <-chan struct{}) {
	// 防止 panic 导致进程退出。
	defer utilruntime.HandleCrash()
	// 确保关闭 workqueue，从而让 worker 退出。
	defer c.queue.ShutDown()

	klog.Info("Starting autoregister controller")
	defer klog.Info("Shutting down autoregister controller")

	// 在开始工作前先等待二级缓存填充完成。
	// 缓存未同步就开始协调会看到空列表，进而把本应保留的对象当作多余对象删除；
	// 宁可启动慢一点，也不能在启动瞬间产生破坏性写入。
	if !controllers.WaitForCacheSync("autoregister", stopCh, c.apiServiceSynced) {
		return
	}

	// 记录启动时就已存在的 APIService 对象。
	// 只在这里采样一次：后续新增的对象不享有 onstart 语义，
	// 否则每次重启都会把运行中新建的对象误认为“启动即存在”。
	if services, err := c.apiServiceLister.List(labels.Everything()); err == nil {
		for _, service := range services {
			c.apiServicesAtStart[service.Name] = true
		}
	}

	// 按 workers 数量启动 worker；有些控制器有不止一类 worker。
	for i := 0; i < workers; i++ {
		// runWorker 循环运行直到出错，.Until 会在 1 秒后重启 worker。
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	// 等待停止信号。
	<-stopCh
}

func (c *autoRegisterController) runWorker() {
	// 持续循环直到收到停止信号；processNextWorkItem 会自行等待有工作可用，无需额外等待。
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的一个 key；需要退出时返回 false。
// 错误一律重新入队而不是就地重试：一次失败通常是 apiserver 暂时不可用，
// 带退避的重试既能自愈，也不会让一个卡住的对象饿死队列里的其他工作。
func (c *autoRegisterController) processNextWorkItem() bool {
	// 从队列取出下一项，它应是可用来查缓存的 key。
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// 处理完必须通知队列，避免该项一直被视为进行中。
	defer c.queue.Done(key)

	// 对 key 执行实际的对账逻辑。
	err := c.syncHandler(key)
	if err == nil {
		// 成功时让队列忘记该 key，重置按项限速的失败计数。
		c.queue.Forget(key)
		return true
	}

	// 失败时上报错误；这里可插入自定义错误处理，例如集群监控。
	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", key, err))
	// 失败后重新入队并加退避：既避免对暂不可用的项忙等，
	// 也防止控制器整体失控时饿死其他正常工作。
	c.queue.AddRateLimited(key)

	return true
}

// checkAPIService 把当前 APIService 与期望的 APIService 列表对账。
//
//	                                                | A. 期望：不存在       | B. 期望：启动时同步   | C. 期望：持续同步
//	------------------------------------------------|-----------------------|---------------------------|------------------------
//	1. 当前：查询出错                               | error                 | error                     | error
//	2. 当前：不存在                                 | -                     | 创建一次                  | 创建
//	3. 当前：不同步                                 | -                     | -                         | -
//	4. 当前：启动时同步，但启动时不在                | -                     | -                         | -
//	5. 当前：启动时同步，且启动时存在                | 删除一次              | 更新一次                  | 更新一次
//	6. 当前：持续同步                               | 删除                  | 更新一次                  | 更新
//
// 这张表是实现的权威说明：case 的顺序必须经过精心排列，前面的分支处理更具体的情形，
// 后面的分支才能安全地假设 curr 非空且对象归自己管理。改动时先改表再审代码。
func (c *autoRegisterController) checkAPIService(name string) (err error) {
	desired := c.GetAPIServiceToSync(name)
	curr, err := c.apiServiceLister.Get(name)

	// 若该 service 从未成功同步过，则记录一次成功同步。
	// onstart 要求“只做一次”，是否做过只在进程内记：重启后重新启动一次符合语义，
	// 但同一次运行里 informer 反复触发事件不会让对象被反复写回。
	hasSynced := c.hasSyncedSuccessfully(name)
	if !hasSynced {
		defer func() {
			if err == nil {
				c.setSyncedSuccessfully(name)
			}
		}()
	}

	switch {
	// 真实错误，直接返回（对应表格 1A/1B/1C）。
	case err != nil && !apierrors.IsNotFound(err):
		return err

	// 当前与期望都不存在，无需处理（2A）。
	case apierrors.IsNotFound(err) && desired == nil:
		return nil

	// 本地对象只要求启动时同步且已同步过，不再处理（2B、5B、6B 的 once 约束）。
	case isAutomanagedOnStart(desired) && hasSynced:
		return nil

	// 我们没有对应的 APIService，但期望有一个（2B、2C）。
	// AlreadyExists 表示另一个协调循环刚刚创建成功，informer 事件随后会再来，
	// 这里直接返回成功，避免把一个正常的并发结果当成错误重试。
	case apierrors.IsNotFound(err) && desired != nil:
		_, err := c.apiServiceClient.APIServices().Create(context.TODO(), desired, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			// 期间对象已被创建，下一轮会再次处理。
			return nil
		}
		return err

	// 本控制器不管理该 APIService（3A、3B、3C）。
	case !isAutomanaged(curr):
		return nil

	// 远端对象只要求启动时同步，但它是在控制器启动后才加入的（4A、4B、4C）。
	case isAutomanagedOnStart(curr) && !c.apiServicesAtStart[name]:
		return nil

	// 远端对象只要求启动时同步且已同步过（5A、5B、5C 的 once 约束）。
	case isAutomanagedOnStart(curr) && hasSynced:
		return nil

	// 我们有一个自己管理的、多余的 APIService，删除它（5A、6A）。
	// UID 前置条件保证只删除刚刚看到的那一个实例：若期间对象被重建，
	// 删除会因 UID 不匹配而冲突，下一次协调再按新对象判断，防止误删别人的服务。
	case desired == nil:
		opts := metav1.DeleteOptions{Preconditions: metav1.NewUIDPreconditions(string(curr.UID))}
		err := c.apiServiceClient.APIServices().Delete(context.TODO(), curr.Name, opts)
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// 期间对象被删除或改动，下轮会重新处理。
			return nil
		}
		return err

	// spec 已一致，无需操作。
	case reflect.DeepEqual(curr.Spec, desired.Spec):
		return nil
	}

	// 我们已有条目、也有期望对象，现在做冲突消解；只有少数几个字段重要（5B、5C、6B、6C）。
	// 只覆盖 Spec 并保留元数据、状态：状态由状态控制器维护，标签和注解可能来自
	// 其他控制器，整体替换 desired 会把这些并发写入全部抹掉，还会造成无谓的冲突。
	apiService := curr.DeepCopy()
	apiService.Spec = desired.Spec
	_, err = c.apiServiceClient.APIServices().Update(context.TODO(), apiService, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// 期间对象被删除或改动，下轮会重新处理。
		return nil
	}
	return err
}

// GetAPIServiceToSync 取出一个待同步的 APIService。
func (c *autoRegisterController) GetAPIServiceToSync(name string) *v1.APIService {
	c.apiServicesToSyncLock.RLock()
	defer c.apiServicesToSyncLock.RUnlock()

	return c.apiServicesToSync[name]
}

// AddAPIServiceToSyncOnStart 注册一个仅在控制器启动时同步一次的 APIService。
func (c *autoRegisterController) AddAPIServiceToSyncOnStart(in *v1.APIService) {
	c.addAPIServiceToSync(in, manageOnStart)
}

// AddAPIServiceToSync 注册一个持续同步的 APIService。
func (c *autoRegisterController) AddAPIServiceToSync(in *v1.APIService) {
	c.addAPIServiceToSync(in, manageContinuously)
}

// 传入的对象来自调用方，DeepCopy 之后再保存，避免调用方后续修改通过指针
// 污染期望状态；同时自动打上管理标签，让协调和删除逻辑有据可依。
func (c *autoRegisterController) addAPIServiceToSync(in *v1.APIService, syncType string) {
	c.apiServicesToSyncLock.Lock()
	defer c.apiServicesToSyncLock.Unlock()

	apiService := in.DeepCopy()
	if apiService.Labels == nil {
		apiService.Labels = map[string]string{}
	}
	apiService.Labels[AutoRegisterManagedLabel] = syncType

	c.apiServicesToSync[apiService.Name] = apiService
	c.queue.Add(apiService.Name)
}

// RemoveAPIServiceToSync 删除一个已注册的 APIService。
func (c *autoRegisterController) RemoveAPIServiceToSync(name string) {
	c.apiServicesToSyncLock.Lock()
	defer c.apiServicesToSyncLock.Unlock()

	delete(c.apiServicesToSync, name)
	// 删除期望对象也必须入队：只有协调一次才能判断现存对象是不是自己管理的，
	// 并决定是否清理，光改内存不会减少集群里已经创建出来的 APIService。
	c.queue.Add(name)
}

func (c *autoRegisterController) hasSyncedSuccessfully(name string) bool {
	c.syncedSuccessfullyLock.RLock()
	defer c.syncedSuccessfullyLock.RUnlock()
	return c.syncedSuccessfully[name]
}

func (c *autoRegisterController) setSyncedSuccessfully(name string) {
	c.syncedSuccessfullyLock.Lock()
	defer c.syncedSuccessfullyLock.Unlock()
	c.syncedSuccessfully[name] = true
}

// nil 返回空串，让调用方不必先判空就能区分“不存在”“onstart”“持续管理”三种情况，
// 协调循环里的分支因此少一层嵌套。
func automanagedType(service *v1.APIService) string {
	if service == nil {
		return ""
	}
	return service.Labels[AutoRegisterManagedLabel]
}

func isAutomanagedOnStart(service *v1.APIService) bool {
	return automanagedType(service) == manageOnStart
}

func isAutomanaged(service *v1.APIService) bool {
	managedType := automanagedType(service)
	return managedType == manageOnStart || managedType == manageContinuously
}
