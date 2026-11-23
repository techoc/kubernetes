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
	// AutoRegisterManagedLabel is a label attached to the APIService that identifies how the APIService wants to be synced.
	AutoRegisterManagedLabel = "kube-aggregator.kubernetes.io/automanaged"

	// manageOnStart is a value for the AutoRegisterManagedLabel that indicates the APIService wants to be synced one time when the controller starts.
	manageOnStart = "onstart"
	// manageContinuously is a value for the AutoRegisterManagedLabel that indicates the APIService wants to be synced continuously.
	manageContinuously = "true"
)

// AutoAPIServiceRegistration is an interface which callers can re-declare locally and properly cast to for
// adding and removing APIServices
// 让调用方在本地重声明这个接口，是为了让各组件只依赖自己需要的注册方法，
// 不必引入整个控制器的实现类型；编译期仍由 Go 的鸭子类型保证兼容。
type AutoAPIServiceRegistration interface {
	// AddAPIServiceToSyncOnStart adds an API service to sync on start.
	AddAPIServiceToSyncOnStart(in *v1.APIService)
	// AddAPIServiceToSync adds an API service to sync continuously.
	AddAPIServiceToSync(in *v1.APIService)
	// RemoveAPIServiceToSync removes an API service to auto-register.
	RemoveAPIServiceToSync(name string)
}

// autoRegisterController is used to keep a particular set of APIServices present in the API.  It is useful
// for cases where you want to auto-register APIs like TPRs or groups from the core kube-apiserver
type autoRegisterController struct {
	apiServiceLister listers.APIServiceLister
	apiServiceSynced cache.InformerSynced
	apiServiceClient apiregistrationclient.APIServicesGetter

	// 期望状态由注册方写入（Add/Remove），协调循环只读取；分离锁避免协调时
	// 阻塞注册方，也让快照语义清晰：取出的对象已经被 DeepCopy，调用方改不到这里。
	apiServicesToSyncLock sync.RWMutex
	apiServicesToSync     map[string]*v1.APIService

	syncHandler func(apiServiceName string) error

	// track which services we have synced
	syncedSuccessfullyLock *sync.RWMutex
	syncedSuccessfully     map[string]bool

	// remember names of services that existed when we started
	// onstart 语义只对启动时就存在的对象生效：后来才出现的同名对象可能是别的组件创建的，
	// 不能因为打了个历史标签就把人家的对象接管或删掉。
	apiServicesAtStart map[string]bool

	// queue is where incoming work is placed to de-dup and to allow "easy" rate limited requeues on errors
	queue workqueue.TypedRateLimitingInterface[string]
}

// NewAutoRegisterController creates a new autoRegisterController.
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

// Run starts the autoregister controller in a loop which syncs API services until stopCh is closed.
func (c *autoRegisterController) Run(workers int, stopCh <-chan struct{}) {
	// don't let panics crash the process
	defer utilruntime.HandleCrash()
	// make sure the work queue is shutdown which will trigger workers to end
	defer c.queue.ShutDown()

	klog.Info("Starting autoregister controller")
	defer klog.Info("Shutting down autoregister controller")

	// wait for your secondary caches to fill before starting your work
	// 缓存未同步就开始协调会看到空列表，进而把本应保留的对象当作多余对象删除；
	// 宁可启动慢一点，也不能在启动瞬间产生破坏性写入。
	if !controllers.WaitForCacheSync("autoregister", stopCh, c.apiServiceSynced) {
		return
	}

	// record APIService objects that existed when we started
	// 只在这里采样一次：后续新增的对象不享有 onstart 语义，
	// 否则每次重启都会把运行中新建的对象误认为“启动即存在”。
	if services, err := c.apiServiceLister.List(labels.Everything()); err == nil {
		for _, service := range services {
			c.apiServicesAtStart[service.Name] = true
		}
	}

	// start up your worker threads based on workers.  Some controllers have multiple kinds of workers
	for i := 0; i < workers; i++ {
		// runWorker will loop until "something bad" happens.  The .Until will then rekick the worker
		// after one second
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	// wait until we're told to stop
	<-stopCh
}

func (c *autoRegisterController) runWorker() {
	// hot loop until we're told to stop.  processNextWorkItem will automatically wait until there's work
	// available, so we don't worry about secondary waits
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
// 错误一律重新入队而不是就地重试：一次失败通常是 apiserver 暂时不可用，
// 带退避的重试既能自愈，也不会让一个卡住的对象饿死队列里的其他工作。
func (c *autoRegisterController) processNextWorkItem() bool {
	// pull the next work item from queue.  It should be a key we use to lookup something in a cache
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// you always have to indicate to the queue that you've completed a piece of work
	defer c.queue.Done(key)

	// do your work on the key.  This method will contains your "do stuff" logic
	err := c.syncHandler(key)
	if err == nil {
		// if you had no error, tell the queue to stop tracking history for your key.  This will
		// reset things like failure counts for per-item rate limiting
		c.queue.Forget(key)
		return true
	}

	// there was a failure so be sure to report it.  This method allows for pluggable error handling
	// which can be used for things like cluster-monitoring
	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", key, err))
	// since we failed, we should requeue the item to work on later.  This method will add a backoff
	// to avoid hotlooping on particular items (they're probably still not going to work right away)
	// and overall controller protection (everything I've done is broken, this controller needs to
	// calm down or it can starve other useful work) cases.
	c.queue.AddRateLimited(key)

	return true
}

// checkAPIService syncs the current APIService against a list of desired APIService objects
//
//	                                                | A. desired: not found | B. desired: sync on start | C. desired: sync always
//	------------------------------------------------|-----------------------|---------------------------|------------------------
//	1. current: lookup error                        | error                 | error                     | error
//	2. current: not found                           | -                     | create once               | create
//	3. current: no sync                             | -                     | -                         | -
//	4. current: sync on start, not present at start | -                     | -                         | -
//	5. current: sync on start, present at start     | delete once           | update once               | update once
//	6. current: sync always                         | delete                | update once               | update
//
// 这张表是实现的权威说明：case 的顺序必须经过精心排列，前面的分支处理更具体的情形，
// 后面的分支才能安全地假设 curr 非空且对象归自己管理。改动时先改表再审代码。
func (c *autoRegisterController) checkAPIService(name string) (err error) {
	desired := c.GetAPIServiceToSync(name)
	curr, err := c.apiServiceLister.Get(name)

	// if we've never synced this service successfully, record a successful sync.
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
	// we had a real error, just return it (1A,1B,1C)
	case err != nil && !apierrors.IsNotFound(err):
		return err

	// we don't have an entry and we don't want one (2A)
	case apierrors.IsNotFound(err) && desired == nil:
		return nil

	// the local object only wants to sync on start and has already synced (2B,5B,6B "once" enforcement)
	case isAutomanagedOnStart(desired) && hasSynced:
		return nil

	// we don't have an entry and we do want one (2B,2C)
	// AlreadyExists 表示另一个协调循环刚刚创建成功，informer 事件随后会再来，
	// 这里直接返回成功，避免把一个正常的并发结果当成错误重试。
	case apierrors.IsNotFound(err) && desired != nil:
		_, err := c.apiServiceClient.APIServices().Create(context.TODO(), desired, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			// created in the meantime, we'll get called again
			return nil
		}
		return err

	// we aren't trying to manage this APIService (3A,3B,3C)
	case !isAutomanaged(curr):
		return nil

	// the remote object only wants to sync on start, but was added after we started (4A,4B,4C)
	case isAutomanagedOnStart(curr) && !c.apiServicesAtStart[name]:
		return nil

	// the remote object only wants to sync on start and has already synced (5A,5B,5C "once" enforcement)
	case isAutomanagedOnStart(curr) && hasSynced:
		return nil

	// we have a spurious APIService that we're managing, delete it (5A,6A)
	// UID 前置条件保证只删除刚刚看到的那一个实例：若期间对象被重建，
	// 删除会因 UID 不匹配而冲突，下一次协调再按新对象判断，防止误删别人的服务。
	case desired == nil:
		opts := metav1.DeleteOptions{Preconditions: metav1.NewUIDPreconditions(string(curr.UID))}
		err := c.apiServiceClient.APIServices().Delete(context.TODO(), curr.Name, opts)
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// deleted or changed in the meantime, we'll get called again
			return nil
		}
		return err

	// if the specs already match, nothing for us to do
	case reflect.DeepEqual(curr.Spec, desired.Spec):
		return nil
	}

	// we have an entry and we have a desired, now we deconflict.  Only a few fields matter. (5B,5C,6B,6C)
	// 只覆盖 Spec 并保留元数据、状态：状态由状态控制器维护，标签和注解可能来自
	// 其他控制器，整体替换 desired 会把这些并发写入全部抹掉，还会造成无谓的冲突。
	apiService := curr.DeepCopy()
	apiService.Spec = desired.Spec
	_, err = c.apiServiceClient.APIServices().Update(context.TODO(), apiService, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// deleted or changed in the meantime, we'll get called again
		return nil
	}
	return err
}

// GetAPIServiceToSync gets a single API service to sync.
func (c *autoRegisterController) GetAPIServiceToSync(name string) *v1.APIService {
	c.apiServicesToSyncLock.RLock()
	defer c.apiServicesToSyncLock.RUnlock()

	return c.apiServicesToSync[name]
}

// AddAPIServiceToSyncOnStart registers an API service to sync only when the controller starts.
func (c *autoRegisterController) AddAPIServiceToSyncOnStart(in *v1.APIService) {
	c.addAPIServiceToSync(in, manageOnStart)
}

// AddAPIServiceToSync registers an API service to sync continuously.
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

// RemoveAPIServiceToSync deletes a registered APIService.
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
