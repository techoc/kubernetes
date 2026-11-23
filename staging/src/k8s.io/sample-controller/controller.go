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

// 本文件是 sample-controller 的核心实现：把 informer 的变更事件收进 workqueue，
// 让 Foo 的期望状态逐步收敛到它拥有的 Deployment 上。
// 这里刻意只存对象引用而不存对象本体，处理时再查缓存，避免拿到过期快照。

package main

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	samplev1alpha1 "k8s.io/sample-controller/pkg/apis/samplecontroller/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	informers "k8s.io/sample-controller/pkg/generated/informers/externalversions/samplecontroller/v1alpha1"
	listers "k8s.io/sample-controller/pkg/generated/listers/samplecontroller/v1alpha1"
)

// 事件来源统一用这个 agent 名，便于在 Event 和审计日志里定位是哪个写入方。
const controllerAgentName = "sample-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	//
	// reason 保持为简短的稳定标识，便于按 reason 聚合事件；具体上下文放 message 里。
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Foo fails
	// to sync due to a Deployment of the same name already existing.
	//
	// 同名资源不属于本 Foo 时不能强行接管，否则会篡改别人的对象；
	// 这里的失败是有意为之，宁可报错让人工介入。
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	//
	// %q 只填资源名，这样同一条模板能覆盖所有冲突场景，不必为每种名字拼字符串。
	MessageResourceExists = "Resource %q already exists and is not managed by Foo"
	// MessageResourceSynced is the message used for an Event fired when a Foo
	// is synced successfully
	//
	// 只在整轮同步成功后发，避免中间状态被误读为最终结果。
	MessageResourceSynced = "Foo synced successfully"
	// FieldManager distinguishes this controller from other things writing to API objects
	//
	// 复用 agent 名而非另起一个，让 server-side apply 的字段归属和事件来源对得上。
	FieldManager = controllerAgentName
)

// Controller is the controller implementation for Foo resources
//
// 控制器本身只保留客户端、缓存读取口和队列，不缓存业务状态：
// 每次处理都重新查 lister，保证决策基于当前缓存而非入队那一刻的旧对象。
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	//
	// 内置资源（这里是 Deployment）走这个客户端；分开持有便于测试时单独注入 fake。
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	//
	// 自定义资源 Foo 的读写走这里；状态更新需要它，所以不能省。
	sampleclientset clientset.Interface

	// 四个字段成对出现：Lister 读缓存，Synced 判断缓存是否已完成首次全量同步。
	// 启动 worker 前必须等 Synced，否则空缓存会被误判成“资源不存在”而触发创建。
	deploymentsLister appslisters.DeploymentLister
	deploymentsSynced cache.InformerSynced
	foosLister        listers.FooLister
	foosSynced        cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	//
	// 队列里只放 namespace/name 而不是整个 Foo，能天然去重：
	// 同一对象连续变更多次也只处理一次，且多个 worker 不会并发处理同一项。
	workqueue workqueue.TypedRateLimitingInterface[cache.ObjectName]
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	//
	// 事件用于把“拒绝接管已有 Deployment”这类决策暴露给用户，
	// 单看日志很难把原因关联回具体的 Foo。
	recorder record.EventRecorder
}

// NewController returns a new sample controller
//
// 构造函数只装配依赖和事件处理函数，不启动任何 goroutine；
// 真正的生命周期由 Run 控制，这样调用方可以先注册更多 informer 再去启动。
func NewController(
	ctx context.Context,
	kubeclientset kubernetes.Interface,
	sampleclientset clientset.Interface,
	deploymentInformer appsinformers.DeploymentInformer,
	fooInformer informers.FooInformer) *Controller {
	logger := klog.FromContext(ctx)

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	//
	// 自定义类型必须先注册进 scheme，广播器才能识别 Event 的 involvedObject 并正确序列化。
	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	logger.V(4).Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	// 同步写日志、异步写 API：API 写失败不该拖慢甚至卡住控制器主循环。
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	// Component 填 agent 名，便于从 Event 反查是哪个控制器产生的。
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	// 指数退避处理“对象暂时不可用”，令牌桶限制整体重试速率：
	// 前者兜住单个坏对象，后者避免大量失败项同时重试而淹没 API server。
	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](5*time.Millisecond, 1000*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)},
	)

	controller := &Controller{
		kubeclientset:     kubeclientset,
		sampleclientset:   sampleclientset,
		deploymentsLister: deploymentInformer.Lister(),
		deploymentsSynced: deploymentInformer.Informer().HasSynced,
		foosLister:        fooInformer.Lister(),
		foosSynced:        fooInformer.Informer().HasSynced,
		workqueue:         workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:          recorder,
	}

	logger.Info("Setting up event handlers")
	// Set up an event handler for when Foo resources change
	//
	// 只监听期望状态相关的变化；Foo 的状态更新同样会触发 Update，
	// 但多入队一次比漏掉一次便宜，去重交给 workqueue。
	fooInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueFoo,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueFoo(new)
		},
	})
	// Set up an event handler for when Deployment resources change. This
	// handler will lookup the owner of the given Deployment, and if it is
	// owned by a Foo resource then the handler will enqueue that Foo resource for
	// processing. This way, we don't need to implement custom logic for
	// handling Deployment resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	//
	// 把 Deployment 的变化反向映射到所属 Foo，才能感知“Deployment 被外部改动或删除”；
	// 用 ownerReference 而非自定义标签约定，可以复用 Kubernetes 原生归属关系。
	deploymentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			newDepl := new.(*appsv1.Deployment)
			oldDepl := old.(*appsv1.Deployment)
			if newDepl.ResourceVersion == oldDepl.ResourceVersion {
				// Periodic resync will send update events for all known Deployments.
				// Two different versions of the same Deployment will always have different RVs.
				//
				// 过滤掉 resync 带来的伪更新，否则每轮周期性同步都会把全部对象重新入队。
				return
			}
			controller.handleObject(new)
		},
		DeleteFunc: controller.handleObject,
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
//
// 先等缓存同步再起 worker：空缓存会让“不存在”判断失真，进而误创建资源。
// ctx 取消后只负责让 workqueue 停止，worker 的退出由同一个 ctx 传递。
func (c *Controller) Run(ctx context.Context, workers int) error {
	// 兜住 worker 之外的 panic；workqueue 的关闭放在最后，确保退出的 worker 仍能安全 Get。
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting Foo controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.deploymentsSynced, c.foosSynced); !ok {
		// 同步失败通常意味着 ctx 已被取消，此时启动 worker 只会基于残缺缓存做决策。
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process Foo resources
	//
	// worker 数决定并行处理的 Foo 数，workqueue 保证同一项不会被两个 worker 同时取走。
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	logger.Info("Started workers")
	// 阻塞在这里，让 Run 的调用方用一个 ctx 就能控制整个控制器的生命周期。
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
//
// processNextWorkItem 返回 false 代表队列已关闭，循环随即退出；
// 若不检查返回值就会在关队列后陷入空转。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
//
// 成功与失败走两条不同路径：成功 Forget 清掉退避记录，失败保留退避并重新入队。
// 两者都必须先 Done，否则队列会认为该项仍在处理中。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	objRef, shutdown := c.workqueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		// 队列关闭后不应再取任务，否则会和正在退出的 worker 抢已经失效的对象引用。
		return false
	}

	// We call Done at the end of this func so the workqueue knows we have
	// finished processing this item. We also must remember to call Forget
	// if we do not want this work item being re-queued. For example, we do
	// not call Forget if a transient error occurs, instead the item is
	// put back on the workqueue and attempted again after a back-off
	// period.
	//
	// 用 defer 保证任何返回路径（含后续可能的 panic）都能释放处理标记。
	defer c.workqueue.Done(objRef)

	// Run the syncHandler, passing it the structured reference to the object to be synced.
	err := c.syncHandler(ctx, objRef)
	if err == nil {
		// If no error occurs then we Forget this item so it does not
		// get queued again until another change happens.
		//
		// 清掉失败计数，避免这个对象以后再次失败时继承旧的退避间隔。
		c.workqueue.Forget(objRef)
		logger.Info("Successfully synced", "objectName", objRef)
		return true
	}
	// there was a failure so be sure to report it.  This method allows for
	// pluggable error handling which can be used for things like
	// cluster-monitoring.
	utilruntime.HandleErrorWithContext(ctx, err, "Error syncing; requeuing for later retry", "objectReference", objRef)
	// since we failed, we should requeue the item to work on later.  This
	// method will add a backoff to avoid hotlooping on particular items
	// (they're probably still not going to work right away) and overall
	// controller protection (everything I've done is broken, this controller
	// needs to calm down or it can starve other useful work) cases.
	//
	// 带上退避重试既给暂时性故障留恢复时间，也避免坏对象把队列刷成死循环。
	c.workqueue.AddRateLimited(objRef)
	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Foo resource
// with the current status of the resource.
//
// 按“取 Foo -> 找/建 Deployment -> 校验归属 -> 对齐副本数 -> 写回状态”顺序推进：
// 归属校验放在修改之前，确保控制器不会去更新不属于自己的 Deployment。
func (c *Controller) syncHandler(ctx context.Context, objectRef cache.ObjectName) error {
	// 把对象引用绑到 logger 上，后续每一行日志都能回溯到同一个 Foo。
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "objectRef", objectRef)

	// Get the Foo resource with this namespace/name
	foo, err := c.foosLister.Foos(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		// The Foo resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			// 队列里是入队时的快照，Foo 可能已被删除；删除场景下无需重建，正常结束即可。
			utilruntime.HandleErrorWithContext(ctx, err, "Foo referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	deploymentName := foo.Spec.DeploymentName
	if deploymentName == "" {
		// We choose to absorb the error here as the worker would requeue the
		// resource otherwise. Instead, the next time the resource is updated
		// the resource will be queued again.
		//
		// 必填字段缺失属于用户输入问题，重试也不会变好；等下一次更新再处理更重要。
		utilruntime.HandleErrorWithContext(ctx, nil, "Deployment name missing from object reference", "objectReference", objectRef)
		return nil
	}

	// Get the deployment with the name specified in Foo.spec
	deployment, err := c.deploymentsLister.Deployments(foo.Namespace).Get(deploymentName)
	// If the resource doesn't exist, we'll create it
	//
	// 缺失即创建；同名但不属于本 Foo 的情况留给下面的归属校验处理。
	if errors.IsNotFound(err) {
		deployment, err = c.kubeclientset.AppsV1().Deployments(foo.Namespace).Create(ctx, newDeployment(foo), metav1.CreateOptions{FieldManager: FieldManager})
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	//
	// 把真正错误留到这里统一处理，避免分支里各写一遍重试逻辑。
	if err != nil {
		return err
	}

	// If the Deployment is not controlled by this Foo resource, we should log
	// a warning to the event recorder and return error msg.
	//
	// 只认 controller ownerReference，普通 ownerReference 不算归属；
	// 同名 Deployment 若被别的对象控制，直接修改它会破坏别人的期望状态。
	if !metav1.IsControlledBy(deployment, foo) {
		msg := fmt.Sprintf(MessageResourceExists, deployment.Name)
		c.recorder.Event(foo, corev1.EventTypeWarning, ErrResourceExists, msg)
		return fmt.Errorf("%s", msg)
	}

	// If this number of the replicas on the Foo resource is specified, and the
	// number does not equal the current desired replicas on the Deployment, we
	// should update the Deployment resource.
	//
	// 只在副本数真的不同才发 Update，避免反复写同一个对象产生无意义的事件和版本 churn。
	if foo.Spec.Replicas != nil && *foo.Spec.Replicas != *deployment.Spec.Replicas {
		logger.V(4).Info("Update deployment resource", "currentReplicas", *deployment.Spec.Replicas, "desiredReplicas", *foo.Spec.Replicas)
		deployment, err = c.kubeclientset.AppsV1().Deployments(foo.Namespace).Update(ctx, newDeployment(foo), metav1.UpdateOptions{FieldManager: FieldManager})
	}

	// If an error occurs during Update, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	//
	// 更新后的 deployment 要继续用于状态回填，所以失败必须终止本轮。
	if err != nil {
		return err
	}

	// Finally, we update the status block of the Foo resource to reflect the
	// current state of the world
	//
	// 状态读自刚拿到的 Deployment，而不是再发一次请求，减少一次往返也更一致。
	err = c.updateFooStatus(ctx, foo, deployment)
	if err != nil {
		return err
	}

	// 事件仅在全部步骤成功后发出，让用户能把它当作“本轮收敛完成”的信号。
	c.recorder.Event(foo, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

// updateFooStatus 把 Deployment 的可用副本数回填到 Foo 的 status。
//
// 只改 status 子资源，避免碰 spec：spec 是用户声明的期望状态，控制器不该覆盖它。
func (c *Controller) updateFooStatus(ctx context.Context, foo *samplev1alpha1.Foo, deployment *appsv1.Deployment) error {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	//
	// lister 返回的是共享缓存里的对象，原地改会污染其他读取者且不会真正写回 API。
	fooCopy := foo.DeepCopy()
	fooCopy.Status.AvailableReplicas = deployment.Status.AvailableReplicas
	// If the CustomResourceSubresources feature gate is not enabled,
	// we must use Update instead of UpdateStatus to update the Status block of the Foo resource.
	// UpdateStatus will not allow changes to the Spec of the resource,
	// which is ideal for ensuring nothing other than resource status has been updated.
	//
	// 用 FieldManager 标明写入方，配合 server-side apply 时字段归属更清晰。
	_, err := c.sampleclientset.SamplecontrollerV1alpha1().Foos(foo.Namespace).UpdateStatus(ctx, fooCopy, metav1.UpdateOptions{FieldManager: FieldManager})
	return err
}

// enqueueFoo takes a Foo resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Foo.
//
// 队列里放引用而非对象，让处理阶段总能基于缓存里的最新版本做决定。
func (c *Controller) enqueueFoo(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		// 转换失败说明事件来源不是我们期望的元数据对象，入队也只会再次失败。
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.Add(objectRef)
	}
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Foo resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Foo resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
//
// 这是“从属资源反向触发”的通用入口：只处理由本例 Foo 控制的对象，
// 其他 owner 直接忽略，避免一个控制器抢管集群里的所有 Deployment。
func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	logger := klog.FromContext(context.Background())
	if object, ok = obj.(metav1.Object); !ok {
		// 删除事件的回调拿到的可能是 tombstone，而不是对象本体；
		// 不解包就会丢掉“所属 Foo 被外部删掉 Deployment”这类关键事件。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			// If the object value is not too big and does not contain sensitive information then
			// it may be useful to include it.
			utilruntime.HandleErrorWithContext(context.Background(), nil, "Error decoding object, invalid type", "type", fmt.Sprintf("%T", obj))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			// If the object value is not too big and does not contain sensitive information then
			// it may be useful to include it.
			utilruntime.HandleErrorWithContext(context.Background(), nil, "Error decoding object tombstone, invalid type", "type", fmt.Sprintf("%T", tombstone.Obj))
			return
		}
		logger.V(4).Info("Recovered deleted object", "resourceName", object.GetName())
	}
	logger.V(4).Info("Processing object", "object", klog.KObj(object))
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Foo, we should not do anything more
		// with it.
		//
		// 只认 controller ownerReference；同一对象可以同时挂多个普通 owner，
		// 但真正的生命周期责任人只有一个。
		if ownerRef.Kind != "Foo" {
			return
		}

		foo, err := c.foosLister.Foos(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			// owner 引用可能指向已删除的 Foo，孤儿对象无需入队，等下一次变化再说。
			logger.V(4).Info("Ignore orphaned object", "object", klog.KObj(object), "foo", ownerRef.Name)
			return
		}

		c.enqueueFoo(foo)
		return
	}
}

// newDeployment creates a new Deployment for a Foo resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the Foo resource that 'owns' it.
//
// 这是纯函数：每次都从 Foo 构造完整的 Deployment 期望状态，
// 创建和更新共用同一份定义，避免两条路径的配置日后各自漂移。
func newDeployment(foo *samplev1alpha1.Foo) *appsv1.Deployment {
	// selector 与 template 的标签必须一致，否则 Deployment 永远选不到自己创建的 Pod。
	labels := map[string]string{
		"app":        "nginx",
		"controller": foo.Name,
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      foo.Spec.DeploymentName,
			Namespace: foo.Namespace,
			// 控制器 ownerReference 同时承担归属标识和级联删除语义：
			// Foo 被删时，它创建的 Deployment 会被 Kubernetes 自动回收。
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(foo, samplev1alpha1.SchemeGroupVersion.WithKind("Foo")),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: foo.Spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "nginx",
							Image: "nginx:latest",
						},
					},
				},
			},
		},
	}
}
