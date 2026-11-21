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

// 本文件包含 RC 与 RS 之间转换的适配器，把 ReplicationController 当作
// ReplicaSet 的旧 API 版本对待。这样 ReplicaSetController 就能直接取代旧的
// ReplicationManager，后者原本是 RSC 的一份需要手工维护的复制代码。

package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	apps "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	appsv1apply "k8s.io/client-go/applyconfigurations/apps/v1"
	appsv1autoscaling "k8s.io/client-go/applyconfigurations/autoscaling/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	appsv1client "k8s.io/client-go/kubernetes/typed/apps/v1"
	v1client "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	v1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	appsinternal "k8s.io/kubernetes/pkg/apis/apps"
	appsconversion "k8s.io/kubernetes/pkg/apis/apps/v1"
	apiv1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/controller"
)

// informerAdapter 包装 ReplicationControllerInformer，把 RC 转换成 RS，
// 从而实现 ReplicaSetInformer 接口，让 ReplicaSetController 能无差别地消费 RC。
type informerAdapter struct {
	rcInformer coreinformers.ReplicationControllerInformer
}

// Informer 返回包装后的共享 informer，确保事件在交给处理器前完成 RC→RS 转换。
func (i informerAdapter) Informer() cache.SharedIndexInformer {
	return conversionInformer{i.rcInformer.Informer()}
}

// Lister 返回包装后的 lister，使查询返回的对象都是 RS 类型。
func (i informerAdapter) Lister() appslisters.ReplicaSetLister {
	return conversionLister{i.rcInformer.Lister()}
}

// conversionInformer 嵌入真正的 SharedIndexInformer，只重写事件处理器注册方法，
// 注入转换层，其余 informer 行为完全复用底层实现。
type conversionInformer struct {
	cache.SharedIndexInformer
}

// AddEventHandler 注册经过转换包装的事件处理器。
func (i conversionInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return i.SharedIndexInformer.AddEventHandler(conversionEventHandler{handler})
}

// AddEventHandlerWithResyncPeriod 带重同步周期注册处理器，同样注入转换包装。
func (i conversionInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return i.SharedIndexInformer.AddEventHandlerWithResyncPeriod(conversionEventHandler{handler}, resyncPeriod)
}

// conversionLister 把 RC lister 的查询结果转换为 RS。
type conversionLister struct {
	rcLister v1listers.ReplicationControllerLister
}

// List 按标签选择器列出 RC 并以 RS 形式返回。
func (l conversionLister) List(selector labels.Selector) ([]*apps.ReplicaSet, error) {
	rcList, err := l.rcLister.List(selector)
	if err != nil {
		return nil, err
	}
	return convertSlice(rcList)
}

// ReplicaSets 返回命名空间级的 lister 适配器。
func (l conversionLister) ReplicaSets(namespace string) appslisters.ReplicaSetNamespaceLister {
	return conversionNamespaceLister{l.rcLister.ReplicationControllers(namespace)}
}

// GetPodReplicaSets 找出管理给定 Pod 的 RC，转换为 RS 后返回。
func (l conversionLister) GetPodReplicaSets(pod *v1.Pod) ([]*apps.ReplicaSet, error) {
	rcList, err := l.rcLister.GetPodControllers(pod)
	if err != nil {
		return nil, err
	}
	return convertSlice(rcList)
}

// conversionNamespaceLister 是命名空间级的 RC→RS lister 适配器。
type conversionNamespaceLister struct {
	rcLister v1listers.ReplicationControllerNamespaceLister
}

// List 在该命名空间内按标签选择器查询 RC 并转换为 RS。
func (l conversionNamespaceLister) List(selector labels.Selector) ([]*apps.ReplicaSet, error) {
	rcList, err := l.rcLister.List(selector)
	if err != nil {
		return nil, err
	}
	return convertSlice(rcList)
}

// Get 读取单个 RC 并转换为 RS。
func (l conversionNamespaceLister) Get(name string) (*apps.ReplicaSet, error) {
	rc, err := l.rcLister.Get(name)
	if err != nil {
		return nil, err
	}
	return convertRCtoRS(rc, nil)
}

// conversionEventHandler 把底层 RC 事件转换后转发给 RS 事件处理器。
type conversionEventHandler struct {
	handler cache.ResourceEventHandler
}

// OnAdd 将新增的 RC 转成 RS 再通知处理器；转换失败时丢弃事件并记录错误，
// 避免把一个语义不完整的对象交给控制器引发更隐蔽的错误。
func (h conversionEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	rs, err := convertRCtoRS(obj.(*v1.ReplicationController), nil)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("dropping RC OnAdd event: can't convert object %#v to RS: %v", obj, err))
		return
	}
	h.handler.OnAdd(rs, isInInitialList)
}

// OnUpdate 同时转换新旧对象后转发更新事件，保证控制器看到的前后状态一致。
func (h conversionEventHandler) OnUpdate(oldObj, newObj interface{}) {
	oldRS, err := convertRCtoRS(oldObj.(*v1.ReplicationController), nil)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("dropping RC OnUpdate event: can't convert old object %#v to RS: %v", oldObj, err))
		return
	}
	newRS, err := convertRCtoRS(newObj.(*v1.ReplicationController), nil)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("dropping RC OnUpdate event: can't convert new object %#v to RS: %v", newObj, err))
		return
	}
	h.handler.OnUpdate(oldRS, newRS)
}

// OnDelete 转发删除事件。
// informer 在无法取得最终状态时可能投递 DeletedFinalStateUnknown 墓碑，
// 此时必须解出其中的 RC 再转换，并保留 Key 构造墓碑，
// 否则控制器无法正确清理队列中的键。
func (h conversionEventHandler) OnDelete(obj interface{}) {
	rc, ok := obj.(*v1.ReplicationController)
	if !ok {
		// 事件可能是墓碑，先解包其中的 RC。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("dropping RC OnDelete event: couldn't get object from tombstone %+v", obj))
			return
		}
		rc, ok = tombstone.Obj.(*v1.ReplicationController)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("dropping RC OnDelete event: tombstone contained object that is not a RC %#v", obj))
			return
		}
		rs, err := convertRCtoRS(rc, nil)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("dropping RC OnDelete event: can't convert object %#v to RS: %v", obj, err))
			return
		}
		h.handler.OnDelete(cache.DeletedFinalStateUnknown{Key: tombstone.Key, Obj: rs})
		return
	}

	// 普通删除事件，直接转换对象。
	rs, err := convertRCtoRS(rc, nil)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("dropping RC OnDelete event: can't convert object %#v to RS: %v", obj, err))
		return
	}
	h.handler.OnDelete(rs)
}

// clientsetAdapter 包装 clientset，使 AppsV1() 返回由 RC 客户端实现的 RS 客户端。
type clientsetAdapter struct {
	clientset.Interface
}

// AppsV1 返回把 ReplicaSet 操作转发到 ReplicationController 的适配器。
func (c clientsetAdapter) AppsV1() appsv1client.AppsV1Interface {
	return conversionAppsV1Client{c.Interface, c.Interface.AppsV1()}
}

// Apps 与 AppsV1 相同，兼容通过 Apps() 获取客户端的调用方。
func (c clientsetAdapter) Apps() appsv1client.AppsV1Interface {
	return conversionAppsV1Client{c.Interface, c.Interface.AppsV1()}
}

// conversionAppsV1Client 同时持有原 clientset 与 apps/v1 客户端，
// 便于在需要时切换到 CoreV1 的 RC 客户端。
type conversionAppsV1Client struct {
	clientset clientset.Interface
	appsv1client.AppsV1Interface
}

// ReplicaSets 返回把 RS 请求转换为 RC 请求的客户端适配器。
func (c conversionAppsV1Client) ReplicaSets(namespace string) appsv1client.ReplicaSetInterface {
	return conversionClient{c.clientset.CoreV1().ReplicationControllers(namespace)}
}

// conversionClient 把 ReplicaSetInterface 的调用映射到 ReplicationControllerInterface。
type conversionClient struct {
	v1client.ReplicationControllerInterface
}

// Create 将 RS 转为 RC 后创建，并把结果转回 RS。
func (c conversionClient) Create(ctx context.Context, rs *apps.ReplicaSet, opts metav1.CreateOptions) (*apps.ReplicaSet, error) {
	return convertCall(func(rc *v1.ReplicationController) (*v1.ReplicationController, error) {
		return c.ReplicationControllerInterface.Create(ctx, rc, opts)
	}, rs)
}

// Update 将 RS 转为 RC 后更新，并把结果转回 RS。
func (c conversionClient) Update(ctx context.Context, rs *apps.ReplicaSet, opts metav1.UpdateOptions) (*apps.ReplicaSet, error) {
	return convertCall(func(rc *v1.ReplicationController) (*v1.ReplicationController, error) {
		return c.ReplicationControllerInterface.Update(ctx, rc, opts)
	}, rs)
}

// UpdateStatus 通过 RC 的状态接口更新状态，保持与 RS 接口语义一致。
func (c conversionClient) UpdateStatus(ctx context.Context, rs *apps.ReplicaSet, opts metav1.UpdateOptions) (*apps.ReplicaSet, error) {
	return convertCall(func(rc *v1.ReplicationController) (*v1.ReplicationController, error) {
		return c.ReplicationControllerInterface.UpdateStatus(ctx, rc, opts)
	}, rs)
}

// Get 读取 RC 并转换为 RS 返回。
func (c conversionClient) Get(ctx context.Context, name string, options metav1.GetOptions) (*apps.ReplicaSet, error) {
	rc, err := c.ReplicationControllerInterface.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return convertRCtoRS(rc, nil)
}

// List 列出 RC 并转换为 RS 列表。
func (c conversionClient) List(ctx context.Context, opts metav1.ListOptions) (*apps.ReplicaSetList, error) {
	rcList, err := c.ReplicationControllerInterface.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	return convertList(rcList)
}

// Watch 不支持直接调用：控制器使用包装后的共享 informer 接收事件，
// 这里的 Watch 若被使用反而会绕过转换层得到未转换的 RC 对象。
func (c conversionClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	// RSC 不调用此方法，因为事件来自已包装的共享 informer。
	return nil, errors.New("Watch() is not implemented for conversionClient")
}

// Patch 未实现，因为 RSC 不通过补丁接口操作 ReplicaSet。
func (c conversionClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *apps.ReplicaSet, err error) {
	// RSC 不调用此方法。
	return nil, errors.New("Patch() is not implemented for conversionClient")
}

// Apply 未实现：RC 不存在对应的 server-side apply 配置类型。
func (c conversionClient) Apply(ctx context.Context, rs *appsv1apply.ReplicaSetApplyConfiguration, opts metav1.ApplyOptions) (*apps.ReplicaSet, error) {
	return nil, errors.New("Apply() is not implemented for conversionClient")
}

// ApplyStatus 未实现，原因同 Apply。
func (c conversionClient) ApplyStatus(ctx context.Context, rs *appsv1apply.ReplicaSetApplyConfiguration, opts metav1.ApplyOptions) (*apps.ReplicaSet, error) {
	return nil, errors.New("ApplyStatus() is not implemented for conversionClient")
}

// GetScale 未实现：RSC 不读取 Scale 子资源。
func (c conversionClient) GetScale(ctx context.Context, name string, options metav1.GetOptions) (result *autoscalingv1.Scale, err error) {
	// RSC 不调用此方法。
	return nil, errors.New("GetScale() is not implemented for conversionClient")
}

// UpdateScale 未实现：RSC 不更新 Scale 子资源。
func (c conversionClient) UpdateScale(ctx context.Context, name string, scale *autoscalingv1.Scale, opts metav1.UpdateOptions) (result *autoscalingv1.Scale, err error) {
	// RSC 不调用此方法。
	return nil, errors.New("UpdateScale() is not implemented for conversionClient")
}

// ApplyScale 未实现：RC 没有对应的 Scale apply 接口。
func (c conversionClient) ApplyScale(ctx context.Context, name string, scale *appsv1autoscaling.ScaleApplyConfiguration, opts metav1.ApplyOptions) (*autoscalingv1.Scale, error) {
	return nil, errors.New("ApplyScale() is not implemented for conversionClient")
}

// convertSlice 批量转换 RC 切片；任一对象转换失败就整体失败，避免出现部分结果。
func convertSlice(rcList []*v1.ReplicationController) ([]*apps.ReplicaSet, error) {
	rsList := make([]*apps.ReplicaSet, 0, len(rcList))
	for _, rc := range rcList {
		rs, err := convertRCtoRS(rc, nil)
		if err != nil {
			return nil, err
		}
		rsList = append(rsList, rs)
	}
	return rsList, nil
}

// convertList 把 RC 列表原地转换为 RS 列表，将结果写入预分配的目标槽位。
func convertList(rcList *v1.ReplicationControllerList) (*apps.ReplicaSetList, error) {
	rsList := &apps.ReplicaSetList{Items: make([]apps.ReplicaSet, len(rcList.Items))}
	for i := range rcList.Items {
		rc := &rcList.Items[i]
		_, err := convertRCtoRS(rc, &rsList.Items[i])
		if err != nil {
			return nil, err
		}
	}
	return rsList, nil
}

// convertCall 在两次转换之间执行以 RC 为参数的 API 调用，
// 使控制器可以用 RS 语义调用只接受 RC 的客户端。
func convertCall(fn func(*v1.ReplicationController) (*v1.ReplicationController, error), rs *apps.ReplicaSet) (*apps.ReplicaSet, error) {
	rc, err := convertRStoRC(rs)
	if err != nil {
		return nil, err
	}
	result, err := fn(rc)
	if err != nil {
		return nil, err
	}
	return convertRCtoRS(result, nil)
}

// convertRCtoRS 通过内部类型作为中介把 v1 RC 转成 apps/v1 RS。
// out 非 nil 时写入该对象，便于在列表转换中复用已分配的槽位。
func convertRCtoRS(rc *v1.ReplicationController, out *apps.ReplicaSet) (*apps.ReplicaSet, error) {
	var rsInternal appsinternal.ReplicaSet
	if err := apiv1.Convert_v1_ReplicationController_To_apps_ReplicaSet(rc, &rsInternal, nil); err != nil {
		return nil, fmt.Errorf("can't convert ReplicationController %v/%v to ReplicaSet: %v", rc.Namespace, rc.Name, err)
	}
	if out == nil {
		out = new(apps.ReplicaSet)
	}
	if err := appsconversion.Convert_apps_ReplicaSet_To_v1_ReplicaSet(&rsInternal, out, nil); err != nil {
		return nil, fmt.Errorf("can't convert ReplicaSet (converted from ReplicationController %v/%v) from internal to apps/v1: %v", rc.Namespace, rc.Name, err)
	}
	return out, nil
}

// convertRStoRC 经过内部类型把 apps/v1 RS 转成 v1 RC。
func convertRStoRC(rs *apps.ReplicaSet) (*v1.ReplicationController, error) {
	var rsInternal appsinternal.ReplicaSet
	if err := appsconversion.Convert_v1_ReplicaSet_To_apps_ReplicaSet(rs, &rsInternal, nil); err != nil {
		return nil, fmt.Errorf("can't convert ReplicaSet (converting to ReplicationController %v/%v) from apps/v1 to internal: %v", rs.Namespace, rs.Name, err)
	}
	var rc v1.ReplicationController
	if err := apiv1.Convert_apps_ReplicaSet_To_v1_ReplicationController(&rsInternal, &rc, nil); err != nil {
		return nil, fmt.Errorf("can't convert ReplicaSet to ReplicationController %v/%v: %v", rs.Namespace, rs.Name, err)
	}
	return &rc, nil
}

// podControlAdapter 把 Pod 操作中的 RS 对象转换为 RC，
// 使底层 PodControlInterface 能继续按 RC 语义工作。
type podControlAdapter struct {
	controller.PodControlInterface
}

// CreatePods 创建 Pod 前把所有者 RS 转成 RC。
func (pc podControlAdapter) CreatePods(ctx context.Context, namespace string, template *v1.PodTemplateSpec, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	rc, err := convertRStoRC(object.(*apps.ReplicaSet))
	if err != nil {
		return err
	}
	return pc.PodControlInterface.CreatePods(ctx, namespace, template, rc, controllerRef)
}

// DeletePod 删除 Pod 前把所有者 RS 转成 RC。
func (pc podControlAdapter) DeletePod(ctx context.Context, namespace string, podID string, object runtime.Object) error {
	rc, err := convertRStoRC(object.(*apps.ReplicaSet))
	if err != nil {
		return err
	}
	return pc.PodControlInterface.DeletePod(ctx, namespace, podID, rc)
}
