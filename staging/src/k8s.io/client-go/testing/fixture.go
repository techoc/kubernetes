/*
Copyright 2015 The Kubernetes Authors.

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

package testing

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
)

// ObjectTracker 跟踪对象，可按 kind、namespace 与 name 返回对象以模拟服务端调用。
//
// tracker 是 fake clientset 的“内存 API Server”：按 GVR 存储对象副本，
// 并向 watch 订阅者广播事件。它不实现校验、默认值、准入与真正的子资源语义。
type ObjectTracker interface {
	// Add 把对象加入 tracker；传入 List 时逐项添加。
	Add(obj runtime.Object) error

	// Get 按 kind、namespace 与 name 读取对象。
	Get(gvr schema.GroupVersionResource, ns, name string, opts ...metav1.GetOptions) (runtime.Object, error)

	// Create 在指定命名空间向 tracker 添加对象。
	Create(gvr schema.GroupVersionResource, obj runtime.Object, ns string, opts ...metav1.CreateOptions) error

	// Update 更新 tracker 中指定命名空间的已有对象。
	Update(gvr schema.GroupVersionResource, obj runtime.Object, ns string, opts ...metav1.UpdateOptions) error

	// Patch 修补 tracker 中指定命名空间的已有对象。
	Patch(gvr schema.GroupVersionResource, obj runtime.Object, ns string, opts ...metav1.PatchOptions) error

	// Apply 对 tracker 中指定命名空间的对象执行 apply。
	Apply(gvr schema.GroupVersionResource, applyConfiguration runtime.Object, ns string, opts ...metav1.PatchOptions) error

	// List 返回给定命名空间内某 kind 的所有对象，不接受 List kind。
	List(gvr schema.GroupVersionResource, gvk schema.GroupVersionKind, ns string, opts ...metav1.ListOptions) (runtime.Object, error)

	// Delete 从 tracker 删除已有对象；对象原本不存在时不报错。
	Delete(gvr schema.GroupVersionResource, ns, name string, opts ...metav1.DeleteOptions) error

	// Watch 监听 tracker 中的对象，返回的 channel 会推送新增/修改/删除事件。
	Watch(gvr schema.GroupVersionResource, ns string, opts ...metav1.ListOptions) (watch.Interface, error)
}

// ObjectScheme 抽象对象的通用操作。
type ObjectScheme interface {
	runtime.ObjectCreater
	runtime.ObjectTyper
}

// ObjectReaction 返回把 Action 应用到给定 tracker 的 ReactionFunc。
//
// 若 tracker 同时实现 ManagedFieldObjectTracker，则 apply 会按 field manager 与字段所有权求值；
// 否则不考虑字段所有权。
//
// 警告：fake 客户端不执行服务端默认值填充、校验与转换，子资源语义也不精确（例如更新 scale
// 不会自动更新根资源字段）。
func ObjectReaction(tracker ObjectTracker) ReactionFunc {
	reactor := objectTrackerReact{tracker: tracker}
	return func(action Action) (bool, runtime.Object, error) { // Here and below we need to switch on implementation types,
		// 按具体类型而非接口分派：UpdateAction 与 CreateAction 接口相同，用接口会落入同一分支。
		switch action := action.(type) {
		case ListActionImpl:
			obj, err := reactor.List(action)
			return true, obj, err
		case GetActionImpl:
			obj, err := reactor.Get(action)
			return true, obj, err
		case CreateActionImpl:
			obj, err := reactor.Create(action)
			return true, obj, err
		case UpdateActionImpl:
			obj, err := reactor.Update(action)
			return true, obj, err
		case DeleteActionImpl:
			obj, err := reactor.Delete(action)
			return true, obj, err
		case PatchActionImpl:
			// apply 在 HTTP 层也是 PATCH，但需要按字段所有权合并，因此走单独分支。
			if action.GetPatchType() == types.ApplyPatchType {
				obj, err := reactor.Apply(action)
				return true, obj, err
			}
			obj, err := reactor.Patch(action)
			return true, obj, err
		default:
			return false, nil, fmt.Errorf("no reaction implemented for %s", action)
		}
	}
}

// objectTrackerReact 把具体的 ActionImpl 适配到 ObjectTracker 的方法，
// 避免 ObjectReaction 中混入 tracker 实现细节。
type objectTrackerReact struct {
	tracker ObjectTracker
}

func (o objectTrackerReact) List(action ListActionImpl) (runtime.Object, error) {
	return o.tracker.List(action.GetResource(), action.GetKind(), action.GetNamespace(), action.ListOptions)
}

func (o objectTrackerReact) Get(action GetActionImpl) (runtime.Object, error) {
	return o.tracker.Get(action.GetResource(), action.GetNamespace(), action.GetName(), action.GetOptions)
}

func (o objectTrackerReact) Create(action CreateActionImpl) (runtime.Object, error) {
	ns := action.GetNamespace()
	gvr := action.GetResource()
	objMeta, err := meta.Accessor(action.GetObject())
	if err != nil {
		return nil, err
	}
	if action.GetSubresource() == "" {
		err = o.tracker.Create(gvr, action.GetObject(), ns, action.CreateOptions)
		if err != nil {
			return nil, err
		}
	} else {
		// 子资源的 Create 在 fake 中没有真实语义，退化为对其所属对象的 Update。
		oldObj, getOldObjErr := o.tracker.Get(gvr, ns, objMeta.GetName(), metav1.GetOptions{})
		if getOldObjErr != nil {
			return nil, getOldObjErr
		}
		// 仅当已有对象类型与待更新对象类型相同时才执行更新。
		if reflect.TypeOf(oldObj) == reflect.TypeOf(action.GetObject()) {
			// TODO：目前把子资源 Create 当作对所属资源的 Update，这对部分子资源可行，但不够通用。
			err = o.tracker.Update(gvr, action.GetObject(), ns, metav1.UpdateOptions{
				DryRun:          action.CreateOptions.DryRun,
				FieldManager:    action.CreateOptions.FieldManager,
				FieldValidation: action.CreateOptions.FieldValidation,
			})
		} else {
			// 类型不同时返回提交的对象，但不把它写入 tracker。
			return action.GetObject(), nil
		}
	}
	if err != nil {
		return nil, err
	}
	obj, err := o.tracker.Get(gvr, ns, objMeta.GetName(), metav1.GetOptions{})
	return obj, err
}

// Update 写入对象后重新从 tracker 读取，保证返回的是 tracker 中的副本而非调用方对象。
func (o objectTrackerReact) Update(action UpdateActionImpl) (runtime.Object, error) {
	ns := action.GetNamespace()
	gvr := action.GetResource()
	objMeta, err := meta.Accessor(action.GetObject())
	if err != nil {
		return nil, err
	}

	err = o.tracker.Update(gvr, action.GetObject(), ns, action.UpdateOptions)
	if err != nil {
		return nil, err
	}

	obj, err := o.tracker.Get(gvr, ns, objMeta.GetName(), metav1.GetOptions{})
	return obj, err
}

func (o objectTrackerReact) Delete(action DeleteActionImpl) (runtime.Object, error) {
	err := o.tracker.Delete(action.GetResource(), action.GetNamespace(), action.GetName(), action.DeleteOptions)
	return nil, err
}

// Apply 把 apply configuration 交给 tracker 合并；patch 内容可能是 YAML 或 JSON。
func (o objectTrackerReact) Apply(action PatchActionImpl) (runtime.Object, error) {
	ns := action.GetNamespace()
	gvr := action.GetResource()

	// apply configuration 没有固定的 Go 类型，用 unstructured 承载。
	patchObj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal(action.GetPatch(), &patchObj.Object); err != nil {
		return nil, err
	}
	patchObj.SetName(action.GetName())
	err := o.tracker.Apply(gvr, patchObj, ns, action.PatchOptions)
	if err != nil {
		return nil, err
	}
	obj, err := o.tracker.Get(gvr, ns, action.GetName(), metav1.GetOptions{})
	return obj, err
}

// Patch 在客户端计算补丁结果：先取当前对象序列化为旧值，应用补丁后再写回 tracker。
func (o objectTrackerReact) Patch(action PatchActionImpl) (runtime.Object, error) {
	ns := action.GetNamespace()
	gvr := action.GetResource()

	obj, err := o.tracker.Get(gvr, ns, action.GetName(), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	old, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	// 反序列化前重置对象：unmarshal 不保证清除被 patch 删除的字段。
	value := reflect.ValueOf(obj)
	value.Elem().Set(reflect.New(value.Type().Elem()).Elem())

	switch action.GetPatchType() {
	case types.JSONPatchType:
		// RFC 6902：由操作列表描述增删改，直接作用于旧 JSON。
		patch, err := jsonpatch.DecodePatch(action.GetPatch())
		if err != nil {
			return nil, err
		}
		modified, err := patch.Apply(old)
		if err != nil {
			return nil, err
		}

		if err = json.Unmarshal(modified, obj); err != nil {
			return nil, err
		}
	case types.MergePatchType:
		modified, err := jsonpatch.MergePatch(old, action.GetPatch())
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(modified, obj); err != nil {
			return nil, err
		}
	case types.StrategicMergePatchType:
		// 需要 Go 类型上的 patchStrategy 标签，因此把 obj 作为 schema 传入。
		mergedByte, err := strategicpatch.StrategicMergePatch(old, action.GetPatch(), obj)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(mergedByte, obj); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("PatchType %s is not supported", action.GetPatchType())
	}

	if err = o.tracker.Patch(gvr, obj, ns, action.PatchOptions); err != nil {
		return nil, err
	}

	return obj, nil
}

type tracker struct {
	scheme  ObjectScheme
	decoder runtime.Decoder
	// lock 同时保护 objects、watchers 与 resourceVersions；
	// 广播事件时也持锁，保证 watcher 看到的事件顺序与写入顺序一致。
	lock    sync.RWMutex
	objects map[schema.GroupVersionResource]map[types.NamespacedName]versionedObject
	// watchers 的 key 是命名空间或表示全部的 ""，value 是 watcher 列表。
	// 资源变更会向这些 channel 广播事件；未被消费的事件过多（目前 100 个，见
	// apimachinery/pkg/watch.DefaultChanSize）会导致 panic。
	watchers map[schema.GroupVersionResource]map[string][]*watch.RaceFreeFakeWatcher
	// resourceVersions 记录某 GVR 下对象的最高 resourceVersion。空集合时概念上为 1
	// （0 在查询中有特殊含义），但 map 中没有对应条目。存入新对象或修改对象前会递增。
	//
	// 对象内容不被改写以保持历史行为，因此用 versionedObject 包装，而不是直接修改
	// runtime.Object 的 ResourceVersion。
	//
	// 对 resourceVersion 的支持非常有限（https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions），
	// 只覆盖一种场景：List（不校验 RV，返回的 ListMeta 带 RV）+ Watch（精确匹配 List 返回的 RV）。
	//
	// 这足以满足 informer factory 建立 informer 时 Reflector.ListAndWatch 的需求：
	// https://github.com/kubernetes/kubernetes/blob/b53b9fb5573323484af9a19cf3f5bfe80760abba/staging/src/k8s.io/client-go/tools/cache/reflector.go#L401
	//
	// 严格来说应按 GroupVersion 记录，但对象本身按 GVR 存储，这里与 List 的实现保持一致。
	resourceVersions map[schema.GroupVersionResource]int64
}

// versionedObject 把对象与 tracker 分配的 resourceVersion 一起保存。版本本可直接写在对象里，但 fake client-go 历来不是这么做，改变它可能破坏测试。
type versionedObject struct {
	// 已存储对象的 resourceVersion 始终大于 1，因为空集合的初始值是 1。
	resourceVersion int64
	runtime.Object
}

var _ ObjectTracker = &tracker{}

// NewObjectTracker 创建供 fake clientset 跟踪对象的 ObjectTracker，主要用于单元测试。
func NewObjectTracker(scheme ObjectScheme, decoder runtime.Decoder) ObjectTracker {
	return &tracker{
		scheme:           scheme,
		decoder:          decoder,
		objects:          make(map[schema.GroupVersionResource]map[types.NamespacedName]versionedObject),
		watchers:         make(map[schema.GroupVersionResource]map[string][]*watch.RaceFreeFakeWatcher),
		resourceVersions: make(map[schema.GroupVersionResource]int64),
	}
}

func (t *tracker) List(gvr schema.GroupVersionResource, gvk schema.GroupVersionKind, ns string, opts ...metav1.ListOptions) (runtime.Object, error) {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return nil, err
	}
	// 用「原 kind + List 后缀」猜测 List kind。该 tracker 对 API 模型的理解有限，猜测未必总是正确。
	listGVK := gvk
	listGVK.Kind = listGVK.Kind + "List"
	// GVK 有 "internal version" 概念，scheme 认得 runtime.APIVersionInternal，但不认空字符串。
	if listGVK.Version == "" {
		listGVK.Version = runtime.APIVersionInternal
	}

	list, err := t.scheme.New(listGVK)
	if err != nil {
		return nil, err
	}

	if !meta.IsListType(list) {
		return nil, fmt.Errorf("%q is not a list type", listGVK.Kind)
	}

	t.lock.RLock()
	defer t.lock.RUnlock()

	if listMeta, err := meta.ListAccessor(list); err == nil {
		// 空集合也从 1 开始，与真实 API Server 的 resourceVersion 语义保持一致。
		resourceVersion, ok := t.resourceVersions[gvr]
		if !ok {
			resourceVersion = 1
		}
		listMeta.SetResourceVersion(fmt.Sprintf("%d", resourceVersion))
	}

	objs, ok := t.objects[gvr]
	if !ok {
		return list, nil
	}

	matchingVersionedObjs, err := filterByNamespace(objs, ns)
	if err != nil {
		return nil, err
	}
	// 只把对象本体放进 List，versionedObject 的包装不应泄漏给调用方。
	matchingObjs := make([]runtime.Object, len(matchingVersionedObjs))
	for i, obj := range matchingVersionedObjs {
		matchingObjs[i] = obj.Object
	}
	if err := meta.SetList(list, matchingObjs); err != nil {
		return nil, err
	}
	return list.DeepCopyObject(), nil
}

func (t *tracker) Watch(gvr schema.GroupVersionResource, ns string, opts ...metav1.ListOptions) (watch.Interface, error) {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return nil, err
	}

	// 默认沿用 tracker 传统行为：未提供 list options 时不回灌任何已有对象。
	addExisting := false
	addFromRV := int64(0)
	if len(opts) > 0 {
		// 像生成的 fake 客户端那样提供 options 后，才会按 resourceVersion 回灌已有对象。
		// ResourceVersion 为空时默认「从最新开始」，即回灌所有对象；所有存储对象的
		// addFromRV 都大于 0，因此 addFromRV == 0 能匹配下面全部对象。
		addExisting = true
		if opts[0].ResourceVersion != "" {
			rv, err := strconv.ParseInt(opts[0].ResourceVersion, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid ResourceVersion %q in ListOptions, must be int64: %w", opts[0].ResourceVersion, err)
			}
			addFromRV = rv
		}
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	fakewatcher := watch.NewRaceFreeFake()

	// 注册 watcher 后再回灌已有对象，避免漏掉注册与回灌之间发生的写入。
	if _, exists := t.watchers[gvr]; !exists {
		t.watchers[gvr] = make(map[string][]*watch.RaceFreeFakeWatcher)
	}
	t.watchers[gvr][ns] = append(t.watchers[gvr][ns], fakewatcher)

	// 回灌所有匹配 list options 的对象，例如初次 List 与随后 Watch 之间产生的对象。
	if addExisting {
		objs := t.objects[gvr]
		matchingObjs, err := filterByNamespace(objs, ns)
		if err != nil {
			return nil, err
		}
		for _, obj := range matchingObjs {
			// 只回灌比请求 resourceVersion 更新的对象，模拟“从某版本开始 watch”。
			if addFromRV < obj.resourceVersion {
				fakewatcher.Add(obj.Object)
			}
		}
	}

	return fakewatcher, nil
}

func (t *tracker) Get(gvr schema.GroupVersionResource, ns, name string, opts ...metav1.GetOptions) (runtime.Object, error) {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return nil, err
	}
	errNotFound := apierrors.NewNotFound(gvr.GroupResource(), name)

	t.lock.RLock()
	defer t.lock.RUnlock()

	objs, ok := t.objects[gvr]
	if !ok {
		return nil, errNotFound
	}

	matchingObj, ok := objs[types.NamespacedName{Namespace: ns, Name: name}]
	if !ok {
		return nil, errNotFound
	}

	// tracker 正常工作时应只有一个对象匹配：Add/Update 保证了 kind/namespace/name 唯一。
	obj := matchingObj.DeepCopyObject()
	// 与真实服务端一致：body 中 StatusFailure 的对象在 Get 时转为错误返回。
	if status, ok := obj.(*metav1.Status); ok {
		if status.Status != metav1.StatusSuccess {
			return nil, &apierrors.StatusError{ErrStatus: *status}
		}
	}

	return obj, nil
}

// Add 把对象加入 tracker；对象若有多个已注册 GVK，会分别以各自的 GVR 存储。
func (t *tracker) Add(obj runtime.Object) error {
	if meta.IsListType(obj) {
		return t.addList(obj, false)
	}
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	gvks, _, err := t.scheme.ObjectKinds(obj)
	if err != nil {
		return err
	}

	if partial, ok := obj.(*metav1.PartialObjectMetadata); ok && len(partial.TypeMeta.APIVersion) > 0 {
		// PartialObjectMetadata 的 Go 类型无法反查具体 GVK，只能依赖对象自带的 TypeMeta。
		gvks = []schema.GroupVersionKind{partial.TypeMeta.GroupVersionKind()}
	}

	if len(gvks) == 0 {
		return fmt.Errorf("no registered kinds for %v", obj)
	}
	for _, gvk := range gvks {
		// 注意：UnsafeGuessKindToResource 只是启发式猜测。apiserver 中 gvk 的实际路由
		// 可以是任意的；测试若使用这类对象，不能通过 Add() 预置，而应调用 tracker 的
		// Create() 并显式指定 gvr。
		gvr, _ := meta.UnsafeGuessKindToResource(gvk)
		// Resource 没有 "__internal" 版本概念，直接设为空字符串。
		if gvr.Version == runtime.APIVersionInternal {
			gvr.Version = ""
		}

		err := t.add(gvr, obj, objMeta.GetNamespace(), false)
		if err != nil {
			return err
		}
	}
	return nil
}

// Create 以 replaceExisting=false 写入：对象已存在时返回 AlreadyExists。
func (t *tracker) Create(gvr schema.GroupVersionResource, obj runtime.Object, ns string, opts ...metav1.CreateOptions) error {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return err
	}
	return t.add(gvr, obj, ns, false)
}

// Update 以 replaceExisting=true 写入：对象不存在时返回 NotFound。
func (t *tracker) Update(gvr schema.GroupVersionResource, obj runtime.Object, ns string, opts ...metav1.UpdateOptions) error {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return err
	}
	return t.add(gvr, obj, ns, true)
}

// Patch 与 Update 相同：补丁结果已在上层算好，这里只做覆盖写。
func (t *tracker) Patch(gvr schema.GroupVersionResource, patchedObject runtime.Object, ns string, opts ...metav1.PatchOptions) error {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return err
	}
	return t.add(gvr, patchedObject, ns, true)
}

// Apply 在没有字段管理能力时用 strategic merge patch 模拟 apply。
func (t *tracker) Apply(gvr schema.GroupVersionResource, applyConfiguration runtime.Object, ns string, opts ...metav1.PatchOptions) error {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return err
	}
	applyConfigurationMeta, err := meta.Accessor(applyConfiguration)
	if err != nil {
		return err
	}

	obj, err := t.Get(gvr, ns, applyConfigurationMeta.GetName(), metav1.GetOptions{})
	if err != nil {
		return err
	}

	old, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	// 反序列化前重置对象：unmarshal 不保证清除被 patch 删除的字段。
	value := reflect.ValueOf(obj)
	value.Elem().Set(reflect.New(value.Type().Elem()).Elem())

	// 为兼容 1.30 及更早版本的行为，仍用 strategic merge patch 处理 apply；需要完整字段管理
	// 能力的调用方可使用 fake.NewClientset 与 ManagedFieldObjectTracker。
	patch, err := json.Marshal(applyConfiguration)
	if err != nil {
		return err
	}
	mergedByte, err := strategicpatch.StrategicMergePatch(old, patch, obj)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(mergedByte, obj); err != nil {
		return err
	}

	return t.add(gvr, obj, ns, true)
}

// IsWatchListSemanticsUnSupported 告知 Reflector 该客户端不支持 WatchList 语义。
// 这是为满足 Reflector 的可选接口检查而设的合成方法，返回 true 即表示不能使用 WatchList，
// 本身没有其他逻辑。
func (t *tracker) IsWatchListSemanticsUnSupported() bool {
	return true
}

// getWatches 返回匹配 namespace 的 watcher：集群级 watcher（NamespaceAll）
// 会收到所有 namespace 的事件，因此两者都要包含。
func (t *tracker) getWatches(gvr schema.GroupVersionResource, ns string) []*watch.RaceFreeFakeWatcher {
	watches := []*watch.RaceFreeFakeWatcher{}
	if t.watchers[gvr] != nil {
		if w := t.watchers[gvr][ns]; w != nil {
			watches = append(watches, w...)
		}
		if ns != metav1.NamespaceAll {
			if w := t.watchers[gvr][metav1.NamespaceAll]; w != nil {
				watches = append(watches, w...)
			}
		}
	}
	return watches
}

func (t *tracker) add(gvr schema.GroupVersionResource, obj runtime.Object, ns string, replaceExisting bool) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	gr := gvr.GroupResource()

	// 始终存深拷贝，防止调用方在加入 tracker 后意外修改对象。
	obj = obj.DeepCopyObject()

	newMeta, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	// 新对象未设置 namespace 时，把请求的 namespace 写进去。
	if len(newMeta.GetNamespace()) == 0 {
		// 便于调用方在 tracker 上直接 Add 集群级对象而不必重复填写 namespace。
		newMeta.SetNamespace(ns)
	}

	// 请求 namespace 与对象 namespace 不一致时拒绝，模拟服务端的路径校验。
	if ns != newMeta.GetNamespace() {
		msg := fmt.Sprintf("request namespace does not match object namespace, request: %q object: %q", ns, newMeta.GetNamespace())
		return apierrors.NewBadRequest(msg)
	}

	_, ok := t.objects[gvr]
	if !ok {
		t.objects[gvr] = make(map[types.NamespacedName]versionedObject)
	}

	// 确定新建或更新对象使用的 resource version：每个 GVR 维护一个单调递增的版本号，
	// 用于 List/Watch 之间的衔接。
	resourceVersion, ok := t.resourceVersions[gvr]
	if !ok {
		resourceVersion = 1
	}
	resourceVersion++

	namespacedName := types.NamespacedName{Namespace: newMeta.GetNamespace(), Name: newMeta.GetName()}
	if _, ok = t.objects[gvr][namespacedName]; ok {
		if replaceExisting {
			t.resourceVersions[gvr] = resourceVersion
			t.objects[gvr][namespacedName] = versionedObject{resourceVersion, obj}

			for _, w := range t.getWatches(gvr, ns) {
				// 发深拷贝给 watcher，避免其意外修改对象。
				w.Modify(obj.DeepCopyObject())
			}
			return nil
		}
		return apierrors.NewAlreadyExists(gr, newMeta.GetName())
	}

	if replaceExisting {
		// 尝试更新却没有找到匹配的对象：与真实服务端一样，Update 不存在的对象返回 NotFound，
		// 而不是隐式创建。
		return apierrors.NewNotFound(gr, newMeta.GetName())
	}

	t.resourceVersions[gvr] = resourceVersion
	t.objects[gvr][namespacedName] = versionedObject{resourceVersion, obj}

	for _, w := range t.getWatches(gvr, ns) {
		// 发深拷贝给 watcher，避免其意外修改对象。
		w.Add(obj.DeepCopyObject())
	}

	return nil
}

func (t *tracker) addList(obj runtime.Object, replaceExisting bool) error {
	list, err := meta.ExtractList(obj)
	if err != nil {
		return err
	}
	errs := runtime.DecodeList(list, t.decoder)
	if len(errs) > 0 {
		return errs[0]
	}
	for _, obj := range list {
		if err := t.Add(obj); err != nil {
			return err
		}
	}
	return nil
}

func (t *tracker) Delete(gvr schema.GroupVersionResource, ns, name string, opts ...metav1.DeleteOptions) error {
	_, err := assertOptionalSingleArgument(opts)
	if err != nil {
		return err
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	objs, ok := t.objects[gvr]
	if !ok {
		return apierrors.NewNotFound(gvr.GroupResource(), name)
	}

	namespacedName := types.NamespacedName{Namespace: ns, Name: name}
	obj, ok := objs[namespacedName]
	if !ok {
		return apierrors.NewNotFound(gvr.GroupResource(), name)
	}

	delete(objs, namespacedName)
	for _, w := range t.getWatches(gvr, ns) {
		w.Delete(obj.DeepCopyObject())
	}
	return nil
}

// managedFieldObjectTracker 在普通 tracker 之上引入 managedfields.FieldManager，
// 使 fake apply 能模拟字段所有权与冲突（409）语义。
type managedFieldObjectTracker struct {
	ObjectTracker
	scheme          ObjectScheme
	objectConverter runtime.ObjectConvertor
	mapper          func() meta.RESTMapper
	typeConverter   managedfields.TypeConverter
}

var _ ObjectTracker = &managedFieldObjectTracker{}

// NewFieldManagedObjectTracker 创建可同时跟踪对象与 managed fields 的 ObjectTracker，主要用于单元测试。
func NewFieldManagedObjectTracker(scheme *runtime.Scheme, decoder runtime.Decoder, typeConverter managedfields.TypeConverter) ObjectTracker {
	// mapper 延迟构造：RESTMapper 只依赖 scheme，且创建成本较高。
	return &managedFieldObjectTracker{
		ObjectTracker:   NewObjectTracker(scheme, decoder),
		scheme:          scheme,
		objectConverter: scheme,
		mapper: func() meta.RESTMapper {
			return testrestmapper.TestOnlyStaticRESTMapper(scheme)
		},
		typeConverter: typeConverter,
	}
}

func (t *managedFieldObjectTracker) Create(gvr schema.GroupVersionResource, obj runtime.Object, ns string, vopts ...metav1.CreateOptions) error {
	opts, err := assertOptionalSingleArgument(vopts)
	if err != nil {
		return err
	}
	gvk, err := t.mapper().KindFor(gvr)
	if err != nil {
		return err
	}
	mgr, err := t.fieldManagerFor(gvk)
	if err != nil {
		return err
	}

	objType, err := meta.TypeAccessor(obj)
	if err != nil {
		return err
	}
	// 把 GVK 写回对象。
	apiVersion, kind := gvk.ToAPIVersionAndKind()
	objType.SetAPIVersion(apiVersion)
	objType.SetKind(kind)

	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	liveObject, err := t.ObjectTracker.Get(gvr, ns, objMeta.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		liveObject, err = t.scheme.New(gvk)
		if err != nil {
			return err
		}
		liveObject.GetObjectKind().SetGroupVersionKind(gvk)
	} else if err != nil {
		return err
	}
	objWithManagedFields, err := mgr.Update(liveObject, obj, opts.FieldManager)
	if err != nil {
		return err
	}
	return t.ObjectTracker.Create(gvr, objWithManagedFields, ns, opts)
}

func (t *managedFieldObjectTracker) Update(gvr schema.GroupVersionResource, obj runtime.Object, ns string, vopts ...metav1.UpdateOptions) error {
	opts, err := assertOptionalSingleArgument(vopts)
	if err != nil {
		return err
	}
	gvk, err := t.mapper().KindFor(gvr)
	if err != nil {
		return err
	}
	mgr, err := t.fieldManagerFor(gvk)
	if err != nil {
		return err
	}

	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	oldObj, err := t.ObjectTracker.Get(gvr, ns, objMeta.GetName(), metav1.GetOptions{})
	if err != nil {
		return err
	}
	objWithManagedFields, err := mgr.Update(oldObj, obj, opts.FieldManager)
	if err != nil {
		return err
	}

	return t.ObjectTracker.Update(gvr, objWithManagedFields, ns, opts)
}

func (t *managedFieldObjectTracker) Patch(gvr schema.GroupVersionResource, patchedObject runtime.Object, ns string, vopts ...metav1.PatchOptions) error {
	opts, err := assertOptionalSingleArgument(vopts)
	if err != nil {
		return err
	}
	gvk, err := t.mapper().KindFor(gvr)
	if err != nil {
		return err
	}
	mgr, err := t.fieldManagerFor(gvk)
	if err != nil {
		return err
	}

	objMeta, err := meta.Accessor(patchedObject)
	if err != nil {
		return err
	}
	oldObj, err := t.ObjectTracker.Get(gvr, ns, objMeta.GetName(), metav1.GetOptions{})
	if err != nil {
		return err
	}
	objWithManagedFields, err := mgr.Update(oldObj, patchedObject, opts.FieldManager)
	if err != nil {
		return err
	}
	return t.ObjectTracker.Patch(gvr, objWithManagedFields, ns, vopts...)
}

func (t *managedFieldObjectTracker) Apply(gvr schema.GroupVersionResource, applyConfiguration runtime.Object, ns string, vopts ...metav1.PatchOptions) error {
	opts, err := assertOptionalSingleArgument(vopts)
	if err != nil {
		return err
	}
	gvk, err := t.mapper().KindFor(gvr)
	if err != nil {
		return err
	}
	applyConfigurationMeta, err := meta.Accessor(applyConfiguration)
	if err != nil {
		return err
	}

	exists := true
	liveObject, err := t.ObjectTracker.Get(gvr, ns, applyConfigurationMeta.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		exists = false
		liveObject, err = t.scheme.New(gvk)
		if err != nil {
			return err
		}
		liveObject.GetObjectKind().SetGroupVersionKind(gvk)
	} else if err != nil {
		return err
	}
	mgr, err := t.fieldManagerFor(gvk)
	if err != nil {
		return err
	}
	force := false
	if opts.Force != nil {
		// apply 的 force 语义：为 true 时抢占他人拥有的冲突字段。
		force = *opts.Force
	}
	objWithManagedFields, err := mgr.Apply(liveObject, applyConfiguration, opts.FieldManager, force)
	if err != nil {
		return err
	}

	if !exists {
		return t.ObjectTracker.Create(gvr, objWithManagedFields, ns, metav1.CreateOptions{
			DryRun:          opts.DryRun,
			FieldManager:    opts.FieldManager,
			FieldValidation: opts.FieldValidation,
		})
	} else {
		return t.ObjectTracker.Update(gvr, objWithManagedFields, ns, metav1.UpdateOptions{
			DryRun:          opts.DryRun,
			FieldManager:    opts.FieldManager,
			FieldValidation: opts.FieldValidation,
		})
	}
}

func (t *managedFieldObjectTracker) fieldManagerFor(gvk schema.GroupVersionKind) (*managedfields.FieldManager, error) {
	// objectDefaulter 为空实现：fake client 不做默认值填充，避免测试依赖默认化行为。
	return managedfields.NewDefaultFieldManager(
		t.typeConverter,
		t.objectConverter,
		&objectDefaulter{},
		t.scheme,
		gvk,
		gvk.GroupVersion(),
		"",
		nil)
}

// objectDefaulter 实现 runtime.Defaulter，但实际不做任何默认值填充。
type objectDefaulter struct{}

func (d *objectDefaulter) Default(_ runtime.Object) {}

// filterByNamespace 返回集合中匹配给定命名空间的对象；空命名空间匹配非命名空间对象。
//
// 结果按 namespace、name 排序，使 List 返回值稳定（map 遍历本身是随机的）。
func filterByNamespace(objs map[types.NamespacedName]versionedObject, ns string) ([]versionedObject, error) {
	var res []versionedObject

	for _, obj := range objs {
		acc, err := meta.Accessor(obj.Object)
		if err != nil {
			return nil, err
		}
		if ns != "" && acc.GetNamespace() != ns {
			continue
		}
		res = append(res, obj)
	}

	// 排序以获得确定顺序。
	sort.Slice(res, func(i, j int) bool {
		acc1, _ := meta.Accessor(res[i].Object)
		acc2, _ := meta.Accessor(res[j].Object)
		if acc1.GetNamespace() != acc2.GetNamespace() {
			return acc1.GetNamespace() < acc2.GetNamespace()
		}
		return acc1.GetName() < acc2.GetName()
	})
	return res, nil
}

// DefaultWatchReactor 返回一个始终命中、直接返回给定 watch.Interface 的 WatchReactionFunc。
func DefaultWatchReactor(watchInterface watch.Interface, err error) WatchReactionFunc {
	return func(action Action) (bool, watch.Interface, error) {
		return true, watchInterface, err
	}
}

// SimpleReactor 把响应函数绑定到给定的 verb/resource 组合，任一字段用 "*" 通配，例如 *,pods 匹配针对 pods 的所有动词。
type SimpleReactor struct {
	Verb     string
	Resource string

	Reaction ReactionFunc
}

func (r *SimpleReactor) Handles(action Action) bool {
	// "*" 通配所有 verb；resource 匹配逻辑封装在 resourceCovers 中。
	verbCovers := r.Verb == "*" || r.Verb == action.GetVerb()
	if !verbCovers {
		return false
	}

	return resourceCovers(r.Resource, action)
}

func (r *SimpleReactor) React(action Action) (bool, runtime.Object, error) {
	return r.Reaction(action)
}

// SimpleWatchReactor 把响应函数绑定到给定资源，"*" 表示通配，便于组合响应逻辑。
type SimpleWatchReactor struct {
	Resource string

	Reaction WatchReactionFunc
}

func (r *SimpleWatchReactor) Handles(action Action) bool {
	return resourceCovers(r.Resource, action)
}

func (r *SimpleWatchReactor) React(action Action) (bool, watch.Interface, error) {
	return r.Reaction(action)
}

// SimpleProxyReactor 把响应函数绑定到给定资源，"*" 表示通配，便于组合响应逻辑。
type SimpleProxyReactor struct {
	Resource string

	Reaction ProxyReactionFunc
}

func (r *SimpleProxyReactor) Handles(action Action) bool {
	return resourceCovers(r.Resource, action)
}

func (r *SimpleProxyReactor) React(action Action) (bool, restclient.ResponseWrapper, error) {
	return r.Reaction(action)
}

// resourceCovers 判断 reactor 的 resource 声明是否匹配 action。
// 支持 "resource/subresource" 形式，也支持 "*" 通配。
func resourceCovers(resource string, action Action) bool {
	if resource == "*" {
		return true
	}

	if resource == action.GetResource().Resource {
		return true
	}

	if index := strings.Index(resource, "/"); index != -1 &&
		resource[:index] == action.GetResource().Resource &&
		resource[index+1:] == action.GetSubresource() {
		return true
	}

	return false
}

// assertOptionalSingleArgument 在可变参数多于一个时报错，否则返回第一个参数，无参数时返回零值。
func assertOptionalSingleArgument[T any](arguments []T) (T, error) {
	var a T
	switch len(arguments) {
	case 0:
		return a, nil
	case 1:
		return arguments[0], nil
	default:
		return a, fmt.Errorf("expected only one option argument but got %d", len(arguments))
	}
}
