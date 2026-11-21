/*
Copyright 2019 The Kubernetes Authors.

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
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/google/go-cmp/cmp"
	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
)

// ErrVersionConflict 在请求对象的资源版本与存储中对象冲突时返回。
var ErrVersionConflict = errors.New("VersionError")

// VolumeReactor 是模拟 etcd 和 API Server 的 core.Reactor。它保存：
//   - 控制器保存的卷/声明的最新版本。
//   - 所有保存操作的队列（用于模拟“卷/声明更新”事件）。队列包含对象的所有中间状态，
//     例如先更新 claim.VolumeName、再更新 claim.Phase，两个更新会作为独立条目入队。
//   - 自上次调用 VolumeReactor.syncAll() 以来的变更数。
//   - 可选的卷/声明 fake watcher，应与控制器使用的是同一批。
//     每当调用 deleteVolumeEvent 等事件函数模拟事件时，reactor 的存储会更新，
//     并通过 fake watcher 将事件发送给控制器。
//   - 可选的、由 reactor 返回的错误列表，用于模拟 etcd / API Server 故障。
//     这些错误按顺序匹配，每个只返回一次：匹配到 ReactorError 后返回相应错误，
//     并将其从列表中移除。
type VolumeReactor struct {
	volumes              map[string]*v1.PersistentVolume
	claims               map[string]*v1.PersistentVolumeClaim
	changedObjects       []interface{}
	changedSinceLastSync int
	fakeVolumeWatch      *watch.FakeWatcher
	fakeClaimWatch       *watch.FakeWatcher
	lock                 sync.RWMutex
	errors               []ReactorError
	watchers             map[schema.GroupVersionResource]map[string][]*watch.RaceFreeFakeWatcher
}

// ReactorError 定义测试 reactor（即模拟的 etcd/API Server）要返回的错误：
// 当 reactor 执行的动作在给定资源（"persistentvolumes"、"persistentvolumeclaims" 或 "*"）
// 上匹配给定动词（"get"、"update"、"create"、"delete" 或 "*"）时返回该错误。
type ReactorError struct {
	Verb     string
	Resource string
	Error    error
}

// React 是控制器通过 fake kubeClient 调用的回调，
// 即控制器对声明/卷的所有修改最终都会汇集到这里。
// 回调会检查被更新对象的版本，拒绝过旧的更新以模拟真实 etcd 的并发控制。
// 所有更新对象都会保存在本地，用于跟踪对象版本和校验测试结果；
// 同时进入 changedObjects 队列，并可选择通过 watcher 回传给控制器。
func (r *VolumeReactor) React(ctx context.Context, action core.Action) (handled bool, ret runtime.Object, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Reactor got operation", "resource", action.GetResource(), "verb", action.GetVerb())

	// 测试若要求注入错误，则优先返回，不再模拟实际的对象变更。
	err = r.injectReactError(ctx, action)
	if err != nil {
		return true, nil, err
	}

	// 未注入错误时继续模拟 API Server 的正常处理。
	switch {
	case action.Matches("create", "persistentvolumes"):
		obj := action.(core.UpdateAction).GetObject()
		volume := obj.(*v1.PersistentVolume)

		// 创建前先确认卷不存在，模拟 API Server 的唯一性约束。
		_, found := r.volumes[volume.Name]
		if found {
			return true, nil, fmt.Errorf("cannot create volume %s: volume already exists", volume.Name)
		}

		// 模拟 API Server 的默认值填充。
		if volume.Spec.VolumeMode == nil {
			volume.Spec.VolumeMode = new(v1.PersistentVolumeMode)
			*volume.Spec.VolumeMode = v1.PersistentVolumeFilesystem
		}

		// 将更新后的对象保存到存储、watcher 和变更队列，
		// 让控制器能观察到变更，测试也能据此断言。
		r.volumes[volume.Name] = volume
		for _, w := range r.getWatches(action.GetResource(), action.GetNamespace()) {
			w.Add(volume)
		}
		r.changedObjects = append(r.changedObjects, volume)
		r.changedSinceLastSync++
		logger.V(4).Info("Created volume", "volumeName", volume.Name)
		return true, volume, nil

	case action.Matches("create", "persistentvolumeclaims"):
		obj := action.(core.UpdateAction).GetObject()
		claim := obj.(*v1.PersistentVolumeClaim)

		// 创建前先确认声明不存在，模拟 API Server 的唯一性约束。
		_, found := r.claims[claim.Name]
		if found {
			return true, nil, fmt.Errorf("cannot create claim %s: claim already exists", claim.Name)
		}

		// 将更新后的对象保存到存储、watcher 和变更队列，
		// 让控制器能观察到变更，测试也能据此断言。
		r.claims[claim.Name] = claim
		for _, w := range r.getWatches(action.GetResource(), action.GetNamespace()) {
			w.Add(claim)
		}
		r.changedObjects = append(r.changedObjects, claim)
		r.changedSinceLastSync++
		logger.V(4).Info("Created claim", "PVC", klog.KObj(claim))
		return true, claim, nil

	case action.Matches("update", "persistentvolumes"):
		obj := action.(core.UpdateAction).GetObject()
		volume := obj.(*v1.PersistentVolume)

		// 校验版本号后递增，模拟 etcd 的乐观并发控制。
		storedVolume, found := r.volumes[volume.Name]
		if found {
			storedVer, _ := strconv.Atoi(storedVolume.ResourceVersion)
			requestedVer, _ := strconv.Atoi(volume.ResourceVersion)
			if storedVer != requestedVer {
				return true, obj, ErrVersionConflict
			}
			// 内容未变则直接返回，避免产生无意义的更新事件。
			if reflect.DeepEqual(storedVolume, volume) {
				logger.V(4).Info("Nothing updated volume", "volumeName", volume.Name)
				return true, volume, nil
			}
			// 深拷贝后再改版本号，避免修改调用方持有的对象。
			volume = volume.DeepCopy()
			volume.ResourceVersion = strconv.Itoa(storedVer + 1)
		} else {
			return true, nil, fmt.Errorf("cannot update volume %s: volume not found", volume.Name)
		}

		// 将更新后的对象保存到存储、watcher 和变更队列，
		// 让控制器能观察到变更，测试也能据此断言。
		for _, w := range r.getWatches(action.GetResource(), action.GetNamespace()) {
			w.Modify(volume)
		}
		r.volumes[volume.Name] = volume
		r.changedObjects = append(r.changedObjects, volume)
		r.changedSinceLastSync++
		logger.V(4).Info("Saved updated volume", "volumeName", volume.Name)
		return true, volume, nil

	case action.Matches("update", "persistentvolumeclaims"):
		obj := action.(core.UpdateAction).GetObject()
		claim := obj.(*v1.PersistentVolumeClaim)

		// 校验版本号后递增，模拟 etcd 的乐观并发控制。
		storedClaim, found := r.claims[claim.Name]
		if found {
			storedVer, _ := strconv.Atoi(storedClaim.ResourceVersion)
			requestedVer, _ := strconv.Atoi(claim.ResourceVersion)
			if storedVer != requestedVer {
				return true, obj, ErrVersionConflict
			}
			// 内容未变则直接返回，避免产生无意义的更新事件。
			if reflect.DeepEqual(storedClaim, claim) {
				logger.V(4).Info("Nothing updated claim", "PVC", klog.KObj(claim))
				return true, claim, nil
			}
			// 深拷贝后再改版本号，避免修改调用方持有的对象。
			claim = claim.DeepCopy()
			claim.ResourceVersion = strconv.Itoa(storedVer + 1)
		} else {
			return true, nil, fmt.Errorf("cannot update claim %s: claim not found", claim.Name)
		}

		// 将更新后的对象保存到存储、watcher 和变更队列，
		// 让控制器能观察到变更，测试也能据此断言。
		for _, w := range r.getWatches(action.GetResource(), action.GetNamespace()) {
			w.Modify(claim)
		}
		r.claims[claim.Name] = claim
		r.changedObjects = append(r.changedObjects, claim)
		r.changedSinceLastSync++
		logger.V(4).Info("Saved updated claim", "PVC", klog.KObj(claim))
		return true, claim, nil

	case action.Matches("get", "persistentvolumes"):
		name := action.(core.GetAction).GetName()
		volume, found := r.volumes[name]
		if found {
			logger.V(4).Info("GetVolume: found volume", "volumeName", volume.Name)
			return true, volume.DeepCopy(), nil
		}
		logger.V(4).Info("GetVolume: volume not found", "volumeName", name)
		return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), name)

	case action.Matches("get", "persistentvolumeclaims"):
		name := action.(core.GetAction).GetName()
		nameSpace := action.(core.GetAction).GetNamespace()
		claim, found := r.claims[name]
		if found {
			logger.V(4).Info("GetClaim: found claim", "PVC", klog.KObj(claim))
			return true, claim.DeepCopy(), nil
		}
		logger.V(4).Info("GetClaim: claim not found", "PVC", klog.KRef(nameSpace, name))
		return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), name)

	case action.Matches("delete", "persistentvolumes"):
		name := action.(core.DeleteAction).GetName()
		logger.V(4).Info("Deleted volume", "volumeName", name)
		obj, found := r.volumes[name]
		if found {
			delete(r.volumes, name)
			for _, w := range r.getWatches(action.GetResource(), action.GetNamespace()) {
				w.Delete(obj)
			}
			r.changedSinceLastSync++
			return true, nil, nil
		}
		return true, nil, fmt.Errorf("cannot delete volume %s: not found", name)

	case action.Matches("delete", "persistentvolumeclaims"):
		name := action.(core.DeleteAction).GetName()
		nameSpace := action.(core.DeleteAction).GetNamespace()
		logger.V(4).Info("Deleted claim", "PVC", klog.KRef(nameSpace, name))
		obj, found := r.claims[name]
		if found {
			delete(r.claims, name)
			for _, w := range r.getWatches(action.GetResource(), action.GetNamespace()) {
				w.Delete(obj)
			}
			r.changedSinceLastSync++
			return true, nil, nil
		}
		return true, nil, fmt.Errorf("cannot delete claim %s: not found", name)
	}

	return false, nil, nil
}

// Watch 监听 VolumeReactor 中的对象，返回的 watcher 会推送新增/修改/删除事件。
func (r *VolumeReactor) Watch(gvr schema.GroupVersionResource, ns string) (watch.Interface, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	fakewatcher := watch.NewRaceFreeFake()

	if _, exists := r.watchers[gvr]; !exists {
		r.watchers[gvr] = make(map[string][]*watch.RaceFreeFakeWatcher)
	}
	r.watchers[gvr][ns] = append(r.watchers[gvr][ns], fakewatcher)
	return fakewatcher, nil
}

// getWatches 返回指定命名空间以及全命名空间范围内的所有 watcher，
// 与 API Server 的跨命名空间监听语义一致。
func (r *VolumeReactor) getWatches(gvr schema.GroupVersionResource, ns string) []*watch.RaceFreeFakeWatcher {
	watches := []*watch.RaceFreeFakeWatcher{}
	if r.watchers[gvr] != nil {
		if w := r.watchers[gvr][ns]; w != nil {
			watches = append(watches, w...)
		}
		// 全命名空间 watcher 也能收到带命名空间的变更，避免事件漏发。
		if ns != metav1.NamespaceAll {
			if w := r.watchers[gvr][metav1.NamespaceAll]; w != nil {
				watches = append(watches, w...)
			}
		}
	}
	return watches
}

// injectReactError 在测试要求指定动作失败时返回相应错误，否则返回 nil；
// 每个注入错误只生效一次，匹配后即从列表中移除。
func (r *VolumeReactor) injectReactError(ctx context.Context, action core.Action) error {
	if len(r.errors) == 0 {
		// 没有待注入的错误，后续操作应全部成功。
		return nil
	}
	logger := klog.FromContext(ctx)
	for i, expected := range r.errors {
		logger.V(4).Info("Trying to match resource verb", "resource", action.GetResource(), "verb", action.GetVerb(), "expectedResource", expected.Resource, "expectedVerb", expected.Verb)
		if action.Matches(expected.Verb, expected.Resource) {
			// 匹配到目标动作后将其移除，保证该错误只注入一次。
			r.errors = append(r.errors[:i], r.errors[i+1:]...)
			logger.V(4).Info("Reactor found matching error", "index", i, "expectedResource", expected.Resource, "expectedVerb", expected.Verb, "err", expected.Error)
			return expected.Error
		}
	}
	return nil
}

// CheckVolumes 比较测试结束时的实际卷集合与期望卷集合，并报告差异。
func (r *VolumeReactor) CheckVolumes(expectedVolumes []*v1.PersistentVolume) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	expectedMap := make(map[string]*v1.PersistentVolume)
	gotMap := make(map[string]*v1.PersistentVolume)
	// 版本号由 reactor 自动生成，比较前清除以聚焦业务字段。
	for _, v := range expectedVolumes {
		// 深拷贝以免修改测试持有的期望对象。
		v := v.DeepCopy()
		v.ResourceVersion = ""
		if v.Spec.ClaimRef != nil {
			v.Spec.ClaimRef.ResourceVersion = ""
		}
		expectedMap[v.Name] = v
	}
	for _, v := range r.volumes {
		// 控制器可能无锁写入这些对象；深拷贝以避免竞态检测报警。
		v := v.DeepCopy()
		v.ResourceVersion = ""
		if v.Spec.ClaimRef != nil {
			v.Spec.ClaimRef.ResourceVersion = ""
		}
		gotMap[v.Name] = v
	}
	if !reflect.DeepEqual(expectedMap, gotMap) {
		// 输出详细 diff，便于定位不一致的字段。
		return fmt.Errorf("Volume check failed [A-expected, B-got]: %s", cmp.Diff(expectedMap, gotMap))
	}
	return nil
}

// CheckClaims 比较测试结束时的实际声明集合与期望声明集合，并报告差异。
func (r *VolumeReactor) CheckClaims(expectedClaims []*v1.PersistentVolumeClaim) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	expectedMap := make(map[string]*v1.PersistentVolumeClaim)
	gotMap := make(map[string]*v1.PersistentVolumeClaim)
	for _, c := range expectedClaims {
		// 深拷贝以免修改测试持有的期望对象。
		c = c.DeepCopy()
		c.ResourceVersion = ""
		expectedMap[c.Name] = c
	}
	for _, c := range r.claims {
		// 控制器可能无锁写入这些对象；深拷贝以避免竞态检测报警。
		c = c.DeepCopy()
		c.ResourceVersion = ""
		gotMap[c.Name] = c
	}
	if !reflect.DeepEqual(expectedMap, gotMap) {
		// 输出详细 diff，便于定位不一致的字段。
		return fmt.Errorf("Claim check failed [A-expected, B-got result]: %s", cmp.Diff(expectedMap, gotMap))
	}
	return nil
}

// PopChange 取出一个记录的更新对象（*v1.PersistentVolume 或 *v1.PersistentVolumeClaim），
// 没有变更时返回 nil。
func (r *VolumeReactor) PopChange(ctx context.Context) interface{} {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.changedObjects) == 0 {
		return nil
	}

	// 打印队列内容，便于调试事件顺序。
	logger := klog.FromContext(ctx)
	for _, obj := range r.changedObjects {
		switch obj.(type) {
		case *v1.PersistentVolume:
			vol, _ := obj.(*v1.PersistentVolume)
			logger.V(4).Info("Reactor queue", "volumeName", vol.Name)
		case *v1.PersistentVolumeClaim:
			claim, _ := obj.(*v1.PersistentVolumeClaim)
			logger.V(4).Info("Reactor queue", "PVC", klog.KObj(claim))
		}
	}

	// 按入队顺序取出最早的变更，保证事件顺序可预测。
	obj := r.changedObjects[0]
	r.changedObjects = r.changedObjects[1:]
	return obj
}

// SyncAll 模拟控制器的周期性同步：把所有卷和声明加入内部更新队列。
// 测试手动调用 syncClaim/syncVolume 时应使用该方法；
// 使用真实控制器循环（ctrl.Run()）的测试会自动获得周期同步。
func (r *VolumeReactor) SyncAll() {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, c := range r.claims {
		r.changedObjects = append(r.changedObjects, c)
	}
	for _, v := range r.volumes {
		r.changedObjects = append(r.changedObjects, v)
	}
	// 同步后重置计数，便于统计下一周期的变更数。
	r.changedSinceLastSync = 0
}

// GetChangeCount 返回自上次同步以来的变更数。
func (r *VolumeReactor) GetChangeCount() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.changedSinceLastSync
}

// DeleteVolumeEvent 模拟卷在 etcd 中被删除，并向控制器发送“卷已删除”事件。
func (r *VolumeReactor) DeleteVolumeEvent(volume *v1.PersistentVolume) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 从最终卷列表中移除。
	delete(r.volumes, volume.Name)

	// 发送删除事件时传入深拷贝，避免竞态（从 etcd 拿到的也会是副本）。
	if r.fakeVolumeWatch != nil {
		r.fakeVolumeWatch.Delete(volume.DeepCopy())
	}
}

// DeleteClaimEvent 模拟声明在 etcd 中被删除，并向控制器发送“声明已删除”事件。
func (r *VolumeReactor) DeleteClaimEvent(claim *v1.PersistentVolumeClaim) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 从最终声明列表中移除。
	delete(r.claims, claim.Name)

	// 发送删除事件时传入深拷贝，避免竞态（从 etcd 拿到的也会是副本）。
	if r.fakeClaimWatch != nil {
		r.fakeClaimWatch.Delete(claim.DeepCopy())
	}
}

// AddClaimEvent 模拟新声明写入 etcd，并向控制器发送“声明已添加”事件。
func (r *VolumeReactor) AddClaimEvent(claim *v1.PersistentVolumeClaim) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.claims[claim.Name] = claim
	// 该声明还未进入控制器缓存，不存在共享写入，无需深拷贝。
	if r.fakeClaimWatch != nil {
		r.fakeClaimWatch.Add(claim)
	}
}

// AddClaims 将一组 PVC 加入 VolumeReactor。
func (r *VolumeReactor) AddClaims(claims []*v1.PersistentVolumeClaim) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, claim := range claims {
		r.claims[claim.Name] = claim
	}
}

// AddVolumes 将一组 PV 加入 VolumeReactor。
func (r *VolumeReactor) AddVolumes(volumes []*v1.PersistentVolume) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, volume := range volumes {
		r.volumes[volume.Name] = volume
	}
}

// AddClaim 将一个 PVC 加入 VolumeReactor。
func (r *VolumeReactor) AddClaim(claim *v1.PersistentVolumeClaim) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.claims[claim.Name] = claim
}

// AddVolume 将一个 PV 加入 VolumeReactor。
func (r *VolumeReactor) AddVolume(volume *v1.PersistentVolume) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.volumes[volume.Name] = volume
}

// DeleteVolume 按名称删除一个 PV。
func (r *VolumeReactor) DeleteVolume(name string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.volumes, name)
}

// AddClaimBoundToVolume 加入一个 PVC，并将其绑定的 PV 状态置为 Bound。
func (r *VolumeReactor) AddClaimBoundToVolume(claim *v1.PersistentVolumeClaim) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.claims[claim.Name] = claim
	// PV 存在时同步更新其状态为 Bound，保持与 PVC 的绑定关系一致。
	if volume, ok := r.volumes[claim.Spec.VolumeName]; ok {
		volume.Status.Phase = v1.VolumeBound
	}
}

// MarkVolumeAvailable 按名称将 PV 标记为 Available，清除绑定引用和注解。
func (r *VolumeReactor) MarkVolumeAvailable(name string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if volume, ok := r.volumes[name]; ok {
		// 释放卷需清除声明引用与注解，避免残留信息影响绑定判断。
		volume.Spec.ClaimRef = nil
		volume.Status.Phase = v1.VolumeAvailable
		volume.Annotations = nil
	}
}

// NewVolumeReactor 创建 VolumeReactor，并向 fake client 注册所有卷/声明动作的回调。
func NewVolumeReactor(ctx context.Context, client *fake.Clientset, fakeVolumeWatch, fakeClaimWatch *watch.FakeWatcher, errors []ReactorError) *VolumeReactor {
	reactor := &VolumeReactor{
		volumes:         make(map[string]*v1.PersistentVolume),
		claims:          make(map[string]*v1.PersistentVolumeClaim),
		fakeVolumeWatch: fakeVolumeWatch,
		fakeClaimWatch:  fakeClaimWatch,
		errors:          errors,
		watchers:        make(map[schema.GroupVersionResource]map[string][]*watch.RaceFreeFakeWatcher),
	}
	client.AddReactor("create", "persistentvolumes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})

	client.AddReactor("create", "persistentvolumeclaims", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})
	client.AddReactor("update", "persistentvolumes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})
	client.AddReactor("update", "persistentvolumeclaims", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})
	client.AddReactor("get", "persistentvolumes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})
	client.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})
	client.AddReactor("delete", "persistentvolumes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})
	client.AddReactor("delete", "persistentvolumeclaims", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		return reactor.React(ctx, action)
	})
	return reactor
}
