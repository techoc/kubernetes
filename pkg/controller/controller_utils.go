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

package controller

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	clientretry "k8s.io/client-go/util/retry"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
	taintutils "k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	"k8s.io/klog/v2"
)

const (
	// watch 若丢失 Pod 的删除事件，休眠中的控制器最迟会在此时限后被唤醒。
	// 专门应对期望更新受阻的情况：否则控制器可能永久休眠；取值应参考 watch 事件的预期延迟。
	//
	// 当前控制器每秒约能处理（创建并观察其 watch 事件）10 个 Pod，处理 500 个约需 1 分钟；
	// 纯创建受 20qps 限制，而在 100 节点、3000 Pod 的规模下，watch 延迟约为每 Pod 10-30 秒。
	ExpectationsTimeout = 5 * time.Minute
	// 批量创建 Pod 时，SlowStartInitialBatchSize 为首批大小，后续每批翻倍。
	// 例如取 1 时批大小为 1、2、4、8……；取 10 时为 10、20、40、80……。
	// 取值偏大会在配额不足时产生更多注定失败的 API 调用和事件噪音；偏小则大批量需更多往返。
	//
	// 设待启动 Pod 数为 N：
	// 配额耗尽后每次同步注定失败的调用数为：
	//      min(N,SlowStartInitialBatchSize)
	// 批次数为：
	//      1+floor(log_2(ceil(N/SlowStartInitialBatchSize)))
	SlowStartInitialBatchSize = 1

	// PodNodeNameKeyIndex 是 PodInformer 按节点名索引 Pod 所用的索引名。
	PodNodeNameKeyIndex = "spec.nodeName"

	// PodControllerIndex 是 Pod store 索引函数名，按 PodControllerIndexKey 返回的键索引。
	PodControllerIndex = "podController"
)

var UpdateTaintBackoff = wait.Backoff{
	Steps:    5,
	Duration: 100 * time.Millisecond,
	Jitter:   1.0,
}

var UpdateLabelBackoff = wait.Backoff{
	Steps:    5,
	Duration: 100 * time.Millisecond,
	Jitter:   1.0,
}

var (
	KeyFunc           = cache.DeletionHandlingMetaNamespaceKeyFunc
	podPhaseToOrdinal = map[v1.PodPhase]int{v1.PodPending: 0, v1.PodUnknown: 1, v1.PodRunning: 2}
)

type ResyncPeriodFunc func() time.Duration

// NoResyncPeriodFunc 在无需重新同步时返回 0 作为同步周期。
func NoResyncPeriodFunc() time.Duration {
	return 0
}

// StaticResyncPeriodFunc 返回固定重同步周期的工厂函数。
func StaticResyncPeriodFunc(resyncPeriod time.Duration) ResyncPeriodFunc {
	return func() time.Duration {
		return resyncPeriod
	}
}

// Expectations 是控制器向控制器管理器声明预期的方式，例如：
//	ControllerExpectations: {
//		controller1: 2 分钟内预期新增 2 个
//		controller2: 2 分钟内预期删除 2 个
//		controller3: 2 分钟内预期新增 -1 个 => controller3 的预期已满足
//	}
//
// 实现：
//	ControlleeExpectation = 一对原子计数，跟踪下属对象的创建/删除
//	ControllerExpectationsStore = TTLStore + 每个控制器一个 ControlleeExpectation
//
// * 期望一经设置只能减少
// * 期望被满足或过期前，控制器不会被同步
// * 未设置期望的控制器，每出现一个匹配的下属对象都会被唤醒

// ExpKeyFunc 从 ControlleeExpectation 中解析出键。
var ExpKeyFunc = func(obj interface{}) (string, error) {
	if e, ok := obj.(*ControlleeExpectations); ok {
		return e.key, nil
	}
	return "", fmt.Errorf("could not find key for obj %#v", obj)
}

// ControllerExpectationsInterface 允许设置并等待期望，抽象成接口仅为方便测试。
// 注意：若使用 KeyFunc，不要让不同类型的控制器共用一个 ControllerExpectationsInterface，
// 因为它们的键可能冲突。
type ControllerExpectationsInterface interface {
	GetExpectations(controllerKey string) (*ControlleeExpectations, bool, error)
	SatisfiedExpectations(logger klog.Logger, controllerKey string) bool
	DeleteExpectations(logger klog.Logger, controllerKey string)
	SetExpectations(logger klog.Logger, controllerKey string, add, del int) error
	ExpectCreations(logger klog.Logger, controllerKey string, adds int) error
	ExpectDeletions(logger klog.Logger, controllerKey string, dels int) error
	CreationObserved(logger klog.Logger, controllerKey string)
	DeletionObserved(logger klog.Logger, controllerKey string)
	RaiseExpectations(logger klog.Logger, controllerKey string, add, del int)
	LowerExpectations(logger klog.Logger, controllerKey string, add, del int)
}

// ControllerExpectations 是缓存，记录各控制器被唤醒同步前预期观察到的事件。
type ControllerExpectations struct {
	cache.Store
}

// GetExpectations 返回指定控制器的 ControlleeExpectations。
func (r *ControllerExpectations) GetExpectations(controllerKey string) (*ControlleeExpectations, bool, error) {
	exp, exists, err := r.GetByKey(controllerKey)
	if err == nil && exists {
		return exp.(*ControlleeExpectations), true, nil
	}
	return nil, false, err
}

// DeleteExpectations 从 TTLStore 删除指定控制器的期望。
func (r *ControllerExpectations) DeleteExpectations(logger klog.Logger, controllerKey string) {
	if exp, exists, err := r.GetByKey(controllerKey); err == nil && exists {
		if err := r.Delete(exp); err != nil {

			logger.V(2).Info("Error deleting expectations", "controller", controllerKey, "err", err)
		}
	}
}

// SatisfiedExpectations 在指定控制器所需的新增/删除事件都被观察到时返回 true。
// 增减计数由控制器在同步时设定，并随控制器管理器观察到下属对象而更新。
func (r *ControllerExpectations) SatisfiedExpectations(logger klog.Logger, controllerKey string) bool {
	if exp, exists, err := r.GetExpectations(controllerKey); exists {
		if exp.Fulfilled() {
			logger.V(4).Info("Controller expectations fulfilled", "expectations", exp)
			return true
		} else if exp.isExpired() {
			logger.V(4).Info("Controller expectations expired", "expectations", exp)
			return true
		} else {
			logger.V(4).Info("Controller still waiting on expectations", "expectations", exp)
			return false
		}
	} else if err != nil {
		logger.V(2).Info("Error encountered while checking expectations, forcing sync", "err", err)
	} else {
		// 新控制器没有期望；超过 TTL 未看到预期 watch 事件时期望过期，
		// 此时它会被唤醒、创建/删除下属对象并重新设置期望。
		// 期望已满足且超过 TTL 无需新建/销毁时同样过期，
		// 此后在真正需要创建/删除前不再设置期望。
		logger.V(4).Info("Controller either never recorded expectations, or the ttl expired", "controller", controllerKey)
	}
	// 从本地 store 读取不应出错；一旦出错或控制器尚未建立期望，就触发一次同步兜底。
	return true
}

// isExpired 判断期望是否已超过超时时限。
// TODO: 扩展 ExpirationCache 以支持显式过期。
// TODO: 支持在测试中禁用该行为。
// TODO: 支持注入时钟。
func (exp *ControlleeExpectations) isExpired() bool {
	return clock.RealClock{}.Since(exp.timestamp) > ExpectationsTimeout
}

// SetExpectations 为指定控制器登记新期望，并覆盖旧期望。
func (r *ControllerExpectations) SetExpectations(logger klog.Logger, controllerKey string, add, del int) error {
	exp := &ControlleeExpectations{add: int64(add), del: int64(del), key: controllerKey, timestamp: clock.RealClock{}.Now()}
	logger.V(4).Info("Setting expectations", "expectations", exp)
	return r.Add(exp)
}

// ExpectCreations 登记指定控制器的新增期望，删除期望置零。
func (r *ControllerExpectations) ExpectCreations(logger klog.Logger, controllerKey string, adds int) error {
	return r.SetExpectations(logger, controllerKey, adds, 0)
}

// ExpectDeletions 登记指定控制器的删除期望，新增期望置零。
func (r *ControllerExpectations) ExpectDeletions(logger klog.Logger, controllerKey string, dels int) error {
	return r.SetExpectations(logger, controllerKey, 0, dels)
}

// LowerExpectations 减少指定控制器的期望计数。
func (r *ControllerExpectations) LowerExpectations(logger klog.Logger, controllerKey string, add, del int) {
	if exp, exists, err := r.GetExpectations(controllerKey); err == nil && exists {
		exp.Add(int64(-add), int64(-del))
		// 读取期望与这里的更新之间有并发窗口，日志中的值可能已被其他 goroutine 改动。
		logger.V(4).Info("Lowered expectations", "expectations", exp)
	}
}

// RaiseExpectations 增加指定控制器的期望计数。
func (r *ControllerExpectations) RaiseExpectations(logger klog.Logger, controllerKey string, add, del int) {
	if exp, exists, err := r.GetExpectations(controllerKey); err == nil && exists {
		exp.Add(int64(add), int64(del))
		// 读取期望与这里的更新之间有并发窗口，日志中的值可能已被其他 goroutine 改动。
		logger.V(4).Info("Raised expectations", "expectations", exp)
	}
}

// CreationObserved 原子地减少指定控制器的新增期望计数。
func (r *ControllerExpectations) CreationObserved(logger klog.Logger, controllerKey string) {
	r.LowerExpectations(logger, controllerKey, 1, 0)
}

// DeletionObserved 原子地减少指定控制器的删除期望计数。
func (r *ControllerExpectations) DeletionObserved(logger klog.Logger, controllerKey string) {
	r.LowerExpectations(logger, controllerKey, 0, 1)
}

// ControlleeExpectations 跟踪下属对象的创建/删除期望。
type ControlleeExpectations struct {
	// 重要：这两个 int64 字段用于 sync/atomic，受 32 位平台对齐限制必须放在结构体开头，
	// 详见 https://golang.org/pkg/sync/atomic/。
	add       int64
	del       int64
	key       string
	timestamp time.Time
}

// Add 原子地增加新增和删除计数。
func (e *ControlleeExpectations) Add(add, del int64) {
	atomic.AddInt64(&e.add, add)
	atomic.AddInt64(&e.del, del)
}

// Fulfilled 在新增和删除期望均已满足（计数不大于零）时返回 true。
func (e *ControlleeExpectations) Fulfilled() bool {
	// TODO: 思考为何此处使用原子操作并不影响结果。
	return atomic.LoadInt64(&e.add) <= 0 && atomic.LoadInt64(&e.del) <= 0
}

// GetExpectations 原子读取当前的新增和删除期望计数。
func (e *ControlleeExpectations) GetExpectations() (int64, int64) {
	return atomic.LoadInt64(&e.add), atomic.LoadInt64(&e.del)
}

// MarshalLog 对期望值做线程安全拷贝，供日志安全读取。
func (e *ControlleeExpectations) MarshalLog() interface{} {
	return struct {
		add int64
		del int64
		key string
	}{
		add: atomic.LoadInt64(&e.add),
		del: atomic.LoadInt64(&e.del),
		key: e.key,
	}
}

// NewControllerExpectations 创建存储 ControllerExpectations 的 store。
func NewControllerExpectations() *ControllerExpectations {
	return &ControllerExpectations{cache.NewStore(ExpKeyFunc)}
}

// UIDSetKeyFunc 从 UIDSet 中解析出键。
var UIDSetKeyFunc = func(obj interface{}) (string, error) {
	if u, ok := obj.(*UIDSet); ok {
		return u.key, nil
	}
	return "", fmt.Errorf("could not find key for obj %#v", obj)
}

// UIDSet 保存键和 UID 集合，供 UIDTrackingControllerExpectations 记录
// 已观察到或仍在等待的 UID。
type UIDSet struct {
	sets.String
	key string
}

// UIDTrackingControllerExpectations 跟踪被删除 Pod 的 UID。
// 相比普通期望，额外的缓存是为安全处理优雅删除：把设置 DeletionTimestamp 的
// 更新视作删除，只有记住预期删除的 UID 才能避免重复计数。
// TODO: 同时跟踪创建事件（#22599）。
type UIDTrackingControllerExpectations struct {
	ControllerExpectationsInterface
	// TODO: 可改用单个 store、每项一把锁和 ControlleeExpectationsInterface 类型，实现会更简洁。
	uidStoreLock sync.Mutex
	// 存放与 ControllerExpectationsInterface 所跟踪期望关联的 UID。
	uidStore cache.Store
}

// GetUIDs 避免直接暴露期望 UID 集合；返回的集合非线程安全，
// 所有修改都必须在持有 uidStoreLock 时进行。
func (u *UIDTrackingControllerExpectations) GetUIDs(controllerKey string) sets.String {
	if uid, exists, err := u.uidStore.GetByKey(controllerKey); err == nil && exists {
		return uid.(*UIDSet).String
	}
	return nil
}

// ExpectDeletions 为指定控制器登记待删除 UID 集合对应的删除期望。
func (u *UIDTrackingControllerExpectations) ExpectDeletions(logger klog.Logger, rcKey string, deletedKeys []string) error {
	expectedUIDs := sets.NewString()
	for _, k := range deletedKeys {
		expectedUIDs.Insert(k)
	}
	logger.V(4).Info("Controller waiting on deletions", "controller", rcKey, "keys", deletedKeys)
	u.uidStoreLock.Lock()
	defer u.uidStoreLock.Unlock()

	if existing := u.GetUIDs(rcKey); existing != nil && existing.Len() != 0 {
		// 同一控制器再次登记会覆盖旧 UID 集合，记录日志便于发现重复设置。
		logger.Error(nil, "Clobbering existing delete keys", "keys", existing)
	}
	if err := u.uidStore.Add(&UIDSet{expectedUIDs, rcKey}); err != nil {
		return err
	}
	return u.ControllerExpectationsInterface.ExpectDeletions(logger, rcKey, expectedUIDs.Len())
}

// DeletionObserved 为指定 rc 记录一次 UID 删除，并递减对应期望。
func (u *UIDTrackingControllerExpectations) DeletionObserved(logger klog.Logger, rcKey, deleteKey string) {
	u.uidStoreLock.Lock()
	defer u.uidStoreLock.Unlock()

	uids := u.GetUIDs(rcKey)
	// 只对该 UID 首次出现的删除递减期望，避免优雅删除时重复计数。
	if uids != nil && uids.Has(deleteKey) {
		logger.V(4).Info("Controller received delete for pod", "controller", rcKey, "key", deleteKey)
		u.ControllerExpectationsInterface.DeletionObserved(logger, rcKey)
		uids.Delete(deleteKey)
	}
}

// DeleteExpectations 删除 UID 集合，并调用底层 ControllerExpectationsInterface 的同名方法。
func (u *UIDTrackingControllerExpectations) DeleteExpectations(logger klog.Logger, rcKey string) {
	u.uidStoreLock.Lock()
	defer u.uidStoreLock.Unlock()

	u.ControllerExpectationsInterface.DeleteExpectations(logger, rcKey)
	if uidExp, exists, err := u.uidStore.GetByKey(rcKey); err == nil && exists {
		if err := u.uidStore.Delete(uidExp); err != nil {
			logger.V(2).Info("Error deleting uid expectations", "controller", rcKey, "err", err)
		}
	}
}

// NewUIDTrackingControllerExpectations 返回能感知 deleteKeys 的 ControllerExpectations 包装器。
func NewUIDTrackingControllerExpectations(ce ControllerExpectationsInterface) *UIDTrackingControllerExpectations {
	return &UIDTrackingControllerExpectations{ControllerExpectationsInterface: ce, uidStore: cache.NewStore(UIDSetKeyFunc)}
}

// Pod 事件的原因枚举。
const (
	// FailedCreatePodReason 用于副本集 Pod 创建失败时的事件和 condition。
	FailedCreatePodReason = "FailedCreate"
	// SuccessfulCreatePodReason 用于副本集 Pod 创建成功的事件。
	SuccessfulCreatePodReason = "SuccessfulCreate"
	// FailedDeletePodReason 用于副本集 Pod 删除失败时的事件和 condition。
	FailedDeletePodReason = "FailedDelete"
	// SuccessfulDeletePodReason 用于副本集 Pod 删除成功的事件。
	SuccessfulDeletePodReason = "SuccessfulDelete"
)

// RSControlInterface 抽象 ReplicaSet 的增删和扩缩，
// 供 deployment 控制器注入假实现以测试其行为。
type RSControlInterface interface {
	PatchReplicaSet(ctx context.Context, namespace, name string, data []byte) error
}

// RealRSControl 是 RSControlInterface 的默认实现。
type RealRSControl struct {
	KubeClient clientset.Interface
	Recorder   record.EventRecorder
}

var _ RSControlInterface = &RealRSControl{}

// PatchReplicaSet 用策略合并补丁更新指定 ReplicaSet。
func (r RealRSControl) PatchReplicaSet(ctx context.Context, namespace, name string, data []byte) error {
	_, err := r.KubeClient.AppsV1().ReplicaSets(namespace).Patch(ctx, name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
	return err
}

// TODO: 将 controller_history.go 中的 ControllerRevision 接口与此接口合并。
// ControllerRevisionControlInterface 抽象 ControllerRevision 的补丁更新和扩缩，
// 供 daemonset 控制器注入假实现以测试其行为。
type ControllerRevisionControlInterface interface {
	PatchControllerRevision(ctx context.Context, namespace, name string, data []byte) error
}

// RealControllerRevisionControl 是 ControllerRevisionControlInterface 的默认实现。
type RealControllerRevisionControl struct {
	KubeClient clientset.Interface
}

var _ ControllerRevisionControlInterface = &RealControllerRevisionControl{}

// PatchControllerRevision 用策略合并补丁更新指定 ControllerRevision。
func (r RealControllerRevisionControl) PatchControllerRevision(ctx context.Context, namespace, name string, data []byte) error {
	_, err := r.KubeClient.AppsV1().ControllerRevisions(namespace).Patch(ctx, name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
	return err
}

// PodControlInterface 抽象 Pod 的创建/删除/补丁，抽象出来是为了便于测试。
type PodControlInterface interface {
	// CreatePods 按模板创建 Pod，并将 object 设为 Pod 的控制器。
	CreatePods(ctx context.Context, namespace string, template *v1.PodTemplateSpec, object runtime.Object, controllerRef *metav1.OwnerReference) error
	// CreatePodsWithGenerateName 按模板创建 Pod，设置控制器并指定 generateName。
	CreatePodsWithGenerateName(ctx context.Context, namespace string, template *v1.PodTemplateSpec, object runtime.Object, controllerRef *metav1.OwnerReference, generateName string) error
	// DeletePod 删除 podID 指定的 Pod。
	DeletePod(ctx context.Context, namespace string, podID string, object runtime.Object) error
	// PatchPod 给 Pod 打补丁。
	PatchPod(ctx context.Context, namespace, name string, data []byte) error
}

// RealPodControl 是 PodControlInterface 的默认实现。
type RealPodControl struct {
	KubeClient clientset.Interface
	Recorder   record.EventRecorder
	OnWrite    func(*v1.Pod, *metav1.OwnerReference)
}

var _ PodControlInterface = &RealPodControl{}

// getPodsLabelSet 拷贝模板标签，避免与模板共享 map。
func getPodsLabelSet(template *v1.PodTemplateSpec) labels.Set {
	desiredLabels := make(labels.Set)
	for k, v := range template.Labels {
		desiredLabels[k] = v
	}
	return desiredLabels
}

// getPodsFinalizers 拷贝模板 finalizer，避免与模板共享切片。
func getPodsFinalizers(template *v1.PodTemplateSpec) []string {
	desiredFinalizers := make([]string, len(template.Finalizers))
	copy(desiredFinalizers, template.Finalizers)
	return desiredFinalizers
}

// getPodsAnnotationSet 拷贝模板注解，避免与模板共享 map。
func getPodsAnnotationSet(template *v1.PodTemplateSpec) labels.Set {
	desiredAnnotations := make(labels.Set)
	for k, v := range template.Annotations {
		desiredAnnotations[k] = v
	}
	return desiredAnnotations
}

// getPodsPrefix 生成 Pod 名称前缀，名字允许时用连字符让名称更可读。
func getPodsPrefix(controllerName string) string {
	// 带连字符的前缀若超过 Pod 名称长度限制则退回控制器原名。
	prefix := fmt.Sprintf("%s-", controllerName)
	if len(validation.ValidatePodName(prefix, true)) != 0 {
		prefix = controllerName
	}
	return prefix
}

// validateControllerRef 在创建 Pod 前校验 controllerRef 字段完整，防止产生无主或无法级联删除的对象。
func validateControllerRef(controllerRef *metav1.OwnerReference) error {
	if controllerRef == nil {
		return fmt.Errorf("controllerRef is nil")
	}
	if len(controllerRef.APIVersion) == 0 {
		return fmt.Errorf("controllerRef has empty APIVersion")
	}
	if len(controllerRef.Kind) == 0 {
		return fmt.Errorf("controllerRef has empty Kind")
	}
	if controllerRef.Controller == nil || !*controllerRef.Controller {
		return fmt.Errorf("controllerRef.Controller is not set to true")
	}
	if controllerRef.BlockOwnerDeletion == nil || !*controllerRef.BlockOwnerDeletion {
		return fmt.Errorf("controllerRef.BlockOwnerDeletion is not set")
	}
	return nil
}

// CreatePods 按模板创建 Pod，generateName 留空由控制器名生成。
func (r RealPodControl) CreatePods(ctx context.Context, namespace string, template *v1.PodTemplateSpec, controllerObject runtime.Object, controllerRef *metav1.OwnerReference) error {
	return r.CreatePodsWithGenerateName(ctx, namespace, template, controllerObject, controllerRef, "")
}

// CreatePodsWithGenerateName 按模板创建 Pod，并校验 controllerRef、设置 generateName。
func (r RealPodControl) CreatePodsWithGenerateName(ctx context.Context, namespace string, template *v1.PodTemplateSpec, controllerObject runtime.Object, controllerRef *metav1.OwnerReference, generateName string) error {
	if err := validateControllerRef(controllerRef); err != nil {
		return err
	}
	pod, err := GetPodFromTemplate(template, controllerObject, controllerRef)
	if err != nil {
		return err
	}
	if len(generateName) > 0 {
		pod.ObjectMeta.GenerateName = generateName
	}
	return r.createPods(ctx, namespace, pod, controllerObject, controllerRef)
}

// PatchPod 用策略合并补丁更新 Pod，成功时触发 OnWrite 回调。
func (r RealPodControl) PatchPod(ctx context.Context, namespace, name string, data []byte) error {
	pod, err := r.KubeClient.CoreV1().Pods(namespace).Patch(ctx, name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
	if err == nil && r.OnWrite != nil {
		ownerRef := metav1.GetControllerOfNoCopy(pod)
		r.OnWrite(pod, ownerRef)
	}
	return err
}

// GetPodFromTemplate 依据模板构造 Pod，复制标签、注解、finalizer 和 Spec，并设置所属者引用。
func GetPodFromTemplate(template *v1.PodTemplateSpec, parentObject runtime.Object, controllerRef *metav1.OwnerReference) (*v1.Pod, error) {
	desiredLabels := getPodsLabelSet(template)
	desiredFinalizers := getPodsFinalizers(template)
	desiredAnnotations := getPodsAnnotationSet(template)
	accessor, err := meta.Accessor(parentObject)
	if err != nil {
		return nil, fmt.Errorf("parentObject does not have ObjectMeta, %v", err)
	}
	prefix := getPodsPrefix(accessor.GetName())

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:       desiredLabels,
			Annotations:  desiredAnnotations,
			GenerateName: prefix,
			Finalizers:   desiredFinalizers,
		},
	}
	if controllerRef != nil {
		pod.OwnerReferences = append(pod.OwnerReferences, *controllerRef)
	}
	// 深拷贝 Spec，避免创建的 Pod 与模板共享 map/切片等引用。
	pod.Spec = *template.Spec.DeepCopy()
	return pod, nil
}

// createPods 真正调用 API 创建 Pod，并处理事件记录与 OnWrite 回调。
func (r RealPodControl) createPods(ctx context.Context, namespace string, pod *v1.Pod, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	if len(labels.Set(pod.Labels)) == 0 {
		return fmt.Errorf("unable to create pods, no labels")
	}
	newPod, err := r.KubeClient.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		// 命名空间正在终止时创建失败是预期行为，无需再发送告警事件。
		if !apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
			r.Recorder.Eventf(object, v1.EventTypeWarning, FailedCreatePodReason, "Error creating: %v", err)
		}
		return err
	}
	logger := klog.FromContext(ctx)
	if r.OnWrite != nil {
		r.OnWrite(newPod, controllerRef)
	}
	accessor, err := meta.Accessor(object)
	if err != nil {
		logger.Error(err, "parentObject does not have ObjectMeta")
		return nil
	}
	logger.V(4).Info("Controller created pod", "controller", accessor.GetName(), "pod", klog.KObj(newPod))
	r.Recorder.Eventf(object, v1.EventTypeNormal, SuccessfulCreatePodReason, "Created pod: %v", newPod.Name)

	return nil
}

// DeletePod 删除指定 Pod，已不存在时直接返回错误，不额外发送告警事件。
func (r RealPodControl) DeletePod(ctx context.Context, namespace string, podID string, object runtime.Object) error {
	accessor, err := meta.Accessor(object)
	if err != nil {
		return fmt.Errorf("object does not have ObjectMeta, %v", err)
	}
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Deleting pod", "controller", accessor.GetName(), "pod", klog.KRef(namespace, podID))
	if err := r.KubeClient.CoreV1().Pods(namespace).Delete(ctx, podID, metav1.DeleteOptions{}); err != nil {
		// 已删除属于幂等成功，不再发送删除失败事件。
		if apierrors.IsNotFound(err) {
			logger.V(4).Info("Pod has already been deleted.", "pod", klog.KRef(namespace, podID))
			return err
		}
		r.Recorder.Eventf(object, v1.EventTypeWarning, FailedDeletePodReason, "Error deleting: %v", err)
		return fmt.Errorf("unable to delete pods: %v", err)
	}
	r.Recorder.Eventf(object, v1.EventTypeNormal, SuccessfulDeletePodReason, "Deleted pod: %v", podID)

	return nil
}

// FakePodControl 记录 Pod 操作的测试替身，可用 Err 注入错误、CreateLimit 限制创建次数。
type FakePodControl struct {
	sync.Mutex
	Templates       []v1.PodTemplateSpec
	ControllerRefs  []metav1.OwnerReference
	DeletePodName   []string
	Patches         [][]byte
	Err             error
	CreateLimit     int
	CreateCallCount int
}

var _ PodControlInterface = &FakePodControl{}

// PatchPod 记录补丁内容并按 Err 返回预设错误。
func (f *FakePodControl) PatchPod(ctx context.Context, namespace, name string, data []byte) error {
	f.Lock()
	defer f.Unlock()
	f.Patches = append(f.Patches, data)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

// CreatePods 记录一次 Pod 创建，generateName 留空。
func (f *FakePodControl) CreatePods(ctx context.Context, namespace string, spec *v1.PodTemplateSpec, object runtime.Object, controllerRef *metav1.OwnerReference) error {
	return f.CreatePodsWithGenerateName(ctx, namespace, spec, object, controllerRef, "")
}

// CreatePodsWithGenerateName 记录创建的模板和 controllerRef，受 CreateLimit 限制。
func (f *FakePodControl) CreatePodsWithGenerateName(ctx context.Context, namespace string, spec *v1.PodTemplateSpec, object runtime.Object, controllerRef *metav1.OwnerReference, generateNamePrefix string) error {
	f.Lock()
	defer f.Unlock()
	f.CreateCallCount++
	// CreateLimit 为 0 表示不限制，否则超出上限即报错，
	// 用于验证控制器不会在已设置期望后重复创建 Pod。
	if f.CreateLimit != 0 && f.CreateCallCount > f.CreateLimit {
		return fmt.Errorf("not creating pod, limit %d already reached (create call %d)", f.CreateLimit, f.CreateCallCount)
	}
	spec.GenerateName = generateNamePrefix
	f.Templates = append(f.Templates, *spec)
	f.ControllerRefs = append(f.ControllerRefs, *controllerRef)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

// DeletePod 记录被删除的 Pod 名称并按 Err 返回预设错误。
func (f *FakePodControl) DeletePod(ctx context.Context, namespace string, podID string, object runtime.Object) error {
	f.Lock()
	defer f.Unlock()
	f.DeletePodName = append(f.DeletePodName, podID)
	if f.Err != nil {
		return f.Err
	}
	return nil
}

// Clear 清空记录的操作和限制，便于用例间复用同一个 FakePodControl。
func (f *FakePodControl) Clear() {
	f.Lock()
	defer f.Unlock()
	f.DeletePodName = []string{}
	f.Templates = []v1.PodTemplateSpec{}
	f.ControllerRefs = []metav1.OwnerReference{}
	f.Patches = [][]byte{}
	f.CreateLimit = 0
	f.CreateCallCount = 0
}

// ByLogging 自定义 Pod 排序，优先挑选最适合取日志的 Pod。
type ByLogging []*v1.Pod

// Len 返回待排序 Pod 数量。
func (s ByLogging) Len() int { return len(s) }

// Swap 交换两个 Pod 的位置。
func (s ByLogging) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less 按适合取日志的优先级比较两个 Pod，越优先的排越前。
func (s ByLogging) Less(i, j int) bool {
	// 1. 已调度优先于未调度。
	if s[i].Spec.NodeName != s[j].Spec.NodeName && (len(s[i].Spec.NodeName) == 0 || len(s[j].Spec.NodeName) == 0) {
		return len(s[i].Spec.NodeName) > 0
	}
	// 2. PodRunning 优先于 PodUnknown，再优先于 PodPending。
	if s[i].Status.Phase != s[j].Status.Phase {
		return podPhaseToOrdinal[s[i].Status.Phase] > podPhaseToOrdinal[s[j].Status.Phase]
	}
	// 3. 已就绪优先于未就绪。
	if podutil.IsPodReady(s[i]) != podutil.IsPodReady(s[j]) {
		return podutil.IsPodReady(s[i])
	}
	// TODO: 将 Deployment 的 minReadySeconds 信息下发到 Pod 时考虑可用性，
	//       参见 https://github.com/kubernetes/kubernetes/issues/22065。
	// 4. 就绪时间越长越优先，无就绪时间最末。
	if podutil.IsPodReady(s[i]) && podutil.IsPodReady(s[j]) {
		readyTime1 := podReadyTime(s[i])
		readyTime2 := podReadyTime(s[j])
		if !readyTime1.Equal(readyTime2) {
			return afterOrZero(readyTime2, readyTime1)
		}
	}
	// 5. 容器重启次数多的优先被选中。
	if res := compareMaxContainerRestarts(s[i], s[j]); res != nil {
		return *res
	}
	// 6. 创建时间早的优先；无时间戳的排最后。
	if !s[i].CreationTimestamp.Equal(&s[j].CreationTimestamp) {
		return afterOrZero(&s[j].CreationTimestamp, &s[i].CreationTimestamp)
	}
	return false
}

// ActivePods 自定义 Pod 排序，让控制器优先删除最合适的 Pod。
type ActivePods []*v1.Pod

// Len 返回待排序 Pod 数量。
func (s ActivePods) Len() int { return len(s) }

// Swap 交换两个 Pod 的位置。
func (s ActivePods) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less 按优先删除的顺序比较两个 Pod。
func (s ActivePods) Less(i, j int) bool {
	// 1. 未调度优先删除；仅一方未调度时该方更小。
	if s[i].Spec.NodeName != s[j].Spec.NodeName && (len(s[i].Spec.NodeName) == 0 || len(s[j].Spec.NodeName) == 0) {
		return len(s[i].Spec.NodeName) == 0
	}
	// 2. PodPending 优先于 PodUnknown，再优先于 PodRunning。
	if podPhaseToOrdinal[s[i].Status.Phase] != podPhaseToOrdinal[s[j].Status.Phase] {
		return podPhaseToOrdinal[s[i].Status.Phase] < podPhaseToOrdinal[s[j].Status.Phase]
	}
	// 3. 未就绪优先删除；仅一方未就绪时该方更小。
	if podutil.IsPodReady(s[i]) != podutil.IsPodReady(s[j]) {
		return !podutil.IsPodReady(s[i])
	}
	// TODO: 将 Deployment 的 minReadySeconds 信息下发到 Pod 时考虑可用性，
	//       参见 https://github.com/kubernetes/kubernetes/issues/22065。
	// 4. 就绪时间短的优先删除；双方都就绪时，最近才就绪的更小。
	if podutil.IsPodReady(s[i]) && podutil.IsPodReady(s[j]) {
		readyTime1 := podReadyTime(s[i])
		readyTime2 := podReadyTime(s[j])
		if !readyTime1.Equal(readyTime2) {
			return afterOrZero(readyTime1, readyTime2)
		}
	}
	// 5. 容器重启次数多的优先删除。
	if res := compareMaxContainerRestarts(s[i], s[j]); res != nil {
		return *res
	}
	// 6. 创建时间晚的优先删除；无时间戳的排最前。
	if !s[i].CreationTimestamp.Equal(&s[j].CreationTimestamp) {
		return afterOrZero(&s[i].CreationTimestamp, &s[j].CreationTimestamp)
	}
	return false
}

// ActivePodsWithRanks 是可排序的 Pod 列表及对应 rank 列表，两列表长度必须一致。
// 排序时依次应用下列规则，命中即停：
//
//  1. 仅一方已调度时，未调度的排在前面。
//  2. 阶段不同时：Pending 排在 Unknown 前，Unknown 排在 Running 前。
//  3. 仅一方就绪时，未就绪的排在前面。
//  4. 设置了 controller.kubernetes.io/pod-deletion-cost 注解时，值小的排在前面。
//  5. rank 不同时，rank 大的排在前面。
//  6. 双方就绪且就绪时长不同时，就绪时间短的排在前面。
//  7. 某 Pod 存在比对方任何容器重启次数都多的容器时，该 Pod 排在前面。
//  8. 创建时间不同时，较新的排在较旧的之前。
//
// 规则 6、8 以对数尺度比较时间，使等价 Pod 之间保留一定随机性；
// 对数等级相同时按 UID 排序，提供伪随机顺序。
//
// 若所有规则都不匹配，则第二个 Pod 排在第一个之前。
//
// 排序意图是把最应优先删除的 Pod 放在列表前面。
type ActivePodsWithRanks struct {
	// Pods 为待排序的 Pod 列表。
	Pods []*v1.Pod

	// Rank 为 Pod 的排名，仅当两个 Pod 都已调度、阶段相同且就绪状态相同时参与比较。
	Rank []int

	// Now 是对数时间比较的参考时刻；为零时不缩放直接比较。
	Now metav1.Time
}

// Len 返回待排序 Pod 数量。
func (s ActivePodsWithRanks) Len() int {
	return len(s.Pods)
}

// Swap 同步交换两个 Pod 及其 rank，保持两列表对应。
func (s ActivePodsWithRanks) Swap(i, j int) {
	s.Pods[i], s.Pods[j] = s.Pods[j], s.Pods[i]
	s.Rank[i], s.Rank[j] = s.Rank[j], s.Rank[i]
}

// Less 比较带 rank 的两个 Pod，第一个更应优先删除时返回 true。
func (s ActivePodsWithRanks) Less(i, j int) bool {
	// 1. 未调度优先删除；仅一方未调度时该方更小。
	if s.Pods[i].Spec.NodeName != s.Pods[j].Spec.NodeName && (len(s.Pods[i].Spec.NodeName) == 0 || len(s.Pods[j].Spec.NodeName) == 0) {
		return len(s.Pods[i].Spec.NodeName) == 0
	}
	// 2. PodPending 优先于 PodUnknown，再优先于 PodRunning。
	if podPhaseToOrdinal[s.Pods[i].Status.Phase] != podPhaseToOrdinal[s.Pods[j].Status.Phase] {
		return podPhaseToOrdinal[s.Pods[i].Status.Phase] < podPhaseToOrdinal[s.Pods[j].Status.Phase]
	}
	// 3. 未就绪优先删除；仅一方未就绪时该方更小。
	if podutil.IsPodReady(s.Pods[i]) != podutil.IsPodReady(s.Pods[j]) {
		return !podutil.IsPodReady(s.Pods[i])
	}

	// 4. deletion-cost 越小越优先删除；特性关闭时跳过该规则。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodDeletionCost) {
		pi, _ := helper.GetDeletionCostFromPodAnnotations(s.Pods[i].Annotations)
		pj, _ := helper.GetDeletionCostFromPodAnnotations(s.Pods[j].Annotations)
		if pi != pj {
			return pi < pj
		}
	}

	// 5. 同节点上同副本集的就绪 Pod 越多，越优先删除（rank 大者优先）。
	if s.Rank[i] != s.Rank[j] {
		return s.Rank[i] > s.Rank[j]
	}
	// TODO: 将 Deployment 的 minReadySeconds 信息下发到 Pod 时考虑可用性，
	//       参见 https://github.com/kubernetes/kubernetes/issues/22065。
	// 6. 就绪时间短的优先删除；双方都就绪时，最近才就绪的更小。
	if podutil.IsPodReady(s.Pods[i]) && podutil.IsPodReady(s.Pods[j]) {
		readyTime1 := podReadyTime(s.Pods[i])
		readyTime2 := podReadyTime(s.Pods[j])
		if !readyTime1.Equal(readyTime2) {
			// 参考时刻或就绪时间缺失时无法缩放，退回直接比较。
			if s.Now.IsZero() || readyTime1.IsZero() || readyTime2.IsZero() {
				return afterOrZero(readyTime1, readyTime2)
			}
			rankDiff := logarithmicRankDiff(*readyTime1, *readyTime2, s.Now)
			// 对数等级相同则按 UID 排，避免等价 Pod 总保持固定顺序。
			if rankDiff == 0 {
				return s.Pods[i].UID < s.Pods[j].UID
			}
			return rankDiff < 0
		}
	}
	// 7. 容器重启次数多的优先删除。
	if res := compareMaxContainerRestarts(s.Pods[i], s.Pods[j]); res != nil {
		return *res
	}
	// 8. 创建时间晚的优先删除；无时间戳的排最前。
	if !s.Pods[i].CreationTimestamp.Equal(&s.Pods[j].CreationTimestamp) {
		if s.Now.IsZero() || s.Pods[i].CreationTimestamp.IsZero() || s.Pods[j].CreationTimestamp.IsZero() {
			return afterOrZero(&s.Pods[i].CreationTimestamp, &s.Pods[j].CreationTimestamp)
		}
		rankDiff := logarithmicRankDiff(s.Pods[i].CreationTimestamp, s.Pods[j].CreationTimestamp, s.Now)
		// 对数等级相同则按 UID 排，打破创建时间相近 Pod 的固定顺序。
		if rankDiff == 0 {
			return s.Pods[i].UID < s.Pods[j].UID
		}
		return rankDiff < 0
	}
	return false
}

// afterOrZero 判断 t1 是否晚于 t2；零值时间被视为晚于非零时间。
func afterOrZero(t1, t2 *metav1.Time) bool {
	if t1.Time.IsZero() || t2.Time.IsZero() {
		return t1.Time.IsZero()
	}
	return t1.After(t2.Time)
}

// logarithmicRankDiff 计算两个时间戳相对当前时刻的以 2 为底的对数等级之差。
func logarithmicRankDiff(t1, t2, now metav1.Time) int64 {
	d1 := now.Sub(t1.Time)
	d2 := now.Sub(t2.Time)
	r1 := int64(-1)
	r2 := int64(-1)
	if d1 > 0 {
		r1 = int64(math.Log2(float64(d1)))
	}
	if d2 > 0 {
		r2 = int64(math.Log2(float64(d2)))
	}
	return r1 - r2
}

// podReadyTime 返回 Pod 进入就绪状态的时间，未就绪时返回零值。
func podReadyTime(pod *v1.Pod) *metav1.Time {
	if podutil.IsPodReady(pod) {
		for _, c := range pod.Status.Conditions {
			// 只关心处于 True 的 PodReady 条件，其跳转时间即就绪起点。
			if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
				return &c.LastTransitionTime
			}
		}
	}
	return &metav1.Time{}
}

// maxContainerRestarts 返回普通容器和常驻 sidecar 各自的最大重启次数。
func maxContainerRestarts(pod *v1.Pod) (regularRestarts, sidecarRestarts int) {
	for _, c := range pod.Status.ContainerStatuses {
		regularRestarts = max(regularRestarts, int(c.RestartCount))
	}
	names := sets.New[string]()
	// 只有 RestartPolicy=Always 的 init 容器才是 sidecar，需单独统计其重启次数。
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			names.Insert(c.Name)
		}
	}
	for _, c := range pod.Status.InitContainerStatuses {
		if names.Has(c.Name) {
			sidecarRestarts = max(sidecarRestarts, int(c.RestartCount))
		}
	}
	return
}

// compareMaxContainerRestarts 比较两个 Pod 的最大重启次数。
// 返回值为 true 表示 pi 更高，false 表示 pj 更高，nil 表示相同。
func compareMaxContainerRestarts(pi *v1.Pod, pj *v1.Pod) *bool {
	regularRestartsI, sidecarRestartsI := maxContainerRestarts(pi)
	regularRestartsJ, sidecarRestartsJ := maxContainerRestarts(pj)
	if regularRestartsI != regularRestartsJ {
		res := regularRestartsI > regularRestartsJ
		return &res
	}
	// 普通容器重启次数相同时，再用 sidecar 的重启次数区分。
	if sidecarRestartsI != sidecarRestartsJ {
		res := sidecarRestartsI > sidecarRestartsJ
		return &res
	}
	return nil
}

// FilterClaimedPods 返回由该控制器掌控且匹配选择器的 Pod。
func FilterClaimedPods(controller metav1.Object, selector labels.Selector, pods []*v1.Pod) []*v1.Pod {
	var result []*v1.Pod
	for _, pod := range pods {
		if !metav1.IsControlledBy(pod, controller) {
			// 孤儿 Pod 或属于其他控制器，直接跳过。
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			result = append(result, pod)
		}
	}
	return result
}

// FilterActivePods 返回尚未终止的 Pod。
func FilterActivePods(logger klog.Logger, pods []*v1.Pod) []*v1.Pod {
	var result []*v1.Pod
	for _, p := range pods {
		if IsPodActive(p) {
			result = append(result, p)
		}
	}
	return result
}

// FilterTerminatingPods 返回处于终止中（已设删除时间戳但未终止）的 Pod。
func FilterTerminatingPods(pods []*v1.Pod) []*v1.Pod {
	var result []*v1.Pod
	for _, p := range pods {
		if IsPodTerminating(p) {
			result = append(result, p)
		}
	}
	return result
}

// CountTerminatingPods 统计处于终止中的 Pod 数量。
func CountTerminatingPods(pods []*v1.Pod) int32 {
	numberOfTerminatingPods := 0
	for _, p := range pods {
		if IsPodTerminating(p) {
			numberOfTerminatingPods += 1
		}
	}
	return int32(numberOfTerminatingPods)
}

// nextPodAvailabilityCheck 计算 Pod 距变为可用还需多久，逻辑与 podutil.IsPodAvailable 一致。
func nextPodAvailabilityCheck(pod *v1.Pod, minReadySeconds int32, now time.Time) *time.Duration {
	// 未就绪或未设置 minReadySeconds 时无需安排后续可用性检查。
	if !podutil.IsPodReady(pod) || minReadySeconds <= 0 {
		return nil
	}

	c := podutil.GetPodReadyCondition(pod.Status)
	// 缺少就绪跳转时间时无法推算，放弃本次检查。
	if c.LastTransitionTime.IsZero() {
		return nil
	}
	minReadySecondsDuration := time.Duration(minReadySeconds) * time.Second
	nextCheck := c.LastTransitionTime.Add(minReadySecondsDuration).Sub(now)
	// 已过检查点则无需再安排。
	if nextCheck > 0 {
		return ptr.To(nextCheck)
	}
	return nil
}

// findMinNextPodAvailabilitySimpleCheck 找出最早需要再次做可用性检查的时间，
// 并返回触发该检查的 Pod（可能有多个同时就绪，此处取首个）。
func findMinNextPodAvailabilitySimpleCheck(pods []*v1.Pod, minReadySeconds int32, now time.Time) (*time.Duration, *v1.Pod) {
	var minAvailabilityCheck *time.Duration
	var checkPod *v1.Pod
	for _, p := range pods {
		nextCheck := nextPodAvailabilityCheck(p, minReadySeconds, now)
		// 仅保留最早的一次检查及其触发 Pod。
		if nextCheck != nil && (minAvailabilityCheck == nil || *nextCheck < *minAvailabilityCheck) {
			minAvailabilityCheck = nextCheck
			checkPod = p
		}
	}
	return minAvailabilityCheck, checkPod
}

// FindMinNextPodAvailabilityCheck 返回距下一次可用性检查的时长。
// 传入 lastOwnerStatusEvaluation（状态评估/更新时间，如 .status.availableReplicas），
// 确保不会漏掉届时可能变为可用的 Pod（否则简单检查在未来时刻会返回 nil）；
// 再由 clock 计算精确的下次检查时间。
func FindMinNextPodAvailabilityCheck(pods []*v1.Pod, minReadySeconds int32, lastOwnerStatusEvaluation time.Time, clock clock.PassiveClock) *time.Duration {
	nextCheckAccordingToOwnerStatusEvaluation, checkPod := findMinNextPodAvailabilitySimpleCheck(pods, minReadySeconds, lastOwnerStatusEvaluation)
	if nextCheckAccordingToOwnerStatusEvaluation == nil || checkPod == nil {
		return nil
	}
	// 用最早触发检查的 Pod 和当前时刻重算，避免被更晚的 Pod 抢占判断。
	if updatedNextCheck := nextPodAvailabilityCheck(checkPod, minReadySeconds, clock.Now()); updatedNextCheck != nil {
		// 距上次状态评估已有延迟，采用基于当前时刻的更精确结果。
		return updatedNextCheck
	}
	// 重算返回 nil 说明检查点可能已过，改为立即检查。
	return ptr.To(time.Duration(0))
}

// IsPodActive 判断 Pod 是否仍在运行（未成功/失败终止且未进入删除）。
func IsPodActive(p *v1.Pod) bool {
	return v1.PodSucceeded != p.Status.Phase &&
		v1.PodFailed != p.Status.Phase &&
		p.DeletionTimestamp == nil
}

// IsPodTerminating 判断 Pod 是否已开始删除但尚未终止。
func IsPodTerminating(p *v1.Pod) bool {
	return !podutil.IsPodTerminal(p) &&
		p.DeletionTimestamp != nil
}

// FilterActiveReplicaSets 返回当前有（或至少应有）Pod 的副本集。
func FilterActiveReplicaSets(replicaSets []*apps.ReplicaSet) []*apps.ReplicaSet {
	activeFilter := func(rs *apps.ReplicaSet) bool {
		return rs != nil && *(rs.Spec.Replicas) > 0
	}
	return FilterReplicaSets(replicaSets, activeFilter)
}

// filterRS 定义副本集的过滤条件。
type filterRS func(rs *apps.ReplicaSet) bool

// FilterReplicaSets 返回所有满足 filterFn 的副本集。
func FilterReplicaSets(RSes []*apps.ReplicaSet, filterFn filterRS) []*apps.ReplicaSet {
	var filtered []*apps.ReplicaSet
	for i := range RSes {
		if filterFn(RSes[i]) {
			filtered = append(filtered, RSes[i])
		}
	}
	return filtered
}

// AddPodNodeNameIndexer 为 PodInformer 添加按 nodeName 的索引，便于按节点快速查找 Pod。
func AddPodNodeNameIndexer(podInformer cache.SharedIndexInformer) error {
	if _, exists := podInformer.GetIndexer().GetIndexers()[PodNodeNameKeyIndex]; exists {
		// 索引已存在时直接返回，避免重复注册。
		return nil
	}

	return podInformer.AddIndexers(cache.Indexers{
		PodNodeNameKeyIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return []string{}, nil
			}
			if len(pod.Spec.NodeName) == 0 {
				return []string{}, nil
			}
			return []string{pod.Spec.NodeName}, nil
		},
	})
}

// PodControllerIndexKey 返回定位指定控制器所属 Pod 的索引键。
// ownerReference 为 nil 时返回的键定位该命名空间下的孤儿 Pod。
func PodControllerIndexKey(namespace string, ownerReference *metav1.OwnerReference) string {
	if ownerReference == nil {
		return namespace
	}
	return namespace + "/" + ownerReference.Kind + "/" + ownerReference.Name + "/" + string(ownerReference.UID)
}

// AddPodControllerIndexer 为 PodInformer 添加按 controllerRef.UID 的索引，便于按控制器快速查找 Pod。
func AddPodControllerIndexer(podInformer cache.SharedIndexInformer) error {
	if _, exists := podInformer.GetIndexer().GetIndexers()[PodControllerIndex]; exists {
		// 索引已存在时直接返回，避免重复注册。
		return nil
	}
	return podInformer.AddIndexers(cache.Indexers{
		PodControllerIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return nil, nil
			}
			// 按控制器引用建索引：非 nil 表示有主 Pod，nil 表示孤儿 Pod。
			return []string{PodControllerIndexKey(pod.Namespace, metav1.GetControllerOf(pod))}, nil
		},
	})
}

// FilterPodsByOwner 获取指定 owner 管理的 Pod，可选地包含其命名空间下的孤儿 Pod。
func FilterPodsByOwner(podIndexer cache.Indexer, owner *metav1.ObjectMeta, ownerKind string, includeOrphanedPods bool) ([]*v1.Pod, error) {
	result := []*v1.Pod{}

	if len(owner.Namespace) == 0 {
		return nil, fmt.Errorf("no owner namespace provided")
	}
	if len(owner.Name) == 0 {
		return nil, fmt.Errorf("no owner name provided")
	}
	if len(owner.UID) == 0 {
		return nil, fmt.Errorf("no owner uid provided")
	}
	if len(ownerKind) == 0 {
		return nil, fmt.Errorf("no owner kind provided")
	}
	// owner 键必然包含，用于定位受其控制的 Pod。
	keys := []string{PodControllerIndexKey(owner.Namespace, &metav1.OwnerReference{Name: owner.Name, Kind: ownerKind, UID: owner.UID})}
	if includeOrphanedPods {
		// 按需加入无主键：这些孤儿 Pod 位于 owner 的命名空间，日后可能被其接管。
		keys = append(keys, PodControllerIndexKey(owner.Namespace, nil))
	}
	for _, key := range keys {
		pods, err := podIndexer.ByIndex(PodControllerIndex, key)
		if err != nil {
			return nil, err
		}
		for _, obj := range pods {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				utilruntime.HandleError(fmt.Errorf("unexpected object type in pod indexer: %v", obj))
				continue
			}
			result = append(result, pod)
		}
	}
	return result, nil
}

// PodKey 返回集群内唯一标识该 Pod 的键，保证本模块键格式一致。
// 行为与 cache.MetaNamespaceKeyFunc 相同，但因类型确定而不会出错。
func PodKey(pod *v1.Pod) string {
	return fmt.Sprintf("%v/%v", pod.Namespace, pod.Name)
}

// ControllersByCreationTimestamp 按创建时间排列 ReplicationController，时间相同则按名称决胜。
type ControllersByCreationTimestamp []*v1.ReplicationController

// Len 返回控制器数量。
func (o ControllersByCreationTimestamp) Len() int { return len(o) }

// Swap 交换两个控制器的位置。
func (o ControllersByCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 按创建时间升序排列，时间相同按名称升序。
func (o ControllersByCreationTimestamp) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// ReplicaSetsByCreationTimestamp 按创建时间排列副本集，时间相同则按名称决胜。
type ReplicaSetsByCreationTimestamp []*apps.ReplicaSet

// Len 返回副本集数量。
func (o ReplicaSetsByCreationTimestamp) Len() int { return len(o) }

// Swap 交换两个副本集的位置。
func (o ReplicaSetsByCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 按创建时间升序排列，时间相同按名称升序。
func (o ReplicaSetsByCreationTimestamp) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// ReplicaSetsBySizeOlder 按副本数降序排列副本集，数量相同按创建时间或名称决胜。
// 使用创建时间使同规模副本集从旧到新排列。
type ReplicaSetsBySizeOlder []*apps.ReplicaSet

// Len 返回副本集数量。
func (o ReplicaSetsBySizeOlder) Len() int { return len(o) }

// Swap 交换两个副本集的位置。
func (o ReplicaSetsBySizeOlder) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 副本数多者优先，数量相同则较旧的排在前面。
func (o ReplicaSetsBySizeOlder) Less(i, j int) bool {
	if *(o[i].Spec.Replicas) == *(o[j].Spec.Replicas) {
		return ReplicaSetsByCreationTimestamp(o).Less(i, j)
	}
	return *(o[i].Spec.Replicas) > *(o[j].Spec.Replicas)
}

// ReplicaSetsBySizeNewer 按副本数降序排列副本集，数量相同按创建时间或名称决胜。
// 使用创建时间使同规模副本集从新到旧排列。
type ReplicaSetsBySizeNewer []*apps.ReplicaSet

// Len 返回副本集数量。
func (o ReplicaSetsBySizeNewer) Len() int { return len(o) }

// Swap 交换两个副本集的位置。
func (o ReplicaSetsBySizeNewer) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 副本数多者优先，数量相同则较新的排在前面。
func (o ReplicaSetsBySizeNewer) Less(i, j int) bool {
	if *(o[i].Spec.Replicas) == *(o[j].Spec.Replicas) {
		return ReplicaSetsByCreationTimestamp(o).Less(j, i)
	}
	return *(o[i].Spec.Replicas) > *(o[j].Spec.Replicas)
}

// AddOrUpdateTaintOnNode 为节点添加或更新污点；仅当节点实际发生变化时才发起 API 更新。
func AddOrUpdateTaintOnNode(ctx context.Context, c clientset.Interface, nodeName string, taints ...*v1.Taint) error {
	if len(taints) == 0 {
		return nil
	}
	firstTry := true
	return clientretry.RetryOnConflict(UpdateTaintBackoff, func() error {
		var err error
		var oldNode *v1.Node
		// 首次尝试从 apiserver 缓存读取（更廉价），失败则直读 etcd 以获取最新数据。
		option := metav1.GetOptions{}
		if firstTry {
			option.ResourceVersion = "0"
			firstTry = false
		}
		oldNode, err = c.CoreV1().Nodes().Get(ctx, nodeName, option)
		if err != nil {
			return err
		}

		var newNode *v1.Node
		oldNodeCopy := oldNode
		updated := false
		for _, taint := range taints {
			curNewNode, ok, err := taintutils.AddOrUpdateTaint(oldNodeCopy, taint)
			if err != nil {
				return fmt.Errorf("failed to update taint of node")
			}
			// 逐个累加污点变更；以 oldNodeCopy 为基准避免丢失前面的修改。
			updated = updated || ok
			newNode = curNewNode
			oldNodeCopy = curNewNode
		}
		// 没有实际变更就不发 API 请求。
		if !updated {
			return nil
		}
		return PatchNodeTaints(ctx, c, nodeName, oldNode, newNode)
	})
}

// RemoveTaintOffNode 清理节点上临时添加的污点；目标污点不存在时不会报错。
// 若传入 node，会先在本地检查以减少不必要的 API 调用。
func RemoveTaintOffNode(ctx context.Context, c clientset.Interface, nodeName string, node *v1.Node, taints ...*v1.Taint) error {
	if len(taints) == 0 {
		return nil
	}
	// 传入的节点中若没有目标污点，直接短路返回，省去 API 调用。
	if node != nil {
		match := false
		for _, taint := range taints {
			if taintutils.TaintExists(node.Spec.Taints, taint) {
				match = true
				break
			}
		}
		if !match {
			return nil
		}
	}

	firstTry := true
	return clientretry.RetryOnConflict(UpdateTaintBackoff, func() error {
		var err error
		var oldNode *v1.Node
		// 首次尝试从 apiserver 缓存读取（更廉价），失败则直读 etcd 以获取最新数据。
		option := metav1.GetOptions{}
		if firstTry {
			option.ResourceVersion = "0"
			firstTry = false
		}
		oldNode, err = c.CoreV1().Nodes().Get(ctx, nodeName, option)
		if err != nil {
			return err
		}

		var newNode *v1.Node
		oldNodeCopy := oldNode
		updated := false
		for _, taint := range taints {
			curNewNode, ok, err := taintutils.RemoveTaint(oldNodeCopy, taint)
			if err != nil {
				return fmt.Errorf("failed to remove taint of node")
			}
			// 以累积后的副本继续移除，保证多个污点的删除都生效。
			updated = updated || ok
			newNode = curNewNode
			oldNodeCopy = curNewNode
		}
		if !updated {
			return nil
		}
		return PatchNodeTaints(ctx, c, nodeName, oldNode, newNode)
	})
}

// PatchNodeTaints 用策略合并补丁更新节点污点，并借助 RV 检测并发冲突。
func PatchNodeTaints(ctx context.Context, c clientset.Interface, nodeName string, oldNode *v1.Node, newNode *v1.Node) error {
	// 从基准数据中去掉 ResourceVersion，让 Patch 请求带上 RV 以检测 .spec.taints 的并发冲突。
	// .spec.taints 未定义 patchMergeKey/patchStrategy，出于兼容性也无法再加；
	// 换其他 Patch 策略虽能新增污点，却无法正确处理污点删除。
	oldNodeNoRV := oldNode.DeepCopy()
	oldNodeNoRV.ResourceVersion = ""
	oldDataNoRV, err := json.Marshal(&oldNodeNoRV)
	if err != nil {
		return fmt.Errorf("failed to marshal old node %#v for node %q: %v", oldNodeNoRV, nodeName, err)
	}

	// 仅替换污点字段，避免新节点对象上的其他改动被一并写入补丁。
	newTaints := newNode.Spec.Taints
	newNodeClone := oldNode.DeepCopy()
	newNodeClone.Spec.Taints = newTaints
	newData, err := json.Marshal(newNodeClone)
	if err != nil {
		return fmt.Errorf("failed to marshal new node %#v for node %q: %v", newNodeClone, nodeName, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldDataNoRV, newData, v1.Node{})
	if err != nil {
		return fmt.Errorf("failed to create patch for node %q: %v", nodeName, err)
	}

	_, err = c.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// ComputeHash 根据 Pod 模板和 collisionCount 计算哈希，并用安全编码避免出现不当字符串。
func ComputeHash(template *v1.PodTemplateSpec, collisionCount *int32) string {
	podTemplateSpecHasher := fnv.New32a()
	hashutil.DeepHashObject(podTemplateSpecHasher, *template)

	// 将 collisionCount 纳入哈希，使哈希碰撞时可使用不同取值重新生成名称。
	if collisionCount != nil {
		collisionCountBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(collisionCountBytes, uint32(*collisionCount))
		podTemplateSpecHasher.Write(collisionCountBytes)
	}

	return rand.SafeEncodeString(fmt.Sprint(podTemplateSpecHasher.Sum32()))
}

// AddOrUpdateLabelsOnNode 添加或更新节点标签。
func AddOrUpdateLabelsOnNode(kubeClient clientset.Interface, nodeName string, labelsToUpdate map[string]string) error {
	firstTry := true
	return clientretry.RetryOnConflict(UpdateLabelBackoff, func() error {
		var err error
		var node *v1.Node
		// 首次尝试从 apiserver 缓存读取（更廉价），失败则直读 etcd 以获取最新数据。
		option := metav1.GetOptions{}
		if firstTry {
			option.ResourceVersion = "0"
			firstTry = false
		}
		node, err = kubeClient.CoreV1().Nodes().Get(context.TODO(), nodeName, option)
		if err != nil {
			return err
		}

		// 在副本上改标签，生成两路合并补丁时保留原对象。
		newNode := node.DeepCopy()
		if newNode.Labels == nil {
			newNode.Labels = make(map[string]string)
		}
		for key, value := range labelsToUpdate {
			newNode.Labels[key] = value
		}

		oldData, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("failed to marshal the existing node %#v: %v", node, err)
		}
		newData, err := json.Marshal(newNode)
		if err != nil {
			return fmt.Errorf("failed to marshal the new node %#v: %v", newNode, err)
		}
		patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &v1.Node{})
		if err != nil {
			return fmt.Errorf("failed to create a two-way merge patch: %v", err)
		}
		if _, err := kubeClient.CoreV1().Nodes().Patch(context.TODO(), node.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("failed to patch the node: %v", err)
		}
		return nil
	})
}
