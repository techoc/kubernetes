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

package kubelet

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/allocation"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/queue"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

// fakePodWorkers 串行执行 sync pod 函数，使测试行为确定；
// 同时记录删除与终端事件，并提供各类 Pod 状态查询。
type fakePodWorkers struct {
	lock      sync.Mutex
	syncPodFn syncPodFnType
	cache     kubecontainer.ROCache
	t         TestingInterface

	triggeredDeletion []types.UID
	triggeredTerminal []types.UID

	statusLock            sync.Mutex
	running               map[types.UID]bool
	terminating           map[types.UID]bool
	terminated            map[types.UID]bool
	terminationRequested  map[types.UID]bool
	finished              map[types.UID]bool
	removeRuntime         map[types.UID]bool
	removeContent         map[types.UID]bool
	terminatingStaticPods map[string]bool
}

// 串行执行一次 Pod 同步，保证测试行为确定。
// 根据 UpdateType 决定记录为删除还是调用 syncPodFn；终端状态会被单独记录。
func (f *fakePodWorkers) UpdatePod(ctx context.Context, options UpdatePodOptions) {
	f.lock.Lock()
	defer f.lock.Unlock()
	var uid types.UID
	switch {
	case options.Pod != nil:
		uid = options.Pod.UID
	case options.RunningPod != nil:
		uid = options.RunningPod.ID
	default:
		return
	}
	status, err := f.cache.Get(uid)
	if err != nil {
		f.t.Errorf("Unexpected error: %v", err)
	}
	switch options.UpdateType {
	case kubetypes.SyncPodKill:
		f.triggeredDeletion = append(f.triggeredDeletion, uid)
	default:
		isTerminal, _, err := f.syncPodFn(ctx, options.UpdateType, options.Pod, options.MirrorPod, status)
		if err != nil {
			f.t.Errorf("Unexpected error: %v", err)
		}
		if isTerminal {
			f.triggeredTerminal = append(f.triggeredTerminal, uid)
		}
	}
}

// fake 实现不维护同步状态，直接返回空 map。
func (f *fakePodWorkers) SyncKnownPods(_ klog.Logger, desiredPods []*v1.Pod) map[types.UID]PodWorkerSync {
	return map[types.UID]PodWorkerSync{}
}

// 返回该 Pod 是否已被标记为终止。
func (f *fakePodWorkers) IsPodKnownTerminated(uid types.UID) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.terminated[uid]
}

// 返回该 Pod 是否可能仍有运行中的容器。
func (f *fakePodWorkers) CouldHaveRunningContainers(uid types.UID) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.running[uid]
}

// 返回该 Pod 是否已完成全部清理。
func (f *fakePodWorkers) ShouldPodBeFinished(uid types.UID) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.finished[uid]
}

// 返回该 Pod 是否已收到终止请求。
func (f *fakePodWorkers) IsPodTerminationRequested(uid types.UID) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.terminationRequested[uid]
}

// 返回该 Pod 的容器是否应进入终止流程。
func (f *fakePodWorkers) ShouldPodContainersBeTerminating(uid types.UID) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.terminating[uid]
}

// 返回该 Pod 的运行时资源是否应被移除。
func (f *fakePodWorkers) ShouldPodRuntimeBeRemoved(uid types.UID) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.removeRuntime[uid]
}

// 把指定 Pod 标记为运行时资源已移除。
func (f *fakePodWorkers) setPodRuntimeBeRemoved(uid types.UID) {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	f.removeRuntime = map[types.UID]bool{uid: true}
}

// 返回该 Pod 的内容（卷、cgroup 等）是否应被移除。
func (f *fakePodWorkers) ShouldPodContentBeRemoved(uid types.UID) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.removeContent[uid]
}

// 按 Pod 全名查询对应的 static/mirror Pod 是否正在终止。
func (f *fakePodWorkers) IsPodForMirrorPodTerminatingByFullName(podFullname string) bool {
	f.statusLock.Lock()
	defer f.statusLock.Unlock()
	return f.terminatingStaticPods[podFullname]
}

type TestingInterface interface {
	Errorf(format string, args ...interface{})
}

// 构造带指定名称与阶段的测试 Pod。
func newPodWithPhase(uid, name string, phase v1.PodPhase) *v1.Pod {
	pod := newNamedPod(uid, "ns", name, false)
	pod.Status.Phase = phase
	return pod
}

// 构造带 file source 注解的 static Pod，用于验证 static Pod 互斥逻辑。
func newStaticPod(uid, name string) *v1.Pod {
	thirty := int64(30)
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID(uid),
			Name: name,
			Annotations: map[string]string{
				kubetypes.ConfigSourceAnnotationKey: kubetypes.FileSource,
			},
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &thirty,
		},
	}
}

// 构造指定 UID/namespace/name 的测试 Pod，可选标记为 static。
func newNamedPod(uid, namespace, name string, isStatic bool) *v1.Pod {
	thirty := int64(30)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(uid),
			Namespace: namespace,
			Name:      name,
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &thirty,
		},
	}
	if isStatic {
		pod.Annotations = map[string]string{
			kubetypes.ConfigSourceAnnotationKey: kubetypes.FileSource,
		}
	}
	return pod
}

// syncPodRecord 记录一次 sync pod 调用，包括名称、更新类型、RuntimePod、
// 是否已终止以及使用的优雅终止时间。
type syncPodRecord struct {
	name        string
	updateType  kubetypes.SyncPodType
	runningPod  *kubecontainer.Pod
	terminated  bool
	gracePeriod *int64
}

type FakeQueueItem struct {
	UID   types.UID
	Delay time.Duration
}

type fakeQueue struct {
	lock         sync.Mutex
	queue        []FakeQueueItem
	currentStart int
}

// 判断队列中是否还有未取出的元素。
func (q *fakeQueue) Empty() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return (len(q.queue) - q.currentStart) == 0
}

// 返回队列的完整快照，供测试断言延迟与顺序。
func (q *fakeQueue) Items() []FakeQueueItem {
	q.lock.Lock()
	defer q.lock.Unlock()
	return append(make([]FakeQueueItem, 0, len(q.queue)), q.queue...)
}

// 返回未取出元素对应的 UID 集合。
func (q *fakeQueue) Set() sets.Set[string] {
	q.lock.Lock()
	defer q.lock.Unlock()
	work := sets.New[string]()
	for _, item := range q.queue[q.currentStart:] {
		work.Insert(string(item.UID))
	}
	return work
}

// 把 UID 与延迟追加到队列尾部。
func (q *fakeQueue) Enqueue(uid types.UID, delay time.Duration) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.queue = append(q.queue, FakeQueueItem{UID: uid, Delay: delay})
}

// 取出当前全部待处理 UID，并把读取位置推进到队尾。
func (q *fakeQueue) GetWork() []types.UID {
	q.lock.Lock()
	defer q.lock.Unlock()
	work := make([]types.UID, 0, len(q.queue)-q.currentStart)
	for _, item := range q.queue[q.currentStart:] {
		work = append(work, item.UID)
	}
	q.currentStart = len(q.queue)
	return work
}

type timeIncrementingWorkers struct {
	lock    sync.Mutex
	w       *podWorkers
	runtime *containertest.FakeRuntime
	holds   map[types.UID]chan struct{}
}

// 持有锁调用 UpdatePod 并推进时钟，再执行 afterFns（用于在 worker 读取状态前断言），
// 最后等待所有未暂停的 worker 完成，使一次调用内的状态变化可被确定性地观察。
func (w *timeIncrementingWorkers) UpdatePod(ctx context.Context, options UpdatePodOptions, afterFns ...func()) {
	func() {
		w.lock.Lock()
		defer w.lock.Unlock()
		w.w.UpdatePod(ctx, options)
		w.w.clock.(*clocktesting.FakePassiveClock).SetTime(w.w.clock.Now().Add(time.Second))
		for _, fn := range afterFns {
			fn()
		}
	}()
	w.drainUnpausedWorkers()
}

// fake 实现不维护同步状态，直接返回空 map。
func (w *timeIncrementingWorkers) SyncKnownPods(logger klog.Logger, desiredPods []*v1.Pod) (knownPods map[types.UID]PodWorkerSync) {
	func() {
		w.lock.Lock()
		defer w.lock.Unlock()
		knownPods = w.w.SyncKnownPods(logger, desiredPods)
		w.w.clock.(*clocktesting.FakePassiveClock).SetTime(w.w.clock.Now().Add(time.Second))
	}()
	w.drainUnpausedWorkers()
	return
}

// 为指定 Pod 创建 hold channel，暂停其 worker 继续执行。
func (w *timeIncrementingWorkers) PauseWorkers(uids ...types.UID) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.holds == nil {
		w.holds = make(map[types.UID]chan struct{})
	}
	for _, uid := range uids {
		if _, ok := w.holds[uid]; !ok {
			w.holds[uid] = make(chan struct{})
		}
	}
}

// 释放指定 Pod 的 pause，允许其 worker 继续。
func (w *timeIncrementingWorkers) ReleaseWorkers(uids ...types.UID) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.ReleaseWorkersUnderLock(uids...)
}

// 在已持有锁的前提下关闭并删除 hold channel。
func (w *timeIncrementingWorkers) ReleaseWorkersUnderLock(uids ...types.UID) {
	for _, uid := range uids {
		if ch, ok := w.holds[uid]; ok {
			close(ch)
			delete(w.holds, uid)
		}
	}
}

// 阻塞直到指定 Pod 的 hold channel 被释放。
func (w *timeIncrementingWorkers) waitForPod(uid types.UID) {
	w.lock.Lock()
	ch, ok := w.holds[uid]
	w.lock.Unlock()
	if !ok {
		return
	}
	<-ch
}

// 轮询等待所有未被 pause 的 worker 完成当前工作。
func (w *timeIncrementingWorkers) drainUnpausedWorkers() {
	pausedWorkers := make(map[types.UID]struct{})
	for {
		for uid := range pausedWorkers {
			delete(pausedWorkers, uid)
		}
		stillWorking := false

		// 忽略被暂停的 worker
		w.lock.Lock()
		for uid := range w.holds {
			pausedWorkers[uid] = struct{}{}
		}
		w.lock.Unlock()

		// 检查是否至少有一个未暂停的 worker 仍在工作
		w.w.podLock.Lock()
		for uid, worker := range w.w.podSyncStatuses {
			if _, ok := pausedWorkers[uid]; ok {
				continue
			}
			if worker.working {
				stillWorking = true
				break
			}
		}
		w.w.podLock.Unlock()

		if !stillWorking {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

// 将 fake 时钟推进一秒。
func (w *timeIncrementingWorkers) tick() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.w.clock.(*clocktesting.FakePassiveClock).SetTime(w.w.clock.Now().Add(time.Second))
}

// 包装真实的 podWorkers，使每次 UpdatePod 和 worker 调用都推进 fake 时钟一秒，
// 便于观察单个 Pod 的连续状态变更。
func createTimeIncrementingPodWorkers(logger klog.Logger) (*timeIncrementingWorkers, map[types.UID][]syncPodRecord) {
	nested, runtime, processed := createPodWorkers(logger)
	w := &timeIncrementingWorkers{
		w:       nested,
		runtime: runtime,
	}
	nested.workerChannelFn = func(uid types.UID, in chan struct{}) <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			// TODO: 当前是主动循环；可以考虑仅在 ch 为空时从 in 惰性读取一次。
			for range in {
				w.waitForPod(uid)
				w.tick()
				ch <- struct{}{}
			}
		}()
		return ch
	}
	return w, processed
}

// 创建测试用 podWorkers，等价于使用默认 logger 的版本。
func createPodWorkers(logger klog.Logger) (*podWorkers, *containertest.FakeRuntime, map[types.UID][]syncPodRecord) {
	return createPodWorkersWithLogger(logger)
}

// 用 fake runtime/cache/queue 和可控时钟创建 podWorkers，
// 并把各阶段同步调用记录到 processed 中供测试断言。
func createPodWorkersWithLogger(logger klog.Logger) (*podWorkers, *containertest.FakeRuntime, map[types.UID][]syncPodRecord) {
	lock := sync.Mutex{}
	processed := make(map[types.UID][]syncPodRecord)
	fakeRecorder := &record.FakeRecorder{}
	fakeRuntime := &containertest.FakeRuntime{}
	fakeCache := containertest.NewFakeCache(fakeRuntime)
	fakeQueue := &fakeQueue{}
	clock := clocktesting.NewFakePassiveClock(time.Unix(1, 0))
	w := newPodWorkers(
		&podSyncerFuncs{
			syncPod: func(ctx context.Context, updateType kubetypes.SyncPodType, pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
				func() {
					lock.Lock()
					defer lock.Unlock()
					pod := pod
					processed[pod.UID] = append(processed[pod.UID], syncPodRecord{
						name:       pod.Name,
						updateType: updateType,
					})
				}()
				return false, nil, nil
			},
			syncTerminatingPod: func(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, gracePeriod *int64, podStatusFn func(*v1.PodStatus)) error {
				func() {
					lock.Lock()
					defer lock.Unlock()
					processed[pod.UID] = append(processed[pod.UID], syncPodRecord{
						name:        pod.Name,
						updateType:  kubetypes.SyncPodKill,
						gracePeriod: gracePeriod,
					})
				}()
				return nil
			},
			syncTerminatingRuntimePod: func(ctx context.Context, runningPod *kubecontainer.Pod) error {
				func() {
					lock.Lock()
					defer lock.Unlock()
					processed[runningPod.ID] = append(processed[runningPod.ID], syncPodRecord{
						name:       runningPod.Name,
						updateType: kubetypes.SyncPodKill,
						runningPod: runningPod,
					})
				}()
				return nil
			},
			syncTerminatedPod: func(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) error {
				func() {
					lock.Lock()
					defer lock.Unlock()
					processed[pod.UID] = append(processed[pod.UID], syncPodRecord{
						name:       pod.Name,
						terminated: true,
					})
				}()
				return nil
			},
		},
		fakeRecorder,
		fakeQueue,
		time.Second,
		time.Millisecond,
		fakeCache,
		allocation.NewInMemoryManager(logger, nil, nil, nil, nil, nil, nil),
	)
	workers := w.(*podWorkers)
	workers.clock = clock
	return workers, fakeRuntime, processed
}

// 等待编号 0..numPods-1 的 worker 全部完成当前工作。
func drainWorkers(podWorkers *podWorkers, numPods int) {
	for {
		stillWorking := false
		podWorkers.podLock.Lock()
		for i := 0; i < numPods; i++ {
			if s, ok := podWorkers.podSyncStatuses[types.UID(strconv.Itoa(i))]; ok && s.working {
				stillWorking = true
				break
			}
		}
		podWorkers.podLock.Unlock()
		if !stillWorking {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// 等待除指定 UID 外的所有 worker 完成当前工作。
func drainWorkersExcept(podWorkers *podWorkers, uids ...types.UID) {
	set := sets.New[string]()
	for _, uid := range uids {
		set.Insert(string(uid))
	}
	for {
		stillWorking := false
		podWorkers.podLock.Lock()
		for k, v := range podWorkers.podSyncStatuses {
			if set.Has(string(k)) {
				continue
			}
			if v.working {
				stillWorking = true
				break
			}
		}
		podWorkers.podLock.Unlock()
		if !stillWorking {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// 等待所有 worker 完成当前工作。
func drainAllWorkers(podWorkers *podWorkers) {
	for {
		stillWorking := false
		podWorkers.podLock.Lock()
		for _, worker := range podWorkers.podSyncStatuses {
			if worker.working {
				stillWorking = true
				break
			}
		}
		podWorkers.podLock.Unlock()
		if !stillWorking {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// 并发更新同一批 Pod，验证每个 Pod 最终都被处理，
// 且最后一次处理的总是最新版本。
func TestUpdatePodParallel(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, processed := createPodWorkers(logger)

	numPods := 20
	for i := 0; i < numPods; i++ {
		for j := i; j < numPods; j++ {
			podWorkers.UpdatePod(tCtx, UpdatePodOptions{
				Pod:        newNamedPod(strconv.Itoa(j), "ns", strconv.Itoa(i), false),
				UpdateType: kubetypes.SyncPodCreate,
			})
		}
	}
	drainWorkers(podWorkers, numPods)

	if len(processed) != numPods {
		t.Fatalf("Not all pods processed: %v", len(processed))
	}
	for i := 0; i < numPods; i++ {
		uid := types.UID(strconv.Itoa(i))
		events := processed[uid]
		if len(events) < 1 || len(events) > i+1 {
			t.Errorf("Pod %v processed %v times", i, len(events))
			continue
		}

		// PodWorker 保证最后一次更新一定会被处理，因此最后一条记录必须是最新版本。
		last := len(events) - 1
		if events[last].name != strconv.Itoa(i) {
			t.Errorf("Pod %v: incorrect order %v, %#v", i, last, events)
		}
	}
}

// 表驱动覆盖 UpdatePod 的状态机：新建、终止中重复创建、删除时间戳、驱逐、
// 终端 Pod、孤儿 RuntimePod 等场景下的 podSyncStatus 变化。
func TestUpdatePod(t *testing.T) {
	one := int64(1)
	hasCancelFn := func(status *podSyncStatus) *podSyncStatus {
		status.cancelFn = func() {}
		return status
	}
	withLabel := func(pod *v1.Pod, label, value string) *v1.Pod {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		pod.Labels[label] = value
		return pod
	}
	withDeletionTimestamp := func(pod *v1.Pod, ts time.Time, gracePeriod *int64) *v1.Pod {
		pod.DeletionTimestamp = &metav1.Time{Time: ts}
		pod.DeletionGracePeriodSeconds = gracePeriod
		return pod
	}
	intp := func(i int64) *int64 {
		return &i
	}
	expectPodSyncStatus := func(t *testing.T, expected, status *podSyncStatus) {
		t.Helper()
		// cancelFn 与 ctx 无法直接比较，先比较是否为 nil 再清零。
		if status != nil {
			clearLogger := func(opts *UpdatePodOptions) {
				if opts == nil {
					return
				}
			}
			clearLogger(expected.pendingUpdate)
			clearLogger(expected.activeUpdate)
			clearLogger(status.pendingUpdate)
			clearLogger(status.activeUpdate)

			if e, a := expected.cancelFn != nil, status.cancelFn != nil; e != a {
				t.Errorf("expected cancelFn %t, has cancelFn %t", e, a)
			} else {
				expected.cancelFn, status.cancelFn = nil, nil
			}
			expected.ctx, status.ctx = nil, nil
		}
		if e, a := expected, status; !reflect.DeepEqual(e, a) {
			t.Fatalf("unexpected status: %s", cmp.Diff(e, a, cmp.AllowUnexported(podSyncStatus{})))
		}
	}
	for _, tc := range []struct {
		name          string
		update        UpdatePodOptions
		runtimeStatus *kubecontainer.PodStatus
		prepare       func(t *testing.T, tCtx context.Context, w *timeIncrementingWorkers) (afterUpdateFn func())

		expect                *podSyncStatus
		expectBeforeWorker    *podSyncStatus
		expectKnownTerminated bool
	}{
		{
			name: "a new pod is recorded and started",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodCreate,
				Pod:        newNamedPod("1", "ns", "running-pod", false),
			},
			expect: hasCancelFn(&podSyncStatus{
				fullname:  "running-pod_ns",
				syncedAt:  time.Unix(1, 0),
				startedAt: time.Unix(3, 0),
				activeUpdate: &UpdatePodOptions{
					Pod: newNamedPod("1", "ns", "running-pod", false),
				},
			}),
		},
		{
			name: "a new pod is recorded and started unless it is a duplicate of an existing terminating pod UID",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodCreate,
				Pod:        withLabel(newNamedPod("1", "ns", "running-pod", false), "updated", "value"),
			},
			prepare: func(t *testing.T, tCtx context.Context, w *timeIncrementingWorkers) func() {
				w.UpdatePod(tCtx, UpdatePodOptions{
					UpdateType: kubetypes.SyncPodCreate,
					Pod:        newNamedPod("1", "ns", "running-pod", false),
				})
				w.PauseWorkers("1")
				w.UpdatePod(tCtx, UpdatePodOptions{
					UpdateType: kubetypes.SyncPodKill,
					Pod:        newNamedPod("1", "ns", "running-pod", false),
				})
				return func() { w.ReleaseWorkersUnderLock("1") }
			},
			expect: hasCancelFn(&podSyncStatus{
				fullname:           "running-pod_ns",
				syncedAt:           time.Unix(1, 0),
				startedAt:          time.Unix(3, 0),
				terminatingAt:      time.Unix(3, 0),
				terminatedAt:       time.Unix(6, 0),
				gracePeriod:        30,
				startedTerminating: true,
				restartRequested:   true, // 因为终止期间收到了 create
				finished:           true,
				activeUpdate: &UpdatePodOptions{
					Pod:            newNamedPod("1", "ns", "running-pod", false),
					KillPodOptions: &KillPodOptions{PodTerminationGracePeriodSecondsOverride: intp(30)},
				},
			}),
			expectKnownTerminated: true,
		},
		{
			name: "a new pod is recorded and started and running pod is ignored",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodCreate,
				Pod:        newNamedPod("1", "ns", "running-pod", false),
				RunningPod: &kubecontainer.Pod{ID: "1", Name: "orphaned-pod", Namespace: "ns"},
			},
			expect: hasCancelFn(&podSyncStatus{
				fullname:  "running-pod_ns",
				syncedAt:  time.Unix(1, 0),
				startedAt: time.Unix(3, 0),
				activeUpdate: &UpdatePodOptions{
					Pod: newNamedPod("1", "ns", "running-pod", false),
				},
			}),
		},
		{
			name: "a running pod is terminated when an update contains a deletionTimestamp",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodUpdate,
				Pod:        withDeletionTimestamp(newNamedPod("1", "ns", "running-pod", false), time.Unix(1, 0), intp(15)),
			},
			prepare: func(t *testing.T, tCtx context.Context, w *timeIncrementingWorkers) func() {
				w.UpdatePod(tCtx, UpdatePodOptions{
					UpdateType: kubetypes.SyncPodCreate,
					Pod:        newNamedPod("1", "ns", "running-pod", false),
				})
				return nil
			},
			expect: hasCancelFn(&podSyncStatus{
				fullname:           "running-pod_ns",
				syncedAt:           time.Unix(1, 0),
				startedAt:          time.Unix(3, 0),
				terminatingAt:      time.Unix(3, 0),
				terminatedAt:       time.Unix(5, 0),
				gracePeriod:        15,
				startedTerminating: true,
				finished:           true,
				deleted:            true,
				activeUpdate: &UpdatePodOptions{
					Pod:            withDeletionTimestamp(newNamedPod("1", "ns", "running-pod", false), time.Unix(1, 0), intp(15)),
					KillPodOptions: &KillPodOptions{PodTerminationGracePeriodSecondsOverride: intp(15)},
				},
			}),
			expectKnownTerminated: true,
		},
		{
			name: "a running pod is terminated when an eviction is requested",
			update: UpdatePodOptions{
				UpdateType:     kubetypes.SyncPodKill,
				Pod:            newNamedPod("1", "ns", "running-pod", false),
				KillPodOptions: &KillPodOptions{Evict: true},
			},
			prepare: func(t *testing.T, tCtx context.Context, w *timeIncrementingWorkers) func() {
				w.UpdatePod(tCtx, UpdatePodOptions{
					UpdateType: kubetypes.SyncPodCreate,
					Pod:        newNamedPod("1", "ns", "running-pod", false),
				})
				return nil
			},
			expect: hasCancelFn(&podSyncStatus{
				fullname:           "running-pod_ns",
				syncedAt:           time.Unix(1, 0),
				startedAt:          time.Unix(3, 0),
				terminatingAt:      time.Unix(3, 0),
				terminatedAt:       time.Unix(5, 0),
				gracePeriod:        30,
				startedTerminating: true,
				finished:           true,
				evicted:            true,
				activeUpdate: &UpdatePodOptions{
					Pod: newNamedPod("1", "ns", "running-pod", false),
					KillPodOptions: &KillPodOptions{
						PodTerminationGracePeriodSecondsOverride: intp(30),
						Evict:                                    true,
					},
				},
			}),
			expectKnownTerminated: true,
		},
		{
			name: "a pod that is terminal and has never started must be terminated if the runtime does not have a cached terminal state",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodCreate,
				Pod:        newPodWithPhase("1", "done-pod", v1.PodSucceeded),
			},
			expect: hasCancelFn(&podSyncStatus{
				fullname:      "done-pod_ns",
				syncedAt:      time.Unix(1, 0),
				terminatingAt: time.Unix(1, 0),
				startedAt:     time.Unix(3, 0),
				terminatedAt:  time.Unix(3, 0),
				activeUpdate: &UpdatePodOptions{
					Pod:            newPodWithPhase("1", "done-pod", v1.PodSucceeded),
					KillPodOptions: &KillPodOptions{PodTerminationGracePeriodSecondsOverride: intp(30)},
				},
				gracePeriod:        30,
				startedTerminating: true,
				finished:           true,
			}),
			expectKnownTerminated: true,
		},
		{
			name: "a pod that is terminal and has never started advances to finished if the runtime has a cached terminal state",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodCreate,
				Pod:        newPodWithPhase("1", "done-pod", v1.PodSucceeded),
			},
			runtimeStatus: &kubecontainer.PodStatus{ /* 已知该 Pod */ },
			expectBeforeWorker: &podSyncStatus{
				fullname:      "done-pod_ns",
				syncedAt:      time.Unix(1, 0),
				terminatingAt: time.Unix(1, 0),
				terminatedAt:  time.Unix(1, 0),
				pendingUpdate: &UpdatePodOptions{
					UpdateType: kubetypes.SyncPodCreate,
					Pod:        newPodWithPhase("1", "done-pod", v1.PodSucceeded),
				},
				finished:           false, // 初始应标记为未完成（确保 `SyncTerminatedPod` 会运行），随后状态进展为已终止。
				startedTerminating: true,
				working:            true,
			},
			expect: hasCancelFn(&podSyncStatus{
				fullname:           "done-pod_ns",
				syncedAt:           time.Unix(1, 0),
				terminatingAt:      time.Unix(1, 0),
				terminatedAt:       time.Unix(1, 0),
				startedAt:          time.Unix(3, 0),
				startedTerminating: true,
				finished:           true,
				activeUpdate: &UpdatePodOptions{
					UpdateType: kubetypes.SyncPodSync,
					Pod:        newPodWithPhase("1", "done-pod", v1.PodSucceeded),
				},

				// 之前从未见过该 Pod，因此不存在“重启”语义。
				restartRequested: false,
			}),
			expectKnownTerminated: true,
		},
		{
			name: "an orphaned running pod we have not seen is marked terminating and advances to finished and then is removed",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodKill,
				RunningPod: &kubecontainer.Pod{ID: "1", Name: "orphaned-pod", Namespace: "ns"},
			},
			expectBeforeWorker: &podSyncStatus{
				fullname:      "orphaned-pod_ns",
				syncedAt:      time.Unix(1, 0),
				terminatingAt: time.Unix(1, 0),
				pendingUpdate: &UpdatePodOptions{
					UpdateType:     kubetypes.SyncPodKill,
					RunningPod:     &kubecontainer.Pod{ID: "1", Name: "orphaned-pod", Namespace: "ns"},
					KillPodOptions: &KillPodOptions{PodTerminationGracePeriodSecondsOverride: &one},
				},
				gracePeriod:     1,
				deleted:         true,
				observedRuntime: true,
				working:         true,
			},
			// 运行中的孤儿 Pod 终止后会被移出历史；由于它本来就不在期望 Pod 集合中，
			// 因此这里特意期望外部查询返回“未知”。
			expectKnownTerminated: false,
		},
		{
			name: "an orphaned running pod with a non-kill update type does nothing",
			update: UpdatePodOptions{
				UpdateType: kubetypes.SyncPodCreate,
				RunningPod: &kubecontainer.Pod{ID: "1", Name: "orphaned-pod", Namespace: "ns"},
			},
			expect: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var uid types.UID
			switch {
			case tc.update.Pod != nil:
				uid = tc.update.Pod.UID
			case tc.update.RunningPod != nil:
				uid = tc.update.RunningPod.ID
			default:
				t.Fatalf("unable to find uid for update")
			}

			var fns []func()

			logger, tCtx := ktesting.NewTestContext(t)
			podWorkers, _ := createTimeIncrementingPodWorkers(logger)

			if tc.expectBeforeWorker != nil {
				fns = append(fns, func() {
					expectPodSyncStatus(t, tc.expectBeforeWorker, podWorkers.w.podSyncStatuses[uid])
				})
			}

			if tc.prepare != nil {
				if fn := tc.prepare(t, tCtx, podWorkers); fn != nil {
					fns = append(fns, fn)
				}
			}

			// 预先设置 UpdatePod 阶段可见的 runtime 状态；worker 实际调用 podCache 前会再清空。
			if tc.runtimeStatus != nil {
				podWorkers.runtime.PodStatus = *tc.runtimeStatus
				podWorkers.runtime.Err = nil
			} else {
				podWorkers.runtime.PodStatus = kubecontainer.PodStatus{}
				podWorkers.runtime.Err = status.Error(codes.NotFound, "No such pod")
			}
			fns = append(fns, func() {
				podWorkers.runtime.PodStatus = kubecontainer.PodStatus{}
				podWorkers.runtime.Err = nil
			})

			podWorkers.UpdatePod(tCtx, tc.update, fns...)

			if podWorkers.w.IsPodKnownTerminated(uid) != tc.expectKnownTerminated {
				t.Errorf("podWorker.IsPodKnownTerminated expected to be %t", tc.expectKnownTerminated)
			}

			expectPodSyncStatus(t, tc.expect, podWorkers.w.podSyncStatuses[uid])

			// TODO: 根据测试用例校验 Pod 的处理记录，以减少 kubelet_pods_test.go 中的测试量。
		})
	}
}

// 验证 completeWork 根据同步结果选择不同的重新入队延迟：
// 阶段转换立即重排、普通错误走默认退避、BackoffError 使用指定时间、
// 网络未就绪使用瞬时错误退避，并限制最大延迟。
func TestCompleteWork_Enqueue(t *testing.T) {
	const noJitter = 0.0

	defaultBackoff := 10 * time.Second
	resyncInterval := 20 * time.Second
	clock := clocktesting.NewFakePassiveClock(time.Unix(1, 0))

	testCases := []struct {
		name            string
		phaseTransition bool
		syncErr         error
		expectedMin     time.Duration
		jitterFactor    float64
	}{
		{
			name:            "phase transition requeues for immediate processing",
			phaseTransition: true,
			expectedMin:     0,
			jitterFactor:    noJitter,
		},
		{
			name:         "no error uses regular resync interval",
			syncErr:      nil,
			expectedMin:  resyncInterval,
			jitterFactor: workerResyncIntervalJitterFactor,
		},
		{
			name:         "generic error uses default backoff",
			syncErr:      errors.New("generic error"),
			expectedMin:  defaultBackoff,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name: "BackoffError uses error's backoff",
			syncErr: kubecontainer.NewBackoffError(
				errors.New("backoff error"),
				clock.Now().Add(5*time.Second),
			),
			expectedMin:  5 * time.Second,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name: "Aggregate error with one BackoffError uses its backoff",
			syncErr: utilerrors.NewAggregate([]error{
				errors.New("some other error"),
				kubecontainer.NewBackoffError(
					errors.New("backoff error in aggregate"),
					clock.Now().Add(7*time.Second),
				),
			}),
			expectedMin:  7 * time.Second,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name: "Aggregate error with multiple BackoffErrors uses minimum backoff",
			syncErr: utilerrors.NewAggregate([]error{
				kubecontainer.NewBackoffError(
					errors.New("backoff error 1"),
					clock.Now().Add(10*time.Second),
				),
				kubecontainer.NewBackoffError(
					errors.New("backoff error 2"),
					clock.Now().Add(3*time.Second),
				),
			}),
			expectedMin:  3 * time.Second,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name:         "BackoffError in the past enqueues for immediate processing with jitter",
			syncErr:      kubecontainer.NewBackoffError(errors.New("backoff error"), clock.Now().Add(-5*time.Second)),
			expectedMin:  0,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name:         "Excessively long backoff duration enqueues for the maximum allowed",
			syncErr:      kubecontainer.NewBackoffError(errors.New("backoff error"), clock.Now().Add(resyncInterval*2)),
			expectedMin:  resyncInterval,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name:         "NetworkNotReadyError uses backOffOnTransientErrorPeriod",
			syncErr:      errors.New(NetworkNotReadyErrorMsg),
			expectedMin:  backOffOnTransientErrorPeriod,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name: "Aggregate with NetworkNotReadyError",
			syncErr: utilerrors.NewAggregate([]error{
				errors.New("some other error"),
				errors.New(NetworkNotReadyErrorMsg),
			}),
			expectedMin:  backOffOnTransientErrorPeriod,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
		{
			name: "Aggregate with NetworkNotReadyError and BackoffError",
			syncErr: utilerrors.NewAggregate([]error{
				errors.New("some other error"),
				errors.New(NetworkNotReadyErrorMsg),
				kubecontainer.NewBackoffError(
					errors.New("backoff error 2"),
					clock.Now().Add(3*time.Second),
				),
			}),
			expectedMin:  backOffOnTransientErrorPeriod,
			jitterFactor: workerBackOffPeriodJitterFactor,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			podWorkers, _, _ := createPodWorkers(logger)
			podWorkers.clock = clock
			fakeQueue := podWorkers.workQueue.(*fakeQueue)
			podUID := types.UID("12345")

			podWorkers.resyncInterval = resyncInterval
			podWorkers.backOffPeriod = defaultBackoff
			podWorkers.podSyncStatuses[podUID] = &podSyncStatus{}
			podWorkers.completeWork(logger, podUID, tc.phaseTransition, tc.syncErr)

			if fakeQueue.Empty() {
				t.Fatalf("work queue should not be empty")
			}
			items := fakeQueue.Items()
			if len(items) != 1 {
				t.Fatalf("expected 1 item in queue, got %d", len(items))
			}
			item := items[0]
			if item.UID != podUID {
				t.Errorf("expected pod UID %q, got %q", podUID, item.UID)
			}

			expectedMax := tc.expectedMin + time.Duration(float64(tc.expectedMin)*tc.jitterFactor)
			if item.Delay < tc.expectedMin || item.Delay > expectedMax {
				t.Errorf("expected delay in range [%v, %v], got %v", tc.expectedMin, expectedMax, item.Delay)
			}
		})
	}
}

// 验证 completeWork 在无 pendingUpdate 时清除 working 状态，
// 有 pendingUpdate 时向 podUpdates channel 投递唤醒信号。
func TestCompleteWork_PendingUpdate(t *testing.T) {
	podUID := types.UID("pod-with-pending-update-check")

	t.Run("with nil pendingUpdate, clears working status", func(t *testing.T) {
		logger, _ := ktesting.NewTestContext(t)
		p := &podWorkers{
			podSyncStatuses: make(map[types.UID]*podSyncStatus),
			workQueue:       &fakeQueue{},
		}
		p.podSyncStatuses[podUID] = &podSyncStatus{working: true, pendingUpdate: nil}

		p.completeWork(logger, podUID, false, nil)

		p.podLock.Lock()
		defer p.podLock.Unlock()
		if p.podSyncStatuses[podUID].working {
			t.Error("expected pod status 'working' to be false, but it was true")
		}
	})

	t.Run("with non-nil pendingUpdate, queues an update signal", func(t *testing.T) {
		logger, _ := ktesting.NewTestContext(t)
		p := &podWorkers{
			podSyncStatuses: make(map[types.UID]*podSyncStatus),
			podUpdates:      make(map[types.UID]chan struct{}),
			workQueue:       &fakeQueue{},
		}
		p.podUpdates[podUID] = make(chan struct{}, 1)

		dummyUpdate := &UpdatePodOptions{
			Pod: newNamedPod("1", "ns", "running-pod", false),
		}
		p.podSyncStatuses[podUID] = &podSyncStatus{working: true, pendingUpdate: dummyUpdate}

		p.completeWork(logger, podUID, false, nil)

		select {
		case <-p.podUpdates[podUID]:
			// 成功！通道按预期收到信号。
		default:
			t.Fatal("expected an update to be queued on the podUpdates channel, but none was received")
		}
	})
}

// 验证非 kill 类型的 RuntimePod 更新被忽略，
// 而 kill 更新会合成 Pod 并触发终止同步。
func TestUpdatePodForRuntimePod(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, processed := createPodWorkers(logger)

	// 非 kill 类型的 RuntimePod 更新应被忽略。
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		UpdateType: kubetypes.SyncPodCreate,
		RunningPod: &kubecontainer.Pod{ID: "1", Name: "1", Namespace: "test"},
	})
	drainAllWorkers(podWorkers)
	if len(processed) != 0 {
		t.Fatalf("Not all pods processed: %v", len(processed))
	}

	// kill 类型的 RuntimePod 会合成一个 Pod 并触发终止同步。
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		UpdateType: kubetypes.SyncPodKill,
		RunningPod: &kubecontainer.Pod{ID: "1", Name: "1", Namespace: "test"},
	})
	drainAllWorkers(podWorkers)
	if len(processed) != 1 {
		t.Fatalf("Not all pods processed: %v", processed)
	}
	updates := processed["1"]
	if len(updates) != 1 {
		t.Fatalf("unexpected updates: %v", updates)
	}
	if updates[0].runningPod == nil || updates[0].updateType != kubetypes.SyncPodKill || updates[0].name != "1" {
		t.Fatalf("unexpected update: %v", updates)
	}
}

// 验证对已终止 RuntimePod 的 kill 更新不会重复触发同步。
func TestUpdatePodForTerminatedRuntimePod(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, processed := createPodWorkers(logger)

	now := time.Now()
	podWorkers.podSyncStatuses[types.UID("1")] = &podSyncStatus{
		startedTerminating: true,
		terminatedAt:       now.Add(-time.Second),
		terminatingAt:      now.Add(-2 * time.Second),
		gracePeriod:        1,
	}

	// 创建合成 Pod
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		UpdateType: kubetypes.SyncPodKill,
		RunningPod: &kubecontainer.Pod{ID: "1", Name: "1", Namespace: "test"},
	})
	drainAllWorkers(podWorkers)
	if len(processed) != 0 {
		t.Fatalf("Not all pods processed: %v", processed)
	}
	updates := processed["1"]
	if len(updates) != 0 {
		t.Fatalf("unexpected updates: %v", updates)
	}
}

// 验证 create、kill、update 连续到达后 kill 事件不会被后续 update 丢弃。
func TestUpdatePodDoesNotForgetSyncPodKill(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, processed := createPodWorkers(logger)
	numPods := 20
	for i := 0; i < numPods; i++ {
		pod := newNamedPod(strconv.Itoa(i), "ns", strconv.Itoa(i), false)
		podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        pod,
			UpdateType: kubetypes.SyncPodCreate,
		})
		podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        pod,
			UpdateType: kubetypes.SyncPodKill,
		})
		podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        pod,
			UpdateType: kubetypes.SyncPodUpdate,
		})
	}
	drainWorkers(podWorkers, numPods)
	if len(processed) != numPods {
		t.Errorf("Not all pods processed: %v", len(processed))
		return
	}
	for i := 0; i < numPods; i++ {
		uid := types.UID(strconv.Itoa(i))
		// 每个 Pod 应被处理两到三次（kill、terminate 或 create、kill、terminate）：
		// pending 更新会被缓冲，worker 可能把 create 与 kill 合并。
		syncPodRecords := processed[uid]
		var match bool
		grace := int64(30)
		for _, possible := range [][]syncPodRecord{
			{{name: string(uid), updateType: kubetypes.SyncPodKill, gracePeriod: &grace}, {name: string(uid), terminated: true}},
			{{name: string(uid), updateType: kubetypes.SyncPodCreate}, {name: string(uid), updateType: kubetypes.SyncPodKill, gracePeriod: &grace}, {name: string(uid), terminated: true}},
		} {
			if reflect.DeepEqual(possible, syncPodRecords) {
				match = true
				break
			}
		}
		if !match {
			t.Fatalf("unexpected history for pod %v: %#v", i, syncPodRecords)
		}
	}
}

// 把 UID 列表转换为字符串集合，便于比较。
func newUIDSet(uids ...types.UID) sets.Set[string] {
	set := sets.New[string]()
	for _, uid := range uids {
		set.Insert(string(uid))
	}
	return set
}

type terminalPhaseSync struct {
	lock     sync.Mutex
	fn       syncPodFnType
	terminal sets.Set[string]
}

// SyncPod 调用底层同步函数，并在其未判定终端时叠加测试预设的终端标记，
// 用于精确控制 Pod 进入终端阶段的时机。
func (s *terminalPhaseSync) SyncPod(ctx context.Context, updateType kubetypes.SyncPodType, pod *v1.Pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
	isTerminal, _, err := s.fn(ctx, updateType, pod, mirrorPod, podStatus)
	if err != nil {
		return false, nil, err
	}
	if !isTerminal {
		s.lock.Lock()
		defer s.lock.Unlock()
		isTerminal = s.terminal.Has(string(pod.UID))
	}
	return isTerminal, nil, nil
}

// 把指定 UID 标记为终端阶段。
func (s *terminalPhaseSync) SetTerminal(uid types.UID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.terminal.Insert(string(uid))
}

// 创建带终端标记集合的包装同步器。
func newTerminalPhaseSync(fn syncPodFnType) *terminalPhaseSync {
	return &terminalPhaseSync{
		fn:       fn,
		terminal: sets.New[string](),
	}
}

// 验证 Pod 从运行到终止请求、再到终止状态的阶段转换。
func TestTerminalPhaseTransition(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, _ := createPodWorkers(logger)
	var channels WorkChannel
	podWorkers.workerChannelFn = channels.Intercept
	terminalPhaseSyncer := newTerminalPhaseSync(podWorkers.podSyncer.(*podSyncerFuncs).syncPod)
	podWorkers.podSyncer.(*podSyncerFuncs).syncPod = terminalPhaseSyncer.SyncPod

	// 启动 Pod
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("1", "test1", "pod1", false),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 应观察到 Pod 正在运行
	pod1 := podWorkers.podSyncStatuses[types.UID("1")]
	if pod1.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod1)
	}

	// 向 Pod 发送另一条更新
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("1", "test1", "pod1", false),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 应观察到 Pod 仍在运行
	pod1 = podWorkers.podSyncStatuses[types.UID("1")]
	if pod1.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod1)
	}

	// 下一次同步应使状态转换为终止
	terminalPhaseSyncer.SetTerminal(types.UID("1"))
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("1", "test1", "pod1", false),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 应观察到 Pod 正在终止
	pod1 = podWorkers.podSyncStatuses[types.UID("1")]
	if !pod1.IsTerminationRequested() || !pod1.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod1)
	}
}

// 验证同名 static Pod 的互斥启动语义：同一时刻只允许一个 static Pod 启动，
// 其余排队等待；删除、终止或 config 变化后的清理与重启也要符合预期。
func TestStaticPodExclusion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, processed := createPodWorkers(logger)
	var channels WorkChannel
	podWorkers.workerChannelFn = channels.Intercept

	testPod := newNamedPod("2-static", "test1", "pod1", true)
	if !kubetypes.IsStaticPod(testPod) {
		t.Fatalf("unable to test static pod")
	}

	// 启动两个同名 Pod，一个为静态 Pod，一个来自 API 服务器
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("1-normal", "test1", "pod1", false),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("2-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 应观察到两个 Pod 都在运行
	pod1 := podWorkers.podSyncStatuses[types.UID("1-normal")]
	if pod1.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod1)
	}
	pod2 := podWorkers.podSyncStatuses[types.UID("2-static")]
	if pod2.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod2)
	}

	if len(processed) != 2 {
		t.Fatalf("unexpected synced pods: %#v", processed)
	}
	if e, a :=
		[]syncPodRecord{{name: "pod1", updateType: kubetypes.SyncPodUpdate}},
		processed[types.UID("2-static")]; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(e, a))
	}
	if e, a := map[string]types.UID{"pod1_test1": "2-static"}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}

	// 尝试启动第二个和第三个静态 Pod，它们不应启动
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("3-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("4-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 应观察到两个 Pod 在运行，但最后一个 Pod 尚未同步
	pod1 = podWorkers.podSyncStatuses[types.UID("1-normal")]
	if pod1.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod1)
	}
	pod2 = podWorkers.podSyncStatuses[types.UID("2-static")]
	if pod2.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod2)
	}
	pod3 := podWorkers.podSyncStatuses[types.UID("3-static")]
	if pod3.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod3)
	}
	pod4 := podWorkers.podSyncStatuses[types.UID("4-static")]
	if pod4.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod4)
	}

	if len(processed) != 2 {
		t.Fatalf("unexpected synced pods: %#v", processed)
	}
	if expected, actual :=
		[]syncPodRecord{{name: "pod1", updateType: kubetypes.SyncPodUpdate}},
		processed[types.UID("2-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}
	if expected, actual :=
		[]syncPodRecord(nil),
		processed[types.UID("3-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}
	if expected, actual :=
		[]syncPodRecord(nil),
		processed[types.UID("4-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}
	if e, a := map[string]types.UID{"pod1_test1": "2-static"}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	if e, a := map[string][]types.UID{"pod1_test1": {"3-static", "4-static"}}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}
	// 确认所有 Pod 均已入队
	if e, a := sets.New[string]("1-normal", "2-static", "4-static", "3-static"), podWorkers.workQueue.(*fakeQueue).Set(); !e.Equal(a) {
		t.Fatalf("unexpected queued items: %s", cmp.Diff(e, a))
	}

	// 向 3-static 发送基本更新
	podWorkers.workQueue.GetWork()
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("3-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 2-static 仍在运行，因此不应启动 3-static
	if e, a := map[string]types.UID{"pod1_test1": "2-static"}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	if e, a := map[string][]types.UID{"pod1_test1": {"3-static", "4-static"}}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}
	// 队列中应仅有 3-static（表示稍后需要重试）
	if e, a := sets.New[string]("3-static"), newUIDSet(podWorkers.workQueue.GetWork()...); !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected queued items: %s", cmp.Diff(e, a))
	}

	// 在 2-static 仍运行时将 3-static 标记为已删除
	podWorkers.workQueue.GetWork()
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("3-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodKill,
	})
	drainAllWorkers(podWorkers)

	// 3-static 从未启动，因此应观察到它已终止，其他状态保持不变
	pod3 = podWorkers.podSyncStatuses[types.UID("3-static")]
	if !pod3.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod3)
	}
	// worker 已完成，因此队列应为空
	if e, a := sets.New[string](), newUIDSet(podWorkers.workQueue.GetWork()...); !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected queued items: %s", cmp.Diff(e, a))
	}
	// 2-static 仍在运行
	if e, a := map[string]types.UID{"pod1_test1": "2-static"}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	// 3-static 和 4-static 仍在队列中
	if e, a := map[string][]types.UID{"pod1_test1": {"3-static", "4-static"}}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}

	// 终止 2-static
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("2-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodKill,
	})
	drainAllWorkers(podWorkers)

	// 应观察到 2-static 已终止，且不再将其上报为已启动的静态 Pod
	pod2 = podWorkers.podSyncStatuses[types.UID("2-static")]
	if !pod2.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod3)
	}
	if e, a := map[string]types.UID{}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	if e, a := map[string][]types.UID{"pod1_test1": {"3-static", "4-static"}}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}

	// 模拟工作队列向 4-static 发送周期事件
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("4-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 3-static 已终止，因此应启动 4-static
	pod4 = podWorkers.podSyncStatuses[types.UID("4-static")]
	if pod4.IsTerminated() {
		t.Fatalf("unexpected pod state: %#v", pod3)
	}
	if e, a := map[string]types.UID{"pod1_test1": "4-static"}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	if e, a := map[string][]types.UID{}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}

	// 对全部剩余 Pod 发起同步
	state := podWorkers.SyncKnownPods(logger, []*v1.Pod{
		newNamedPod("1-normal", "test1", "pod1", false),
		newNamedPod("2-static", "test1", "pod1", true),
		newNamedPod("3-static", "test1", "pod1", true),
		newNamedPod("4-static", "test1", "pod1", true),
	})
	drainAllWorkers(podWorkers)

	// 2-static 和 3-static 均应被列为已终止
	if e, a := map[types.UID]PodWorkerSync{
		"1-normal": {State: SyncPod, HasConfig: true},
		"2-static": {State: TerminatedPod, HasConfig: true, Static: true},
		"3-static": {State: TerminatedPod},
		"4-static": {State: SyncPod, HasConfig: true, Static: true},
	}, state; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected actual state: %s", cmp.Diff(e, a))
	}
	// 3-static 仍在配置中，因此应保留其状态
	if status, ok := podWorkers.podSyncStatuses["3-static"]; !ok || status.terminatedAt.IsZero() || !status.finished || status.working {
		t.Fatalf("unexpected post termination status: %#v", status)
	}

	// 移除 3-static 后发起同步
	state = podWorkers.SyncKnownPods(logger, []*v1.Pod{
		newNamedPod("1-normal", "test1", "pod1", false),
		newNamedPod("2-static", "test1", "pod1", true),
		newNamedPod("4-static", "test1", "pod1", true),
	})
	drainAllWorkers(podWorkers)

	// 预期同步将 3-static 置为最终状态并移除其状态记录
	if e, a := map[types.UID]PodWorkerSync{
		"1-normal": {State: SyncPod, HasConfig: true},
		"2-static": {State: TerminatedPod, HasConfig: true, Static: true},
		"4-static": {State: SyncPod, HasConfig: true, Static: true},
	}, state; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected actual state: %s", cmp.Diff(e, a))
	}
	if status, ok := podWorkers.podSyncStatuses["3-static"]; ok {
		t.Fatalf("unexpected post termination status: %#v", status)
	}

	// 启动一个静态 Pod，将其终止后再添加另一个，但确保 Pod 5 的 worker
	// 看不到 kill 事件（因此它仍在等待启动）
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("5-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	// 等待先前的工作交付给 worker
	drainAllWorkers(podWorkers)
	channels.Channel("5-static").Hold()
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("5-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodKill,
	})
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("6-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainWorkersExcept(podWorkers, "5-static")

	// Pod 5 应已请求终止，但尚未完成清理
	pod5 := podWorkers.podSyncStatuses[types.UID("5-static")]
	if !pod5.IsTerminationRequested() || pod5.IsTerminated() {
		t.Fatalf("unexpected status for pod 5: %#v", pod5)
	}
	if e, a := map[string]types.UID{"pod1_test1": "4-static"}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	if e, a := map[string][]types.UID{"pod1_test1": {"5-static", "6-static"}}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}

	// 终止 4-static 并唤醒 6-static
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("4-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodKill,
	})
	drainWorkersExcept(podWorkers, "5-static")
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("6-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainWorkersExcept(podWorkers, "5-static")

	// 5-static 应仍在等待，6-static 应已启动并同步
	pod5 = podWorkers.podSyncStatuses[types.UID("5-static")]
	if !pod5.IsTerminationRequested() || pod5.IsTerminated() {
		t.Fatalf("unexpected status for pod 5: %#v", pod5)
	}
	if e, a := map[string]types.UID{"pod1_test1": "6-static"}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	// 不应有静态 Pod 等待启动
	if e, a := map[string][]types.UID{}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}
	// 确认 6-static 已同步
	if expected, actual :=
		[]syncPodRecord{{name: "pod1", updateType: kubetypes.SyncPodUpdate}},
		processed[types.UID("6-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}

	// 确认乱序交付事件时 5-static 会退出
	channels.Channel("5-static").Release()
	drainAllWorkers(podWorkers)
	pod5 = podWorkers.podSyncStatuses[types.UID("5-static")]
	if !pod5.IsTerminated() {
		t.Fatalf("unexpected status for pod 5: %#v", pod5)
	}

	// 再启动三个静态 Pod，终止此前阻塞启动的静态 Pod，
	// 并模拟三个 Pod 中的第二个（8）先运行
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("7-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("8-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("9-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("6-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodKill,
	})
	drainAllWorkers(podWorkers)
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("6-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodCreate,
	})
	drainAllWorkers(podWorkers)
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("8-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 6 应已被识别为可重启
	if status := podWorkers.podSyncStatuses["6-static"]; !status.restartRequested {
		t.Fatalf("unexpected restarted static pod: %#v", status)
	}
	// 7 和 8 应仍在等待，且尚未同步
	if e, a := map[string]types.UID{}, podWorkers.startedStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected started static pods: %s", cmp.Diff(e, a))
	}
	// 当前仅 7-static 可以启动，但它尚未收到事件
	if e, a := map[string][]types.UID{"pod1_test1": {"7-static", "8-static", "9-static"}}, podWorkers.waitingToStartStaticPodsByFullname; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected waiting static pods: %s", cmp.Diff(e, a))
	}
	// 新 Pod 均尚未同步
	if expected, actual :=
		[]syncPodRecord(nil),
		processed[types.UID("7-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}
	if expected, actual :=
		[]syncPodRecord(nil),
		processed[types.UID("8-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}
	if expected, actual :=
		[]syncPodRecord(nil),
		processed[types.UID("9-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}

	// 终止 7-static 并唤醒 8-static
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("7-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodKill,
	})
	drainAllWorkers(podWorkers)
	podWorkers.UpdatePod(tCtx, UpdatePodOptions{
		Pod:        newNamedPod("8-static", "test1", "pod1", true),
		UpdateType: kubetypes.SyncPodUpdate,
	})
	drainAllWorkers(podWorkers)

	// 8 应已同步
	if expected, actual :=
		[]syncPodRecord{{name: "pod1", updateType: kubetypes.SyncPodUpdate}},
		processed[types.UID("8-static")]; !reflect.DeepEqual(expected, actual) {
		t.Fatalf("unexpected sync pod calls: %s", cmp.Diff(expected, actual))
	}

	// 发起同步，仅保留 8-static 为期望 Pod
	state = podWorkers.SyncKnownPods(logger, []*v1.Pod{
		newNamedPod("8-static", "test1", "pod1", true),
	})
	drainAllWorkers(podWorkers)
	if e, a := map[types.UID]PodWorkerSync{
		"1-normal": {State: TerminatingPod, Orphan: true, HasConfig: true},
		"8-static": {State: SyncPod, HasConfig: true, Static: true},
	}, state; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected actual restartable: %s", cmp.Diff(e, a))
	}
}

// WorkChannelItem 封装单个 Pod worker 的工作通道及其暂停队列。
type WorkChannelItem struct {
	out   chan struct{}
	lock  sync.Mutex
	pause bool
	queue int
}

// 向 out channel 投递一次工作信号；暂停时先累计到队列。
func (item *WorkChannelItem) Handle() {
	item.lock.Lock()
	defer item.lock.Unlock()
	if item.pause {
		item.queue++
		return
	}
	item.out <- struct{}{}
}

// 暂停该 channel 的工作投递。
func (item *WorkChannelItem) Hold() {
	item.lock.Lock()
	defer item.lock.Unlock()
	item.pause = true
}

// 关闭 out channel 并清空引用。
func (item *WorkChannelItem) Close() {
	item.lock.Lock()
	defer item.lock.Unlock()
	if item.out != nil {
		close(item.out)
		item.out = nil
	}
}

// 恢复投递并一次性补发暂停期间累计的工作信号。
func (item *WorkChannelItem) Release() {
	item.lock.Lock()
	defer item.lock.Unlock()
	item.pause = false
	for i := 0; i < item.queue; i++ {
		item.out <- struct{}{}
	}
	item.queue = 0
}

// WorkChannel 拦截 pod worker 与其子 goroutine 之间的 podWork channel，
// 让测试可以暂停或恢复工作信号，从而精确控制并发时序。
type WorkChannel struct {
	lock     sync.Mutex
	channels map[types.UID]*WorkChannelItem
}

// 获取或创建指定 UID 的拦截 channel。
func (w *WorkChannel) Channel(uid types.UID) *WorkChannelItem {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.channels == nil {
		w.channels = make(map[types.UID]*WorkChannelItem)
	}
	channel, ok := w.channels[uid]
	if !ok {
		channel = &WorkChannelItem{
			out: make(chan struct{}, 1),
		}
		w.channels[uid] = channel
	}
	return channel
}

// 用可暂停的 WorkChannelItem 代理 worker 的输入 channel，
// 并把底层 channel 的关闭同步到拦截器。
func (w *WorkChannel) Intercept(uid types.UID, ch chan struct{}) (outCh <-chan struct{}) {
	channel := w.Channel(uid)
	w.lock.Lock()

	defer w.lock.Unlock()
	go func() {
		defer func() {
			channel.Close()
			w.lock.Lock()
			defer w.lock.Unlock()
			delete(w.channels, uid)
		}()
		for range ch {
			channel.Handle()
		}
	}()
	return channel.out
}

// 验证 SyncKnownPods 会清理不再需要的 worker 与状态，
// 同时保留仍有 Pod 引用的 worker，直到其正常终止。
func TestSyncKnownPods(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, _ := createPodWorkers(logger)

	numPods := 20
	for i := 0; i < numPods; i++ {
		podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        newNamedPod(strconv.Itoa(i), "ns", "name", false),
			UpdateType: kubetypes.SyncPodUpdate,
		})
	}
	drainWorkers(podWorkers, numPods)

	if len(podWorkers.podUpdates) != numPods {
		t.Errorf("Incorrect number of open channels %v", len(podWorkers.podUpdates))
	}

	desiredPods := map[types.UID]sets.Empty{}
	desiredPods[types.UID("2")] = sets.Empty{}
	desiredPods[types.UID("14")] = sets.Empty{}
	desiredPodList := []*v1.Pod{newNamedPod("2", "ns", "name", false), newNamedPod("14", "ns", "name", false)}

	// 终止除指定 Pod 外的所有 Pod
	for i := 0; i < numPods; i++ {
		pod := newNamedPod(strconv.Itoa(i), "ns", "name", false)
		if _, ok := desiredPods[pod.UID]; ok {
			continue
		}
		if (i % 2) == 0 {
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		}
		podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        pod,
			UpdateType: kubetypes.SyncPodKill,
		})
	}
	drainWorkers(podWorkers, numPods)

	if !podWorkers.ShouldPodContainersBeTerminating(types.UID("0")) {
		t.Errorf("Expected pod to be terminating")
	}
	if !podWorkers.ShouldPodContainersBeTerminating(types.UID("1")) {
		t.Errorf("Expected pod to be terminating")
	}
	if podWorkers.ShouldPodContainersBeTerminating(types.UID("2")) {
		t.Errorf("Expected pod to not be terminating")
	}
	if !podWorkers.IsPodTerminationRequested(types.UID("0")) {
		t.Errorf("Expected pod to be terminating")
	}
	if podWorkers.IsPodTerminationRequested(types.UID("2")) {
		t.Errorf("Expected pod to not be terminating")
	}

	if podWorkers.CouldHaveRunningContainers(types.UID("0")) {
		t.Errorf("Expected pod to be terminated (deleted and terminated)")
	}
	if podWorkers.CouldHaveRunningContainers(types.UID("1")) {
		t.Errorf("Expected pod to be terminated")
	}
	if !podWorkers.CouldHaveRunningContainers(types.UID("2")) {
		t.Errorf("Expected pod to not be terminated")
	}

	if !podWorkers.ShouldPodContentBeRemoved(types.UID("0")) {
		t.Errorf("Expected pod to be suitable for removal (deleted and terminated)")
	}
	if podWorkers.ShouldPodContentBeRemoved(types.UID("1")) {
		t.Errorf("Expected pod to not be suitable for removal (terminated but not deleted)")
	}
	if podWorkers.ShouldPodContentBeRemoved(types.UID("2")) {
		t.Errorf("Expected pod to not be suitable for removal (not terminated)")
	}

	if podWorkers.ShouldPodContainersBeTerminating(types.UID("abc")) {
		t.Errorf("Expected pod to not be known to be terminating (does not exist but not yet synced)")
	}
	if !podWorkers.CouldHaveRunningContainers(types.UID("abc")) {
		t.Errorf("Expected pod to potentially have running containers (does not exist but not yet synced)")
	}
	if podWorkers.ShouldPodContentBeRemoved(types.UID("abc")) {
		t.Errorf("Expected pod to not be suitable for removal (does not exist but not yet synced)")
	}

	podWorkers.SyncKnownPods(logger, desiredPodList)
	if len(podWorkers.podUpdates) != 2 {
		t.Errorf("Incorrect number of open channels %v", len(podWorkers.podUpdates))
	}
	if _, exists := podWorkers.podUpdates[types.UID("2")]; !exists {
		t.Errorf("No updates channel for pod 2")
	}
	if _, exists := podWorkers.podUpdates[types.UID("14")]; !exists {
		t.Errorf("No updates channel for pod 14")
	}
	if podWorkers.IsPodTerminationRequested(types.UID("2")) {
		t.Errorf("Expected pod termination request to be cleared after sync")
	}

	if !podWorkers.ShouldPodContainersBeTerminating(types.UID("abc")) {
		t.Errorf("Expected pod to be expected to terminate containers (does not exist and synced at least once)")
	}
	if podWorkers.CouldHaveRunningContainers(types.UID("abc")) {
		t.Errorf("Expected pod to be known not to have running containers (does not exist and synced at least once)")
	}
	if !podWorkers.ShouldPodContentBeRemoved(types.UID("abc")) {
		t.Errorf("Expected pod to be suitable for removal (does not exist and synced at least once)")
	}

	// 确认即使配置中已不存在，尚未终止的 Pod worker 仍保持打开
	podWorkers.SyncKnownPods(logger, nil)
	drainAllWorkers(podWorkers)
	if len(podWorkers.podUpdates) != 0 {
		t.Errorf("Incorrect number of open channels %v", len(podWorkers.podUpdates))
	}
	if len(podWorkers.podSyncStatuses) != 2 {
		t.Errorf("Incorrect number of tracked statuses: %#v", podWorkers.podSyncStatuses)
	}

	for uid := range desiredPods {
		pod := newNamedPod(string(uid), "ns", "name", false)
		podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        pod,
			UpdateType: kubetypes.SyncPodKill,
		})
	}
	drainWorkers(podWorkers, numPods)

	// 确认这些 Pod 经其他流程终止后，worker 会被清理
	podWorkers.SyncKnownPods(logger, nil)
	if len(podWorkers.podUpdates) != 0 {
		t.Errorf("Incorrect number of open channels %v", len(podWorkers.podUpdates))
	}
	if len(podWorkers.podSyncStatuses) != 0 {
		t.Errorf("Incorrect number of tracked statuses: %#v", podWorkers.podSyncStatuses)
	}
}

// 表驱动验证 removeTerminatedWorker 对已完成、等待启动、孤儿、终止中、
// 已终止等状态是否删除 worker，以及 pendingUpdate 的保留规则。
func Test_removeTerminatedWorker(t *testing.T) {
	podUID := types.UID("pod-uid")

	testCases := []struct {
		desc                               string
		orphan                             bool
		podSyncStatus                      *podSyncStatus
		startedStaticPodsByFullname        map[string]types.UID
		waitingToStartStaticPodsByFullname map[string][]types.UID
		removed                            bool
		expectGracePeriod                  int64
		expectPending                      *UpdatePodOptions
	}{
		{
			desc: "finished worker",
			podSyncStatus: &podSyncStatus{
				finished: true,
			},
			removed: true,
		},
		{
			desc: "waiting to start worker because of another started pod with the same fullname",
			podSyncStatus: &podSyncStatus{
				finished: false,
				fullname: "fake-fullname",
			},
			startedStaticPodsByFullname: map[string]types.UID{
				"fake-fullname": "another-pod-uid",
			},
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"fake-fullname": {podUID},
			},
			removed: false,
		},
		{
			desc: "not yet started worker",
			podSyncStatus: &podSyncStatus{
				finished: false,
				fullname: "fake-fullname",
			},
			startedStaticPodsByFullname: make(map[string]types.UID),
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"fake-fullname": {podUID},
			},
			removed: false,
		},
		{
			desc: "orphaned not started worker",
			podSyncStatus: &podSyncStatus{
				finished: false,
				fullname: "fake-fullname",
			},
			orphan:  true,
			removed: true,
		},
		{
			desc: "orphaned started worker",
			podSyncStatus: &podSyncStatus{
				startedAt: time.Unix(1, 0),
				finished:  false,
				fullname:  "fake-fullname",
			},
			orphan:  true,
			removed: false,
		},
		{
			desc: "orphaned terminating worker with no activeUpdate",
			podSyncStatus: &podSyncStatus{
				startedAt:     time.Unix(1, 0),
				terminatingAt: time.Unix(2, 0),
				finished:      false,
				fullname:      "fake-fullname",
			},
			orphan:  true,
			removed: false,
		},
		{
			desc: "orphaned terminating worker",
			podSyncStatus: &podSyncStatus{
				startedAt:     time.Unix(1, 0),
				terminatingAt: time.Unix(2, 0),
				finished:      false,
				fullname:      "fake-fullname",
				activeUpdate: &UpdatePodOptions{
					Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "1"}},
				},
			},
			orphan:  true,
			removed: false,
			expectPending: &UpdatePodOptions{
				Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "1"}},
			},
		},
		{
			desc: "orphaned terminating worker with pendingUpdate",
			podSyncStatus: &podSyncStatus{
				startedAt:     time.Unix(1, 0),
				terminatingAt: time.Unix(2, 0),
				finished:      false,
				fullname:      "fake-fullname",
				working:       true,
				pendingUpdate: &UpdatePodOptions{
					Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "2"}},
				},
				activeUpdate: &UpdatePodOptions{
					Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "1"}},
				},
			},
			orphan:  true,
			removed: false,
			expectPending: &UpdatePodOptions{
				Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "2"}},
			},
		},
		{
			desc: "orphaned terminated worker with no activeUpdate",
			podSyncStatus: &podSyncStatus{
				startedAt:     time.Unix(1, 0),
				terminatingAt: time.Unix(2, 0),
				terminatedAt:  time.Unix(3, 0),
				finished:      false,
				fullname:      "fake-fullname",
			},
			orphan:  true,
			removed: false,
		},
		{
			desc: "orphaned terminated worker",
			podSyncStatus: &podSyncStatus{
				startedAt:     time.Unix(1, 0),
				terminatingAt: time.Unix(2, 0),
				terminatedAt:  time.Unix(3, 0),
				finished:      false,
				fullname:      "fake-fullname",
				activeUpdate: &UpdatePodOptions{
					Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "1"}},
				},
			},
			orphan:  true,
			removed: false,
			expectPending: &UpdatePodOptions{
				Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "1"}},
			},
		},
		{
			desc: "orphaned terminated worker with pendingUpdate",
			podSyncStatus: &podSyncStatus{
				startedAt:     time.Unix(1, 0),
				terminatingAt: time.Unix(2, 0),
				terminatedAt:  time.Unix(3, 0),
				finished:      false,
				working:       true,
				fullname:      "fake-fullname",
				pendingUpdate: &UpdatePodOptions{
					Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "2"}},
				},
				activeUpdate: &UpdatePodOptions{
					Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "1"}},
				},
			},
			orphan:  true,
			removed: false,
			expectPending: &UpdatePodOptions{
				Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID, Name: "2"}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			normalizeUpdatePodOptions := func(opts *UpdatePodOptions) *UpdatePodOptions {
				if opts == nil {
					return nil
				}
				normalized := *opts
				return &normalized
			}

			logger, _ := ktesting.NewTestContext(t)
			podWorkers, _, _ := createPodWorkers(logger)
			podWorkers.podSyncStatuses[podUID] = tc.podSyncStatus
			podWorkers.podUpdates[podUID] = make(chan struct{}, 1)
			if tc.podSyncStatus.working {
				podWorkers.podUpdates[podUID] <- struct{}{}
			}
			podWorkers.startedStaticPodsByFullname = tc.startedStaticPodsByFullname
			podWorkers.waitingToStartStaticPodsByFullname = tc.waitingToStartStaticPodsByFullname

			podWorkers.removeTerminatedWorker(logger, podUID, podWorkers.podSyncStatuses[podUID], tc.orphan)
			status, exists := podWorkers.podSyncStatuses[podUID]
			if tc.removed && exists {
				t.Fatalf("Expected pod worker to be removed")
			}
			if !tc.removed && !exists {
				t.Fatalf("Expected pod worker to not be removed")
			}
			if tc.removed {
				return
			}
			if tc.expectGracePeriod > 0 && status.gracePeriod != tc.expectGracePeriod {
				t.Errorf("Unexpected grace period %d", status.gracePeriod)
			}
			expectedPending := normalizeUpdatePodOptions(tc.expectPending)
			actualPending := normalizeUpdatePodOptions(status.pendingUpdate)
			if !reflect.DeepEqual(expectedPending, actualPending) {
				t.Errorf("Unexpected pending: %s", cmp.Diff(expectedPending, actualPending))
			}
			if tc.expectPending != nil {
				if !status.working {
					t.Errorf("Should be working")
				}
				if len(podWorkers.podUpdates[podUID]) != 1 {
					t.Errorf("Should have one entry in podUpdates")
				}
			}
		})
	}
}

type simpleFakeKubelet struct {
	pod       *v1.Pod
	mirrorPod *v1.Pod
	podStatus *kubecontainer.PodStatus
	wg        sync.WaitGroup
}

// 记录收到的 Pod、mirror Pod 与状态，供 fake/real worker 行为对比。
func (kl *simpleFakeKubelet) SyncPod(ctx context.Context, updateType kubetypes.SyncPodType, pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
	kl.pod, kl.mirrorPod, kl.podStatus = pod, mirrorPod, podStatus
	return false, nil, nil
}

// 记录参数后发出 WaitGroup 信号，让测试等待真实 worker 完成。
func (kl *simpleFakeKubelet) SyncPodWithWaitGroup(ctx context.Context, updateType kubetypes.SyncPodType, pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
	kl.pod, kl.mirrorPod, kl.podStatus = pod, mirrorPod, podStatus
	kl.wg.Done()
	return false, nil, nil
}

// fake kubelet 不执行实际终止逻辑。
func (kl *simpleFakeKubelet) SyncTerminatingPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, gracePeriod *int64, podStatusFn func(*v1.PodStatus)) error {
	return nil
}

// fake kubelet 不执行 RuntimePod 终止逻辑。
func (kl *simpleFakeKubelet) SyncTerminatingRuntimePod(ctx context.Context, runningPod *kubecontainer.Pod) error {
	return nil
}

// fake kubelet 不执行终止后的清理逻辑。
func (kl *simpleFakeKubelet) SyncTerminatedPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) error {
	return nil
}

// 验证 fakePodWorkers 与真实 podWorkers 调用 syncPodFn 时传入的参数完全一致。
func TestFakePodWorkers(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	fakeRecorder := &record.FakeRecorder{}
	fakeRuntime := &containertest.FakeRuntime{}
	fakeCache := containertest.NewFakeCache(fakeRuntime)

	kubeletForRealWorkers := &simpleFakeKubelet{}
	kubeletForFakeWorkers := &simpleFakeKubelet{}
	realPodSyncer := newPodSyncerFuncs(kubeletForRealWorkers)
	realPodSyncer.syncPod = kubeletForRealWorkers.SyncPodWithWaitGroup

	realPodWorkers := newPodWorkers(
		realPodSyncer,
		fakeRecorder,
		queue.NewBasicWorkQueue(&clock.RealClock{}),
		time.Second,
		time.Second,
		fakeCache,
		allocation.NewInMemoryManager(logger, nil, nil, nil, nil, nil, nil),
	)
	fakePodWorkers := &fakePodWorkers{
		syncPodFn: kubeletForFakeWorkers.SyncPod,
		cache:     fakeCache,
		t:         t,
	}

	tests := []struct {
		pod       *v1.Pod
		mirrorPod *v1.Pod
	}{
		{
			&v1.Pod{},
			&v1.Pod{},
		},
		{
			podWithUIDNameNs("12345678", "foo", "new"),
			podWithUIDNameNs("12345678", "fooMirror", "new"),
		},
		{
			podWithUIDNameNs("98765", "bar", "new"),
			podWithUIDNameNs("98765", "barMirror", "new"),
		},
	}

	for i, tt := range tests {
		kubeletForRealWorkers.wg.Add(1)
		realPodWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        tt.pod,
			MirrorPod:  tt.mirrorPod,
			UpdateType: kubetypes.SyncPodUpdate,
		})
		fakePodWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        tt.pod,
			MirrorPod:  tt.mirrorPod,
			UpdateType: kubetypes.SyncPodUpdate,
		})

		kubeletForRealWorkers.wg.Wait()

		if !reflect.DeepEqual(kubeletForRealWorkers.pod, kubeletForFakeWorkers.pod) {
			t.Errorf("%d: Expected: %#v, Actual: %#v", i, kubeletForRealWorkers.pod, kubeletForFakeWorkers.pod)
		}

		if !reflect.DeepEqual(kubeletForRealWorkers.mirrorPod, kubeletForFakeWorkers.mirrorPod) {
			t.Errorf("%d: Expected: %#v, Actual: %#v", i, kubeletForRealWorkers.mirrorPod, kubeletForFakeWorkers.mirrorPod)
		}

		if !reflect.DeepEqual(kubeletForRealWorkers.podStatus, kubeletForFakeWorkers.podStatus) {
			t.Errorf("%d: Expected: %#v, Actual: %#v", i, kubeletForRealWorkers.podStatus, kubeletForFakeWorkers.podStatus)
		}
	}
}

// 验证 killPodNow 以阻塞方式完成 kill 并等待终止同步，
// 事件顺序应为 kill 后跟 terminated。
func TestKillPodNowFunc(t *testing.T) {
	fakeRecorder := &record.FakeRecorder{}
	logger, tCtx := ktesting.NewTestContext(t)
	podWorkers, _, processed := createPodWorkers(logger)
	killPodFunc := killPodNow(tCtx, podWorkers, fakeRecorder)
	pod := newNamedPod("test", "ns", "test", false)
	gracePeriodOverride := int64(0)
	err := killPodFunc(pod, false, &gracePeriodOverride, func(status *v1.PodStatus) {
		status.Phase = v1.PodFailed
		status.Reason = "reason"
		status.Message = "message"
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	drainAllWorkers(podWorkers)
	if len(processed) != 1 {
		t.Fatalf("len(processed) expected: %v, actual: %#v", 1, processed)
	}
	syncPodRecords := processed[pod.UID]
	if len(syncPodRecords) != 2 {
		t.Fatalf("Pod processed expected %v times, got %#v", 1, syncPodRecords)
	}
	if syncPodRecords[0].updateType != kubetypes.SyncPodKill {
		t.Errorf("Pod update type was %v, but expected %v", syncPodRecords[0].updateType, kubetypes.SyncPodKill)
	}
	if !syncPodRecords[1].terminated {
		t.Errorf("Pod terminated %v, but expected %v", syncPodRecords[1].terminated, true)
	}
}

// 表驱动验证 allowPodStart 对普通 Pod、同名 static Pod、排队等待的 static Pod
// 以及终端/未知状态的准入判断与队列维护。
func Test_allowPodStart(t *testing.T) {
	testCases := []struct {
		desc                               string
		pod                                *v1.Pod
		podSyncStatuses                    map[types.UID]*podSyncStatus
		startedStaticPodsByFullname        map[string]types.UID
		waitingToStartStaticPodsByFullname map[string][]types.UID

		expectedStartedStaticPodsByFullname        map[string]types.UID
		expectedWaitingToStartStaticPodsByFullname map[string][]types.UID
		allowed                                    bool
		allowedEver                                bool
	}{
		{
			// TODO: 是否允许非静态 Pod 使用相同的全名？
			// 注意这可能会禁用 Pod 的强制删除。
			desc: "non-static pod",
			pod:  newNamedPod("uid-0", "ns", "test", false),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "test_",
				},
				"uid-1": {
					fullname: "test_",
				},
			},
			allowed:     true,
			allowedEver: true,
		},
		{
			// TODO: 是否允许非静态 Pod 与已启动的静态 Pod 使用相同的全名？
			desc: "non-static pod when there is a started static pod with the same full name",
			pod:  newNamedPod("uid-0", "ns", "test", false),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "test_",
				},
				"uid-1": {
					fullname: "test_",
				},
			},
			startedStaticPodsByFullname: map[string]types.UID{
				"test_": types.UID("uid-1"),
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{
				"test_": types.UID("uid-1"),
			},
			allowed:     true,
			allowedEver: true,
		},
		{
			// TODO: 是否允许静态 Pod 与已启动的非静态 Pod 使用相同的全名？
			desc: "static pod when there is a started non-static pod with the same full name",
			pod:  newNamedPod("uid-0", "ns", "test", false),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "test_",
				},
				"uid-1": {
					fullname: "test_",
				},
			},
			allowed:     true,
			allowedEver: true,
		},
		{
			desc: "static pod when there are no started static pods with the same full name",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "foo_",
				},
				"uid-1": {
					fullname: "bar_",
				},
			},
			startedStaticPodsByFullname: map[string]types.UID{
				"bar_": types.UID("uid-1"),
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-0"),
				"bar_": types.UID("uid-1"),
			},
			allowed:     true,
			allowedEver: true,
		},
		{
			desc: "static pod when there is a started static pod with the same full name",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "foo_",
				},
				"uid-1": {
					fullname: "foo_",
				},
			},
			startedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-1"),
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-1"),
			},
			allowed:     false,
			allowedEver: true,
		},
		{
			desc: "static pod if the static pod has already started",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "foo_",
				},
			},
			startedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-0"),
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-0"),
			},
			allowed:     true,
			allowedEver: true,
		},
		{
			desc: "static pod if the static pod is the first pod waiting to start",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "foo_",
				},
			},
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-0"),
				},
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-0"),
			},
			expectedWaitingToStartStaticPodsByFullname: make(map[string][]types.UID),
			allowed:     true,
			allowedEver: true,
		},
		{
			desc: "static pod if the static pod is not the first pod waiting to start",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "foo_",
				},
				"uid-1": {
					fullname: "foo_",
				},
			},
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-1"),
					types.UID("uid-0"),
				},
			},
			expectedStartedStaticPodsByFullname: make(map[string]types.UID),
			expectedWaitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-1"),
					types.UID("uid-0"),
				},
			},
			allowed:     false,
			allowedEver: true,
		},
		{
			desc: "static pod if the static pod is the first valid pod waiting to start / clean up until picking the first valid pod",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "foo_",
				},
				"uid-1": {
					fullname: "foo_",
				},
			},
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-2"),
					types.UID("uid-2"),
					types.UID("uid-3"),
					types.UID("uid-0"),
					types.UID("uid-1"),
				},
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-0"),
			},
			expectedWaitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-1"),
				},
			},
			allowed:     true,
			allowedEver: true,
		},
		{
			desc: "static pod if the static pod is the first pod that is not termination requested and waiting to start",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname: "foo_",
				},
				"uid-1": {
					fullname: "foo_",
				},
				"uid-2": {
					fullname:      "foo_",
					terminatingAt: time.Now(),
				},
				"uid-3": {
					fullname:     "foo_",
					terminatedAt: time.Now(),
				},
			},
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-2"),
					types.UID("uid-3"),
					types.UID("uid-0"),
					types.UID("uid-1"),
				},
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{
				"foo_": types.UID("uid-0"),
			},
			expectedWaitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-1"),
				},
			},
			allowed:     true,
			allowedEver: true,
		},
		{
			desc: "static pod if there is no sync status for the pod should be denied",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-1": {
					fullname: "foo_",
				},
				"uid-2": {
					fullname:      "foo_",
					terminatingAt: time.Now(),
				},
				"uid-3": {
					fullname:     "foo_",
					terminatedAt: time.Now(),
				},
			},
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-1"),
				},
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{},
			expectedWaitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-1"),
				},
			},
			allowed:     false,
			allowedEver: false,
		},
		{
			desc: "static pod if the static pod is terminated should not be allowed",
			pod:  newStaticPod("uid-0", "foo"),
			podSyncStatuses: map[types.UID]*podSyncStatus{
				"uid-0": {
					fullname:      "foo_",
					terminatingAt: time.Now(),
				},
			},
			waitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-2"),
					types.UID("uid-3"),
					types.UID("uid-0"),
					types.UID("uid-1"),
				},
			},
			expectedStartedStaticPodsByFullname: map[string]types.UID{},
			expectedWaitingToStartStaticPodsByFullname: map[string][]types.UID{
				"foo_": {
					types.UID("uid-2"),
					types.UID("uid-3"),
					types.UID("uid-0"),
					types.UID("uid-1"),
				},
			},
			allowed:     false,
			allowedEver: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			podWorkers, _, _ := createPodWorkers(logger)
			if tc.podSyncStatuses != nil {
				podWorkers.podSyncStatuses = tc.podSyncStatuses
			}
			if tc.startedStaticPodsByFullname != nil {
				podWorkers.startedStaticPodsByFullname = tc.startedStaticPodsByFullname
			}
			if tc.waitingToStartStaticPodsByFullname != nil {
				podWorkers.waitingToStartStaticPodsByFullname = tc.waitingToStartStaticPodsByFullname
			}
			allowed, allowedEver := podWorkers.allowPodStart(logger, tc.pod)
			if allowed != tc.allowed {
				if tc.allowed {
					t.Errorf("Pod should be allowed")
				} else {
					t.Errorf("Pod should not be allowed")
				}
			}

			if allowedEver != tc.allowedEver {
				if tc.allowedEver {
					t.Errorf("Pod should be allowed ever")
				} else {
					t.Errorf("Pod should not be allowed ever")
				}
			}

			// 若映射既非 nil 也非空
			if len(podWorkers.startedStaticPodsByFullname) != 0 ||
				len(podWorkers.startedStaticPodsByFullname) != len(tc.expectedStartedStaticPodsByFullname) {
				if !reflect.DeepEqual(
					podWorkers.startedStaticPodsByFullname,
					tc.expectedStartedStaticPodsByFullname) {
					t.Errorf("startedStaticPodsByFullname: expected %v, got %v",
						tc.expectedStartedStaticPodsByFullname,
						podWorkers.startedStaticPodsByFullname)
				}
			}

			// 若映射既非 nil 也非空
			if len(podWorkers.waitingToStartStaticPodsByFullname) != 0 ||
				len(podWorkers.waitingToStartStaticPodsByFullname) != len(tc.expectedWaitingToStartStaticPodsByFullname) {
				if !reflect.DeepEqual(
					podWorkers.waitingToStartStaticPodsByFullname,
					tc.expectedWaitingToStartStaticPodsByFullname) {
					t.Errorf("waitingToStartStaticPodsByFullname: expected %v, got %v",
						tc.expectedWaitingToStartStaticPodsByFullname,
						podWorkers.waitingToStartStaticPodsByFullname)
				}
			}
		})
	}
}

// 验证优雅终止时间的选择优先级：显式覆盖 > 删除时间戳上的值 > Pod spec，
// 零值会被提升为 1 秒。
func Test_calculateEffectiveGracePeriod(t *testing.T) {
	zero := int64(0)
	two := int64(2)
	five := int64(5)
	thirty := int64(30)
	testCases := []struct {
		desc                                 string
		podSpecTerminationGracePeriodSeconds *int64
		podDeletionGracePeriodSeconds        *int64
		gracePeriodOverride                  *int64
		expectedGracePeriod                  int64
	}{
		{
			desc:                                 "use termination grace period from the spec when no overrides",
			podSpecTerminationGracePeriodSeconds: &thirty,
			expectedGracePeriod:                  thirty,
		},
		{
			desc:                                 "use pod DeletionGracePeriodSeconds when set",
			podSpecTerminationGracePeriodSeconds: &thirty,
			podDeletionGracePeriodSeconds:        &five,
			expectedGracePeriod:                  five,
		},
		{
			desc:                                 "use grace period override when set",
			podSpecTerminationGracePeriodSeconds: &thirty,
			podDeletionGracePeriodSeconds:        &five,
			gracePeriodOverride:                  &two,
			expectedGracePeriod:                  two,
		},
		{
			desc:                                 "use 1 when pod DeletionGracePeriodSeconds is zero",
			podSpecTerminationGracePeriodSeconds: &thirty,
			podDeletionGracePeriodSeconds:        &zero,
			expectedGracePeriod:                  1,
		},
		{
			desc:                                 "use 1 when grace period override is zero",
			podSpecTerminationGracePeriodSeconds: &thirty,
			podDeletionGracePeriodSeconds:        &five,
			gracePeriodOverride:                  &zero,
			expectedGracePeriod:                  1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pod := newNamedPod("1", "ns", "running-pod", false)
			pod.Spec.TerminationGracePeriodSeconds = tc.podSpecTerminationGracePeriodSeconds
			pod.DeletionGracePeriodSeconds = tc.podDeletionGracePeriodSeconds
			gracePeriod, _ := calculateEffectiveGracePeriod(&podSyncStatus{}, pod, &KillPodOptions{
				PodTerminationGracePeriodSecondsOverride: tc.gracePeriodOverride,
			})
			if gracePeriod != tc.expectedGracePeriod {
				t.Errorf("Expected a grace period of %v, but was %v", tc.expectedGracePeriod, gracePeriod)
			}
		})
	}
}
