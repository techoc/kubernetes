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

package config

import (
	"context"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/securitycontext"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	// TestSource 是测试中使用的配置源名称。
	TestSource = "test"
)

// expectEmptyChannel 断言通道中没有任何待处理的更新。
func expectEmptyChannel(t *testing.T, ch <-chan sourceUpdate) {
	select {
	case update := <-ch:
		t.Errorf("Expected no update in channel, Got %v", update)
	default:
	}
}

// sortedPods 为 Pod 切片实现排序接口，按命名空间排序以便稳定比较。
type sortedPods []*v1.Pod

// Len 返回 Pod 数量。
func (s sortedPods) Len() int {
	return len(s)
}

// Swap 交换两个 Pod 的位置。
func (s sortedPods) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less 按命名空间字典序比较，保证排序结果稳定。
func (s sortedPods) Less(i, j int) bool {
	return s[i].Namespace < s[j].Namespace
}

// mockPodStartupSLIObserver 是 PodStartupSLIObserver 的空实现，仅用于测试构造。
type mockPodStartupSLIObserver struct{}

// ObservedPodOnWatch 不执行任何操作。
func (m *mockPodStartupSLIObserver) ObservedPodOnWatch(logger klog.Logger, pod *v1.Pod, when time.Time) {
}

// CreateValidPod 构造一个通过基本校验的 Pod，用于配置更新测试。
func CreateValidPod(name, namespace string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(name + namespace), // 测试场景下该 UID 已足够唯一。
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyAlways,
			DNSPolicy:     v1.DNSClusterFirst,
			Containers: []v1.Container{
				{
					Name:                     "ctr",
					Image:                    "image",
					ImagePullPolicy:          "IfNotPresent",
					SecurityContext:          securitycontext.ValidSecurityContextWithContainerDefaults(),
					TerminationMessagePolicy: v1.TerminationMessageReadFile,
				},
			},
		},
	}
}

// CreatePodUpdate 构造期望的 PodUpdate，供断言使用。
func CreatePodUpdate(op kubetypes.PodOperation, source string, pods ...*v1.Pod) kubetypes.PodUpdate {
	return kubetypes.PodUpdate{Pods: pods, Op: op, Source: source}
}

// createSourceUpdate 构造配置源更新。
func createSourceUpdate(pods ...*v1.Pod) sourceUpdate {
	return sourceUpdate{pods}
}

// createPodConfigTester 创建带测试源的 PodConfig，并返回写入通道与更新输出通道。
func createPodConfigTester(ctx context.Context) (chan<- sourceUpdate, <-chan kubetypes.PodUpdate, *PodConfig) {
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	config := NewPodConfig(eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "kubelet"}), &mockPodStartupSLIObserver{})
	channel := config.Channel(ctx, TestSource)
	ch := config.Updates()
	return channel, ch, config
}

// expectPodUpdate 依次校验期望的 PodUpdate，并确认之后没有多余更新。
func expectPodUpdate(t *testing.T, ch <-chan kubetypes.PodUpdate, expected ...kubetypes.PodUpdate) {
	t.Helper()
	for i := range expected {
		update := <-ch
		sort.Sort(sortedPods(update.Pods))
		sort.Sort(sortedPods(expected[i].Pods))
		// 复制期望值与实际值，先比较除 "Pods" 之外的所有字段，
		// "Pods" 在下面单独比较。
		expectedCopy, updateCopy := expected[i], update
		expectedCopy.Pods, updateCopy.Pods = nil, nil
		if !apiequality.Semantic.DeepEqual(expectedCopy, updateCopy) {
			t.Fatalf("Expected %#v, Got %#v", expectedCopy, updateCopy)
		}

		if len(expected[i].Pods) != len(update.Pods) {
			t.Fatalf("Expected %#v, Got %#v", expected[i], update)
		}
		// 逐个比较 Pod，避免把本地注解等无关字段纳入比较。
		for j := range expected[i].Pods {
			if podsDifferSemantically(expected[i].Pods[j], update.Pods[j]) || !reflect.DeepEqual(expected[i].Pods[j].Status, update.Pods[j].Status) {
				t.Fatalf("Expected %#v, Got %#v", expected[i].Pods[j], update.Pods[j])
			}
		}
	}
	expectNoPodUpdate(t, ch)
}

// expectNoPodUpdate 断言通道中没有 Pod 更新。
func expectNoPodUpdate(t *testing.T, ch <-chan kubetypes.PodUpdate) {
	select {
	case update := <-ch:
		t.Errorf("Expected no update in channel, Got %#v", update)
	default:
	}
}

// TestNewPodAdded 验证新增 Pod 会产生 ADD 更新。
func TestNewPodAdded(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 预期收到一次更新。
	podUpdate := createSourceUpdate(CreateValidPod("foo", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "new")))
}

// TestNewPodAddedInvalidNamespace 验证空命名空间的 Pod 仍会原样产生 ADD 更新。
func TestNewPodAddedInvalidNamespace(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 预期收到一次更新。
	podUpdate := createSourceUpdate(CreateValidPod("foo", ""))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "")))
}

// TestNewPodAddedDefaultNamespace 验证 default 命名空间的 Pod 会产生 ADD 更新。
func TestNewPodAddedDefaultNamespace(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 预期收到一次更新。
	podUpdate := createSourceUpdate(CreateValidPod("foo", "default"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "default")))
}

// TestNewPodAddedDifferentNamespaces 验证不同命名空间下的同名 Pod 会被视为不同对象。
func TestNewPodAddedDifferentNamespaces(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 预期收到一次更新。
	pod1 := CreateValidPod("foo", "default")
	podUpdate := createSourceUpdate(pod1)
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "default")))

	// 另一个命名空间中的同名 Pod 应产生新的 ADD 更新。
	pod2 := CreateValidPod("foo", "new")
	podUpdate = createSourceUpdate(pod1, pod2)
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "new")))
}

// TestInvalidPodFiltered 验证与已有 Pod 完全相同的重复更新会被过滤，不产生新更新。
func TestInvalidPodFiltered(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 预期收到一次更新。
	podUpdate := createSourceUpdate(CreateValidPod("foo", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "new")))

	// 提交一个同名且内容相同的 Pod，应被过滤掉。
	podUpdate = createSourceUpdate(CreateValidPod("foo", "new"))
	channel <- podUpdate
	expectNoPodUpdate(t, ch)
}

// TestNewPodAddedUpdatedRemoved 验证同一 Pod 的 ADD、UPDATE 与 REMOVE 状态转换。
func TestNewPodAddedUpdatedRemoved(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 应产生一个 ADD 更新。
	podUpdate := createSourceUpdate(CreateValidPod("foo", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "new")))

	// 内容相同的 ADD 应被忽略。
	expectNoPodUpdate(t, ch)

	// 内容变化的 ADD 应被转换为 UPDATE。
	pod := CreateValidPod("foo", "new")
	pod.Spec.Containers = []v1.Container{{Name: "bar", Image: "test", ImagePullPolicy: v1.PullIfNotPresent, TerminationMessagePolicy: v1.TerminationMessageReadFile}}
	podUpdate = createSourceUpdate(pod)
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.UPDATE, TestSource, pod))

	// 提交空集合应产生 REMOVE 更新。
	podUpdate = createSourceUpdate()
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.REMOVE, TestSource, pod))
}

// TestNewPodAddedDelete 验证带删除时间戳的 Pod 会产生 DELETE 更新。
func TestNewPodAddedDelete(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 应产生一个 ADD 更新。
	addedPod := CreateValidPod("foo", "new")
	podUpdate := createSourceUpdate(addedPod)
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, addedPod))

	// 将该 Pod 标记为已删除。
	timestamp := metav1.NewTime(time.Now())
	deletedPod := CreateValidPod("foo", "new")
	deletedPod.ObjectMeta.DeletionTimestamp = &timestamp
	podUpdate = createSourceUpdate(deletedPod)
	channel <- podUpdate
	// 已存在的 Pod 应被优雅删除。
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.DELETE, TestSource, addedPod))
}

// TestNewPodAddedUpdatedSet 验证一次提交多个 Pod 时，会按差异拆分为 ADD、REMOVE 与 UPDATE。
func TestNewPodAddedUpdatedSet(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 应产生一个包含三个 Pod 的 ADD 更新。
	podUpdate := createSourceUpdate(CreateValidPod("foo", "new"), CreateValidPod("foo2", "new"), CreateValidPod("foo3", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "new"), CreateValidPod("foo2", "new"), CreateValidPod("foo3", "new")))

	// 内容相同的 ADD 应被忽略。
	channel <- podUpdate
	expectNoPodUpdate(t, ch)

	// 差异应被拆分为 ADD、REMOVE 与 UPDATE 三种更新。
	pod := CreateValidPod("foo2", "new")
	pod.Spec.Containers = []v1.Container{{Name: "bar", Image: "test", ImagePullPolicy: v1.PullIfNotPresent, TerminationMessagePolicy: v1.TerminationMessageReadFile}}
	podUpdate = createSourceUpdate(pod, CreateValidPod("foo3", "new"), CreateValidPod("foo4", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch,
		CreatePodUpdate(kubetypes.REMOVE, TestSource, CreateValidPod("foo", "new")),
		CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo4", "new")),
		CreatePodUpdate(kubetypes.UPDATE, TestSource, pod))
}

// TestNewPodAddedSetReconciled 验证仅状态变化的 Pod 会触发 RECONCILE，
// 而 spec 也变化的 Pod 只产生 UPDATE。
func TestNewPodAddedSetReconciled(t *testing.T) {
	tCtx := ktesting.Init(t)

	// 创建并修改测试 Pod，返回新的 Pod 列表与被修改的 Pod。
	// 必须先创建完整列表再修改，避免数据竞争。
	newTestPods := func(touchStatus, touchSpec bool) ([]*v1.Pod, *v1.Pod) {
		pods := []*v1.Pod{
			CreateValidPod("changeable-pod-0", "new"),
			CreateValidPod("constant-pod-1", "new"),
			CreateValidPod("constant-pod-2", "new"),
		}
		if touchStatus {
			pods[0].Status = v1.PodStatus{Message: strconv.Itoa(rand.Int())}
		}
		if touchSpec {
			pods[0].Spec.Containers[0].Name = strconv.Itoa(rand.Int())
		}
		return pods, pods[0]
	}
	var podWithStatusChange *v1.Pod
	pods, _ := newTestPods(false, false)
	channel, ch, _ := createPodConfigTester(tCtx)

	// 用 SET 初始化配置，尤其是初始化源集合。
	channel <- createSourceUpdate(pods...)
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, pods...))

	// 状态未变化时不应触发 reconcile。
	channel <- createSourceUpdate(pods...)
	expectNoPodUpdate(t, ch)

	// 仅状态变化且未更新 spec 时，应触发 RECONCILE。
	pods, podWithStatusChange = newTestPods(true, false)
	channel <- createSourceUpdate(pods...)
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.RECONCILE, TestSource, podWithStatusChange))

	// 状态变化但 spec 同时更新时，只应产生 UPDATE，不触发 RECONCILE。
	pods, podWithStatusChange = newTestPods(true, true)
	channel <- createSourceUpdate(pods...)
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.UPDATE, TestSource, podWithStatusChange))
}

// TestInitialEmptySet 验证初始空集合会注册一次空 ADD，之后空集合被忽略。
func TestInitialEmptySet(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	// 应注册一次空的 PodUpdate 操作。
	podUpdate := createSourceUpdate()
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource))

	// 后续的空集合应被忽略。
	podUpdate = createSourceUpdate()
	channel <- podUpdate
	podUpdate = createSourceUpdate(CreateValidPod("foo", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo", "new")))
}

// TestPodUpdateAnnotations 验证注解的增加、修改与删除都会触发 UPDATE。
func TestPodUpdateAnnotations(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	pod := CreateValidPod("foo2", "new")
	pod.Annotations = make(map[string]string)
	pod.Annotations["kubernetes.io/blah"] = "blah"

	clone := pod.DeepCopy()

	podUpdate := createSourceUpdate(CreateValidPod("foo1", "new"), clone, CreateValidPod("foo3", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, CreateValidPod("foo1", "new"), pod, CreateValidPod("foo3", "new")))

	pod.Annotations["kubernetes.io/blah"] = "superblah"
	podUpdate = createSourceUpdate(CreateValidPod("foo1", "new"), pod, CreateValidPod("foo3", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.UPDATE, TestSource, pod))

	pod.Annotations["kubernetes.io/otherblah"] = "doh"
	podUpdate = createSourceUpdate(CreateValidPod("foo1", "new"), pod, CreateValidPod("foo3", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.UPDATE, TestSource, pod))

	delete(pod.Annotations, "kubernetes.io/blah")
	podUpdate = createSourceUpdate(CreateValidPod("foo1", "new"), pod, CreateValidPod("foo3", "new"))
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.UPDATE, TestSource, pod))
}

// TestPodUpdateLabels 验证标签变化会触发 UPDATE。
func TestPodUpdateLabels(t *testing.T) {
	tCtx := ktesting.Init(t)

	channel, ch, _ := createPodConfigTester(tCtx)

	pod := CreateValidPod("foo2", "new")
	pod.Labels = make(map[string]string)
	pod.Labels["key"] = "value"

	clone := pod.DeepCopy()

	podUpdate := createSourceUpdate(clone)
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.ADD, TestSource, pod))

	pod.Labels["key"] = "newValue"
	podUpdate = createSourceUpdate(pod)
	channel <- podUpdate
	expectPodUpdate(t, ch, CreatePodUpdate(kubetypes.UPDATE, TestSource, pod))

}

// TestPodConfigRace 通过并发创建源与调用 SeenAllSources 检测数据竞争。
func TestPodConfigRace(t *testing.T) {
	tCtx := ktesting.Init(t)

	eventBroadcaster := record.NewBroadcaster(record.WithContext(tCtx))
	config := NewPodConfig(eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "kubelet"}), &mockPodStartupSLIObserver{})
	seenSources := sets.New[string](TestSource)
	var wg sync.WaitGroup
	const iterations = 100
	wg.Add(2)

	// 并发地创建新配置源。
	go func() {
		ctx, cancel := context.WithCancel(tCtx)
		defer cancel()
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			config.Channel(ctx, strconv.Itoa(i))
		}
	}()
	// 并发地检查所有源是否就绪，与上面的创建操作形成竞争条件。
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			config.SeenAllSources(tCtx.Logger(), seenSources)
		}
	}()

	wg.Wait()
}
