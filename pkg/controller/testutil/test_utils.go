/*
Copyright 2016 The Kubernetes Authors.

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

package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/watch"
	v1apply "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	ref "k8s.io/client-go/tools/reference"
	utilnode "k8s.io/component-helpers/node/topology"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

var (
	keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc
)

// FakeNodeHandler 是 NodesInterface 和 NodeInterface 的假实现，
// 让测试用例能够精细控制 mock 行为。由于还需要 PodsInterface 和 PodInterface
// 来测试 pod 的 list 与 delete，因此嵌入了 client.Fake 提供的那部分实现。
type FakeNodeHandler struct {
	*fake.Clientset

	// 输入：Hook 决定请求是否有效。
	CreateHook func(*FakeNodeHandler, *v1.Node) bool
	Existing   []*v1.Node
	AsyncCalls []func(*FakeNodeHandler)

	// 输出：记录控制器对各接口的实际调用结果，供断言使用。
	CreatedNodes        []*v1.Node
	DeletedNodes        []*v1.Node
	UpdatedNodes        []*v1.Node
	UpdatedNodeStatuses []*v1.Node
	RequestCount        int

	// 同步原语：锁保护上述记录，WaitChan 让测试可以阻塞并观察中间状态。
	lock           sync.Mutex
	DeleteWaitChan chan struct{}
	PatchWaitChan  chan struct{}
}

// FakeLegacyHandler 是 CoreV1Interface 的假实现，
// 用于把 Core()/CoreV1() 返回的接口重新路由回 FakeNodeHandler。
type FakeLegacyHandler struct {
	v1core.CoreV1Interface
	n *FakeNodeHandler
}

// GetUpdatedNodesCopy 返回已应用更新的 Node 切片的副本。
func (m *FakeNodeHandler) GetUpdatedNodesCopy() []*v1.Node {
	m.lock.Lock()
	defer m.lock.Unlock()
	// 复制切片本身而非节点指针，这样调用方遍历时不会与后续写入产生数据竞争。
	updatedNodesCopy := make([]*v1.Node, len(m.UpdatedNodes), len(m.UpdatedNodes))
	copy(updatedNodesCopy, m.UpdatedNodes)
	return updatedNodesCopy
}

// Core 返回假的 CoreInterface 实现。
func (m *FakeNodeHandler) Core() v1core.CoreV1Interface {
	return &FakeLegacyHandler{m.Clientset.CoreV1(), m}
}

// CoreV1 返回假的 CoreV1Interface 实现。
func (m *FakeNodeHandler) CoreV1() v1core.CoreV1Interface {
	return &FakeLegacyHandler{m.Clientset.CoreV1(), m}
}

// Nodes 返回假的 NodeInterface，即 FakeNodeHandler 自身。
func (m *FakeLegacyHandler) Nodes() v1core.NodeInterface {
	return m.n
}

// Create 向假存储中添加一个新 Node。
func (m *FakeNodeHandler) Create(_ context.Context, node *v1.Node, _ metav1.CreateOptions) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	// 真实 apiserver 会对同名对象返回 AlreadyExists，这里必须保持一致，
	// 否则依赖该错误做幂等处理的控制器逻辑无法被正确测试。
	for _, n := range m.Existing {
		if n.Name == node.Name {
			return nil, apierrors.NewAlreadyExists(api.Resource("nodes"), node.Name)
		}
	}
	if m.CreateHook == nil || m.CreateHook(m, node) {
		// 保存副本，避免测试方之后修改原对象污染假存储。
		nodeCopy := *node
		m.CreatedNodes = append(m.CreatedNodes, &nodeCopy)
		return node, nil
	}
	return nil, errors.New("create error")
}

// Get 从假存储中返回一个 Node。
func (m *FakeNodeHandler) Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		// 在持锁期间执行异步钩子，保证钩子看到的状态与本次 Get 一致，
		// 也让测试可以在钩子中安全地读写假存储的字段。
		m.runAsyncCalls()
		m.lock.Unlock()
	}()
	// 先查 UpdatedNodes：更新后的对象代表最新状态，必须优先于初始的 Existing。
	for i := range m.UpdatedNodes {
		if m.UpdatedNodes[i].Name == name {
			nodeCopy := *m.UpdatedNodes[i]
			return &nodeCopy, nil
		}
	}
	for i := range m.Existing {
		if m.Existing[i].Name == name {
			nodeCopy := *m.Existing[i]
			return &nodeCopy, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, name)
}

// runAsyncCalls 按注册顺序执行测试注入的异步钩子。
func (m *FakeNodeHandler) runAsyncCalls() {
	for _, a := range m.AsyncCalls {
		a(m)
	}
}

// List 返回假存储中的 Node 列表。
func (m *FakeNodeHandler) List(_ context.Context, opts metav1.ListOptions) (*v1.NodeList, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	// 按 Updated -> Existing -> Created 的优先级合并，并按名字去重，
	// 模拟 apiserver 中同一对象只有一条记录；已删除的节点不再出现。
	var nodes []*v1.Node
	for i := 0; i < len(m.UpdatedNodes); i++ {
		if !contains(m.UpdatedNodes[i], m.DeletedNodes) {
			nodes = append(nodes, m.UpdatedNodes[i])
		}
	}
	for i := 0; i < len(m.Existing); i++ {
		if !contains(m.Existing[i], m.DeletedNodes) && !contains(m.Existing[i], nodes) {
			nodes = append(nodes, m.Existing[i])
		}
	}
	for i := 0; i < len(m.CreatedNodes); i++ {
		if !contains(m.CreatedNodes[i], m.DeletedNodes) && !contains(m.CreatedNodes[i], nodes) {
			nodes = append(nodes, m.CreatedNodes[i])
		}
	}
	nodeList := &v1.NodeList{}
	for _, node := range nodes {
		nodeList.Items = append(nodeList.Items, *node)
	}
	return nodeList, nil
}

// Delete 从假存储中删除一个 Node。
func (m *FakeNodeHandler) Delete(_ context.Context, id string, opt metav1.DeleteOptions) error {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		// 通过 WaitChan 向测试发送信号，使测试能观察到删除已发生，
		// 常用于让控制器在删除后的下一步阻塞在受控状态。
		if m.DeleteWaitChan != nil {
			m.DeleteWaitChan <- struct{}{}
		}
		m.lock.Unlock()
	}()
	m.DeletedNodes = append(m.DeletedNodes, NewNode(id))
	return nil
}

// DeleteCollection 批量删除假存储中的 Node（此假实现为空操作，仅满足接口）。
func (m *FakeNodeHandler) DeleteCollection(_ context.Context, opt metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return nil
}

// Update 更新假存储中的一个 Node。
func (m *FakeNodeHandler) Update(_ context.Context, node *v1.Node, _ metav1.UpdateOptions) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	nodeCopy := *node
	for i, updateNode := range m.UpdatedNodes {
		if updateNode.Name == nodeCopy.Name {
			// 模拟 apiserver 的乐观并发：resourceVersion 不匹配时拒绝更新，
			// 让依赖冲突重试的控制器逻辑可以被测试覆盖。
			if updateNode.GetObjectMeta().GetResourceVersion() != nodeCopy.GetObjectMeta().GetResourceVersion() {
				return nil, apierrors.NewConflict(schema.GroupResource{}, "fake conflict", nil)
			}
			m.UpdatedNodes[i] = &nodeCopy
			return node, nil
		}
	}
	m.UpdatedNodes = append(m.UpdatedNodes, &nodeCopy)
	return node, nil
}

// UpdateStatus 更新假存储中某个 Node 的 status 子资源。
func (m *FakeNodeHandler) UpdateStatus(_ context.Context, node *v1.Node, _ metav1.UpdateOptions) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	var origNodeCopy v1.Node
	found := false
	for i := range m.Existing {
		if m.Existing[i].Name == node.Name {
			origNodeCopy = *m.Existing[i]
			found = true
			break
		}
	}
	updatedNodeIndex := -1
	for i := range m.UpdatedNodes {
		if m.UpdatedNodes[i].Name == node.Name {
			origNodeCopy = *m.UpdatedNodes[i]
			updatedNodeIndex = i
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("not found node %v", node)
	}

	origNodeCopy.Status = node.Status
	if updatedNodeIndex < 0 {
		m.UpdatedNodes = append(m.UpdatedNodes, &origNodeCopy)
	} else {
		m.UpdatedNodes[updatedNodeIndex] = &origNodeCopy
	}

	nodeCopy := *node
	m.UpdatedNodeStatuses = append(m.UpdatedNodeStatuses, &nodeCopy)
	return node, nil
}

// PatchStatus 对假存储中某个 Node 的 status 子资源打补丁。
func (m *FakeNodeHandler) PatchStatus(ctx context.Context, nodeName string, data []byte) (*v1.Node, error) {
	m.RequestCount++
	return m.Patch(ctx, nodeName, types.StrategicMergePatchType, data, metav1.PatchOptions{}, "status")
}

// Watch 返回一个假的 watch 接口，用于满足控制器对 Node 变更的监听需求。
func (m *FakeNodeHandler) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	logger := klog.FromContext(ctx)
	return watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger}), nil
}

// Patch 对假存储中的一个 Node 打补丁，支持 JSON/Merge/StrategicMerge/Apply 等补丁类型。
func (m *FakeNodeHandler) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, _ metav1.PatchOptions, subresources ...string) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		if m.PatchWaitChan != nil {
			m.PatchWaitChan <- struct{}{}
		}
		m.lock.Unlock()
	}()
	var nodeCopy v1.Node
	for i := range m.Existing {
		if m.Existing[i].Name == name {
			nodeCopy = *m.Existing[i]
		}
	}
	updatedNodeIndex := -1
	for i := range m.UpdatedNodes {
		if m.UpdatedNodes[i].Name == name {
			nodeCopy = *m.UpdatedNodes[i]
			updatedNodeIndex = i
		}
	}

	originalObjJS, err := json.Marshal(nodeCopy)
	if err != nil {
		klog.FromContext(ctx).Error(nil, "Failed to marshal", "node", klog.KObj(&nodeCopy))
		return nil, nil
	}
	var originalNode v1.Node
	if err = json.Unmarshal(originalObjJS, &originalNode); err != nil {
		klog.FromContext(ctx).Error(err, "Failed to unmarshal original object")
		return nil, nil
	}

	var patchedObjJS []byte
	switch pt {
	case types.JSONPatchType:
		patchObj, err := jsonpatch.DecodePatch(data)
		if err != nil {
			klog.FromContext(ctx).Error(err, "")
			return nil, nil
		}
		if patchedObjJS, err = patchObj.Apply(originalObjJS); err != nil {
			klog.FromContext(ctx).Error(err, "")
			return nil, nil
		}
	case types.MergePatchType:
		if patchedObjJS, err = jsonpatch.MergePatch(originalObjJS, data); err != nil {
			klog.FromContext(ctx).Error(err, "")
			return nil, nil
		}
	case types.StrategicMergePatchType:
		if patchedObjJS, err = strategicpatch.StrategicMergePatch(originalObjJS, data, originalNode); err != nil {
			klog.FromContext(ctx).Error(err, "")
			return nil, nil
		}
	default:
		klog.FromContext(ctx).Error(nil, "Unknown Content-Type header", "patch", pt)
		return nil, nil
	}

	var updatedNode v1.Node
	if err = json.Unmarshal(patchedObjJS, &updatedNode); err != nil {
		klog.FromContext(ctx).Error(err, "Failed to unmarshal patched object")
		return nil, nil
	}

	if updatedNodeIndex < 0 {
		m.UpdatedNodes = append(m.UpdatedNodes, &updatedNode)
	} else {
		if updatedNode.GetObjectMeta().GetResourceVersion() != m.UpdatedNodes[updatedNodeIndex].GetObjectMeta().GetResourceVersion() {
			return nil, apierrors.NewConflict(schema.GroupResource{}, "fake conflict", nil)
		}
		m.UpdatedNodes[updatedNodeIndex] = &updatedNode
	}

	return &updatedNode, nil
}

// Apply 将 NodeApplyConfiguration 以 ApplyPatchType 补丁的形式应用到假存储中的 Node。
func (m *FakeNodeHandler) Apply(ctx context.Context, node *v1apply.NodeApplyConfiguration, opts metav1.ApplyOptions) (*v1.Node, error) {
	patchOpts := opts.ToPatchOptions()
	data, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}
	name := node.Name
	// Apply 语义要求对象名必须存在，否则无法定位目标资源。
	if name == nil {
		return nil, fmt.Errorf("deployment.Name must be provided to Apply")
	}

	return m.Patch(ctx, *name, types.ApplyPatchType, data, patchOpts)
}

// ApplyStatus 将 NodeApplyConfiguration 应用到假存储中某个 Node 的 status 子资源。
func (m *FakeNodeHandler) ApplyStatus(ctx context.Context, node *v1apply.NodeApplyConfiguration, opts metav1.ApplyOptions) (*v1.Node, error) {
	patchOpts := opts.ToPatchOptions()
	data, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}
	name := node.Name
	if name == nil {
		return nil, fmt.Errorf("deployment.Name must be provided to Apply")
	}

	return m.Patch(ctx, *name, types.ApplyPatchType, data, patchOpts, "status")
}

// FakeRecorder 是测试中使用的假 EventRecorder，把事件记录在内存切片里供断言。
type FakeRecorder struct {
	sync.Mutex
	source v1.EventSource
	Events []*v1.Event
	clock  clock.Clock
}

// Event 向假 recorder 发送一条事件。
func (f *FakeRecorder) Event(obj runtime.Object, eventtype, reason, message string) {
	f.generateEvent(obj, metav1.Now(), eventtype, reason, message)
}

// Eventf 向假 recorder 发送一条格式化后的事件。
func (f *FakeRecorder) Eventf(obj runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	f.Event(obj, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

// AnnotatedEventf 发送带注解的事件；此假实现忽略注解，仅委托给 Eventf。
func (f *FakeRecorder) AnnotatedEventf(obj runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	f.Eventf(obj, eventtype, reason, messageFmt, args...)
}

// generateEvent 构造事件并追加到事件列表，全程持锁以保证并发安全。
func (f *FakeRecorder) generateEvent(obj runtime.Object, timestamp metav1.Time, eventtype, reason, message string) {
	f.Lock()
	defer f.Unlock()
	ctx := context.TODO()
	ref, err := ref.GetReference(legacyscheme.Scheme, obj)
	if err != nil {
		klog.FromContext(ctx).Error(err, "Encountered error while getting reference")
		return
	}
	event := f.makeEvent(ref, eventtype, reason, message)
	event.Source = f.source
	if f.Events != nil {
		f.Events = append(f.Events, event)
	}
}

// makeEvent 根据对象引用构造 Event：命名空间为空时回退到 default，
// 并用纳秒时间戳保证事件名唯一。
func (f *FakeRecorder) makeEvent(ref *v1.ObjectReference, eventtype, reason, message string) *v1.Event {
	t := metav1.Time{Time: f.clock.Now()}
	namespace := ref.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}

	clientref := v1.ObjectReference{
		Kind:            ref.Kind,
		Namespace:       ref.Namespace,
		Name:            ref.Name,
		UID:             ref.UID,
		APIVersion:      ref.APIVersion,
		ResourceVersion: ref.ResourceVersion,
		FieldPath:       ref.FieldPath,
	}

	return &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%v.%x", ref.Name, t.UnixNano()),
			Namespace: namespace,
		},
		InvolvedObject: clientref,
		Reason:         reason,
		Message:        message,
		FirstTimestamp: t,
		LastTimestamp:  t,
		Count:          1,
		Type:           eventtype,
	}
}

// NewFakeRecorder 返回一个新建的 FakeRecorder 指针，使用假时钟避免测试依赖真实时间。
func NewFakeRecorder() *FakeRecorder {
	return &FakeRecorder{
		source: v1.EventSource{Component: "nodeControllerTest"},
		Events: []*v1.Event{},
		clock:  testingclock.NewFakeClock(time.Now()),
	}
}

// NewNode 是创建测试用 Node 的辅助函数，预置了 CPU/内存容量。
func NewNode(name string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
			},
		},
	}
}

// NewPod 是创建测试用 Pod 的辅助函数，预置了 Ready 条件。
func NewPod(name, host string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
		},
		Spec: v1.PodSpec{
			NodeName: host,
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

	return pod
}

// contains 按名字判断节点是否已出现在切片中，供 List 去重和删除判断使用。
func contains(node *v1.Node, nodes []*v1.Node) bool {
	for i := 0; i < len(nodes); i++ {
		if node.Name == nodes[i].Name {
			return true
		}
	}
	return false
}

// GetZones 返回 FakeNodeHandler 中所有 Node 所在的 zone 列表。
func GetZones(nodeHandler *FakeNodeHandler) []string {
	nodes, _ := nodeHandler.List(context.TODO(), metav1.ListOptions{})
	zones := sets.NewString()
	// 用 set 收集，因为多个节点可以属于同一个 zone，最终返回需去重。
	for _, node := range nodes.Items {
		zones.Insert(utilnode.GetZoneKey(&node))
	}
	return zones.List()
}

// CreateZoneID 根据 region 和 zone 生成唯一的 zoneID（用 NUL 分隔避免歧义）。
func CreateZoneID(region, zone string) string {
	return region + ":\x00:" + zone
}

// GetKey 是控制器单测中用来获取给定 Kubernetes 资源的 queue key 的辅助函数。
func GetKey(obj interface{}, t *testing.T) string {
	t.Helper()
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if ok {
		// 删除事件可能带来 tombstone：对象已从缓存移除，真实内容保存在 Obj 中。
		obj = tombstone.Obj
	}
	val := reflect.ValueOf(obj).Elem()
	name := val.FieldByName("Name").String()
	if len(name) == 0 {
		t.Errorf("Unexpected object %v", obj)
	}

	key, err := keyFunc(obj)
	if err != nil {
		t.Errorf("Unexpected error getting key for %T %v: %v", val.Interface(), name, err)
		return ""
	}
	return key
}
