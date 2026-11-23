/*
Copyright 2018 The Kubernetes Authors.

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

package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
)

// TestNewNodeLease 用假时钟固定续租时间，使期望值可以精确比较；
// 用例覆盖新建和基于已有对象刷新两条路径，因为两者对 ownerReference 的处理不同：
// 新建依赖后置钩子补全，刷新则必须保留已有的引用。
func TestNewNodeLease(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
			UID:  types.UID("foo-uid"),
		},
	}
	cases := []struct {
		desc       string
		controller *controller
		base       *coordinationv1.Lease
		expect     *coordinationv1.Lease
	}{
		{
			// 场景一：全新租约且集群里没有 Node 对象。钩子补不到 ownerReference，
			// 期望对象只有基本字段，不带任何引用。
			desc: "nil base without node",
			controller: &controller{
				client:               fake.NewSimpleClientset(),
				leaseName:            node.Name,
				holderIdentity:       node.Name,
				leaseDurationSeconds: 10,
				clock:                fakeClock,
			},
			base: nil,
			expect: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](10),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now()},
				},
			},
		},
		{
			// 场景二：全新租约且 Node 存在，后置钩子应当补齐 ownerReference，
			// 同时 spec 里写入持有者、时长和当前续租时间。
			desc: "nil base with node",
			controller: &controller{
				client:               fake.NewSimpleClientset(node),
				leaseName:            node.Name,
				holderIdentity:       node.Name,
				leaseDurationSeconds: 10,
				clock:                fakeClock,
			},
			base: nil,
			expect: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
							Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
							Name:       node.Name,
							UID:        node.UID,
						},
					},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](10),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now()},
				},
			},
		},
		{
			// 场景三：已有租约但缺 ownerReference。刷新时既要补上引用，
			// 也要把过期的 renewTime 推进到当前时间。
			desc: "non-nil base without owner ref, renew time is updated",
			controller: &controller{
				client:               fake.NewSimpleClientset(node),
				holderIdentity:       node.Name,
				leaseDurationSeconds: 10,
				clock:                fakeClock,
			},
			base: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](10),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now().Add(-10 * time.Second)},
				},
			},
			expect: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
							Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
							Name:       node.Name,
							UID:        node.UID,
						},
					},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](10),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now()},
				},
			},
		},
		{
			// 场景四：已有租约且已带 ownerReference。刷新应原样保留引用，只更新 renewTime，
			// 确保 DeepCopy 不会把已有元数据弄丢。
			desc: "non-nil base with owner ref, renew time is updated",
			controller: &controller{
				client:               fake.NewSimpleClientset(node),
				holderIdentity:       node.Name,
				leaseDurationSeconds: 10,
				clock:                fakeClock,
			},
			base: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
							Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
							Name:       node.Name,
							UID:        node.UID,
						},
					},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](10),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now().Add(-10 * time.Second)},
				},
			},
			expect: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
							Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
							Name:       node.Name,
							UID:        node.UID,
						},
					},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](10),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now()},
				},
			},
		},
		{
			// 场景五：配置里的租约时长变了，刷新时必须把 base 里的旧值同步为最新值（456）。
			desc: "non-nil base with owner ref, lease duration is updated",
			controller: &controller{
				client:               fake.NewSimpleClientset(node),
				holderIdentity:       node.Name,
				leaseDurationSeconds: 456,
				clock:                fakeClock,
			},
			base: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
							Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
							Name:       node.Name,
							UID:        node.UID,
						},
					},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](123),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now().Add(-10 * time.Second)},
				},
			},
			expect: &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node.Name,
					Namespace: corev1.NamespaceNodeLease,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
							Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
							Name:       node.Name,
							UID:        node.UID,
						},
					},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr.To(node.Name),
					LeaseDurationSeconds: ptr.To[int32](456),
					RenewTime:            &metav1.MicroTime{Time: fakeClock.Now()},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			tc.controller.newLeasePostProcessFunc = setNodeOwnerFunc(logger, tc.controller.client, node.Name)
			tc.controller.leaseNamespace = corev1.NamespaceNodeLease
			newLease, _ := tc.controller.newLease(tc.base)
			// 地址不同是硬性要求：调用方会持有 base 作为冲突重试的基准，
			// 若就地修改会让一次失败的请求污染后续构造。
			if newLease == tc.base {
				t.Fatalf("the new lease must be newly allocated, but got same address as base")
			}
			if !apiequality.Semantic.DeepEqual(tc.expect, newLease) {
				t.Errorf("expected %#v, got %#v", tc.expect, newLease)
			}
		})
	}
}

// TestRetryUpdateNodeLease 通过 fake client 的 reactor 精确控制每次 UPDATE 的返回，
// 重点验证两件事：冲突时会重新 GET 而不是无限重试旧版本；连续失败才会触发
// onRepeatedHeartbeatFailure，避免一次抖动就当作心跳异常上报。
func TestRetryUpdateNodeLease(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
			UID:  types.UID("foo-uid"),
		},
	}
	gr := schema.GroupResource{Group: "v1", Resource: "lease"}
	noConnectionUpdateErr := apierrors.NewServerTimeout(gr, "put", 1)
	optimistcLockUpdateErr := apierrors.NewConflict(gr, "lease", fmt.Errorf("conflict"))
	cases := []struct {
		desc                       string
		updateReactor              func(action clienttesting.Action) (bool, runtime.Object, error)
		getReactor                 func(action clienttesting.Action) (bool, runtime.Object, error)
		onRepeatedHeartbeatFailure func()
		expectErr                  bool
		client                     *fake.Clientset
	}{
		{
			// 场景一：UPDATE 一次成功，不应报错，也不该触发重复失败回调。
			desc: "no errors",
			updateReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, &coordinationv1.Lease{}, nil
			},
			getReactor:                 nil,
			onRepeatedHeartbeatFailure: nil,
			expectErr:                  false,
			client:                     fake.NewSimpleClientset(node),
		},
		{
			// 场景二：UPDATE 持续返回服务端超时，重试耗尽后应当返回错误。
			desc: "connection errors",
			updateReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, noConnectionUpdateErr
			},
			getReactor:                 nil,
			onRepeatedHeartbeatFailure: nil,
			expectErr:                  true,
			client:                     fake.NewSimpleClientset(node),
		},
		{
			// 场景三：先瞬时错误再乐观锁冲突。冲突后应重新 GET 最新版本并最终更新成功，
			// 且第一次失败不应触发“连续失败”回调。
			desc: "optimistic lock errors",
			updateReactor: func() func(action clienttesting.Action) (bool, runtime.Object, error) {
				i := 0
				return func(action clienttesting.Action) (bool, runtime.Object, error) {
					i++
					switch i {
					case 1:
						return true, nil, noConnectionUpdateErr
					case 2:
						return true, nil, optimistcLockUpdateErr
					default:
						return true, &coordinationv1.Lease{}, nil
					}
				}
			}(),
			getReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, &coordinationv1.Lease{}, nil
			},
			// 冲突重试场景里第一次失败不该触发回调，所以用 Fatalf 把它钉死。
			onRepeatedHeartbeatFailure: func() { t.Fatalf("onRepeatedHeartbeatFailure called") },
			expectErr:                  false,
			client:                     fake.NewSimpleClientset(node),
		},
		{
			// 场景四：节点不存在导致后置钩子失败，期望没有任何 UPDATE 请求发出。
			desc: "node not found errors",
			// 节点不存在时后置钩子会失败，此时不应发出任何 UPDATE；
			// 用 Fatalf 而不是返回错误，是为了抓住“错误被吞掉但请求照发”的实现退化。
			updateReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				t.Fatalf("lease was updated when node does not exist!")
				return true, nil, nil
			},
			getReactor:                 nil,
			onRepeatedHeartbeatFailure: nil,
			expectErr:                  true,
			client:                     fake.NewSimpleClientset(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			cl := tc.client
			if tc.updateReactor != nil {
				cl.PrependReactor("update", "leases", tc.updateReactor)
			}
			if tc.getReactor != nil {
				cl.PrependReactor("get", "leases", tc.getReactor)
			}
			c := &controller{
				clock:                      testingclock.NewFakeClock(time.Now()),
				client:                     cl,
				leaseClient:                cl.CoordinationV1().Leases(corev1.NamespaceNodeLease),
				holderIdentity:             node.Name,
				leaseNamespace:             corev1.NamespaceNodeLease,
				leaseDurationSeconds:       10,
				onRepeatedHeartbeatFailure: tc.onRepeatedHeartbeatFailure,
				newLeasePostProcessFunc:    setNodeOwnerFunc(logger, cl, node.Name),
			}
			// 按用例声明核对最终是否报错：既覆盖重试成功，也覆盖重试耗尽仍失败的路径。
			if err := c.retryUpdateLease(ctx, nil); tc.expectErr != (err != nil) {
				t.Fatalf("got %v, expected %v", err != nil, tc.expectErr)
			}
		})
	}
}

// TestUpdateUsingLatestLease 覆盖 sync 的快路径与回退路径：缓存的 latestLease
// 更新成功时不应发 GET；失败时才回退到 ensureLease 重新查证租约是否存在。
// 断言 resourceVersion 是为了确认最终缓存的是最新一次写入返回的对象。
func TestUpdateUsingLatestLease(t *testing.T) {
	nodeName := "foo"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			UID:  types.UID("foo-uid"),
		},
	}

	notFoundErr := apierrors.NewNotFound(coordinationv1.Resource("lease"), nodeName)
	internalErr := apierrors.NewInternalError(errors.New("unreachable code"))

	// makeLease 构造只带名字与 resourceVersion 的最小租约，便于在各 reactor 和断言间
	// 用版本号追踪对象是否被正确刷新。
	makeLease := func(name, resourceVersion string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       corev1.NamespaceNodeLease,
				Name:            name,
				ResourceVersion: resourceVersion,
			},
		}
	}

	cases := []struct {
		desc                       string
		existingObjs               []runtime.Object
		latestLease                *coordinationv1.Lease
		updateReactor              func(action clienttesting.Action) (bool, runtime.Object, error)
		getReactor                 func(action clienttesting.Action) (bool, runtime.Object, error)
		createReactor              func(action clienttesting.Action) (bool, runtime.Object, error)
		expectLatestLease          bool
		expectLeaseResourceVersion string
	}{
		{
			// 场景一：没有缓存且租约不存在，走创建路径；成功后 latestLease 应缓存新对象（RV=1）。
			desc:          "latestLease is nil and need to create",
			existingObjs:  []runtime.Object{node},
			latestLease:   nil,
			updateReactor: nil,
			getReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, notFoundErr
			},
			createReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, makeLease(nodeName, "1"), nil
			},
			expectLatestLease:          true,
			expectLeaseResourceVersion: "1",
		},
		{
			// 场景二：同样走创建路径，但 Node 不存在导致后置钩子失败，创建被跳过，
			// latestLease 应保持 nil。
			desc:          "latestLease is nil and need to create, node doesn't exist",
			existingObjs:  nil,
			latestLease:   nil,
			updateReactor: nil,
			getReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, notFoundErr
			},
			createReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, makeLease(nodeName, "1"), nil
			},
			expectLatestLease:          false,
			expectLeaseResourceVersion: "1",
		},
		{
			// 场景三：无缓存但租约已存在，走 GET + UPDATE；缓存应更新为 UPDATE 返回的版本（RV=2）。
			desc:         "latestLease is nil and need to update",
			existingObjs: []runtime.Object{node},
			latestLease:  nil,
			updateReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, makeLease(nodeName, "2"), nil
			},
			getReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, makeLease(nodeName, "1"), nil
			},
			expectLatestLease:          true,
			expectLeaseResourceVersion: "2",
		},
		{
			// 场景四：缓存命中走快路径直接 UPDATE，不发 GET；缓存更新为返回值（RV=2）。
			desc:         "latestLease exist and need to update",
			existingObjs: []runtime.Object{node},
			latestLease:  makeLease(nodeName, "1"),
			updateReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, makeLease(nodeName, "2"), nil
			},
			expectLatestLease:          true,
			expectLeaseResourceVersion: "2",
		},
		{
			// 先用缓存版本更新失败，再让回退路径拿到一个更新的版本，
			// 验证 sync 最终把 latestLease 刷新成回退后写入返回的对象。
			desc:         "update with latest lease failed",
			existingObjs: []runtime.Object{node},
			latestLease:  makeLease(nodeName, "1"),
			updateReactor: func() func(action clienttesting.Action) (bool, runtime.Object, error) {
				i := 0
				return func(action clienttesting.Action) (bool, runtime.Object, error) {
					i++
					switch i {
					case 1:
						return true, nil, notFoundErr
					case 2:
						return true, makeLease(nodeName, "3"), nil
					default:
						t.Fatalf("unexpect call update lease")
						return true, nil, internalErr
					}
				}
			}(),
			getReactor: func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, makeLease(nodeName, "2"), nil
			},
			expectLatestLease:          true,
			expectLeaseResourceVersion: "3",
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			cl := fake.NewSimpleClientset(tc.existingObjs...)
			if tc.updateReactor != nil {
				cl.PrependReactor("update", "leases", tc.updateReactor)
			}
			if tc.getReactor != nil {
				cl.PrependReactor("get", "leases", tc.getReactor)
			}
			if tc.createReactor != nil {
				cl.PrependReactor("create", "leases", tc.createReactor)
			}
			c := &controller{
				clock:                   testingclock.NewFakeClock(time.Now()),
				client:                  cl,
				leaseClient:             cl.CoordinationV1().Leases(corev1.NamespaceNodeLease),
				holderIdentity:          node.Name,
				leaseNamespace:          corev1.NamespaceNodeLease,
				leaseDurationSeconds:    10,
				latestLease:             tc.latestLease,
				newLeasePostProcessFunc: setNodeOwnerFunc(logger, cl, node.Name),
			}

			c.sync(ctx)

			// 断言缓存状态：期望有缓存时核对 resourceVersion（确认缓存的是最新写入），
			// 期望无缓存时确认缓存仍为 nil（钩子失败时不应留下半成品）。
			if tc.expectLatestLease {
				if tc.expectLeaseResourceVersion != c.latestLease.ResourceVersion {
					t.Fatalf("latestLease RV got %v, expected %v", c.latestLease.ResourceVersion, tc.expectLeaseResourceVersion)
				}
			} else {
				if c.latestLease != nil {
					t.Fatalf("unexpected latestLease: %v", c.latestLease)
				}
			}
		})
	}
}

// TestStopDoesNotBlockWhenLeaseCannotBeCreated 验证：即使 sync 正在重试一个永远不会成功的租约创建，
// Stop 也必须及时返回。sync 在整个重试期间持有 reconcilingLock，所以 Stop 必须能打断它。
// 用 channel 观测第一次 CREATE，是为了确保测试确实停在重试循环内部，
// 而不是碰巧在两次重试的间隙里通过了停机检查。
func TestStopDoesNotBlockWhenLeaseCannotBeCreated(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cl := fake.NewSimpleClientset()
	retrying := make(chan struct{})
	var retryingOnce sync.Once
	cl.PrependReactor("create", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		retryingOnce.Do(func() { close(retrying) })
		return true, nil, apierrors.NewInvalid(
			schema.GroupKind{Group: coordinationv1.GroupName, Kind: "Lease"}, "foo", nil)
	})

	c := NewController(testingclock.NewFakeClock(time.Now()), cl, "foo", 60, nil,
		10*time.Millisecond, "foo", corev1.NamespaceNodeLease, nil)
	go c.Run(ctx)

	// 等到 sync 真正进入创建重试循环并持有 reconcilingLock，
	// 确保后续对 Stop 的检验发生在最坏时序下。
	select {
	case <-retrying:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("timed out waiting for the controller to attempt lease creation")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		c.Stop()
	}()

	select {
	case <-stopped:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("Stop() did not return while lease creation keeps failing")
	}

	// Stop 必须可以安全地重复调用。
	// 再次调用用于验证 stopOnce：重复 close 同一个 channel 会 panic。
	c.Stop()
}

// setNodeOwnerFunc 构造一个 newLeasePostProcessFunc，用于给租约对象补上指向对应节点的
// OwnerReference。
// 这里把“每次续租都尝试补 ownerReference”做成后置钩子，而不是控制器内置逻辑，
// 是因为节点 UID 在首次创建租约时未必查得到；即使钩子报错，创建路径也会吞掉错误、
// 下一轮再试，而更新路径照常提交，让心跳业务优先于附加元数据。
func setNodeOwnerFunc(logger klog.Logger, c clientset.Interface, nodeName string) func(lease *coordinationv1.Lease) error {
	return func(lease *coordinationv1.Lease) error {
		// 设置 ownerReference 需要节点的 UID，注意它与 kubelet.nodeRef.UID 并不是一回事。
		// 租约首次创建时，master 与 node 之间的连接可能还没就绪，暂时查不到节点；
		// 所以每次续租都重新尝试补一次，直到成功为止。
		if len(lease.OwnerReferences) == 0 {
			if node, err := c.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{}); err == nil {
				lease.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
						Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
						Name:       nodeName,
						UID:        node.UID,
					},
				}
			} else {
				logger.Error(err, "failed to get node when trying to set owner ref to the node lease", "node", nodeName)
				return err
			}
		}
		return nil
	}
}
