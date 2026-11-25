/*
Copyright 2022 The Kubernetes Authors.

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

package utils

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"google.golang.org/grpc"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	cgoresource "k8s.io/client-go/kubernetes/typed/resource/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	draclient "k8s.io/dynamic-resource-allocation/client"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/cmd/exec"
	"k8s.io/kubernetes/test/e2e/dra/test-driver/app"
	"k8s.io/kubernetes/test/e2e/dra/test-driver/deploy/example"
	testdrivergomega "k8s.io/kubernetes/test/e2e/dra/test-driver/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2ereplicaset "k8s.io/kubernetes/test/e2e/framework/replicaset"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/storage/drivers/proxy"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

type Nodes struct {
	// NodeNames 保存测试使用的主要节点名集合。
	NodeNames []string
	tempDir   string
	// NumReservedNodes 指定要预留的额外节点数量。
	// 这些节点名会存入 ExtraNodeNames。
	//
	// 必须小于等于请求的最少节点数。
	NumReservedNodes int
	// ExtraNodeNames 中的节点名数量与 NumReservedNodes 的请求值一致，
	// 且这些节点与 NodeNames 中列出的节点不同。
	ExtraNodeNames []string
}

// NewNodes 选择运行测试的节点。
//
// 在 ginkgo.It 之外调用本函数，然后在 ginkgo.It 内使用该实例。
func NewNodes(f *framework.Framework, minNodes, maxNodes int) *Nodes {
	nodes := &Nodes{}
	ginkgo.BeforeEach(func(ctx context.Context) {
		nodes.init(f.TContext(ctx), minNodes, maxNodes)
	})
	return nodes
}

// NewNodesNow 是 NewNodes 的变体，可在 ginkgo.It 或 Go 单元测试内使用。
func NewNodesNow(tCtx ktesting.TContext, minNodes, maxNodes int) *Nodes {
	nodes := &Nodes{}
	nodes.init(tCtx, minNodes, maxNodes)
	return nodes
}

func (nodes *Nodes) init(tCtx ktesting.TContext, minNodes, maxNodes int) {
	nodes.tempDir = tCtx.TempDir()

	tCtx.Log("selecting nodes")
	// kubelet 插件的处理更复杂：需要先把驱动名和要运行插件的节点
	// 打进内置清单，再部署它。
	//
	// 这里只挑选一部分节点，避免在大规模集群上造成不必要的负载。
	nodeList, err := e2enode.GetBoundedReadySchedulableNodes(tCtx, tCtx.Client(), maxNodes)
	tCtx.ExpectNoError(err, "get nodes")
	numNodes := int32(len(nodeList.Items))
	if int(numNodes) < minNodes {
		e2eskipper.Skipf("%d ready nodes required, only have %d", minNodes+nodes.NumReservedNodes, numNodes)
	}
	nodes.NodeNames = nil
	for i, node := range nodeList.Items {
		if i < nodes.NumReservedNodes {
			nodes.ExtraNodeNames = append(nodes.ExtraNodeNames, node.Name)
			continue
		}
		nodes.NodeNames = append(nodes.NodeNames, node.Name)
	}
	sort.Strings(nodes.NodeNames)
	tCtx.Logf("testing on nodes %v", nodes.NodeNames)

	// 监听命名空间中的 claim：既便于观察测试进展，也能做额外的健全性检查。
	resourceClaimLogger := klog.LoggerWithName(klog.FromContext(tCtx), "ResourceClaimListWatch")
	var resourceClaimWatchCounter atomic.Int32
	resourceClient := draclient.New(tCtx.Client())
	claimInformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
				tCtx := tCtx.WithContext(ctx)
				slices, err := resourceClient.ResourceClaims(tCtx.Namespace()).List(tCtx, options)
				if err == nil {
					resourceClaimLogger.Info("Listed ResourceClaims", "resourceAPI", resourceClient.CurrentAPI(), "numClaims", len(slices.Items), "listMeta", slices.ListMeta)
				} else {
					resourceClaimLogger.Info("Listing ResourceClaims failed", "resourceAPI", resourceClient.CurrentAPI(), "err", err)
				}
				return slices, err
			},
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				tCtx := tCtx.WithContext(ctx)
				w, err := resourceClient.ResourceClaims(tCtx.Namespace()).Watch(tCtx, options)
				if err == nil {
					resourceClaimLogger.Info("Started watching ResourceClaims", "resourceAPI", resourceClient.CurrentAPI())
					wrapper := newWatchWrapper(klog.LoggerWithName(resourceClaimLogger, fmt.Sprintf("%d", resourceClaimWatchCounter.Load())), w)
					resourceClaimWatchCounter.Add(1)
					go wrapper.run()
					w = wrapper
				} else {
					resourceClaimLogger.Info("Watching ResourceClaims failed", "resourceAPI", resourceClient.CurrentAPI(), "err", err)
				}
				return w, err
			},
		},
		&resourceapi.ResourceClaim{},
		// 不做 resync：它只会把旧/新对象都设成同一个对象，把所有 slice 报成
		// 已更新，从而周期性地重新触发 pool 同步，没有实际价值。
		0,
		nil,
	)
	cancelCtx, cancel := context.WithCancelCause(context.Background())
	var wg sync.WaitGroup
	tCtx.Cleanup(func() {
		cancel(errors.New("test has completed"))
		wg.Wait()
	})
	_, err = claimInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			claim := obj.(*resourceapi.ResourceClaim)
			resourceClaimLogger.Info("New claim", "claim", format.Object(claim, 0))
			validateClaim(tCtx, claim)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldClaim := oldObj.(*resourceapi.ResourceClaim)
			newClaim := newObj.(*resourceapi.ResourceClaim)
			resourceClaimLogger.Info("Updated claim", "newClaim", format.Object(newClaim, 0), "diff", cmp.Diff(oldClaim, newClaim))
			validateClaim(tCtx, newClaim)
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			claim := obj.(*resourceapi.ResourceClaim)
			resourceClaimLogger.Info("Deleted claim", "claim", format.Object(claim, 0))
		},
	})
	tCtx.ExpectNoError(err, "AddEventHandler")
	wg.Add(1)
	go func() {
		defer wg.Done()
		claimInformer.RunWithContext(cancelCtx)
	}()
}

type watchWrapper struct {
	logger     klog.Logger
	delegate   watch.Interface
	resultChan chan watch.Event
}

func newWatchWrapper(logger klog.Logger, delegate watch.Interface) *watchWrapper {
	return &watchWrapper{
		logger:     logger,
		delegate:   delegate,
		resultChan: make(chan watch.Event, 100),
	}
}

func (w *watchWrapper) run() {
	defer utilruntime.HandleCrashWithLogger(w.logger)
	defer close(w.resultChan)
	inputChan := w.delegate.ResultChan()
	for {
		event, ok := <-inputChan
		if !ok {
			w.logger.Info("Wrapped result channel was closed, stopping event forwarding")
			return
		}
		w.logger.Info("Received event", "event", event.Type, "content", fmt.Sprintf("%T", event.Object))
		w.resultChan <- event
	}
}

func (w *watchWrapper) Stop() {
	w.delegate.Stop()
}

func (w *watchWrapper) ResultChan() <-chan watch.Event {
	return w.resultChan
}

func validateClaim(tCtx ktesting.TContext, claim *resourceapi.ResourceClaim) {
	// apiserver 不会强制要求已分配的 claim 始终带有 finalizer。
	// 这是分配方必须遵守的约定，用来避免使用可能被删除的 claim。
	if claim.Status.Allocation != nil && len(claim.Finalizers) == 0 {
		tCtx.Errorf("Invalid claim: allocated without any finalizer:\n%s", format.Object(claim, 1))
	}
}

const (
	// multiHostDriverResources 标识与多个设备关联、即不由某个驱动管理的
	// DriverResources。这类 Pools 和 ResourceSlices 会直接发布到集群，
	// 而不是通过驱动发布。
	multiHostDriverResources = "multi-host"

	// LongRollingUpdateDriverName 是一个 30 字符、*.sigs.k8s.io 风格的驱动名。
	// 在滚动更新时，旧版注册 socket 的基名
	// ({driver}-{pod UID}-reg.sock) 会超过常见的 AF_UNIX 路径长度限制；
	// 参见 https://github.com/kubernetes/kubernetes/issues/139166。
	LongRollingUpdateDriverName = "gpu.dra-example-driver.sigs.k8s.io"
)

// driverResourcesGenFunc 定义由驱动调用的回调，用于生成构造 ResourceSlices
// 所需的 DriverResources。
type driverResourcesGenFunc func(nodes *Nodes) map[string]resourceslice.DriverResources

// driverResourcesMutatorFunc 定义在 DriverResources 生成后对其进行修改的
// mutator 的函数签名。
type driverResourcesMutatorFunc func(map[string]resourceslice.DriverResources)

// NewDriver 在测试运行前搭好 controller（作为集群客户端）和
// kubelet 插件（通过代理），并在测试结束后清理。
//
// 在 ginkgo.It 之外调用本函数，然后在 ginkgo.It 内使用该实例。
func NewDriver(f *framework.Framework, nodes *Nodes, driverResourcesGenerator driverResourcesGenFunc, driverResourcesMutators ...driverResourcesMutatorFunc) *Driver {
	d := NewDriverInstance(ktesting.TContext{} /* no namespace yet, will be set later */)

	ginkgo.BeforeEach(func() {
		tCtx := f.TContext(context.Background())
		d.initName(tCtx)
		driverResources := driverResourcesGenerator(nodes)
		for _, mutator := range driverResourcesMutators {
			mutator(driverResources)
		}
		d.Run(tCtx, framework.TestContext.KubeletRootDir, nodes, driverResources)
	})
	return d
}

// NewDriverInstance 是 NewDriver 的变体：创建的驱动处于未激活状态，
// 必须显式调用 Run 启动。可在 ginkgo.It 或 Go 单元测试内使用。
// 该 context 用于确定测试的命名空间，也就是驱动的命名空间。
func NewDriverInstance(tCtx ktesting.TContext) *Driver {
	d := &Driver{
		fail:       map[MethodInstance]bool{},
		callCounts: map[MethodInstance]int64{},
		// 默认测试所有 gRPC API。
		// TODO: 是否应让这里可配置，以便测试 helper 实际的默认值？
		NodeV1:      true,
		NodeV1beta1: true,
		// 默认假定 kubelet 支持 DRA，且驱动移除会触发 ResourceSlice 清理。
		WithKubelet:                true,
		WithRealNodes:              true,
		ExpectResourceSliceRemoval: true,
	}
	d.initName(tCtx)
	return d
}

// ClientV1 返回一个 client-go 包装器，无论集群启用了哪些 API 版本，
// 它都提供 V1 API。
func (d *Driver) ClientV1(tCtx ktesting.TContext) cgoresource.ResourceV1Interface {
	return draclient.New(tCtx.Client())
}

func (d *Driver) Run(tCtx ktesting.TContext, kubeletRootDir string, nodes *Nodes, driverResources map[string]resourceslice.DriverResources) {
	d.SetUp(tCtx, kubeletRootDir, nodes, driverResources)
	tCtx.CleanupCtx(d.TearDown)
}

// PublishResources 通过已在运行的 kubelet 插件重新发布给定的按节点划分的
// driver resources，替换此前发布的内容。它用于 feature gate 切换后重置
// resourceslice controller 的期望状态：gate 关闭时 apiserver 会丢弃受控字段，
// 而 controller 会把期望状态锁定为存储中（被裁剪过的）结果，以避免热更新循环。
// gate 重新打开后再发布原始资源，就能恢复这些字段。本方法只处理按节点发布的
// 路径，不处理 multi-host DriverResources。
func (d *Driver) PublishResources(tCtx ktesting.TContext, driverResources map[string]resourceslice.DriverResources) {
	for nodename, plugin := range d.Nodes {
		dr, ok := driverResources[nodename]
		if !ok {
			continue
		}
		tCtx.ExpectNoError(plugin.PublishResources(tCtx, dr), "re-publish resources for node %s", nodename)
	}
}

// SetExpectDroppedFields 控制驱动的错误处理器是否容忍 apiserver 丢弃已发布
// ResourceSlice 中的字段。在关闭某个控制 ResourceSlice 字段的 feature gate
// 之前必须设为 true；gate 重新打开且驱动重新发布受影响的 slice 后再设回 false。
// 详见 expectDroppedFields 字段。
func (d *Driver) SetExpectDroppedFields(expect bool) {
	d.expectDroppedFields.Store(expect)
}

// NewGetSlices 生成一个供 ktesting.Eventually/Consistently 使用的函数，
// 该函数返回 ResourceSliceList。
func (d *Driver) NewGetSlices() func(tCtx ktesting.TContext) *resourceapi.ResourceSliceList {
	return func(tCtx ktesting.TContext) *resourceapi.ResourceSliceList {
		slices, err := framework.ListObjects(d.ClientV1(tCtx).ResourceSlices().List, metav1.ListOptions{FieldSelector: resourceapi.ResourceSliceSelectorDriver + "=" + d.Name})(tCtx)
		tCtx.ExpectNoError(err, "list ResourceSlices")
		return slices
	}
}

type MethodInstance struct {
	NodeName   string
	FullMethod string
}

type Driver struct {
	cleanup            []func(ktesting.TContext) // 按先进先出顺序执行
	wg                 sync.WaitGroup
	serviceAccountName string

	// NameSuffix 可在注册测试时设置，用于在同一测试命名空间中部署不同的驱动。
	NameSuffix string

	// InstanceSuffix 可在注册测试时设置，用于部署同一驱动的两个不同实例。
	// 它用于在 API server 中生成唯一对象，socket 路径仍保持不变。
	InstanceSuffix string

	// RollingUpdate 设为 true 时，不同 pod 使用不同的 socket 名，从而实现无缝升级。
	// 这需要 kubelet 支持！
	RollingUpdate bool

	// 正常情况下，拆除驱动最终应导致 ResourceSlice 被移除。
	// 例外情况是驱动属于滚动更新的一部分且先被拆除时。
	ExpectResourceSliceRemoval bool

	// Name 在为测试搭建驱动时，由当前测试命名空间和（若设置了）NameSuffix
	// 自动推导得出。
	Name string

	// Nodes 包含测试运行时所选每个节点的条目，另有一个条目对应虚构节点。
	Nodes map[string]KubeletPlugin

	// 使用 local-up-cluster.sh 时，若同时确保当前用户可写
	// /var/lib/kubelet/plugins、/var/lib/kubelet/plugins_registry 和
	// /var/run/cdi，可将 IsLocal 设为 true。
	IsLocal bool

	NodeV1      bool
	NodeV1beta1 bool

	// 向 kubelet 注册 DRA 测试驱动，并期望 DRA 正常工作（即 feature.DynamicResourceAllocation）。
	WithKubelet bool

	// UsePrivilegedClient 允许测试驱动发布集群范围的 ResourceSlices。
	// 默认的节点级客户端被 admission 有意限制为只能操作本节点的 ResourceSlices。
	UsePrivilegedClient bool

	// 运行驱动 pod。设为 false 时只创建 slice 和 class。
	WithRealNodes bool

	EnableDeviceMetadata   bool
	DeviceMetadataVersions []schema.GroupVersion // EnableDeviceMetadata 为 true 时必须非空。

	// ReconcilePoolWithName 让每个测试驱动插件中的 ResourceSlice controller
	// 只 reconcile 具有该名称的 pool。
	ReconcilePoolWithName string

	// expectDroppedFields 为 true 时，会抑制驱动错误处理器在 apiserver 丢弃
	// 已发布 ResourceSlice 中的字段（resourceslice.DroppedFieldsError）时
	// 引发的测试失败。feature gate 循环测试会在有意关闭某个 gate 期间设置它，
	// 因为此时预期 apiserver 会丢弃受 gate 控制的字段。该值由 resourceslice
	// controller 的后台 goroutine 读取，因此通过 atomic 访问。
	expectDroppedFields atomic.Bool

	mutex      sync.Mutex
	fail       map[MethodInstance]bool
	callCounts map[MethodInstance]int64
}

type KubeletPlugin struct {
	*app.ExamplePlugin
	ClientSet kubernetes.Interface
}

func (d *Driver) initName(tCtx ktesting.TContext) {
	d.Name = tCtx.Namespace() + d.NameSuffix + ".k8s.io"
}

func (d *Driver) SetNameSuffix(tCtx ktesting.TContext, suffix string) {
	d.NameSuffix = suffix
	d.initName(tCtx)
}

// deploymentID 返回一个由驱动名和实例后缀派生的标识符，用作 Kubernetes 对象
// （ServiceAccount、ClusterRole 等）的名称。API 和 kubelet 插件仍使用 d.Name
// 中保存的完整驱动名。当完整名称超过对象名长度限制时，使用简短的哈希形式。
func (d *Driver) deploymentID() string {
	base := d.Name + d.InstanceSuffix
	const maxLen = 40 // 给 "dra-kubelet-plugin-" 和 "-service-account" 留出空间
	if len(base) <= maxLen {
		return base
	}
	sum := sha256.Sum256([]byte(base))
	return "dra-" + hex.EncodeToString(sum[:8])
}

func (d *Driver) SetUp(tCtx ktesting.TContext, kubeletRootDir string, nodes *Nodes, driverResources map[string]resourceslice.DriverResources) {
	tCtx.Logf("deploying driver %s on nodes %v", d.Name, nodes.NodeNames)
	d.Nodes = make(map[string]KubeletPlugin)

	tCtx = tCtx.WithCancel()
	logger := klog.FromContext(tCtx)
	logger = klog.LoggerWithValues(logger, "driverName", d.Name)
	if d.InstanceSuffix != "" {
		instance, _ := strings.CutPrefix(d.InstanceSuffix, "-")
		logger = klog.LoggerWithValues(logger, "instance", instance)
	}
	tCtx = tCtx.WithLogger(logger)
	d.cleanup = append(d.cleanup, func(ktesting.TContext) { tCtx.Cancel("cleaning up test") })

	// 关闭后检查所有 ResourceSlice 都已被移除，无论是被 kubelet 还是被测试代码移除。
	// 它最先注册，所以最后执行。
	if d.ExpectResourceSliceRemoval {
		tCtx.CleanupCtx(d.IsGone)
	}

	driverResource, useMultiHostDriverResources := driverResources[multiHostDriverResources]
	if useMultiHostDriverResources || !d.WithKubelet || d.UsePrivilegedClient {
		// 必须自行删除 ResourceSlice。否则正常情况下 kubelet 会在注销驱动后删除它们，
		// 但 privileged client 创建的集群级 slice 不属于 kubelet，kubelet 无法删除。
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().ResourceV1().ResourceSlices().DeleteCollection(tCtx, metav1.DeleteOptions{}, metav1.ListOptions{FieldSelector: resourceapi.ResourceSliceSelectorDriver + "=" + d.Name})
			tCtx.ExpectNoError(err, "delete ResourceSlices of the driver")
		})
	}

	// 若存在，就通过 node selector 创建与多个节点关联的 ResourceSlice。
	// 因此这些 ResourceSlice 在此处发布，而不是由某个特定节点上的驱动发布。
	if useMultiHostDriverResources {
		for poolName, pool := range driverResource.Pools {
			for i, slice := range pool.Slices {
				resourceSlice := &resourceapi.ResourceSlice{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("%s-%d", d.Name, i), // 全局唯一
					},
					Spec: resourceapi.ResourceSliceSpec{
						Driver: d.Name,
						Pool: resourceapi.ResourcePool{
							Name:               poolName,
							Generation:         pool.Generation,
							ResourceSliceCount: int64(len(pool.Slices)),
						},
						NodeSelector:           pool.NodeSelector,
						Devices:                slice.Devices,
						SharedCounters:         slice.SharedCounters,
						PerDeviceNodeSelection: slice.PerDeviceNodeSelection,
						PartitionTypeAttribute: slice.PartitionTypeAttribute,
					},
				}
				_, err := tCtx.Client().ResourceV1().ResourceSlices().Create(tCtx, resourceSlice, metav1.CreateOptions{})
				tCtx.ExpectNoError(err)
			}
		}
	}

	if !d.WithRealNodes {
		// slice 已照常创建。没有真实节点时无法运行 pod，可以跳过。
		return
	}

	// 创建 service account 及相应的 RBAC 规则。
	deploymentID := d.deploymentID()
	d.serviceAccountName = "dra-kubelet-plugin-" + deploymentID + "-service-account"
	content := example.PluginPermissions

	content = strings.ReplaceAll(content, "dra-kubelet-plugin-namespace", tCtx.Namespace())
	content = strings.ReplaceAll(content, "dra-kubelet-plugin-driver-name", d.Name)
	content = strings.ReplaceAll(content, "dra-kubelet-plugin", "dra-kubelet-plugin-"+deploymentID)
	d.createFromYAML(tCtx, []byte(content), tCtx.Namespace())

	// 确定使用哪个 hostpathplugin：基本上就是 test/e2e/testing-manifests/storage-csi
	// 清单里的最新版本。SIG Storage 在这些清单中维护着 Kubernetes E2E 测试所用的
	// hostpath 镜像版本，test/utils/image 会解析这些文件。
	//
	// 这里借用该机制而不是自行控制版本，可以省些力气，代价是可能出现意外损坏。
	// 另一个好处是 -list-images 以及通过 test/utils/image + KUBE_TEST_REPO_LIST
	// 进行的 registry 替换都能正常工作。
	hostPathImage := "registry.k8s.io/sig-storage/hostpathplugin"
	hostPathVersion := ""
	for _, config := range image.GetOriginalImageConfigs() {
		parts := strings.SplitN(config.GetE2EImage(), ":", 2)
		if len(parts) < 2 {
			continue
		}
		image, version := parts[0], parts[1]
		if image != hostPathImage {
			continue
		}
		// 对 v1.16.1 < v1.17.0 这类比较，"朴素"的字符串比较就够用了。
		// major/patch 版本号需要多位数字、或版本增长超过 99 的可能性不大。
		if hostPathVersion == "" || hostPathVersion < version {
			hostPathVersion = version
		}
	}
	origImageURL := hostPathImage + ":" + hostPathVersion
	patchedImageURL, err := image.ReplaceRegistryInImageURL(origImageURL)
	tCtx.ExpectNoError(err, "look up E2E image")

	// 用 ReplicaSet 而不是 DaemonSet 的好处是能显式控制生命周期，
	// 尤其是可以让每个节点上的两个 pod 存活足够长的时间来完成检查。
	manifests := []string{
		// 下面的代码与该清单的内容（端口、容器名等）相互对应。
		"test/e2e/testing-manifests/dra/dra-test-driver-proxy.yaml",
	}
	instanceKey := "app.kubernetes.io/instance"
	rsName := ""
	numNodes := int32(len(nodes.NodeNames))
	pluginDataDirectoryPath := path.Join(kubeletRootDir, "plugins", d.Name)
	registrarDirectoryPath := path.Join(kubeletRootDir, "plugins_registry")
	instanceName := d.Name + d.InstanceSuffix
	err = utils.CreateFromManifestsTCtx(tCtx, func(item interface{}) error {
		switch item := item.(type) {
		case *appsv1.ReplicaSet:
			item.Name += d.NameSuffix + d.InstanceSuffix
			rsName = item.Name
			item.Spec.Replicas = &numNodes
			item.Spec.Selector.MatchLabels[instanceKey] = instanceName
			item.Spec.Template.Labels[instanceKey] = instanceName
			item.Spec.Template.Spec.Containers[0].Image = patchedImageURL
			item.Spec.Template.Spec.ServiceAccountName = d.serviceAccountName
			item.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchLabels[instanceKey] = instanceName
			item.Spec.Template.Spec.Affinity.NodeAffinity = &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: v1.NodeSelectorOpIn,
									Values:   nodes.NodeNames,
								},
							},
						},
					},
				},
			}
			if d.IsLocal {
				// 本地模式下所有操作都在本机运行，去掉目录挂载。
				item.Spec.Template.Spec.Volumes = nil
				item.Spec.Template.Spec.Containers[0].VolumeMounts = nil
				// 同样不需要特权。
				item.Spec.Template.Spec.SecurityContext = nil
				item.Spec.Template.Spec.Containers[0].SecurityContext = nil
			} else {
				item.Spec.Template.Spec.Volumes[0].HostPath.Path = pluginDataDirectoryPath
				item.Spec.Template.Spec.Volumes[1].HostPath.Path = registrarDirectoryPath
				item.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = pluginDataDirectoryPath
				item.Spec.Template.Spec.Containers[0].VolumeMounts[1].MountPath = registrarDirectoryPath
			}
		}
		return nil
	}, manifests...)
	tCtx.ExpectNoError(err, "deploy kubelet plugin replicaset")

	rs, err := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Get(tCtx, rsName, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get replicaset")

	// 等待所有 pod 进入 running 状态。
	if err := e2ereplicaset.WaitForReplicaSetTargetAvailableReplicas(tCtx, tCtx.Client(), rs, numNodes); err != nil {
		tCtx.ExpectNoError(err, "all kubelet plugin proxies running")
	}
	requirement, err := labels.NewRequirement(instanceKey, selection.Equals, []string{instanceName})
	tCtx.ExpectNoError(err, "create label selector requirement")
	selector := labels.NewSelector().Add(*requirement)
	pods, err := tCtx.Client().CoreV1().Pods(tCtx.Namespace()).List(tCtx, metav1.ListOptions{LabelSelector: selector.String()})
	tCtx.ExpectNoError(err, "list proxy pods")
	tCtx.Expect(numNodes).To(gomega.Equal(int32(len(pods.Items))), "number of proxy pods")
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].Spec.NodeName < pods.Items[j].Spec.NodeName
	})

	// 为每个 pod 运行 registrar 和插件。
	for _, pod := range pods.Items {
		nodename := pod.Spec.NodeName

		// 对插件进行身份认证，使其拥有与 daemonset pod 完全相同的权限，
		// 包括 RBAC 以及限制只能写本节点 ResourceSlice 的 validating admission policy。
		//
		// 也可以从每个 pod 中取出
		// /var/run/secrets/kubernetes.io/serviceaccount/token 并使用，
		// 这样能验证 ServiceAccountTokenNodeBindingValidation 是否生效。
		// 不过这部分由 SIG Auth 旗下的测试覆盖更合适
		// （例如 https://github.com/kubernetes/kubernetes/pull/124711 中的测试）。
		//
		// 这里只使用 impersonation，速度更快。
		driverClient := d.ImpersonateKubeletPlugin(tCtx, &pod)
		if d.UsePrivilegedClient {
			driverClient = tCtx.Client()
		}

		logger := klog.LoggerWithValues(klog.LoggerWithName(logger, "kubelet-plugin"), "node", pod.Spec.NodeName, "pod", klog.KObj(&pod))
		loggerCtx := klog.NewContext(tCtx, logger)
		fileOps := app.FileOperations{
			Create: func(name string, content []byte) error {
				logger.Info("creating CDI file", "node", nodename, "filename", name, "content", string(content))
				if d.IsLocal {
					// 名字以 /cdi 开头，这是它在容器中的映射路径。
					// 在本地需要把它放到 /var/run 下。
					// /var/run/cdi 可能尚不存在，需要先创建。
					name = path.Join("/var/run", name)
					if err := os.MkdirAll(path.Dir(name), 0700); err != nil {
						return fmt.Errorf("create CDI directory: %w", err)
					}
					if err := os.WriteFile(name, content, 0644); err != nil {
						return fmt.Errorf("write CDI file: %w", err)
					}
					return nil
				}
				return d.createFile(tCtx, &pod, name, content)
			},
			Remove: func(name string) error {
				logger.Info("deleting CDI file", "node", nodename, "filename", name)
				if d.IsLocal {
					name = path.Join("/var/run", name)
					// 忽略文件已不存在的情况。NodeUnprepareResources 必须幂等，
					// 而且在 feature gate 切换循环中 kubelet 会重启，可能对同一个
					// claim 执行多次 unprepare。这与默认的 FileOperations.Remove 一致。
					if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
						return err
					}
					return nil
				}
				return d.removeFile(tCtx, &pod, name)
			},
			HandleError: func(ctx context.Context, err error, msg string) {
				// 记录失败，但不要杀掉后台 goroutine。
				// TODO: 是否加到 TContext 里，或在 Error/Assert 等方法中处理？
				defer ginkgo.GinkgoRecover()

				// 测试中取消 context 时，可能收到各种随之而来的错误，例如：
				//   processing ResourceSlice objects: retrieve node "127.0.0.1": client rate limiter Wait returned an error: context canceled
				//
				// "context canceled" 错误没有被包装，因此 `errors.Is` 不起作用。
				// 与其尝试判断哪些错误可以忽略，不如只把肯定不应发生的错误视为失败：
				var droppedFields *resourceslice.DroppedFieldsError
				if errors.As(err, &droppedFields) && !d.expectDroppedFields.Load() {
					tCtx.Errorf("driver %s: %v", d.Name, err)
				}
			},
		}

		if dr, ok := driverResources[nodename]; ok {
			fileOps.DriverResources = &dr
		}
		// 该 pod 中所有 listener 通过对这个变量做原子自增，各自获得一个唯一的本地端口。
		var listenerPort atomic.Int32
		listenerPort.Store(9000)
		rollingUpdateUID := pod.UID
		serialize := true
		if !d.RollingUpdate {
			rollingUpdateUID = ""
			// 测试可能需要并行执行两个 gRPC 调用，因此只有明确要测试滚动更新时才序列化。
			serialize = false
		}

		pluginOpts := []any{
			app.Options{EnableHealthService: true},
			kubeletplugin.GRPCVerbosity(0),
			kubeletplugin.GRPCInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
				return d.interceptor(nodename, ctx, req, info, handler)
			}),
			kubeletplugin.GRPCStreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
				return d.streamInterceptor(nodename, srv, ss, info, handler)
			}),
			kubeletplugin.NodeV1(d.NodeV1),
			kubeletplugin.NodeV1beta1(d.NodeV1beta1),

			kubeletplugin.RollingUpdate(rollingUpdateUID),
			kubeletplugin.Serialize(serialize),
			kubeletplugin.FlockDirectoryPath(nodes.tempDir),

			kubeletplugin.PluginDataDirectoryPath(pluginDataDirectoryPath),
			kubeletplugin.PluginListener(d.listen(tCtx, &pod, &listenerPort)),

			kubeletplugin.RegistrarDirectoryPath(registrarDirectoryPath),
			kubeletplugin.RegistrarListener(d.listen(tCtx, &pod, &listenerPort)),

			kubeletplugin.EnableDeviceMetadata(d.EnableDeviceMetadata, d.DeviceMetadataVersions),
		}
		if d.ReconcilePoolWithName != "" {
			pluginOpts = append(pluginOpts, kubeletplugin.ReconcilePoolWithName(d.ReconcilePoolWithName))
		}
		if d.EnableDeviceMetadata {
			if !d.IsLocal {
				pluginOpts = append(pluginOpts,
					kubeletplugin.MetadataFileOps(d.buildRemoteMetadataFileOps(tCtx, &pod)),
					kubeletplugin.CDIDirectory("/cdi"),
				)
			}
		}

		plugin, err := app.StartPlugin(loggerCtx, "/cdi", d.Name, driverClient, nodename, fileOps, pluginOpts...)
		tCtx.ExpectNoError(err, "start kubelet plugin for node %s", pod.Spec.NodeName)
		d.cleanup = append(d.cleanup, func(tCtx ktesting.TContext) {
			// 依赖先调用 cancel。
			plugin.Stop()
		})
		d.Nodes[nodename] = KubeletPlugin{ExamplePlugin: plugin, ClientSet: driverClient}
	}

	// 等所有按节点运行的插件都停止后，再缩容 proxy ReplicaSet。
	d.cleanup = append(d.cleanup, func(tCtx ktesting.TContext) {
		tCtx.Log("scaling down driver proxy pods for", d.Name)
		rs, err := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Get(tCtx, rsName, metav1.GetOptions{})
		tCtx.ExpectNoError(err, "get ReplicaSet for driver "+d.Name)
		rs.Spec.Replicas = ptr.To(int32(0))
		rs, err = tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Update(tCtx, rs, metav1.UpdateOptions{})
		tCtx.ExpectNoError(err, "scale down ReplicaSet for driver "+d.Name)
		if err := e2ereplicaset.WaitForReplicaSetTargetAvailableReplicas(tCtx, tCtx.Client(), rs, 0); err != nil {
			tCtx.ExpectNoError(err, "all kubelet plugin proxies stopped")
		}
	})

	if !d.WithKubelet {
		return
	}

	// 等待插件注册完成。
	tCtx.Log("wait for plugin registration")
	tCtx.Eventually(func(tCtx ktesting.TContext) map[string][]app.GRPCCall {
		notRegistered := make(map[string][]app.GRPCCall)
		for nodename, plugin := range d.Nodes {
			calls := plugin.GetGRPCCalls()
			if contains, err := testdrivergomega.BeRegistered.Match(calls); err != nil || !contains {
				notRegistered[nodename] = calls
			}
		}
		return notRegistered
	}).WithTimeout(time.Minute).Should(gomega.BeEmpty(), "hosts where the plugin has not been registered yet")
}

func (d *Driver) ImpersonateKubeletPlugin(tCtx ktesting.TContext, pod *v1.Pod) kubernetes.Interface {
	tCtx.Helper()
	driverUserInfo := (&serviceaccount.ServiceAccountInfo{
		Name:      d.serviceAccountName,
		Namespace: pod.Namespace,
		NodeName:  pod.Spec.NodeName,
		PodName:   pod.Name,
		PodUID:    string(pod.UID),
	}).UserInfo()
	driverClientConfig := tCtx.RESTConfig()
	driverClientConfig.Impersonate = rest.ImpersonationConfig{
		UserName: driverUserInfo.GetName(),
		Groups:   driverUserInfo.GetGroups(),
		Extra:    driverUserInfo.GetExtra(),
	}
	driverClient, err := kubernetes.NewForConfig(driverClientConfig)
	tCtx.ExpectNoError(err, "create client for driver")
	return driverClient
}

func (d *Driver) createFile(tCtx ktesting.TContext, pod *v1.Pod, name string, content []byte) error {
	buffer := bytes.NewBuffer(content)
	// 写入内容可能较慢，先写到临时文件，完成后再移到目标位置。
	tmpName := name + ".tmp"
	if err := d.podIO(tCtx, pod).CreateFile(tmpName, buffer); err != nil {
		_ = d.podIO(tCtx, pod).RemoveAll(tmpName)
		return err
	}
	return d.podIO(tCtx, pod).Rename(tmpName, name)
}

func (d *Driver) removeFile(tCtx ktesting.TContext, pod *v1.Pod, name string) error {
	return d.podIO(tCtx, pod).RemoveAll(name)
}

func (d *Driver) createFromYAML(tCtx ktesting.TContext, content []byte, namespace string) {
	// 不缓存 discovery 结果效率不高，但已经够用。
	discoveryCache := memory.NewMemCacheClientWithContext(tCtx.Client().Discovery())
	restMapper := restmapper.NewDeferredDiscoveryRESTMapperWithContext(discoveryCache)

	for _, content := range bytes.Split(content, []byte("---\n")) {
		if len(content) == 0 {
			continue
		}

		var obj *unstructured.Unstructured
		tCtx.ExpectNoError(yaml.UnmarshalStrict(content, &obj), fmt.Sprintf("Full YAML:\n%s\n", string(content)))

		gv, err := schema.ParseGroupVersion(obj.GetAPIVersion())
		tCtx.ExpectNoError(err, fmt.Sprintf("extract group+version from object %q", klog.KObj(obj)))
		gk := schema.GroupKind{Group: gv.Group, Kind: obj.GetKind()}

		mapping, err := restMapper.RESTMappingWithContext(tCtx, gk, gv.Version)
		tCtx.ExpectNoError(err, fmt.Sprintf("map %q to resource", gk))

		resourceClient := tCtx.Dynamic().Resource(mapping.Resource)
		options := metav1.CreateOptions{
			// YAML 输入无效时，希望 apiserver 直接报错。
			// 因为解码成 unstructured 对象时不会做校验。
			FieldValidation: "Strict",
		}
		switch mapping.Scope.Name() {
		case meta.RESTScopeNameRoot:
			_, err = resourceClient.Create(tCtx, obj, options)
		case meta.RESTScopeNameNamespace:
			if namespace == "" {
				tCtx.Fatalf("need namespace for object type %s", gk)
			}
			_, err = resourceClient.Namespace(namespace).Create(tCtx, obj, options)
		}
		tCtx.ExpectNoError(err, "create object")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			del := resourceClient.Delete
			if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
				del = resourceClient.Namespace(namespace).Delete
			}
			err := del(tCtx, obj.GetName(), metav1.DeleteOptions{})
			if !apierrors.IsNotFound(err) {
				tCtx.ExpectNoError(err, fmt.Sprintf("deleting %s.%s %s", obj.GetKind(), obj.GetAPIVersion(), klog.KObj(obj)))
			}
		})
	}
}

func (d *Driver) podIO(tCtx ktesting.TContext, pod *v1.Pod) proxy.PodDirIO {
	logger := tCtx.Logger()
	return proxy.PodDirIO{
		TCtx:          tCtx,
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		ContainerName: pod.Spec.Containers[0].Name,
		Logger:        &logger,
	}
}

func (d *Driver) buildRemoteMetadataFileOps(tCtx ktesting.TContext, pod *v1.Pod) kubeletplugin.MetadataFileOperations {
	execInPod := func(command []string) (string, error) {
		stdout, stderr, err := e2epod.Exec(tCtx, e2epod.ExecOptions{
			Command:       command,
			Namespace:     pod.Namespace,
			PodName:       pod.Name,
			ContainerName: pod.Spec.Containers[0].Name,
			CaptureStdout: true,
			CaptureStderr: true,
			Quiet:         true,
		})
		if err != nil {
			return "", fmt.Errorf("%v: stderr=%q, %w", command, stderr, err)
		}
		return stdout, nil
	}

	return kubeletplugin.MetadataFileOperations{
		WriteFile: func(name string, data []byte, perm os.FileMode) error {
			return d.createFile(tCtx, pod, name, data)
		},
		ReadFile: func(name string) ([]byte, error) {
			stdout, err := execInPod([]string{"cat", name})
			if err != nil {
				return nil, err
			}
			return []byte(stdout), nil
		},
		MkdirAll: func(p string, perm os.FileMode) error {
			_, err := execInPod([]string{"mkdir", "-p", p})
			return err
		},
		RemoveAll: func(p string) error {
			return d.podIO(tCtx, pod).RemoveAll(p)
		},
		Remove: func(name string) error {
			_, err := execInPod([]string{"rm", "-f", name})
			return err
		},
		Glob: func(pattern string) ([]string, error) {
			stdout, err := execInPod([]string{"sh", "-c", fmt.Sprintf("ls -1d %s 2>/dev/null || true", pattern)})
			if err != nil {
				return nil, err
			}
			stdout = strings.TrimSpace(stdout)
			if stdout == "" {
				return nil, nil
			}
			return strings.Split(stdout, "\n"), nil
		},
	}
}

// errListenerDone 是用于关闭 listener 的特殊错误，不需要记录到日志。
var errListenerDone = errors.New("listener is shutting down")

// listen 返回 kubeletplugin helper 打开监听 socket 所需的函数。
// 它会在目标节点的 pod 中启动 hostpathplugin，并通过端口转发与之连接。
func (d *Driver) listen(tCtx ktesting.TContext, pod *v1.Pod, port *atomic.Int32) func(ctx context.Context, endpoint string) (net.Listener, error) {
	return func(ctx context.Context, endpoint string) (l net.Listener, e error) {
		// 无需创建 socket，因为预期 kubelet 不会使用它们。
		if !d.WithKubelet {
			return newNullListener(), nil
		}

		// 先尝试直接在本地主机上打开 socket，失败时回退到 pod。
		// 关闭 listener 会同时删除 socket 文件。
		if d.IsLocal {
			dir := path.Dir(endpoint)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, err
			}
			return net.ListenUnix("unix", &net.UnixAddr{Name: endpoint, Net: "unix"})
		}

		// 将每个 pod 的计数器加一，以此"分配"一个新端口。
		port := port.Add(1)

		logger := klog.FromContext(ctx)
		logger = klog.LoggerWithName(logger, "socket-listener")
		logger = klog.LoggerWithValues(logger, "endpoint", endpoint, "port", port)
		ctx = klog.NewContext(ctx, logger)

		// 以 proxy 模式启动 hostpathplugin，并保持运行直到 listener 被关闭。
		req := tCtx.Client().CoreV1().RESTClient().Post().
			Resource("pods").
			Namespace(tCtx.Namespace()).
			Name(pod.Name).
			SubResource("exec").
			VersionedParams(&v1.PodExecOptions{
				Container: pod.Spec.Containers[0].Name,
				Command: []string{
					"/hostpathplugin",
					"--v=5",
					"--endpoint=" + endpoint,
					fmt.Sprintf("--proxy-endpoint=tcp://:%d", port),
				},
				Stdout: true,
				Stderr: true,
			}, scheme.ParameterCodec)
		var wg sync.WaitGroup
		wg.Add(1)
		cmdCtx, cmdCancel := context.WithCancelCause(ctx)
		go func() {
			defer wg.Done()
			cmdLogger := klog.LoggerWithName(logger, "hostpathplugin")
			cmdCtx := klog.NewContext(cmdCtx, cmdLogger)
			logger.V(1).Info("Starting...")
			defer logger.V(1).Info("Stopped")

			// 这可能暂时失败，重试执行即可恢复。
			delayFn := wait.Backoff{
				Duration: time.Second,
				Cap:      30 * time.Second,
				Steps:    30,
				Factor:   2.0,
				Jitter:   1.0,
			}.DelayWithReset(clock.RealClock{}, 5*time.Minute)
			runHostpathPlugin := func(ctx context.Context) (bool, error) {
				// 用 errors.Is(err, listenerDoneErr) 会更好，但 remotecommand 不会返回
				// 这个错误。因此改为在已经关闭时不再记录日志。
				if err := execute(ctx, req.URL(), tCtx.RESTConfig(), 5); err != nil && ctx.Err() == nil {
					klog.FromContext(ctx).V(5).Info("execution failed, will retry", "err", err)
				}
				// 除 context 取消外没有停止的理由，因此 condition 恒为 false，也不会有致命错误。
				return false, nil
			}
			_ = delayFn.Until(cmdCtx, true /* immediate */, true /* sliding */, runHostpathPlugin)

			// 杀掉 hostpathplugin 不会删除 socket，需要手动删除。
			req := tCtx.Client().CoreV1().RESTClient().Post().
				Resource("pods").
				Namespace(tCtx.Namespace()).
				Name(pod.Name).
				SubResource("exec").
				VersionedParams(&v1.PodExecOptions{
					Container: pod.Spec.Containers[0].Name,
					Command: []string{
						"rm",
						"-f",
						endpoint,
					},
					Stdout: true,
					Stderr: true,
				}, scheme.ParameterCodec)
			cleanupLogger := klog.LoggerWithName(logger, "cleanup")
			cleanupCtx := klog.NewContext(ctx, cleanupLogger)
			if err := execute(cleanupCtx, req.URL(), tCtx.RESTConfig(), 0); err != nil {
				cleanupLogger.Error(err, "Socket removal failed")
			}
		}()
		defer func() {
			// 如果没有返回可用的 listener，则进行清理。
			if e != nil {
				cmdCancel(e)
			}
		}()
		stopHostpathplugin := func() {
			cmdCancel(errListenerDone)
			wg.Wait()
		}

		addr := proxy.Addr{
			Namespace:     tCtx.Namespace(),
			PodName:       pod.Name,
			ContainerName: pod.Spec.Containers[0].Name,
			Port:          int(port),
		}
		listener, err := proxy.Listen(ctx, tCtx.Client(), tCtx.RESTConfig(), addr)
		if err != nil {
			return nil, fmt.Errorf("listen for connections from %+v: %w", addr, err)
		}
		return &listenerWithClose{Listener: listener, close: stopHostpathplugin}, nil
	}
}

// listenerWithClose 包装 Close，使其同时关闭 hostpathplugin。
type listenerWithClose struct {
	net.Listener
	close func()
}

func (l *listenerWithClose) Close() error {
	// 先关闭连接，再停止远端命令，否则连接处理逻辑会报错并输出错误日志。
	err := l.Listener.Close()
	l.close()
	return err
}

func newNullListener() net.Listener {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &nullListener{ctx: ctx, cancel: cancel}
}

// nullListener 会阻塞所有 Accept 调用，直到 listener 被关闭。
type nullListener struct {
	ctx    context.Context
	cancel func(err error)
}

func (l *nullListener) Accept() (net.Conn, error) {
	<-l.ctx.Done()
	return nil, context.Cause(l.ctx)
}

func (l *nullListener) Close() error {
	l.cancel(errors.New("listener was closed"))
	return nil
}

func (l *nullListener) Addr() net.Addr {
	return &net.UnixAddr{}
}

// execute 运行远端命令，并把 stdout/stderr 按指定详细级别重定向为日志消息。
func execute(ctx context.Context, url *url.URL, config *rest.Config, verbosity int) error {
	// 只要命令还在运行就持续输出流内容，即忽略取消信号。
	stdout := pipe(context.WithoutCancel(ctx), "STDOUT", verbosity)
	stderr := pipe(context.WithoutCancel(ctx), "STDERR", verbosity)
	defer func() { _ = stdout.Close() }()
	defer func() { _ = stderr.Close() }()

	executor := exec.DefaultRemoteExecutor{}
	return executor.ExecuteWithContext(ctx, url, config, nil, stdout, stderr, false, nil)
}

// pipe 创建一个内存管道，并在后台把通过管道传来的内容写入日志。
func pipe(ctx context.Context, msg string, verbosity int) *io.PipeWriter {
	logger := klog.FromContext(ctx)

	reader, writer := io.Pipe()
	go func() {
		buffer := make([]byte, 10*1024)
		for {
			n, err := reader.Read(buffer)
			if n > 0 {
				logger.V(verbosity).Info(msg, "msg", string(buffer[0:n]))
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					logger.Error(err, msg)
				}
				reader.CloseWithError(err)
				return
			}
			if ctx.Err() != nil {
				reader.CloseWithError(context.Cause(ctx))
				return
			}
		}
	}()
	return writer
}

func (d *Driver) TearDown(tCtx ktesting.TContext) {
	for _, c := range d.cleanup {
		c(tCtx)
	}
	d.cleanup = nil
	d.wg.Wait()
}

// IsGone 检查 kubelet 是否已用完该驱动。
// 做法等待 kubelet 删除驱动的 ResourceSlice；由于 kubelet 有延迟，
// 这至少需要 30 秒。
//
// 只在确定 kubelet 支持 DRA 的测试中使用本函数。
func (d *Driver) IsGone(tCtx ktesting.TContext) {
	tCtx.Helper()
	tCtx.Logf("Waiting for ResourceSlices of driver %s to be removed...", d.Name)
	tCtx.Eventually(d.NewGetSlices()).WithTimeout(2 * time.Minute).Should(gomega.HaveField("Items", gomega.BeEmpty()))
}

func (d *Driver) interceptor(nodename string, ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	m := MethodInstance{nodename, info.FullMethod}
	d.callCounts[m]++
	if d.fail[m] {
		return nil, errors.New("injected error")
	}

	return handler(ctx, req)
}

func (d *Driver) streamInterceptor(nodename string, srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// stream 调用会长时间阻塞，运行期间不能持有锁。
	d.mutex.Lock()
	m := MethodInstance{nodename, info.FullMethod}
	d.callCounts[m]++
	fail := d.fail[m]
	d.mutex.Unlock()

	if fail {
		return errors.New("injected error")
	}

	return handler(srv, stream)
}

func (d *Driver) Fail(m MethodInstance, injectError bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.fail[m] = injectError
}

func (d *Driver) CallCount(m MethodInstance) int64 {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return d.callCounts[m]
}

func (d *Driver) Nodenames() (nodenames []string) {
	for nodename := range d.Nodes {
		nodenames = append(nodenames, nodename)
	}
	sort.Strings(nodenames)
	return
}
