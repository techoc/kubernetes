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

package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapiv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingapiv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	cliflag "k8s.io/component-base/cli/flag"
	pvutil "k8s.io/component-helpers/storage/volume"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/klog/v2"
	kubeschedulerconfigv1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/disruption"
	"k8s.io/kubernetes/pkg/controller/garbagecollector"
	"k8s.io/kubernetes/pkg/controller/namespace"
	"k8s.io/kubernetes/pkg/controller/resourceclaim"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	taintutils "k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/ptr"
)

// ShutdownFunc 表示要调用的函数句柄，通常在 defer 中调用，用于关闭正在运行的模块
type ShutdownFunc func()

// StartScheduler 是 StartSchedulerWithDone 的包装器，用于向后兼容。
func StartScheduler(tCtx ktesting.TContext, cfg *kubeschedulerconfig.KubeSchedulerConfiguration, outOfTreePluginRegistry frameworkruntime.Registry) (*scheduler.Scheduler, informers.SharedInformerFactory) {
	sched, informerFactory, _ := StartSchedulerWithDone(tCtx, cfg, outOfTreePluginRegistry)
	return sched, informerFactory
}

// StartSchedulerWithDone 配置并启动 scheduler。后台 goroutine
// 会一直运行，直到 context 被取消。它返回正在运行的 scheduler、
// informer factory，以及一个在 scheduler goroutine 实际退出时关闭的 channel。
// 调用方可以使用该 channel 确保 scheduler 已完全停止，
// 再执行可能与其竞争的操作（例如重置全局 metrics）。
func StartSchedulerWithDone(tCtx ktesting.TContext, cfg *kubeschedulerconfig.KubeSchedulerConfiguration, outOfTreePluginRegistry frameworkruntime.Registry) (*scheduler.Scheduler, informers.SharedInformerFactory, <-chan struct{}) {
	clientSet := tCtx.Client()
	informerFactory := scheduler.NewInformerFactory(clientSet, 0, nil)
	evtBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{
		Interface: clientSet.EventsV1()})
	go func() {
		<-tCtx.Done()
		evtBroadcaster.Shutdown()
	}()

	evtBroadcaster.StartRecordingToSink(tCtx.Done())

	logger := tCtx.Logger()

	sched, err := scheduler.New(
		tCtx,
		clientSet,
		informerFactory,
		nil,
		profile.NewRecorderFactory(evtBroadcaster),
		scheduler.WithKubeConfig(tCtx.RESTConfig()),
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithPercentageOfNodesToScore(cfg.PercentageOfNodesToScore),
		scheduler.WithPodMaxBackoffSeconds(cfg.PodMaxBackoffSeconds),
		scheduler.WithPodInitialBackoffSeconds(cfg.PodInitialBackoffSeconds),
		scheduler.WithExtenders(cfg.Extenders...),
		scheduler.WithParallelism(cfg.Parallelism),
		scheduler.WithFrameworkOutOfTreeRegistry(outOfTreePluginRegistry),
	)
	tCtx.ExpectNoError(err, "creating scheduler")

	informerFactory.Start(tCtx.Done())
	informerFactory.WaitForCacheSync(tCtx.Done())
	err = sched.WaitForHandlersSync(tCtx)
	tCtx.ExpectNoError(err, "waiting for handlers to sync")
	logger.V(3).Info("Handlers synced")
	done := make(chan struct{})
	go func() {
		sched.Run(tCtx)
		close(done)
	}()

	return sched, informerFactory, done
}

// CreateResourceClaimController 创建一个 ResourceClaim controller，并返回一个阻塞式的 run 函数。
// 调用方负责管理调用该方法的 goroutine。
func CreateResourceClaimController(ctx context.Context, tb ktesting.TB, clientSet clientset.Interface, informerFactory informers.SharedInformerFactory) func() {
	podInformer := informerFactory.Core().V1().Pods()
	podGroupInformer := informerFactory.Scheduling().V1beta1().PodGroups()
	claimInformer := informerFactory.Resource().V1().ResourceClaims()
	claimTemplateInformer := informerFactory.Resource().V1().ResourceClaimTemplates()
	claimController, err := resourceclaim.NewController(klog.FromContext(ctx), clientSet, podInformer, podGroupInformer, claimInformer, claimTemplateInformer)
	if err != nil {
		tb.Fatalf("Error creating claim controller: %v", err)
	}
	return func() {
		claimController.Run(ctx, 5 /* workers */)
	}
}

// StartFakePVController 是简化版的 pv controller 逻辑，为每个 PV 绑定设置 PVC 的 VolumeName 和 annotation。
// TODO(mborsz): 这里改用真正的 PV controller。
func StartFakePVController(ctx context.Context, clientSet clientset.Interface, informerFactory informers.SharedInformerFactory) {
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	syncPV := func(obj *v1.PersistentVolume) {
		if obj.Spec.ClaimRef != nil {
			claimRef := obj.Spec.ClaimRef
			pvc, err := clientSet.CoreV1().PersistentVolumeClaims(claimRef.Namespace).Get(ctx, claimRef.Name, metav1.GetOptions{})
			if err != nil {
				// 注意，error 可能是任意值，因为 apiserver 等组件
				// 此时也在关闭，但这里的判断比较保守，
				// 只忽略关闭过程中的 "context canceled" error。
				if ctx.Err() == nil || !errors.Is(err, context.Canceled) {
					klog.Errorf("error while getting %v/%v: %v", claimRef.Namespace, claimRef.Name, err)
				}
				return
			}

			if pvc.Spec.VolumeName == "" {
				pvc.Spec.VolumeName = obj.Name
				metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, pvutil.AnnBindCompleted, "yes")
				_, err := clientSet.CoreV1().PersistentVolumeClaims(claimRef.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
				if err != nil {
					if ctx.Err() == nil || !errors.Is(err, context.Canceled) {
						// 正在关闭，无需记录。
						klog.Errorf("error while updating %v/%v: %v", claimRef.Namespace, claimRef.Name, err)
					}
					return
				}
			}
		}
	}

	pvInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			syncPV(obj.(*v1.PersistentVolume))
		},
		UpdateFunc: func(_, obj interface{}) {
			syncPV(obj.(*v1.PersistentVolume))
		},
	})
}

// CreateGCController 创建一个 garbage controller 并返回对应的 run 函数。
// 调用该函数前需要先启动 informer factory。
func CreateGCController(ctx context.Context, tb ktesting.TB, restConfig restclient.Config, informerSet informers.SharedInformerFactory) func() {
	restclient.AddUserAgent(&restConfig, "gc-controller")
	clientSet := clientset.NewForConfigOrDie(&restConfig)
	metadataClient, err := metadata.NewForConfig(&restConfig)
	if err != nil {
		tb.Fatalf("Failed to create metadataClient: %v", err)
	}
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(cacheddiscovery.NewMemCacheClient(clientSet.Discovery()))
	restMapper.Reset()
	metadataInformers := metadatainformer.NewSharedInformerFactory(metadataClient, 0)
	alwaysStarted := make(chan struct{})
	close(alwaysStarted)
	gc, err := garbagecollector.NewGarbageCollector(
		ctx,
		clientSet,
		metadataClient,
		restMapper,
		garbagecollector.DefaultIgnoredResources(),
		informerfactory.NewInformerFactory(informerSet, metadataInformers),
		alwaysStarted,
	)
	if err != nil {
		tb.Fatalf("Failed creating garbage collector")
	}
	startGC := func() {
		syncPeriod := 5 * time.Second
		go wait.Until(func() {
			restMapper.Reset()
		}, syncPeriod, ctx.Done())
		go gc.Run(ctx, 1, syncPeriod)
		go gc.Sync(ctx, clientSet.Discovery(), syncPeriod)
	}
	return startGC
}

// CreateNamespaceController 创建一个 namespace controller 并返回对应的 run 函数。
// 调用该函数前需要先启动 informer factory。
func CreateNamespaceController(ctx context.Context, tb ktesting.TB, restConfig restclient.Config, informerSet informers.SharedInformerFactory) func() {
	restclient.AddUserAgent(&restConfig, "namespace-controller")
	clientSet := clientset.NewForConfigOrDie(&restConfig)
	metadataClient, err := metadata.NewForConfig(&restConfig)
	if err != nil {
		tb.Fatalf("Failed to create metadataClient: %v", err)
	}
	discoverResourcesFn := clientSet.Discovery().ServerPreferredNamespacedResources
	controller := namespace.NewNamespaceController(
		ctx,
		clientSet,
		metadataClient,
		discoverResourcesFn,
		informerSet.Core().V1().Namespaces(),
		10*time.Hour,
		v1.FinalizerKubernetes)
	return func() {
		go controller.Run(ctx, 5)
	}
}

// TestContext 存储必要的 context 信息。
// 它还包含一些供 InitTestScheduler 使用的可选参数。
type TestContext struct {
	// DisableEventSink 如果在调用 InitTestScheduler 前设置为 true，
	// 将跳过 eventBroadcaster.StartRecordingToSink，
	// 从而避免测试结束后那些难以清理的
	// 额外 goroutine。
	DisableEventSink bool

	NS                 *v1.Namespace
	ClientSet          clientset.Interface
	KubeConfig         *restclient.Config
	InformerFactory    informers.SharedInformerFactory
	DynInformerFactory dynamicinformer.DynamicSharedInformerFactory
	Scheduler          *scheduler.Scheduler
	// 这是初始化测试环境时的顶层 context。
	Ctx context.Context
	// CloseFn 会停止 apiserver 并清理其自身资源，
	// 包括关闭其存储层。
	CloseFn framework.TearDownFunc
	// 这是初始化 scheduler 时的 context。
	SchedulerCtx context.Context
	// SchedulerCloseFn 会清理创建 scheduler 时产生的资源，
	// 包括 scheduler 本身。
	SchedulerCloseFn framework.TearDownFunc

	// RoundTrip 如果设置，将在每个发往 apiserver 的 HTTP 请求上调用。
	// 它可用于错误注入。
	RoundTrip atomic.Pointer[RoundTripWrapper]
}

type RoundTripWrapper func(http.RoundTripper, *http.Request) (*http.Response, error)

type roundTripWrapper struct {
	tc        *TestContext
	transport http.RoundTripper
}

func (r roundTripWrapper) RoundTrip(req *http.Request) (*http.Response, error) {
	wrapper := r.tc.RoundTrip.Load()
	if wrapper != nil {
		return (*wrapper)(r.transport, req)
	}
	return r.transport.RoundTrip(req)
}

var _ http.RoundTripper = roundTripWrapper{}

// CleanupNodes 清理集成测试期间创建的所有 Node
func CleanupNodes(cs clientset.Interface, t *testing.T) {
	err := cs.CoreV1().Nodes().DeleteCollection(context.TODO(), *metav1.NewDeleteOptions(0), metav1.ListOptions{})
	if err != nil {
		t.Errorf("error while deleting all nodes: %v", err)
	}
}

// PodDeleted 在给定 namespace 中找不到 Pod 时返回 true。
func PodDeleted(ctx context.Context, c clientset.Interface, podNamespace, podName string) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if pod.DeletionTimestamp != nil {
			return true, nil
		}
		return false, nil
	}
}

// PodsCleanedUp 在指定 namespace 中所有 Pod 都被删除时返回 true。
func PodsCleanedUp(ctx context.Context, c clientset.Interface, namespace string) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		list, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		return len(list.Items) == 0, nil
	}
}

// SyncSchedulerInformerFactory 启动 informer 并等待缓存同步完成
func SyncSchedulerInformerFactory(testCtx *TestContext) {
	testCtx.InformerFactory.Start(testCtx.SchedulerCtx.Done())
	if testCtx.DynInformerFactory != nil {
		testCtx.DynInformerFactory.Start(testCtx.SchedulerCtx.Done())
	}
	testCtx.InformerFactory.WaitForCacheSync(testCtx.SchedulerCtx.Done())
	if testCtx.DynInformerFactory != nil {
		testCtx.DynInformerFactory.WaitForCacheSync(testCtx.SchedulerCtx.Done())
	}
}

// CleanupTest 清理集成测试期间创建的相关资源
func CleanupTest(t *testing.T, testCtx *TestContext) {
	// 清理 Node 和 Namespace。
	if err := testCtx.ClientSet.CoreV1().Nodes().DeleteCollection(testCtx.Ctx, *metav1.NewDeleteOptions(0), metav1.ListOptions{}); err != nil {
		t.Errorf("error while cleaning up nodes, error: %v", err)
	}
	framework.DeleteNamespaceOrDie(testCtx.ClientSet, testCtx.NS, t)
	// 终止 scheduler 和 apiserver。
	testCtx.CloseFn()
}

func RemovePodFinalizersInNamespace(ctx context.Context, cs clientset.Interface, t *testing.T, ns string) {
	t.Helper()
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed obtaining list of pods: %v", err)
	}
	RemovePodFinalizers(ctx, cs, t, pods.Items...)
}

// RemovePodFinalizers 移除给定 Pod 的 finalizer
func RemovePodFinalizers(ctx context.Context, cs clientset.Interface, t *testing.T, pods ...v1.Pod) {
	t.Helper()
	for _, p := range pods {
		pod, err := cs.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("error while removing pod finalizers for %v: %v", klog.KObj(&p), err)
		} else if pod != nil && len(pod.Finalizers) > 0 {
			// 使用 Patch 而非 Update 移除 finalizer，
			// 以避免瞬态冲突。
			patchBytes, _ := json.Marshal(map[string]interface{}{
				"metadata": map[string]interface{}{
					"$deleteFromPrimitiveList/finalizers": pod.Finalizers,
				},
			})
			_, err = cs.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				t.Errorf("error while updating pod status for %v: %v", klog.KObj(&p), err)
			}
		}
	}
}

// CleanupPods 删除给定的 Pod，并等待它们真正被删除。
func CleanupPods(ctx context.Context, cs clientset.Interface, t *testing.T, pods []*v1.Pod) {
	for _, p := range pods {
		err := cs.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, *metav1.NewDeleteOptions(0))
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("error while deleting pod %v/%v: %v", p.Namespace, p.Name, err)
		}
	}
	for _, p := range pods {
		if err := wait.PollUntilContextTimeout(ctx, time.Duration(time.Microsecond.Seconds()), wait.ForeverTestTimeout, true,
			PodDeleted(ctx, cs, p.Namespace, p.Name)); err != nil {
			t.Errorf("error while waiting for pod  %v/%v to get deleted: %v", p.Namespace, p.Name, err)
		}
	}
}

// AddTaintToNode 给指定 Node 添加 taint
func AddTaintToNode(cs clientset.Interface, nodeName string, taint v1.Taint) error {
	node, err := cs.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	node.Spec.Taints = append(node.Spec.Taints, taint)
	_, err = cs.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
	return err
}

// RemoveTaintOffNode 从 Node 上移除指定的 taint
func RemoveTaintOffNode(cs clientset.Interface, nodeName string, taint v1.Taint) error {
	node, err := cs.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	var taints []v1.Taint
	for _, t := range node.Spec.Taints {
		if !t.MatchTaint(&taint) {
			taints = append(taints, t)
		}
	}
	node.Spec.Taints = taints
	_, err = cs.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
	return err
}

// WaitForNodeTaints 等待 Node 具有目标 taint，
// 如果在给定超时时间内没有这些 taint，则返回 error。
func WaitForNodeTaints(ctx context.Context, cs clientset.Interface, node *v1.Node, taints []v1.Taint) error {
	return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, false, NodeTainted(ctx, cs, node.Name, taints))
}

// NodeTainted 返回一个条件函数，当给定 Node 包含
// 这些 taint 时返回 true。
func NodeTainted(ctx context.Context, cs clientset.Interface, nodeName string, taints []v1.Taint) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		// node.Spec.Taints 可能包含更多 taint
		if len(taints) > len(node.Spec.Taints) {
			return false, nil
		}

		for _, taint := range taints {
			if !taintutils.TaintExists(node.Spec.Taints, &taint) {
				return false, nil
			}
		}

		return true, nil
	}
}

// NodeReadyStatus 返回第一个类型为 NodeReady 的 condition 的 status。
// 如果没有类型为 NodeReady 的 condition，则返回 error。
func NodeReadyStatus(conditions []v1.NodeCondition) (v1.ConditionStatus, error) {
	for _, c := range conditions {
		if c.Type != v1.NodeReady {
			continue
		}
		// 直接返回第一个类型为 NodeReady 的 condition
		return c.Status, nil
	}
	return v1.ConditionFalse, errors.New("None of the conditions is of type NodeReady")
}

// GetTolerationSeconds 获取 toleration 的时间段
func GetTolerationSeconds(tolerations []v1.Toleration) (int64, error) {
	for _, t := range tolerations {
		if t.Key == v1.TaintNodeNotReady && t.Effect == v1.TaintEffectNoExecute && t.Operator == v1.TolerationOpExists {
			return *t.TolerationSeconds, nil
		}
	}
	return 0, fmt.Errorf("cannot find toleration")
}

// NodeCopyWithConditions 复制 Node 对象并设置 conditions
func NodeCopyWithConditions(node *v1.Node, conditions []v1.NodeCondition) *v1.Node {
	copy := node.DeepCopy()
	copy.ResourceVersion = "0"
	copy.Status.Conditions = conditions
	for i := range copy.Status.Conditions {
		copy.Status.Conditions[i].LastHeartbeatTime = metav1.Now()
	}
	return copy
}

// UpdateNodeStatus 更新 Node 的 status。
func UpdateNodeStatus(cs clientset.Interface, node *v1.Node) error {
	_, err := cs.CoreV1().Nodes().UpdateStatus(context.TODO(), node, metav1.UpdateOptions{})
	return err
}

// InitTestAPIServer 初始化测试环境，并使用默认配置
// 创建一个 API server。
// 它会将清理函数注册到 t.Cleanup()，测试结束时会被调用，
// 无需重复执行此操作。
func InitTestAPIServer(t *testing.T, nsPrefix string, admission admission.Interface) *TestContext {
	tCtx := ktesting.Init(t)
	testCtx := &TestContext{Ctx: tCtx}

	testCtx.ClientSet, testCtx.KubeConfig, testCtx.CloseFn = framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(options *options.ServerRunOptions) {
			options.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount", "TaintNodesByCondition", "Priority", "StorageObjectInUseProtection"}
			if options.APIEnablement.RuntimeConfig == nil {
				options.APIEnablement.RuntimeConfig = cliflag.ConfigurationMap{}
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
				options.APIEnablement.RuntimeConfig[resourceapi.SchemeGroupVersion.String()] = "true"
				if utilfeature.DefaultMutableFeatureGate.EmulationVersion().LessThan(version.MustParse("v1.34.0")) {
					// 在 emulating < 1.34 时无法启用 resourceapi.SchemeGroupVersion，
					// 除非启用 --runtime-config-emulation-forward-compatible。
					options.GenericServerRunOptions.RuntimeConfigEmulationForwardCompatible = true
				}
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
				options.APIEnablement.RuntimeConfig[schedulingapiv1beta1.SchemeGroupVersion.String()] = "true"
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
				options.APIEnablement.RuntimeConfig[schedulingapiv1alpha3.SchemeGroupVersion.String()] = "true"
			}
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			if admission != nil {
				config.ControlPlane.Generic.AdmissionControl = admission
			}
		},
	})

	// 支持包装 HTTP 请求。
	testCtx.KubeConfig.Wrap(func(transport http.RoundTripper) http.RoundTripper {
		return roundTripWrapper{tc: testCtx, transport: transport}
	})
	var err error
	testCtx.ClientSet, err = clientset.NewForConfig(testCtx.KubeConfig)
	if err != nil {
		t.Fatal(err)
	}

	oldCloseFn := testCtx.CloseFn
	testCtx.CloseFn = func() {
		tCtx.Cancel("tearing down apiserver")
		oldCloseFn()
	}

	if nsPrefix != "default" {
		testCtx.NS = framework.CreateNamespaceOrDie(testCtx.ClientSet, nsPrefix+string(uuid.NewUUID()), t)
	} else {
		testCtx.NS = framework.CreateNamespaceOrDie(testCtx.ClientSet, "default", t)
	}

	t.Cleanup(func() {
		CleanupTest(t, testCtx)
	})

	return testCtx
}

// WithNewNamespace 创建一个子 TestContext，它与父 TestContext 共享 API server，
// 但获得一个新的 namespace。t.Cleanup 时只删除该 namespace；
// API server 的生命周期由调用方管理。当多个子测试共享一个 API server 时，
// 这可以避免每个子测试的启动开销。
func WithNewNamespace(t *testing.T, parent *TestContext, nsPrefix string) *TestContext {
	t.Helper()
	child := &TestContext{
		ClientSet:  parent.ClientSet,
		KubeConfig: parent.KubeConfig,
		Ctx:        parent.Ctx,
		CloseFn:    func() {}, // API server 由父级拥有，不要在此处销毁它。
	}
	child.NS = framework.CreateNamespaceOrDie(child.ClientSet, nsPrefix+string(uuid.NewUUID()), t)
	t.Cleanup(func() {
		framework.DeleteNamespaceOrDie(child.ClientSet, child.NS, t)
	})
	return child
}

// WaitForSchedulerCacheCleanup 等待 scheduler 缓存清理完成
func WaitForSchedulerCacheCleanup(ctx context.Context, sched *scheduler.Scheduler, t *testing.T) {
	schedulerCacheIsEmpty := func(context.Context) (bool, error) {
		dump := sched.Cache.Dump()

		return len(dump.Nodes) == 0 && len(dump.AssumedPods) == 0, nil
	}

	if err := wait.PollUntilContextTimeout(ctx, time.Second, wait.ForeverTestTimeout, false, schedulerCacheIsEmpty); err != nil {
		t.Errorf("Failed to wait for scheduler cache cleanup: %v", err)
	}
}

// InitTestScheduler 初始化测试环境，并使用默认配置
// 创建 scheduler。
func InitTestScheduler(
	t *testing.T,
	testCtx *TestContext,
) *TestContext {
	// 默认 scheduler 配置会启用 Pod 抢占。
	return InitTestSchedulerWithOptions(t, testCtx, 0)
}

// InitTestSchedulerWithOptions 初始化测试环境，并使用默认配置
// 和其他选项创建 scheduler。
func InitTestSchedulerWithOptions(
	t *testing.T,
	testCtx *TestContext,
	resyncPeriod time.Duration,
	opts ...scheduler.Option,
) *TestContext {
	ctx, cancel := context.WithCancel(testCtx.Ctx)
	testCtx.SchedulerCtx = ctx

	// 1. 创建 scheduler
	testCtx.InformerFactory = scheduler.NewInformerFactory(testCtx.ClientSet, resyncPeriod, nil)
	if testCtx.KubeConfig != nil {
		dynClient := dynamic.NewForConfigOrDie(testCtx.KubeConfig)
		testCtx.DynInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 0, v1.NamespaceAll, nil)
	}

	var err error
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{
		Interface: testCtx.ClientSet.EventsV1(),
	})

	opts = append(opts, scheduler.WithKubeConfig(testCtx.KubeConfig))
	testCtx.Scheduler, err = scheduler.New(
		ctx,
		testCtx.ClientSet,
		testCtx.InformerFactory,
		testCtx.DynInformerFactory,
		profile.NewRecorderFactory(eventBroadcaster),
		opts...,
	)

	if err != nil {
		t.Fatalf("Couldn't create scheduler: %v", err)
	}

	if !testCtx.DisableEventSink {
		eventBroadcaster.StartRecordingToSink(ctx.Done())
	}

	oldCloseFn := testCtx.CloseFn
	testCtx.CloseFn = func() {
		oldCloseFn()
		eventBroadcaster.Shutdown()
	}

	testCtx.SchedulerCloseFn = func() {
		cancel()
		eventBroadcaster.Shutdown()
	}

	return testCtx
}

// WaitForPodToScheduleWithTimeout 等待 Pod 被调度，
// 如果在给定超时时间内未被调度，则返回 error。
func WaitForPodToScheduleWithTimeout(ctx context.Context, cs clientset.Interface, pod *v1.Pod, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, false, PodScheduled(cs, pod.Namespace, pod.Name))
}

// WaitForPodToSchedule 等待 Pod 被调度，如果在
// 超时时间（30 秒）内未被调度，则返回 error。
func WaitForPodToSchedule(ctx context.Context, cs clientset.Interface, pod *v1.Pod) error {
	return WaitForPodToScheduleWithTimeout(ctx, cs, pod, 30*time.Second)
}

// PodScheduled 检查 Pod 是否已被调度
func PodScheduled(c clientset.Interface, podNamespace, podName string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			// 可能是连接错误，所以重试。
			return false, nil
		}
		if pod.Spec.NodeName == "" {
			return false, nil
		}
		return true, nil
	}
}

// InitDisruptionController 初始化并运行 Disruption Controller，
// 以正确更新 PodDisruptionBudget 对象。
func InitDisruptionController(t *testing.T, testCtx *TestContext) *disruption.DisruptionController {
	informers := informers.NewSharedInformerFactory(testCtx.ClientSet, 12*time.Hour)

	discoveryClient := cacheddiscovery.NewMemCacheClient(testCtx.ClientSet.Discovery())
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)

	config := restclient.CopyConfig(testCtx.KubeConfig)
	scaleKindResolver := scale.NewDiscoveryScaleKindResolver(testCtx.ClientSet.Discovery())
	scaleClient, err := scale.NewForConfig(config, mapper, dynamic.LegacyAPIPathResolverFunc, scaleKindResolver)
	if err != nil {
		t.Fatalf("Error in create scaleClient: %v", err)
	}

	dc := disruption.NewDisruptionController(
		testCtx.Ctx,
		informers.Core().V1().Pods(),
		informers.Policy().V1().PodDisruptionBudgets(),
		informers.Core().V1().ReplicationControllers(),
		informers.Apps().V1().ReplicaSets(),
		informers.Apps().V1().Deployments(),
		informers.Apps().V1().StatefulSets(),
		testCtx.ClientSet,
		mapper,
		scaleClient,
		testCtx.ClientSet.Discovery())

	informers.Start(testCtx.Scheduler.StopEverything)
	informers.WaitForCacheSync(testCtx.Scheduler.StopEverything)
	go dc.Run(testCtx.Ctx, 1)
	return dc
}

// InitTestSchedulerWithNS 初始化测试环境，并使用默认配置
// 创建 API server 和 scheduler。
func InitTestSchedulerWithNS(t *testing.T, nsPrefix string, opts ...scheduler.Option) *TestContext {
	testCtx := InitTestSchedulerWithOptions(t, InitTestAPIServer(t, nsPrefix, nil), 0, opts...)
	SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.SchedulerCtx)
	return testCtx
}

// InitTestDisablePreemption 初始化测试环境，并使用默认配置创建
// API server 和 scheduler，但禁用 Pod 抢占。
func InitTestDisablePreemption(t *testing.T, nsPrefix string) *TestContext {
	cfg := configtesting.V1ToInternalWithDefaults(t, kubeschedulerconfigv1.KubeSchedulerConfiguration{
		Profiles: []kubeschedulerconfigv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &kubeschedulerconfigv1.Plugins{
				PostFilter: kubeschedulerconfigv1.PluginSet{
					Disabled: []kubeschedulerconfigv1.Plugin{
						{Name: defaultpreemption.Name},
					},
				},
			},
		}},
	})
	testCtx := InitTestSchedulerWithOptions(
		t, InitTestAPIServer(t, nsPrefix, nil),
		0,
		scheduler.WithProfiles(cfg.Profiles...))
	SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.SchedulerCtx)
	return testCtx
}

// WaitForReflection 等待 passFunc 确认其期望看到的
// 对象已在 store 中。用于观察反射事件。
func WaitForReflection(ctx context.Context, t *testing.T, nodeLister corelisters.NodeLister, key string,
	passFunc func(n interface{}) bool) error {
	var nodes []*v1.Node
	err := wait.PollUntilContextTimeout(ctx, time.Millisecond*100, wait.ForeverTestTimeout, false, func(context.Context) (bool, error) {
		n, err := nodeLister.Get(key)

		switch {
		case err == nil && passFunc(n):
			return true, nil
		case apierrors.IsNotFound(err):
			nodes = append(nodes, nil)
		case err != nil:
			t.Errorf("Unexpected error: %v", err)
		default:
			nodes = append(nodes, n)
		}

		return false, nil
	})
	if err != nil {
		t.Logf("Logging consecutive node versions received from store:")
		for i, n := range nodes {
			t.Logf("%d: %#v", i, n)
		}
	}
	return err
}

func UpdateNode(cs clientset.Interface, node *v1.Node) (*v1.Node, error) {
	return cs.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
}

func CreateNode(cs clientset.Interface, node *v1.Node) (*v1.Node, error) {
	return cs.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
}

func createNodes(cs clientset.Interface, prefix string, wrapper *st.NodeWrapper, numNodes int) ([]*v1.Node, error) {
	nodes := make([]*v1.Node, numNodes)
	for i := range numNodes {
		nodeName := fmt.Sprintf("%v-%d", prefix, i)
		node, err := CreateNode(cs, wrapper.Name(nodeName).Label("kubernetes.io/hostname", nodeName).Obj())
		if err != nil {
			return nodes[:], err
		}
		nodes[i] = node
	}
	return nodes[:], nil
}

// CreateAndWaitForNodesInCache 调用 createNodes()，并等待创建的
// Node 出现在 scheduler 缓存中。
func CreateAndWaitForNodesInCache(testCtx *TestContext, prefix string, wrapper *st.NodeWrapper, numNodes int) ([]*v1.Node, error) {
	existingNodes := testCtx.Scheduler.Cache.NodeCount()
	nodes, err := createNodes(testCtx.ClientSet, prefix, wrapper, numNodes)
	if err != nil {
		return nodes, fmt.Errorf("cannot create nodes: %v", err)
	}
	return nodes, WaitForNodesInCache(testCtx.Ctx, testCtx.Scheduler, numNodes+existingNodes)
}

// WaitForNodesInCache 确保 30 秒内 scheduler 缓存中至少存在
// <nodeCount> 个 Node；否则返回 false。
func WaitForNodesInCache(ctx context.Context, sched *scheduler.Scheduler, nodeCount int) error {
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false, func(context.Context) (bool, error) {
		return sched.Cache.NodeCount() >= nodeCount, nil
	})
	if err != nil {
		return fmt.Errorf("cannot obtain available nodes in scheduler cache: %v", err)
	}
	return nil
}

type PausePodConfig struct {
	Name                              string
	Namespace                         string
	PodGroupName                      string
	Affinity                          *v1.Affinity
	Annotations, Labels, NodeSelector map[string]string
	Resources                         *v1.ResourceRequirements
	Tolerations                       []v1.Toleration
	NodeName                          string
	SchedulerName                     string
	Priority                          *int32
	PreemptionPolicy                  *v1.PreemptionPolicy
	PriorityClassName                 string
	Volumes                           []v1.Volume
	ContainerPorts                    []v1.ContainerPort
	RestartableInitContainerPorts     []v1.ContainerPort
	NonRestartableInitContainerPorts  []v1.ContainerPort
}

// InitPausePod 根据给定配置初始化 Pod API 对象。它主要
// 用于 Pod 创建流程。
func InitPausePod(conf *PausePodConfig) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        conf.Name,
			Namespace:   conf.Namespace,
			Labels:      conf.Labels,
			Annotations: conf.Annotations,
		},
		Spec: v1.PodSpec{
			NodeSelector: conf.NodeSelector,
			Affinity:     conf.Affinity,
			Containers: []v1.Container{
				{
					Name:  conf.Name,
					Image: imageutils.GetPauseImageName(),
					Ports: conf.ContainerPorts,
				},
			},
			Tolerations:       conf.Tolerations,
			NodeName:          conf.NodeName,
			SchedulerName:     conf.SchedulerName,
			Priority:          conf.Priority,
			PreemptionPolicy:  conf.PreemptionPolicy,
			PriorityClassName: conf.PriorityClassName,
			Volumes:           conf.Volumes,
		},
	}
	if conf.Resources != nil {
		pod.Spec.Containers[0].Resources = *conf.Resources
	}
	if conf.RestartableInitContainerPorts != nil {
		var containerRestartPolicyAlways = v1.ContainerRestartPolicyAlways
		pod.Spec.InitContainers = []v1.Container{
			{
				Name:          conf.Name + "-init",
				Image:         imageutils.GetPauseImageName(),
				RestartPolicy: &containerRestartPolicyAlways,
				Ports:         conf.RestartableInitContainerPorts,
			},
		}
	}
	if conf.NonRestartableInitContainerPorts != nil {
		pod.Spec.InitContainers = []v1.Container{
			{
				Name:  conf.Name + "-init",
				Image: imageutils.GetPauseImageName(),
				Ports: conf.NonRestartableInitContainerPorts,
			},
		}
	}
	if conf.PodGroupName != "" {
		pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
			PodGroupName: &conf.PodGroupName,
		}
	}
	return pod
}

// CreatePausePod 使用 "Pause" 镜像和给定配置创建 Pod，
// 返回其指针和 error 状态。
func CreatePausePod(cs clientset.Interface, p *v1.Pod) (*v1.Pod, error) {
	return cs.CoreV1().Pods(p.Namespace).Create(context.TODO(), p, metav1.CreateOptions{})
}

// CreatePausePodWithResource 使用 "Pause" 镜像和给定资源创建 Pod，
// 返回其指针和 error 状态。资源列表可以为
// nil。
func CreatePausePodWithResource(cs clientset.Interface, podName string,
	nsName string, res *v1.ResourceList) (*v1.Pod, error) {
	var conf PausePodConfig
	if res == nil {
		conf = PausePodConfig{
			Name:      podName,
			Namespace: nsName,
		}
	} else {
		conf = PausePodConfig{
			Name:      podName,
			Namespace: nsName,
			Resources: &v1.ResourceRequirements{
				Requests: *res,
			},
		}
	}
	return CreatePausePod(cs, InitPausePod(&conf))
}

// CreatePVC 使用给定配置创建 PersistentVolumeClaim，并返回
// 其指针和 error 状态。
func CreatePVC(cs clientset.Interface, pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	return cs.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
}

// CreatePV 使用给定配置创建 PersistentVolume，并返回其
// 指针和 error 状态。
func CreatePV(cs clientset.Interface, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	return cs.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
}

// DeletePVC 删除给定 namespace 中的 PVC。
func DeletePVC(cs clientset.Interface, pvcName string, nsName string) error {
	return cs.CoreV1().PersistentVolumeClaims(nsName).Delete(context.TODO(), pvcName, *metav1.NewDeleteOptions(0))
}

// DeletePV 删除给定 namespace 中的 PV。
func DeletePV(cs clientset.Interface, pvName string) error {
	return cs.CoreV1().PersistentVolumes().Delete(context.TODO(), pvName, *metav1.NewDeleteOptions(0))
}

// RunPausePod 使用 "Pause" 镜像和给定配置创建 Pod，并等待
// 其被调度。它返回其指针和 error 状态。
func RunPausePod(cs clientset.Interface, pod *v1.Pod) (*v1.Pod, error) {
	pod, err := cs.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create pause pod: %v", err)
	}
	if err = WaitForPodToSchedule(context.TODO(), cs, pod); err != nil {
		return pod, fmt.Errorf("Pod %v/%v didn't schedule successfully. Error: %v", pod.Namespace, pod.Name, err)
	}
	if pod, err = cs.CoreV1().Pods(pod.Namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{}); err != nil {
		return pod, fmt.Errorf("failed to get pod %v/%v info: %v", pod.Namespace, pod.Name, err)
	}
	return pod, nil
}

type PodWithContainersConfig struct {
	Name       string
	Namespace  string
	Containers []v1.Container
}

// InitPodWithContainers 根据给定配置初始化 Pod API 对象。主要用于生成
// 每个容器使用特定镜像的 Pod。
func InitPodWithContainers(cs clientset.Interface, conf *PodWithContainersConfig) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      conf.Name,
			Namespace: conf.Namespace,
		},
		Spec: v1.PodSpec{
			Containers: conf.Containers,
		},
	}
	return pod
}

// RunPodWithContainers 使用给定配置和容器创建 Pod，并等待
// 其被调度。它返回其指针和 error 状态。
func RunPodWithContainers(cs clientset.Interface, pod *v1.Pod) (*v1.Pod, error) {
	pod, err := cs.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create pod-with-containers: %v", err)
	}
	if err = WaitForPodToSchedule(context.TODO(), cs, pod); err != nil {
		return pod, fmt.Errorf("Pod %v didn't schedule successfully. Error: %v", pod.Name, err)
	}
	if pod, err = cs.CoreV1().Pods(pod.Namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{}); err != nil {
		return pod, fmt.Errorf("failed to get pod %v info: %v", pod.Name, err)
	}
	return pod, nil
}

// PodIsGettingEvicted 在 Pod 设置了 deletion timestamp 时返回 true。
func PodIsGettingEvicted(c clientset.Interface, podNamespace, podName string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.DeletionTimestamp != nil {
			return true, nil
		}
		return false, nil
	}
}

// PodScheduledIn 在给定 Pod 被放到预期 Node 之一上时返回 true。
func PodScheduledIn(c clientset.Interface, podNamespace, podName string, nodeNames []string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			// 可能是连接错误，所以重试。
			return false, nil
		}
		if pod.Spec.NodeName == "" {
			return false, nil
		}
		for _, nodeName := range nodeNames {
			if pod.Spec.NodeName == nodeName {
				return true, nil
			}
		}
		return false, nil
	}
}

// PodUnschedulable 返回一个条件函数，当给定 Pod 进入
// 原因为 'Unschedulable' 的 unschedulable 状态时返回 true。
func PodUnschedulable(c clientset.Interface, podNamespace, podName string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			// 可能是连接错误，所以重试。
			return false, nil
		}
		_, cond := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
		return cond != nil && cond.Status == v1.ConditionFalse &&
			cond.Reason == v1.PodReasonUnschedulable && pod.Spec.NodeName == "", nil
	}
}

// PodSchedulingError 返回一个条件函数，当给定 Pod 因
// "Unschedulable" 之外的原因进入 unschedulable 状态时返回 true。scheduler
// 会在出错时记录这类原因。
func PodSchedulingError(c clientset.Interface, podNamespace, podName string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			// 可能是连接错误，所以重试。
			return false, nil
		}
		_, cond := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
		return cond != nil && cond.Status == v1.ConditionFalse &&
			cond.Reason != v1.PodReasonUnschedulable, nil
	}
}

// PodSchedulingGated 返回一个条件函数，当给定 Pod 进入
// 原因为 'SchedulingGated' 的 unschedulable 状态时返回 true。
func PodSchedulingGated(ctx context.Context, c clientset.Interface, podNamespace, podName string) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			// 可能是连接错误，所以重试。
			return false, nil
		}
		_, cond := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
		return cond != nil && cond.Status == v1.ConditionFalse &&
			cond.Reason == v1.PodReasonSchedulingGated && pod.Spec.NodeName == "", nil
	}
}

// WaitForPodUnschedulableWithTimeout 等待 Pod 调度失败，
// 如果在给定超时时间内未变为 unschedulable，则返回 error。
func WaitForPodUnschedulableWithTimeout(ctx context.Context, cs clientset.Interface, pod *v1.Pod, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, false, PodUnschedulable(cs, pod.Namespace, pod.Name))
}

// WaitForPodUnschedulable 等待 Pod 调度失败，
// 如果在超时时间（30 秒）内未变为 unschedulable，则返回 error。
func WaitForPodUnschedulable(ctx context.Context, cs clientset.Interface, pod *v1.Pod) error {
	return WaitForPodUnschedulableWithTimeout(ctx, cs, pod, 30*time.Second)
}

// WaitForPodSchedulingGated 等待 Pod 进入 scheduling gated 状态，
// 如果在给定超时时间内未进入该状态，则返回 error。
func WaitForPodSchedulingGated(ctx context.Context, cs clientset.Interface, pod *v1.Pod, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, false, PodSchedulingGated(ctx, cs, pod.Namespace, pod.Name))
}

// WaitForPDBsStable 等待 PDB 的 "CurrentHealthy" status
// 等于预期值。
func WaitForPDBsStable(testCtx *TestContext, pdbs []*policy.PodDisruptionBudget, pdbPodNum []int32) error {
	return wait.PollUntilContextTimeout(testCtx.Ctx, time.Second, 60*time.Second, false, func(context.Context) (bool, error) {
		pdbList, err := testCtx.ClientSet.PolicyV1().PodDisruptionBudgets(testCtx.NS.Name).List(testCtx.Ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		if len(pdbList.Items) != len(pdbs) {
			return false, nil
		}
		for i, pdb := range pdbs {
			found := false
			for _, cpdb := range pdbList.Items {
				if pdb.Name == cpdb.Name && pdb.Namespace == cpdb.Namespace {
					found = true
					if cpdb.Status.CurrentHealthy != pdbPodNum[i] {
						return false, nil
					}
				}
			}
			if !found {
				return false, nil
			}
		}
		return true, nil
	})
}

// WaitCachedPodsStable 等待 scheduler 缓存包含给定的 Pod。
func WaitCachedPodsStable(testCtx *TestContext, pods []*v1.Pod) error {
	return wait.PollUntilContextTimeout(testCtx.Ctx, time.Second, 30*time.Second, false, func(context.Context) (bool, error) {
		cachedPods, err := testCtx.Scheduler.Cache.PodCount()
		if err != nil {
			return false, err
		}
		if len(pods) != cachedPods {
			return false, nil
		}
		for _, p := range pods {
			actualPod, err1 := testCtx.ClientSet.CoreV1().Pods(p.Namespace).Get(testCtx.Ctx, p.Name, metav1.GetOptions{})
			if err1 != nil {
				return false, err1
			}
			cachedPod, err2 := testCtx.Scheduler.Cache.GetPod(actualPod)
			if err2 != nil || cachedPod == nil {
				return false, err2
			}
		}
		return true, nil
	})
}

// DeletePod 删除给定 namespace 中的 Pod。
func DeletePod(cs clientset.Interface, podName string, nsName string) error {
	return cs.CoreV1().Pods(nsName).Delete(context.TODO(), podName, *metav1.NewDeleteOptions(0))
}

func GetPod(cs clientset.Interface, podName string, podNamespace string) (*v1.Pod, error) {
	return cs.CoreV1().Pods(podNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
}

func CreateNamespacesWithLabels(cs clientset.Interface, namespaces []string, labels map[string]string) error {
	for _, n := range namespaces {
		ns := v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n, Labels: labels}}
		if _, err := cs.CoreV1().Namespaces().Create(context.TODO(), &ns, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// timeout 如果给定的 `f` 函数未在 `d` 时间内完成，则返回超时 error；
// 否则返回 nil。
func timeout(ctx context.Context, d time.Duration, f func()) error {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NextEntityOrDie 返回 scheduler 队列中的下一个 entity（Pod 或 PodGroup）。
// 该操作必须在 5 秒内完成，否则测试将中止。
func NextEntityOrDie(t *testing.T, testCtx *TestContext) schedulerframework.QueuedEntityInfo {
	t.Helper()

	var entity schedulerframework.QueuedEntityInfo
	logger := klog.FromContext(testCtx.Ctx)
	// NextEntity() 是阻塞操作。用 timeout() 包装它，避免依赖
	// Go 默认的测试超时（10m）来中止。
	if err := timeout(testCtx.Ctx, time.Second*5, func() {
		entity, _ = testCtx.Scheduler.NextEntity(logger)
	}); err != nil {
		t.Fatalf("Timed out waiting for the entity to be popped: %v", err)
	}
	return entity
}

func WaitForNominatedNodeNameWithTimeout(ctx context.Context, cs clientset.Interface, pod *v1.Pod, timeout time.Duration) error {
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, false, func(ctx context.Context) (bool, error) {
		pod, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if len(pod.Status.NominatedNodeName) > 0 {
			return true, nil
		}
		return false, err
	}); err != nil {
		return fmt.Errorf(".status.nominatedNodeName of Pod %v/%v did not get set: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

func WaitForNominatedNodeName(ctx context.Context, cs clientset.Interface, pod *v1.Pod) error {
	return WaitForNominatedNodeNameWithTimeout(ctx, cs, pod, wait.ForeverTestTimeout)
}
