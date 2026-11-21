/*
Copyright 2020 The Kubernetes Authors.

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

package resourceclaim

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	resourceclaimmetrics "k8s.io/dynamic-resource-allocation/resourceclaim/metrics"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	controllermetrics "k8s.io/kubernetes/pkg/controller/resourceclaim/metrics"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// 以下是所有测试用例共享的夹具：名称、命名空间、UID，以及由构造函数派生的各种资源状态。
var (
	// 基础名称、命名空间与 UID。
	testPodName          = "test-pod"
	testPodGroupName     = "test-podgroup"
	testNamespace        = "my-namespace"
	testPodUID           = types.UID("uidpod1")
	testPodGroupUID      = types.UID("uidpodgroup1")
	otherNamespace       = "not-my-namespace"
	podResourceClaimName = "acme-resource"
	templateName         = "my-template"
	nodeName             = "worker"

	// 由基础 Pod 派生的各种形态：带资源声明、带 PodGroup 资源声明等。
	testPod                     = makePod(testPodName, testNamespace, testPodUID)
	testPodWithResource         = makePod(testPodName, testNamespace, testPodUID, *makePodResourceClaim(podResourceClaimName, templateName))
	testPodWithPodGroupResource = podInPodGroup(testPodWithResource, testPodName, testPodGroupName)

	otherTestPod = makePod(testPodName+"-II", testNamespace, testPodUID+"-II")

	testPodGroup                     = makePodGroup(testPodGroupName, testNamespace, testPodGroupUID)
	testPodGroupWithResource         = makePodGroup(testPodGroupName, testNamespace, testPodGroupUID, *makePodGroupResourceClaim(podResourceClaimName, templateName))
	testPodGroupWithResourceInStatus = func() *schedulingapi.PodGroup {
		podGroup := testPodGroupWithResource.DeepCopy()
		podGroup.Status.ResourceClaimStatuses = []schedulingapi.PodGroupResourceClaimStatus{
			{Name: podResourceClaimName, ResourceClaimName: &testPodGroupClaim.Name},
		}
		return podGroup
	}()

	// 预置的 ResourceClaim：未分配、已分配、被保留，以及对应的队列键。
	testClaim              = makeClaim(testPodName+"-"+podResourceClaimName, testNamespace, makeOwnerReference(testPodWithResource, true))
	testPodGroupClaim      = makeClaim(testPodName+"-"+podResourceClaimName, testNamespace, makeOwnerReference(testPodGroupWithResource, true))
	testClaimAllocated     = allocateClaim(testClaim)
	testClaimReserved      = reserveClaim(testClaimAllocated, testPodWithResource)
	testClaimReservedTwice = reserveClaim(testClaimReserved, otherTestPod)
	testClaimKey           = claimKeyPrefix + testClaim.Namespace + "/" + testClaim.Name
	testPodKey             = podKeyPrefix + testNamespace + "/" + testPodName
	testPodGroupKey        = podGroupKeyPrefix + testNamespace + "/" + testPodGroupName

	// 被 PodGroup 保留的声明，用于验证工作负载维度的保留逻辑。
	testClaimReservedForPodGroup = reserveClaim(testClaimAllocated, testPodGroupWithResource)

	// 模板生成的声明及其各阶段状态；GenerateName 用于覆盖模板创建路径。
	templatedTestClaim                    = makeTemplatedClaim(podResourceClaimName, testPodName+"-"+podResourceClaimName+"-", testNamespace, 1, makeOwnerReference(testPodWithResource, true), nil)
	templatedTestClaimAllocated           = allocateClaim(templatedTestClaim)
	templatedTestClaimReserved            = reserveClaim(templatedTestClaimAllocated, testPodWithResource)
	templatedTestClaimReservedForPodGroup = reserveClaim(templatedTestClaimAllocated, testPodGroupWithResource)
	templatedTestPodGroupClaim            = makeTemplatedClaim(podResourceClaimName, testPodGroupName+"-"+podResourceClaimName+"-", testNamespace, 1, makeOwnerReference(testPodGroupWithResource, true), nil)

	templatedTestClaimWithAdmin          = makeTemplatedClaim(podResourceClaimName, testPodName+"-"+podResourceClaimName+"-", testNamespace, 1, makeOwnerReference(testPodWithResource, true), new(true))
	templatedTestClaimWithAdminAllocated = allocateClaim(templatedTestClaimWithAdmin)

	// 扩展资源声明来自 Pod 状态而非模板，走独立的创建路径。
	extendedTestClaim          = makeExtendedResourceClaim(testPodName, testNamespace, 1, makeOwnerReference(testPodWithResource, true))
	extendedTestClaimAllocated = allocateClaim(extendedTestClaim)

	// 无 owner 的同名声明、其他命名空间的声明，以及带管理员访问的模板。
	conflictingClaim         = makeClaim(testPodName+"-"+podResourceClaimName, testNamespace, nil)
	conflictingPodGroupClaim = makeClaim(testPodGroupName+"-"+podResourceClaimName, testNamespace, nil)
	otherNamespaceClaim      = makeClaim(testPodName+"-"+podResourceClaimName, otherNamespace, nil)
	template                 = makeTemplate(templateName, testNamespace, nil)
	templateWithAdminAccess  = makeTemplate(templateName, testNamespace, new(true))

	// 已调度到节点的 Pod：控制器据此判断可以写入保留信息。
	testPodWithNodeName = func() *v1.Pod {
		pod := testPodWithResource.DeepCopy()
		pod.Spec.NodeName = nodeName
		pod.Status.ResourceClaimStatuses = append(pod.Status.ResourceClaimStatuses, v1.PodResourceClaimStatus{
			Name:              pod.Spec.ResourceClaims[0].Name,
			ResourceClaimName: &templatedTestClaim.Name,
		})
		return pod
	}()
	testPodWithPodGroupAndNodeName = podInPodGroup(testPodWithNodeName, testPodName, testPodGroupName)
	adminAccessFeatureOffError     = "admin access is requested, but the feature is disabled"

	// WorkloadResourceClaims 依赖 GenericWorkload，只枚举两者兼容的组合。
	allPossibleFeatures = []controllerFeatures{
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
	}
	// 以下变量是 allPossibleFeatures 的子集，按单个特性开关筛选用例。
	adminAccessDisabled = []controllerFeatures{
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
	}
	// 仅开启管理员访问的组合。
	adminAccessEnabled = []controllerFeatures{
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
	}
	// 关闭通用工作负载的组合。
	genericWorkloadDisabled = []controllerFeatures{
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
	}
	// 开启通用工作负载的组合。
	genericWorkloadEnabled = []controllerFeatures{
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
	}
	// 关闭工作负载资源声明的组合。
	workloadResourceClaimsDisabled = []controllerFeatures{
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: false, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
	}
	// 开启工作负载资源声明的组合。
	workloadResourceClaimsEnabled = []controllerFeatures{
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: true},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: true},
	}
	// 通用工作负载开启但工作负载资源声明关闭：用于验证特性依赖报错。
	workloadResourceClaimsDisabledGenericWorkloadEnabled = []controllerFeatures{
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: false, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: false, WorkloadResourceClaims: false},
		{AdminAccess: true, GenericWorkload: true, PrioritizedList: true, WorkloadResourceClaims: false},
	}
)

// TestSyncHandler 是 testSyncHandler 的入口，负责初始化测试上下文。
func TestSyncHandler(t *testing.T) { testSyncHandler(ktesting.Init(t)) }

// TestCreateClaimDoesNotMutateTemplate 验证从已有注解的模板创建声明时会复制注解 map，
// 不修改共享 informer 缓存中的模板；Pod 与 PodGroup 两条创建路径都要覆盖。
func TestCreateClaimDoesNotMutateTemplate(t *testing.T) {
	// trigger 是触发事件的对象，key 是期望处理的队列键；features 决定走哪条创建路径。
	for _, tc := range []struct {
		name     string
		trigger  runtime.Object
		key      string
		features controllerFeatures
	}{
		{"pod", testPodWithResource.DeepCopy(), podKey(testPodWithResource), controllerFeatures{}},
		{"podGroup", testPodGroupWithResource.DeepCopy(), testPodGroupKey, controllerFeatures{GenericWorkload: true, WorkloadResourceClaims: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)

			template := makeTemplate(templateName, testNamespace, nil)
			template.Spec.ObjectMeta.Annotations = map[string]string{"example.com/color": "blue"}

			fakeKubeClient := createTestClient(tc.trigger, template)
			informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			podGroupInformer := informerFactory.Scheduling().V1beta1().PodGroups()
			claimInformer := informerFactory.Resource().V1().ResourceClaims()
			templateInformer := informerFactory.Resource().V1().ResourceClaimTemplates()
			setupMetrics()

			ec, err := newControllerWithFeatures(tCtx.Logger(), fakeKubeClient, podInformer, podGroupInformer, claimInformer, templateInformer, tc.features)
			if err != nil {
				tCtx.Fatalf("error creating controller: %v", err)
			}

			informerFactory.Start(tCtx.Done())
			defer func() {
				tCtx.Cancel("stopping informers")
				informerFactory.Shutdown()
			}()
			informerFactory.WaitForCacheSync(tCtx.Done())

			// cachedTemplate 是控制器从共享 informer 缓存中读到的对象；先做快照，
			// 这样任何原地修改都能被检测出来。
			cachedTemplate, err := templateInformer.Lister().ResourceClaimTemplates(testNamespace).Get(templateName)
			if err != nil {
				tCtx.Fatalf("template not in informer cache: %v", err)
			}
			templateBefore := cachedTemplate.DeepCopy()

			if err := ec.syncHandler(tCtx, tc.key); err != nil {
				tCtx.Fatalf("syncHandler: %v", err)
			}

			assert.Equal(tCtx, templateBefore, cachedTemplate, "controller must not mutate the shared informer-cached ResourceClaimTemplate")
			if _, leaked := cachedTemplate.Spec.ObjectMeta.Annotations[resourceapi.PodResourceClaimAnnotation]; leaked {
				tCtx.Errorf("PodResourceClaimAnnotation leaked into the shared template's annotations")
			}

			// 生成的声明必须同时保留模板原有注解和控制器追加的 PodResourceClaimAnnotation。
			claims, err := fakeKubeClient.ResourceV1().ResourceClaims(testNamespace).List(tCtx, metav1.ListOptions{})
			if err != nil {
				tCtx.Fatalf("listing claims: %v", err)
			}
			if assert.Len(tCtx, claims.Items, 1, "exactly one claim should be created") {
				created := claims.Items[0]
				assert.Equal(tCtx, "blue", created.Annotations["example.com/color"], "template annotation should be copied to the claim")
				assert.Equal(tCtx, podResourceClaimName, created.Annotations[resourceapi.PodResourceClaimAnnotation], "claim should carry the PodResourceClaimAnnotation")
			}
		})
	}
}

// testSyncHandler 覆盖 syncHandler 的创建、幂等、冲突、状态清理和保留处理场景。
func testSyncHandler(tCtx ktesting.TContext) {
	// claims 写入 fake 客户端；claimsInCache 只写入变更缓存；claimsLater 在 informer 停止后写入以模拟竞态。
	tests := []struct {
		name                string
		key                 string
		featureCombinations []controllerFeatures
		claims              []*resourceapi.ResourceClaim
		claimsInCache       []*resourceapi.ResourceClaim
		pods                []*v1.Pod
		podsLater           []*v1.Pod
		claimsLater         []*resourceapi.ResourceClaim
		podGroups           []*schedulingapi.PodGroup
		templates           []*resourceapi.ResourceClaimTemplate
		expectedClaims      []resourceapi.ResourceClaim
		// expectedStatuses 与 expectedPodGroupStatuses 只收集非空状态。
		expectedStatuses         map[string][]v1.PodResourceClaimStatus
		expectedPodGroupStatuses map[string][]schedulingapi.PodGroupResourceClaimStatus
		// expectedError 为空表示期望同步成功；expectedMetrics 需精确匹配。
		expectedError   string
		expectedMetrics claimCreateMetrics
	}{
		{
			name:           "create",
			pods:           []*v1.Pod{testPodWithResource},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podKey(testPodWithResource),
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
		},
		{
			name:                "create-adminaccess-feature-disabled",
			featureCombinations: adminAccessDisabled,
			pods:                []*v1.Pod{testPodWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{templateWithAdminAccess},
			key:                 podKey(testPodWithResource),
			expectedError:       adminAccessFeatureOffError,
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name:                "create-adminaccess-feature-enabled",
			featureCombinations: adminAccessEnabled,
			pods:                []*v1.Pod{testPodWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{templateWithAdminAccess},
			key:                 podKey(testPodWithResource),
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestClaimWithAdmin},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaimWithAdmin.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "true", ownerKind: "Pod"}: 1,
			},
		},
		{
			name:                "create-for-grouped-pod",
			featureCombinations: workloadResourceClaimsEnabled,
			pods:                []*v1.Pod{podInPodGroup(testPodWithResource, testPodName, testPodGroupName)},
			podGroups:           []*schedulingapi.PodGroup{testPodGroup},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podKey(testPodWithResource),
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
		},
		{
			name:                "skip-create-for-pod-podgroup-does-not-exist",
			featureCombinations: genericWorkloadEnabled,
			pods:                []*v1.Pod{podInPodGroup(testPodWithResource, testPodName, testPodGroupName)},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podKey(testPodWithResource),
			expectedError:       `podgroup.scheduling.k8s.io "test-podgroup" not found`,
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name:                "create-for-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podGroupKey(testPodGroupWithResource),
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerGroup: "scheduling.k8s.io", ownerKind: "PodGroup"}: 1,
			},
		},
		{
			name:                "skip-create-podgroup-claim-for-pod",
			featureCombinations: workloadResourceClaimsEnabled,
			pods:                []*v1.Pod{testPodWithPodGroupResource},
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podKey(testPodWithPodGroupResource),
			expectedClaims:      nil,
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name:                "update-pod-status-with-podgroup-claim",
			featureCombinations: workloadResourceClaimsEnabled,
			pods:                []*v1.Pod{testPodWithPodGroupResource},
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			claims:              []*resourceapi.ResourceClaim{templatedTestPodGroupClaim},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podKey(testPodWithPodGroupResource),
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestPodGroupClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithPodGroupResource.Name: {
					{Name: testPodWithPodGroupResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "skip-create-for-pod-with-podgroup-claim-genericworkload-disabled",
			featureCombinations: genericWorkloadDisabled,
			pods:                []*v1.Pod{testPodWithPodGroupResource},
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podKey(testPodWithPodGroupResource),
			expectedError:       "GenericWorkload feature is disabled",
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name:                "skip-create-for-pod-with-podgroup-claim-workloadresourceclaims-disabled",
			featureCombinations: workloadResourceClaimsDisabledGenericWorkloadEnabled,
			pods:                []*v1.Pod{testPodWithPodGroupResource},
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podKey(testPodWithPodGroupResource),
			expectedError:       "DRAWorkloadResourceClaims feature is disabled",
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name:                "create-for-grouped-pod-with-claim-workloadresourceclaims-disabled",
			featureCombinations: workloadResourceClaimsDisabledGenericWorkloadEnabled,
			pods:                []*v1.Pod{podInPodGroup(testPodWithResource, testPodName, testPodGroupName)},
			podGroups:           []*schedulingapi.PodGroup{testPodGroup},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podKey(testPodWithResource),
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodName: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
		},
		{
			name: "nop",
			pods: []*v1.Pod{func() *v1.Pod {
				pod := testPodWithResource.DeepCopy()
				pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				}
				return pod
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podKey(testPodWithResource),
			claims:         []*resourceapi.ResourceClaim{templatedTestClaim},
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "nop-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups: []*schedulingapi.PodGroup{func() *schedulingapi.PodGroup {
				podGroup := testPodGroupWithResource.DeepCopy()
				podGroup.Status.ResourceClaimStatuses = []schedulingapi.PodGroupResourceClaimStatus{
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				}
				return podGroup
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podGroupKey(testPodGroupWithResource),
			claims:         []*resourceapi.ResourceClaim{templatedTestPodGroupClaim},
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "nop-claim-in-mutation-cache-only",
			pods: []*v1.Pod{func() *v1.Pod {
				pod := testPodWithResource.DeepCopy()
				pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				}
				return pod
			}()},
			templates:     []*resourceapi.ResourceClaimTemplate{template},
			key:           podKey(testPodWithResource),
			claimsInCache: []*resourceapi.ResourceClaim{templatedTestClaim},
			// 声明只存在于变更缓存中，apiserver 里没有任何对象。
			expectedClaims: nil,
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "nop-claim-in-mutation-cache-only-for-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups: []*schedulingapi.PodGroup{func() *schedulingapi.PodGroup {
				podGroup := testPodGroupWithResource.DeepCopy()
				podGroup.Status.ResourceClaimStatuses = []schedulingapi.PodGroupResourceClaimStatus{
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				}
				return podGroup
			}()},
			templates:     []*resourceapi.ResourceClaimTemplate{template},
			key:           podGroupKey(testPodGroupWithResource),
			claimsInCache: []*resourceapi.ResourceClaim{templatedTestPodGroupClaim},
			// 声明只存在于变更缓存中，apiserver 里没有任何对象。
			expectedClaims: nil,
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "nop-claim-in-apiserver-only",
			pods: []*v1.Pod{func() *v1.Pod {
				pod := testPodWithResource.DeepCopy()
				pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				}
				return pod
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podKey(testPodWithResource),
			claimsLater:    []*resourceapi.ResourceClaim{templatedTestClaim},
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "nop-claim-in-apiserver-only-for-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups: []*schedulingapi.PodGroup{func() *schedulingapi.PodGroup {
				podGroup := testPodGroupWithResource.DeepCopy()
				podGroup.Status.ResourceClaimStatuses = []schedulingapi.PodGroupResourceClaimStatus{
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				}
				return podGroup
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podGroupKey(testPodGroupWithResource),
			claimsLater:    []*resourceapi.ResourceClaim{templatedTestPodGroupClaim},
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "recreate",
			pods: []*v1.Pod{func() *v1.Pod {
				pod := testPodWithResource.DeepCopy()
				pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				}
				return pod
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podKey(testPodWithResource),
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
		},
		{
			name:                "recreate-for-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups: []*schedulingapi.PodGroup{func() *schedulingapi.PodGroup {
				pod := testPodGroupWithResource.DeepCopy()
				pod.Status.ResourceClaimStatuses = []schedulingapi.PodGroupResourceClaimStatus{
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				}
				return pod
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podGroupKey(testPodGroupWithResource),
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerGroup: "scheduling.k8s.io", ownerKind: "PodGroup"}: 1,
			},
		},
		{
			name:            "missing-template",
			pods:            []*v1.Pod{testPodWithResource},
			templates:       nil,
			key:             podKey(testPodWithResource),
			expectedError:   "resource claim template \"my-template\": resourceclaimtemplate.resource.k8s.io \"my-template\" not found",
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "missing-template-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			templates:           nil,
			key:                 podGroupKey(testPodGroupWithResource),
			expectedError:       "resource claim template \"my-template\": resourceclaimtemplate.resource.k8s.io \"my-template\" not found",
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name:           "find-existing-claim-by-label",
			pods:           []*v1.Pod{testPodWithResource},
			key:            podKey(testPodWithResource),
			claims:         []*resourceapi.ResourceClaim{templatedTestClaim},
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "find-existing-claim-by-label-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			key:                 podGroupKey(testPodGroupWithResource),
			claims:              []*resourceapi.ResourceClaim{templatedTestPodGroupClaim},
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:          "find-created-claim-in-cache",
			pods:          []*v1.Pod{testPodWithResource},
			key:           podKey(testPodWithResource),
			claimsInCache: []*resourceapi.ResourceClaim{templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "find-created-claim-in-cache-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			key:                 podGroupKey(testPodGroupWithResource),
			claimsInCache:       []*resourceapi.ResourceClaim{templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "recreate-wrong-owner",
			pods: []*v1.Pod{func() *v1.Pod {
				pod := testPodWithResource.DeepCopy()
				pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &conflictingClaim.Name},
				}
				return pod
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podKey(testPodWithResource),
			claims:         []*resourceapi.ResourceClaim{conflictingClaim},
			expectedClaims: []resourceapi.ResourceClaim{*conflictingClaim, *templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
		},
		{
			name:                "recreate-wrong-owner-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups: []*schedulingapi.PodGroup{func() *schedulingapi.PodGroup {
				podGroup := testPodGroupWithResource.DeepCopy()
				podGroup.Status.ResourceClaimStatuses = []schedulingapi.PodGroupResourceClaimStatus{
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &conflictingPodGroupClaim.Name},
				}
				return podGroup
			}()},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podGroupKey(testPodGroupWithResource),
			claims:         []*resourceapi.ResourceClaim{conflictingPodGroupClaim},
			expectedClaims: []resourceapi.ResourceClaim{*conflictingPodGroupClaim, *templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerGroup: "scheduling.k8s.io", ownerKind: "PodGroup"}: 1,
			},
		},
		{
			name:            "no-such-pod",
			key:             podKey(testPodWithResource),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "no-such-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			key:                 podGroupKey(testPodGroupWithResource),
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name: "pod-deleted",
			pods: func() []*v1.Pod {
				deleted := metav1.Now()
				pods := []*v1.Pod{testPodWithResource.DeepCopy()}
				pods[0].DeletionTimestamp = &deleted
				return pods
			}(),
			key:             podKey(testPodWithResource),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "podgroup-deleted",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups: func() []*schedulingapi.PodGroup {
				deleted := metav1.Now()
				podGroups := []*schedulingapi.PodGroup{testPodGroupWithResource.DeepCopy()}
				podGroups[0].DeletionTimestamp = &deleted
				return podGroups
			}(),
			key:             podGroupKey(testPodGroupWithResource),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:            "no-volumes",
			pods:            []*v1.Pod{testPod},
			key:             podKey(testPod),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:           "create-with-other-claim",
			pods:           []*v1.Pod{testPodWithResource},
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			key:            podKey(testPodWithResource),
			claims:         []*resourceapi.ResourceClaim{otherNamespaceClaim},
			expectedClaims: []resourceapi.ResourceClaim{*otherNamespaceClaim, *templatedTestClaim},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithResource.Name: {
					{Name: testPodWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
		},
		{
			name:                "create-with-other-claim-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podGroupKey(testPodGroupWithResource),
			claims:              []*resourceapi.ResourceClaim{otherNamespaceClaim},
			expectedClaims:      []resourceapi.ResourceClaim{*otherNamespaceClaim, *templatedTestPodGroupClaim},
			expectedPodGroupStatuses: map[string][]schedulingapi.PodGroupResourceClaimStatus{
				testPodGroupWithResource.Name: {
					{Name: testPodGroupWithResource.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestPodGroupClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerGroup: "scheduling.k8s.io", ownerKind: "PodGroup"}: 1,
			},
		},
		{
			name:            "wrong-claim-owner",
			pods:            []*v1.Pod{testPodWithResource},
			key:             podKey(testPodWithResource),
			claims:          []*resourceapi.ResourceClaim{conflictingClaim},
			expectedClaims:  []resourceapi.ResourceClaim{*conflictingClaim},
			expectedError:   "resource claim template \"my-template\": resourceclaimtemplate.resource.k8s.io \"my-template\" not found",
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "wrong-claim-owner-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			key:                 podGroupKey(testPodGroupWithResource),
			claims:              []*resourceapi.ResourceClaim{conflictingPodGroupClaim},
			expectedClaims:      []resourceapi.ResourceClaim{*conflictingPodGroupClaim},
			expectedError:       "resource claim template \"my-template\": resourceclaimtemplate.resource.k8s.io \"my-template\" not found",
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			// 注入冲突错误，验证创建失败时的指标计数。
			name:      "create-conflict",
			pods:      []*v1.Pod{testPodWithResource},
			templates: []*resourceapi.ResourceClaimTemplate{template},
			key:       podKey(testPodWithResource),
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "failure", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
			expectedError: "create ResourceClaim : Operation cannot be fulfilled on resourceclaims.resource.k8s.io \"fake name\": fake conflict",
		},
		{
			name:                "create-conflict-admin-access",
			featureCombinations: adminAccessEnabled,
			pods:                []*v1.Pod{testPodWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{templateWithAdminAccess},
			key:                 podKey(testPodWithResource),
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "failure", adminAccess: "true", ownerKind: "Pod"}: 1,
			},
			expectedError: "create ResourceClaim : Operation cannot be fulfilled on resourceclaims.resource.k8s.io \"fake name\": fake conflict",
		},
		{
			name:                "create-conflict-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			key:                 podGroupKey(testPodGroupWithResource),
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "failure", adminAccess: "false", ownerGroup: "scheduling.k8s.io", ownerKind: "PodGroup"}: 1,
			},
			expectedError: "create ResourceClaim : Operation cannot be fulfilled on resourceclaims.resource.k8s.io \"fake name\": fake conflict",
		},
		{
			name:            "stay-reserved-seen",
			pods:            []*v1.Pod{testPodWithResource},
			key:             claimKey(testClaimReserved),
			claims:          []*resourceapi.ResourceClaim{testClaimReserved},
			expectedClaims:  []resourceapi.ResourceClaim{*testClaimReserved},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			// Pod 在 informer 停止后才出现，已有保留仍应保持不变。
			name:            "stay-reserved-not-seen",
			podsLater:       []*v1.Pod{testPodWithResource},
			key:             claimKey(testClaimReserved),
			claims:          []*resourceapi.ResourceClaim{testClaimReserved},
			expectedClaims:  []resourceapi.ResourceClaim{*testClaimReserved},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:   "clear-reserved-structured",
			pods:   []*v1.Pod{},
			key:    claimKey(testClaimReserved),
			claims: []*resourceapi.ResourceClaim{structuredParameters(testClaimReserved)},
			expectedClaims: func() []resourceapi.ResourceClaim {
				claim := testClaimAllocated.DeepCopy()
				claim.Finalizers = []string{}
				claim.Status.Allocation = nil
				return []resourceapi.ResourceClaim{*claim}
			}(),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "clear-reserved-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			podGroups:           []*schedulingapi.PodGroup{},
			key:                 claimKey(testClaimReservedForPodGroup),
			claims:              []*resourceapi.ResourceClaim{structuredParameters(testClaimReservedForPodGroup)},
			expectedClaims: func() []resourceapi.ResourceClaim {
				claim := testClaimAllocated.DeepCopy()
				claim.Finalizers = []string{}
				claim.Status.Allocation = nil
				return []resourceapi.ResourceClaim{*claim}
			}(),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "dont-clear-reserved-podgroup-feature-disabled",
			featureCombinations: workloadResourceClaimsDisabled,
			podGroups:           []*schedulingapi.PodGroup{},
			key:                 claimKey(testClaimReservedForPodGroup),
			claims:              []*resourceapi.ResourceClaim{structuredParameters(testClaimReservedForPodGroup)},
			expectedClaims:      []resourceapi.ResourceClaim{*structuredParameters(testClaimReservedForPodGroup)},
			expectedMetrics:     claimCreateMetrics{},
		},
		{
			name: "dont-clear-reserved-structured",
			pods: []*v1.Pod{testPodWithResource},
			key:  claimKey(testClaimReserved),
			claims: func() []*resourceapi.ResourceClaim {
				claim := structuredParameters(testClaimReserved)
				claim = reserveClaim(claim, otherTestPod)
				return []*resourceapi.ResourceClaim{claim}
			}(),
			expectedClaims:  []resourceapi.ResourceClaim{*structuredParameters(testClaimReserved)},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "clear-reserved-structured-deleted",
			pods: []*v1.Pod{},
			key:  claimKey(testClaimReserved),
			claims: func() []*resourceapi.ResourceClaim {
				claim := structuredParameters(testClaimReserved.DeepCopy())
				claim.DeletionTimestamp = &metav1.Time{}
				return []*resourceapi.ResourceClaim{claim}
			}(),
			expectedClaims: func() []resourceapi.ResourceClaim {
				claim := structuredParameters(testClaimAllocated.DeepCopy())
				claim.DeletionTimestamp = &metav1.Time{}
				claim.Finalizers = []string{}
				claim.Status.Allocation = nil
				return []resourceapi.ResourceClaim{*claim}
			}(),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "structured-deleted",
			pods: []*v1.Pod{},
			key:  claimKey(testClaimReserved),
			claims: func() []*resourceapi.ResourceClaim {
				claim := structuredParameters(testClaimAllocated.DeepCopy())
				claim.DeletionTimestamp = &metav1.Time{}
				return []*resourceapi.ResourceClaim{claim}
			}(),
			expectedClaims: func() []resourceapi.ResourceClaim {
				claim := structuredParameters(testClaimAllocated.DeepCopy())
				claim.DeletionTimestamp = &metav1.Time{}
				claim.Finalizers = []string{}
				claim.Status.Allocation = nil
				return []resourceapi.ResourceClaim{*claim}
			}(),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			// Pod 成功后应清除保留和分配结果。
			name: "clear-reserved-when-done",
			pods: func() []*v1.Pod {
				pods := []*v1.Pod{testPodWithResource.DeepCopy()}
				pods[0].Status.Phase = v1.PodSucceeded
				return pods
			}(),
			key: claimKey(testClaimReserved),
			claims: func() []*resourceapi.ResourceClaim {
				claims := []*resourceapi.ResourceClaim{testClaimReserved.DeepCopy()}
				claims[0].OwnerReferences = nil
				return claims
			}(),
			expectedClaims: func() []resourceapi.ResourceClaim {
				claims := []resourceapi.ResourceClaim{*testClaimAllocated.DeepCopy()}
				claims[0].OwnerReferences = nil
				return claims
			}(),
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:            "remove-reserved",
			pods:            []*v1.Pod{testPod},
			key:             claimKey(testClaimReservedTwice),
			claims:          []*resourceapi.ResourceClaim{testClaimReservedTwice},
			expectedClaims:  []resourceapi.ResourceClaim{*testClaimReserved},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			// Pod 成功后且声明归该 Pod 所有时直接删除声明。
			name: "delete-claim-when-done",
			pods: func() []*v1.Pod {
				pods := []*v1.Pod{testPodWithResource.DeepCopy()}
				pods[0].Status.Phase = v1.PodSucceeded
				return pods
			}(),
			key:             claimKey(testClaimReserved),
			claims:          []*resourceapi.ResourceClaim{testClaimReserved},
			expectedClaims:  nil,
			expectedMetrics: claimCreateMetrics{},
		},
		{
			// Pod 已调度到节点，允许写入保留信息。
			name:           "add-reserved",
			pods:           []*v1.Pod{testPodWithNodeName},
			key:            podKey(testPodWithNodeName),
			templates:      []*resourceapi.ResourceClaimTemplate{template},
			claims:         []*resourceapi.ResourceClaim{templatedTestClaimAllocated},
			expectedClaims: []resourceapi.ResourceClaim{*templatedTestClaimReserved},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithNodeName.Name: {
					{Name: testPodWithNodeName.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "add-reserved-podgroup",
			featureCombinations: workloadResourceClaimsEnabled,
			pods:                []*v1.Pod{testPodWithPodGroupAndNodeName},
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			key:                 podKey(testPodWithPodGroupAndNodeName),
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			claims:              []*resourceapi.ResourceClaim{templatedTestClaimAllocated},
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestClaimReservedForPodGroup},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithPodGroupAndNodeName.Name: {
					{Name: testPodWithPodGroupAndNodeName.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "skip-add-reserved-podgroup-genericworkload-disabled",
			featureCombinations: genericWorkloadDisabled,
			pods:                []*v1.Pod{testPodWithPodGroupAndNodeName},
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			key:                 podKey(testPodWithPodGroupAndNodeName),
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			claims:              []*resourceapi.ResourceClaim{templatedTestClaimAllocated},
			expectedError:       "GenericWorkload feature is disabled",
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestClaimAllocated},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithPodGroupAndNodeName.Name: {
					{Name: testPodWithPodGroupAndNodeName.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name:                "skip-add-reserved-podgroup-workloadresourceclaims-disabled",
			featureCombinations: workloadResourceClaimsDisabledGenericWorkloadEnabled,
			pods:                []*v1.Pod{testPodWithPodGroupAndNodeName},
			podGroups:           []*schedulingapi.PodGroup{testPodGroupWithResource},
			key:                 podKey(testPodWithPodGroupAndNodeName),
			templates:           []*resourceapi.ResourceClaimTemplate{template},
			claims:              []*resourceapi.ResourceClaim{templatedTestClaimAllocated},
			expectedClaims:      []resourceapi.ResourceClaim{*templatedTestClaimAllocated},
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodWithPodGroupAndNodeName.Name: {
					{Name: testPodWithPodGroupAndNodeName.Spec.ResourceClaims[0].Name, ResourceClaimName: &templatedTestClaim.Name},
				},
			},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "remove-pod-reservation-with-non-pod-reservation-present",
			pods: func() []*v1.Pod {
				pod := testPodWithResource.DeepCopy()
				pod.Status.Phase = v1.PodSucceeded
				return []*v1.Pod{pod}
			}(),
			claims: func() []*resourceapi.ResourceClaim {
				claim := testClaimReserved.DeepCopy()
				nonPodRef := resourceapi.ResourceClaimConsumerReference{
					APIGroup: "foo.com",
					Resource: "foo",
					Name:     "foo",
					UID:      "123",
				}
				claim.Status.ReservedFor = append(claim.Status.ReservedFor, nonPodRef)
				return []*resourceapi.ResourceClaim{claim}
			}(),
			key: testClaimKey,
			expectedClaims: []resourceapi.ResourceClaim{func() resourceapi.ResourceClaim {
				claim := testClaimReserved.DeepCopy()
				nonPodRef := resourceapi.ResourceClaimConsumerReference{
					APIGroup: "foo.com",
					Resource: "foo",
					Name:     "foo",
					UID:      "123",
				}
				claim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{nonPodRef}
				return *claim
			}()},
			expectedMetrics: claimCreateMetrics{},
		},
		{
			name: "flapping-resourceclaim-statuses",
			pods: func() []*v1.Pod {
				pod := makePod(testPodName, testNamespace, testPodUID,
					*makePodResourceClaim("claimA", templateName),
					*makePodResourceClaim("claimB", templateName),
				)
				// 初始状态中只有 claimA，期望同步后补上 claimB。
				pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
					{Name: "claimA", ResourceClaimName: new("claimA-object")},
				}
				return []*v1.Pod{pod}
			}(),
			templates: []*resourceapi.ResourceClaimTemplate{template},
			claims: []*resourceapi.ResourceClaim{
				makeClaim("claimA-object", testNamespace, makeOwnerReference(testPod, true)),
			},
			key: podKeyPrefix + testNamespace + "/" + testPodName,
			expectedStatuses: map[string][]v1.PodResourceClaimStatus{
				testPodName: {
					{Name: "claimA", ResourceClaimName: new("claimA-object")},
					{Name: "claimB", ResourceClaimName: new("test-pod-claimB--1")},
				},
			},
			expectedClaims: []resourceapi.ResourceClaim{
				*makeClaim("claimA-object", testNamespace, makeOwnerReference(testPod, true)),
				*makeTemplatedClaim("claimB", testPodName+"-claimB-", testNamespace, 1, makeOwnerReference(testPod, true), nil),
			},
			expectedMetrics: claimCreateMetrics{
				claimCreateMetricLabels{status: "success", adminAccess: "false", ownerKind: "Pod"}: 1,
			},
		},
	}

	for _, tc := range tests {
		// 日志与指标都是全局状态，用例必须串行执行。
		run := func(tCtx ktesting.TContext, features controllerFeatures) {
			var objects []runtime.Object
			for _, pod := range tc.pods {
				objects = append(objects, pod)
			}
			for _, podGroup := range tc.podGroups {
				objects = append(objects, podGroup)
			}
			for _, claim := range tc.claims {
				objects = append(objects, claim)
			}
			for _, template := range tc.templates {
				objects = append(objects, template)
			}

			fakeKubeClient := createTestClient(objects...)
			// 期望失败指标时注入冲突错误。
			for labels, expected := range tc.expectedMetrics {
				if expected > 0 && labels.status == "failure" {
					fakeKubeClient.PrependReactor("create", "resourceclaims", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), "fake name", errors.New("fake conflict"))
					})
					break
				}
			}
			// 记录状态补丁但不处理该 action（返回 false），交由默认 reactor 执行。
			var appliedPatches []string
			fakeKubeClient.PrependReactor("patch", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				patchAction := action.(k8stesting.PatchAction)
				if patchAction.GetSubresource() == "status" {
					appliedPatches = append(appliedPatches, string(patchAction.GetPatch()))
				}
				return false, nil, nil
			})
			informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			podGroupInformer := informerFactory.Scheduling().V1beta1().PodGroups()
			claimInformer := informerFactory.Resource().V1().ResourceClaims()
			templateInformer := informerFactory.Resource().V1().ResourceClaimTemplates()
			setupMetrics()

			ec, err := newControllerWithFeatures(tCtx.Logger(), fakeKubeClient, podInformer, podGroupInformer, claimInformer, templateInformer, features)
			if err != nil {
				tCtx.Fatalf("error creating ephemeral controller : %v", err)
			}

			// 等待 informer 缓存同步完成。
			informerFactory.Start(tCtx.Done())
			stopInformers := func() {
				tCtx.Cancel("stopping informers")
				informerFactory.Shutdown()
			}
			defer stopInformers()
			informerFactory.WaitForCacheSync(tCtx.Done())

			// 加入只存在于变更缓存中的声明。
			for _, claim := range tc.claimsInCache {
				ec.claimCache.Mutation(claim)
			}

			// 模拟竞态：停止 informer 后再写入控制器尚不知晓的对象。
			stopInformers()
			for _, pod := range tc.podsLater {
				_, err := fakeKubeClient.CoreV1().Pods(pod.Namespace).Create(tCtx, pod, metav1.CreateOptions{})
				if err != nil {
					tCtx.Fatalf("unexpected error while creating pod: %v", err)
				}
			}
			for _, claim := range tc.claimsLater {
				_, err := fakeKubeClient.ResourceV1().ResourceClaims(claim.Namespace).Create(tCtx, claim, metav1.CreateOptions{})
				if err != nil {
					tCtx.Fatalf("unexpected error while creating claim: %v", err)
				}
			}

			err = ec.syncHandler(tCtx, tc.key)
			if err != nil {
				if len(tc.expectedError) == 0 {
					assert.NoError(tCtx, err)
				} else {
					assert.ErrorContains(tCtx, err, tc.expectedError, "the error message should have contained the expected error message")
				}

				return
			}
			if tc.expectedError != "" {
				assert.ErrorContains(tCtx, err, tc.expectedError, "the error message should have contained the expected error message")
			} else if err != nil {
				tCtx.Errorf("unexpected sync handler error: %v", err)
			}
			// 继续执行以检查其他副作用。

			// 该用例验证一次补丁同时写入两个声明的状态。
			if tc.name == "flapping-resourceclaim-statuses" {
				assert.Len(tCtx, appliedPatches, 1, "should have applied status once")
				assert.Contains(tCtx, appliedPatches[0], `"name":"claimA"`, "patch should contain claimA")
				assert.Contains(tCtx, appliedPatches[0], `"name":"claimB"`, "patch should contain claimB")
			}

			claims, err := fakeKubeClient.ResourceV1().ResourceClaims("").List(tCtx, metav1.ListOptions{})
			if err != nil {
				tCtx.Fatalf("unexpected error while listing claims: %v", err)
			}
			assert.Equal(tCtx, normalizeClaims(tc.expectedClaims), normalizeClaims(claims.Items))

			pods, err := fakeKubeClient.CoreV1().Pods("").List(tCtx, metav1.ListOptions{})
			if err != nil {
				tCtx.Fatalf("unexpected error while listing pods: %v", err)
			}
			// 没有状态的 Pod 不参与比较，避免空 slice 与 nil 的差异干扰断言。
			var actualStatuses map[string][]v1.PodResourceClaimStatus
			for _, pod := range pods.Items {
				if len(pod.Status.ResourceClaimStatuses) == 0 {
					continue
				}
				if actualStatuses == nil {
					actualStatuses = make(map[string][]v1.PodResourceClaimStatus)
				}
				actualStatuses[pod.Name] = pod.Status.ResourceClaimStatuses
			}
			assert.Equal(tCtx, tc.expectedStatuses, actualStatuses, "pod resource claim statuses")

			podGroups, err := fakeKubeClient.SchedulingV1beta1().PodGroups("").List(tCtx, metav1.ListOptions{})
			if err != nil {
				tCtx.Fatalf("unexpected error while listing podgroups: %v", err)
			}
			// PodGroup 状态同样只收集非空值。
			var actualPodGroupStatuses map[string][]schedulingapi.PodGroupResourceClaimStatus
			for _, podGroup := range podGroups.Items {
				if len(podGroup.Status.ResourceClaimStatuses) == 0 {
					continue
				}
				if actualPodGroupStatuses == nil {
					actualPodGroupStatuses = make(map[string][]schedulingapi.PodGroupResourceClaimStatus)
				}
				actualPodGroupStatuses[podGroup.Name] = podGroup.Status.ResourceClaimStatuses
			}
			assert.Equal(tCtx, tc.expectedPodGroupStatuses, actualPodGroupStatuses, "podgroup resource claim statuses")

			expectMetrics(tCtx, tc.expectedMetrics)
		}
		tCtx.Run(tc.name, func(tCtx ktesting.TContext) {
			if len(tc.featureCombinations) == 0 {
				tc.featureCombinations = allPossibleFeatures
			}
			for _, features := range tc.featureCombinations {
				tCtx.Run(features.String(), func(tCtx ktesting.TContext) {
					run(tCtx, features)
				})
			}
		})
	}
}

// TestClaimExists 覆盖 claimExists 查找声明的三个位置（变更缓存、informer 底层存储、
// apiserver 回退），以及可选的 owner 检查。
func TestClaimExists(t *testing.T) { testClaimExists(ktesting.Init(t)) }

// testClaimExists 是 TestClaimExists 的实际实现。
func testClaimExists(tCtx ktesting.TContext) {
	// 用桩函数控制 owner 检查结果，避免构造真实 owner 引用。
	alwaysOwned := func(*resourceapi.ResourceClaim) bool { return true }
	neverOwned := func(*resourceapi.ResourceClaim) bool { return false }

	for _, tc := range []struct {
		name                 string
		claimInMutationCache *resourceapi.ResourceClaim
		claimInInformerCache *resourceapi.ResourceClaim
		claimInAPIServer     *resourceapi.ResourceClaim
		// 在 GET 上注入 reactor，模拟 apiserver 的瞬时错误。
		getReactor   k8stesting.ReactionFunc
		invalidOwner bool
		wantExists   bool
		wantErr      bool
	}{
		{
			name:                 "found in mutation cache",
			claimInMutationCache: templatedTestClaim,
			wantExists:           true,
		},
		{
			name:                 "found in underlay store",
			claimInInformerCache: templatedTestClaim,
			wantExists:           true,
		},
		{
			name:             "found in apiserver fallback",
			claimInAPIServer: templatedTestClaim,
			wantExists:       true,
		},
		{
			name:       "not found anywhere",
			wantExists: false,
		},
		{
			name: "apiserver returns error",
			getReactor: func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewInternalError(errors.New("apiserver is down"))
			},
			wantExists: false,
			wantErr:    true,
		},
		{
			name:                 "owner check fails",
			claimInMutationCache: templatedTestClaim,
			invalidOwner:         true,
			wantExists:           false,
		},
	} {
		tCtx.Run(tc.name, func(tCtx ktesting.TContext) {
			var objects []runtime.Object
			if tc.claimInInformerCache != nil {
				objects = append(objects, tc.claimInInformerCache)
			}

			fakeKubeClient := createTestClient(objects...)
			if tc.getReactor != nil {
				fakeKubeClient.PrependReactor("get", "resourceclaims", tc.getReactor)
			}

			informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
			ec, err := newControllerWithFeatures(tCtx.Logger(), fakeKubeClient,
				informerFactory.Core().V1().Pods(),
				informerFactory.Scheduling().V1beta1().PodGroups(),
				informerFactory.Resource().V1().ResourceClaims(),
				informerFactory.Resource().V1().ResourceClaimTemplates(),
				controllerFeatures{})
			tCtx.ExpectNoError(err, "creating controller")

			// 等待 informer 缓存同步完成。
			informerFactory.Start(tCtx.Done())
			stopInformers := func() {
				tCtx.Cancel("stopping informers")
				informerFactory.Shutdown()
			}
			defer stopInformers()
			informerFactory.WaitForCacheSync(tCtx.Done())

			// 加入只存在于变更缓存中的声明。
			if claim := tc.claimInMutationCache; claim != nil {
				ec.claimCache.Mutation(claim)
			}

			// 模拟竞态：停止 informer 后再写入控制器尚不知晓的对象。
			stopInformers()
			if claim := tc.claimInAPIServer; claim != nil {
				_, err := fakeKubeClient.ResourceV1().ResourceClaims(claim.Namespace).Create(tCtx, claim, metav1.CreateOptions{})
				if err != nil {
					tCtx.Fatalf("unexpected error while creating claim: %v", err)
				}
			}

			checkOwner := alwaysOwned
			if tc.invalidOwner {
				checkOwner = neverOwned
			}

			exists, err := ec.claimExists(tCtx, testNamespace, templatedTestClaim.Name, checkOwner)
			if tc.wantErr {
				assert.Error(tCtx, err)
			} else {
				assert.NoError(tCtx, err)
			}
			assert.Equal(tCtx, tc.wantExists, exists)
		})
	}
}

// TestEventHandlers 是 testEventHandlers 的入口。
func TestEventHandlers(t *testing.T) { testEventHandlers(ktesting.Init(t)) }

// testEventHandlers 验证各类资源事件是否正确转换为工作队列键，并更新索引与指标。
func testEventHandlers(tCtx ktesting.TContext) {
	// object 同时满足 runtime.Object 与 metav1.Object，便于统一处理事件对象。
	type object interface {
		runtime.Object
		metav1.Object
	}

	modifiedClaim := testClaim.DeepCopy()
	modifiedClaim.Labels = map[string]string{"foo": "bar"}

	modifiedClaimAdminAccess := templatedTestClaimWithAdmin.DeepCopy()
	modifiedClaim.Labels = map[string]string{"foo": "bar"}

	otherClaimAllocated := testClaimAllocated.DeepCopy()
	otherClaimAllocated.Name += "2"
	otherClaimKey := testClaimKey + "2"

	otherClaimAllocatedAdminAccess := templatedTestClaimWithAdminAllocated.DeepCopy()
	otherClaimAllocatedAdminAccess.Name += "2"

	templatedTestClaimKey := testClaimKey + "--1"
	templatedOtherClaimKey := templatedTestClaimKey + "2"
	extendedResourceTemplatedClaimKey := claimKeyPrefix + testNamespace + "/" + testPodName + "-extended-resources--1"

	otherNSTemplate := makeTemplate(templateName, otherNamespace, nil)

	otherNSPod := makePod("fake-1", otherNamespace, "uidpod2", *makePodResourceClaim(podResourceClaimName, templateName))
	otherNSPodKey := podKeyPrefix + otherNamespace + "/" + otherNSPod.Name

	extendedResourceClaimName := "test-extended-claim"
	podWithExtendedResourceClaim := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testPodName, Namespace: testNamespace},
		Spec:       v1.PodSpec{},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
				ResourceClaimName: extendedResourceClaimName,
			},
		},
	}

	completedPodWithExtendedResourceClaim := podWithExtendedResourceClaim.DeepCopy()
	completedPodWithExtendedResourceClaim.Status.Phase = v1.PodSucceeded

	completedPodWithRegularAndExtendedResourceClaim := podWithExtendedResourceClaim.DeepCopy()
	completedPodWithRegularAndExtendedResourceClaim.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "regular-claim"}}
	completedPodWithRegularAndExtendedResourceClaim.Status.Phase = v1.PodSucceeded

	failedPodWithExtendedResourceClaim := podWithExtendedResourceClaim.DeepCopy()
	failedPodWithExtendedResourceClaim.Status.Phase = v1.PodFailed

	// 扩展资源声明的队列键，由 Pod 状态中的名称推导。
	extendedResourceClaimKey := claimKeyPrefix + testNamespace + "/" + extendedResourceClaimName

	tests := map[string]struct {
		featureCombinations []controllerFeatures
		initialObjects      []runtime.Object
		createObjects       []object
		updateObjects       []object
		deleteObjects       []object

		// expectedKeys 是期望进入队列的键；expectedMetrics 是期望的数量指标。
		expectedKeys    []string
		expectedMetrics map[controllermetrics.NumResourceClaimLabels]float64

		// 该索引按模板记录 Pod，仅部分用例需要校验。
		expectedIndexedPodsByResourceClaimTemplate []string
	}{
		"nothing": {},
		"new-claim": {
			createObjects: []object{testClaim},
			expectedKeys:  []string{testClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "false"}: 1,
			},
		},
		"update-claim": {
			initialObjects: []runtime.Object{testClaim},
			updateObjects:  []object{modifiedClaim},
			expectedKeys:   []string{testClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "false"}: 1,
			},
		},
		"allocate-claim": {
			initialObjects: []runtime.Object{testClaim},
			updateObjects:  []object{testClaimAllocated},
			expectedKeys:   []string{testClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "false"}: 1,
			},
		},
		"allocate-another-claim": {
			initialObjects: []runtime.Object{testClaimAllocated},
			createObjects:  []object{otherClaimAllocated},
			expectedKeys:   []string{otherClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "false"}: 2,
			},
		},
		"deallocate-claim": {
			initialObjects: []runtime.Object{testClaimAllocated, otherClaimAllocated},
			updateObjects:  []object{testClaim},
			expectedKeys:   []string{testClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "false"}:  1,
				{Allocated: "false", AdminAccess: "false"}: 1,
			},
		},
		"delete-deallocated-claim": {
			initialObjects: []runtime.Object{testClaim, otherClaimAllocated},
			deleteObjects:  []object{testClaim},
			expectedKeys:   []string{},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "false"}: 1,
			},
		},
		"delete-allocated-claim": {
			initialObjects:  []runtime.Object{otherClaimAllocated},
			deleteObjects:   []object{otherClaimAllocated},
			expectedKeys:    []string{},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{},
		},
		"new-claim-admin-access": {
			createObjects: []object{templatedTestClaimWithAdmin},
			expectedKeys:  []string{templatedTestClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "true", Source: "resource_claim_template"}: 1,
			},
		},
		"update-claim-admin-access": {
			initialObjects: []runtime.Object{templatedTestClaimWithAdmin},
			updateObjects:  []object{modifiedClaimAdminAccess},
			expectedKeys:   []string{templatedTestClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "true", Source: "resource_claim_template"}: 1,
			},
		},
		"allocate-claim-admin-access": {
			initialObjects: []runtime.Object{modifiedClaimAdminAccess},
			updateObjects:  []object{templatedTestClaimWithAdminAllocated},
			expectedKeys:   []string{templatedTestClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "true", Source: "resource_claim_template"}: 1,
			},
		},
		"allocate-another-claim-admin-access": {
			initialObjects: []runtime.Object{templatedTestClaimWithAdminAllocated},
			createObjects:  []object{otherClaimAllocatedAdminAccess},
			expectedKeys:   []string{templatedOtherClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "true", Source: "resource_claim_template"}: 2,
			},
		},
		"deallocate-claim-admin-access": {
			initialObjects: []runtime.Object{templatedTestClaimWithAdminAllocated, otherClaimAllocatedAdminAccess},
			updateObjects:  []object{templatedTestClaimWithAdmin},
			expectedKeys:   []string{templatedTestClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "true", Source: "resource_claim_template"}:  1,
				{Allocated: "false", AdminAccess: "true", Source: "resource_claim_template"}: 1,
			},
		},
		"delete-deallocated-claim-admin-access": {
			initialObjects: []runtime.Object{templatedTestClaimWithAdmin, otherClaimAllocatedAdminAccess},
			deleteObjects:  []object{templatedTestClaimWithAdmin},
			expectedKeys:   []string{},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "true", Source: "resource_claim_template"}: 1,
			},
		},
		"delete-allocated-claim-admin-access": {
			initialObjects:  []runtime.Object{otherClaimAllocatedAdminAccess},
			deleteObjects:   []object{otherClaimAllocatedAdminAccess},
			expectedKeys:    []string{},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{},
		},
		"new-claim-extended-resources": {
			createObjects: []object{extendedTestClaim},
			expectedKeys:  []string{extendedResourceTemplatedClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "false", Source: "extended_resource"}: 1,
			},
		},
		"allocate-claim-extended-resources": {
			initialObjects: []runtime.Object{extendedTestClaim},
			updateObjects:  []object{extendedTestClaimAllocated},
			expectedKeys:   []string{extendedResourceTemplatedClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "true", AdminAccess: "false", Source: "extended_resource"}: 1,
			},
		},
		"deallocate-claim-extended-resources": {
			initialObjects: []runtime.Object{extendedTestClaimAllocated},
			updateObjects:  []object{extendedTestClaim},
			expectedKeys:   []string{extendedResourceTemplatedClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "false", Source: "extended_resource"}: 1,
			},
		},
		"delete-claim-extended-resources": {
			initialObjects:  []runtime.Object{extendedTestClaimAllocated},
			deleteObjects:   []object{extendedTestClaimAllocated},
			expectedKeys:    []string{},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{},
		},
		"new-pods": {
			createObjects: []object{testPodWithResource, otherNSPod},
			expectedKeys:  []string{testPodKey, otherNSPodKey},
			expectedIndexedPodsByResourceClaimTemplate: []string{
				testNamespace + "/" + templateName,
				otherNamespace + "/" + templateName,
			},
		},
		"new-template-for-pod": {
			initialObjects: []runtime.Object{testPodWithResource, otherNSPod},
			createObjects:  []object{template},
			expectedKeys:   []string{testPodKey},
			expectedIndexedPodsByResourceClaimTemplate: []string{
				testNamespace + "/" + templateName,
				otherNamespace + "/" + templateName,
			},
		},
		// 模板位于其他命名空间，不应触发该 Pod 入队。
		"new-template-for-pod-other-namespace": {
			initialObjects: []runtime.Object{testPodWithResource, otherNSPod, template},
			createObjects:  []object{otherNSTemplate},
			expectedKeys:   []string{otherNSPodKey},
			expectedIndexedPodsByResourceClaimTemplate: []string{
				testNamespace + "/" + templateName,
				otherNamespace + "/" + templateName,
			},
		},
		"pod-without-resource-claims": {
			createObjects: []object{testPod},
			expectedKeys:  []string{},
		},
		// 运行中的 Pod 无需入队；完成或失败后才需要清理扩展资源声明。
		"running-pod-with-extended-resource-claim": {
			createObjects: []object{podWithExtendedResourceClaim},
			expectedKeys:  []string{},
		},
		"completed-pod-with-extended-resource-claim": {
			createObjects: []object{completedPodWithExtendedResourceClaim},
			expectedKeys:  []string{extendedResourceClaimKey},
		},
		"faled-pod-with-extended-resource-claim": {
			createObjects: []object{failedPodWithExtendedResourceClaim},
			expectedKeys:  []string{extendedResourceClaimKey},
		},
		"delete-pod-with-extended-resource-claim": {
			initialObjects: []runtime.Object{podWithExtendedResourceClaim},
			deleteObjects:  []object{podWithExtendedResourceClaim},
			expectedKeys:   []string{extendedResourceClaimKey},
		},
		"completed-pod-with-regular-and-extended-resource-claim": {
			createObjects: []object{completedPodWithRegularAndExtendedResourceClaim},
			expectedKeys:  []string{extendedResourceClaimKey, testPodKey},
		},
		"new-podgroup-feature-disabled": {
			featureCombinations: workloadResourceClaimsDisabled,
			createObjects:       []object{testPodGroupWithResourceInStatus},
			expectedKeys:        []string{},
		},
		"new-podgroup": {
			featureCombinations: workloadResourceClaimsEnabled,
			createObjects:       []object{testPodGroupWithResourceInStatus},
			expectedKeys:        []string{testPodGroupKey},
		},
		"new-podgroup-templated-claim-already-exists": {
			featureCombinations: workloadResourceClaimsEnabled,
			initialObjects:      []runtime.Object{testPodGroupClaim},
			createObjects:       []object{testPodGroupWithResourceInStatus},
			expectedKeys:        []string{},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "false"}: 1,
			},
		},
		"new-templated-claim-for-podgroup": {
			featureCombinations: workloadResourceClaimsEnabled,
			initialObjects:      []runtime.Object{testPodGroupWithResourceInStatus},
			createObjects:       []object{testPodGroupClaim},
			expectedKeys:        []string{testClaimKey},
			expectedMetrics: map[controllermetrics.NumResourceClaimLabels]float64{
				{Allocated: "false", AdminAccess: "false"}: 1,
			},
		},
		"new-template-for-podgroup": {
			featureCombinations: workloadResourceClaimsEnabled,
			initialObjects:      []runtime.Object{testPodGroupWithResource},
			createObjects:       []object{template},
			expectedKeys:        []string{testPodGroupKey},
		},
		// 状态更新后只有同组且使用该声明的 Pod 需要入队。
		"podgroup-claim-status-update": {
			featureCombinations: workloadResourceClaimsEnabled,
			initialObjects: []runtime.Object{
				testPodGroupWithResource,
				podInPodGroup(testPodWithPodGroupResource, testPodName+"-1", testPodGroupName),
				podInPodGroup(testPodWithPodGroupResource, testPodName+"-2", testPodGroupName),
				podInPodGroup(testPodWithPodGroupResource, testPodName+"-3", testPodGroupName+"-2"),
				podInPodGroup(testPod, testPodName+"-4", testPodGroupName),
			},
			updateObjects: []object{testPodGroupWithResourceInStatus},
			expectedKeys: []string{
				testPodGroupKey,
				testPodKey + "-1",
				testPodKey + "-2",
			},
			expectedIndexedPodsByResourceClaimTemplate: []string{testNamespace + "/" + templateName},
		},
		"podgroup-claim-status-update-feature-disabled": {
			featureCombinations: workloadResourceClaimsDisabled,
			initialObjects: []runtime.Object{
				testPodGroupWithResource,
				podInPodGroup(testPodWithPodGroupResource, testPodName+"-1", testPodGroupName),
				podInPodGroup(testPodWithPodGroupResource, testPodName+"-2", testPodGroupName),
				podInPodGroup(testPodWithPodGroupResource, testPodName+"-3", testPodGroupName+"-2"),
				podInPodGroup(testPod, testPodName+"-4", testPodGroupName),
			},
			updateObjects: []object{testPodGroupWithResourceInStatus},
			expectedKeys:  []string{},
			expectedIndexedPodsByResourceClaimTemplate: []string{testNamespace + "/" + templateName},
		},
	}
	for name, test := range tests {
		run := func(tCtx ktesting.TContext, features controllerFeatures) {
			fakeKubeClient := createTestClient(test.initialObjects...)
			informerFactory := informers.NewSharedInformerFactory(fakeKubeClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			podGroupInformer := informerFactory.Scheduling().V1beta1().PodGroups()
			claimInformer := informerFactory.Resource().V1().ResourceClaims()
			templateInformer := informerFactory.Resource().V1().ResourceClaimTemplates()
			setupMetrics()

			ec, err := newControllerWithFeatures(tCtx.Logger(), fakeKubeClient, podInformer, podGroupInformer, claimInformer, templateInformer, features)
			tCtx.ExpectNoError(err, "creating ephemeral controller")
			tCtx.Cleanup(ec.queue.ShutDown)

			informerFactory.Start(tCtx.Done())
			stopInformers := func() {
				tCtx.Cancel("stopping informers")
				informerFactory.Shutdown()
			}
			tCtx.Cleanup(stopInformers)

			// drainQueue 等待事件处理完成并取出队列中的所有键。
			drainQueue := func() []string {
				tCtx.Wait()
				actualKeys := []string{}
				for ec.queue.Len() > 0 {
					actual, shuttingDown := ec.queue.Get()
					tCtx.Expect(shuttingDown).To(gomega.BeFalseBecause("workqueue should not be shutting down"))
					actualKeys = append(actualKeys, actual)
					ec.queue.Forget(actual)
					ec.queue.Done(actual)
				}
				return actualKeys
			}
			// gvr 把测试对象映射到 fake tracker 所需的 GVR。
			gvr := func(obj metav1.Object) schema.GroupVersionResource {
				switch obj.(type) {
				case *v1.Pod:
					return v1.SchemeGroupVersion.WithResource("pods")
				case *schedulingapi.PodGroup:
					return schedulingapi.SchemeGroupVersion.WithResource("podgroups")
				case *resourceapi.ResourceClaim:
					return resourceapi.SchemeGroupVersion.WithResource("resourceclaims")
				case *resourceapi.ResourceClaimTemplate:
					return resourceapi.SchemeGroupVersion.WithResource("resourceclaimtemplates")
				}
				tCtx.Fatalf("invalid object type %T", obj)
				return schema.GroupVersionResource{}
			}

			// 初始对象入队不在校验范围内，只关注后续操作产生的键。
			_ = drainQueue()

			for _, object := range test.createObjects {
				err := fakeKubeClient.Tracker().Create(gvr(object), object, object.GetNamespace(), metav1.CreateOptions{})
				tCtx.ExpectNoError(err)
			}
			for _, object := range test.updateObjects {
				err := fakeKubeClient.Tracker().Update(gvr(object), object, object.GetNamespace(), metav1.UpdateOptions{})
				tCtx.ExpectNoError(err)
			}
			for _, object := range test.deleteObjects {
				err := fakeKubeClient.Tracker().Delete(gvr(object), object.GetNamespace(), object.GetName(), metav1.DeleteOptions{})
				tCtx.ExpectNoError(err)
			}

			actualKeys := drainQueue()
			tCtx.Expect(actualKeys).To(gomega.ConsistOf(test.expectedKeys), "Workqueue does not contain expected elements")

			em := newNumMetrics(claimInformer.Lister())
			for labels, val := range test.expectedMetrics {
				em = em.withUpdates(labels, val)
			}
			em.verify(tCtx)

			actualIndexedPodsByResourceClaimTemplate := ec.podIndexer.ListIndexFuncValues(podResourceClaimTemplateIndex)
			tCtx.Expect(actualIndexedPodsByResourceClaimTemplate).To(gomega.ConsistOf(test.expectedIndexedPodsByResourceClaimTemplate), "expected Pods were not indexed by ResourceClaimTemplate")
		}
		if len(test.featureCombinations) == 0 {
			test.featureCombinations = allPossibleFeatures
		}
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			for _, features := range test.featureCombinations {
				tCtx.SyncTest(features.String(), func(tCtx ktesting.TContext) {
					run(tCtx, features)
				})
			}
		})
	}
}

// TestWorkqueue 是 testWorkqueue 的入口。
func TestWorkqueue(t *testing.T) { testWorkqueue(ktesting.Init(t)) }

// testWorkqueue 验证 processNextWorkItem 对成功、可重试错误、不可重试错误及关闭的处理。
func testWorkqueue(tCtx ktesting.TContext) {
	tests := map[string]struct {
		shutdown       bool
		syncHandlerErr error
		expectStopLoop bool
		expectSynced   bool
		expectRequeue  bool
	}{
		"no-error": {
			syncHandlerErr: nil,
			expectSynced:   true,
		},
		"retryable-error": {
			syncHandlerErr: fmt.Errorf("will retry"),
			expectSynced:   true,
			expectRequeue:  true,
		},
		"nonretryable-error": {
			syncHandlerErr: nonRetryableError{fmt.Errorf("will not retry")},
			expectSynced:   true,
			expectRequeue:  false,
		},
		"wrapped-nonretryable-error": {
			syncHandlerErr: fmt.Errorf("%w", nonRetryableError{fmt.Errorf("will not retry")}),
			expectSynced:   true,
			expectRequeue:  false,
		},
		"shutdown": {
			shutdown:       true,
			expectSynced:   false,
			expectStopLoop: true,
		},
	}
	for name, test := range tests {
		tCtx.SyncTest(name, func(tCtx ktesting.TContext) {
			// 默认的 [utilruntime.ErrorHandlers] 依赖 [time]，但在 synctest bubble 建立之前
			// 就已初始化。因此错误处理器被调用时，默认限速会按 *fake* 时间与 *actual*
			// 时间之差休眠；workqueue 内部的周期性计算会因此空转 26 年(!)的虚拟时间，
			// 虽然最终能结束，但会白白消耗真实 CPU。
			oldErrorHandlers := utilruntime.ErrorHandlers
			tCtx.Cleanup(func() {
				utilruntime.ErrorHandlers = oldErrorHandlers
			})
			utilruntime.ErrorHandlers = []utilruntime.ErrorHandler{
				func(ctx context.Context, err error, msg string, keysAndValues ...any) {
					klog.FromContext(ctx).Error(err, msg, keysAndValues...)
				},
			}

			rateLimitDelay := 1 * time.Second
			ec := &Controller{
				queue: workqueue.NewTypedRateLimitingQueueWithConfig(
					&staticRateLimiter[string]{delay: rateLimitDelay, requeues: map[string]int{}},
					workqueue.TypedRateLimitingQueueConfig[string]{Name: "resource_claim"},
				),
			}
			tCtx.Cleanup(ec.queue.ShutDown)

			synced := false
			ec.syncHandlerFunc = func(ctx context.Context, s string) error {
				synced = true
				return test.syncHandlerErr
			}

			if test.shutdown {
				ec.queue.ShutDown()
			}

			var key string
			ec.queue.Add(key)

			actual := ec.processNextWorkItem(tCtx)
			tCtx.Expect(actual).To(gomega.Equal(!test.expectStopLoop))

			if test.expectSynced {
				tCtx.Expect(synced).To(gomega.BeTrueBecause("sync handler should have run but did not"))
			} else {
				tCtx.Expect(synced).To(gomega.BeFalseBecause("sync handler should not have run but did"))
			}
			time.Sleep(rateLimitDelay)
			// 延迟的 Add 与检查队列长度存在竞态，先等待 Add 完成再断言长度。
			tCtx.Wait()
			if test.expectRequeue {
				tCtx.Expect(ec.queue.Len()).To(gomega.Equal(1), "key should have been queued")
			} else {
				tCtx.Expect(ec.queue.Len()).To(gomega.Equal(0), "no key should have been queued")
			}
		})
	}
}

// TestGetAdminAccessMetricLabel 验证 getAdminAccessMetricLabel 能从设备请求中
// 正确提取管理员访问标签：任一请求为 true 即返回 "true"。
func TestGetAdminAccessMetricLabel(t *testing.T) {
	tests := []struct {
		name  string
		claim *resourceapi.ResourceClaim
		want  string
	}{
		{
			name:  "nil-claim",
			claim: nil,
			want:  "false",
		},
		{
			name: "no-requests",
			claim: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: nil,
					},
				},
			},
			want: "false",
		},
		{
			name: "admin-access-false",
			claim: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Exactly: &resourceapi.ExactDeviceRequest{
									AdminAccess: new(false),
								},
							},
						},
					},
				},
			},
			want: "false",
		},
		{
			name: "admin-access-true",
			claim: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Exactly: &resourceapi.ExactDeviceRequest{
									AdminAccess: new(true),
								},
							},
						},
					},
				},
			},
			want: "true",
		},
		{
			// 优先列表目前只检查 Exactly 请求，应返回 "false"。
			name: "prioritized-list",
			claim: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								FirstAvailable: []resourceapi.DeviceSubRequest{{}},
							},
						},
					},
				},
			},
			want: "false",
		},
		{
			name: "one-of-multiple-requests-with-admin-access-true",
			claim: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Exactly: &resourceapi.ExactDeviceRequest{
									AdminAccess: new(false),
								},
							},
							{
								Exactly: &resourceapi.ExactDeviceRequest{
									AdminAccess: new(true),
								},
							},
						},
					},
				},
			},
			want: "true",
		},
		{
			name: "multiple-requests-admin-access-false-or-nil",
			claim: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Exactly: &resourceapi.ExactDeviceRequest{
									AdminAccess: nil,
								},
							},
							{
								Exactly: &resourceapi.ExactDeviceRequest{
									AdminAccess: new(false),
								},
							},
						},
					},
				},
			},
			want: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getAdminAccessMetricLabel(tt.claim)
			if got != tt.want {
				t.Errorf("GetAdminAccessMetricLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

// makeClaim 构造一个最小的 ResourceClaim，可选地设置 owner 引用。
func makeClaim(name, namespace string, owner *metav1.OwnerReference) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if owner != nil {
		claim.OwnerReferences = []metav1.OwnerReference{*owner}
	}

	return claim
}

// makeTemplatedClaim 模拟由模板生成的声明：GenerateName 与名称中的计数对应，
// 并带上 PodResourceClaim 注解。
func makeTemplatedClaim(podClaimName, generateName, namespace string, createCounter int, owner *metav1.OwnerReference, adminAccess *bool) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:         fmt.Sprintf("%s-%d", generateName, createCounter),
			GenerateName: generateName,
			Namespace:    namespace,
			Annotations:  map[string]string{resourceapi.PodResourceClaimAnnotation: podClaimName},
		},
	}
	if owner != nil {
		claim.OwnerReferences = []metav1.OwnerReference{*owner}
	}
	if adminAccess != nil {
		claim.Spec = resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: "req-0",
						Exactly: &resourceapi.ExactDeviceRequest{
							DeviceClassName: "class",
							AdminAccess:     adminAccess,
						},
					},
				},
			},
		}
	}

	return claim
}

// makeExtendedResourceClaim 构造扩展资源声明，其来源由注解标识而不是模板。
func makeExtendedResourceClaim(podName, namespace string, createCounter int, owner *metav1.OwnerReference) *resourceapi.ResourceClaim {
	generateName := podName + "-extended-resources-"
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:         fmt.Sprintf("%s-%d", generateName, createCounter),
			GenerateName: generateName,
			Namespace:    namespace,
			Annotations:  map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"},
		},
	}
	if owner != nil {
		claim.OwnerReferences = []metav1.OwnerReference{*owner}
	}

	return claim
}

// allocateClaim 返回带有分配结果的声明副本。
func allocateClaim(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	claim = claim.DeepCopy()
	claim.Status.Allocation = &resourceapi.AllocationResult{}
	return claim
}

// structuredParameters 返回带有结构化参数 finalizer 的声明副本；控制器据此判断
// 分配是否由自己完成。
func structuredParameters(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	claim = claim.DeepCopy()
	// 对控制器而言，带有该 finalizer 即表示声明由它分配；测试里无需真正填写分配结果。
	claim.Finalizers = append(claim.Finalizers, resourceapi.Finalizer)
	return claim
}

// reserveClaim 返回追加了指定 Pod 或 PodGroup 消费者引用的声明副本。
func reserveClaim(claim *resourceapi.ResourceClaim, obj metav1.Object) *resourceapi.ResourceClaim {
	claim = claim.DeepCopy()
	var apiGroup, resource string
	switch obj.(type) {
	case *v1.Pod:
		apiGroup = v1.GroupName
		resource = "pods"
	case *schedulingapi.PodGroup:
		apiGroup = schedulingapi.GroupName
		resource = "podgroups"
	default:
		panic(fmt.Sprintf("invalid type: %T", obj))
	}
	claim.Status.ReservedFor = append(claim.Status.ReservedFor,
		resourceapi.ResourceClaimConsumerReference{
			APIGroup: apiGroup,
			Resource: resource,
			Name:     obj.GetName(),
			UID:      obj.GetUID(),
		},
	)
	return claim
}

// makePodResourceClaim 构造一个引用模板的 PodResourceClaim。
func makePodResourceClaim(name, templateName string) *v1.PodResourceClaim {
	return &v1.PodResourceClaim{
		Name:                      name,
		ResourceClaimTemplateName: &templateName,
	}
}

// makePod 构造带有指定名称、命名空间、UID 和资源声明列表的 Pod。
func makePod(name, namespace string, uid types.UID, podClaims ...v1.PodResourceClaim) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: uid},
		Spec: v1.PodSpec{
			ResourceClaims: podClaims,
		},
	}

	return pod
}

// makePodGroupResourceClaim 构造一个引用模板的 PodGroupResourceClaim。
func makePodGroupResourceClaim(name, templateName string) *schedulingapi.PodGroupResourceClaim {
	return &schedulingapi.PodGroupResourceClaim{
		Name:                      name,
		ResourceClaimTemplateName: &templateName,
	}
}

// makePodGroup 构造带有指定名称、命名空间、UID 和资源声明列表的 PodGroup。
func makePodGroup(name, namespace string, uid types.UID, podGroupClaims ...schedulingapi.PodGroupResourceClaim) *schedulingapi.PodGroup {
	podGroup := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: uid},
		Spec: schedulingapi.PodGroupSpec{
			ResourceClaims: podGroupClaims,
		},
	}

	return podGroup
}

// podInPodGroup 复制 Pod，重命名并把其加入指定的 PodGroup。
func podInPodGroup(pod *v1.Pod, podName, podGroupName string) *v1.Pod {
	pod = pod.DeepCopy()
	pod.Name = podName
	pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
		PodGroupName: &podGroupName,
	}
	return pod
}

// makeTemplate 构造 ResourceClaimTemplate；adminAccess 非 nil 时写入对应的设备请求。
func makeTemplate(name, namespace string, adminAccess *bool) *resourceapi.ResourceClaimTemplate {
	template := &resourceapi.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if adminAccess != nil {
		template.Spec = resourceapi.ResourceClaimTemplateSpec{
			Spec: resourceapi.ResourceClaimSpec{
				Devices: resourceapi.DeviceClaim{
					Requests: []resourceapi.DeviceRequest{
						{
							Name: "req-0",
							Exactly: &resourceapi.ExactDeviceRequest{
								DeviceClassName: "class",
								AdminAccess:     adminAccess,
							},
						},
					},
				},
			},
		}
	}
	return template
}

// podKey 返回 Pod 对应的工作队列键。
func podKey(pod *v1.Pod) string {
	return podKeyPrefix + pod.Namespace + "/" + pod.Name
}

// podGroupKey 返回 PodGroup 对应的工作队列键。
func podGroupKey(podGroup *schedulingapi.PodGroup) string {
	return podGroupKeyPrefix + podGroup.Namespace + "/" + podGroup.Name
}

// claimKey 返回 ResourceClaim 对应的工作队列键。
func claimKey(claim *resourceapi.ResourceClaim) string {
	return claimKeyPrefix + claim.Namespace + "/" + claim.Name
}

// makeOwnerReference 为 Pod 或 PodGroup 构造控制器 owner 引用。
func makeOwnerReference(obj metav1.Object, isController bool) *metav1.OwnerReference {
	var apiVersion, kind string
	switch obj.(type) {
	case *v1.Pod:
		apiVersion = v1.SchemeGroupVersion.String()
		kind = "Pod"
	case *schedulingapi.PodGroup:
		apiVersion = schedulingapi.SchemeGroupVersion.String()
		kind = "PodGroup"
	default:
		panic(fmt.Sprintf("invalid type %T", obj))
	}
	return &metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       obj.GetName(),
		UID:        obj.GetUID(),
		Controller: &isController,
	}
}

// normalizeClaims 按命名空间和名称排序声明，并把空的 ReservedFor 归一化为 nil，
// 便于比较期望值与实际值。
func normalizeClaims(claims []resourceapi.ResourceClaim) []resourceapi.ResourceClaim {
	sort.Slice(claims, func(i, j int) bool {
		if claims[i].Namespace < claims[j].Namespace {
			return true
		}
		if claims[i].Namespace > claims[j].Namespace {
			return false
		}
		return claims[i].Name < claims[j].Name
	})
	for i := range claims {
		if len(claims[i].Status.ReservedFor) == 0 {
			claims[i].Status.ReservedFor = nil
		}
	}
	return claims
}

// createTestClient 创建 fake 客户端，并注册 GenerateName 所需的 reactor。
func createTestClient(objects ...runtime.Object) *fake.Clientset {
	fakeClient := fake.NewSimpleClientset(objects...)
	fakeClient.PrependReactor("create", "resourceclaims", createResourceClaimReactor())
	return fakeClient
}

// createResourceClaimReactor 为 fake 客户端补齐 GenerateName 行为：创建时按计数器
// 生成真实名称。通过 PrependReactor 注册到 fake 客户端。
func createResourceClaimReactor() func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
	nameCounter := 1
	var mutex sync.Mutex
	return func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		mutex.Lock()
		defer mutex.Unlock()
		claim := action.(k8stesting.CreateAction).GetObject().(*resourceapi.ResourceClaim)
		if claim.Name == "" && claim.GenerateName != "" {
			claim.Name = fmt.Sprintf("%s-%d", claim.GenerateName, nameCounter)
		}
		nameCounter++
		return false, nil, nil
	}
}

// numMetrics 保存期望的 ResourceClaim 数量指标以及用于实时采集的 lister。
type numMetrics struct {
	// metrics 是按标签聚合的期望值；lister 用于采集当前指标。
	metrics map[controllermetrics.NumResourceClaimLabels]float64
	lister  resourcelisters.ResourceClaimLister
}

// getNumMetric 通过独立注册表采集数量指标，避免与全局 collector 冲突。
func getNumMetric(lister resourcelisters.ResourceClaimLister, logger klog.Logger) (em numMetrics, err error) {
	if lister == nil {
		return numMetrics{}, nil
	}

	// 每次调用都创建新的 collector，避免重复注册冲突。
	freshCollector := newCustomCollector(lister, getAdminAccessMetricLabel, logger)
	testRegistry := metrics.NewKubeRegistry()
	testRegistry.CustomMustRegister(freshCollector)

	gatheredMetrics, err := testRegistry.Gather()
	if err != nil {
		return numMetrics{}, fmt.Errorf("failed to gather metrics: %w", err)
	}

	metricName := "dynamic_resource_allocation_resource_claims"

	em = newNumMetrics(lister)

	for _, mf := range gatheredMetrics {
		if mf.GetName() != metricName {
			continue
		}
		for _, metric := range mf.GetMetric() {
			labels := make(map[string]string)
			for _, labelPair := range metric.GetLabel() {
				labels[labelPair.GetName()] = labelPair.GetValue()
			}

			allocated := labels["allocated"]
			adminAccess := labels["admin_access"]
			source := labels["source"]
			value := metric.GetGauge().GetValue()

			em.metrics[controllermetrics.NumResourceClaimLabels{
				Allocated:   allocated,
				AdminAccess: adminAccess,
				Source:      source,
			}] = value
		}
	}

	return em, nil
}

// verify 采集当前指标并与期望值比较。
func (em numMetrics) verify(tCtx ktesting.TContext) {
	tCtx.Helper()
	result, err := getNumMetric(em.lister, tCtx.Logger())
	tCtx.ExpectNoError(err)
	tCtx.Expect(result.metrics).To(gomega.Equal(em.metrics))
}

// claimCreateMetricLabels 是资源声明创建计数器的一组标签。
type claimCreateMetricLabels struct {
	// status 为 success/failure；adminAccess、ownerGroup、ownerKind 标识声明来源。
	status      string
	adminAccess string
	ownerGroup  string
	ownerKind   string
}

// claimCreateMetrics 按标签聚合资源声明创建计数器的期望值。
type claimCreateMetrics map[claimCreateMetricLabels]float64

// expectMetrics 从全局注册表采集创建计数器，并与期望值比较。
func expectMetrics(tCtx ktesting.TContext, em claimCreateMetrics) {
	tCtx.Helper()

	gatheredMetrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		tCtx.Errorf("failed to gather metrics: %v", err)
		return
	}

	actualMetrics := claimCreateMetrics{}
	for _, mf := range gatheredMetrics {
		if mf.GetName() != "dynamic_resource_allocation_resourceclaim_creates_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			labels := make(map[string]string)
			for _, labelPair := range metric.GetLabel() {
				labels[labelPair.GetName()] = labelPair.GetValue()
			}

			actualMetrics[claimCreateMetricLabels{
				status:      labels["status"],
				adminAccess: labels["admin_access"],
				ownerGroup:  labels["owner_api_group"],
				ownerKind:   labels["owner_api_kind"],
			}] = metric.GetCounter().GetValue()
		}
	}

	tCtx.Expect(actualMetrics).To(gomega.Equal(em))
}

// setupMetrics 打开指标测试模式并重置每个用例的创建计数器。
func setupMetrics() {
	// 开启测试模式，避免向全局注册表注册自定义 collector。
	controllermetrics.SetTestMode(true)

	// 计数器由控制器自行注册，每个用例前都需要重置。
	resourceclaimmetrics.ResourceClaimCreate.Reset()
}

// newNumMetrics 构造所有标签组合、计数均为 0 的期望指标。
func newNumMetrics(lister resourcelisters.ResourceClaimLister) numMetrics {
	metrics := make(map[controllermetrics.NumResourceClaimLabels]float64)
	for _, allocated := range []string{"false", "true"} {
		for _, adminAccess := range []string{"false", "true"} {
			for _, source := range []string{"", "extended_resource", "resource_claim_template"} {
				metrics[controllermetrics.NumResourceClaimLabels{
					Allocated:   allocated,
					AdminAccess: adminAccess,
					Source:      source,
				}] = 0
			}
		}
	}
	return numMetrics{
		metrics: metrics,
		lister:  lister,
	}
}

// withUpdates 返回叠加增量后的新期望指标。
func (em numMetrics) withUpdates(rcLabels controllermetrics.NumResourceClaimLabels, n float64) numMetrics {
	em.metrics[rcLabels] += n
	return numMetrics{
		metrics: em.metrics,
		lister:  em.lister,
	}
}

// staticRateLimiter 以固定时长延迟条目入队，使测试能够精确知道限速条目
// 何时会被重新加入队列。
type staticRateLimiter[T comparable] struct {
	// delay 是每次重试前固定的等待时间。
	delay time.Duration

	// requeuesLock 保护并发访问的 requeues 映射。
	requeuesLock sync.Mutex
	// requeues 记录每个条目到目前为止的重试次数。
	requeues map[T]int
}

// Forget 实现 [workqueue.TypedRateLimiter]，清除指定条目的重试计数。
func (s *staticRateLimiter[T]) Forget(item T) {
	s.requeuesLock.Lock()
	defer s.requeuesLock.Unlock()
	delete(s.requeues, item)
}

// NumRequeues 实现 [workqueue.TypedRateLimiter]，返回指定条目的重试次数。
func (s *staticRateLimiter[T]) NumRequeues(item T) int {
	s.requeuesLock.Lock()
	defer s.requeuesLock.Unlock()
	return s.requeues[item]
}

// When 实现 [workqueue.TypedRateLimiter]，递增重试计数并返回固定延迟。
func (s *staticRateLimiter[T]) When(item T) time.Duration {
	s.requeuesLock.Lock()
	defer s.requeuesLock.Unlock()
	s.requeues[item]++
	return s.delay
}
