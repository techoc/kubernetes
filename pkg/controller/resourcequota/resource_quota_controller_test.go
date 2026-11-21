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

package resourcequota

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/quota/v1/install"
)

// getResourceList 按给定的 CPU 与内存字符串构造资源列表。
func getResourceList(cpu, memory string) v1.ResourceList {
	res := v1.ResourceList{}
	if cpu != "" {
		res[v1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		res[v1.ResourceMemory] = resource.MustParse(memory)
	}
	return res
}

// getResourceRequirements 用给定的 requests 与 limits 构造容器资源需求。
func getResourceRequirements(requests, limits v1.ResourceList) v1.ResourceRequirements {
	res := v1.ResourceRequirements{}
	res.Requests = requests
	res.Limits = limits
	return res
}

// mockDiscoveryFunc 返回空的发现结果，避免测试依赖真实 API 发现。
func mockDiscoveryFunc() ([]*metav1.APIResourceList, error) {
	return []*metav1.APIResourceList{}, nil
}

// mockListerForResourceFunc 按 GVR 返回预置 lister，缺失时报错以便定位配置遗漏。
func mockListerForResourceFunc(listersForResource map[schema.GroupVersionResource]cache.GenericLister) quota.ListerForResourceFunc {
	return func(gvr schema.GroupVersionResource) (cache.GenericLister, error) {
		lister, found := listersForResource[gvr]
		if !found {
			return nil, fmt.Errorf("no lister found for resource")
		}
		return lister, nil
	}
}

// newGenericLister 用给定对象构造带 namespace 索引的通用 lister。
func newGenericLister(groupResource schema.GroupResource, items []runtime.Object) cache.GenericLister {
	store := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{"namespace": cache.MetaNamespaceIndexFunc})
	for _, item := range items {
		store.Add(item)
	}
	return cache.NewGenericLister(store, groupResource)
}

// newErrorLister 返回一个列表与获取操作总是失败的 lister。
func newErrorLister() cache.GenericLister {
	return errorLister{}
}

// errorLister 用于模拟 lister 访问失败。
type errorLister struct {
}

// List 始终返回列表错误。
func (errorLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	return nil, fmt.Errorf("error listing")
}

// Get 始终返回获取错误。
func (errorLister) Get(name string) (runtime.Object, error) {
	return nil, fmt.Errorf("error getting")
}

// ByNamespace 返回同样会报错的 namespace lister。
func (errorLister) ByNamespace(namespace string) cache.GenericNamespaceLister {
	return errorLister{}
}

// quotaController 包装控制器与其停止通道，便于测试中启停 informer。
type quotaController struct {
	// Controller 嵌入的实际控制器。
	*Controller
	// stop 关闭后停止 informer。
	stop chan struct{}
}

// setupQuotaController 构造使用 fake 客户端与自定义 lister 的配额控制器。
func setupQuotaController(t *testing.T, kubeClient kubernetes.Interface, lister quota.ListerForResourceFunc, discoveryFunc NamespacedResourcesFunc) quotaController {
	informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
	quotaConfiguration, err := install.NewQuotaConfigurationForControllers(lister, informerFactory)
	if err != nil {
		t.Fatal(err)
	}
	alwaysStarted := make(chan struct{})
	close(alwaysStarted)
	resourceQuotaControllerOptions := &ControllerOptions{
		QuotaClient:               kubeClient.CoreV1(),
		ResourceQuotaInformer:     informerFactory.Core().V1().ResourceQuotas(),
		ResyncPeriod:              controller.NoResyncPeriodFunc,
		ReplenishmentResyncPeriod: controller.NoResyncPeriodFunc,
		IgnoredResourcesFunc:      quotaConfiguration.IgnoredResources,
		DiscoveryFunc:             discoveryFunc,
		Registry:                  generic.NewRegistry(quotaConfiguration.Evaluators()),
		InformersStarted:          alwaysStarted,
		InformerFactory:           informerFactory,
	}
	_, ctx := ktesting.NewTestContext(t)
	qc, err := NewController(ctx, resourceQuotaControllerOptions)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	informerFactory.Start(stop)
	return quotaController{qc, stop}
}

// newTestPods 构造两个运行中的 Burstable Pod 和一个失败的 Pod。
func newTestPods() []runtime.Object {
	return []runtime.Object{
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-running", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodRunning},
			Spec: v1.PodSpec{
				Volumes:    []v1.Volume{{Name: "vol"}},
				Containers: []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", ""))}},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-running-2", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodRunning},
			Spec: v1.PodSpec{
				Volumes:    []v1.Volume{{Name: "vol"}},
				Containers: []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", ""))}},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-failed", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodFailed},
			Spec: v1.PodSpec{
				Volumes:    []v1.Volume{{Name: "vol"}},
				Containers: []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", ""))}},
			},
		},
	}
}

// newBestEffortTestPods 构造两个 BestEffort 运行 Pod 与一个失败 Pod。
func newBestEffortTestPods() []runtime.Object {
	return []runtime.Object{
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-running", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodRunning},
			Spec: v1.PodSpec{
				Volumes:    []v1.Volume{{Name: "vol"}},
				Containers: []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("", ""), getResourceList("", ""))}},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-running-2", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodRunning},
			Spec: v1.PodSpec{
				Volumes:    []v1.Volume{{Name: "vol"}},
				Containers: []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("", ""), getResourceList("", ""))}},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-failed", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodFailed},
			Spec: v1.PodSpec{
				Volumes:    []v1.Volume{{Name: "vol"}},
				Containers: []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", ""))}},
			},
		},
	}
}

// newTestPodsWithPriorityClasses 构造带不同优先级类及无优先级类的 Pod。
func newTestPodsWithPriorityClasses() []runtime.Object {
	return []runtime.Object{
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-running", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodRunning},
			Spec: v1.PodSpec{
				Volumes:           []v1.Volume{{Name: "vol"}},
				Containers:        []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("500m", "50Gi"), getResourceList("", ""))}},
				PriorityClassName: "high",
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-running-2", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodRunning},
			Spec: v1.PodSpec{
				Volumes:           []v1.Volume{{Name: "vol"}},
				Containers:        []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", ""))}},
				PriorityClassName: "low",
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-failed", Namespace: "testing"},
			Status:     v1.PodStatus{Phase: v1.PodFailed},
			Spec: v1.PodSpec{
				Volumes:    []v1.Volume{{Name: "vol"}},
				Containers: []v1.Container{{Name: "ctr", Image: "image", Resources: getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", ""))}},
			},
		},
	}
}

// TestSyncResourceQuota 验证配额同步的状态计算、错误传播与写回行为。
func TestSyncResourceQuota(t *testing.T) {
	testCases := map[string]struct {
		// gvr 配置了通用 lister 的资源。
		gvr schema.GroupVersionResource
		// errorGVR 被配置为列表失败的资源。
		errorGVR schema.GroupVersionResource
		// items 供通用 lister 返回的对象。
		items []runtime.Object
		// quota 待同步的 ResourceQuota。
		quota v1.ResourceQuota
		// status 期望写回的状态。
		status v1.ResourceQuotaStatus
		// expectedError 期望的错误子串，为空表示不应报错。
		expectedError string
		// expectedActionSet 期望客户端收到的动作集合。
		expectedActionSet sets.String
	}{
		"non-matching-best-effort-scoped-quota": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					Scopes: []v1.ResourceQuotaScope{v1.ResourceQuotaScopeBestEffort},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("0"),
					v1.ResourceMemory: resource.MustParse("0"),
					v1.ResourcePods:   resource.MustParse("0"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPods(),
		},
		"matching-best-effort-scoped-quota": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					Scopes: []v1.ResourceQuotaScope{v1.ResourceQuotaScopeBestEffort},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("0"),
					v1.ResourceMemory: resource.MustParse("0"),
					v1.ResourcePods:   resource.MustParse("2"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newBestEffortTestPods(),
		},
		"non-matching-priorityclass-scoped-quota-OpExists": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpExists},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("0"),
					v1.ResourceMemory: resource.MustParse("0"),
					v1.ResourcePods:   resource.MustParse("0"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPods(),
		},
		"matching-priorityclass-scoped-quota-OpExists": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpExists},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("600m"),
					v1.ResourceMemory: resource.MustParse("51Gi"),
					v1.ResourcePods:   resource.MustParse("2"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPodsWithPriorityClasses(),
		},
		"matching-priorityclass-scoped-quota-OpIn": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpIn,
								Values:    []string{"high", "low"},
							},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("600m"),
					v1.ResourceMemory: resource.MustParse("51Gi"),
					v1.ResourcePods:   resource.MustParse("2"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPodsWithPriorityClasses(),
		},
		"matching-priorityclass-scoped-quota-OpIn-high": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpIn,
								Values:    []string{"high"},
							},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("500m"),
					v1.ResourceMemory: resource.MustParse("50Gi"),
					v1.ResourcePods:   resource.MustParse("1"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPodsWithPriorityClasses(),
		},
		"matching-priorityclass-scoped-quota-OpIn-low": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpIn,
								Values:    []string{"low"},
							},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("1Gi"),
					v1.ResourcePods:   resource.MustParse("1"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPodsWithPriorityClasses(),
		},
		"matching-priorityclass-scoped-quota-OpNotIn-low": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpNotIn,
								Values:    []string{"high"},
							},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("1Gi"),
					v1.ResourcePods:   resource.MustParse("1"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPodsWithPriorityClasses(),
		},
		"non-matching-priorityclass-scoped-quota-OpIn": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpIn,
								Values:    []string{"random"},
							},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("0"),
					v1.ResourceMemory: resource.MustParse("0"),
					v1.ResourcePods:   resource.MustParse("0"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPodsWithPriorityClasses(),
		},
		"non-matching-priorityclass-scoped-quota-OpNotIn": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpNotIn,
								Values:    []string{"random"},
							},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("200m"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
					v1.ResourcePods:   resource.MustParse("2"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPods(),
		},
		"matching-priorityclass-scoped-quota-OpDoesNotExist": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
					ScopeSelector: &v1.ScopeSelector{
						MatchExpressions: []v1.ScopedResourceSelectorRequirement{
							{
								ScopeName: v1.ResourceQuotaScopePriorityClass,
								Operator:  v1.ScopeSelectorOpDoesNotExist,
							},
						},
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("200m"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
					v1.ResourcePods:   resource.MustParse("2"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPods(),
		},
		"pods": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "testing"},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("3"),
						v1.ResourceMemory: resource.MustParse("100Gi"),
						v1.ResourcePods:   resource.MustParse("5"),
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("100Gi"),
					v1.ResourcePods:   resource.MustParse("5"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("200m"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
					v1.ResourcePods:   resource.MustParse("2"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: newTestPods(),
		},
		"quota-spec-hard-updated": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
				},
				Status: v1.ResourceQuotaStatus{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("3"),
					},
					Used: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("0"),
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("4"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("0"),
				},
			},
			expectedActionSet: sets.NewString(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			items: []runtime.Object{},
		},
		"quota-unchanged": {
			gvr: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
				},
				Status: v1.ResourceQuotaStatus{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("0"),
					},
				},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("4"),
				},
				Used: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("0"),
				},
			},
			expectedActionSet: sets.NewString(),
			items:             []runtime.Object{},
		},
		"quota-missing-status-with-calculation-error": {
			errorGVR: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourcePods: resource.MustParse("1"),
					},
				},
				Status: v1.ResourceQuotaStatus{},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourcePods: resource.MustParse("1"),
				},
			},
			expectedError:     "error listing",
			expectedActionSet: sets.NewString("update-resourcequotas-status"),
			items:             []runtime.Object{},
		},
		"quota-missing-status-with-partial-calculation-error": {
			gvr:      v1.SchemeGroupVersion.WithResource("configmaps"),
			errorGVR: v1.SchemeGroupVersion.WithResource("pods"),
			quota: v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourcePods:       resource.MustParse("1"),
						v1.ResourceConfigMaps: resource.MustParse("1"),
					},
				},
				Status: v1.ResourceQuotaStatus{},
			},
			status: v1.ResourceQuotaStatus{
				Hard: v1.ResourceList{
					v1.ResourcePods:       resource.MustParse("1"),
					v1.ResourceConfigMaps: resource.MustParse("1"),
				},
				Used: v1.ResourceList{
					v1.ResourceConfigMaps: resource.MustParse("0"),
				},
			},
			expectedError:     "error listing",
			expectedActionSet: sets.NewString("update-resourcequotas-status"),
			items:             []runtime.Object{},
		},
	}

	// 每个用例独立创建客户端与控制器，避免动作记录相互污染。
	for testName, testCase := range testCases {
		kubeClient := fake.NewSimpleClientset(&testCase.quota)
		listersForResourceConfig := map[schema.GroupVersionResource]cache.GenericLister{
			testCase.gvr:      newGenericLister(testCase.gvr.GroupResource(), testCase.items),
			testCase.errorGVR: newErrorLister(),
		}
		qc := setupQuotaController(t, kubeClient, mockListerForResourceFunc(listersForResourceConfig), mockDiscoveryFunc)
		defer close(qc.stop)

		if err := qc.syncResourceQuota(context.TODO(), &testCase.quota); err != nil {
			if len(testCase.expectedError) == 0 || !strings.Contains(err.Error(), testCase.expectedError) {
				t.Fatalf("test: %s, unexpected error: %v", testName, err)
			}
		} else if len(testCase.expectedError) > 0 {
			t.Fatalf("test: %s, expected error %q, got none", testName, testCase.expectedError)
		}

		actionSet := sets.NewString()
		for _, action := range kubeClient.Actions() {
			actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
		}
		if !actionSet.IsSuperset(testCase.expectedActionSet) {
			t.Errorf("test: %s,\nExpected actions:\n%v\n but got:\n%v\nDifference:\n%v", testName, testCase.expectedActionSet, actionSet, testCase.expectedActionSet.Difference(actionSet))
		}

		var usage *v1.ResourceQuota
		actions := kubeClient.Actions()
		for i := len(actions) - 1; i >= 0; i-- {
			if updateAction, ok := actions[i].(core.UpdateAction); ok {
				usage = updateAction.GetObject().(*v1.ResourceQuota)
				break
			}
		}
		if usage == nil {
			t.Fatalf("test: %s,\nExpected update action usage, got none: actions:\n%v", testName, actions)
		}

		// 校验最终写回的用量与期望一致
		if len(usage.Status.Hard) != len(testCase.status.Hard) {
			t.Errorf("test: %s, status hard lengths do not match", testName)
		}
		if len(usage.Status.Used) != len(testCase.status.Used) {
			t.Errorf("test: %s, status used lengths do not match", testName)
		}
		for k, v := range testCase.status.Hard {
			actual := usage.Status.Hard[k]
			actualValue := actual.String()
			expectedValue := v.String()
			if expectedValue != actualValue {
				t.Errorf("test: %s, Usage Hard: Key: %v, Expected: %v, Actual: %v", testName, k, expectedValue, actualValue)
			}
		}
		for k, v := range testCase.status.Used {
			actual := usage.Status.Used[k]
			actualValue := actual.String()
			expectedValue := v.String()
			if expectedValue != actualValue {
				t.Errorf("test: %s, Usage Used: Key: %v, Expected: %v, Actual: %v", testName, k, expectedValue, actualValue)
			}
		}
	}
}

// TestAddQuota 验证新增配额按是否缺少用量进入不同工作队列。
func TestAddQuota(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	gvr := v1.SchemeGroupVersion.WithResource("pods")
	listersForResourceConfig := map[schema.GroupVersionResource]cache.GenericLister{
		gvr: newGenericLister(gvr.GroupResource(), newTestPods()),
	}

	qc := setupQuotaController(t, kubeClient, mockListerForResourceFunc(listersForResourceConfig), mockDiscoveryFunc)
	defer close(qc.stop)

	testCases := []struct {
		// name 用例名称。
		name string
		// quota 待加入控制器的 ResourceQuota。
		quota *v1.ResourceQuota
		// expectedPriority 是否应进入优先处理队列。
		expectedPriority bool
	}{
		{
			name:             "no status",
			expectedPriority: true,
			quota: &v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
				},
			},
		},
		{
			name:             "status, no usage",
			expectedPriority: true,
			quota: &v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
				},
				Status: v1.ResourceQuotaStatus{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
				},
			},
		},
		{
			name:             "status, no usage(to validate it works for extended resources)",
			expectedPriority: true,
			quota: &v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						"requests.example/foobars.example.com": resource.MustParse("4"),
					},
				},
				Status: v1.ResourceQuotaStatus{
					Hard: v1.ResourceList{
						"requests.example/foobars.example.com": resource.MustParse("4"),
					},
				},
			},
		},
		{
			name:             "status, mismatch",
			expectedPriority: true,
			quota: &v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
				},
				Status: v1.ResourceQuotaStatus{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("6"),
					},
					Used: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("0"),
					},
				},
			},
		},
		{
			name:             "status, missing usage, but don't care (no informer)",
			expectedPriority: false,
			quota: &v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						"foobars.example.com": resource.MustParse("4"),
					},
				},
				Status: v1.ResourceQuotaStatus{
					Hard: v1.ResourceList{
						"foobars.example.com": resource.MustParse("4"),
					},
				},
			},
		},
		{
			name:             "ready",
			expectedPriority: false,
			quota: &v1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "rq",
				},
				Spec: v1.ResourceQuotaSpec{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
				},
				Status: v1.ResourceQuotaStatus{
					Hard: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("4"),
					},
					Used: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("0"),
					},
				},
			},
		},
	}

	// 逐用例清空队列，避免前一个用例的键影响后续断言。
	for _, tc := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		qc.addQuota(logger, tc.quota)
		if tc.expectedPriority {
			if e, a := 1, qc.missingUsageQueue.Len(); e != a {
				t.Errorf("%s: expected %v, got %v", tc.name, e, a)
			}
			if e, a := 0, qc.queue.Len(); e != a {
				t.Errorf("%s: expected %v, got %v", tc.name, e, a)
			}
		} else {
			if e, a := 0, qc.missingUsageQueue.Len(); e != a {
				t.Errorf("%s: expected %v, got %v", tc.name, e, a)
			}
			if e, a := 1, qc.queue.Len(); e != a {
				t.Errorf("%s: expected %v, got %v", tc.name, e, a)
			}
		}
		for qc.missingUsageQueue.Len() > 0 {
			key, _ := qc.missingUsageQueue.Get()
			qc.missingUsageQueue.Done(key)
		}
		for qc.queue.Len() > 0 {
			key, _ := qc.queue.Get()
			qc.queue.Done(key)
		}
	}
}

// TestDiscoverySync 验证发现客户端报错时
// 配额控制器不会无限阻塞。
func TestDiscoverySync(t *testing.T) {
	serverResources := []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"create", "delete", "list", "watch"}},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment", Verbs: metav1.Verbs{"create", "delete", "list", "watch"}},
			},
		},
	}
	unsyncableServerResources := []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"create", "delete", "list", "watch"}},
				{Name: "secrets", Namespaced: true, Kind: "Secret", Verbs: metav1.Verbs{"create", "delete", "list", "watch"}},
			},
		},
	}
	appsV1Resources := []*metav1.APIResourceList{
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment", Verbs: metav1.Verbs{"create", "delete", "list", "watch"}},
			},
		},
	}
	appsV1Error := &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{{Group: "apps", Version: "v1"}: fmt.Errorf(":-/")}}
	coreV1Error := &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{{Group: "", Version: "v1"}: fmt.Errorf(":-/")}}
	fakeDiscoveryClient := &fakeServerResources{
		PreferredResources: serverResources,
		Error:              nil,
		Lock:               sync.Mutex{},
		InterfaceUsedCount: 0,
	}

	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/api/v1/pods": {
				200,
				[]byte("{}"),
			},
			"GET" + "/api/v1/secrets": {
				404,
				[]byte("{}"),
			},
			"GET" + "/apis/apps/v1/deployments": {
				200,
				[]byte("{}"),
			},
		},
	}

	srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
	defer srv.Close()
	clientConfig.ContentConfig.NegotiatedSerializer = nil
	kubeClient, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		t.Fatal(err)
	}

	pods := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	secrets := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	deployments := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	listersForResourceConfig := map[schema.GroupVersionResource]cache.GenericLister{
		pods:        newGenericLister(pods.GroupResource(), []runtime.Object{}),
		secrets:     newGenericLister(secrets.GroupResource(), []runtime.Object{}),
		deployments: newGenericLister(deployments.GroupResource(), []runtime.Object{}),
	}
	qc := setupQuotaController(t, kubeClient, mockListerForResourceFunc(listersForResourceConfig), fakeDiscoveryClient.ServerPreferredNamespacedResources)
	defer close(qc.stop)

	stopSync := make(chan struct{})
	defer close(stopSync)
	// Sync() 的伪代码：
	// Sync(client, period, stopCh)：
	// wait.Until() 按 `period` 循环，直到 `stopCh` 关闭：
	//       GetQuotableResources()
	//       resyncMonitors()
	// cache.WaitForNamedCacheSync() 按 `syncedPollPeriod`（硬编码为 100ms）轮询，直到等待期结束或全部缓存同步完成。
	//
	// 将周期设为 200ms，可让每轮 wait.Until() 中大约进行两次
	// 缓存同步检查。
	//
	// 测试中 1s 的睡眠让 GetQuotableResources 与 resyncMonitors
	// 运行约 5 次，确保 fakeDiscoveryClient 的变化
	// 能被同步循环捕获。
	_, ctx := ktesting.NewTestContext(t)
	go qc.Sync(ctx, fakeDiscoveryClient.ServerPreferredNamespacedResources, 200*time.Millisecond)

	// 等待同步循环发现初始资源
	time.Sleep(1 * time.Second)

	err = expectSyncNotBlocked(fakeDiscoveryClient, &qc.workerLock)
	if err != nil {
		t.Fatalf("Expected quotacontroller.Sync to be running but it is blocked: %v", err)
	}
	assertMonitors(t, qc, "pods", "deployments")

	// 模拟发现客户端返回错误
	fakeDiscoveryClient.setPreferredResources(nil, fmt.Errorf("error calling discoveryClient.ServerPreferredResources()"))

	// 等待同步循环感知变化
	time.Sleep(1 * time.Second)
	// 不应移除任何 monitor
	assertMonitors(t, qc, "pods", "deployments")

	// 清除错误，确认配额同步仍可正常工作
	fakeDiscoveryClient.setPreferredResources(serverResources, nil)

	err = expectSyncNotBlocked(fakeDiscoveryClient, &qc.workerLock)
	if err != nil {
		t.Fatalf("Expected quotacontroller.Sync to still be running but it is blocked: %v", err)
	}
	assertMonitors(t, qc, "pods", "deployments")

	// 模拟返回 restmapper 能解析但缓存无法同步的资源
	fakeDiscoveryClient.setPreferredResources(unsyncableServerResources, nil)

	// 等待同步循环感知变化
	time.Sleep(1 * time.Second)
	// deployments 被移除，secrets 被加入
	assertMonitors(t, qc, "pods", "secrets")

	// 恢复正常资源，确认配额同步能够恢复
	fakeDiscoveryClient.setPreferredResources(serverResources, nil)

	// 等待同步循环感知变化
	time.Sleep(1 * time.Second)
	err = expectSyncNotBlocked(fakeDiscoveryClient, &qc.workerLock)
	if err != nil {
		t.Fatalf("Expected quotacontroller.Sync to still be running but it is blocked: %v", err)
	}
	// secrets 被移除，deployments 重新加入
	assertMonitors(t, qc, "pods", "deployments")

	// apps/v1 发现失败
	fakeDiscoveryClient.setPreferredResources(unsyncableServerResources, appsV1Error)
	// 等待同步循环感知变化
	time.Sleep(1 * time.Second)
	err = expectSyncNotBlocked(fakeDiscoveryClient, &qc.workerLock)
	if err != nil {
		t.Fatalf("Expected quotacontroller.Sync to still be running but it is blocked: %v", err)
	}
	// appsv1 报错时保留 deployments，同时加入 secrets
	assertMonitors(t, qc, "pods", "deployments", "secrets")

	// core/v1 发现失败
	fakeDiscoveryClient.setPreferredResources(appsV1Resources, coreV1Error)
	// 等待同步循环感知变化
	time.Sleep(1 * time.Second)
	err = expectSyncNotBlocked(fakeDiscoveryClient, &qc.workerLock)
	if err != nil {
		t.Fatalf("Expected quotacontroller.Sync to still be running but it is blocked: %v", err)
	}
	// corev1 报错时保留 pods 与 secrets
	assertMonitors(t, qc, "pods", "deployments", "secrets")

	// 恢复正常资源，确认配额同步能够恢复
	fakeDiscoveryClient.setPreferredResources(serverResources, nil)

	err = expectSyncNotBlocked(fakeDiscoveryClient, &qc.workerLock)
	if err != nil {
		t.Fatalf("Expected quotacontroller.Sync to still be running but it is blocked: %v", err)
	}
	// secrets 被移除，deployments 保留
	assertMonitors(t, qc, "pods", "deployments")
}

// assertMonitors 断言当前 monitor 集合与给定资源列表一致。
func assertMonitors(t *testing.T, qc quotaController, resources ...string) {
	t.Helper()
	expected := sets.NewString(resources...)
	actual := sets.NewString()
	for m := range qc.Controller.quotaMonitor.monitors {
		actual.Insert(m.Resource)
	}
	if !actual.Equal(expected) {
		t.Fatalf("expected monitors %v, got %v", expected.List(), actual.List())
	}
}

// testServerAndClientConfig 返回监听中的测试服务器及指向它的客户端配置。
func testServerAndClientConfig(handler func(http.ResponseWriter, *http.Request)) (*httptest.Server, *rest.Config) {
	srv := httptest.NewServer(http.HandlerFunc(handler))
	config := &rest.Config{
		Host: srv.URL,
	}
	return srv, config
}

// expectSyncNotBlocked 通过发现接口调用次数与加锁耗时判断同步循环未被卡住。
func expectSyncNotBlocked(fakeDiscoveryClient *fakeServerResources, workerLock *sync.RWMutex) error {
	before := fakeDiscoveryClient.getInterfaceUsedCount()
	t := 1 * time.Second
	time.Sleep(t)
	after := fakeDiscoveryClient.getInterfaceUsedCount()
	if before == after {
		return fmt.Errorf("discoveryClient.ServerPreferredResources() called %d times over %v", after-before, t)
	}

	workerLockAcquired := make(chan struct{})
	// 用能否及时拿到锁判断工作协程没有长期占用 workerLock。
	go func() {
		workerLock.Lock()
		defer workerLock.Unlock()
		close(workerLockAcquired)
	}()
	select {
	case <-workerLockAcquired:
		return nil
	case <-time.After(t):
		return fmt.Errorf("workerLock blocked for at least %v", t)
	}
}

// fakeServerResources 是可并发读写的发现客户端伪造实现。
type fakeServerResources struct {
	// PreferredResources 是下一次发现调用返回的资源列表。
	PreferredResources []*metav1.APIResourceList
	// Error 是下一次发现调用返回的错误。
	Error error
	// Lock 保护伪造状态，避免与同步协程并发读写。
	Lock sync.Mutex
	// InterfaceUsedCount 记录发现接口被调用的次数。
	InterfaceUsedCount int
}

// ServerResourcesForGroupVersion 未在本测试中使用，返回空结果。
func (*fakeServerResources) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return nil, nil
}

// ServerPreferredResources 未在本测试中使用，返回空结果。
func (*fakeServerResources) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}

// setPreferredResources 设置下一次发现调用返回的资源与错误。
func (f *fakeServerResources) setPreferredResources(resources []*metav1.APIResourceList, err error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.PreferredResources = resources
	f.Error = err
}

// getInterfaceUsedCount 返回发现接口被调用的次数。
func (f *fakeServerResources) getInterfaceUsedCount() int {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	return f.InterfaceUsedCount
}

// ServerPreferredNamespacedResources 返回预置结果并累计调用次数。
func (f *fakeServerResources) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.InterfaceUsedCount++
	return f.PreferredResources, f.Error
}

// fakeAction 记录请求信息，便于测试断言。
type fakeAction struct {
	// method HTTP 方法。
	method string
	// path 请求路径。
	path string
	// query 请求查询串。
	query string
}

// String 返回 method=path 形式，便于测试断言。
func (f *fakeAction) String() string {
	return strings.Join([]string{f.method, f.path}, "=")
}

// FakeResponse 是 handler 返回的伪造状态码与响应体。
type FakeResponse struct {
	// statusCode HTTP 状态码。
	statusCode int
	// content 响应体内容。
	content []byte
}

// fakeActionHandler 保存收到的请求及预置响应。
type fakeActionHandler struct {
	// response 按 method + path 返回对应的状态码与内容。
	response map[string]FakeResponse

	// lock 保护 actions 与 response 的并发访问。
	lock sync.Mutex
	// actions 按接收顺序记录的请求。
	actions []fakeAction
}

// ServeHTTP 记录收到的请求并返回预置的状态码与内容。
func (f *fakeActionHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	func() {
		f.lock.Lock()
		defer f.lock.Unlock()

		f.actions = append(f.actions, fakeAction{method: request.Method, path: request.URL.Path, query: request.URL.RawQuery})
		fakeResponse, ok := f.response[request.Method+request.URL.Path]
		if !ok {
			fakeResponse.statusCode = 200
			fakeResponse.content = []byte("{\"kind\": \"List\"}")
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(fakeResponse.statusCode)
		response.Write(fakeResponse.content)
	}()

	// 对 watch=true 的请求保持连接，模拟 informer 的 watch 长连接。
	if strings.Contains(request.URL.RawQuery, "watch=true") {
		hijacker, ok := response.(http.Hijacker)
		if !ok {
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		time.Sleep(30 * time.Second)
	}
}
