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

package resourcequota

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/admission/plugin/resourcequota"
	resourcequotaapi "k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	testcore "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	api "k8s.io/kubernetes/pkg/apis/core"
	controlplaneadmission "k8s.io/kubernetes/pkg/controlplane/apiserver/admission"
	"k8s.io/kubernetes/pkg/quota/v1/install"
)

// getResourceList 根据 CPU/内存字符串构造资源列表，空字符串表示不设置该项。
func getResourceList(cpu, memory string) api.ResourceList {
	res := api.ResourceList{}
	if cpu != "" {
		res[api.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		res[api.ResourceMemory] = resource.MustParse(memory)
	}
	return res
}

// getResourceRequirements 由请求与限制列表构造容器资源需求。
func getResourceRequirements(requests, limits api.ResourceList) api.ResourceRequirements {
	res := api.ResourceRequirements{}
	res.Requests = requests
	res.Limits = limits
	return res
}

// getVolumeResourceRequirements 由请求与限制列表构造卷资源需求。
func getVolumeResourceRequirements(requests, limits api.ResourceList) api.VolumeResourceRequirements {
	res := api.VolumeResourceRequirements{}
	res.Requests = requests
	res.Limits = limits
	return res
}

// validPod 构造一个位于 test 命名空间、含指定数量容器的 Pod。
func validPod(name string, numContainers int, resources api.ResourceRequirements) *api.Pod {
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		Spec:       api.PodSpec{},
	}
	pod.Spec.Containers = make([]api.Container, 0, numContainers)
	for i := 0; i < numContainers; i++ {
		pod.Spec.Containers = append(pod.Spec.Containers, api.Container{
			Image:     "foo:V" + strconv.Itoa(i),
			Resources: resources,
		})
	}
	return pod
}

// validPodWithPriority 构造带指定优先级的 Pod，空优先级名表示不设置。
func validPodWithPriority(name string, numContainers int, resources api.ResourceRequirements, priorityClass string) *api.Pod {
	pod := validPod(name, numContainers, resources)
	if priorityClass != "" {
		pod.Spec.PriorityClassName = priorityClass
	}
	return pod
}

// validPersistentVolumeClaim 构造一个位于 test 命名空间的 PVC。
func validPersistentVolumeClaim(name string, resources api.VolumeResourceRequirements) *api.PersistentVolumeClaim {
	return &api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		Spec: api.PersistentVolumeClaimSpec{
			Resources: resources,
		},
	}
}

// createHandler 用默认配置构造并初始化配额准入插件。
func createHandler(kubeClient kubernetes.Interface, informerFactory informers.SharedInformerFactory, stopCh chan struct{}) (*resourcequota.QuotaAdmission, error) {
	return createHandlerWithConfig(kubeClient, informerFactory, nil, stopCh)
}

// createHandlerWithConfig 按给定配置构造插件并注入初始化器，返回已校验的实例。
func createHandlerWithConfig(kubeClient kubernetes.Interface, informerFactory informers.SharedInformerFactory, config *resourcequotaapi.Configuration, stopCh chan struct{}) (*resourcequota.QuotaAdmission, error) {
	// nil 配置等价于空配置：插件对配额没有特殊限制。
	if config == nil {
		config = &resourcequotaapi.Configuration{}
	}
	quotaConfiguration, err := install.NewQuotaConfigurationForAdmission(informerFactory, nil)
	if err != nil {
		return nil, err
	}

	handler, err := resourcequota.NewResourceQuota(config, 5)
	if err != nil {
		return nil, err
	}

	initializers := admission.PluginInitializers{
		genericadmissioninitializer.New(kubeClient, nil, informerFactory, nil, nil, nil, stopCh, nil),
		controlplaneadmission.NewPluginInitializer(quotaConfiguration, nil),
	}
	initializers.Initialize(handler)

	return handler, admission.ValidateInitialization(handler)
}

// TestAdmissionIgnoresDelete 验证准入控制器忽略删除操作。
func TestAdmissionIgnoresDelete(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	namespace := "default"
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(nil, nil, api.Kind("Pod").WithVersion("version"), namespace, "name", corev1.Resource("pods").WithVersion("version"), "", admission.Delete, &metav1.DeleteOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("ResourceQuota should admit all deletes: %v", err)
	}
}

// TestAdmissionIgnoresSubresources 验证准入控制器忽略子资源：超限 Pod 的正常创建会被拒绝，
// 而针对同一 Pod 的子资源创建请求则放行。
func TestAdmissionIgnoresSubresources(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{}
	resourceQuota.Name = "quota"
	resourceQuota.Namespace = "test"
	resourceQuota.Status = corev1.ResourceQuotaStatus{
		Hard: corev1.ResourceList{},
		Used: corev1.ResourceList{},
	}
	resourceQuota.Status.Hard[corev1.ResourceMemory] = resource.MustParse("2Gi")
	resourceQuota.Status.Used[corev1.ResourceMemory] = resource.MustParse("1Gi")
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	newPod := validPod("123", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected an error because the pod exceeded allowed quota")
	}
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "subresource", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Did not expect an error because the action went to a subresource: %v", err)
	}
}

// TestAdmitBelowQuotaLimit 验证 Pod 创建后其资源用量会被计入配额。
func TestAdmitBelowQuotaLimit(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("50Gi"),
				corev1.ResourcePods:   resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(kubeClient.Actions()) == 0 {
		t.Errorf("Expected a client action")
	}

	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}

	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)
	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1100m"),
				corev1.ResourceMemory: resource.MustParse("52Gi"),
				corev1.ResourcePods:   resource.MustParse("4"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}
}

// TestAdmitDryRun 验证 dry-run 请求不更新配额用量，但超限时仍会被拒绝。
func TestAdmitDryRun(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("50Gi"),
				corev1.ResourcePods:   resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)

	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, true, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	newPod = validPod("too-large-pod", 1, getResourceRequirements(getResourceList("100m", "60Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, true, nil), nil)
	if err == nil {
		t.Errorf("Expected error but got none")
	}

	if len(kubeClient.Actions()) != 0 {
		t.Errorf("Expected no client action on dry-run")
	}
}

// TestAdmitHandlesOldObjects 验证带旧对象的更新能被正确处理：旧对象用于计算用量差值。
func TestAdmitHandlesOldObjects(t *testing.T) {
	// 本场景中旧配额按 LoadBalancer 类型 Service 计费
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("10"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("10"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("10"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("1"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("1"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("0"),
			},
		},
	}

	// 启动配额系统
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)

	// 旧 Service 是 LoadBalancer，更新后变成 NodePort。
	existingService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test", ResourceVersion: "1"},
		Spec:       api.ServiceSpec{Type: api.ServiceTypeLoadBalancer},
	}
	newService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test"},
		Spec: api.ServiceSpec{
			Type:  api.ServiceTypeNodePort,
			Ports: []api.ServicePort{{Port: 1234}},
		},
	}
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newService, existingService, api.Kind("Service").WithVersion("version"), newService.Namespace, newService.Name, corev1.Resource("services").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(kubeClient.Actions()) == 0 {
		t.Errorf("Expected a client action")
	}

	// 唯一的动作应是更新配额状态：无需额外读取前一个版本对象。
	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}

	// 校验 LoadBalancer 用量减一、NodePort 加一，Service 总数不变。
	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)

	// 用量不写入负值，因此 LoadBalancer 用量保持上次上报值。
	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("10"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("10"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("10"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("1"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("1"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("1"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}
}

// TestAdmitHandlesNegativePVCUpdates 验证 PVC 请求变小（负增量）时无需更新配额。
func TestAdmitHandlesNegativePVCUpdates(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}

	// 启动配额系统
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)

	oldPVC := &api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-to-update", Namespace: "test", ResourceVersion: "1"},
		Spec: api.PersistentVolumeClaimSpec{
			Resources: getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("10Gi")}, api.ResourceList{}),
		},
	}

	newPVC := &api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-to-update", Namespace: "test"},
		Spec: api.PersistentVolumeClaimSpec{
			Resources: getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("5Gi")}, api.ResourceList{}),
		},
	}

	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPVC, oldPVC, api.Kind("PersistentVolumeClaim").WithVersion("version"), newPVC.Namespace, newPVC.Name, corev1.Resource("persistentvolumeclaims").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(kubeClient.Actions()) != 0 {
		t.Errorf("No client action should be taken in case of negative updates")
	}
}

// TestAdmitHandlesPVCUpdates 验证 PVC 扩容时配额用量按新请求值更新。
func TestAdmitHandlesPVCUpdates(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}

	// 启动配额系统
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)

	oldPVC := &api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-to-update", Namespace: "test", ResourceVersion: "1"},
		Spec: api.PersistentVolumeClaimSpec{
			Resources: getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("10Gi")}, api.ResourceList{}),
		},
	}

	newPVC := &api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-to-update", Namespace: "test"},
		Spec: api.PersistentVolumeClaimSpec{
			Resources: getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("15Gi")}, api.ResourceList{}),
		},
	}

	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPVC, oldPVC, api.Kind("PersistentVolumeClaim").WithVersion("version"), newPVC.Namespace, newPVC.Name, corev1.Resource("persistentvolumeclaims").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(kubeClient.Actions()) == 0 {
		t.Errorf("Expected a client action")
	}

	// 唯一的动作应是更新配额状态：无需额外读取前一个版本对象。
	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}

	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)
	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("15Gi"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}

}

// TestAdmitHandlesCreatingUpdates 验证旧对象 ResourceVersion 为空的更新会被当作创建处理。
func TestAdmitHandlesCreatingUpdates(t *testing.T) {
	// 本场景中已存在一个 Service
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("10"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("10"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("10"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("1"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("1"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("0"),
			},
		},
	}

	// 启动配额系统
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)

	// 旧 Service 没有 ResourceVersion，说明这次 update 实为创建。
	oldService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test", ResourceVersion: ""},
		Spec:       api.ServiceSpec{Type: api.ServiceTypeLoadBalancer},
	}
	newService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test"},
		Spec: api.ServiceSpec{
			Type:  api.ServiceTypeNodePort,
			Ports: []api.ServicePort{{Port: 1234}},
		},
	}
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newService, oldService, api.Kind("Service").WithVersion("version"), newService.Namespace, newService.Name, corev1.Resource("services").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(kubeClient.Actions()) == 0 {
		t.Errorf("Expected a client action")
	}

	// 唯一的动作应是更新配额状态：无需额外读取前一个版本对象。
	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}

	// 校验计算新用量时忽略了“旧”对象。
	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)
	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("10"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("10"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("10"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceServices:              resource.MustParse("2"),
				corev1.ResourceServicesLoadBalancers: resource.MustParse("1"),
				corev1.ResourceServicesNodePorts:     resource.MustParse("1"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}
}

// TestAdmitExceedQuotaLimit 验证 Pod 超出配额上限时在准入阶段被拒绝。
func TestAdmitExceedQuotaLimit(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("50Gi"),
				corev1.ResourcePods:   resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	newPod := validPod("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected an error exceeding quota")
	}
}

// TestAdmitEnforceQuotaConstraints 验证配额跟踪某资源时 Pod 必须显式指定该资源：
// 此处配额跟踪 CPU/内存请求和内存限制，未声明内存限制的 Pod 应准入失败。
func TestAdmitEnforceQuotaConstraints(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:          resource.MustParse("3"),
				corev1.ResourceMemory:       resource.MustParse("100Gi"),
				corev1.ResourceLimitsMemory: resource.MustParse("200Gi"),
				corev1.ResourcePods:         resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:          resource.MustParse("1"),
				corev1.ResourceMemory:       resource.MustParse("50Gi"),
				corev1.ResourceLimitsMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:         resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	// Pod 缺少内存限制，不满足配额要求的字段完整性。
	newPod := validPod("not-allowed-pod", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("200m", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected an error because the pod does not specify a memory limit")
	}
}

// TestAdmitPodInNamespaceWithoutQuota 验证命名空间无配额时 Pod 可以正常准入。
func TestAdmitPodInNamespaceWithoutQuota(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "other", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:          resource.MustParse("3"),
				corev1.ResourceMemory:       resource.MustParse("100Gi"),
				corev1.ResourceLimitsMemory: resource.MustParse("200Gi"),
				corev1.ResourcePods:         resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:          resource.MustParse("1"),
				corev1.ResourceMemory:       resource.MustParse("50Gi"),
				corev1.ResourceLimitsMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:         resource.MustParse("3"),
			},
		},
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	// 配额属于 other 命名空间，仅加入索引供插件查询。
	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("200m", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Did not expect an error because the pod is in a different namespace than the quota: %v", err)
	}
}

// TestAdmitBelowTerminatingQuotaLimit 验证 terminating Pod 的费用只计入 terminating 配额：
// 同时创建两种范围的配额，带 activeDeadlineSeconds 的 Pod 只增加前者用量。
func TestAdmitBelowTerminatingQuotaLimit(t *testing.T) {
	resourceQuotaNonTerminating := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-non-terminating", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeNotTerminating},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("50Gi"),
				corev1.ResourcePods:   resource.MustParse("3"),
			},
		},
	}
	resourceQuotaTerminating := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-terminating", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeTerminating},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("50Gi"),
				corev1.ResourcePods:   resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuotaTerminating, resourceQuotaNonTerminating)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaNonTerminating)
	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaTerminating)

	// activeDeadlineSeconds 使 Pod 落入 terminating 范围。
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("", "")))
	activeDeadlineSeconds := int64(30)
	newPod.Spec.ActiveDeadlineSeconds = &activeDeadlineSeconds
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(kubeClient.Actions()) == 0 {
		t.Errorf("Expected a client action")
	}

	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}

	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)

	// 只应更新 terminating 配额。
	if usage.Name != resourceQuotaTerminating.Name {
		t.Errorf("Incremented the wrong quota, expected %v, actual %v", resourceQuotaTerminating.Name, usage.Name)
	}

	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1100m"),
				corev1.ResourceMemory: resource.MustParse("52Gi"),
				corev1.ResourcePods:   resource.MustParse("4"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}
}

// TestAdmitBelowTerminatingQuotaLimitWhenPodScopeUpdated 验证 Pod 更新后落入 terminating 范围时，
// 费用只计入 terminating 配额。
//
// 此处配额准入故意“多算”：若请求后续被拒，预留的配额无法自动回收，依赖控制器对账实际用量来释放。
// 因此宁可高估也不能低估，否则未完成的请求也能缩减配额。
func TestAdmitBelowTerminatingQuotaLimitWhenPodScopeUpdated(t *testing.T) {
	resourceQuotaNonTerminating := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-non-terminating", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeNotTerminating},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("50Gi"),
				corev1.ResourcePods:   resource.MustParse("3"),
			},
		},
	}
	resourceQuotaTerminating := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-terminating", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeTerminating},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("50Gi"),
				corev1.ResourcePods:   resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuotaTerminating, resourceQuotaNonTerminating)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	err = informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaNonTerminating)
	if err != nil {
		t.Errorf("Error occurred while adding resource quota to the indexer: %v", err)
	}
	err = informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaTerminating)
	if err != nil {
		t.Errorf("Error occurred while adding resource quota to the indexer: %v", err)
	}

	// 旧 Pod 属于 non-terminating，更新后属于 terminating。
	existingPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("", "")))
	existingPod.ResourceVersion = "1"
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "2Gi"), getResourceList("", "")))
	activeDeadlineSeconds := int64(30)
	newPod.Spec.ActiveDeadlineSeconds = &activeDeadlineSeconds
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, existingPod, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(kubeClient.Actions()) == 0 {
		t.Errorf("Expected a client action")
	}

	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}

	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)

	// 只应更新 terminating 配额。
	if usage.Name != resourceQuotaTerminating.Name {
		t.Errorf("Incremented the wrong quota, expected %v, actual %v", resourceQuotaTerminating.Name, usage.Name)
	}

	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("100Gi"),
				corev1.ResourcePods:   resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1100m"),
				corev1.ResourceMemory: resource.MustParse("52Gi"),
				corev1.ResourcePods:   resource.MustParse("4"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}
}

// TestAdmitBelowVolumeAttributesClassQuotaLimit 验证指定 VAC 的 PVC 只计入对应配额：
// gold 与 silver 配额并存时，gold 类 PVC 只增加 gold 配额用量。
func TestAdmitBelowVolumeAttributesClassQuotaLimit(t *testing.T) {
	classGold := "gold"
	classSilver := "silver"

	resourceQuotaGold := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-gold", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			ScopeSelector: &corev1.ScopeSelector{
				MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
					{
						ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
						Operator:  corev1.ScopeSelectorOpIn,
						Values:    []string{classGold},
					},
				},
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}
	resourceQuotaSilver := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-silver", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			ScopeSelector: &corev1.ScopeSelector{
				MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
					{
						ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
						Operator:  corev1.ScopeSelectorOpIn,
						Values:    []string{classSilver},
					},
				},
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuotaGold, resourceQuotaSilver)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	err = informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaGold)
	if err != nil {
		t.Errorf("Error occurred while adding resource quota to the indexer: %v", err)
	}
	err = informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaSilver)
	if err != nil {
		t.Errorf("Error occurred while adding resource quota to the indexer: %v", err)
	}

	// 创建一个引用 gold 类的 PVC。
	newPvc := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
	newPvc.Spec.VolumeAttributesClassName = &classGold
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPvc, nil, api.Kind("PersistentVolumeClaim").WithVersion("version"), newPvc.Namespace, newPvc.Name, corev1.Resource("persistentvolumeclaims").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(kubeClient.Actions()) == 0 {
		t.Errorf("Expected a client action")
	}

	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}

	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)

	// 只应更新 gold 配额。
	if usage.Name != resourceQuotaGold.Name {
		t.Errorf("Incremented the wrong quota, expected %v, actual %v", resourceQuotaGold.Name, usage.Name)
	}

	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("2"),
				corev1.ResourceRequestsStorage:        resource.MustParse("11Gi"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}
}

// TestAdmitBelowVolumeAttributesClassQuotaLimitWhenPVCScopeUpdated 验证 PVC 切换 VAC 类后，
// 费用只计入新类对应的配额；gold/silver/copper 三个配额中只有预期的那个被更新。
//
// 此处配额准入故意“多算”：若请求后续被拒，预留的配额无法自动回收，依赖控制器对账实际用量来释放。
// 因此宁可高估也不能低估，否则未完成的请求也能缩减配额。
func TestAdmitBelowVolumeAttributesClassQuotaLimitWhenPVCScopeUpdated(t *testing.T) {
	classGold := "gold"
	classSilver := "silver"
	classCopper := "copper"

	resourceQuotaGold := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-gold", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			ScopeSelector: &corev1.ScopeSelector{
				MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
					{
						ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
						Operator:  corev1.ScopeSelectorOpIn,
						Values:    []string{classGold},
					},
				},
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}
	resourceQuotaSilver := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-silver", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			ScopeSelector: &corev1.ScopeSelector{
				MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
					{
						ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
						Operator:  corev1.ScopeSelectorOpIn,
						Values:    []string{classSilver},
					},
				},
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}
	resourceQuotaCopper := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-copper", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			ScopeSelector: &corev1.ScopeSelector{
				MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
					{
						ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
						Operator:  corev1.ScopeSelectorOpIn,
						Values:    []string{classCopper},
					},
				},
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}

	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("2"),
				corev1.ResourceRequestsStorage:        resource.MustParse("11Gi"),
			},
		},
	}

	testCases := []struct {
		desc              string
		existingQuotas    []runtime.Object
		subresource       string
		claims            func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim)
		expectedActionSet sets.Set[string]
		expectedQuotaName string
		expectedUsage     corev1.ResourceQuota
	}{
		{
			desc:           "update the desired class of a pvc from nil to gold",
			existingQuotas: []runtime.Object{resourceQuotaGold, resourceQuotaSilver, resourceQuotaCopper},
			claims: func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim) {
				// 旧 PVC 未引用任何类。
				existingPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				existingPVC.ResourceVersion = "1"
				existingPVC.Status.Phase = api.ClaimBound

				// 更新后引用单一类 gold。
				newPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				newPVC.Spec.VolumeAttributesClassName = &classGold
				newPVC.Status.Phase = api.ClaimBound
				return existingPVC, newPVC
			},
			expectedActionSet: sets.New(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			expectedQuotaName: resourceQuotaGold.Name,
			expectedUsage:     expectedUsage,
		},
		{
			desc:           "update the target class of a pvc to gold ",
			existingQuotas: []runtime.Object{resourceQuotaGold, resourceQuotaSilver, resourceQuotaCopper},
			subresource:    "status",
			claims: func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim) {
				// 旧 PVC 引用单一类 gold。
				existingPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				existingPVC.ResourceVersion = "1"
				existingPVC.Spec.VolumeAttributesClassName = &classGold
				existingPVC.Status.Phase = api.ClaimBound

				// 更新后仍引用单一类 gold，与旧 PVC 相同。
				newPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				newPVC.Spec.VolumeAttributesClassName = &classGold
				newPVC.Status.Phase = api.ClaimBound
				newPVC.Status.ModifyVolumeStatus = &api.ModifyVolumeStatus{TargetVolumeAttributesClassName: classGold}
				return existingPVC, newPVC
			},
			expectedActionSet: sets.New[string](),
			expectedQuotaName: "",
		},
		{
			desc:           "update the current class of a pvc from nil to gold",
			existingQuotas: []runtime.Object{resourceQuotaGold, resourceQuotaSilver, resourceQuotaCopper},
			subresource:    "status",
			claims: func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim) {
				// 旧 PVC 引用单一类 gold。
				existingPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				existingPVC.ResourceVersion = "1"
				existingPVC.Spec.VolumeAttributesClassName = &classGold
				existingPVC.Status.Phase = api.ClaimBound
				existingPVC.Status.ModifyVolumeStatus = &api.ModifyVolumeStatus{TargetVolumeAttributesClassName: classGold}

				// 更新后仍引用单一类 gold，与旧 PVC 相同。
				newPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				newPVC.Spec.VolumeAttributesClassName = &classGold
				newPVC.Status.Phase = api.ClaimBound
				newPVC.Status.CurrentVolumeAttributesClassName = &classGold
				return existingPVC, newPVC
			},
			expectedActionSet: sets.New[string](),
			expectedQuotaName: "",
		},
		{
			desc:           "update the desired class of a pvc from gold to silver",
			existingQuotas: []runtime.Object{resourceQuotaGold, resourceQuotaSilver, resourceQuotaCopper},
			claims: func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim) {
				// 旧 PVC 引用 gold 类。
				existingPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				existingPVC.ResourceVersion = "1"
				existingPVC.Spec.VolumeAttributesClassName = &classGold
				existingPVC.Status.Phase = api.ClaimBound
				existingPVC.Status.CurrentVolumeAttributesClassName = &classGold

				// 更新后引用两个类：silver 与 gold。
				newPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				newPVC.Spec.VolumeAttributesClassName = &classSilver
				newPVC.Status.Phase = api.ClaimBound
				newPVC.Status.CurrentVolumeAttributesClassName = &classGold
				return existingPVC, newPVC
			},
			expectedActionSet: sets.New(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			expectedQuotaName: resourceQuotaSilver.Name,
			expectedUsage:     expectedUsage,
		},
		{
			desc:           "update the target class of a pvc to silver",
			existingQuotas: []runtime.Object{resourceQuotaGold, resourceQuotaSilver, resourceQuotaCopper},
			subresource:    "status",
			claims: func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim) {
				// 旧 PVC 引用两个类：silver 与 gold。
				existingPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				existingPVC.ResourceVersion = "1"
				existingPVC.Spec.VolumeAttributesClassName = &classSilver
				existingPVC.Status.Phase = api.ClaimBound
				existingPVC.Status.CurrentVolumeAttributesClassName = &classGold

				// 更新后仍引用 silver 与 gold，与旧 PVC 相同。
				newPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				newPVC.Spec.VolumeAttributesClassName = &classSilver
				newPVC.Status.Phase = api.ClaimBound
				newPVC.Status.ModifyVolumeStatus = &api.ModifyVolumeStatus{TargetVolumeAttributesClassName: classSilver}
				newPVC.Status.CurrentVolumeAttributesClassName = &classGold
				return existingPVC, newPVC
			},
			expectedActionSet: sets.New[string](),
			expectedQuotaName: "",
		},
		{
			desc:           "update the desired class of a pvc from silver to copper",
			existingQuotas: []runtime.Object{resourceQuotaGold, resourceQuotaSilver, resourceQuotaCopper},
			claims: func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim) {
				// 旧 PVC 引用两个类：silver 与 gold。
				existingPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				existingPVC.ResourceVersion = "1"
				existingPVC.Spec.VolumeAttributesClassName = &classSilver
				existingPVC.Status.Phase = api.ClaimBound
				existingPVC.Status.ModifyVolumeStatus = &api.ModifyVolumeStatus{TargetVolumeAttributesClassName: classSilver}
				existingPVC.Status.CurrentVolumeAttributesClassName = &classGold

				// 更新后引用三个类：copper、silver 与 gold。
				newPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				newPVC.Spec.VolumeAttributesClassName = &classCopper
				newPVC.Status.Phase = api.ClaimBound
				newPVC.Status.ModifyVolumeStatus = &api.ModifyVolumeStatus{TargetVolumeAttributesClassName: classSilver}
				newPVC.Status.CurrentVolumeAttributesClassName = &classGold
				return existingPVC, newPVC
			},
			expectedActionSet: sets.New(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			expectedQuotaName: resourceQuotaCopper.Name,
			expectedUsage:     expectedUsage,
		},
		{
			desc: "allow update pvc status when a quota is exceeded",
			existingQuotas: []runtime.Object{resourceQuotaGold, &corev1.ResourceQuota{
				ObjectMeta: resourceQuotaSilver.ObjectMeta,
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: resourceQuotaSilver.Spec.ScopeSelector,
					Hard: corev1.ResourceList{
						corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
						corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
					},
				},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
						corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
					},
					Used: corev1.ResourceList{
						corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
						corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
					},
				},
			}},
			subresource: "status",
			claims: func() (*api.PersistentVolumeClaim, *api.PersistentVolumeClaim) {
				// 旧 PVC 引用单一类 gold。
				existingPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				existingPVC.ResourceVersion = "1"
				existingPVC.Spec.VolumeAttributesClassName = &classGold
				existingPVC.Status.Phase = api.ClaimBound
				existingPVC.Status.CurrentVolumeAttributesClassName = &classGold

				// 更新后引用两个类：silver 与 gold。
				newPVC := validPersistentVolumeClaim("allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
				newPVC.Spec.VolumeAttributesClassName = &classGold
				newPVC.Status.Phase = api.ClaimBound
				newPVC.Status.ModifyVolumeStatus = &api.ModifyVolumeStatus{TargetVolumeAttributesClassName: classGold}
				newPVC.Status.CurrentVolumeAttributesClassName = &classSilver
				return existingPVC, newPVC
			},
			expectedActionSet: sets.New(
				strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
			),
			expectedQuotaName: resourceQuotaSilver.Name,
			expectedUsage: corev1.ResourceQuota{
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
						corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
					},
					Used: corev1.ResourceList{
						corev1.ResourcePersistentVolumeClaims: resource.MustParse("2"),
						corev1.ResourceRequestsStorage:        resource.MustParse("11Gi"),
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stopCh := make(chan struct{})
			defer close(stopCh)

			kubeClient := fake.NewSimpleClientset(testCase.existingQuotas...)
			informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

			handler, err := createHandler(kubeClient, informerFactory, stopCh)
			if err != nil {
				t.Errorf("Error occurred while creating admission plugin: %v", err)
			}

			for _, obj := range testCase.existingQuotas {
				err = informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(obj)
				if err != nil {
					t.Errorf("Error occurred while adding resource quota to the indexer: %v", err)
				}
			}

			existingPVC, newPVC := testCase.claims()
			err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPVC, existingPVC, api.Kind("PersistentVolumeClaim").WithVersion("version"), newPVC.Namespace, newPVC.Name, corev1.Resource("persistentvolumeclaims").WithVersion("version"), testCase.subresource, admission.Update, &metav1.CreateOptions{}, false, nil), nil)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if len(testCase.expectedActionSet) == 0 || testCase.expectedQuotaName == "" {
				decimatedActions := removeListWatch(kubeClient.Actions())
				if len(decimatedActions) != 0 {
					t.Errorf("Expected no actions, but got %v", decimatedActions)
				}
				return
			}

			if len(kubeClient.Actions()) == 0 {
				t.Errorf("Expected a client action")
			}

			actionSet := sets.New[string]()
			for _, action := range kubeClient.Actions() {
				actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
			}

			if !actionSet.HasAll(sets.List(testCase.expectedActionSet)...) {
				t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", testCase.expectedActionSet, actionSet, testCase.expectedActionSet.Difference(actionSet))
			}

			decimatedActions := removeListWatch(kubeClient.Actions())
			lastActionIndex := len(decimatedActions) - 1
			usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)

			// 只应更新预期的那个配额。
			if usage.Name != testCase.expectedQuotaName {
				t.Errorf("Incremented the wrong quota, expected %v, actual %v", testCase.expectedQuotaName, usage.Name)
			}

			for k, v := range testCase.expectedUsage.Status.Used {
				actual := usage.Status.Used[k]
				actualValue := actual.String()
				expectedValue := v.String()
				if expectedValue != actualValue {
					t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
				}
			}
		})
	}
}

// TestAdmitBelowBestEffortQuotaLimit 验证 BestEffort Pod 的费用只计入 BestEffort 配额。
func TestAdmitBelowBestEffortQuotaLimit(t *testing.T) {
	resourceQuotaBestEffort := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-besteffort", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("3"),
			},
		},
	}
	resourceQuotaNotBestEffort := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-not-besteffort", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeNotBestEffort},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuotaBestEffort, resourceQuotaNotBestEffort)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaBestEffort)
	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuotaNotBestEffort)

	// 未声明任何请求，Pod 属于 BestEffort。
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	expectedActionSet := sets.NewString(
		strings.Join([]string{"update", "resourcequotas", "status"}, "-"),
	)
	actionSet := sets.NewString()
	for _, action := range kubeClient.Actions() {
		actionSet.Insert(strings.Join([]string{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()}, "-"))
	}
	if !actionSet.HasAll(expectedActionSet.List()...) {
		t.Errorf("Expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet, actionSet, expectedActionSet.Difference(actionSet))
	}
	decimatedActions := removeListWatch(kubeClient.Actions())
	lastActionIndex := len(decimatedActions) - 1
	usage := decimatedActions[lastActionIndex].(testcore.UpdateAction).GetObject().(*corev1.ResourceQuota)

	if usage.Name != resourceQuotaBestEffort.Name {
		t.Errorf("Incremented the wrong quota, expected %v, actual %v", resourceQuotaBestEffort.Name, usage.Name)
	}

	expectedUsage := corev1.ResourceQuota{
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("4"),
			},
		},
	}
	for k, v := range expectedUsage.Status.Used {
		actual := usage.Status.Used[k]
		actualValue := actual.String()
		expectedValue := v.String()
		if expectedValue != actualValue {
			t.Errorf("Usage Used: Key: %v, Expected: %v, Actual: %v", k, expectedValue, actualValue)
		}
	}
}

// removeListWatch 过滤掉 client 为维护缓存而产生的 list/watch 动作。
func removeListWatch(in []testcore.Action) []testcore.Action {
	decimatedActions := []testcore.Action{}
	// list/watch 配额是缓存同步的固定噪声，与被测行为无关。
	for i := range in {
		if in[i].Matches("list", "resourcequotas") || in[i].Matches("watch", "resourcequotas") {
			continue
		}

		decimatedActions = append(decimatedActions, in[i])
	}
	return decimatedActions
}

// TestAdmitBestEffortQuotaLimitIgnoresBurstable 验证 BestEffort 配额不匹配有资源请求的 Pod。
func TestAdmitBestEffortQuotaLimitIgnoresBurstable(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-besteffort", Namespace: "test", ResourceVersion: "124"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("5"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("3"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	decimatedActions := removeListWatch(kubeClient.Actions())
	if len(decimatedActions) != 0 {
		t.Errorf("Expected no client actions because the incoming pod did not match best effort quota: %v", kubeClient.Actions())
	}
}

// TestAdmissionSetsMissingNamespace 验证对象缺少命名空间时会被填入请求中的命名空间。
func TestAdmissionSetsMissingNamespace(t *testing.T) {
	namespace := "test"
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: namespace, ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("3"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("1"),
			},
		},
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	newPod := validPod("pod-without-namespace", 1, getResourceRequirements(getResourceList("1", "2Gi"), getResourceList("", "")))

	// 故意清空命名空间，验证插件会回填。
	newPod.ObjectMeta.Namespace = ""

	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}
	if newPod.Namespace != namespace {
		t.Errorf("Got unexpected pod namespace: %q != %q", newPod.Namespace, namespace)
	}
}

// TestAdmitRejectsNegativeUsage 验证资源用量不能为负：负数 PVC 存储请求被拒绝，正常值放行。
func TestAdmitRejectsNegativeUsage(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("3"),
				corev1.ResourceRequestsStorage:        resource.MustParse("100Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				corev1.ResourceRequestsStorage:        resource.MustParse("10Gi"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)
	// 负数存储请求应被拒绝。
	newPvc := validPersistentVolumeClaim("not-allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("-1Gi")}, api.ResourceList{}))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPvc, nil, api.Kind("PersistentVolumeClaim").WithVersion("version"), newPvc.Namespace, newPvc.Name, corev1.Resource("persistentvolumeclaims").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected an error because the pvc has negative storage usage")
	}

	// 非负存储请求应放行。
	newPvc = validPersistentVolumeClaim("not-allowed-pvc", getVolumeResourceRequirements(api.ResourceList{api.ResourceStorage: resource.MustParse("1Gi")}, api.ResourceList{}))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPvc, nil, api.Kind("PersistentVolumeClaim").WithVersion("version"), newPvc.Namespace, newPvc.Name, corev1.Resource("persistentvolumeclaims").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestAdmitWhenUnrelatedResourceExceedsQuota 验证一种资源超配额不影响其他资源的准入。
func TestAdmitWhenUnrelatedResourceExceedsQuota(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceServices: resource.MustParse("3"),
				corev1.ResourcePods:     resource.MustParse("4"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceServices: resource.MustParse("4"),
				corev1.ResourcePods:     resource.MustParse("1"),
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)

	kubeClient := fake.NewSimpleClientset(resourceQuota)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	handler, err := createHandler(kubeClient, informerFactory, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	informerFactory.Core().V1().ResourceQuotas().Informer().GetIndexer().Add(resourceQuota)

	// Service 已超额，但 Pod 用量未超额，应放行。
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestAdmitLimitedResourceNoQuota 验证受限资源在没有覆盖它的配额时不可被消费。
func TestAdmitLimitedResourceNoQuota(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止消费受限资源。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "pods",
				MatchContains: []string{"cpu"},
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	newPod := validPod("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected an error for consuming a limited resource without quota.")
	}
}

// TestAdmitLimitedResourceNoQuotaIgnoresNonMatchingResources 验证不匹配的受限资源配置不会影响 Pod 准入。
func TestAdmitLimitedResourceNoQuotaIgnoresNonMatchingResources(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止消费受限资源。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "services",
				MatchContains: []string{"services"},
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// TestAdmitLimitedResourceWithQuota 验证受限资源存在覆盖配额时可以消费。
func TestAdmitLimitedResourceWithQuota(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsCPU: resource.MustParse("10"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceRequestsCPU: resource.MustParse("1"),
			},
		},
	}
	kubeClient := fake.NewSimpleClientset(resourceQuota)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{"namespace": cache.MetaNamespaceIndexFunc})
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止消费受限资源。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "pods",
				MatchContains: []string{"requests.cpu"}, // 只匹配 requests.cpu
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	indexer.Add(resourceQuota)
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAdmitLimitedResourceWithMultipleQuota 验证多个配额中只要有一个匹配，受限资源即可消费。
func TestAdmitLimitedResourceWithMultipleQuota(t *testing.T) {
	resourceQuota1 := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota1", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsCPU: resource.MustParse("10"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceRequestsCPU: resource.MustParse("1"),
			},
		},
	}
	resourceQuota2 := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota2", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("10Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
	}
	kubeClient := fake.NewSimpleClientset(resourceQuota1, resourceQuota2)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{"namespace": cache.MetaNamespaceIndexFunc})
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止消费受限资源。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "pods",
				MatchContains: []string{"requests.cpu"}, // 只匹配 requests.cpu
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	indexer.Add(resourceQuota1)
	indexer.Add(resourceQuota2)
	newPod := validPod("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAdmitLimitedResourceWithQuotaThatDoesNotCover 验证配额必须真正覆盖受限资源，否则消费仍被拒绝。
func TestAdmitLimitedResourceWithQuotaThatDoesNotCover(t *testing.T) {
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("10Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
	}
	kubeClient := fake.NewSimpleClientset(resourceQuota)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{"namespace": cache.MetaNamespaceIndexFunc})
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止消费受限资源。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "pods",
				MatchContains: []string{"cpu"}, // 只匹配 cpu
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	indexer.Add(resourceQuota)
	newPod := validPod("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")))
	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
	if err == nil {
		t.Fatalf("Expected an error since the quota did not cover cpu")
	}
}

// TestAdmitLimitedScopeWithCoverQuota 验证受限范围配置下，必须有覆盖该范围（及资源）的配额才能准入。
func TestAdmitLimitedScopeWithCoverQuota(t *testing.T) {
	testCases := []struct {
		description  string
		testPod      *api.Pod
		quota        *corev1.ResourceQuota
		anotherQuota *corev1.ResourceQuota
		config       *resourcequotaapi.Configuration
		expErr       string
	}{
		{
			description: "Covering quota exists for configured limited scope PriorityClassNameExists.",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "fake-priority"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			description: "configured limited scope PriorityClassNameExists and limited cpu resource. No covering quota for cpu and pod admit fails.",
			testPod:     validPodWithPriority("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "fake-priority"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
						MatchContains: []string{"requests.cpu"}, // 只匹配 requests.cpu
					},
				},
			},
			expErr: "insufficient quota to consume: requests.cpu",
		},
		{
			description: "Covering quota does not exist for configured limited scope PriorityClassNameExists.",
			testPod:     validPodWithPriority("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "fake-priority"),
			quota:       &corev1.ResourceQuota{},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{PriorityClass Exists []}]",
		},
		{
			description: "Covering quota does not exist for configured limited scope resourceQuotaBestEffort",
			testPod:     validPodWithPriority("not-allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")), "fake-priority"),
			quota:       &corev1.ResourceQuota{},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{BestEffort Exists []}]",
		},
		{
			description: "Covering quota exist for configured limited scope resourceQuotaBestEffort",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")), "fake-priority"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota-besteffort", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
				},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						corev1.ResourcePods: resource.MustParse("5"),
					},
					Used: corev1.ResourceList{
						corev1.ResourcePods: resource.MustParse("3"),
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			description: "Two scopes,BestEffort and PriorityClassIN, in two LimitedResources. Neither matches pod. Pod allowed",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", "")), "fake-priority"),
			quota:       &corev1.ResourceQuota{},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			description: "Two scopes,BestEffort and PriorityClassIN, in two LimitedResources. Only BestEffort scope matches pod. Pod admit fails because covering quota is missing for BestEffort scope",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")), "fake-priority"),
			quota:       &corev1.ResourceQuota{},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{BestEffort Exists []}]",
		},
		{
			description: "Two scopes,BestEffort and PriorityClassIN, in two LimitedResources. Only PriorityClass scope matches pod. Pod admit fails because covering quota is missing for PriorityClass scope",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("100m", "1Gi"), getResourceList("", "")), "cluster-services"),
			quota:       &corev1.ResourceQuota{},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{PriorityClass In [cluster-services]}]",
		},
		{
			description: "Two scopes,BestEffort and PriorityClassIN, in two LimitedResources. Both the scopes matches pod. Pod admit fails because covering quota is missing for PriorityClass scope and BestEffort scope",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")), "cluster-services"),
			quota:       &corev1.ResourceQuota{},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{BestEffort Exists []} {PriorityClass In [cluster-services]}]",
		},
		{
			description: "Two scopes,BestEffort and PriorityClassIN, in two LimitedResources. Both the scopes matches pod. Quota available only for BestEffort scope. Pod admit fails because covering quota is missing for PriorityClass scope",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")), "cluster-services"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota-besteffort", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
				},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						corev1.ResourcePods: resource.MustParse("5"),
					},
					Used: corev1.ResourceList{
						corev1.ResourcePods: resource.MustParse("3"),
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{PriorityClass In [cluster-services]}]",
		},
		{
			description: "Two scopes,BestEffort and PriorityClassIN, in two LimitedResources. Both the scopes matches pod. Quota available only for PriorityClass scope. Pod admit fails because covering quota is missing for BestEffort scope",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")), "cluster-services"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{BestEffort Exists []}]",
		},
		{
			description: "Two scopes,BestEffort and PriorityClassIN, in two LimitedResources. Both the scopes matches pod. Quota available only for both the scopes. Pod admit success. No Error",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("", ""), getResourceList("", "")), "cluster-services"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota-besteffort", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
				},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						corev1.ResourcePods: resource.MustParse("5"),
					},
					Used: corev1.ResourceList{
						corev1.ResourcePods: resource.MustParse("3"),
					},
				},
			},
			anotherQuota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopeBestEffort,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			description: "Pod allowed with priorityclass if limited scope PriorityClassNameExists not configured.",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "fake-priority"),
			quota:       &corev1.ResourceQuota{},
			config:      &resourcequotaapi.Configuration{},
			expErr:      "",
		},
		{
			description: "quota fails, though covering quota for configured limited scope PriorityClassNameExists exists.",
			testPod:     validPodWithPriority("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "20Gi"), getResourceList("", "")), "fake-priority"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists},
						},
					},
				},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("10Gi"),
					},
					Used: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists,
							},
						},
					},
				},
			},
			expErr: "forbidden: exceeded quota: quota, requested: memory=20Gi, used: memory=1Gi, limited: memory=10Gi",
		},
		{
			description: "Pod has different priorityclass than configured limited. Covering quota exists for configured limited scope PriorityClassIn.",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "fake-priority"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			description: "Pod has limited priorityclass. Covering quota exists for configured limited scope PriorityClassIn.",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "cluster-services"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"cluster-services"},
							},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"another-priorityclass-name", "cluster-services"},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			description: "Pod has limited priorityclass. Covering quota  does not exist for configured limited scope PriorityClassIn.",
			testPod:     validPodWithPriority("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "cluster-services"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"another-priorityclass-name"},
							},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"another-priorityclass-name", "cluster-services"},
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{PriorityClass In [another-priorityclass-name cluster-services]}]",
		},
		{
			description: "From the above test case, just changing pod priority from cluster-services to another-priorityclass-name. expecting no error",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "another-priorityclass-name"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"another-priorityclass-name"},
							},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"another-priorityclass-name", "cluster-services"},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			description: "Pod has limited priorityclass. Covering quota does NOT exists for configured limited scope PriorityClassIn.",
			testPod:     validPodWithPriority("not-allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "cluster-services"),
			quota:       &corev1.ResourceQuota{},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"another-priorityclass-name", "cluster-services"},
							},
						},
					},
				},
			},
			expErr: "insufficient quota to match these scopes: [{PriorityClass In [another-priorityclass-name cluster-services]}]",
		},
		{
			description: "Pod has limited priorityclass. Covering quota exists for configured limited scope PriorityClassIn through PriorityClassNameExists",
			testPod:     validPodWithPriority("allowed-pod", 1, getResourceRequirements(getResourceList("3", "2Gi"), getResourceList("", "")), "cluster-services"),
			quota: &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: "test", ResourceVersion: "124"},
				Spec: corev1.ResourceQuotaSpec{
					ScopeSelector: &corev1.ScopeSelector{
						MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpExists},
						},
					},
				},
			},
			config: &resourcequotaapi.Configuration{
				LimitedResources: []resourcequotaapi.LimitedResource{
					{
						Resource: "pods",
						MatchScopes: []corev1.ScopedResourceSelectorRequirement{
							{
								ScopeName: corev1.ResourceQuotaScopePriorityClass,
								Operator:  corev1.ScopeSelectorOpIn,
								Values:    []string{"another-priorityclass-name", "cluster-services"},
							},
						},
					},
				},
			},
			expErr: "",
		},
	}

	for _, testCase := range testCases {
		newPod := testCase.testPod
		config := testCase.config
		resourceQuota := testCase.quota
		kubeClient := fake.NewSimpleClientset(resourceQuota)
		if testCase.anotherQuota != nil {
			kubeClient = fake.NewSimpleClientset(resourceQuota, testCase.anotherQuota)
		}
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{"namespace": cache.MetaNamespaceIndexFunc})
		stopCh := make(chan struct{})
		defer close(stopCh)

		informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

		handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
		if err != nil {
			t.Errorf("Error occurred while creating admission plugin: %v", err)
		}

		indexer.Add(resourceQuota)
		if testCase.anotherQuota != nil {
			indexer.Add(testCase.anotherQuota)
		}
		err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newPod, nil, api.Kind("Pod").WithVersion("version"), newPod.Namespace, newPod.Name, corev1.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil), nil)
		if testCase.expErr == "" {
			if err != nil {
				t.Fatalf("Testcase, %v, failed with unexpected error: %v. ExpErr: %v", testCase.description, err, testCase.expErr)
			}
		} else {
			if !strings.Contains(fmt.Sprintf("%v", err), testCase.expErr) {
				t.Fatalf("Testcase, %v, failed with unexpected error: %v. ExpErr: %v", testCase.description, err, testCase.expErr)
			}
		}

	}
}

// TestAdmitZeroDeltaUsageWithoutCoveringQuota 验证用量增量为零的更新无需覆盖配额。
func TestAdmitZeroDeltaUsageWithoutCoveringQuota(t *testing.T) {

	kubeClient := fake.NewSimpleClientset()
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止创建 Service。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "services",
				MatchContains: []string{"services.loadbalancers"},
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	existingService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test", ResourceVersion: "1"},
		Spec:       api.ServiceSpec{Type: api.ServiceTypeLoadBalancer},
	}
	newService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test"},
		Spec:       api.ServiceSpec{Type: api.ServiceTypeLoadBalancer},
	}

	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newService, existingService, api.Kind("Service").WithVersion("version"), newService.Namespace, newService.Name, corev1.Resource("services").WithVersion("version"), "", admission.Update, &metav1.CreateOptions{}, false, nil), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAdmitRejectIncreaseUsageWithoutCoveringQuota 验证增加用量的更新必须有覆盖配额，否则被拒绝。
func TestAdmitRejectIncreaseUsageWithoutCoveringQuota(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止创建 Service。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "services",
				MatchContains: []string{"services.loadbalancers"},
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	existingService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test", ResourceVersion: "1"},
		Spec: api.ServiceSpec{
			Type:  api.ServiceTypeNodePort,
			Ports: []api.ServicePort{{Port: 1234}},
		},
	}
	newService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test"},
		Spec:       api.ServiceSpec{Type: api.ServiceTypeLoadBalancer},
	}

	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newService, existingService, api.Kind("Service").WithVersion("version"), newService.Namespace, newService.Name, corev1.Resource("services").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)
	if err == nil {
		t.Errorf("Expected an error for consuming a limited resource without quota.")
	}
}

// TestAdmitAllowDecreaseUsageWithoutCoveringQuota 验证减少用量的更新无需覆盖配额。
func TestAdmitAllowDecreaseUsageWithoutCoveringQuota(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	// 没有覆盖配额时禁止创建 Service。
	config := &resourcequotaapi.Configuration{
		LimitedResources: []resourcequotaapi.LimitedResource{
			{
				Resource:      "services",
				MatchContains: []string{"services.loadbalancers"},
			},
		},
	}

	handler, err := createHandlerWithConfig(kubeClient, informerFactory, config, stopCh)
	if err != nil {
		t.Errorf("Error occurred while creating admission plugin: %v", err)
	}

	existingService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test", ResourceVersion: "1"},
		Spec:       api.ServiceSpec{Type: api.ServiceTypeLoadBalancer},
	}
	newService := &api.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "test"},
		Spec: api.ServiceSpec{
			Type:  api.ServiceTypeNodePort,
			Ports: []api.ServicePort{{Port: 1234}},
		},
	}

	err = handler.Validate(context.TODO(), admission.NewAttributesRecord(newService, existingService, api.Kind("Service").WithVersion("version"), newService.Namespace, newService.Name, corev1.Resource("services").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil), nil)

	if err != nil {
		t.Errorf("Expected no error for decreasing a limited resource without quota, got %v", err)
	}
}
