/*
Copyright 2017 The Kubernetes Authors.

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

package fake

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/testing"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
	cmclient "k8s.io/metrics/pkg/client/custom_metrics"
)

// GetForActionImpl 把指标名和标签选择器塞进标准 Get action。
// 这样 fake 的 reactor 才能像真实 API 那样按子资源（指标名）区分请求。
type GetForActionImpl struct {
	testing.GetAction
	MetricName    string
	LabelSelector labels.Selector
}

// GetForAction 暴露自定义指标查询特有的参数，便于 reactor 对 Action 做类型断言。
type GetForAction interface {
	testing.GetAction
	GetMetricName() string
	GetLabelSelector() labels.Selector
}

func (i GetForActionImpl) GetMetricName() string {
	return i.MetricName
}

func (i GetForActionImpl) GetLabelSelector() labels.Selector {
	return i.LabelSelector
}

// GetSubresource 让指标名在 action 层面充当子资源，匹配真实的自定义指标 URL 形态。
func (i GetForActionImpl) GetSubresource() string {
	return i.MetricName
}

// DeepCopy 单独复制标签选择器：它是接口，沿用同一个引用会让测试 reactor 意外共享可变状态。
func (i GetForActionImpl) DeepCopy() testing.Action {
	var labelSelector labels.Selector
	if i.LabelSelector != nil {
		labelSelector = i.LabelSelector.DeepCopySelector()
	}
	return GetForActionImpl{
		GetAction:     i.GetAction.DeepCopy().(testing.GetAction),
		MetricName:    i.MetricName,
		LabelSelector: labelSelector,
	}
}

// NewGetForAction 构造 namespaced 查询对应的 action。
// 复用的客户端只认 v1beta2 端点，因此这里统一套一层 v1beta2 的 group 名。
func NewGetForAction(groupKind schema.GroupKind, namespace, name string, metricName string, labelSelector labels.Selector) GetForActionImpl {
	// the version doesn't matter
	gvk := groupKind.WithVersion("")
	gvr, _ := meta.UnsafeGuessKindToResource(gvk)
	groupResourceForKind := schema.GroupResource{
		Group:    gvr.Group,
		Resource: gvr.Resource,
	}
	resource := schema.GroupResource{
		Group:    v1beta2.SchemeGroupVersion.Group,
		Resource: groupResourceForKind.String(),
	}
	return GetForActionImpl{
		GetAction:     testing.NewGetAction(resource.WithVersion(""), namespace, name),
		MetricName:    metricName,
		LabelSelector: labelSelector,
	}
}

// NewRootGetForAction 构造集群级（无 namespace）查询对应的 action。
func NewRootGetForAction(groupKind schema.GroupKind, name string, metricName string, labelSelector labels.Selector) GetForActionImpl {
	// the version doesn't matter
	gvk := groupKind.WithVersion("")
	gvr, _ := meta.UnsafeGuessKindToResource(gvk)
	groupResourceForKind := schema.GroupResource{
		Group:    gvr.Group,
		Resource: gvr.Resource,
	}
	resource := schema.GroupResource{
		Group:    v1beta2.SchemeGroupVersion.Group,
		Resource: groupResourceForKind.String(),
	}
	return GetForActionImpl{
		GetAction:     testing.NewRootGetAction(resource.WithVersion(""), name),
		MetricName:    metricName,
		LabelSelector: labelSelector,
	}
}

// FakeCustomMetricsClient 只记录 action，不实现指标查询逻辑；
// 结果由测试通过 PrependReactor 提供，方便校验请求参数而不是校验数据本身。
type FakeCustomMetricsClient struct {
	testing.Fake
}

func (c *FakeCustomMetricsClient) RootScopedMetrics() cmclient.MetricsInterface {
	return &fakeRootScopedMetrics{
		Fake: c,
	}
}

func (c *FakeCustomMetricsClient) NamespacedMetrics(namespace string) cmclient.MetricsInterface {
	return &fakeNamespacedMetrics{
		Fake: c,
		ns:   namespace,
	}
}

// fakeNamespacedMetrics 把 namespace 预先绑定到接口上，调用方不必每个方法重复传。
type fakeNamespacedMetrics struct {
	Fake *FakeCustomMetricsClient
	ns   string
}

// GetForObject 强制要求 reactor 返回恰好一项：单对象查询返回多项说明适配器实现有问题。
func (m *fakeNamespacedMetrics) GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error) {
	obj, err := m.Fake.
		Invokes(NewGetForAction(groupKind, m.ns, name, metricName, nil), &v1beta2.MetricValueList{})

	if obj == nil {
		return nil, err
	}

	objList := obj.(*v1beta2.MetricValueList)
	if len(objList.Items) != 1 {
		return nil, fmt.Errorf("the custom metrics API server returned %v results when we asked for exactly one", len(objList.Items))
	}

	return &objList.Items[0], err
}

func (m *fakeNamespacedMetrics) GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValueList, error) {
	obj, err := m.Fake.
		Invokes(NewGetForAction(groupKind, m.ns, "*", metricName, selector), &v1beta2.MetricValueList{})

	if obj == nil {
		return nil, err
	}

	return obj.(*v1beta2.MetricValueList), err
}

// fakeRootScopedMetrics 走 RootGet action，对应不带 namespace 的集群级指标路径。
type fakeRootScopedMetrics struct {
	Fake *FakeCustomMetricsClient
}

func (m *fakeRootScopedMetrics) GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error) {
	obj, err := m.Fake.
		Invokes(NewRootGetForAction(groupKind, name, metricName, nil), &v1beta2.MetricValueList{})

	if obj == nil {
		return nil, err
	}

	objList := obj.(*v1beta2.MetricValueList)
	if len(objList.Items) != 1 {
		return nil, fmt.Errorf("the custom metrics API server returned %v results when we asked for exactly one", len(objList.Items))
	}

	return &objList.Items[0], err
}

func (m *fakeRootScopedMetrics) GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValueList, error) {
	obj, err := m.Fake.
		Invokes(NewRootGetForAction(groupKind, "*", metricName, selector), &v1beta2.MetricValueList{})

	if obj == nil {
		return nil, err
	}

	return obj.(*v1beta2.MetricValueList), err
}
