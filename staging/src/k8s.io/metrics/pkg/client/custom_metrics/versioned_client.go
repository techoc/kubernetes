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

package custom_metrics

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"

	cmint "k8s.io/metrics/pkg/apis/custom_metrics"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta1"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
	"k8s.io/metrics/pkg/client/custom_metrics/scheme"
)

// versionConverter 全局共享一份即可：它只做无状态的结构转换，无需按客户端实例重复构造。
var versionConverter = NewMetricConverter()

// customMetricsClient 的 mapper 用来把 GroupKind 解析成 REST 资源名，
// 因为指标 API 的路径是“资源/对象/指标”，而调用方只给出 Kind。
type customMetricsClient struct {
	client  rest.Interface
	version schema.GroupVersion
	mapper  meta.RESTMapper
}

// NewForVersion 为指定 API 版本创建 CustomMetricsClient。
// 版本信息留在客户端里，后续请求和结果转换都按该版本协商。
func NewForVersion(client rest.Interface, mapper meta.RESTMapper, version schema.GroupVersion) CustomMetricsClient {
	return &customMetricsClient{
		client:  client,
		version: version,
		mapper:  mapper,
	}
}

// NewForVersionForConfig 基于基础配置为指定 API 版本创建 CustomMetricsClient。
// 这里复制一份配置再改，避免把 GroupVersion 等设置写回调用方持有的 Config。
// 使用 WithoutConversion 是因为转换由 versionConverter 显式完成，
// 编码器只负责按选定版本读写线上格式。
func NewForVersionForConfig(c *rest.Config, mapper meta.RESTMapper, version schema.GroupVersion) (CustomMetricsClient, error) {
	configShallowCopy := *c
	if configShallowCopy.RateLimiter == nil && configShallowCopy.QPS > 0 {
		if configShallowCopy.Burst <= 0 {
			return nil, fmt.Errorf("burst is required to be greater than 0 when RateLimiter is not set and QPS is set to greater than 0")
		}
		configShallowCopy.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(configShallowCopy.QPS, configShallowCopy.Burst)
	}
	configShallowCopy.APIPath = "/apis"
	if configShallowCopy.UserAgent == "" {
		configShallowCopy.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	configShallowCopy.GroupVersion = &version
	configShallowCopy.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	client, err := rest.RESTClientFor(&configShallowCopy)
	if err != nil {
		return nil, err
	}

	return NewForVersion(client, mapper, version), nil
}

// RootScopedMetrics 返回一个把请求发往集群级资源路径的视图。
// 命名空间本身也算集群级对象，因此查询某个 Namespace 的指标应走这里。
func (c *customMetricsClient) RootScopedMetrics() MetricsInterface {
	return &rootScopedMetrics{c}
}

// NamespacedMetrics 返回一个绑定了具体命名空间的视图，
// 后续查询会自动带上 Namespace 路径段，调用方不必重复指定。
func (c *customMetricsClient) NamespacedMetrics(namespace string) MetricsInterface {
	return &namespacedMetrics{
		client:    c,
		namespace: namespace,
	}
}

// qualResourceForKind 返回指定 GroupKind 对应的限定 group/resource 字符串。
// 指标 API 用“资源名/对象名/指标名”定位一条指标，所以必须先把 Kind 映射为资源名。
// 走 mapper 而不是硬编码，是为了兼容 CRD 等带自定义 group 的对象。
func (c *customMetricsClient) qualResourceForKind(groupKind schema.GroupKind) (string, error) {
	mapping, err := c.mapper.RESTMapping(groupKind)
	if err != nil {
		return "", fmt.Errorf("unable to map kind %s to resource: %v", groupKind.String(), err)
	}

	gr := mapping.Resource.GroupResource()
	return gr.String(), nil
}

// rootScopedMetrics 持有集群级客户端；单独查询某个 Namespace 时走特殊路径。
type rootScopedMetrics struct {
	client *customMetricsClient
}

// getForNamespace 把 Namespace 当作 metrics 子资源查询，
// 因为命名空间没有常规的“资源/名称/子资源”层级。
func (m *rootScopedMetrics) getForNamespace(namespace string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error) {
	params, err := versionConverter.ConvertListOptionsToVersion(&cmint.MetricListOptions{
		MetricLabelSelector: metricSelector.String(),
	}, m.client.version)
	if err != nil {
		return nil, err
	}

	result := m.client.client.Get().
		Resource("metrics").
		Namespace(namespace).
		Name(metricName).
		VersionedParams(params, scheme.ParameterCodec).
		Do(context.TODO())

	metricObj, err := versionConverter.ConvertResultToVersion(result, v1beta2.SchemeGroupVersion)
	if err != nil {
		return nil, err
	}

	var res *v1beta2.MetricValueList
	var ok bool
	if res, ok = metricObj.(*v1beta2.MetricValueList); !ok {
		return nil, fmt.Errorf("the custom metrics API server didn't return MetricValueList, the type is %v", reflect.TypeOf(metricObj))
	}
	if len(res.Items) != 1 {
		return nil, fmt.Errorf("the custom metrics API server returned %v results when we asked for exactly one", len(res.Items))
	}

	return &res.Items[0], nil
}

// GetForObject 取单个对象的一条指标；API 返回列表，这里要求恰好一条，
// 否则说明服务端行为不符合单对象查询的约定，直接报错比悄悄取第一条更安全。
func (m *rootScopedMetrics) GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error) {
	// Namespace 需要单独处理。
	// Namespace 既是集群级对象，又有专用路径，不能按普通子资源拼接。
	if groupKind.Kind == "Namespace" && groupKind.Group == "" {
		return m.getForNamespace(name, metricName, metricSelector)
	}

	resourceName, err := m.client.qualResourceForKind(groupKind)
	if err != nil {
		return nil, err
	}

	params, err := versionConverter.ConvertListOptionsToVersion(&cmint.MetricListOptions{
		MetricLabelSelector: metricSelector.String(),
	}, m.client.version)
	if err != nil {
		return nil, err
	}

	result := m.client.client.Get().
		Resource(resourceName).
		Name(name).
		SubResource(metricName).
		VersionedParams(params, scheme.ParameterCodec).
		Do(context.TODO())

	metricObj, err := versionConverter.ConvertResultToVersion(result, v1beta2.SchemeGroupVersion)
	if err != nil {
		return nil, err
	}

	var res *v1beta2.MetricValueList
	var ok bool
	if res, ok = metricObj.(*v1beta2.MetricValueList); !ok {
		return nil, fmt.Errorf("the custom metrics API server didn't return MetricValueList, the type is %v", reflect.TypeOf(metricObj))
	}
	if len(res.Items) != 1 {
		return nil, fmt.Errorf("the custom metrics API server returned %v results when we asked for exactly one", len(res.Items))
	}

	return &res.Items[0], nil
}

// GetForObjects 通过 AllObjects 通配符一次取回多个对象的同指标值。
// Namespace 没有批量查询入口，因此显式拒绝，避免调用方误以为得到了全部命名空间。
func (m *rootScopedMetrics) GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValueList, error) {
	// 不能对命名空间做通配批量查询。
	if groupKind.Kind == "Namespace" && groupKind.Group == "" {
		return nil, fmt.Errorf("cannot fetch metrics for multiple namespaces at once")
	}

	resourceName, err := m.client.qualResourceForKind(groupKind)
	if err != nil {
		return nil, err
	}

	params, err := versionConverter.ConvertListOptionsToVersion(&cmint.MetricListOptions{
		LabelSelector:       selector.String(),
		MetricLabelSelector: metricSelector.String(),
	}, m.client.version)
	if err != nil {
		return nil, err
	}

	result := m.client.client.Get().
		Resource(resourceName).
		Name(v1beta1.AllObjects).
		SubResource(metricName).
		VersionedParams(params, scheme.ParameterCodec).
		Do(context.TODO())

	metricObj, err := versionConverter.ConvertResultToVersion(result, v1beta2.SchemeGroupVersion)
	if err != nil {
		return nil, err
	}

	var res *v1beta2.MetricValueList
	var ok bool
	if res, ok = metricObj.(*v1beta2.MetricValueList); !ok {
		return nil, fmt.Errorf("the custom metrics API server didn't return MetricValueList, the type is %v", reflect.TypeOf(metricObj))
	}
	return res, nil
}

// namespacedMetrics 把命名空间固化在视图里，避免每次查询都传一遍 namespace。
type namespacedMetrics struct {
	client    *customMetricsClient
	namespace string
}

// GetForObject 取命名空间内单个对象的一条指标；
// 与集群级查询一样要求结果恰好一条，多或少都视为服务端异常。
func (m *namespacedMetrics) GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error) {
	resourceName, err := m.client.qualResourceForKind(groupKind)
	if err != nil {
		return nil, err
	}

	params, err := versionConverter.ConvertListOptionsToVersion(&cmint.MetricListOptions{
		MetricLabelSelector: metricSelector.String(),
	}, m.client.version)
	if err != nil {
		return nil, err
	}

	result := m.client.client.Get().
		Resource(resourceName).
		Namespace(m.namespace).
		Name(name).
		SubResource(metricName).
		VersionedParams(params, scheme.ParameterCodec).
		Do(context.TODO())

	metricObj, err := versionConverter.ConvertResultToVersion(result, v1beta2.SchemeGroupVersion)
	if err != nil {
		return nil, err
	}

	var res *v1beta2.MetricValueList
	var ok bool
	if res, ok = metricObj.(*v1beta2.MetricValueList); !ok {
		return nil, fmt.Errorf("the custom metrics API server didn't return MetricValueList, the type is %v", reflect.TypeOf(metricObj))
	}
	if len(res.Items) != 1 {
		return nil, fmt.Errorf("the custom metrics API server returned %v results when we asked for exactly one", len(res.Items))
	}

	return &res.Items[0], nil
}

// GetForObjects 用 AllObjects 通配符批量取指标；
// 对象选择器和指标选择器分别过滤“哪些对象”和“哪条指标”，两者语义不同。
func (m *namespacedMetrics) GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValueList, error) {
	resourceName, err := m.client.qualResourceForKind(groupKind)
	if err != nil {
		return nil, err
	}

	params, err := versionConverter.ConvertListOptionsToVersion(&cmint.MetricListOptions{
		LabelSelector:       selector.String(),
		MetricLabelSelector: metricSelector.String(),
	}, m.client.version)
	if err != nil {
		return nil, err
	}

	result := m.client.client.Get().
		Resource(resourceName).
		Namespace(m.namespace).
		Name(v1beta1.AllObjects).
		SubResource(metricName).
		VersionedParams(params, scheme.ParameterCodec).
		Do(context.TODO())

	metricObj, err := versionConverter.ConvertResultToVersion(result, v1beta2.SchemeGroupVersion)
	if err != nil {
		return nil, err
	}

	var res *v1beta2.MetricValueList
	var ok bool
	if res, ok = metricObj.(*v1beta2.MetricValueList); !ok {
		return nil, fmt.Errorf("the custom metrics API server didn't return MetricValueList, the type is %v", reflect.TypeOf(metricObj))
	}
	return res, nil
}
