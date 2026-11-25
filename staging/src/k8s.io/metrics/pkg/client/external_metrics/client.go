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

package external_metrics

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/metrics/pkg/apis/external_metrics/v1beta1"
)

// externalMetricsClient 是对 rest.Interface 的薄封装，便于替换底层传输做测试。
type externalMetricsClient struct {
	client rest.Interface
}

// New 基于给定的 REST 客户端创建外部指标客户端。
func New(client rest.Interface) ExternalMetricsClient {
	return &externalMetricsClient{
		client: client,
	}
}

// NewForConfig 修改配置的副本，避免调用方传入的 Config 被限流器和序列化设置污染。
func NewForConfig(c *rest.Config) (ExternalMetricsClient, error) {
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
	configShallowCopy.GroupVersion = &v1beta1.SchemeGroupVersion
	configShallowCopy.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	client, err := rest.RESTClientFor(&configShallowCopy)
	if err != nil {
		return nil, err
	}

	return New(client), nil
}

// NewForConfigOrDie 用于配置错误即为程序初始化失败的场景，例如启动时构建客户端。
func NewForConfigOrDie(c *rest.Config) ExternalMetricsClient {
	client, err := NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return client
}

// NamespacedMetrics 返回绑定指定命名空间的指标查询接口。
func (c *externalMetricsClient) NamespacedMetrics(namespace string) MetricsInterface {
	return &namespacedMetrics{
		client:    c,
		namespace: namespace,
	}
}

// namespacedMetrics 把 namespace 固化到接口实例，调用方只需关心指标名和选择器。
type namespacedMetrics struct {
	client    *externalMetricsClient
	namespace string
}

// List 直接把指标名当作资源路径，因为外部指标 API 没有对应的 Kubernetes 对象类型。
func (m *namespacedMetrics) List(metricName string, metricSelector labels.Selector) (*v1beta1.ExternalMetricValueList, error) {
	res := &v1beta1.ExternalMetricValueList{}
	err := m.client.client.Get().
		Namespace(m.namespace).
		Resource(metricName).
		VersionedParams(&metav1.ListOptions{
			LabelSelector: metricSelector.String(),
		}, metav1.ParameterCodec).
		Do(context.TODO()).
		Into(res)

	if err != nil {
		return nil, err
	}

	return res, nil
}
