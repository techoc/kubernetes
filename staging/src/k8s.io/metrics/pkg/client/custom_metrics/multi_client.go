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

package custom_metrics

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

// AvailableAPIsGetter 负责获取并缓存首选自定义指标 API 版本，并按需使缓存失效。
// 把读版本与失效拆开，调用方自行决定刷新节奏，不必每次请求都做服务发现。
type AvailableAPIsGetter interface {
	// PreferredVersion 返回当前首选版本；缓存为空时从服务发现获取。
	PreferredVersion() (schema.GroupVersion, error)
	// Invalidate 清除首选版本缓存，使下次读取重新发现。
	Invalidate()
}

// PeriodicallyInvalidate 周期性地使首选版本缓存失效，直到收到停止信号。
// 通常跑在后台 goroutine；stopCh 关闭即退出，避免长生命周期客户端泄漏 goroutine。
func PeriodicallyInvalidate(cache AvailableAPIsGetter, interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cache.Invalidate()
		case <-stopCh:
			return
		}
	}
}

// NewForConfig 创建自定义指标客户端，实际查询委托给服务端首选 API 版本对应的客户端。
// 每个版本客户端拿独立的 rest.Config 副本，避免配置被后续修改牵连；版本在首次请求时才确定。
func NewForConfig(baseConfig *rest.Config, mapper meta.RESTMapper, availableAPIs AvailableAPIsGetter) CustomMetricsClient {
	return &multiClient{
		clients:       make(map[schema.GroupVersion]CustomMetricsClient),
		availableAPIs: availableAPIs,

		newClient: func(ver schema.GroupVersion) (CustomMetricsClient, error) {
			return NewForVersionForConfig(rest.CopyConfig(baseConfig), mapper, ver)
		},
	}
}

// multiClient 是可与任何自定义指标 API 版本协作的 CustomMetricsClient。
// 按 GroupVersion 缓存已创建的客户端，版本切换时无需重建整个客户端。
type multiClient struct {
	newClient     func(schema.GroupVersion) (CustomMetricsClient, error)
	clients       map[schema.GroupVersion]CustomMetricsClient
	availableAPIs AvailableAPIsGetter
	mu            sync.RWMutex
}

// getPreferredClient 返回使用首选 API 版本的自定义指标客户端。
// 缓存未命中时在写锁内创建并写入，串行化该客户端的初始化；已缓存版本直接复用。
func (c *multiClient) getPreferredClient() (CustomMetricsClient, error) {
	pref, err := c.availableAPIs.PreferredVersion()
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	client, present := c.clients[pref]
	c.mu.RUnlock()
	if present {
		return client, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	client, err = c.newClient(pref)
	if err != nil {
		return nil, err
	}
	c.clients[pref] = client

	return client, nil
}

// RootScopedMetrics 返回集群作用域视图；版本选择推迟到实际请求时。
func (c *multiClient) RootScopedMetrics() MetricsInterface {
	return &multiClientInterface{clients: c}
}

// NamespacedMetrics 返回指定命名空间的视图；namespace 只在这里记下，请求时才转发。
func (c *multiClient) NamespacedMetrics(namespace string) MetricsInterface {
	return &multiClientInterface{
		clients:   c,
		namespace: &namespace,
	}
}

// multiClientInterface 只携带命名空间，每次请求重新取当前首选版本的客户端，
// 这样版本失效后无需重建视图对象。
type multiClientInterface struct {
	clients   *multiClient
	namespace *string
}

// GetForObject 按当前版本转发单对象查询；namespace 为空时走集群作用域接口。
func (m *multiClientInterface) GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error) {
	client, err := m.clients.getPreferredClient()
	if err != nil {
		return nil, err
	}
	if m.namespace == nil {
		return client.RootScopedMetrics().GetForObject(groupKind, name, metricName, metricSelector)
	} else {
		return client.NamespacedMetrics(*m.namespace).GetForObject(groupKind, name, metricName, metricSelector)
	}
}

// GetForObjects 与 GetForObject 同理，按当前版本和作用域转发批量查询。
func (m *multiClientInterface) GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValueList, error) {
	client, err := m.clients.getPreferredClient()
	if err != nil {
		return nil, err
	}
	if m.namespace == nil {
		return client.RootScopedMetrics().GetForObjects(groupKind, selector, metricName, metricSelector)
	} else {
		return client.NamespacedMetrics(*m.namespace).GetForObjects(groupKind, selector, metricName, metricSelector)
	}
}
