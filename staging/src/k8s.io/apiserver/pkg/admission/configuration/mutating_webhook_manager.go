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

package configuration

import (
	"fmt"
	"sort"
	"sync"

	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
	"k8s.io/client-go/informers"
	admissionregistrationlisters "k8s.io/client-go/listers/admissionregistration/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/cache/synctrack"
	"k8s.io/klog/v2"
)

// Type for test injection.
//
// 作为字段而非直接调用包函数，测试可替换为桩实现来断言 UID 生成逻辑。
type mutatingWebhookAccessorCreator func(uid string, configurationName string, h *v1.MutatingWebhook) webhook.WebhookAccessor

// mutatingWebhookConfigurationManager collects the mutating webhook objects so that they can be called.
//
// 该类型实现 generic.Source：把 MutatingWebhookConfiguration informer 的内容
// 转换成按确定顺序排列的 accessor 列表。lazy 负责缓存整体结果，
// configurationsCache 则按配置名缓存单个配置的 accessor，避免无关配置变更导致全量重建。
type mutatingWebhookConfigurationManager struct {
	lister              admissionregistrationlisters.MutatingWebhookConfigurationLister
	hasSynced           func() bool
	lazy                synctrack.Lazy[[]webhook.WebhookAccessor]
	configurationsCache sync.Map
	// createMutatingWebhookAccessor is used to instantiate webhook accessors.
	// This function is defined as field instead of a struct method to allow injection
	// during tests
	createMutatingWebhookAccessor mutatingWebhookAccessorCreator
	excludedWebhookResources      sets.Set[schema.GroupResource]
}

// 编译期断言：管理器必须满足 generic.Source。
var _ generic.Source = &mutatingWebhookConfigurationManager{}

// NewMutatingWebhookConfigurationManager 基于 informer 构造管理器，并注册事件处理：
// 任何增删改都先失效对应缓存条目，再通知 lazy 重算。
func NewMutatingWebhookConfigurationManager(f informers.SharedInformerFactory, excludedWebhookResources sets.Set[schema.GroupResource]) generic.Source {
	informer := f.Admissionregistration().V1().MutatingWebhookConfigurations()
	manager := &mutatingWebhookConfigurationManager{
		lister:                        informer.Lister(),
		createMutatingWebhookAccessor: webhook.NewMutatingWebhookAccessor,
		excludedWebhookResources:      excludedWebhookResources,
	}
	manager.lazy.Evaluate = manager.getConfiguration

	handle, _ := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(_ interface{}) { manager.lazy.Notify() },
		UpdateFunc: func(old, new interface{}) {
			obj := new.(*v1.MutatingWebhookConfiguration)
			manager.configurationsCache.Delete(obj.GetName())
			manager.lazy.Notify()
		},
		DeleteFunc: func(obj interface{}) {
			// informer 可能以 tombstone 形式投递删除事件（例如 relist 后发现对象消失），
			// 需要先解包才能拿到名字去失效缓存。
			vwc, ok := obj.(*v1.MutatingWebhookConfiguration)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.V(2).Infof("Couldn't get object from tombstone %#v", obj)
					return
				}
				vwc, ok = tombstone.Obj.(*v1.MutatingWebhookConfiguration)
				if !ok {
					klog.V(2).Infof("Tombstone contained object that is not expected %#v", obj)
					return
				}
			}
			manager.configurationsCache.Delete(vwc.Name)
			manager.lazy.Notify()
		},
	})
	manager.hasSynced = handle.HasSynced

	return manager
}

// Webhooks returns the merged MutatingWebhookConfiguration.
//
// 出错时不中断请求，仅记录错误并返回可能为空的列表——准入链必须在配置异常时保持可用。
func (m *mutatingWebhookConfigurationManager) Webhooks() []webhook.WebhookAccessor {
	out, err := m.lazy.Get()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error getting webhook configuration: %v", err))
	}
	return out
}

// HasSynced returns true if the initial set of mutating webhook configurations
// has been loaded.
func (m *mutatingWebhookConfigurationManager) HasSynced() bool { return m.hasSynced() }

// getConfiguration 从 lister 全量读取并转换，是 lazy 的求值函数。
func (m *mutatingWebhookConfigurationManager) getConfiguration() ([]webhook.WebhookAccessor, error) {
	configurations, err := m.lister.List(labels.Everything())
	if err != nil {
		return []webhook.WebhookAccessor{}, err
	}
	return m.getMutatingWebhookConfigurations(configurations), nil
}

// getMutatingWebhookConfigurations returns the webhook accessors for a given list of
// mutating webhook configurations.
//
// This function will, first, try to load the webhook accessors from the cache and avoid
// recreating them, which can be expessive (requiring CEL expression recompilation).
//
// 注意：这里会对传入切片做原地排序，调用方需确保该切片归本函数所有。
func (m *mutatingWebhookConfigurationManager) getMutatingWebhookConfigurations(configurations []*v1.MutatingWebhookConfiguration) []webhook.WebhookAccessor {
	// The internal order of webhooks for each configuration is provided by the user
	// but configurations themselves can be in any order. As we are going to run these
	// webhooks in serial, they are sorted here to have a deterministic order.
	//
	// 配置之间按名字排序，保证多个 webhook 串行执行时顺序稳定、可复现。
	sort.SliceStable(configurations, MutatingWebhookConfigurationSorter(configurations).ByName)
	size := 0
	for _, cfg := range configurations {
		size += len(cfg.Webhooks)
	}
	// 预分配容量，避免追加时多次扩容。
	accessors := make([]webhook.WebhookAccessor, 0, size)

	for _, c := range configurations {
		cachedConfigurationAccessors, ok := m.configurationsCache.Load(c.Name)
		if ok {
			// Pick an already cached webhookAccessor
			accessors = append(accessors, cachedConfigurationAccessors.([]webhook.WebhookAccessor)...)
			continue
		}

		logExcludedResourcesForMutatingWebhook(c.Name, c.Webhooks, m.excludedWebhookResources)

		// webhook names are not validated for uniqueness, so we check for duplicates and
		// add a int suffix to distinguish between them
		//
		// UID 形如 "<config>/<webhook>/<n>"，n 为同名 webhook 的出现序号；
		// 该 UID 会用于指标与日志，必须稳定可复现。
		names := map[string]int{}
		configurationAccessors := make([]webhook.WebhookAccessor, 0, len(c.Webhooks))
		for i := range c.Webhooks {
			n := c.Webhooks[i].Name
			uid := fmt.Sprintf("%s/%s/%d", c.Name, n, names[n])
			names[n]++
			configurationAccessor := m.createMutatingWebhookAccessor(uid, c.Name, &c.Webhooks[i])
			configurationAccessors = append(configurationAccessors, configurationAccessor)
		}
		accessors = append(accessors, configurationAccessors...)
		m.configurationsCache.Store(c.Name, configurationAccessors)
	}
	return accessors
}

// MutatingWebhookConfigurationSorter 提供按配置名排序的比较函数。
type MutatingWebhookConfigurationSorter []*v1.MutatingWebhookConfiguration

// ByName 按名字字典序比较，用于 sort.Slice 的 less 回调。
func (a MutatingWebhookConfigurationSorter) ByName(i, j int) bool {
	return a[i].Name < a[j].Name
}
