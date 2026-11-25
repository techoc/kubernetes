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

// Package configuration 汇总集群中的 mutating/validating webhook 配置，
// 以 generic.Source 的形式提供给准入插件，并负责把配置对象转换为可调用的 webhook accessor。
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

// 该函数类型用于测试注入。
//
// 类型别名仅用于测试注入：把 WebhookAccessor 的构造方式参数化，便于在单测里替换成假实现。
type validatingWebhookAccessorCreator func(uid string, configurationName string, h *v1.ValidatingWebhook) webhook.WebhookAccessor

// validatingWebhookConfigurationManager 收集 validating webhook 对象以便调用。
//
// 它把集群中所有 ValidatingWebhookConfiguration 的 webhook 汇总成一个有序列表供准入插件调用。
// 列表顺序由配置名字典序决定，保证同一批配置在不同 apiserver 实例上得到一致的调用顺序。
type validatingWebhookConfigurationManager struct {
	lister    admissionregistrationlisters.ValidatingWebhookConfigurationLister
	hasSynced func() bool
	// lazy 缓存合并后的 accessor 列表；lister 发生变化时通过 Notify 触发惰性重算，
	// 避免每次请求都全量遍历配置。
	lazy synctrack.Lazy[[]webhook.WebhookAccessor]
	// configurationsCache 以配置名为键缓存单个配置的 accessor 切片。
	// 单条配置未变化时直接复用，避免重复编译 CEL 表达式等昂贵操作。
	configurationsCache sync.Map
	// createValidatingWebhookAccessor 用于实例化 webhook 访问器。把它定义为字段
	// 而非结构体方法，以便在测试中进行注入。
	//
	// 作为字段而非方法存在，是为了在测试中注入桩实现。
	createValidatingWebhookAccessor validatingWebhookAccessorCreator
	// excludedWebhookResources 中的 GroupResource 会跳过 webhook 调用，
	// 主要用于避免系统内部资源触发用户 webhook 造成递归或性能问题。
	excludedWebhookResources sets.Set[schema.GroupResource]
}

// 编译期断言：本类型必须实现 generic.Source 接口。
var _ generic.Source = &validatingWebhookConfigurationManager{}

// NewValidatingWebhookConfigurationManager 创建基于 informer 缓存的 validating webhook 配置管理器。
//
// 返回的 generic.Source 会在 informer 首次同步后提供合并后的 webhook 列表；
// excludedWebhookResources 指定哪些资源不应触发 webhook。
func NewValidatingWebhookConfigurationManager(f informers.SharedInformerFactory, excludedWebhookResources sets.Set[schema.GroupResource]) generic.Source {
	informer := f.Admissionregistration().V1().ValidatingWebhookConfigurations()
	manager := &validatingWebhookConfigurationManager{
		lister:                          informer.Lister(),
		createValidatingWebhookAccessor: webhook.NewValidatingWebhookAccessor,
		excludedWebhookResources:        excludedWebhookResources,
	}
	manager.lazy.Evaluate = manager.getConfiguration

	// 注册事件处理器：任何增删改都让 lazy 失效，并把对应配置从单项缓存中剔除。
	// Update/Delete 主动删缓存而不是依赖 lazy 全量重算，是为了让未变化配置的 accessor 继续复用。
	handle, _ := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(_ interface{}) { manager.lazy.Notify() },
		UpdateFunc: func(old, new interface{}) {
			obj := new.(*v1.ValidatingWebhookConfiguration)
			manager.configurationsCache.Delete(obj.GetName())
			manager.lazy.Notify()
		},
		DeleteFunc: func(obj interface{}) {
			// informer 可能以 DeletedFinalStateUnknown 墓碑形式投递删除事件，
			// 需要从中取出真实对象，否则缓存项会泄漏。
			vwc, ok := obj.(*v1.ValidatingWebhookConfiguration)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.V(2).Infof("Couldn't get object from tombstone %#v", obj)
					return
				}
				vwc, ok = tombstone.Obj.(*v1.ValidatingWebhookConfiguration)
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

// Webhooks 返回合并后的 ValidatingWebhookConfiguration。
//
// 返回按配置名排序后合并的 webhook accessor 列表；出错时记录错误并返回已获得的部分结果。
func (v *validatingWebhookConfigurationManager) Webhooks() []webhook.WebhookAccessor {
	out, err := v.lazy.Get()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error getting webhook configuration: %v", err))
	}
	return out
}

// HasSynced 在初始的 validating webhook 配置集加载完成后返回 true。
//
// 首次同步完成前返回 false，调用方据此决定是否等待 webhook 配置就绪。
func (v *validatingWebhookConfigurationManager) HasSynced() bool { return v.hasSynced() }

// getConfiguration 从 lister 读取全部配置并转换为 accessor 列表，是 lazy 的求值函数。
func (v *validatingWebhookConfigurationManager) getConfiguration() ([]webhook.WebhookAccessor, error) {
	configurations, err := v.lister.List(labels.Everything())
	if err != nil {
		return []webhook.WebhookAccessor{}, err
	}
	return v.getValidatingWebhookConfigurations(configurations), nil
}

// getValidatingWebhookConfigurations 返回给定 validating webhook 配置列表
// 对应的 webhook 访问器（上游原注释误写为 getMutatingWebhook-
// Configurations）。该函数会优先从缓存加载访问器，避免重建带来的
// 开销（重建代价较高，需要重新编译 CEL 表达式）。
//
// 实现要点：先按配置名稳定排序，再逐条配置查缓存；未命中时才重建该配置的 accessor 并回填缓存。
func (v *validatingWebhookConfigurationManager) getValidatingWebhookConfigurations(configurations []*v1.ValidatingWebhookConfiguration) []webhook.WebhookAccessor {
	// 排序保证合并结果顺序确定，不受 informer 返回顺序影响。
	sort.SliceStable(configurations, ValidatingWebhookConfigurationSorter(configurations).ByName)
	size := 0
	for _, cfg := range configurations {
		size += len(cfg.Webhooks)
	}
	accessors := make([]webhook.WebhookAccessor, 0, size)

	for _, c := range configurations {
		cachedConfigurationAccessors, ok := v.configurationsCache.Load(c.Name)
		if ok {
			// 直接使用已缓存的 webhookAccessor。
			accessors = append(accessors, cachedConfigurationAccessors.([]webhook.WebhookAccessor)...)
			continue
		}

		logExcludedResourcesForValidatingWebhook(c.Name, c.Webhooks, v.excludedWebhookResources)

		// webhook 名称没有做唯一性校验，因此这里检查重复并追加整数后缀加以区分。
		//
		// uid 形如 "<配置名>/<webhook名>/<序号>"，序号用于区分同名 webhook。
		names := map[string]int{}
		configurationAccessors := make([]webhook.WebhookAccessor, 0, len(c.Webhooks))
		for i := range c.Webhooks {
			n := c.Webhooks[i].Name
			uid := fmt.Sprintf("%s/%s/%d", c.Name, n, names[n])
			names[n]++
			configurationAccessor := v.createValidatingWebhookAccessor(uid, c.Name, &c.Webhooks[i])
			configurationAccessors = append(configurationAccessors, configurationAccessor)
		}
		accessors = append(accessors, configurationAccessors...)
		v.configurationsCache.Store(c.Name, configurationAccessors)
	}

	return accessors
}

// ValidatingWebhookConfigurationSorter 按配置名对 ValidatingWebhookConfiguration 切片排序。
type ValidatingWebhookConfigurationSorter []*v1.ValidatingWebhookConfiguration

// ByName 是 sort.SliceStable 使用的比较函数，按名字字典序升序。
func (a ValidatingWebhookConfigurationSorter) ByName(i, j int) bool {
	return a[i].Name < a[j].Name
}
