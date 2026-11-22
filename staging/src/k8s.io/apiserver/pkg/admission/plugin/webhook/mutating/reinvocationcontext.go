/*
Copyright 2019 The Kubernetes Authors.

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

package mutating

import (
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
)

// webhookReinvokeContext 记录 mutating webhook 链的调用状态。
// 某些 mutating webhook（如 sidecar 注入）声明了 reinvocationPolicy=IfNeeded，
// 当后续 webhook 再次改动对象后需要重新调用它们，因此要保存上次调用的输出
// 和已调用过、可重复调用的 hook 集合。
type webhookReinvokeContext struct {
	// lastWebhookOutput holds the result of the last webhook admission plugin call
	lastWebhookOutput runtime.Object
	// previouslyInvokedReinvocableWebhooks holds the set of webhooks that have been invoked and
	// should be reinvoked if a later mutation occurs
	previouslyInvokedReinvocableWebhooks sets.Set[string]
	// reinvokeWebhooks holds the set of webhooks that should be reinvoked
	reinvokeWebhooks sets.Set[string]
}

// ShouldReinvokeWebhook 判断指定 webhook 是否需要本轮重新调用。
func (rc *webhookReinvokeContext) ShouldReinvokeWebhook(webhook string) bool {
	return rc.reinvokeWebhooks.Has(webhook)
}

// IsOutputChangedSinceLastWebhookInvocation 用语义相等比较判断对象是否自上次
// 调用后被改动。语义比较忽略空值/默认值差异，避免仅因序列化细节触发重复调用。
func (rc *webhookReinvokeContext) IsOutputChangedSinceLastWebhookInvocation(object runtime.Object) bool {
	return !apiequality.Semantic.DeepEqual(rc.lastWebhookOutput, object)
}

// SetLastWebhookInvocationOutput 保存对象快照。
// 必须深拷贝，因为调用方后续会原地修改对象；直接持有引用会让比较失去意义。
func (rc *webhookReinvokeContext) SetLastWebhookInvocationOutput(object runtime.Object) {
	if object == nil {
		rc.lastWebhookOutput = nil
		return
	}
	rc.lastWebhookOutput = object.DeepCopyObject()
}

// AddReinvocableWebhookToPreviouslyInvoked 记录一个可重复调用的 webhook 已被执行。
func (rc *webhookReinvokeContext) AddReinvocableWebhookToPreviouslyInvoked(webhook string) {
	// map 惰性初始化，避免零值结构体无法直接写入。
	if rc.previouslyInvokedReinvocableWebhooks == nil {
		rc.previouslyInvokedReinvocableWebhooks = sets.New[string]()
	}
	rc.previouslyInvokedReinvocableWebhooks.Insert(webhook)
}

// RequireReinvokingPreviouslyInvokedPlugins 在检测到对象被改动后，
// 把已调用过的可重复调用 webhook 全部标记为需要重新调用，并清空待定集合，
// 防止同一批 hook 被重复标记。
func (rc *webhookReinvokeContext) RequireReinvokingPreviouslyInvokedPlugins() {
	if len(rc.previouslyInvokedReinvocableWebhooks) > 0 {
		if rc.reinvokeWebhooks == nil {
			rc.reinvokeWebhooks = sets.New[string]()
		}
		for s := range rc.previouslyInvokedReinvocableWebhooks {
			rc.reinvokeWebhooks.Insert(s)
		}
		rc.previouslyInvokedReinvocableWebhooks = sets.New[string]()
	}
}
