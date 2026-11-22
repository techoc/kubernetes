/*
Copyright 2023 The Kubernetes Authors.

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

package internalversion

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
)

// TestSetListOptionsDefaults 验证 watch-list（SendInitialEvents）相关的默认值填充：
// 仅当 Watch=true、资源版本为空或 "0"、且 SendInitialEvents 与 ResourceVersionMatch 均未设置时，
// 才会补上 SendInitialEvents=true 与 ResourceVersionMatch=NotOlderThan；其余情况必须保持不变。
// watchListFeatureEnabled 为 false 时不应做任何填充。
func TestSetListOptionsDefaults(t *testing.T) {
	scenarios := []struct {
		name                    string
		watchListFeatureEnabled bool
		targetObj               ListOptions
		expectedObj             ListOptions
	}{
		{
			// 资源版本既非空也非 "0"，不满足 watch-list 的触发条件。
			name:                    "no-op, RV doesn't match",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersion: "1"},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersion: "1"},
		},
		{
			// 调用方已显式设置 SendInitialEvents，尊重其选择（包括 false）。
			name:                    "no-op, SendInitialEvents set",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, SendInitialEvents: ptr.To(true)},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, SendInitialEvents: ptr.To(true)},
		},
		{
			// 调用方已显式设置 ResourceVersionMatch，不再覆盖。
			name:                    "no-op, ResourceVersionMatch set",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersionMatch: "m"},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersionMatch: "m"},
		},
		{
			// 普通 list（非 watch）不涉及初始事件语义。
			name:                    "no-op, Watch=false",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything()},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything()},
		},
		{
			// 空资源版本是 watch-list 的典型起点，应补齐默认值。
			name:                    "defaults applied, match on empty RV",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, SendInitialEvents: ptr.To(true), ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan},
		},
		{
			// "0" 表示从当前缓存状态开始，同样适用 watch-list 默认值。
			name:                    "defaults applied, match on RV=0",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersion: "0"},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersion: "0", SendInitialEvents: ptr.To(true), ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan},
		},
		{
			// 特性开关关闭时不得注入任何默认值，保证行为与旧版本一致。
			name:        "no-op, match on empty RV but watch-list fg is off",
			targetObj:   ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true},
			expectedObj: ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true},
		},
		{
			// SendInitialEvents 已显式为 true，无需重复设置。
			name:                    "no-op, match on empty RV but SendInitialEvents is on",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, SendInitialEvents: ptr.To(true)},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, SendInitialEvents: ptr.To(true)},
		},
		{
			// SendInitialEvents 显式为 false 时必须保留，不能因为"看起来没设置"而改成 true。
			name:                    "no-op, match on empty RV but SendInitialEvents is off",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, SendInitialEvents: ptr.To(false)},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, SendInitialEvents: ptr.To(false)},
		},
		{
			// 显式给出 ResourceVersionMatch 时不得覆盖。
			name:                    "no-op, match on empty RV but ResourceVersionMatch set",
			watchListFeatureEnabled: true,
			targetObj:               ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersionMatch: "m"},
			expectedObj:             ListOptions{LabelSelector: labels.Everything(), FieldSelector: fields.Everything(), Watch: true, ResourceVersionMatch: "m"},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			SetListOptionsDefaults(&scenario.targetObj, scenario.watchListFeatureEnabled)
			if !apiequality.Semantic.DeepEqual(&scenario.expectedObj, &scenario.targetObj) {
				t.Errorf("expected and defaulted objects are different:\n%s", cmp.Diff(&scenario.expectedObj, &scenario.targetObj))
			}
		})
	}
}
