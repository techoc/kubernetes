/*
Copyright 2020 The Kubernetes Authors.

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

package internalbootstrap

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap"
)

// TestBootstrapConfigurationWithDefaulted 验证所有 bootstrap（强制和建议）的
// FlowSchema 和 PriorityLevelConfiguration 对象都已经包含完整的默认值，
// 即再执行一次默认化处理不应改变对象内容。
// 这是为了保证引导配置在写入 API 时不会因为默认值差异产生意外变更。
func TestBootstrapConfigurationWithDefaulted(t *testing.T) {
	// 使用安装了 APF 类型的 Scheme，Default 会调用版本化的默认值逻辑。
	scheme := NewAPFScheme()

	// 收集强制和建议的 FlowSchema，逐一验证默认值稳定性。
	bootstrapFlowSchemas := make([]*flowcontrol.FlowSchema, 0)
	bootstrapFlowSchemas = append(bootstrapFlowSchemas, bootstrap.MandatoryFlowSchemas...)
	bootstrapFlowSchemas = append(bootstrapFlowSchemas, bootstrap.SuggestedFlowSchemas...)
	for _, original := range bootstrapFlowSchemas {
		t.Run(fmt.Sprintf("FlowSchema/%s", original.Name), func(t *testing.T) {
			// 深拷贝后再做默认化，避免污染全局 bootstrap 对象。
			defaulted := original.DeepCopyObject().(*flowcontrol.FlowSchema)
			scheme.Default(defaulted)
			// 若默认化没有引入任何变化，说明原始对象已经是完整默认值状态。
			if apiequality.Semantic.DeepEqual(original, defaulted) {
				t.Logf("Defaulting makes no change to FlowSchema: %q", original.Name)
				return
			}
			t.Errorf("Expected defaulting to not change FlowSchema: %q, diff: %s", original.Name, cmp.Diff(original, defaulted))
		})
	}

	// 收集强制和建议的 PriorityLevelConfiguration，逐一验证默认值稳定性。
	bootstrapPriorityLevels := make([]*flowcontrol.PriorityLevelConfiguration, 0)
	bootstrapPriorityLevels = append(bootstrapPriorityLevels, bootstrap.MandatoryPriorityLevelConfigurations...)
	bootstrapPriorityLevels = append(bootstrapPriorityLevels, bootstrap.SuggestedPriorityLevelConfigurations...)
	for _, original := range bootstrapPriorityLevels {
		t.Run(fmt.Sprintf("PriorityLevelConfiguration/%s", original.Name), func(t *testing.T) {
			// 深拷贝后再做默认化，避免污染全局 bootstrap 对象。
			defaulted := original.DeepCopyObject().(*flowcontrol.PriorityLevelConfiguration)
			scheme.Default(defaulted)
			// 若默认化没有引入任何变化，说明原始对象已经是完整默认值状态。
			if apiequality.Semantic.DeepEqual(original, defaulted) {
				t.Logf("Defaulting makes no change to PriorityLevelConfiguration: %q", original.Name)
				return
			}
			t.Errorf("Expected defaulting to not change PriorityLevelConfiguration: %q, diff: %s", original.Name, cmp.Diff(original, defaulted))
		})
	}
}
