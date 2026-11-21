/*
Copyright 2024 The Kubernetes Authors.

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

package admission

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	quota "k8s.io/apiserver/pkg/quota/v1"
)

// doNothingAdmission 是一个空的准入插件桩，用于测试初始化逻辑。
type doNothingAdmission struct{}

// Admit 不做任何处理，直接返回 nil。
func (doNothingAdmission) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	return nil
}

// Handles 始终返回 false，表示不处理任何操作。
func (doNothingAdmission) Handles(o admission.Operation) bool { return false }

// Validate 始终返回 nil，表示校验通过。
func (doNothingAdmission) Validate() error { return nil }

// doNothingPluginInitialization 是一个空的插件初始化校验桩。
type doNothingPluginInitialization struct{}

// ValidateInitialization 始终返回 nil，表示初始化校验通过。
func (doNothingPluginInitialization) ValidateInitialization() error { return nil }

// doNothingQuotaConfiguration 是一个空的配额配置桩。
type doNothingQuotaConfiguration struct{}

// IgnoredResources 返回 nil，表示没有需要忽略的资源。
func (doNothingQuotaConfiguration) IgnoredResources() map[schema.GroupResource]struct{} { return nil }

// Evaluators 返回 nil，表示没有配额评估器。
func (doNothingQuotaConfiguration) Evaluators() []quota.Evaluator { return nil }

// WantsQuotaConfigurationAdmissionPlugin 是一个声明需要配额配置的测试插件。
type WantsQuotaConfigurationAdmissionPlugin struct {
	doNothingAdmission
	doNothingPluginInitialization
	config quota.Configuration
}

// SetQuotaConfiguration 记录注入的配额配置，供测试断言。
func (p *WantsQuotaConfigurationAdmissionPlugin) SetQuotaConfiguration(config quota.Configuration) {
	p.config = config
}

// TestQuotaConfigurationAdmissionPlugin 验证初始化器会向声明需要的插件注入配额配置。
func TestQuotaConfigurationAdmissionPlugin(t *testing.T) {
	config := doNothingQuotaConfiguration{}
	initializer := NewPluginInitializer(config, nil)
	wantsQuotaConfigurationAdmission := &WantsQuotaConfigurationAdmissionPlugin{}
	// 执行初始化：期望插件的 config 字段被填充。
	initializer.Initialize(wantsQuotaConfigurationAdmission)

	if wantsQuotaConfigurationAdmission.config == nil {
		t.Errorf("Expected quota configuration to be initialized but found nil")
	}
}
