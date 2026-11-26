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

package options

import (
	"fmt"
	"io"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
)

// TestEnabledPluginNames 验证启用插件列表的计算规则：
// 以 RecommendedPluginOrder 为基准，叠加 default-off、enable 与 disable
// 三组开关，并保持推荐顺序。admission-control 是 enable 的旧别名。
func TestEnabledPluginNames(t *testing.T) {
	scenarios := []struct {
		expectedPluginNames       []string
		setDefaultOffPlugins      sets.Set[string]
		setRecommendedPluginOrder []string
		setEnablePlugins          []string
		setDisablePlugins         []string
		setAdmissionControl       []string
	}{
		// 场景 0：检查调用 enabledPluginNames 是否设置了预期值。
		{
			expectedPluginNames: []string{"NamespaceLifecycle", "MutatingAdmissionPolicy", "MutatingAdmissionWebhook", "ValidatingAdmissionPolicy", "ValidatingAdmissionWebhook"},
		},

		// 场景 1：未指定时使用 default-off 插件。
		{
			expectedPluginNames:       []string{"pluginB"},
			setRecommendedPluginOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setDefaultOffPlugins:      sets.New("pluginA", "pluginC", "pluginD"),
		},

		// 场景 2：使用 default-off 插件并给定 RecommendedPluginOrder。
		{
			expectedPluginNames:       []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setRecommendedPluginOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setDefaultOffPlugins:      sets.Set[string]{},
		},

		// 场景 3：使用 default-off 插件，并由 enable-admission-plugins 与 RecommendedPluginOrder 指定。
		{
			expectedPluginNames:       []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setRecommendedPluginOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setDefaultOffPlugins:      sets.New("pluginC", "pluginD"),
			setEnablePlugins:          []string{"pluginD", "pluginC"},
		},

		// 场景 4：使用 default-off 插件，并由 disable-admission-plugins 与 RecommendedPluginOrder 指定。
		{
			expectedPluginNames:       []string{"pluginB"},
			setRecommendedPluginOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setDefaultOffPlugins:      sets.New("pluginC", "pluginD"),
			setDisablePlugins:         []string{"pluginA"},
		},

		// 场景 5：使用 default-off 插件，并由 enable/disable-admission-plugins 与 RecommendedPluginOrder 指定。
		{
			expectedPluginNames:       []string{"pluginA", "pluginC"},
			setRecommendedPluginOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setDefaultOffPlugins:      sets.New("pluginC", "pluginD"),
			setEnablePlugins:          []string{"pluginC"},
			setDisablePlugins:         []string{"pluginB"},
		},

		// 场景 6：使用 default-off 插件，并由 admission-control 与 RecommendedPluginOrder 指定。
		{
			expectedPluginNames:       []string{"pluginA", "pluginB", "pluginC"},
			setRecommendedPluginOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setDefaultOffPlugins:      sets.New("pluginD"),
			setAdmissionControl:       []string{"pluginA", "pluginB"},
		},

		// 场景 7：使用 default-off 插件，并由 admission-control 与 RecommendedPluginOrder 指定。
		{
			expectedPluginNames:       []string{"pluginA", "pluginB", "pluginC"},
			setRecommendedPluginOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			setDefaultOffPlugins:      sets.New("pluginC", "pluginD"),
			setAdmissionControl:       []string{"pluginA", "pluginB", "pluginC"},
		},
	}

	// 执行。
	for index, scenario := range scenarios {
		t.Run(fmt.Sprintf("scenario %d", index), func(t *testing.T) {
			target := NewAdmissionOptions()

			if scenario.setDefaultOffPlugins != nil {
				target.DefaultOffPlugins = scenario.setDefaultOffPlugins
			}
			if scenario.setRecommendedPluginOrder != nil {
				target.RecommendedPluginOrder = scenario.setRecommendedPluginOrder
			}
			if scenario.setEnablePlugins != nil {
				target.EnablePlugins = scenario.setEnablePlugins
			}
			if scenario.setDisablePlugins != nil {
				target.DisablePlugins = scenario.setDisablePlugins
			}
			if scenario.setAdmissionControl != nil {
				target.EnablePlugins = scenario.setAdmissionControl
			}

			// 结果必须同时满足“集合正确”与“顺序正确”，因此逐位比较。
			actualPluginNames := target.enabledPluginNames()

			if len(actualPluginNames) != len(scenario.expectedPluginNames) {
				t.Fatalf("incorrect number of items, got %d, expected = %d", len(actualPluginNames), len(scenario.expectedPluginNames))
			}
			for i := range actualPluginNames {
				if scenario.expectedPluginNames[i] != actualPluginNames[i] {
					t.Errorf("missmatch at index = %d, got = %s, expected = %s", i, actualPluginNames[i], scenario.expectedPluginNames[i])
				}
			}
		})
	}
}

// TestValidate 验证启动期校验会拒绝以下配置：
// 同一插件同时出现在 enable 与 disable、插件未注册、推荐顺序重复或
// 与已注册插件集合不一致。这些错误若放过，会在运行时造成难以排查的行为差异。
func TestValidate(t *testing.T) {
	scenarios := []struct {
		setEnablePlugins           []string
		setDisablePlugins          []string
		setRecommendedPluginsOrder []string
		expectedResult             bool
	}{
		// 场景 0：未设置任何 flag。
		{
			expectedResult: true,
		},

		// 场景 1：同时设置 `--enable-admission-plugins` 与 `--disable-admission-plugins`。
		{
			setEnablePlugins:  []string{"pluginA", "pluginB"},
			setDisablePlugins: []string{"pluginC"},
			expectedResult:    true,
		},

		// 场景 2：设置非法的 `--enable-admission-plugins` 与 `--disable-admission-plugins`。
		{
			setEnablePlugins:  []string{"pluginA", "pluginB"},
			setDisablePlugins: []string{"pluginB"},
			expectedResult:    false,
		},

		// 场景 3：只设置非法的 `--enable-admission-plugins`。
		{
			setEnablePlugins: []string{"pluginA", "pluginE"},
			expectedResult:   false,
		},

		// 场景 4：只设置非法的 `--disable-admission-plugins`。
		{
			setDisablePlugins: []string{"pluginA", "pluginE"},
			expectedResult:    false,
		},

		// 场景 5：设置合法的 `--enable-admission-plugins`。
		{
			setEnablePlugins: []string{"pluginA", "pluginB"},
			expectedResult:   true,
		},

		// 场景 6：设置合法的 `--disable-admission-plugins`。
		{
			setDisablePlugins: []string{"pluginA"},
			expectedResult:    true,
		},

		// 场景 7：RecommendedPluginOrder 中有重复插件。
		{
			setRecommendedPluginsOrder: []string{"pluginA", "pluginB", "pluginB", "pluginC"},
			expectedResult:             false,
		},

		// 场景 8：RecommendedPluginOrder 与已注册插件不一致。
		{
			setRecommendedPluginsOrder: []string{"pluginA", "pluginB", "pluginC"},
			expectedResult:             false,
		},

		// 场景 9：RecommendedPluginOrder 与已注册插件一致。
		{
			setRecommendedPluginsOrder: []string{"pluginA", "pluginB", "pluginC", "pluginD"},
			expectedResult:             true,
		},

		// 场景 10：RecommendedPluginOrder 与已注册插件不一致。
		{
			setRecommendedPluginsOrder: []string{"pluginA", "pluginB", "pluginC", "pluginE"},
			expectedResult:             false,
		},
	}

	for index, scenario := range scenarios {
		t.Run(fmt.Sprintf("scenario %d", index), func(t *testing.T) {
			// 构造一个确定的插件注册表，避免依赖真实插件集合。
			options := NewAdmissionOptions()
			options.DefaultOffPlugins = sets.New("pluginC", "pluginD")
			options.RecommendedPluginOrder = []string{"pluginA", "pluginB", "pluginC", "pluginD"}
			options.Plugins = &admission.Plugins{}
			for _, plugin := range options.RecommendedPluginOrder {
				options.Plugins.Register(plugin, func(config io.Reader) (admission.Interface, error) {
					return nil, nil
				})
			}

			if scenario.setEnablePlugins != nil {
				options.EnablePlugins = scenario.setEnablePlugins
			}
			if scenario.setDisablePlugins != nil {
				options.DisablePlugins = scenario.setDisablePlugins
			}
			if scenario.setRecommendedPluginsOrder != nil {
				options.RecommendedPluginOrder = scenario.setRecommendedPluginsOrder
			}

			err := options.Validate()
			if len(err) > 0 && scenario.expectedResult {
				t.Errorf("Unexpected err: %v", err)
			}
			if len(err) == 0 && !scenario.expectedResult {
				t.Errorf("Expect error, but got none")
			}
		})
	}
}
