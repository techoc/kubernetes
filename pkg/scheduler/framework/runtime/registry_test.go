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

package runtime

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"

	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
)

// TestDecodeInto 验证 DecodeInto 能把 JSON 与 YAML 格式的插件配置解码到目标结构体。
func TestDecodeInto(t *testing.T) {
	type PluginFooConfig struct {
		FooTest string `json:"fooTest,omitempty"`
	}
	tests := []struct {
		name     string
		args     *runtime.Unknown
		expected PluginFooConfig
	}{
		{
			name: "test decode for JSON config",
			args: &runtime.Unknown{
				ContentType: runtime.ContentTypeJSON,
				Raw: []byte(`{
					"fooTest": "test decode"
				}`),
			},
			expected: PluginFooConfig{
				FooTest: "test decode",
			},
		},
		{
			name: "test decode for YAML config",
			args: &runtime.Unknown{
				ContentType: runtime.ContentTypeYAML,
				Raw:         []byte(`fooTest: "test decode"`),
			},
			expected: PluginFooConfig{
				FooTest: "test decode",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var pluginFooConf PluginFooConfig
			if err := DecodeInto(test.args, &pluginFooConf); err != nil {
				t.Errorf("DecodeInto(): failed to decode args %+v: %v", test.args, err)
			}
			if diff := cmp.Diff(test.expected, pluginFooConf); diff != "" {
				t.Errorf("DecodeInto(): failed to decode plugin config (-want,+got):\n%s", diff)
			}
		})
	}
}

// isRegistryEqual 比较两个 Registry 是否等价。
// 不使用 reflect.DeepEqual 或 cmp，因为它们无法比较函数值：
// 这里改为调用工厂构造插件，再比较插件名。
func isRegistryEqual(registryX, registryY Registry) bool {
	for name, pluginFactory := range registryY {
		if val, ok := registryX[name]; ok {
			p1, _ := pluginFactory(nil, nil, nil)
			p2, _ := val(nil, nil, nil)
			if p1.Name() != p2.Name() {
				// 两个工厂构造出的插件名不同，视为不同工厂
				return false
			}
		} else {
			// registryY 包含 registryX 没有的条目
			return false
		}
	}

	for name := range registryX {
		if _, ok := registryY[name]; !ok {
			// registryX 包含 registryY 没有的条目
			return false
		}
	}

	return true
}

// mockNoopPlugin 是以 UUID 作为名称的空插件，用于区分不同工厂实例。
type mockNoopPlugin struct {
	uuid string
}

// Name 返回插件的 UUID。
func (p *mockNoopPlugin) Name() string {
	return p.uuid
}

// NewMockNoopPluginFactory 返回一个构造空插件的工厂，
// 每个工厂实例生成不同的 UUID 以便测试区分。
func NewMockNoopPluginFactory() PluginFactory {
	uuid := uuid.New().String()
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &mockNoopPlugin{uuid}, nil
	}
}

// TestMerge 验证 Registry.Merge 合并两个注册表，遇到重名工厂时报错且不覆盖原条目。
func TestMerge(t *testing.T) {
	m1 := NewMockNoopPluginFactory()
	m2 := NewMockNoopPluginFactory()
	tests := []struct {
		name            string
		primaryRegistry Registry
		registryToMerge Registry
		expected        Registry
		shouldError     bool
	}{
		{
			name: "valid Merge",
			primaryRegistry: Registry{
				"pluginFactory1": m1,
			},
			registryToMerge: Registry{
				"pluginFactory2": m2,
			},
			expected: Registry{
				"pluginFactory1": m1,
				"pluginFactory2": m2,
			},
			shouldError: false,
		},
		{
			name: "Merge duplicate factories",
			primaryRegistry: Registry{
				"pluginFactory1": m1,
			},
			registryToMerge: Registry{
				"pluginFactory1": m2,
			},
			expected: Registry{
				"pluginFactory1": m1,
			},
			shouldError: true,
		},
	}

	for _, scenario := range tests {
		t.Run(scenario.name, func(t *testing.T) {
			err := scenario.primaryRegistry.Merge(scenario.registryToMerge)

			if (err == nil) == scenario.shouldError {
				t.Errorf("Merge() shouldError is: %v, however err is: %v.", scenario.shouldError, err)
				return
			}

			if !isRegistryEqual(scenario.expected, scenario.primaryRegistry) {
				t.Errorf("Merge(). Expected %v. Got %v instead.", scenario.expected, scenario.primaryRegistry)
			}
		})
	}
}

// TestRegister 验证 Registry.Register 注册新工厂成功，重复注册同名工厂时报错且不覆盖。
func TestRegister(t *testing.T) {
	m1 := NewMockNoopPluginFactory()
	m2 := NewMockNoopPluginFactory()
	tests := []struct {
		name              string
		registry          Registry
		nameToRegister    string
		factoryToRegister PluginFactory
		expected          Registry
		shouldError       bool
	}{
		{
			name:              "valid Register",
			registry:          Registry{},
			nameToRegister:    "pluginFactory1",
			factoryToRegister: m1,
			expected: Registry{
				"pluginFactory1": m1,
			},
			shouldError: false,
		},
		{
			name: "Register duplicate factories",
			registry: Registry{
				"pluginFactory1": m1,
			},
			nameToRegister:    "pluginFactory1",
			factoryToRegister: m2,
			expected: Registry{
				"pluginFactory1": m1,
			},
			shouldError: true,
		},
	}

	for _, scenario := range tests {
		t.Run(scenario.name, func(t *testing.T) {
			err := scenario.registry.Register(scenario.nameToRegister, scenario.factoryToRegister)

			if (err == nil) == scenario.shouldError {
				t.Errorf("Register() shouldError is: %v however err is: %v.", scenario.shouldError, err)
				return
			}

			if !isRegistryEqual(scenario.expected, scenario.registry) {
				t.Errorf("Register(). Expected %v. Got %v instead.", scenario.expected, scenario.registry)
			}
		})
	}
}

// TestUnregister 验证 Registry.Unregister 删除已注册工厂成功，删除不存在的工厂时报错。
func TestUnregister(t *testing.T) {
	m1 := NewMockNoopPluginFactory()
	m2 := NewMockNoopPluginFactory()
	tests := []struct {
		name             string
		registry         Registry
		nameToUnregister string
		expected         Registry
		shouldError      bool
	}{
		{
			name: "valid Unregister",
			registry: Registry{
				"pluginFactory1": m1,
				"pluginFactory2": m2,
			},
			nameToUnregister: "pluginFactory1",
			expected: Registry{
				"pluginFactory2": m2,
			},
			shouldError: false,
		},
		{
			name:             "Unregister non-existent plugin factory",
			registry:         Registry{},
			nameToUnregister: "pluginFactory1",
			expected:         Registry{},
			shouldError:      true,
		},
	}

	for _, scenario := range tests {
		t.Run(scenario.name, func(t *testing.T) {
			err := scenario.registry.Unregister(scenario.nameToUnregister)

			if (err == nil) == scenario.shouldError {
				t.Errorf("Unregister() shouldError is: %v however err is: %v.", scenario.shouldError, err)
				return
			}

			if !isRegistryEqual(scenario.expected, scenario.registry) {
				t.Errorf("Unregister(). Expected %v. Got %v instead.", scenario.expected, scenario.registry)
			}
		})
	}
}
