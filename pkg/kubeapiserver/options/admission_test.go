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

package options

import (
	"reflect"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

// TestValidate 覆盖 AdmissionOptions.Validate 的互斥校验、未知插件与 nil 接收者场景。
func TestValidate(t *testing.T) {
	// 1. 同时指定了 `--admission-control` 与 `--enable-admission-plugins`
	options := NewAdmissionOptions()
	options.PluginNames = []string{"ServiceAccount"}
	options.GenericAdmission.EnablePlugins = []string{"NodeRestriction"}
	if len(options.Validate()) == 0 {
		t.Errorf("Expect error, but got none")
	}

	// 2. 同时指定了 `--admission-control` 与 `--disable-admission-plugins`
	options = NewAdmissionOptions()
	options.PluginNames = []string{"ServiceAccount"}
	options.GenericAdmission.DisablePlugins = []string{"NodeRestriction"}
	if len(options.Validate()) == 0 {
		t.Errorf("Expect error, but got none")
	}

	// 3. PluginNames 中的插件未注册
	options = NewAdmissionOptions()
	options.PluginNames = []string{"pluginA"}
	if len(options.Validate()) == 0 {
		t.Errorf("Expect error, but got none")
	}

	// 4. PluginNames 合法时应无错误
	options = NewAdmissionOptions()
	options.PluginNames = []string{"ServiceAccount"}
	if errs := options.Validate(); len(errs) > 0 {
		t.Errorf("Unexpected err: %v", errs)
	}

	// nil 接收者应安全地不报错
	options = nil
	if errs := options.Validate(); errs != nil {
		t.Errorf("expected no errors, error found %+v", errs)
	}
}

// TestComputeEnabledAdmission 校验启用列表原样保留，未启用的插件被正确列入禁用列表。
func TestComputeEnabledAdmission(t *testing.T) {
	tests := []struct {
		name             string
		all              []string
		enabled          []string
		expectedDisabled []string
	}{
		{
			name:             "matches",
			all:              []string{"one", "two"},
			enabled:          []string{"one", "two"},
			expectedDisabled: []string{},
		},
		{
			name:             "choose one",
			all:              []string{"one", "two"},
			enabled:          []string{"one"},
			expectedDisabled: []string{"two"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actualEnabled, actualDisabled := computePluginNames(tc.enabled, tc.all)
			if e, a := tc.enabled, actualEnabled; !reflect.DeepEqual(e, a) {
				t.Errorf("expected %v, got %v", e, a)
			}
			if e, a := tc.expectedDisabled, actualDisabled; !reflect.DeepEqual(e, a) {
				t.Errorf("expected %v, got %v", e, a)
			}
		})
	}
}

// TestAdmissionOptionsAddFlags 校验 AddFlags 注册的标志能被正确解析并写入选项。
func TestAdmissionOptionsAddFlags(t *testing.T) {
	var args = []string{
		"--enable-admission-plugins=foo,bar,baz",
		"--admission-control-config-file=admission_control_config.yaml",
	}

	opts := NewAdmissionOptions()
	pf := pflag.NewFlagSet("test-admission-opts", pflag.ContinueOnError)
	opts.AddFlags(pf)

	if err := pf.Parse(args); err != nil {
		t.Fatal(err)
	}

	// GenericAdmission 内含指针与函数字段，无法直接比较，故逐字段断言。
	assert.Equal(t, "admission_control_config.yaml", opts.GenericAdmission.ConfigFile)
	assert.Equal(t, []string{"foo", "bar", "baz"}, opts.GenericAdmission.EnablePlugins)
}
