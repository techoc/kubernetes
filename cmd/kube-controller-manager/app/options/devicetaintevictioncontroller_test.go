/*
Copyright 2025 The Kubernetes Authors.

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
	"strings"
	"testing"

	"github.com/spf13/pflag"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	devicetaintevictionconfig "k8s.io/kubernetes/pkg/controller/devicetainteviction/config"
)

func TestDeviceTaintEvictionControllerOptions_AddFlags(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts := &DeviceTaintEvictionControllerOptions{
		&devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{
			ConcurrentSyncs: 50,
		},
	}

	opts.AddFlags(fs)

	// 确认参数已注册
	flag := fs.Lookup("concurrent-device-taint-eviction-syncs")
	if flag == nil {
		t.Error("concurrent-device-taint-eviction-syncs flag was not added")
		return
	}

	// 确认参数默认值正确
	if flag.DefValue != "50" {
		t.Errorf("expected default value 50, got %s", flag.DefValue)
	}

	// 测试参数解析
	args := []string{"--concurrent-device-taint-eviction-syncs=25"}
	if err := fs.Parse(args); err != nil {
		t.Errorf("failed to parse flags: %v", err)
	}

	if opts.ConcurrentSyncs != 25 {
		t.Errorf("expected ConcurrentSyncs to be 25, got %d", opts.ConcurrentSyncs)
	}
}

func TestDeviceTaintEvictionControllerOptions_AddFlags_Nil(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var opts *DeviceTaintEvictionControllerOptions

	// 选项为 nil 时不应 panic
	opts.AddFlags(fs)

	// 不应注册参数
	flag := fs.Lookup("concurrent-device-taint-eviction-syncs")
	if flag != nil {
		t.Error("concurrent-device-taint-eviction-syncs flag should not be added when options is nil")
	}
}

func TestDeviceTaintEvictionControllerOptions_ApplyTo(t *testing.T) {
	opts := &DeviceTaintEvictionControllerOptions{
		&devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{
			ConcurrentSyncs: 75,
		},
	}

	cfg := &devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{}

	err := opts.ApplyTo(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if cfg.ConcurrentSyncs != 75 {
		t.Errorf("expected ConcurrentSyncs to be 75, got %d", cfg.ConcurrentSyncs)
	}
}

func TestDeviceTaintEvictionControllerOptions_ApplyTo_Nil(t *testing.T) {
	var opts *DeviceTaintEvictionControllerOptions
	cfg := &devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{
		ConcurrentSyncs: 50,
	}

	err := opts.ApplyTo(cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 配置应保持不变
	if cfg.ConcurrentSyncs != 50 {
		t.Errorf("expected ConcurrentSyncs to remain 50, got %d", cfg.ConcurrentSyncs)
	}
}

func TestDeviceTaintEvictionControllerOptions_Validate(t *testing.T) {
	testCases := []struct {
		name                   string
		concurrentSyncs        int32
		expectErrors           bool
		expectedErrorSubString string
	}{
		{
			name:            "valid concurrent syncs",
			concurrentSyncs: 50,
			expectErrors:    false,
		},
		{
			name:            "valid minimum concurrent syncs",
			concurrentSyncs: 1,
			expectErrors:    false,
		},
		{
			name:                   "invalid zero concurrent syncs",
			concurrentSyncs:        0,
			expectErrors:           true,
			expectedErrorSubString: "concurrent-device-taint-eviction-syncs must be greater than zero, got 0",
		},
		{
			name:                   "invalid negative concurrent syncs",
			concurrentSyncs:        -5,
			expectErrors:           true,
			expectedErrorSubString: "concurrent-device-taint-eviction-syncs must be greater than zero, got -5",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := &DeviceTaintEvictionControllerOptions{
				&devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{
					ConcurrentSyncs: tc.concurrentSyncs,
				},
			}

			errs := opts.Validate()

			if tc.expectErrors && len(errs) == 0 {
				t.Error("expected validation errors, but got none")
			}

			if !tc.expectErrors && len(errs) > 0 {
				t.Errorf("expected no validation errors, but got: %v", errs)
			}

			if tc.expectErrors && len(errs) > 0 {
				gotErr := utilerrors.NewAggregate(errs).Error()
				if !strings.Contains(gotErr, tc.expectedErrorSubString) {
					t.Errorf("expected error to contain %q, but got %q", tc.expectedErrorSubString, gotErr)
				}
			}
		})
	}
}

func TestDeviceTaintEvictionControllerOptions_Validate_Nil(t *testing.T) {
	var opts *DeviceTaintEvictionControllerOptions

	errs := opts.Validate()
	if len(errs) != 0 {
		t.Errorf("expected no validation errors for nil options, but got: %v", errs)
	}
}

func TestDeviceTaintEvictionControllerOptions_Integration(t *testing.T) {
	// 测试完整流程：创建选项、注册参数、解析并应用到配置
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts := &DeviceTaintEvictionControllerOptions{
		&devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{
			ConcurrentSyncs: 50,
		},
	}

	// 注册参数
	opts.AddFlags(fs)

	// 解析自定义参数值
	args := []string{"--concurrent-device-taint-eviction-syncs=100"}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	// 校验
	errs := opts.Validate()
	if len(errs) > 0 {
		t.Fatalf("validation failed: %v", errs)
	}

	// 应用到配置
	cfg := &devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{}
	if err := opts.ApplyTo(cfg); err != nil {
		t.Fatalf("failed to apply options: %v", err)
	}

	// 确认最终配置
	expected := &devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration{
		ConcurrentSyncs: 100,
	}

	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf("expected config %+v, got %+v", expected, cfg)
	}
}
