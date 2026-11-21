/*
Copyright 2022 The Kubernetes Authors.

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

package topologymanager

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// fancyBetaOption 是测试用的 beta 级策略选项名。
var fancyBetaOption = "fancy-new-option"

// fancyAlphaOption 是测试用的 alpha 级策略选项名。
var fancyAlphaOption = "fancy-alpha-option"

// optionAvailTest 描述单个策略选项可用性检查的测试用例。
type optionAvailTest struct {
	// option 是待检查的策略选项名。
	option string
	// featureGate 是控制该选项是否可用的特性门控。
	featureGate featuregate.Feature
	// featureGateEnable 指定测试期间特性门控的开关状态。
	featureGateEnable bool
	// expectedAvailable 是期望的可用性结果。
	expectedAvailable bool
}

// TestNewTopologyManagerOptions 验证 NewPolicyOptions 对各种选项组合的解析结果与错误处理。
func TestNewTopologyManagerOptions(t *testing.T) {
	testCases := []struct {
		description       string
		policyOptions     map[string]string
		featureGate       featuregate.Feature
		featureGateEnable bool
		expectedErr       error
		expectedOptions   PolicyOptions
	}{
		{
			description: "return TopologyManagerOptions with PreferClosestNUMA set to true",
			expectedOptions: PolicyOptions{
				PreferClosestNUMA:     true,
				MaxAllowableNUMANodes: 8,
			},
			policyOptions: map[string]string{
				PreferClosestNUMANodes: "true",
				MaxAllowableNUMANodes:  "8",
			},
		},
		{
			description: "return TopologyManagerOptions with MaxAllowableNUMANodes set to 12",
			expectedOptions: PolicyOptions{
				MaxAllowableNUMANodes: 12,
			},
			policyOptions: map[string]string{
				MaxAllowableNUMANodes: "12",
			},
		},
		{
			description: "return empty TopologyManagerOptions",
			expectedOptions: PolicyOptions{
				MaxAllowableNUMANodes: 8,
			},
		},
		{
			description:       "fail to parse options with error PreferClosestNUMANodes",
			featureGateEnable: true,
			policyOptions: map[string]string{
				PreferClosestNUMANodes: "not a boolean",
			},
			expectedErr: fmt.Errorf("bad value for option"),
		},
		{
			description: "fail to parse options with error MaxAllowableNUMANodes",
			policyOptions: map[string]string{
				MaxAllowableNUMANodes: "can't parse to int",
			},
			expectedErr: fmt.Errorf("unable to convert policy option to integer"),
		},
		{
			description:       "test beta options success",
			featureGate:       pkgfeatures.TopologyManagerPolicyBetaOptions,
			featureGateEnable: true,
			policyOptions: map[string]string{
				fancyBetaOption: "true",
			},
			expectedOptions: PolicyOptions{
				PreferClosestNUMA:     false,
				MaxAllowableNUMANodes: 8,
			},
		},
		{
			description: "test beta options fail",
			featureGate: pkgfeatures.TopologyManagerPolicyBetaOptions,
			policyOptions: map[string]string{
				fancyBetaOption: "true",
			},
			expectedErr: fmt.Errorf("topology manager policy beta-level options not enabled,"),
		},
		{
			description:       "test alpha options success",
			featureGate:       pkgfeatures.TopologyManagerPolicyAlphaOptions,
			featureGateEnable: true,
			policyOptions: map[string]string{
				fancyAlphaOption: "true",
			},
			expectedOptions: PolicyOptions{
				PreferClosestNUMA:     false,
				MaxAllowableNUMANodes: 8,
			},
		},
		{
			description: "test alpha options fail",
			policyOptions: map[string]string{
				fancyAlphaOption: "true",
			},
			expectedErr: fmt.Errorf("topology manager policy alpha-level options not enabled,"),
		},
	}

	// 注册测试专用的 beta/alpha 选项，避免依赖真实的选项列表。
	setTopologyManagerOptionsDuringTest(t, betaOptions, fancyBetaOption)
	setTopologyManagerOptionsDuringTest(t, alphaOptions, fancyAlphaOption)

	// 创建带日志的测试上下文。
	logger, _ := ktesting.NewTestContext(t)

	for _, tcase := range testCases {
		t.Run(tcase.description, func(t *testing.T) {
			// 仅在用例指定了特性门控时才调整其开关状态。
			if tcase.featureGate != "" {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, tcase.featureGate, tcase.featureGateEnable)
			}
			// 解析选项并校验错误或结果是否符合预期。
			opts, err := NewPolicyOptions(logger, tcase.policyOptions)
			if tcase.expectedErr != nil {
				// 期望出错却成功返回，直接判定失败。
				if err == nil {
					t.Errorf("expected error %v, got no error", tcase.expectedErr)
					// 错误文案只做包含匹配，避免耦合完整提示信息。
				} else if !strings.Contains(err.Error(), tcase.expectedErr.Error()) {
					t.Errorf("Unexpected error message. Have: %s, wants %s", err.Error(), tcase.expectedErr.Error())
				}
				return
			}

			// PolicyOptions 是值类型，可直接比较。
			if opts != tcase.expectedOptions {
				t.Errorf("Expected TopologyManagerOptions to equal %v, not %v", tcase.expectedOptions, opts)
			}
		})
	}
}

// setTopologyManagerOptionsDuringTest 在测试期间向选项组注册给定选项，并在测试结束后清理，避免污染其他用例。
func setTopologyManagerOptionsDuringTest(t *testing.T, optionGroup sets.Set[string], opts ...string) {
	t.Helper()
	// 通过 t.Cleanup 保证测试结束（包括失败）后移除注册项。
	t.Cleanup(func() {
		optionGroup.Delete(opts...)
	})
	optionGroup.Insert(opts...)
}

// TestPolicyDefaultsAvailable 验证默认策略选项的可用性判断。
func TestPolicyDefaultsAvailable(t *testing.T) {
	testCases := []optionAvailTest{
		{
			option:            "this-option-does-not-exist",
			expectedAvailable: false,
		},
		{
			option:            PreferClosestNUMANodes,
			expectedAvailable: true,
		},
		{
			option:            MaxAllowableNUMANodes,
			expectedAvailable: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.option, func(t *testing.T) {
			err := CheckPolicyOptionAvailable(testCase.option)
			isEnabled := (err == nil)
			// 仅当选项检查不返回错误时才视为可用。
			if isEnabled != testCase.expectedAvailable {
				t.Errorf("option %q available got=%v expected=%v", testCase.option, isEnabled, testCase.expectedAvailable)
			}
		})
	}
}

// TestPolicyOptionsAvailable 验证在特性门控不同开关组合下各策略选项的可用性。
func TestPolicyOptionsAvailable(t *testing.T) {
	testCases := []optionAvailTest{
		{
			option:            "this-option-does-not-exist",
			featureGate:       pkgfeatures.TopologyManagerPolicyBetaOptions,
			featureGateEnable: false,
			expectedAvailable: false,
		},
		{
			option:            "this-option-does-not-exist",
			featureGate:       pkgfeatures.TopologyManagerPolicyBetaOptions,
			featureGateEnable: true,
			expectedAvailable: false,
		},
		{
			option:            PreferClosestNUMANodes,
			featureGate:       pkgfeatures.TopologyManagerPolicyBetaOptions,
			featureGateEnable: false,
			expectedAvailable: true,
		},
		{
			option:            PreferClosestNUMANodes,
			featureGate:       pkgfeatures.TopologyManagerPolicyAlphaOptions,
			featureGateEnable: false,
			expectedAvailable: true,
		},
		{
			option:            MaxAllowableNUMANodes,
			featureGate:       pkgfeatures.TopologyManagerPolicyBetaOptions,
			featureGateEnable: false,
			expectedAvailable: true,
		},
		{
			option:            PreferClosestNUMANodes,
			featureGate:       pkgfeatures.TopologyManagerPolicyAlphaOptions,
			featureGateEnable: false,
			expectedAvailable: true,
		},
		{
			option:            fancyAlphaOption,
			featureGate:       pkgfeatures.TopologyManagerPolicyAlphaOptions,
			featureGateEnable: true,
			expectedAvailable: true,
		},
		{
			option:            fancyAlphaOption,
			featureGate:       pkgfeatures.TopologyManagerPolicyAlphaOptions,
			featureGateEnable: false,
			expectedAvailable: false,
		},
		{
			option:            fancyBetaOption,
			featureGate:       pkgfeatures.TopologyManagerPolicyBetaOptions,
			featureGateEnable: true,
			expectedAvailable: true,
		},
		{
			option:            fancyBetaOption,
			featureGate:       pkgfeatures.TopologyManagerPolicyBetaOptions,
			featureGateEnable: false,
			expectedAvailable: false,
		},
	}
	setTopologyManagerOptionsDuringTest(t, betaOptions, fancyBetaOption)
	setTopologyManagerOptionsDuringTest(t, alphaOptions, fancyAlphaOption)
	for _, testCase := range testCases {
		t.Run(testCase.option, func(t *testing.T) {
			// 按用例设置特性门控开关。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, testCase.featureGate, testCase.featureGateEnable)
			defer func() {
				// 恢复特性门控原状态，避免影响后续用例。
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, testCase.featureGate, !testCase.featureGateEnable)
			}()
			err := CheckPolicyOptionAvailable(testCase.option)
			isEnabled := (err == nil)
			// 仅当选项检查不返回错误时才视为可用。
			if isEnabled != testCase.expectedAvailable {
				t.Errorf("option %q available got=%v expected=%v", testCase.option, isEnabled, testCase.expectedAvailable)
			}
		})
	}
}
