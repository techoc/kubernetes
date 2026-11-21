/*
Copyright 2021 The Kubernetes Authors.

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

package cpumanager

import (
	"testing"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// optionAvailTest 描述一个策略选项在指定特性门控下是否可用。
type optionAvailTest struct {
	option            string
	featureGate       featuregate.Feature
	featureGateEnable bool
	expectedAvailable bool
}

// TestPolicyDefaultsAvailable 验证默认（不依赖特性门控）可用的选项集合。
func TestPolicyDefaultsAvailable(t *testing.T) {
	testCases := []optionAvailTest{
		{
			option:            "this-option-does-not-exist",
			expectedAvailable: false,
		},
		{
			option:            FullPCPUsOnlyOption,
			expectedAvailable: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.option, func(t *testing.T) {
			err := CheckPolicyOptionAvailable(testCase.option)
			isEnabled := (err == nil)
			if isEnabled != testCase.expectedAvailable {
				t.Errorf("option %q available got=%v expected=%v", testCase.option, isEnabled, testCase.expectedAvailable)
			}
		})
	}
}

// TestPolicyOptionsAvailable 验证 alpha/beta 选项随特性门控开关的可用性。
func TestPolicyOptionsAvailable(t *testing.T) {
	testCases := []optionAvailTest{
		{
			option:            "this-option-does-not-exist",
			featureGate:       pkgfeatures.CPUManagerPolicyBetaOptions,
			featureGateEnable: false,
			expectedAvailable: false,
		},
		{
			option:            "this-option-does-not-exist",
			featureGate:       pkgfeatures.CPUManagerPolicyBetaOptions,
			featureGateEnable: true,
			expectedAvailable: false,
		},
		{
			option:            AlignBySocketOption,
			featureGate:       pkgfeatures.CPUManagerPolicyAlphaOptions,
			featureGateEnable: true,
			expectedAvailable: true,
		},
		{
			option:            AlignBySocketOption,
			featureGate:       pkgfeatures.CPUManagerPolicyBetaOptions,
			featureGateEnable: true,
			expectedAvailable: false,
		},
		{
			option:            DistributeCPUsAcrossNUMAOption,
			featureGate:       pkgfeatures.CPUManagerPolicyBetaOptions,
			featureGateEnable: true,
			expectedAvailable: true,
		},
		{
			option:            DistributeCPUsAcrossNUMAOption,
			featureGate:       pkgfeatures.CPUManagerPolicyBetaOptions,
			featureGateEnable: false,
			expectedAvailable: false,
		},
		{
			option:            DistributeCPUsAcrossCoresOption,
			featureGate:       pkgfeatures.CPUManagerPolicyAlphaOptions,
			featureGateEnable: true,
			expectedAvailable: true,
		},
		{
			option:            DistributeCPUsAcrossCoresOption,
			featureGate:       pkgfeatures.CPUManagerPolicyBetaOptions,
			featureGateEnable: true,
			expectedAvailable: false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.option, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, testCase.featureGate, testCase.featureGateEnable)
			err := CheckPolicyOptionAvailable(testCase.option)
			isEnabled := (err == nil)
			if isEnabled != testCase.expectedAvailable {
				t.Errorf("option %q available got=%v expected=%v", testCase.option, isEnabled, testCase.expectedAvailable)
			}
		})
	}
}

// TestPolicyOptionsAlwaysAvailableOnceGA 验证已 GA 的选项在关闭所有门控后仍然可用。
func TestPolicyOptionsAlwaysAvailableOnceGA(t *testing.T) {
	options := []string{
		FullPCPUsOnlyOption,
		StrictCPUReservationOption,
		PreferAlignByUnCoreCacheOption,
	}
	for _, option := range options {
		t.Run(option, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				pkgfeatures.CPUManagerPolicyAlphaOptions: false,
				pkgfeatures.CPUManagerPolicyBetaOptions:  false,
			})
			if err := CheckPolicyOptionAvailable(option); err != nil {
				t.Errorf("option %q should be available even with all featuregate disabled", option)
			}
		})
	}
}

// TestValidateStaticPolicyOptions 验证 static 策略选项与拓扑管理器策略、
// NUMA/插槽拓扑的兼容性校验。
func TestValidateStaticPolicyOptions(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		description   string
		policyOption  map[string]string
		topology      *topology.CPUTopology
		topoMgrPolicy string
		expectedErr   bool
	}{
		{
			description:   "Align by socket not enabled",
			policyOption:  map[string]string{FullPCPUsOnlyOption: "true"},
			topology:      topoDualSocketMultiNumaPerSocketHT,
			topoMgrPolicy: topologymanager.PolicySingleNumaNode,
			expectedErr:   false,
		},
		{
			description:   "Align by socket enabled with topology manager single numa node",
			policyOption:  map[string]string{AlignBySocketOption: "true"},
			topology:      topoDualSocketMultiNumaPerSocketHT,
			topoMgrPolicy: topologymanager.PolicySingleNumaNode,
			expectedErr:   true,
		},
		{
			description:   "Align by socket enabled with num_sockets > num_numa",
			policyOption:  map[string]string{AlignBySocketOption: "true"},
			topology:      fakeTopoMultiSocketDualSocketPerNumaHT,
			topoMgrPolicy: topologymanager.PolicyNone,
			expectedErr:   true,
		},
		{
			description:   "Align by socket enabled: with topology manager None policy",
			policyOption:  map[string]string{AlignBySocketOption: "true"},
			topology:      topoDualSocketMultiNumaPerSocketHT,
			topoMgrPolicy: topologymanager.PolicyNone,
			expectedErr:   false,
		},
		{
			description:   "Align by socket enabled: with topology manager best-effort policy",
			policyOption:  map[string]string{AlignBySocketOption: "true"},
			topology:      topoDualSocketMultiNumaPerSocketHT,
			topoMgrPolicy: topologymanager.PolicyBestEffort,
			expectedErr:   false,
		},
		{
			description:   "Align by socket enabled: with topology manager restricted policy",
			policyOption:  map[string]string{AlignBySocketOption: "true"},
			topology:      topoDualSocketMultiNumaPerSocketHT,
			topoMgrPolicy: topologymanager.PolicyRestricted,
			expectedErr:   false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			topoMgrPolicy := topologymanager.NewNonePolicy()
			if testCase.topoMgrPolicy == topologymanager.PolicySingleNumaNode {
				topoMgrPolicy = topologymanager.NewSingleNumaNodePolicy(&topologymanager.NUMAInfo{}, topologymanager.PolicyOptions{})
			}
			// 根据表项构造对应策略的假拓扑管理器。
			topoMgrStore := topologymanager.NewFakeManagerWithPolicy(logger, topoMgrPolicy)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.CPUManagerPolicyAlphaOptions, true)
			// 选项解析错误由 NewStaticPolicyOptions 的测试覆盖，这里只校验组合合法性。
			policyOpt, _ := NewStaticPolicyOptions(testCase.policyOption)
			err := ValidateStaticPolicyOptions(policyOpt, testCase.topology, topoMgrStore)
			gotError := (err != nil)
			if gotError != testCase.expectedErr {
				t.Errorf("testCase %q failed, got %v expected %v", testCase.description, gotError, testCase.expectedErr)
			}
		})
	}
}

// TestPolicyOptionsCompatibility 验证各选项组合是否互斥（如 FullPCPUsOnly 与 DistributeCPUsAcrossCores）。
func TestPolicyOptionsCompatibility(t *testing.T) {
	// 每个用例先开启对应特性门控，再校验选项组合。
	testCases := []struct {
		description   string
		featureGate   featuregate.Feature
		policyOptions map[string]string
		expectedErr   bool
	}{
		{
			description: "FullPhysicalCPUsOnly set to true only",
			featureGate: pkgfeatures.CPUManagerPolicyBetaOptions,
			policyOptions: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			expectedErr: false,
		},
		{
			description: "DistributeCPUsAcrossCores set to true only",
			featureGate: pkgfeatures.CPUManagerPolicyAlphaOptions,
			policyOptions: map[string]string{
				DistributeCPUsAcrossCoresOption: "true",
			},
			expectedErr: false,
		},
		{
			description: "PreferAlignByUnCoreCache and StrictCPUReservation set to true",
			featureGate: pkgfeatures.CPUManagerPolicyAlphaOptions,
			policyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
				StrictCPUReservationOption:     "true",
			},
			expectedErr: false,
		},
		{
			description: "PreferAlignByUnCoreCache and FullPCPUsOnly set to true",
			featureGate: pkgfeatures.CPUManagerPolicyAlphaOptions,
			policyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
				FullPCPUsOnlyOption:            "true",
			},
			expectedErr: false,
		},
		{
			description: "PreferAlignByUnCoreCache and AlignBySocket set to true",
			featureGate: pkgfeatures.CPUManagerPolicyAlphaOptions,
			policyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
				AlignBySocketOption:            "true",
			},
			expectedErr: false,
		},
		{
			description: "FullPhysicalCPUsOnly and DistributeCPUsAcrossCores options can not coexist",
			featureGate: pkgfeatures.CPUManagerPolicyAlphaOptions,
			policyOptions: map[string]string{
				FullPCPUsOnlyOption:             "true",
				DistributeCPUsAcrossCoresOption: "true",
			},
			expectedErr: true,
		},
		{
			description: "PreferAlignByUnCoreCache and DistributeCPUsAcrossCores options can not coexist",
			featureGate: pkgfeatures.CPUManagerPolicyAlphaOptions,
			policyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption:  "true",
				DistributeCPUsAcrossCoresOption: "true",
			},
			expectedErr: true,
		},
		{
			description: "PreferAlignByUnCoreCache and DistributeCPUsAcrossNUMA options can not coexist",
			featureGate: pkgfeatures.CPUManagerPolicyAlphaOptions,
			policyOptions: map[string]string{
				PreferAlignByUnCoreCacheOption: "true",
				DistributeCPUsAcrossNUMAOption: "true",
			},
			expectedErr: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, testCase.featureGate, true)
			_, err := NewStaticPolicyOptions(testCase.policyOptions)
			gotError := err != nil
			if gotError != testCase.expectedErr {
				t.Errorf("testCase %q failed, got %v expected %v", testCase.description, gotError, testCase.expectedErr)
			}
		})
	}
}
