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

package framework

import (
	"fmt"
	"os"

	"k8s.io/kubernetes/test/e2e/framework"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
)

func DebugStorageTestsuite(format string, args ...any) {
	if _, ok := os.LookupEnv("DEBUG_STORAGE_TESTSUITE"); !ok {
		return
	}
	fmt.Printf(format, args...)
}

// TestSuite 表示一组与 TestDriver 协同工作的测试的接口。
// 每个 testsuite 都应实现此接口。
// 除 GetTestSuiteInfo() 外的函数都不应直接调用，
// 而应通过 RegisterTests() 以更规范的方式注册测试。
type TestSuite interface {
	GetTestSuiteInfo() TestSuiteInfo
	// DefineTests 为 driver 定义 testpattern 对应的测试。
	// 调用时处于反映当前 driver 和 test pattern 的 Ginkgo 上下文中，
	// 因此测试套件可以直接用 ginkgo.It 定义测试。
	DefineTests(TestDriver, TestPattern)
	// SkipUnsupportedTests 返回给定的 TestPattern 与 TestDriver 组合
	// 无法测试的原因，不会注册任何测试。
	//
	// testsuite 应检查该 pattern 和 driver 对整个 testsuite 是否适用；
	// 针对单个测试用例的检查应在 defineTests 内部进行。不支持的测试
	// 甚至不应被定义。
	SkipUnsupportedTests(TestDriver, TestPattern) string
}

// RegisterTests 将 driver + pattern 组合注册到 TestSuite 内部。
// 该函数在 testsuite 中实际注册测试。
func RegisterTests(suite TestSuite, driver TestDriver, pattern TestPattern) {
	tsInfo := suite.GetTestSuiteInfo()
	var args []interface{}
	args = append(args, fmt.Sprintf("[Testpattern: %s]", pattern.Name))
	args = append(args, pattern.TestTags...)
	args = append(args, tsInfo.Name)
	args = append(args, tsInfo.TestTags...)
	args = append(args, func() {
		reason := SkipInvalidDriverPatternCombination(driver, pattern)
		if reason == "" {
			reason = suite.SkipUnsupportedTests(driver, pattern)
		}
		if reason != "" {
			DebugStorageTestsuite("Skipping registration: [Testpattern: %s] %s [Driver: %s] - %s\n", pattern.Name, tsInfo.Name, driver.GetDriverInfo().Name, reason)
			return
		}

		// 实际定义测试。
		// 此时 testsuite 无需担心 pattern 和 driver 是否适合整个 testsuite，
		// 但对特定的独立测试用例仍可能需要检查 driver 与 pattern。
		suite.DefineTests(driver, pattern)
	})
	framework.Context(args...)
}

// DefineTestSuites 为 driver 定义所有 testpattern 和所有 testSuites 的测试
func DefineTestSuites(driver TestDriver, tsInits []func() TestSuite) {
	for _, testSuiteInit := range tsInits {
		suite := testSuiteInit()
		for _, pattern := range suite.GetTestSuiteInfo().TestPatterns {
			RegisterTests(suite, driver, pattern)
		}
	}
}

// TestSuiteInfo 表示 TestSuite 的一组参数
type TestSuiteInfo struct {
	Name               string              // TestSuite 的名称
	TestTags           []interface{}       // framework.It 的附加参数，例如 framework.WithDisruptive()
	TestPatterns       []TestPattern       // TestSuite 的 TestPattern 切片
	SupportedSizeRange e2evolume.SizeRange // 测试套件支持的 size 范围
}

// SkipInvalidDriverPatternCombination 在 driver 与 testpattern 的组合不兼容、
// 无法测试时跳过测试。该函数在 RegisterTests() 中调用，
// 以确保定义的所有 testsuites 都有效。
//
// 是否需要跳过按以下步骤检查：
// 0. 用 driver 的 SkipUnsupportedTest 检查
// 1. 从接口检查 driver 是否支持 volType
// 2. 检查是否支持 fsType
//
// 测试套件也可以在其自身的 skipUnsupportedTests 函数或单个测试中跳过测试。
func SkipInvalidDriverPatternCombination(driver TestDriver, pattern TestPattern) string {
	dInfo := driver.GetDriverInfo()
	var isSupported bool

	// 0. 用 driver 特定逻辑检查
	if reason := driver.SkipUnsupportedTest(pattern); reason != "" {
		return reason
	}

	// 1. 从接口检查 driver 是否支持 volType
	switch pattern.VolType {
	case InlineVolume:
		_, isSupported = driver.(InlineVolumeTestDriver)
	case PreprovisionedPV:
		_, isSupported = driver.(PreprovisionedPVTestDriver)
	case DynamicPV, GenericEphemeralVolume:
		_, isSupported = driver.(DynamicPVTestDriver)
	case CSIInlineVolume:
		_, isSupported = driver.(EphemeralTestDriver)
	default:
		isSupported = false
	}

	if !isSupported {
		return fmt.Sprintf("Driver %s doesn't support %v", dInfo.Name, pattern.VolType)
	}

	// 2. 检查是否支持 fsType
	if !dInfo.SupportedFsType.Has(pattern.FsType) {
		return fmt.Sprintf("Driver %s doesn't support %v", dInfo.Name, pattern.FsType)
	}
	if pattern.FsType == "xfs" && framework.NodeOSDistroIs("windows") {
		return "Distro doesn't support xfs"
	}
	if pattern.FsType == "ntfs" && !framework.NodeOSDistroIs("windows") {
		return fmt.Sprintf("Distro %s doesn't support ntfs", framework.TestContext.NodeOSDistro)
	}

	return ""
}
