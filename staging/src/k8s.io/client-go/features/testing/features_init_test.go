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

package testing

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/client-go/features"
)

// replaceFeatureGatesForTest 在测试期间全局替换 feature gates，并在测试结束时恢复原值，
// 避免测试随机顺序执行时 fake gates 泄漏到其他测试。
func replaceFeatureGatesForTest(t *testing.T, newGates features.Gates) {
	t.Helper()
	original := features.FeatureGates()
	t.Cleanup(func() {
		features.ReplaceFeatureGates(original)
	})
	features.ReplaceFeatureGates(newGates)
}

// TestDriveInitDefaultFeatureGates 验证默认 gates 对未注册特性会 panic，
// 以及用 fake gates 替换后能按预期返回特性开关值。
func TestDriveInitDefaultFeatureGates(t *testing.T) {
	featureGates := features.FeatureGates()
	// 默认实现只认识编译时注册的特性，未注册的访问属于编程错误。
	assertFunctionPanicsWithMessage(t, func() { featureGates.Enabled("FakeFeatureGate") }, "features.FeatureGates().Enabled", fmt.Sprintf("feature %q is not registered in FeatureGate", "FakeFeatureGate"))

	fakeGates := &fakeFeatureGates{features: map[features.Feature]bool{"FakeFeatureGate": true}}
	require.True(t, fakeGates.Enabled("FakeFeatureGate"))

	replaceFeatureGatesForTest(t, fakeGates)
	featureGates = features.FeatureGates()

	assertFeatureGatesType(t, featureGates)
	require.True(t, featureGates.Enabled("FakeFeatureGate"))
}

// TestSetFeatureGatesDuringTest 验证 SetFeatureDuringTest 修改单个特性后，
// 只影响该特性，并在测试结束后恢复原始 gates。
func TestSetFeatureGatesDuringTest(t *testing.T) {
	featureA := features.Feature("FeatureA")
	featureB := features.Feature("FeatureB")
	fakeGates := &fakeFeatureGates{map[features.Feature]bool{featureA: true, featureB: true}}
	replaceFeatureGatesForTest(t, fakeGates)
	t.Cleanup(func() {
		// Cleanup 按后进先出执行，因此这里在 replaceFeatureGatesForTest 的恢复之后运行，
		// 用来确认原始 gates 已被恢复。
		require.True(t, features.FeatureGates().Enabled(featureA), "the original feature = %v wasn't restored", featureA)
	})

	SetFeatureDuringTest(t, featureA, false)

	require.False(t, features.FeatureGates().Enabled(featureA))
	require.True(t, features.FeatureGates().Enabled(featureB))
}

// TestSetFeatureGatesDuringTestPanics 验证对未知特性或只读 gates 设置特性会 panic。
func TestSetFeatureGatesDuringTestPanics(t *testing.T) {
	fakeGates := &fakeFeatureGates{features: map[features.Feature]bool{"FakeFeatureGate": true}}

	replaceFeatureGatesForTest(t, fakeGates)
	assertFunctionPanicsWithMessage(t, func() { SetFeatureDuringTest(t, "UnknownFeature", false) }, "SetFeatureDuringTest", fmt.Sprintf("feature %q is not registered in featureGates", "UnknownFeature"))

	// 只读实现不满足可写接口，必须明确拒绝而不是静默失败。
	readOnlyGates := &readOnlyAlwaysDisabledFeatureGates{}
	replaceFeatureGatesForTest(t, readOnlyGates)
	assertFunctionPanicsWithMessage(t, func() { SetFeatureDuringTest(t, "FakeFeature", false) }, "SetFeatureDuringTest", fmt.Sprintf("clientfeatures.FeatureGates(): %T does not implement featureGatesSetter interface", readOnlyGates))
}

// TestOverridesForSetFeatureGatesDuringTest 覆盖并发覆盖检测的边界：
// 不同测试并发设置同一特性应报错，同一个测试或子测试重复设置则允许。
func TestOverridesForSetFeatureGatesDuringTest(t *testing.T) {
	scenarios := []struct {
		name           string
		firstTestName  string
		secondTestName string
		expectError    bool
	}{
		{
			name:           "concurrent tests setting the same feature fail",
			firstTestName:  "fooTest",
			secondTestName: "barTest",
			expectError:    true,
		},

		{
			name:           "same test setting the same feature does not fail",
			firstTestName:  "fooTest",
			secondTestName: "fooTest",
			expectError:    false,
		},

		{
			name:           "subtests setting the same feature don't not fail",
			firstTestName:  "fooTest",
			secondTestName: "fooTest/scenario1",
			expectError:    false,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			featureA := features.Feature("FeatureA")
			fakeGates := &fakeFeatureGates{map[features.Feature]bool{featureA: true}}
			fakeTesting := &fakeT{fakeTestName: scenario.firstTestName, TB: t}

			replaceFeatureGatesForTest(t, fakeGates)
			require.NoError(t, setFeatureDuringTestInternal(fakeTesting, featureA, true))
			require.True(t, features.FeatureGates().Enabled(featureA))

			// 第二次设置来自不同的测试名，用于模拟并发测试间的冲突。
			fakeTesting.fakeTestName = scenario.secondTestName
			err := setFeatureDuringTestInternal(fakeTesting, featureA, false)
			require.Equal(t, scenario.expectError, err != nil)
		})
	}
}

// fakeFeatureGates 是测试用的可变 feature gates，未注册特性访问时 panic。
type fakeFeatureGates struct {
	features map[features.Feature]bool
}

func (f *fakeFeatureGates) Enabled(feature features.Feature) bool {
	featureValue, ok := f.features[feature]
	if !ok {
		panic(fmt.Errorf("feature %q is not registered in featureGates", feature))
	}
	return featureValue
}

func (f *fakeFeatureGates) Set(feature features.Feature, value bool) error {
	f.features[feature] = value
	return nil
}

// readOnlyAlwaysDisabledFeatureGates 模拟只读 gates：所有特性恒为 false，且无法设置。
type readOnlyAlwaysDisabledFeatureGates struct{}

func (f *readOnlyAlwaysDisabledFeatureGates) Enabled(feature features.Feature) bool {
	return false
}

// fakeT 包装 testing.TB，并允许伪造测试名以验证并发覆盖检测逻辑。
type fakeT struct {
	fakeTestName string
	testing.TB
}

func (t *fakeT) Name() string {
	return t.fakeTestName
}

// assertFeatureGatesType 断言全局 gates 已被替换为测试用实现。
func assertFeatureGatesType(t *testing.T, fg features.Gates) {
	_, ok := fg.(*fakeFeatureGates)
	if !ok {
		t.Fatalf("passed features.FeatureGates() is NOT of type *alwaysEnabledFakeGates, it is of type = %T", fg)
	}
}

// assertFunctionPanicsWithMessage 断言 f 会 panic，且 panic 的错误信息包含 errMessage。
func assertFunctionPanicsWithMessage(t *testing.T, f func(), fName, errMessage string) {
	didPanic, panicMessage := didFunctionPanic(f)
	if !didPanic {
		t.Fatalf("function %q did not panicked", fName)
	}

	panicError, ok := panicMessage.(error)
	if !ok || !strings.Contains(panicError.Error(), errMessage) {
		t.Fatalf("func %q should panic with error message:\t%#v\n\tPanic value:\t%#v\n", fName, errMessage, panicMessage)
	}
}

// didFunctionPanic 执行 f 并报告其是否 panic、以及 panic 的值。
func didFunctionPanic(f func()) (didPanic bool, panicMessage interface{}) {
	didPanic = true

	defer func() {
		panicMessage = recover()
	}()

	f()
	didPanic = false

	return
}
