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

// Package testing 提供在单元测试中临时覆盖特性门控的辅助函数。
//
// 它回答两个问题：如何安全地设置/恢复门控，以及如何在并行子测试中
// 及时发现相互覆盖（并发写同一门控会让测试结果不可重复）。
package testing

import (
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
)

var (
	// overrideLock 保护下面两个全局记录：测试可能并行执行，
	// 而这些 map 记录的是“哪个测试正在覆盖”，用于检测并发冲突。
	overrideLock          sync.Mutex
	featureFlagOverride   map[featuregate.Feature]string
	versionsOverride      string
	versionsOverrideValue string
)

func init() {
	featureFlagOverride = map[featuregate.Feature]string{}
}

// FeatureOverrides 是特性名到目标值的映射，
// 允许一次性设置多个门控并统一处理依赖关系。
type FeatureOverrides = map[featuregate.Feature]bool

// SetFeatureGateDuringTest 在测试期间把指定门控设为指定值。
// 检测到第二次设置同一标志，或无法设置、恢复特性标志时失败；禁用特性时会自动禁用所有依赖项。
// 警告：在调用 t.Parallel() 的测试中调用可能泄漏已设置变量，但第二次设置同一特性标志会致命失败。
// 用法示例：
//
// featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.<FeatureName>, true)
func SetFeatureGateDuringTest(tb TB, gate featuregate.FeatureGate, f featuregate.Feature, value bool) {
	tb.Helper()
	SetFeatureGatesDuringTest(tb, gate, FeatureOverrides{f: value})
}

// SetFeatureGatesDuringTest 在测试期间设置指定的特性门控值映射。
// 检测到第二次设置同一标志，或无法设置、恢复特性标志时失败；禁用特性时会自动禁用所有
// 未显式设置的依赖项。警告：在调用 t.Parallel() 的测试中调用可能泄漏已设置变量，
// 但第二次设置同一特性标志会致命失败。用法示例：
//
//	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
//		features.<FeatureName>: true,
//	    features.<FeatureName>: false,
//	})
func SetFeatureGatesDuringTest(tb TB, gate featuregate.FeatureGate, features FeatureOverrides) {
	tb.Helper()
	logger := newLogger(tb)
	// 记录原始仿真版本：测试若在未声明的情况下改变它，收尾时会报错。
	originalEmuVer := gate.(featuregate.MutableVersionedFeatureGate).EmulationVersion()
	originalValues := map[string]bool{}
	var originalUnset []featuregate.Feature
	overrides := FeatureOverrides{}

	// 特殊处理 AllAlpha 和 AllBeta。
	allAlphaValue, allAlpha := features["AllAlpha"]
	allBetaValue, allBeta := features["AllBeta"]
	if allAlpha || allBeta {
		// 逐个遍历门控，以便恢复每个门控各自的值。
		for k, v := range gate.(featuregate.MutableFeatureGate).GetAll() {
			if (allAlpha && v.PreRelease == featuregate.Alpha) || (allBeta && v.PreRelease == featuregate.Beta) {
				// AllAlpha/AllBeta 本身只设置未显式设置的特性，但测试需要覆盖全部
				// alpha/beta 特性，因此除 AllAlpha/AllBeta 外再显式设置每个特性。
				if v.PreRelease == featuregate.Alpha {
					overrides[k] = allAlphaValue
				} else {
					overrides[k] = allBetaValue
				}
			}
		}
	}

	// 显式指定的特性优先，现在合并进来。
	for f, v := range features {
		overrides[f] = v
	}

	// 禁用依赖项时自动禁用依赖它的特性。
	dependencies := gate.Dependencies()
	for f := range dependencies {
		if _, overridden := overrides[f]; overridden || !gate.Enabled(f) {
			continue // 不自动禁用显式设置过的特性。
		}
		// 若特性门控默认启用且其依赖项被显式禁用，则自动禁用该特性。
		if disabled, disabledDep := hasDisabledDependency(f, dependencies, features); disabled {
			tb.Logf("Disabling feature %s since it depends on disabled feature %s", f, disabledDep)
			overrides[f] = false
		}
	}

	for f := range overrides {
		// 先保存当前状态；同时记录哪些门控原本未显式设置，
		// 以便收尾时恢复“未设置”而不是硬编码一个默认值。
		originalValues[string(f)] = gate.Enabled(f)
		if !gate.(featuregate.MutableVersionedFeatureGate).ExplicitlySet(f) {
			originalUnset = append(originalUnset, f)
		}
		// 注册并发覆盖检测；若同一阶段有别的测试也在改同一门控，会立即 Fatal。
		tb.Cleanup(detectParallelOverride(tb, featuregate.Feature(f)))
	}

	m := map[string]bool{}
	for f, v := range overrides {
		m[string(f)] = v
	}
	if err := setFromMap(logger, gate, m); err != nil {
		tb.Errorf("Failed to set feature gates: %v", err)
		for f, v := range features {
			if s := suggestChangeEmulationVersion(tb, gate, f, v); s != "" {
				tb.Errorf("error setting %s=%v: %v. %s", f, v, err, s)
			}
		}
	}

	tb.Cleanup(func() {
		tb.Helper()
		emuVer := gate.(featuregate.MutableVersionedFeatureGate).EmulationVersion()
		// 仿真版本变化会影响多个门控的默认值，不能混在普通 SetFeatureGateDuringTest 里做。
		if !emuVer.EqualTo(originalEmuVer) {
			tb.Fatalf("change of feature gate emulation version from %s to %s in the chain of SetFeatureGateDuringTest is not allowed\nuse SetFeatureGateEmulationVersionDuringTest to change emulation version in tests",
				originalEmuVer.String(), emuVer.String())
		}
		// 为避免违反特性依赖关系，先一次性恢复全部原值，再重置原本未显式设置的特性
		// （它们的值应保持不变）。
		if err := setFromMap(logger, gate, originalValues); err != nil {
			tb.Errorf("error restoring features %v: %v", originalValues, err)
		}
		for _, f := range originalUnset {
			if err := gate.(featuregate.MutableVersionedFeatureGate).ResetFeatureValueToDefault(f); err != nil {
				tb.Errorf("error resetting %s: %v", f, err)
			}
		}
	})
}

// newLogger 构造一个把日志写入 tb 的 logger；
// 这样 SetFromMap 的内部告警会出现在测试输出里而不是标准错误。
func newLogger(tb TB) klog.Logger {
	logger, _ := ktesting.NewTestContext(testingT{tb})
	return logger
}

// testingT 在本包传统要求的 Logf 与 ktesting 需要的 Log 之间充当桥接。
type testingT struct {
	TB
}

func (tb testingT) Log(args ...any) {
	// testing.T.Log 模拟 fmt.Println 而非 fmt.Print，即总会在参数之间添加空格。
	tb.Logf("%s", fmt.Sprintln(args...))
}

// setFromMap 优先使用带 logger 的入口，让设置过程的告警归属到当前测试；
// 旧版门控没有该能力时退回普通 SetFromMap。
func setFromMap(logger klog.Logger, gate featuregate.FeatureGate, m map[string]bool) error {
	if gateWithLogger, ok := gate.(featuregate.MutableFeatureGateWithLogger); ok {
		helper, logger := logger.WithCallStackHelper()
		helper()
		return gateWithLogger.SetFromMapWithLogger(logger, m)
	}
	return gate.(featuregate.MutableFeatureGate).SetFromMap(m)
}

// setEmulationVersionAndMinCompatibilityVersion 是版本设置的类型安全包装：
// 有 logger 版本就优先使用，并把调用点上报给 klog。
func setEmulationVersionAndMinCompatibilityVersion(logger klog.Logger, gate featuregate.FeatureGate, emulationVersion *version.Version, minCompatibilityVersion *version.Version) error {
	if gateWithLogger, ok := gate.(featuregate.MutableVersionedFeatureGateWithLogger); ok {
		helper, logger := logger.WithCallStackHelper()
		helper()
		return gateWithLogger.SetEmulationVersionAndMinCompatibilityVersionWithLogger(logger, emulationVersion, minCompatibilityVersion)
	}
	return gate.(featuregate.MutableVersionedFeatureGate).SetEmulationVersionAndMinCompatibilityVersion(emulationVersion, minCompatibilityVersion)
}

// hasDisabledDependency 递归遍历特性 f 的依赖，检查 features 映射中是否有依赖被显式禁用。
func hasDisabledDependency(f featuregate.Feature, dependencies map[featuregate.Feature][]featuregate.Feature, features map[featuregate.Feature]bool) (bool, featuregate.Feature) {
	if enabled, set := features[f]; set {
		return !enabled, f
	}
	for _, dep := range dependencies[f] {
		if disabled, disabledDep := hasDisabledDependency(dep, dependencies, features); disabled {
			return disabled, disabledDep
		}
	}
	return false, ""
}

// suggestChangeEmulationVersion 在设置失败时给出可操作建议：
// 若目标门控被锁定在某个版本且当前仿真版本已越过该版本，
// 提示在测试开头改用 SetFeatureGateEmulationVersionDuringTest。
func suggestChangeEmulationVersion(tb TB, gate featuregate.FeatureGate, f featuregate.Feature, value bool) string {
	mutableVersionedFeatureGate, ok := gate.(featuregate.MutableVersionedFeatureGate)
	if !ok {
		return ""
	}

	emuVer := mutableVersionedFeatureGate.EmulationVersion()
	versionedSpecs, ok := mutableVersionedFeatureGate.GetAllVersioned()[f]
	if !ok {
		return ""
	}
	if len(versionedSpecs) > 1 {
		// 检查特性是否被锁定。
		lastLifecycle := versionedSpecs[len(versionedSpecs)-1]
		if lastLifecycle.LockToDefault && !lastLifecycle.Version.GreaterThan(emuVer) && lastLifecycle.Default != value {
			// 若特性被锁定，将模拟版本设为该特性尚未锁定的前一个版本。
			return fmt.Sprintf("Feature %s is locked at version %s. Try adding SetFeatureGateEmulationVersionDuringTest(t, gate, version.MustParse(\"1.%d\")) at the beginning of your test.", f, emuVer.String(), lastLifecycle.Version.SubtractMinor(1).Minor())
		}
	}
	return ""
}

// SetFeatureGateVersionsDuringTest 在测试期间把指定门控设为指定的模拟版本和最低兼容版本。
// 检测到第二次设置不同版本，或无法设置、恢复这两个版本时失败。
// 警告：在调用 t.Parallel() 的测试中调用可能泄漏已设置变量，但第二次设置不同版本会致命失败。
// 用法示例：

// featuregatetesting.SetFeatureGateVersionsDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.31"), version.MustParse("1.31"))
func SetFeatureGateVersionsDuringTest(tb TB, gate featuregate.FeatureGate, emuVer, minCompatVer *version.Version) {
	tb.Helper()
	logger := newLogger(tb)
	versions := fmt.Sprintf("emu=%s,min=%s", emuVer.String(), minCompatVer.String())
	detectParallelOverrideCleanup := detectParallelOverrideVersions(tb, versions)

	mutableGate := gate.(featuregate.MutableVersionedFeatureGate)
	originalEmuVer := mutableGate.EmulationVersion()
	originalMinCompatVer := mutableGate.MinCompatibilityVersion()

	if err := setEmulationVersionAndMinCompatibilityVersion(logger, gate, emuVer, minCompatVer); err != nil {
		tb.Fatalf("failed to set versions (emu=%s, min=%s) during test: %v", emuVer.String(), minCompatVer.String(), err)
	}

	tb.Cleanup(func() {
		tb.Helper()
		detectParallelOverrideCleanup()
		if err := setEmulationVersionAndMinCompatibilityVersion(logger, gate, originalEmuVer, originalMinCompatVer); err != nil {
			tb.Fatalf("failed to restore versions (emu=%s, min=%s) during test: %v", originalEmuVer.String(), originalMinCompatVer.String(), err)
		}
	})
}

// SetFeatureGateEmulationVersionDuringTest 在测试期间把指定门控设为指定的模拟版本。
// 检测到第二次设置不同模拟版本，或无法设置、恢复模拟版本时失败。
// 警告：在调用 t.Parallel() 的测试中调用可能泄漏已设置变量，但第二次设置不同版本会致命失败。
// 用法示例：

// featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.31"))
func SetFeatureGateEmulationVersionDuringTest(tb TB, gate featuregate.FeatureGate, ver *version.Version) {
	tb.Helper()
	SetFeatureGateVersionsDuringTest(tb, gate, ver, ver.SubtractMinor(1))
}

// detectParallelOverride 登记当前测试对该门控的覆盖，并在清理时校验
// 期间没有被其他并行测试改写；返回的清理函数需挂到 tb.Cleanup。
func detectParallelOverride(tb TB, f featuregate.Feature) func() {
	tb.Helper()
	overrideLock.Lock()
	defer overrideLock.Unlock()
	beforeOverrideTestName := featureFlagOverride[f]
	if beforeOverrideTestName != "" && !sameTestOrSubtest(tb, beforeOverrideTestName) {
		tb.Fatalf("Detected parallel setting of a feature gate by both %q and %q", beforeOverrideTestName, tb.Name())
	}
	featureFlagOverride[f] = tb.Name()

	return func() {
		tb.Helper()
		overrideLock.Lock()
		defer overrideLock.Unlock()
		if afterOverrideTestName := featureFlagOverride[f]; afterOverrideTestName != tb.Name() {
			tb.Fatalf("Detected parallel setting of a feature gate between both %q and %q", afterOverrideTestName, tb.Name())
		}
		featureFlagOverride[f] = beforeOverrideTestName
	}
}

// detectParallelOverrideVersions 与 detectParallelOverride 同理，
// 但只跟踪一对仿真/最小兼容版本。值完全相同视为无冲突，直接返回空清理函数。
func detectParallelOverrideVersions(tb TB, vers string) func() {
	tb.Helper()
	overrideLock.Lock()
	defer overrideLock.Unlock()
	beforeOverrideTestName := versionsOverride
	beforeOverrideValue := versionsOverrideValue
	if vers == beforeOverrideValue {
		return func() {}
	}
	if beforeOverrideTestName != "" && !sameTestOrSubtest(tb, beforeOverrideTestName) {
		tb.Fatalf("Detected parallel setting of feature gate versions by both %q and %q", beforeOverrideTestName, tb.Name())
	}
	versionsOverride = tb.Name()
	versionsOverrideValue = vers

	return func() {
		tb.Helper()
		overrideLock.Lock()
		defer overrideLock.Unlock()
		if afterOverrideTestName := versionsOverride; afterOverrideTestName != tb.Name() {
			tb.Fatalf("Detected parallel setting of feature gate versions between both %q and %q", afterOverrideTestName, tb.Name())
		}
		versionsOverride = beforeOverrideTestName
		versionsOverrideValue = beforeOverrideValue
	}
}

// sameTestOrSubtest 判断 testName 与当前测试是否同属一棵测试树；
// 子测试名以父名 + "/" 开头，据此识别合法嵌套覆盖。
func sameTestOrSubtest(tb TB, testName string) bool {
	// 假设测试名中不使用 "/"。
	return tb.Name() == testName || strings.HasPrefix(tb.Name(), testName+"/")
}

// TB 是本包需要的最小 testing 接口子集，
// 如此既能用于 *testing.T，也能用于包装后的 fake 实现。
type TB interface {
	Cleanup(func())
	Logf(format string, args ...any)
	Error(args ...any)
	Errorf(format string, args ...any)
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Helper()
	Name() string
}
