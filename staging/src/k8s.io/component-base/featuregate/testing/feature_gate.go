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

// SetFeatureGateDuringTest sets the specified gate to the specified value for duration of the test.
// Fails when it detects second call to the same flag or is unable to set or restore feature flag.
// When disabling a feature, this automatically disables all dependents.
//
// WARNING: Can leak set variable when called in test calling t.Parallel(), however second attempt to set the same feature flag will cause fatal.
//
// Example use:
//
// featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.<FeatureName>, true)
func SetFeatureGateDuringTest(tb TB, gate featuregate.FeatureGate, f featuregate.Feature, value bool) {
	tb.Helper()
	SetFeatureGatesDuringTest(tb, gate, FeatureOverrides{f: value})
}

// SetFeatureGatesDuringTest sets the specified map of feature gate values for duration of the test.
// Fails when it detects second call to the same flag or is unable to set or restore feature flag.
// When disabling a feature, this automatically disables all dependents that weren't explicitly set.
//
// WARNING: Can leak set variable when called in test calling t.Parallel(), however second attempt to set the same feature flag will cause fatal.
//
// Example use:
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

	// Specially handle AllAlpha and AllBeta
	allAlphaValue, allAlpha := features["AllAlpha"]
	allBetaValue, allBeta := features["AllBeta"]
	if allAlpha || allBeta {
		// Iterate over individual gates so their individual values get restored
		for k, v := range gate.(featuregate.MutableFeatureGate).GetAll() {
			if (allAlpha && v.PreRelease == featuregate.Alpha) || (allBeta && v.PreRelease == featuregate.Beta) {
				// Setting AllAlpha or AllBeta on their own only sets unset features, but for
				// testing we want to override ALL alpha/beta features. So we explicitly set each
				// alpha/beta feature in addition to AllAlpha/AllBeta
				if v.PreRelease == featuregate.Alpha {
					overrides[k] = allAlphaValue
				} else {
					overrides[k] = allBetaValue
				}
			}
		}
	}

	// Explicit features take precedence, so merge them in now.
	for f, v := range features {
		overrides[f] = v
	}

	// Automatically disable dependents when disabling a dependency.
	dependencies := gate.Dependencies()
	for f := range dependencies {
		if _, overridden := overrides[f]; overridden || !gate.Enabled(f) {
			continue // Don't automatically disable features that have been explicitly set.
		}
		// If the feature gate was default-enabled and has an explicitly
		// disabled dependency, then automatically disable it.
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
		// To avoid violating feature dependencies, first atomicaly restore all original values,
		// then reset features that were unset (the value should be unchanged).
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

// testingT is a bridge between what this package traditionally has required (Logf) and
// what ktesting needs (Log).
type testingT struct {
	TB
}

func (tb testingT) Log(args ...any) {
	// testing.T.Log emulates fmt.Println, not fmt.Print, i.e. it always
	// adds spaces between arguments.
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

// hasDisabledDependency recursively walks the dependencies for feature f, and checks whether any are explicitly disabled in the features map.
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
		// check if the feature is locked
		lastLifecycle := versionedSpecs[len(versionedSpecs)-1]
		if lastLifecycle.LockToDefault && !lastLifecycle.Version.GreaterThan(emuVer) && lastLifecycle.Default != value {
			// if the feature is locked, set the emulation version to the previous version when the feature is not locked.
			return fmt.Sprintf("Feature %s is locked at version %s. Try adding SetFeatureGateEmulationVersionDuringTest(t, gate, version.MustParse(\"1.%d\")) at the beginning of your test.", f, emuVer.String(), lastLifecycle.Version.SubtractMinor(1).Minor())
		}
	}
	return ""
}

// SetFeatureGateVersionsDuringTest sets the specified gate to the specified emulation version and min compatibility version for duration of the test.
// Fails when it detects second call to set a different emulation version or min compatibility version, or is unable to set or restore emulation version and min compatibility version.
// WARNING: Can leak set variable when called in test calling t.Parallel(), however second attempt to set a different emulation version or min compatibility version will cause fatal.
// Example use:

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

// SetFeatureGateEmulationVersionDuringTest sets the specified gate to the specified emulation version for duration of the test.
// Fails when it detects second call to set a different emulation version or is unable to set or restore emulation version.
// WARNING: Can leak set variable when called in test calling t.Parallel(), however second attempt to set a different emulation version will cause fatal.
// Example use:

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
	// Assumes that "/" is not used in test names.
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
