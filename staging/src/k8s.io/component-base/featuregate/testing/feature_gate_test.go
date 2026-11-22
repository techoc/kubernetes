/*
Copyright 2023 The Kubernetes Authors.

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
	"maps"
	gotest "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
)

// TestSpecialGates 验证 AllAlpha/AllBeta 这些“特殊门控”的覆盖语义：
// 打开特殊门控会同时翻转所有对应阶段的普通门控，但不影响 GA 门控。
func TestSpecialGates(t *gotest.T) {
	gate := featuregate.NewFeatureGate()
	gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		"alpha_default_on":         {PreRelease: featuregate.Alpha, Default: true},
		"alpha_default_on_set_off": {PreRelease: featuregate.Alpha, Default: true},
		"alpha_default_off":        {PreRelease: featuregate.Alpha, Default: false},
		"alpha_default_off_set_on": {PreRelease: featuregate.Alpha, Default: false},

		"beta_default_on":         {PreRelease: featuregate.Beta, Default: true},
		"beta_default_on_set_off": {PreRelease: featuregate.Beta, Default: true},
		"beta_default_off":        {PreRelease: featuregate.Beta, Default: false},
		"beta_default_off_set_on": {PreRelease: featuregate.Beta, Default: false},

		"stable_default_on":         {PreRelease: featuregate.GA, Default: true},
		"stable_default_on_set_off": {PreRelease: featuregate.GA, Default: true},
		"stable_default_off":        {PreRelease: featuregate.GA, Default: false},
		"stable_default_off_set_on": {PreRelease: featuregate.GA, Default: false},
	})
	gate.Set("alpha_default_on_set_off=false")
	gate.Set("beta_default_on_set_off=false")
	gate.Set("stable_default_on_set_off=false")
	gate.Set("alpha_default_off_set_on=true")
	gate.Set("beta_default_off_set_on=true")
	gate.Set("stable_default_off_set_on=true")

	before := map[featuregate.Feature]bool{
		"AllAlpha": false,
		"AllBeta":  false,

		"alpha_default_on":         true,
		"alpha_default_on_set_off": false,
		"alpha_default_off":        false,
		"alpha_default_off_set_on": true,

		"beta_default_on":         true,
		"beta_default_on_set_off": false,
		"beta_default_off":        false,
		"beta_default_off_set_on": true,

		"stable_default_on":         true,
		"stable_default_on_set_off": false,
		"stable_default_off":        false,
		"stable_default_off_set_on": true,
	}
	expect(t, gate, before)
	t.Cleanup(func() {
		// 所有覆盖应已在各自 Cleanup 中恢复；这里做最后校验，然后清空全局记录。
		expect(t, gate, before)
		cleanup()
	})

	SetFeatureGateDuringTest(t, gate, "AllAlpha", true)
	expect(t, gate, map[featuregate.Feature]bool{
		"AllAlpha": true,
		"AllBeta":  false,

		"alpha_default_on":         true,
		"alpha_default_on_set_off": true,
		"alpha_default_off":        true,
		"alpha_default_off_set_on": true,

		"beta_default_on":         true,
		"beta_default_on_set_off": false,
		"beta_default_off":        false,
		"beta_default_off_set_on": true,

		"stable_default_on":         true,
		"stable_default_on_set_off": false,
		"stable_default_off":        false,
		"stable_default_off_set_on": true,
	})

	SetFeatureGateDuringTest(t, gate, "AllBeta", true)
	expect(t, gate, map[featuregate.Feature]bool{
		"AllAlpha": true,
		"AllBeta":  true,

		"alpha_default_on":         true,
		"alpha_default_on_set_off": true,
		"alpha_default_off":        true,
		"alpha_default_off_set_on": true,

		"beta_default_on":         true,
		"beta_default_on_set_off": true,
		"beta_default_off":        true,
		"beta_default_off_set_on": true,

		"stable_default_on":         true,
		"stable_default_on_set_off": false,
		"stable_default_off":        false,
		"stable_default_off_set_on": true,
	})
}

// expect 逐项断言门控当前值；失败信息包含门控名与期望值。
func expect(t *gotest.T, gate featuregate.FeatureGate, expect map[featuregate.Feature]bool) {
	t.Helper()
	for k, v := range expect {
		if gate.Enabled(k) != v {
			t.Errorf("Expected %v=%v, got %v", k, v, gate.Enabled(k))
		}
	}
}

// TestSetFeatureGateInTest 验证子测试中的覆盖：
// 普通子测试继承父测试的覆盖；子测试可以临时改写，退出后自动恢复。
func TestSetFeatureGateInTest(t *gotest.T) {
	gate := featuregate.NewFeatureGate()
	err := gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		"feature": {PreRelease: featuregate.Alpha, Default: false},
	})
	require.NoError(t, err)

	assert.False(t, gate.Enabled("feature"))
	SetFeatureGateDuringTest(t, gate, "feature", true)
	SetFeatureGateDuringTest(t, gate, "feature", true)

	assert.True(t, gate.Enabled("feature"))
	t.Run("Subtest", func(t *gotest.T) {
		// 子测试默认继承父测试设置的值。
		assert.True(t, gate.Enabled("feature"))
	})

	t.Run("ParallelSubtest", func(t *gotest.T) {
		assert.True(t, gate.Enabled("feature"))
		// Calling t.Parallel in subtest will resume the main test body
		// 父测试会在子测试暂停后继续执行，因此这里仍能看到覆盖值。
		t.Parallel()
		assert.True(t, gate.Enabled("feature"))
	})
	assert.True(t, gate.Enabled("feature"))

	t.Run("OverwriteInSubtest", func(t *gotest.T) {
		// 子测试内的覆盖在子测试结束后由 Cleanup 自动恢复。
		SetFeatureGateDuringTest(t, gate, "feature", false)
		assert.False(t, gate.Enabled("feature"))
	})
	assert.True(t, gate.Enabled("feature"))
}

// TestSetFeatureGatesInTestWithDependencies 覆盖依赖关系处理：
// 关闭某门控会自动关闭依赖它的门控；但不会自动开启依赖，
// 且与显式设置冲突时必须报错。
func TestSetFeatureGatesInTestWithDependencies(t *gotest.T) {
	const (
		alphaFeature      = "AlphaFeature"
		alphaFeatureDep   = "AlphaFeatureDep"
		betaOffFeature    = "BetaOffFeature"
		betaOffFeatureDep = "BetaOffFeatureDep"
		betaOnFeature     = "BetaOnFeature"
		betaOnFeatureDep  = "BetaOnFeatureDep"
		gaFeature         = "GAFeature"
	)
	gate := featuregate.NewFeatureGate()
	require.NoError(t, gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		alphaFeature:      {PreRelease: featuregate.Alpha, Default: false},
		alphaFeatureDep:   {PreRelease: featuregate.Alpha, Default: false},
		betaOffFeature:    {PreRelease: featuregate.Beta, Default: false},
		betaOffFeatureDep: {PreRelease: featuregate.Beta, Default: false},
		betaOnFeature:     {PreRelease: featuregate.Beta, Default: true},
		betaOnFeatureDep:  {PreRelease: featuregate.Beta, Default: true},
		gaFeature:         {PreRelease: featuregate.GA, Default: true},
	}))
	require.NoError(t, gate.AddDependencies(map[featuregate.Feature][]featuregate.Feature{
		alphaFeature:   {betaOnFeatureDep, betaOffFeatureDep, alphaFeatureDep},
		betaOffFeature: {betaOnFeatureDep, betaOffFeatureDep},
		betaOnFeature:  {betaOnFeatureDep},
		gaFeature:      {},
	}))

	initialState := map[featuregate.Feature]bool{
		"AllAlpha": false,
		"AllBeta":  false,

		alphaFeature:      false,
		alphaFeatureDep:   false,
		betaOffFeature:    false,
		betaOffFeatureDep: false,
		betaOnFeature:     true,
		betaOnFeatureDep:  true,
		gaFeature:         true,
	}
	expect(t, gate, initialState)

	tests := []struct {
		name              string
		overrides         FeatureOverrides
		expectedOverrides FeatureOverrides
		expectError       bool
	}{{
		name:      "AllBeta",
		overrides: FeatureOverrides{"AllBeta": true},
		expectedOverrides: FeatureOverrides{
			"AllBeta":         true,
			betaOffFeature:    true,
			betaOffFeatureDep: true,
		},
	}, {
		name:      "AllBeta=false",
		overrides: FeatureOverrides{"AllBeta": false},
		expectedOverrides: FeatureOverrides{
			"AllBeta":        false,
			betaOnFeature:    false,
			betaOnFeatureDep: false,
		},
	}, {
		name:      "AllBeta+AllAlpha",
		overrides: FeatureOverrides{"AllBeta": true, "AllAlpha": true},
		expectedOverrides: FeatureOverrides{
			"AllAlpha":        true,
			"AllBeta":         true,
			alphaFeature:      true,
			alphaFeatureDep:   true,
			betaOffFeature:    true,
			betaOffFeatureDep: true,
		},
	}, {
		name:        "AllAlpha",
		overrides:   FeatureOverrides{"AllAlpha": true},
		expectError: true,
	}, {
		name:      "Automatically disable deps",
		overrides: FeatureOverrides{betaOnFeatureDep: false},
		expectedOverrides: FeatureOverrides{
			betaOnFeature:    false,
			betaOnFeatureDep: false,
		},
	}, {
		name:      "Don't automatically enable deps",
		overrides: FeatureOverrides{betaOffFeatureDep: true},
		expectedOverrides: FeatureOverrides{
			betaOffFeatureDep: true,
			betaOffFeature:    false,
		},
	}, {
		name: "Error when disabling dependency",
		overrides: FeatureOverrides{
			betaOnFeature:    true, // Explicitly enabled so it's not automatically disabled.
			betaOnFeatureDep: false,
		},
		expectError: true,
	}, {
		name: "Error when enabling dependent",
		overrides: FeatureOverrides{
			betaOffFeature: true,
		},
		expectError: true,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *gotest.T) {
			// Separate inner test so we can verify cleanup in the outer test.
			t.Run("Set", func(t *gotest.T) {
				if test.expectError {
					fakeT := &ignoreErrorT{ignoreFatalT: &ignoreFatalT{T: t}}
					SetFeatureGatesDuringTest(fakeT, gate, test.overrides)
					require.True(t, fakeT.errorRecorded, "should capture error")
					expect(t, gate, initialState)
				} else {
					SetFeatureGatesDuringTest(t, gate, test.overrides)
					expectedState := maps.Clone(initialState)
					maps.Copy(expectedState, test.expectedOverrides)
					expect(t, gate, expectedState)
				}
			})
			// Verify revert in cleanup.
			expect(t, gate, initialState)
		})
	}
}

func TestSpecialGatesVersioned(t *gotest.T) {
	// 使用带版本的门控：不同仿真版本下同一门控的默认值可能不同。
	originalEmulationVersion := version.MustParse("1.31")
	gate := featuregate.NewVersionedFeatureGate(originalEmulationVersion)

	err := gate.AddVersioned(map[featuregate.Feature]featuregate.VersionedSpecs{
		"alpha_default_on": {
			{Version: version.MustParse("1.27"), Default: true, PreRelease: featuregate.Alpha},
		},
		"alpha_default_off": {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		},
		"beta_default_on": {
			{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		},
		"beta_default_on_set_off": {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		},
		"beta_default_off": {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Beta},
		},
		"beta_default_off_set_on": {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		},
	})
	require.NoError(t, err)

	require.NoError(t, gate.Set("beta_default_on_set_off=false"))
	require.NoError(t, gate.Set("beta_default_off_set_on=true"))

	before := map[featuregate.Feature]bool{
		"AllAlpha": false,
		"AllBeta":  false,

		"alpha_default_on":  true,
		"alpha_default_off": false,

		"beta_default_on":         true,
		"beta_default_on_set_off": false,
		"beta_default_off":        false,
		"beta_default_off_set_on": true,
	}
	expect(t, gate, before)
	t.Cleanup(func() {
		expect(t, gate, before)
		cleanup()
	})

	t.Run("OverwriteInSubtest", func(t *gotest.T) {
		SetFeatureGateDuringTest(t, gate, "AllAlpha", true)
		expect(t, gate, map[featuregate.Feature]bool{
			"AllAlpha": true,
			"AllBeta":  false,

			"alpha_default_on":  true,
			"alpha_default_off": true,

			"beta_default_on":         true,
			"beta_default_on_set_off": false,
			"beta_default_off":        false,
			"beta_default_off_set_on": true,
		})

		SetFeatureGateDuringTest(t, gate, "AllBeta", true)
		expect(t, gate, map[featuregate.Feature]bool{
			"AllAlpha": true,
			"AllBeta":  true,

			"alpha_default_on":  true,
			"alpha_default_off": true,

			"beta_default_on":         true,
			"beta_default_on_set_off": true,
			"beta_default_off":        true,
			"beta_default_off_set_on": true,
		})
	})
	expect(t, gate, before)

	t.Run("OverwriteInSubtestAtDifferentVersion", func(t *gotest.T) {
		// 切换到 1.28 后，beta 门控按该版本的规格取默认值。
		SetFeatureGateEmulationVersionDuringTest(t, gate, version.MustParse("1.28"))
		SetFeatureGateDuringTest(t, gate, "AllAlpha", true)
		expect(t, gate, map[featuregate.Feature]bool{
			"AllAlpha": true,
			"AllBeta":  false,

			"alpha_default_on":  true,
			"alpha_default_off": true,

			"beta_default_on":         false,
			"beta_default_on_set_off": true,
			"beta_default_off":        false,
			"beta_default_off_set_on": true,
		})
	})

}

// TestDetectLeakToMainTest 验证子测试调用 t.Parallel 后覆盖会泄漏到父测试，
// 第二次设置不同值时应被并发检测捕获（Fatal）。
func TestDetectLeakToMainTest(t *gotest.T) {
	t.Cleanup(cleanup)
	gate := featuregate.NewFeatureGate()
	err := gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		"feature": {PreRelease: featuregate.Alpha, Default: false},
	})
	require.NoError(t, err)

	// Subtest setting feature gate and calling parallel will leak it out
	t.Run("LeakingSubtest", func(t *gotest.T) {
		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateDuringTest(fakeT, gate, "feature", true)
		// Calling t.Parallel in subtest will resume the main test body
		t.Parallel()
		// Leaked false from main test
		assert.False(t, gate.Enabled("feature"))
	})
	// Leaked true from subtest
	assert.True(t, gate.Enabled("feature"))
	fakeT := &ignoreFatalT{T: t}
	SetFeatureGateDuringTest(fakeT, gate, "feature", false)
	assert.True(t, fakeT.fatalRecorded)
}

// TestDetectLeakToOtherSubtest 验证泄漏到同级子测试也能被检测到：
// 第二个子测试看到的是上一个子测试遗留的值。
func TestDetectLeakToOtherSubtest(t *gotest.T) {
	t.Cleanup(cleanup)
	gate := featuregate.NewFeatureGate()
	err := gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		"feature": {PreRelease: featuregate.Alpha, Default: false},
	})
	require.NoError(t, err)

	subtestName := "Subtest"
	// Subtest setting feature gate and calling parallel will leak it out
	t.Run(subtestName, func(t *gotest.T) {
		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateDuringTest(fakeT, gate, "feature", true)
		t.Parallel()
	})
	// Add suffix to name to prevent tests with the same prefix.
	t.Run(subtestName+"Suffix", func(t *gotest.T) {
		// Leaked true
		assert.True(t, gate.Enabled("feature"))

		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateDuringTest(fakeT, gate, "feature", false)
		assert.True(t, fakeT.fatalRecorded)
	})
}

// TestCannotDetectLeakFromSubtest 记录已知局限：若父测试先设置过同一门控，
// 子测试的并行覆盖无法与父测试区分，检测不到。
func TestCannotDetectLeakFromSubtest(t *gotest.T) {
	t.Cleanup(cleanup)
	gate := featuregate.NewFeatureGate()
	err := gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		"feature": {PreRelease: featuregate.Alpha, Default: false},
	})
	require.NoError(t, err)

	SetFeatureGateDuringTest(t, gate, "feature", false)
	// Subtest setting feature gate and calling parallel will leak it out
	t.Run("Subtest", func(t *gotest.T) {
		SetFeatureGateDuringTest(t, gate, "feature", true)
		t.Parallel()
	})
	// Leaked true
	assert.True(t, gate.Enabled("feature"))
}

// TestCannotDetectLeakFromTwoSubtestsWithDifferentFeatures 说明检测按门控粒度：
// 两个子测试并行修改不同门控时互不干扰，因此不会报错。
func TestCannotDetectLeakFromTwoSubtestsWithDifferentFeatures(t *gotest.T) {
	t.Cleanup(cleanup)
	gate := featuregate.NewFeatureGate()
	err := gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		"feature1": {PreRelease: featuregate.Alpha, Default: false},
		"feature2": {PreRelease: featuregate.Alpha, Default: false},
	})
	require.NoError(t, err)

	assert.False(t, gate.Enabled("feature1"))
	assert.False(t, gate.Enabled("feature2"))
	subtestName := "Subtest"
	// Subtest setting feature gate and calling parallel will leak it out
	t.Run(subtestName, func(t *gotest.T) {
		SetFeatureGateDuringTest(t, gate, "feature1", true)
		t.Parallel()
		assert.True(t, gate.Enabled("feature1"))
		assert.False(t, gate.Enabled("feature2"))
	})
	// Leaked true
	assert.True(t, gate.Enabled("feature1"))
	assert.False(t, gate.Enabled("feature2"))
	// Add suffix to name to prevent tests with the same prefix.
	t.Run(subtestName+"Suffix", func(t *gotest.T) {
		// Leaked true
		assert.True(t, gate.Enabled("feature1"))
		assert.False(t, gate.Enabled("feature2"))
		SetFeatureGateDuringTest(t, gate, "feature2", true)
		assert.True(t, gate.Enabled("feature1"))
		assert.True(t, gate.Enabled("feature2"))
	})
}

// TestDetectEmulationVersionLeakToMainTest 与门控值泄漏同理，
// 仿真版本也会泄漏，并在第二次设置不同版本时被检测。
func TestDetectEmulationVersionLeakToMainTest(t *gotest.T) {
	t.Cleanup(cleanup)
	originalEmulationVersion := version.MustParse("1.31")
	newEmulationVersion := version.MustParse("1.30")
	gate := featuregate.NewVersionedFeatureGate(originalEmulationVersion)
	assert.True(t, gate.EmulationVersion().EqualTo(originalEmulationVersion))

	// Subtest setting feature gate and calling parallel will leak it out
	t.Run("LeakingSubtest", func(t *gotest.T) {
		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateEmulationVersionDuringTest(fakeT, gate, newEmulationVersion)
		// Calling t.Parallel in subtest will resume the main test body
		t.Parallel()
		// Leaked from main test
		assert.True(t, gate.EmulationVersion().EqualTo(originalEmulationVersion))
	})
	// Leaked from subtest
	assert.True(t, gate.EmulationVersion().EqualTo(newEmulationVersion))
	fakeT := &ignoreFatalT{T: t}
	SetFeatureGateEmulationVersionDuringTest(fakeT, gate, originalEmulationVersion)
	assert.True(t, fakeT.fatalRecorded)
}

// TestNoLeakFromSameEmulationVersionToMainTest 验证设置相同版本不会误报：
// 值未发生变化时，并发检测应放行。
func TestNoLeakFromSameEmulationVersionToMainTest(t *gotest.T) {
	t.Cleanup(cleanup)
	originalEmulationVersion := version.MustParse("1.31")
	newEmulationVersion := version.MustParse("1.31")
	gate := featuregate.NewVersionedFeatureGate(originalEmulationVersion)
	assert.True(t, gate.EmulationVersion().EqualTo(originalEmulationVersion))

	// Subtest setting feature gate and calling parallel will leak it out
	t.Run("LeakingSubtest", func(t *gotest.T) {
		SetFeatureGateEmulationVersionDuringTest(t, gate, newEmulationVersion)
		// Calling t.Parallel in subtest will resume the main test body
		t.Parallel()
		// Leaked from main test
		assert.True(t, gate.EmulationVersion().EqualTo(originalEmulationVersion))
	})
	// Leaked from subtest
	assert.True(t, gate.EmulationVersion().EqualTo(newEmulationVersion))
	SetFeatureGateEmulationVersionDuringTest(t, gate, originalEmulationVersion)
}

// TestDetectEmulationVersionLeakToOtherSubtest 验证版本泄漏到同级子测试
// 时同样会被检测。
func TestDetectEmulationVersionLeakToOtherSubtest(t *gotest.T) {
	t.Cleanup(cleanup)
	originalEmulationVersion := version.MustParse("1.31")
	newEmulationVersion := version.MustParse("1.30")
	gate := featuregate.NewVersionedFeatureGate(originalEmulationVersion)
	assert.True(t, gate.EmulationVersion().EqualTo(originalEmulationVersion))

	subtestName := "Subtest"
	// Subtest setting feature gate and calling parallel will leak it out
	t.Run(subtestName, func(t *gotest.T) {
		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateEmulationVersionDuringTest(fakeT, gate, newEmulationVersion)
		t.Parallel()
	})
	// Add suffix to name to prevent tests with the same prefix.
	t.Run(subtestName+"Suffix", func(t *gotest.T) {
		// Leaked newEmulationVersion
		assert.True(t, gate.EmulationVersion().EqualTo(newEmulationVersion))

		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateEmulationVersionDuringTest(fakeT, gate, originalEmulationVersion)
		assert.True(t, fakeT.fatalRecorded)
	})
}

// TestSetFeatureGateVersionsInTest 验证同时设置仿真版本与最小兼容版本：
// 嵌套子测试可以继续收窄，退出后逐层恢复。
func TestSetFeatureGateVersionsInTest(t *gotest.T) {
	t.Cleanup(cleanup)
	originalEmuVer := version.MustParse("1.31")
	originalMinCompatVer := version.MustParse("1.30")
	gate := featuregate.NewVersionedFeatureGateWithMinCompatibility(originalEmuVer, originalMinCompatVer)

	assert.True(t, gate.EmulationVersion().EqualTo(originalEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(originalMinCompatVer))

	newEmuVer := version.MustParse("1.29")
	newMinCompatVer := version.MustParse("1.27")

	SetFeatureGateVersionsDuringTest(t, gate, newEmuVer, newMinCompatVer)
	assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))

	t.Run("Subtest", func(t *gotest.T) {
		assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
		assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))
		newerEmuVer := version.MustParse("1.27")
		newerMinCompatVer := version.MustParse("1.26")
		SetFeatureGateVersionsDuringTest(t, gate, newerEmuVer, newerMinCompatVer)
		assert.True(t, gate.EmulationVersion().EqualTo(newerEmuVer))
		assert.True(t, gate.MinCompatibilityVersion().EqualTo(newerMinCompatVer))
	})
	assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))

	t.Run("ParallelSubtest", func(t *gotest.T) {
		assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
		assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))
		t.Parallel()
		assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
		assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))
	})
	assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))
}

// TestDetectVersionsLeakToMainTest 验证版本对泄漏到父测试时的检测。
func TestDetectVersionsLeakToMainTest(t *gotest.T) {
	t.Cleanup(cleanup)
	originalEmuVer := version.MustParse("1.31")
	originalMinCompatVer := version.MustParse("1.30")
	gate := featuregate.NewVersionedFeatureGateWithMinCompatibility(originalEmuVer, originalMinCompatVer)
	assert.True(t, gate.EmulationVersion().EqualTo(originalEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(originalMinCompatVer))

	newEmuVer := version.MustParse("1.29")
	newMinCompatVer := version.MustParse("1.28")

	// Subtest setting feature gate and calling parallel will leak it out
	t.Run("LeakingSubtest", func(t *gotest.T) {
		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateVersionsDuringTest(fakeT, gate, newEmuVer, newMinCompatVer)
		// Calling t.Parallel in subtest will resume the main test body
		t.Parallel()
		// Leaked from main test
		assert.True(t, gate.EmulationVersion().EqualTo(originalEmuVer))
		assert.True(t, gate.MinCompatibilityVersion().EqualTo(originalMinCompatVer))
	})
	// Leaked from subtest
	assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))
	fakeT := &ignoreFatalT{T: t}
	SetFeatureGateVersionsDuringTest(fakeT, gate, originalEmuVer, originalMinCompatVer)
	assert.True(t, fakeT.fatalRecorded)
}

// TestNoLeakFromSameVersionsToMainTest 验证相同版本对不会触发并发冲突。
func TestNoLeakFromSameVersionsToMainTest(t *gotest.T) {
	t.Cleanup(cleanup)
	originalEmuVer := version.MustParse("1.31")
	originalMinCompatVer := version.MustParse("1.30")
	gate := featuregate.NewVersionedFeatureGateWithMinCompatibility(originalEmuVer, originalMinCompatVer)
	assert.True(t, gate.EmulationVersion().EqualTo(originalEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(originalMinCompatVer))

	newEmuVer := version.MustParse("1.31")
	newMinCompatVer := version.MustParse("1.30")

	// Subtest setting feature gate and calling parallel will leak it out
	t.Run("LeakingSubtest", func(t *gotest.T) {
		SetFeatureGateVersionsDuringTest(t, gate, newEmuVer, newMinCompatVer)
		// Calling t.Parallel in subtest will resume the main test body
		t.Parallel()
		// Leaked from main test
		assert.True(t, gate.EmulationVersion().EqualTo(originalEmuVer))
		assert.True(t, gate.MinCompatibilityVersion().EqualTo(originalMinCompatVer))
	})
	// Leaked from subtest
	assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))
	SetFeatureGateVersionsDuringTest(t, gate, originalEmuVer, originalMinCompatVer)
}

// TestDetectVersionsLeakToOtherSubtest 验证不同版本对在同一层级子测试间
// 互相干扰时能被检测。
func TestDetectVersionsLeakToOtherSubtest(t *gotest.T) {
	t.Cleanup(cleanup)
	originalEmuVer := version.MustParse("1.31")
	originalMinCompatVer := version.MustParse("1.30")
	gate := featuregate.NewVersionedFeatureGateWithMinCompatibility(originalEmuVer, originalMinCompatVer)
	assert.True(t, gate.EmulationVersion().EqualTo(originalEmuVer))
	assert.True(t, gate.MinCompatibilityVersion().EqualTo(originalMinCompatVer))

	subtestName := "Subtest"
	newEmuVer := version.MustParse("1.29")
	newMinCompatVer := version.MustParse("1.28")

	// Subtest setting feature gate and calling parallel will leak it out
	t.Run(subtestName, func(t *gotest.T) {
		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateVersionsDuringTest(fakeT, gate, newEmuVer, newMinCompatVer)
		t.Parallel()
	})
	// Add suffix to name to prevent tests with the same prefix.
	t.Run(subtestName+"Suffix", func(t *gotest.T) {
		// Leaked newEmulationVersion and newMinCompatibilityVersion
		assert.True(t, gate.EmulationVersion().EqualTo(newEmuVer))
		assert.True(t, gate.MinCompatibilityVersion().EqualTo(newMinCompatVer))

		fakeT := &ignoreFatalT{T: t}
		SetFeatureGateVersionsDuringTest(fakeT, gate, originalEmuVer, originalMinCompatVer)
		assert.True(t, fakeT.fatalRecorded)
	})
}

// ignoreFatalT 把 Fatal/Fatalf 降级为记录，使测试可以断言
// “本应触发 Fatal”的路径而不会真的中止进程。
type ignoreFatalT struct {
	*gotest.T
	fatalRecorded bool
}

func (f *ignoreFatalT) Fatal(args ...any) {
	f.T.Helper()
	f.fatalRecorded = true
	newArgs := []any{"[IGNORED Fatal]"}
	newArgs = append(newArgs, args...)
	f.T.Log(newArgs...)
}

func (f *ignoreFatalT) Fatalf(format string, args ...any) {
	f.T.Helper()
	f.fatalRecorded = true
	f.T.Logf("[IGNORED Fatalf] "+format, args...)
}

// ignoreErrorT 在 ignoreFatalT 基础上把 Error/Errorf 也降级为记录。
type ignoreErrorT struct {
	*ignoreFatalT
	errorRecorded bool
}

func (f *ignoreErrorT) Error(args ...any) {
	f.T.Helper()
	f.errorRecorded = true
	newArgs := []any{"[IGNORED Error]"}
	newArgs = append(newArgs, args...)
	f.T.Log(newArgs...)
}

func (f *ignoreErrorT) Errorf(format string, args ...any) {
	f.T.Helper()
	f.errorRecorded = true
	f.T.Logf("[IGNORED Errorf] "+format, args...)
}

// cleanup 重置包级全局状态，保证各测试之间互不影响。
func cleanup() {
	featureFlagOverride = map[featuregate.Feature]string{}
	versionsOverride = ""
	versionsOverrideValue = ""
}
