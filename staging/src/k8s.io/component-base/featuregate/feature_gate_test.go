/*
Copyright 2016 The Kubernetes Authors.

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

package featuregate

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/metrics/legacyregistry"
	featuremetrics "k8s.io/component-base/metrics/prometheus/feature"
	"k8s.io/component-base/metrics/testutil"
)

// TestFeatureGateFlag 验证普通 feature gate 的 flag 解析：
// 默认值生效、AllAlpha/AllBeta 批量开关、单项覆盖批量开关（与先后顺序无关）、
// 已锁定 gate 无法被改动、未知 gate 报错。
func TestFeatureGateFlag(t *testing.T) {
	// gates for testing
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"
	const testDeprecatedGate Feature = "TestDeprecated"
	const testLockedFalseGate Feature = "TestLockedFalse"

	tests := []struct {
		arg        string
		expect     map[Feature]bool
		parseError string
	}{
		{
			arg: "",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testDeprecatedGate:  false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestDeprecated=true",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testDeprecatedGate:  true,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestLockedFalse=true",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
			parseError: "cannot set feature gate TestLockedFalse to true, feature is locked to false",
		},
		{
			arg: "fooBarBaz=true",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
			parseError: "unrecognized feature gate: fooBarBaz",
		},
		{
			arg: "AllAlpha=false",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "AllAlpha=true",
			expect: map[Feature]bool{
				allAlphaGate:        true,
				allBetaGate:         false,
				testAlphaGate:       true,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "AllAlpha=banana",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
			parseError: "invalid value of AllAlpha",
		},
		{
			arg: "AllAlpha=false,TestAlpha=true",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       true,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestAlpha=true,AllAlpha=false",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       true,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "AllAlpha=true,TestAlpha=false",
			expect: map[Feature]bool{
				allAlphaGate:        true,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestAlpha=false,AllAlpha=true",
			expect: map[Feature]bool{
				allAlphaGate:        true,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestBeta=true,AllAlpha=false",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        true,
				testLockedFalseGate: false,
			},
		},

		{
			arg: "AllBeta=false",
			expect: map[Feature]bool{
				allAlphaGate:  false,
				allBetaGate:   false,
				testAlphaGate: false,
				testBetaGate:  false,
			},
		},
		{
			arg: "AllBeta=true",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         true,
				testAlphaGate:       false,
				testBetaGate:        true,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "AllBeta=banana",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
			parseError: "invalid value of AllBeta",
		},
		{
			arg: "AllBeta=false,TestBeta=true",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        true,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestBeta=true,AllBeta=false",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       false,
				testBetaGate:        true,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "AllBeta=true,TestBeta=false",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         true,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestBeta=false,AllBeta=true",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         true,
				testAlphaGate:       false,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
		{
			arg: "TestAlpha=true,AllBeta=false",
			expect: map[Feature]bool{
				allAlphaGate:        false,
				allBetaGate:         false,
				testAlphaGate:       true,
				testBetaGate:        false,
				testLockedFalseGate: false,
			},
		},
	}
	for i, test := range tests {
		t.Run(test.arg, func(t *testing.T) {
			fs := pflag.NewFlagSet("testfeaturegateflag", pflag.ContinueOnError)
			f := NewFeatureGate()
			err := f.Add(map[Feature]FeatureSpec{
				testAlphaGate:       {Default: false, PreRelease: Alpha},
				testBetaGate:        {Default: false, PreRelease: Beta},
				testDeprecatedGate:  {Default: false, PreRelease: Deprecated},
				testLockedFalseGate: {Default: false, PreRelease: GA, LockToDefault: true},
			})
			require.NoError(t, err)
			f.AddFlag(fs)
			err = fs.Parse([]string{fmt.Sprintf("--%s=%s", flagName, test.arg)})
			if test.parseError != "" {
				if !strings.Contains(err.Error(), test.parseError) {
					t.Errorf("%d: Parse() Expected %v, Got %v", i, test.parseError, err)
				}
			} else if err != nil {
				t.Errorf("%d: Parse() Expected nil, Got %v", i, err)
			}
			for k, v := range test.expect {
				if actual := f.enabled.Load().(map[Feature]bool)[k]; actual != v {
					t.Errorf("%d: expected %s=%v, Got %v", i, k, v, actual)
				}
			}
		})
	}
}

// TestFeatureGateOverride 验证 Set 多次调用的覆盖语义：后一次只改显式提到的 gate，
// 未提及的 gate 保持上一次的值。
func TestFeatureGateOverride(t *testing.T) {
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"

	// Don't parse the flag, assert defaults are used.
	var f *featureGate = NewFeatureGate()
	err := f.Add(map[Feature]FeatureSpec{
		testAlphaGate: {Default: false, PreRelease: Alpha},
		testBetaGate:  {Default: false, PreRelease: Beta},
	})
	require.NoError(t, err)

	f.Set("TestAlpha=true,TestBeta=true")
	if errs := f.Validate(); len(errs) > 0 {
		t.Fatalf("Validate() Expected no error, Got %v", errs)
	}
	if f.Enabled(testAlphaGate) != true {
		t.Errorf("Expected true")
	}
	if f.Enabled(testBetaGate) != true {
		t.Errorf("Expected true")
	}

	f.Set("TestAlpha=false")
	if errs := f.Validate(); len(errs) > 0 {
		t.Fatalf("Validate() Expected no error, Got %v", errs)
	}
	if f.Enabled(testAlphaGate) != false {
		t.Errorf("Expected false")
	}
	if f.Enabled(testBetaGate) != true {
		t.Errorf("Expected true")
	}
}

// TestFeatureGateFlagDefaults 验证未解析 flag 时直接使用各 gate 的 Default。
func TestFeatureGateFlagDefaults(t *testing.T) {
	// gates for testing
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"

	// Don't parse the flag, assert defaults are used.
	var f *featureGate = NewFeatureGate()
	err := f.Add(map[Feature]FeatureSpec{
		testAlphaGate: {Default: false, PreRelease: Alpha},
		testBetaGate:  {Default: true, PreRelease: Beta},
	})
	require.NoError(t, err)

	if f.Enabled(testAlphaGate) != false {
		t.Errorf("Expected false")
	}
	if f.Enabled(testBetaGate) != true {
		t.Errorf("Expected true")
	}
}

// TestFeatureGateKnownFeatures 验证 KnownFeatures 只列出 Alpha/Beta gate，
// 已 GA 与 Deprecated 的 gate 不再出现在用户可见列表中。
func TestFeatureGateKnownFeatures(t *testing.T) {
	// gates for testing
	const (
		testAlphaGate      Feature = "TestAlpha"
		testBetaGate       Feature = "TestBeta"
		testGAGate         Feature = "TestGA"
		testDeprecatedGate Feature = "TestDeprecated"
	)

	// Don't parse the flag, assert defaults are used.
	var f *featureGate = NewFeatureGate()
	err := f.Add(map[Feature]FeatureSpec{
		testAlphaGate:      {Default: false, PreRelease: Alpha},
		testBetaGate:       {Default: true, PreRelease: Beta},
		testGAGate:         {Default: true, PreRelease: GA},
		testDeprecatedGate: {Default: false, PreRelease: Deprecated},
	})
	require.NoError(t, err)
	known := strings.Join(f.KnownFeatures(), " ")

	assert.Contains(t, known, testAlphaGate)
	assert.Contains(t, known, testBetaGate)
	assert.NotContains(t, known, testGAGate)
	assert.NotContains(t, known, testDeprecatedGate)
}

// TestFeatureGateSetFromMap 验证 SetFromMap 的 map 形式设置：未知 gate 报错，
// 锁定 gate 设置成非锁定值时拒绝，锁定值本身则允许（无变化）。
func TestFeatureGateSetFromMap(t *testing.T) {
	// gates for testing
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"
	const testLockedTrueGate Feature = "TestLockedTrue"
	const testLockedFalseGate Feature = "TestLockedFalse"

	tests := []struct {
		name        string
		setmap      map[string]bool
		expect      map[Feature]bool
		setmapError string
	}{
		{
			name: "set TestAlpha and TestBeta true",
			setmap: map[string]bool{
				"TestAlpha": true,
				"TestBeta":  true,
			},
			expect: map[Feature]bool{
				testAlphaGate: true,
				testBetaGate:  true,
			},
		},
		{
			name: "set TestBeta true",
			setmap: map[string]bool{
				"TestBeta": true,
			},
			expect: map[Feature]bool{
				testAlphaGate: false,
				testBetaGate:  true,
			},
		},
		{
			name: "set TestAlpha false",
			setmap: map[string]bool{
				"TestAlpha": false,
			},
			expect: map[Feature]bool{
				testAlphaGate: false,
				testBetaGate:  false,
			},
		},
		{
			name: "set TestInvalid true",
			setmap: map[string]bool{
				"TestInvalid": true,
			},
			expect: map[Feature]bool{
				testAlphaGate: false,
				testBetaGate:  false,
			},
			setmapError: "unrecognized feature gate:",
		},
		{
			name: "set locked gates",
			setmap: map[string]bool{
				"TestLockedTrue":  true,
				"TestLockedFalse": false,
			},
			expect: map[Feature]bool{
				testAlphaGate: false,
				testBetaGate:  false,
			},
		},
		{
			name: "set locked gates",
			setmap: map[string]bool{
				"TestLockedTrue": false,
			},
			expect: map[Feature]bool{
				testAlphaGate: false,
				testBetaGate:  false,
			},
			setmapError: "cannot set feature gate TestLockedTrue to false, feature is locked to true",
		},
		{
			name: "set locked gates",
			setmap: map[string]bool{
				"TestLockedFalse": true,
			},
			expect: map[Feature]bool{
				testAlphaGate: false,
				testBetaGate:  false,
			},
			setmapError: "cannot set feature gate TestLockedFalse to true, feature is locked to false",
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("SetFromMap %s", test.name), func(t *testing.T) {
			f := NewFeatureGate()
			err := f.Add(map[Feature]FeatureSpec{
				testAlphaGate:       {Default: false, PreRelease: Alpha},
				testBetaGate:        {Default: false, PreRelease: Beta},
				testLockedTrueGate:  {Default: true, PreRelease: GA, LockToDefault: true},
				testLockedFalseGate: {Default: false, PreRelease: GA, LockToDefault: true},
			})
			require.NoError(t, err)
			err = f.SetFromMap(test.setmap)
			if test.setmapError != "" {
				if err == nil {
					t.Errorf("expected error, got none")
				} else if !strings.Contains(err.Error(), test.setmapError) {
					t.Errorf("%d: SetFromMap(%#v) Expected err:%v, Got err:%v", i, test.setmap, test.setmapError, err)
				}
			} else if err != nil {
				t.Errorf("%d: SetFromMap(%#v) Expected success, Got err:%v", i, test.setmap, err)
			}
			for k, v := range test.expect {
				if actual := f.Enabled(k); actual != v {
					t.Errorf("%d: SetFromMap(%#v) Expected %s=%v, Got %s=%v", i, test.setmap, k, v, k, actual)
				}
			}
		})
	}
}

// TestFeatureGateMetrics 验证 AddMetrics 把每个 gate 的阶段与启用状态导出为
// kubernetes_feature_enabled gauge；先重置指标以免受其他测试注册影响。
func TestFeatureGateMetrics(t *testing.T) {
	// gates for testing
	featuremetrics.ResetFeatureInfoMetric()
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"
	const testAlphaEnabled Feature = "TestAlphaEnabled"
	const testBetaDisabled Feature = "TestBetaDisabled"
	testedMetrics := []string{"kubernetes_feature_enabled"}
	expectedOutput := `
		# HELP kubernetes_feature_enabled [BETA] This metric records the data about the stage and enablement of a k8s feature.
        # TYPE kubernetes_feature_enabled gauge
        kubernetes_feature_enabled{name="TestAlpha",stage="ALPHA"} 0
        kubernetes_feature_enabled{name="TestBeta",stage="BETA"} 1
		kubernetes_feature_enabled{name="TestAlphaEnabled",stage="ALPHA"} 1
        kubernetes_feature_enabled{name="AllAlpha",stage="ALPHA"} 0
        kubernetes_feature_enabled{name="AllBeta",stage="BETA"} 0
		kubernetes_feature_enabled{name="TestBetaDisabled",stage="ALPHA"} 0
`

	f := NewFeatureGate()
	fMap := map[Feature]FeatureSpec{
		testAlphaGate:    {Default: false, PreRelease: Alpha},
		testAlphaEnabled: {Default: false, PreRelease: Alpha},
		testBetaGate:     {Default: true, PreRelease: Beta},
		testBetaDisabled: {Default: true, PreRelease: Alpha},
	}
	require.NoError(t, f.Add(fMap))
	require.NoError(t, f.SetFromMap(map[string]bool{"TestAlphaEnabled": true, "TestBetaDisabled": false}))
	f.AddMetrics()
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedOutput), testedMetrics...); err != nil {
		t.Fatal(err)
	}
}

// TestFeatureGateString 验证 String() 输出排序后的 "gate=value" 列表，
// 可直接作为 --feature-gates 的值复用。
func TestFeatureGateString(t *testing.T) {
	// gates for testing
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"
	const testGAGate Feature = "TestGA"

	featuremap := map[Feature]FeatureSpec{
		testGAGate:    {Default: true, PreRelease: GA},
		testAlphaGate: {Default: false, PreRelease: Alpha},
		testBetaGate:  {Default: true, PreRelease: Beta},
	}

	tests := []struct {
		setmap map[string]bool
		expect string
	}{
		{
			setmap: map[string]bool{
				"TestAlpha": false,
			},
			expect: "TestAlpha=false",
		},
		{
			setmap: map[string]bool{
				"TestAlpha": false,
				"TestBeta":  true,
			},
			expect: "TestAlpha=false,TestBeta=true",
		},
		{
			setmap: map[string]bool{
				"TestGA":    true,
				"TestAlpha": false,
				"TestBeta":  true,
			},
			expect: "TestAlpha=false,TestBeta=true,TestGA=true",
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("SetFromMap %s", test.expect), func(t *testing.T) {
			f := NewFeatureGate()
			require.NoError(t, f.Add(featuremap))
			require.NoError(t, f.SetFromMap(test.setmap))
			result := f.String()
			if result != test.expect {
				t.Errorf("%d: SetFromMap(%#v) Expected %s, Got %s", i, test.setmap, test.expect, result)
			}
		})
	}
}

// TestFeatureGateOverrideDefault 覆盖 OverrideDefault 的语义：
// 覆盖影响有效默认值、可跨 DeepCopy 保留、反映在 KnownFeatures 中、
// 不能改锁定默认值、不覆盖显式设置、阻止重复注册，且在加入 flag set 后拒绝修改。
func TestFeatureGateOverrideDefault(t *testing.T) {
	t.Run("overrides take effect", func(t *testing.T) {
		f := NewFeatureGate()
		if err := f.Add(map[Feature]FeatureSpec{
			"TestFeature1": {Default: true},
			"TestFeature2": {Default: false},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature1", false))
		require.NoError(t, f.OverrideDefault("TestFeature2", true))
		if f.Enabled("TestFeature1") {
			t.Error("expected TestFeature1 to have effective default of false")
		}
		if !f.Enabled("TestFeature2") {
			t.Error("expected TestFeature2 to have effective default of true")
		}
	})

	t.Run("overrides are preserved across deep copies", func(t *testing.T) {
		f := NewFeatureGate()
		require.NoError(t, f.Add(map[Feature]FeatureSpec{"TestFeature": {Default: false}}))
		require.NoError(t, f.OverrideDefault("TestFeature", true))
		fcopy := f.DeepCopy()
		if !fcopy.Enabled("TestFeature") {
			t.Error("default override was not preserved by deep copy")
		}
	})

	t.Run("overrides are preserved across CopyKnownFeatures", func(t *testing.T) {
		f := NewFeatureGate()
		require.NoError(t, f.Add(map[Feature]FeatureSpec{"TestFeature": {Default: false}}))
		require.NoError(t, f.OverrideDefault("TestFeature", true))
		fcopy := f.DeepCopyAndReset()
		if !f.Enabled("TestFeature") {
			t.Error("TestFeature should be enabled by override")
		}
		if !fcopy.Enabled("TestFeature") {
			t.Error("default override was not preserved by CopyKnownFeatures")
		}
	})

	t.Run("overrides are not passed over after CopyKnownFeatures", func(t *testing.T) {
		f := NewFeatureGate()
		require.NoError(t, f.Add(map[Feature]FeatureSpec{"TestFeature": {Default: false}}))
		fcopy := f.DeepCopyAndReset()
		require.NoError(t, f.OverrideDefault("TestFeature", true))
		if !f.Enabled("TestFeature") {
			t.Error("TestFeature should be enabled by override")
		}
		if fcopy.Enabled("TestFeature") {
			t.Error("default override should not be passed over after CopyKnownFeatures")
		}
	})

	t.Run("reflected in known features", func(t *testing.T) {
		f := NewFeatureGate()
		if err := f.Add(map[Feature]FeatureSpec{"TestFeature": {
			Default:    false,
			PreRelease: Alpha,
		}}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature", true))
		var found bool
		for _, s := range f.KnownFeatures() {
			if !strings.Contains(s, "TestFeature") {
				continue
			}
			found = true
			if !strings.Contains(s, "default=true") {
				t.Errorf("expected override of default to be reflected in known feature description %q", s)
			}
		}
		if !found {
			t.Error("found no entry for TestFeature in known features")
		}
	})

	t.Run("may not change default for specs with locked defaults", func(t *testing.T) {
		f := NewFeatureGate()
		if err := f.Add(map[Feature]FeatureSpec{
			"LockedFeature": {
				Default:       true,
				LockToDefault: true,
			},
		}); err != nil {
			t.Fatal(err)
		}
		if f.OverrideDefault("LockedFeature", false) == nil {
			t.Error("expected error when attempting to override the default for a feature with a locked default")
		}
		if f.OverrideDefault("LockedFeature", true) == nil {
			t.Error("expected error when attempting to override the default for a feature with a locked default")
		}
	})

	t.Run("does not supersede explicitly-set value", func(t *testing.T) {
		f := NewFeatureGate()
		require.NoError(t, f.Add(map[Feature]FeatureSpec{"TestFeature": {Default: true}}))
		require.NoError(t, f.OverrideDefault("TestFeature", false))
		require.NoError(t, f.SetFromMap(map[string]bool{"TestFeature": true}))
		if !f.Enabled("TestFeature") {
			t.Error("expected feature to be effectively enabled despite default override")
		}
	})

	t.Run("prevents re-registration of feature spec after overriding default", func(t *testing.T) {
		f := NewFeatureGate()
		if err := f.Add(map[Feature]FeatureSpec{
			"TestFeature": {
				Default:    true,
				PreRelease: Alpha,
			},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature", false))
		if err := f.Add(map[Feature]FeatureSpec{
			"TestFeature": {
				Default:    true,
				PreRelease: Alpha,
			},
		}); err == nil {
			t.Error("expected re-registration to return a non-nil error after overriding its default")
		}
	})

	t.Run("does not allow override for an unknown feature", func(t *testing.T) {
		f := NewFeatureGate()
		if err := f.OverrideDefault("TestFeature", true); err == nil {
			t.Error("expected an error to be returned in attempt to override default for unregistered feature")
		}
	})

	t.Run("returns error if already added to flag set", func(t *testing.T) {
		f := NewFeatureGate()
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		f.AddFlag(fs)

		if err := f.OverrideDefault("TestFeature", true); err == nil {
			t.Error("expected a non-nil error to be returned")
		}
	})
}

// TestVersionedFeatureGateFlag 验证带版本的 feature gate 在模拟版本 1.28、
// 二进制版本 1.29 下的 flag 解析：每个 gate 的默认值/阶段按模拟版本选取，
// 模拟版本中仍是 PreAlpha 的 gate 不能被设置，锁定 gate 仍不可改。
func TestVersionedFeatureGateFlag(t *testing.T) {
	// gates for testing
	const testGAGate Feature = "TestGA"
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"
	const testLockedFalseGate Feature = "TestLockedFalse"
	const testAlphaGateNoVersion Feature = "TestAlphaNoVersion"
	const testBetaGateNoVersion Feature = "TestBetaNoVersion"
	const testCompatibilityGate Feature = "TestCompatibility"

	tests := []struct {
		arg        string
		expect     map[Feature]bool
		parseError string
	}{
		{
			arg: "",
			expect: map[Feature]bool{
				testGAGate:             false,
				allAlphaGate:           false,
				allBetaGate:            false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
		},
		{
			arg: "TestLockedFalse=true",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    true,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
		},
		{
			arg:        "fooBarBaz=true",
			parseError: "unrecognized feature gate: fooBarBaz",
		},
		{
			arg: "AllAlpha=false",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
		},
		{
			arg: "AllAlpha=true",
			expect: map[Feature]bool{
				allAlphaGate:           true,
				allBetaGate:            false,
				testAlphaGate:          false,
				testGAGate:             false,
				testBetaGate:           true,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: true,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
		},
		{
			arg: "AllAlpha=banana",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
			parseError: "invalid value of AllAlpha",
		},
		{
			arg:        "AllAlpha=false,TestAlpha=true,TestAlphaNoVersion=true",
			parseError: "cannot set feature gate TestAlpha to true, feature is PreAlpha at emulated version 1.28",
		},
		{
			arg: "AllAlpha=false,TestAlphaNoVersion=true",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: true,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
		},
		{
			arg:        "TestAlpha=true,TestAlphaNoVersion=true,AllAlpha=false",
			parseError: "cannot set feature gate TestAlpha to true, feature is PreAlpha at emulated version 1.28",
		},
		{
			arg:        "AllAlpha=true,TestAlpha=false,TestAlphaNoVersion=false",
			parseError: "cannot set feature gate TestAlpha to false, feature is PreAlpha at emulated version 1.28",
		},
		{
			arg: "AllAlpha=true,TestAlphaNoVersion=false",
			expect: map[Feature]bool{
				allAlphaGate:           true,
				allBetaGate:            false,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           true,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
		},
		{
			arg:        "TestAlpha=false,TestAlphaNoVersion=false,AllAlpha=true",
			parseError: "cannot set feature gate TestAlpha to false, feature is PreAlpha at emulated version 1.28",
		},
		{
			arg: "TestBeta=true,TestBetaNoVersion=true,TestGA=true,AllAlpha=false",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             true,
				testAlphaGate:          false,
				testBetaGate:           true,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  true,
				testCompatibilityGate:  false,
			},
		},

		{
			arg: "AllBeta=false",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  false,
			},
		},
		{
			arg: "AllBeta=true",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            true,
				testGAGate:             true,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  true,
				testCompatibilityGate:  true,
			},
		},
		{
			arg:        "AllBeta=banana",
			parseError: "invalid value of AllBeta",
		},
		{
			arg: "AllBeta=false,TestBeta=true,TestBetaNoVersion=true,TestGA=true",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             true,
				testAlphaGate:          false,
				testBetaGate:           true,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  true,
				testCompatibilityGate:  false,
			},
		},
		{
			arg: "TestBeta=true,TestBetaNoVersion=true,AllBeta=false",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           true,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  true,
				testCompatibilityGate:  false,
			},
		},
		{
			arg: "AllBeta=true,TestBetaNoVersion=false,TestBeta=false,TestGA=false",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            true,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  true,
			},
		},
		{
			arg: "TestBeta=false,TestBetaNoVersion=false,AllBeta=true",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            true,
				testGAGate:             true,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  true,
			},
		},
		{
			arg:        "TestAlpha=true,AllBeta=false",
			parseError: "cannot set feature gate TestAlpha to true, feature is PreAlpha at emulated version 1.28",
		},
		{
			arg: "TestCompatibility=true",
			expect: map[Feature]bool{
				allAlphaGate:           false,
				allBetaGate:            false,
				testGAGate:             false,
				testAlphaGate:          false,
				testBetaGate:           false,
				testLockedFalseGate:    false,
				testAlphaGateNoVersion: false,
				testBetaGateNoVersion:  false,
				testCompatibilityGate:  true,
			},
		},
	}
	for i, test := range tests {
		t.Run(test.arg, func(t *testing.T) {
			fs := pflag.NewFlagSet("testfeaturegateflag", pflag.ContinueOnError)
			f := NewVersionedFeatureGate(version.MustParse("1.29"))
			if err := f.SetEmulationVersion(version.MustParse("1.28")); err != nil {
				t.Fatalf("failed to SetEmulationVersion: %v", err)
			}
			err := f.AddVersioned(map[Feature]VersionedSpecs{
				testGAGate: {
					{Version: version.MustParse("1.27"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: GA},
				},
				testAlphaGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
				},
				testBetaGate: {
					{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
				},
				testLockedFalseGate: {
					{Version: version.MustParse("1.28"), Default: false, PreRelease: GA},
					{Version: version.MustParse("1.29"), Default: false, PreRelease: GA, LockToDefault: true},
				},
				testCompatibilityGate: {
					{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.28"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.28")},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: GA, LockToDefault: true},
				},
			})
			require.NoError(t, err)
			err = f.Add(map[Feature]FeatureSpec{
				testAlphaGateNoVersion: {Default: false, PreRelease: Alpha},
				testBetaGateNoVersion:  {Default: false, PreRelease: Beta},
			})
			require.NoError(t, err)
			f.AddFlag(fs)

			var errs []error
			err = fs.Parse([]string{fmt.Sprintf("--%s=%s", flagName, test.arg)})
			if err != nil {
				errs = append(errs, err)
			}
			err = utilerrors.NewAggregate(errs)
			if test.parseError != "" {
				if !strings.Contains(err.Error(), test.parseError) {
					t.Errorf("%d: Parse() Expected %v, Got %v", i, test.parseError, err)
				}
				return
			} else if err != nil {
				t.Errorf("%d: Parse() Expected nil, Got %v", i, err)
			}
			for k, v := range test.expect {
				if actual := f.enabled.Load().(map[Feature]bool)[k]; actual != v {
					t.Errorf("%d: expected %s=%v, Got %v", i, k, v, actual)
				}
			}
		})
	}
}

// TestVersionedFeatureGateOverride 验证带版本 gate 的 Set 覆盖行为，
// 并确认模拟版本不能回退到低于已注册规格的版本。
func TestVersionedFeatureGateOverride(t *testing.T) {
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"

	// Don't parse the flag, assert defaults are used.
	f := NewVersionedFeatureGate(version.MustParse("1.29"))

	err := f.AddVersioned(map[Feature]VersionedSpecs{
		testAlphaGate: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
		},
		testBetaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
		},
	})
	require.NoError(t, err)
	if f.Enabled(testAlphaGate) != false {
		t.Errorf("Expected false")
	}
	if f.Enabled(testBetaGate) != false {
		t.Errorf("Expected false")
	}
	if errs := f.Validate(); len(errs) > 0 {
		t.Errorf("Expected no errors when emulation version is equal to binary version.")
	}

	require.NoError(t, f.Set("TestAlpha=true,TestBeta=true"))
	if f.Enabled(testAlphaGate) != true {
		t.Errorf("Expected false")
	}
	if f.Enabled(testBetaGate) != true {
		t.Errorf("Expected true")
	}

	require.NoError(t, f.Set("TestAlpha=false"))
	if f.Enabled(testAlphaGate) != false {
		t.Errorf("Expected false")
	}
	if f.Enabled(testBetaGate) != true {
		t.Errorf("Expected true")
	}
	if errs := f.Validate(); len(errs) > 0 {
		t.Errorf("Expected no errors when emulation version is equal to binary version.")
	}

	if err := f.SetEmulationVersion(version.MustParse("1.28")); err == nil {
		t.Errorf("Expected errors when emulation version is 1.28.")
	}
}

// TestVersionedFeatureGateFlagDefaults 验证模拟版本为 1.28 时各带版本 gate
// 选取到的是 1.28 对应的规格（包括还未进入 Beta 而显示为 PreAlpha/0.0 的情况），
// 并检查 GetAll 的返回集合。
func TestVersionedFeatureGateFlagDefaults(t *testing.T) {
	// gates for testing
	const testGAGate Feature = "TestGA"
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"

	// Don't parse the flag, assert defaults are used.
	f := NewVersionedFeatureGate(version.MustParse("1.29"))
	require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))

	err := f.AddVersioned(map[Feature]VersionedSpecs{
		testGAGate: {
			{Version: version.MustParse("1.25"), Default: true, PreRelease: Alpha},
			{Version: version.MustParse("1.27"), Default: true, PreRelease: Beta},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: GA},
		},
		testAlphaGate: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
		},
		testBetaGate: {
			{Version: version.MustParse("1.26"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
		},
	})
	require.NoError(t, err)

	if f.Enabled(testAlphaGate) != false {
		t.Errorf("Expected false")
	}
	if fs, _ := f.featureSpec(testAlphaGate); fs.PreRelease != PreAlpha || fs.Version.String() != "0.0" {
		t.Errorf("Expected (PreAlpha, 0.0)")
	}
	if f.Enabled(testBetaGate) != false {
		t.Errorf("Expected false")
	}
	if fs, _ := f.featureSpec(testBetaGate); fs.PreRelease != Beta || fs.Version.String() != "1.28" {
		t.Errorf("Expected (Beta, 1.28)")
	}
	if f.Enabled(testGAGate) != true {
		t.Errorf("Expected true")
	}
	if fs, _ := f.featureSpec(testGAGate); fs.PreRelease != Beta || fs.Version.String() != "1.27" {
		t.Errorf("Expected (Beta, 1.27)")
	}
	if _, err := f.featureSpec("NonExist"); err == nil {
		t.Errorf("Expected Error")
	}
	allFeatures := f.GetAll()
	expectedAllFeatures := []Feature{testGAGate, testBetaGate, allAlphaGate, allBetaGate}
	if len(allFeatures) != 4 {
		t.Errorf("Expected 4 features from GetAll(), got %d", len(allFeatures))
	}
	for _, feature := range expectedAllFeatures {
		if _, ok := allFeatures[feature]; !ok {
			t.Errorf("Expected feature %s to be in GetAll()", feature)
		}
	}
}

// TestVersionedFeatureGateKnownFeatures 验证 KnownFeatures 在带版本 gate 下的过滤规则：
// 未来版本的 PreAlpha 与已 GA/Deprecated 的 gate 不列出；带最小兼容版本要求的 gate
// 会附加 "--min-compatibility-version>=x.y" 提示。
func TestVersionedFeatureGateKnownFeatures(t *testing.T) {
	// gates for testing
	const (
		testPreAlphaGate            Feature = "TestPreAlpha"
		testAlphaGate               Feature = "TestAlpha"
		testBetaGate                Feature = "TestBeta"
		testFastBetaGate            Feature = "TestFastBeta"
		testLatestFastBetaGate      Feature = "TestLatestFastBeta"
		testGAGate                  Feature = "TestGA"
		testDeprecatedGate          Feature = "TestDeprecated"
		testGAGateNoVersion         Feature = "TestGANoVersion"
		testAlphaGateNoVersion      Feature = "TestAlphaNoVersion"
		testBetaGateNoVersion       Feature = "TestBetaNoVersion"
		testDeprecatedGateNoVersion Feature = "TestDeprecatedNoVersion"
	)

	// Don't parse the flag, assert defaults are used.
	f := NewVersionedFeatureGate(version.MustParse("1.29"))
	require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
	err := f.AddVersioned(map[Feature]VersionedSpecs{
		testGAGate: {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: GA},
		},
		testPreAlphaGate: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
		},
		testAlphaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
		},
		testBetaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
		},
		testFastBetaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.28")},
		},
		testLatestFastBetaGate: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
		},
		testDeprecatedGate: {
			{Version: version.MustParse("1.26"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: Deprecated},
		},
	})
	require.NoError(t, err)
	err = f.Add(map[Feature]FeatureSpec{
		testAlphaGateNoVersion:      {Default: false, PreRelease: Alpha},
		testBetaGateNoVersion:       {Default: false, PreRelease: Beta},
		testGAGateNoVersion:         {Default: false, PreRelease: GA},
		testDeprecatedGateNoVersion: {Default: false, PreRelease: Deprecated},
	})
	require.NoError(t, err)

	known := strings.Join(f.KnownFeatures(), " ")

	assert.NotContains(t, known, testPreAlphaGate)
	assert.Contains(t, known, testAlphaGate)
	assert.Contains(t, known, testBetaGate)
	assert.Contains(t, known, "TestFastBeta=true|false (BETA - default=true) if --min-compatibility-version>=1.28")
	assert.NotContains(t, known, testLatestFastBetaGate)
	assert.NotContains(t, known, testGAGate)
	assert.NotContains(t, known, testDeprecatedGate)
	assert.Contains(t, known, testAlphaGateNoVersion)
	assert.Contains(t, known, testBetaGateNoVersion)
	assert.NotContains(t, known, testGAGateNoVersion)
	assert.NotContains(t, known, testDeprecatedGateNoVersion)
}

// TestVersionedFeatureGateKnownFeaturesWithMinCompatVersion 验证：
// 同一个 gate 在不同最小兼容版本下会展示不同的默认值/阶段描述，
// 从而让用户清楚模拟不同版本时 gate 的可用性差异。
func TestVersionedFeatureGateKnownFeaturesWithMinCompatVersion(t *testing.T) {
	// gates for testing
	const (
		testPreAlphaGate            Feature = "TestPreAlpha"
		testAlphaGate               Feature = "TestAlpha"
		testBetaGate                Feature = "TestBeta"
		testFastBetaGate            Feature = "TestFastBeta"
		testLatestFastBetaGate      Feature = "TestLatestFastBeta"
		testGAGate                  Feature = "TestGA"
		testDeprecatedGate          Feature = "TestDeprecated"
		testGAGateNoVersion         Feature = "TestGANoVersion"
		testAlphaGateNoVersion      Feature = "TestAlphaNoVersion"
		testBetaGateNoVersion       Feature = "TestBetaNoVersion"
		testDeprecatedGateNoVersion Feature = "TestDeprecatedNoVersion"
	)
	// Don't parse the flag, assert defaults are used.
	f := NewVersionedFeatureGate(version.MustParse("1.29"))
	err := f.AddVersioned(map[Feature]VersionedSpecs{
		testGAGate: {
			{Version: version.MustParse("1.26"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.26"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.26")},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: GA},
		},
		testPreAlphaGate: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
		},
		testAlphaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
		},
		testBetaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
		},
		testFastBetaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.28")},
		},
		testLatestFastBetaGate: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
		},
		testDeprecatedGate: {
			{Version: version.MustParse("1.26"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: Deprecated},
		},
	})
	require.NoError(t, err)
	err = f.Add(map[Feature]FeatureSpec{
		testAlphaGateNoVersion:      {Default: false, PreRelease: Alpha},
		testBetaGateNoVersion:       {Default: false, PreRelease: Beta},
		testGAGateNoVersion:         {Default: false, PreRelease: GA},
		testDeprecatedGateNoVersion: {Default: false, PreRelease: Deprecated},
	})
	require.NoError(t, err)

	knownExpected := []string{
		"AllAlpha=true|false (ALPHA - default=false)",
		"AllBeta=true|false (BETA - default=false)",
		"TestAlpha=true|false (ALPHA - default=false)",
		"TestAlphaNoVersion=true|false (ALPHA - default=false)",
		"TestBeta=true|false (BETA - default=false)",
		"TestBetaNoVersion=true|false (BETA - default=false)",
		"TestFastBeta=true|false (BETA - default=true)",
		"TestLatestFastBeta=true|false (BETA - default=false), or TestLatestFastBeta=true|false (BETA - default=true) if --min-compatibility-version>=1.29",
		"TestPreAlpha=true|false (ALPHA - default=false)",
	}
	assert.ElementsMatch(t, f.KnownFeatures(), knownExpected)

	require.NoError(t, f.SetEmulationVersionAndMinCompatibilityVersion(version.MustParse("1.28"), version.MustParse("1.28")))
	knownExpected = []string{
		"AllAlpha=true|false (ALPHA - default=false)",
		"AllBeta=true|false (BETA - default=false)",
		"TestAlpha=true|false (ALPHA - default=false)",
		"TestAlphaNoVersion=true|false (ALPHA - default=false)",
		"TestBeta=true|false (BETA - default=false)",
		"TestBetaNoVersion=true|false (BETA - default=false)",
		"TestFastBeta=true|false (BETA - default=true)",
	}
	assert.ElementsMatch(t, f.KnownFeatures(), knownExpected)

	require.NoError(t, f.SetEmulationVersionAndMinCompatibilityVersion(version.MustParse("1.28"), version.MustParse("1.26")))
	knownExpected = []string{
		"AllAlpha=true|false (ALPHA - default=false)",
		"AllBeta=true|false (BETA - default=false)",
		"TestAlpha=true|false (ALPHA - default=false)",
		"TestAlphaNoVersion=true|false (ALPHA - default=false)",
		"TestBeta=true|false (BETA - default=false)",
		"TestBetaNoVersion=true|false (BETA - default=false)",
		"TestFastBeta=true|false (BETA - default=false), or TestFastBeta=true|false (BETA - default=true) if --min-compatibility-version>=1.28",
	}
	assert.ElementsMatch(t, f.KnownFeatures(), knownExpected)
}

// TestVersionedFeatureGateMetrics 验证带版本 gate 导出的指标阶段取自模拟版本对应的
// 规格（例如 1.28 下仍为 ALPHA 的 gate），而不是二进制版本对应的阶段。
func TestVersionedFeatureGateMetrics(t *testing.T) {
	// gates for testing
	featuremetrics.ResetFeatureInfoMetric()
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"
	const testAlphaEnabled Feature = "TestAlphaEnabled"
	const testBetaDisabled Feature = "TestBetaDisabled"
	testedMetrics := []string{"kubernetes_feature_enabled"}
	expectedOutput := `
		# HELP kubernetes_feature_enabled [BETA] This metric records the data about the stage and enablement of a k8s feature.
        # TYPE kubernetes_feature_enabled gauge
        kubernetes_feature_enabled{name="TestAlpha",stage="ALPHA"} 0
        kubernetes_feature_enabled{name="TestBeta",stage="BETA"} 1
		kubernetes_feature_enabled{name="TestAlphaEnabled",stage="ALPHA"} 1
        kubernetes_feature_enabled{name="AllAlpha",stage="ALPHA"} 0
        kubernetes_feature_enabled{name="AllBeta",stage="BETA"} 0
		kubernetes_feature_enabled{name="TestBetaDisabled",stage="BETA"} 0
`

	f := NewVersionedFeatureGate(version.MustParse("1.29"))
	require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
	err := f.AddVersioned(map[Feature]VersionedSpecs{
		testAlphaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
		},
		testAlphaEnabled: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
		},
		testBetaGate: {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: Beta},
		},
		testBetaDisabled: {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: Beta},
		},
	})
	require.NoError(t, err)

	require.NoError(t, f.SetFromMap(map[string]bool{"TestAlphaEnabled": true, "TestBetaDisabled": false}))
	f.AddMetrics()
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedOutput), testedMetrics...); err != nil {
		t.Fatal(err)
	}
}

// TestVersionedFeatureGateOverrideDefault 覆盖带版本 gate 的默认值覆盖语义：
// 覆盖随模拟版本变化、可按版本覆盖、跨 DeepCopy 隔离、不能越过锁定默认值，
// 也不能覆盖尚未在模拟版本生效的 gate。
func TestVersionedFeatureGateOverrideDefault(t *testing.T) {
	t.Run("overrides take effect", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature1": {
				{Version: version.MustParse("1.28"), Default: true},
			},
			"TestFeature2": {
				{Version: version.MustParse("1.26"), Default: false},
				{Version: version.MustParse("1.29"), Default: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature1", false))
		require.NoError(t, f.OverrideDefault("TestFeature2", true))
		if f.Enabled("TestFeature1") {
			t.Error("expected TestFeature1 to have effective default of false")
		}
		if !f.Enabled("TestFeature2") {
			t.Error("expected TestFeature2 to have effective default of true")
		}
	})

	t.Run("overrides at specific version take effect", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature1": {
				{Version: version.MustParse("1.28"), Default: true, PreRelease: Beta},
			},
			"TestFeature2": {
				{Version: version.MustParse("1.26"), Default: false, PreRelease: Alpha},
				{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
			},
		}); err != nil {
			t.Fatal(err)
		}
		if f.OverrideDefaultAtVersion("TestFeature1", false, version.MustParse("1.27")) == nil {
			t.Error("expected error when attempting to override the default for a feature not available at given version")
		}
		require.NoError(t, f.OverrideDefaultAtVersion("TestFeature2", true, version.MustParse("1.27")))
		if !f.Enabled("TestFeature1") {
			t.Error("expected TestFeature1 to have effective default of true")
		}
		if !f.Enabled("TestFeature2") {
			t.Error("expected TestFeature2 to have effective default of true")
		}
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.29")))
		if !f.Enabled("TestFeature1") {
			t.Error("expected TestFeature1 to have effective default of true")
		}
		if f.Enabled("TestFeature2") {
			t.Error("expected TestFeature2 to have effective default of false")
		}
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.26")))
		if f.Enabled("TestFeature1") {
			t.Error("expected TestFeature1 to have effective default of false")
		}
		if !f.Enabled("TestFeature2") {
			t.Error("expected TestFeature2 to have effective default of true")
		}
	})

	t.Run("overrides are preserved across deep copies", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature": {
				{Version: version.MustParse("1.28"), Default: false},
				{Version: version.MustParse("1.29"), Default: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature", true))
		fcopy := f.DeepCopy()
		if !fcopy.Enabled("TestFeature") {
			t.Error("default override was not preserved by deep copy")
		}
	})

	t.Run("overrides are not passed over after deep copies", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature": {
				{Version: version.MustParse("1.28"), Default: false},
				{Version: version.MustParse("1.29"), Default: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
		assert.False(t, f.Enabled("TestFeature"))

		fcopy := f.DeepCopy()
		require.NoError(t, f.OverrideDefault("TestFeature", true))
		require.NoError(t, f.OverrideDefaultAtVersion("TestFeature", false, version.MustParse("1.29")))
		assert.True(t, f.Enabled("TestFeature"))
		assert.False(t, fcopy.Enabled("TestFeature"))

		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.29")))
		assert.False(t, f.Enabled("TestFeature"))
		assert.False(t, fcopy.Enabled("TestFeature"))

		require.NoError(t, fcopy.SetEmulationVersion(version.MustParse("1.29")))
		assert.False(t, f.Enabled("TestFeature"))
		assert.True(t, fcopy.Enabled("TestFeature"))
	})

	t.Run("reflected in known features", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature": {
				{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
				{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
			},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature", true))
		var found bool
		for _, s := range f.KnownFeatures() {
			if !strings.Contains(s, "TestFeature") {
				continue
			}
			found = true
			if !strings.Contains(s, "default=true") {
				t.Errorf("expected override of default to be reflected in known feature description %q", s)
			}
		}
		if !found {
			t.Error("found no entry for TestFeature in known features")
		}
	})

	t.Run("may not change default for specs with locked defaults", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"LockedFeature": {
				{Version: version.MustParse("1.28"), Default: true, LockToDefault: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
		if f.OverrideDefault("LockedFeature", false) == nil {
			t.Error("expected error when attempting to override the default for a feature with a locked default")
		}
		if f.OverrideDefault("LockedFeature", true) == nil {
			t.Error("expected error when attempting to override the default for a feature with a locked default")
		}
	})

	t.Run("can change default for specs without locked defaults for emulation version", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"LockedFeature": {
				{Version: version.MustParse("1.28"), Default: true},
				{Version: version.MustParse("1.29"), Default: true, LockToDefault: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("LockedFeature", false))
		if f.Enabled("LockedFeature") {
			t.Error("expected LockedFeature to have effective default of false")
		}
	})

	t.Run("does not supersede explicitly-set value", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature": {
				{Version: version.MustParse("1.28"), Default: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature", false))
		require.NoError(t, f.SetFromMap(map[string]bool{"TestFeature": true}))
		if !f.Enabled("TestFeature") {
			t.Error("expected feature to be effectively enabled despite default override")
		}
	})

	t.Run("prevents re-registration of feature spec after overriding default", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature": {
				{Version: version.MustParse("1.28"), Default: true, PreRelease: Alpha},
			},
		}); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, f.OverrideDefault("TestFeature", false))
		if err := f.Add(map[Feature]FeatureSpec{
			"TestFeature": {
				Default:    true,
				PreRelease: Alpha,
			},
		}); err == nil {
			t.Error("expected re-registration to return a non-nil error after overriding its default")
		}
	})

	t.Run("does not allow override for a feature added after emulation version", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.AddVersioned(map[Feature]VersionedSpecs{
			"TestFeature": {
				{Version: version.MustParse("1.29"), Default: false},
			},
		}); err != nil {
			t.Fatal(err)
		}
		if err := f.OverrideDefault("TestFeature", true); err == nil {
			t.Error("expected an error to be returned in attempt to override default for a feature added after emulation version")
		}
	})

	t.Run("does not allow override for an unknown feature", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		if err := f.OverrideDefault("TestFeature", true); err == nil {
			t.Error("expected an error to be returned in attempt to override default for unregistered feature")
		}
	})

	t.Run("returns error if already added to flag set", func(t *testing.T) {
		f := NewVersionedFeatureGate(version.MustParse("1.29"))
		require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		f.AddFlag(fs)

		if err := f.OverrideDefault("TestFeature", true); err == nil {
			t.Error("expected a non-nil error to be returned")
		}
	})
}

// TestFeatureGateMinCompatibilityVersion 验证最小兼容版本决定 gate 的默认值与
// 阶段：未达到 gate 自身最小兼容版本时 gate 保持禁用；达到后按规格默认启用，
// 同时 KnownFeatures 不再显示兼容性提示。
func TestFeatureGateMinCompatibilityVersion(t *testing.T) {
	const testCompatGateA Feature = "TestCompatA"
	const testCompatGateB Feature = "TestCompatB"
	const testCompatGateC Feature = "TestCompatC"

	f := NewVersionedFeatureGateWithMinCompatibility(version.MustParse("1.30"), version.MustParse("1.27"))
	err := f.AddVersioned(map[Feature]VersionedSpecs{
		testCompatGateA: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.30"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.30")},
		},
		testCompatGateB: {
			{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.30"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.30")},
		},
		testCompatGateC: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
			{Version: version.MustParse("1.28"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.28")},
			{Version: version.MustParse("1.30"), Default: true, PreRelease: GA, LockToDefault: true},
		},
	})
	require.NoError(t, err)

	// KnownFeatures should show min-compatibility-version requirement.
	knownExpected := []string{
		"AllAlpha=true|false (ALPHA - default=false)",
		"AllBeta=true|false (BETA - default=false)",
		"TestCompatA=true|false (BETA - default=false), or TestCompatA=true|false (BETA - default=true) if --min-compatibility-version>=1.30",
		"TestCompatB=true|false (BETA - default=false), or TestCompatB=true|false (BETA - default=true) if --min-compatibility-version>=1.30",
		"TestCompatC=true|false (BETA - default=false)",
	}
	assert.ElementsMatch(t, f.KnownFeatures(), knownExpected)

	// Gate is Alpha min compat version is 1.29, feature's is 1.30. Feature should be disabled.
	assert.False(t, f.Enabled(testCompatGateA), "TestCompatA should be disabled at Alpha stage")
	assert.False(t, f.Enabled(testCompatGateB), "TestCompatB should be disabled when its min compat version is too high")
	assert.False(t, f.Enabled(testCompatGateC), "TestCompatC should be disabled when its min compat version is too high")
	// Trying to override TestCompatA default should work.
	require.NoError(t, f.OverrideDefault(testCompatGateA, true))
	assert.True(t, f.Enabled(testCompatGateA), "TestCompatA should be enabled after overriding default")
	// Trying to enable TestCompatA should work.
	require.NoError(t, f.Set(string(testCompatGateA)+"=false"))
	assert.False(t, f.Enabled(testCompatGateA), "TestCompatA should be enabled after setting it explicitly")
	// Trying to override TestCompatB, TestCompatC default should fail.
	require.NoError(t, f.OverrideDefault(testCompatGateB, false))
	require.NoError(t, f.OverrideDefault(testCompatGateC, false))
	// Trying to enable TestCompatB, TestCompatC should work.
	require.NoError(t, f.Set(string(testCompatGateB)+"=true"))
	require.NoError(t, f.Set(string(testCompatGateC)+"=true"))

	// reset the feature gate first so no overrides or raw flags are carried over.
	f = f.DeepCopyAndReset().(*featureGate)
	// Now, update the gate's min compat version.
	require.NoError(t, f.SetEmulationVersionAndMinCompatibilityVersion(version.MustParse("1.30"), version.MustParse("1.30")))
	// KnownFeatures should not show the min-compatibility-version requirement anymore.
	knownExpected = []string{
		"AllAlpha=true|false (ALPHA - default=false)",
		"AllBeta=true|false (BETA - default=false)",
		"TestCompatA=true|false (BETA - default=true)",
		"TestCompatB=true|false (BETA - default=true)",
	}
	assert.ElementsMatch(t, f.KnownFeatures(), knownExpected)

	// Feature should now be enabled by default.
	assert.True(t, f.Enabled(testCompatGateA), "TestCompatA should be Beta and enabled when its min compat version is met")
	assert.True(t, f.Enabled(testCompatGateB), "TestCompatB should be enabled when its min compat version is met")
	assert.True(t, f.Enabled(testCompatGateC), "TestCompatC should be enabled when its min compat version is met")

	// Trying to disable feature should succeed.
	require.NoError(t, f.Set(string(testCompatGateA)+"=false"))
	assert.False(t, f.Enabled(testCompatGateA), "feature should be disabled")
	require.NoError(t, f.Set(string(testCompatGateB)+"=false"))
	assert.False(t, f.Enabled(testCompatGateB), "feature should be disabled")

	require.NoError(t, f.ResetFeatureValueToDefault(testCompatGateB))
	assert.True(t, f.Enabled(testCompatGateB), "feature should be enabled after resetting to default")
	// Trying to override default should now succeed.
	require.NoError(t, f.OverrideDefault(testCompatGateB, false))
	assert.False(t, f.Enabled(testCompatGateB), "feature should be disabled by default after override default to false")
	// Trying to override GA default should fail.
	require.Error(t, f.OverrideDefault(testCompatGateC, false))
	require.Error(t, f.Set(string(testCompatGateC)+"=false"))
}

// TestFeatureSpecAtEmulationAndMinCompatVersion 验证按模拟版本与最小兼容版本
// 选取规格的规则：优先满足最小兼容版本要求的最早规格；低于所有规格时返回 PreAlpha。
func TestFeatureSpecAtEmulationAndMinCompatVersion(t *testing.T) {
	specs := VersionedSpecs{
		{Version: version.MustParse("1.25"), Default: false, PreRelease: Alpha},
		{Version: version.MustParse("1.25"), Default: false, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.25")},
		{Version: version.MustParse("1.29"), Default: true, PreRelease: GA, MinCompatibilityVersion: version.MustParse("1.25")},
	}
	sort.Sort(specs)
	tests := []struct {
		emuVer       *version.Version
		minCompatVer *version.Version
		expect       FeatureSpec
	}{
		{
			emuVer: version.MustParse("1.30"),
			expect: FeatureSpec{Version: version.MustParse("1.29"), Default: true, PreRelease: GA, MinCompatibilityVersion: version.MustParse("1.25")},
		},
		{
			emuVer: version.MustParse("1.29"),
			expect: FeatureSpec{Version: version.MustParse("1.29"), Default: true, PreRelease: GA, MinCompatibilityVersion: version.MustParse("1.25")},
		},
		{
			emuVer: version.MustParse("1.28"),
			expect: FeatureSpec{Version: version.MustParse("1.25"), Default: false, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.25")},
		},
		{
			emuVer: version.MustParse("1.26"),
			expect: FeatureSpec{Version: version.MustParse("1.25"), Default: false, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.25")},
		},
		{
			emuVer:       version.MustParse("1.25"),
			minCompatVer: version.MustParse("1.25"),
			expect:       FeatureSpec{Version: version.MustParse("1.25"), Default: false, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.25")},
		},
		{
			emuVer: version.MustParse("1.25"),
			expect: FeatureSpec{Version: version.MustParse("1.25"), Default: false, PreRelease: Alpha},
		},
		{
			emuVer: version.MustParse("1.24"),
			expect: FeatureSpec{Version: version.MajorMinor(0, 0), Default: false, PreRelease: PreAlpha},
		},
		{
			emuVer:       version.MustParse("1.26"),
			minCompatVer: version.MustParse("1.23"),
			expect:       FeatureSpec{Version: version.MustParse("1.25"), Default: false, PreRelease: Alpha},
		},
		{
			emuVer:       version.MustParse("1.29"),
			minCompatVer: version.MustParse("1.23"),
			expect:       FeatureSpec{Version: version.MustParse("1.25"), Default: false, PreRelease: Alpha},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("featureSpecAtEmulationVersion for emulationVersion %s", test.emuVer), func(t *testing.T) {
			result := featureSpecAtEmulationAndMinCompatVersion(specs, test.emuVer, test.minCompatVer)
			if !reflect.DeepEqual(*result, test.expect) {
				t.Errorf("%d: featureSpecAtEmulationVersion(, %s) Expected %v, Got %v", i, test.emuVer, test.expect, result)
			}
		})
	}
}

// TestCopyKnownFeatures 验证 DeepCopyAndReset 产生的副本：
// 保留 gate 规格与默认值覆盖，但不继承显式设置的值，也不包含之后新增的 gate。
func TestCopyKnownFeatures(t *testing.T) {
	f := NewFeatureGate()
	require.NoError(t, f.Add(map[Feature]FeatureSpec{"FeatureA": {Default: false}, "FeatureB": {Default: false}}))
	require.NoError(t, f.Set("FeatureA=true"))
	require.NoError(t, f.OverrideDefault("FeatureB", true))
	fcopy := f.DeepCopyAndReset()
	require.NoError(t, f.Add(map[Feature]FeatureSpec{"FeatureC": {Default: false}}))

	assert.True(t, f.Enabled("FeatureA"))
	assert.True(t, f.Enabled("FeatureB"))
	assert.False(t, f.Enabled("FeatureC"))

	assert.False(t, fcopy.Enabled("FeatureA"))
	assert.True(t, fcopy.Enabled("FeatureB"))

	require.NoError(t, fcopy.Set("FeatureB=false"))
	assert.True(t, f.Enabled("FeatureB"))
	assert.False(t, fcopy.Enabled("FeatureB"))
	if err := fcopy.Set("FeatureC=true"); err == nil {
		t.Error("expected FeatureC not registered in the copied feature gate")
	}
}

// TestExplicitlySet 验证 ExplicitlySet 只对命令行中直接出现的 gate 返回 true；
// AllAlpha/AllBeta 只改变被批量覆盖的 gate 的值，不让它们变为“显式设置”。
func TestExplicitlySet(t *testing.T) {
	// gates for testing
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"

	tests := []struct {
		arg                   string
		expectedFeatureValue  map[Feature]bool
		expectedExplicitlySet map[Feature]bool
	}{
		{
			arg: "",
			expectedFeatureValue: map[Feature]bool{
				allAlphaGate:  false,
				allBetaGate:   false,
				testAlphaGate: false,
				testBetaGate:  false,
			},
			expectedExplicitlySet: map[Feature]bool{
				allAlphaGate:  false,
				allBetaGate:   false,
				testAlphaGate: false,
				testBetaGate:  false,
			},
		},
		{
			arg: "AllAlpha=true,TestBeta=false",
			expectedFeatureValue: map[Feature]bool{
				allAlphaGate:  true,
				allBetaGate:   false,
				testAlphaGate: true,
				testBetaGate:  false,
			},
			expectedExplicitlySet: map[Feature]bool{
				allAlphaGate:  true,
				allBetaGate:   false,
				testAlphaGate: false,
				testBetaGate:  true,
			},
		},
		{
			arg: "AllAlpha=true,AllBeta=false",
			expectedFeatureValue: map[Feature]bool{
				allAlphaGate:  true,
				allBetaGate:   false,
				testAlphaGate: true,
				testBetaGate:  false,
			},
			expectedExplicitlySet: map[Feature]bool{
				allAlphaGate:  true,
				allBetaGate:   true,
				testAlphaGate: false,
				testBetaGate:  false,
			},
		},
	}
	for i, test := range tests {
		t.Run(test.arg, func(t *testing.T) {
			fs := pflag.NewFlagSet("testfeaturegateflag", pflag.ContinueOnError)
			f := NewVersionedFeatureGate(version.MustParse("1.29"))
			err := f.AddVersioned(map[Feature]VersionedSpecs{
				testAlphaGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
				},
				testBetaGate: {
					{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
				},
			})
			require.NoError(t, err)
			f.AddFlag(fs)

			var errs []error
			err = fs.Parse([]string{fmt.Sprintf("--%s=%s", flagName, test.arg)})
			if err != nil {
				errs = append(errs, err)
			}
			err = utilerrors.NewAggregate(errs)
			require.NoError(t, err)
			for k, v := range test.expectedFeatureValue {
				if actual := f.Enabled(k); actual != v {
					t.Errorf("%d: expected %s=%v, Got %v", i, k, v, actual)
				}
			}
			for k, v := range test.expectedExplicitlySet {
				if actual := f.ExplicitlySet(k); actual != v {
					t.Errorf("%d: expected ExplicitlySet(%s)=%v, Got %v", i, k, v, actual)
				}
			}
		})
	}
}

// TestResetFeatureValueToDefault 验证 ResetFeatureValueToDefault 只清除某个 gate
// 的显式设置而保留其他 gate；切换模拟版本后会重新按新版本计算默认值。
func TestResetFeatureValueToDefault(t *testing.T) {
	// gates for testing
	const testAlphaGate Feature = "TestAlpha"
	const testBetaGate Feature = "TestBeta"

	f := NewVersionedFeatureGate(version.MustParse("1.29"))
	err := f.AddVersioned(map[Feature]VersionedSpecs{
		testAlphaGate: {
			{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
		},
		testBetaGate: {
			{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
		},
	})
	require.NoError(t, err)

	fs := pflag.NewFlagSet("testfeaturegateflag", pflag.ContinueOnError)
	assert.False(t, f.Enabled("AllAlpha"))
	assert.False(t, f.Enabled("AllBeta"))
	assert.False(t, f.Enabled("TestAlpha"))
	assert.True(t, f.Enabled("TestBeta"))

	f.AddFlag(fs)
	var errs []error
	err = fs.Parse([]string{fmt.Sprintf("--%s=%s", flagName, "AllAlpha=true,TestBeta=false")})
	if err != nil {
		errs = append(errs, err)
	}
	err = utilerrors.NewAggregate(errs)
	require.NoError(t, err)
	assert.True(t, f.Enabled("AllAlpha"))
	assert.False(t, f.Enabled("AllBeta"))
	assert.True(t, f.Enabled("TestAlpha"))
	assert.False(t, f.Enabled("TestBeta"))

	require.NoError(t, f.ResetFeatureValueToDefault("AllAlpha"))
	assert.False(t, f.Enabled("AllAlpha"))
	assert.False(t, f.Enabled("AllBeta"))
	assert.True(t, f.Enabled("TestAlpha"))
	assert.False(t, f.Enabled("TestBeta"))

	require.NoError(t, f.ResetFeatureValueToDefault("TestBeta"))
	assert.False(t, f.Enabled("AllAlpha"))
	assert.False(t, f.Enabled("AllBeta"))
	assert.True(t, f.Enabled("TestAlpha"))
	assert.True(t, f.Enabled("TestBeta"))

	require.NoError(t, f.SetEmulationVersion(version.MustParse("1.28")))
	assert.False(t, f.Enabled("AllAlpha"))
	assert.False(t, f.Enabled("AllBeta"))
	assert.False(t, f.Enabled("TestAlpha"))
	assert.False(t, f.Enabled("TestBeta"))
}

// TestAddVersioned 验证带版本 gate 规格的注册校验：
// 阶段只能单向推进、版本必须递增且不含 patch、同一版本可有多条但只有
// MinCompatibilityVersion 不同、锁定状态不可回退、未知格式一律报错。
func TestAddVersioned(t *testing.T) {
	// gates for testing
	const testAGate Feature = "TestA"
	const testBGate Feature = "TestB"
	// 表中每个用例描述一种规格序列；expectError 为 true 表示该序列应被拒绝。
	tests := []struct {
		name        string
		expectError bool
		features    map[Feature]VersionedSpecs
	}{
		{
			name: "normal progression",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.31"), Default: true, PreRelease: Beta},
					{Version: version.MustParse("1.32"), Default: true, PreRelease: GA},
					{Version: version.MustParse("1.33"), Default: false, PreRelease: Deprecated},
				},
				testBGate: {
					{Version: version.MustParse("1.27"), Default: false, PreRelease: Alpha},
				},
			},
		},
		{
			name:        "conflicting specs",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testBGate: {
					{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
				},
			},
		},
		{
			name:        "deprecated feature with no prior state",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Deprecated},
				},
			},
		},
		{
			name: "deprecated feature with prior state",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.0"), Default: true, PreRelease: GA},
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Deprecated},
				},
			},
		},
		{
			name:        "duplicate version",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
				},
			},
		},
		{
			name:        "decreasing version",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
				},
			},
		},
		{
			name:        "Beta to Alpha",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Alpha},
				},
			},
		},
		{
			name:        "GA to Alpha",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: true, PreRelease: GA},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Alpha},
				},
			},
		},
		{
			name:        "GA to Beta",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: true, PreRelease: GA},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: Beta},
				},
			},
		},
		{
			name:        "Alpha to Deprecated to Beta",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Deprecated},
					{Version: version.MustParse("1.31"), Default: true, PreRelease: Beta},
				},
			},
		},
		{
			name: "Deprecated to Deprecated",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: Deprecated},
					{Version: version.MustParse("1.31"), Default: false, PreRelease: Deprecated},
				},
			},
		},
		{
			name: "always Deprecated",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.0"), Default: false, PreRelease: Deprecated},
				},
			},
		},
		{
			name:        "patch version",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29.1"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta},
				},
			},
		},
		{
			name:        "patch min compat version",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.30.1")},
				},
			},
		},
		{
			name: "Alpha to GA",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: GA},
				},
			},
		},
		{
			name: "GA with default false",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: GA},
				},
			},
		},
		{
			name: "Beta to Deprecated",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Deprecated},
				},
			},
		},
		{
			name: "Alpha to Deprecated",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Deprecated},
				},
			},
		},
		{
			name:        "MinCompatibilityVersion > Version",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.30")},
				},
			},
		},
		{
			name:        "decreasing minCompatibilityVersion",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Alpha, MinCompatibilityVersion: version.MustParse("1.28")},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.27")},
				},
			},
		},
		{
			name:        "transition without stability change",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Alpha},
				},
			},
		},
		{
			name:        "resurrect from locked",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: true, PreRelease: GA, LockToDefault: true},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: GA, LockToDefault: false},
				},
			},
		},
		{
			name:        "no version provided",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Default: false, PreRelease: Alpha},
				},
			},
		},
		{
			name:        "direct to Beta with minCompatibilityVersion, no preceding entry",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
				},
			},
		},
		{
			name: "direct to Beta with minCompatibilityVersion",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
				},
			},
		},
		{
			name:        "minCompatibilityVersion greater than version",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.30")},
				},
			},
		},
		{
			name: "same version with different minCompatibilityVersion",
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: GA},
				},
			},
		},
		{
			name:        "different version with different minCompatibilityVersion",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.28"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: GA},
				},
			},
		},
		{
			name:        "same version with different stability",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: GA},
				},
			},
		},
		{
			name:        "same version with different lockToDefault",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, LockToDefault: true, MinCompatibilityVersion: version.MustParse("1.29")},
				},
			},
		},
		{
			name:        "only minCompatibilityVersion different",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha, MinCompatibilityVersion: version.MustParse("1.29")},
				},
			},
		},
		{
			name:        "multiple entries for the same version and minCompatibilityVersion",
			expectError: true,
			features: map[Feature]VersionedSpecs{
				testAGate: {
					{Version: version.MustParse("1.29"), Default: false, PreRelease: Alpha, MinCompatibilityVersion: version.MustParse("1.29")},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.29")},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("AddVersioned-%s", test.name), func(t *testing.T) {
			f := NewFeatureGate()
			// 先注册 TestB，确保冲突检测不受注册顺序影响。
			err := f.AddVersioned(map[Feature]VersionedSpecs{
				testBGate: {
					{Version: version.MustParse("1.27"), Default: false, PreRelease: Alpha},
				},
			})
			require.NoError(t, err)
			err = f.AddVersioned(test.features)
			if err != nil && !test.expectError {
				t.Errorf("expected no errors, error found %+v", err)
			}

			if err == nil && test.expectError {
				t.Errorf("expected errors, no errors found")
			}
		})
	}
}

// TestAddDependencies 验证 feature gate 依赖关系的注册校验：
// 依赖对象必须已注册、不能成环、接受合并且幂等；同时要求依赖双方的稳定性
// 与默认启用状态不出现“高级依赖低级”的倒置，并禁止闭合后再新增依赖。
func TestAddDependencies(t *testing.T) {
	const (
		// Test features
		fA       Feature = "FeatureA"
		fB       Feature = "FeatureB"
		fC       Feature = "FeatureC"
		fUnknown Feature = "FeatureUnknown"
	)

	var (
		v129 = version.MustParse("1.29")
		v130 = version.MustParse("1.30")
		v131 = version.MustParse("1.31")
		v132 = version.MustParse("1.32")
	)

	testCases := []struct {
		name        string
		features    map[Feature]VersionedSpecs
		initialDeps map[Feature][]Feature
		newDeps     map[Feature][]Feature
		expectedErr string
		finalDeps   map[Feature][]Feature
		closeGate   bool
	}{
		{
			name: "dependency on unknown feature",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129}},
			},
			newDeps:     map[Feature][]Feature{fA: {fUnknown}},
			expectedErr: "cannot add dependency from FeatureA to unknown feature FeatureUnknown",
		},
		{
			name: "unknown feature has dependency",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129}},
			},
			newDeps:     map[Feature][]Feature{fUnknown: {fA}},
			expectedErr: "cannot add dependency for unknown feature FeatureUnknown",
		},
		{
			name: "cycle",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129}},
				fB: {{Version: v129}},
				fC: {{Version: v129}},
			},
			newDeps: map[Feature][]Feature{
				fA: {fB},
				fB: {fC},
				fC: {fA},
			},
			expectedErr: "cycle detected with feature",
		},
		{
			name: "valid: no cycle with overlapping dependencies",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129}},
				fB: {{Version: v129}},
				fC: {{Version: v129}},
			},
			newDeps: map[Feature][]Feature{
				fA: {fB, fC},
				fB: {fC},
			},
		},
		{
			name: "self cycle",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129}},
			},
			newDeps:     map[Feature][]Feature{fA: {fA}},
			expectedErr: "cycle detected with feature FeatureA",
		},
		{
			name: "merge dependencies",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129}},
				fB: {{Version: v129}},
				fC: {{Version: v129}},
			},
			initialDeps: map[Feature][]Feature{fA: {fB}},
			newDeps:     map[Feature][]Feature{fA: {fC}},
			finalDeps:   map[Feature][]Feature{fA: {fB, fC}},
		},
		{
			name: "add after close",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129}},
				fB: {{Version: v129}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			closeGate:   true,
			expectedErr: "cannot add a feature gate dependency after adding it to the flag set",
		},
		{
			name: "valid: dependency is valid across all versions",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Beta}, {Version: v130, PreRelease: GA}},
				fB: {{Version: v129, PreRelease: Beta}, {Version: v130, PreRelease: GA}},
			},
			newDeps: map[Feature][]Feature{fA: {fB}},
		},
		{
			name: "invalid: stability inversion",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Beta}},
				fB: {{Version: v129, PreRelease: Alpha}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "BETA feature FeatureA cannot depend on ALPHA feature FeatureB at version 1.29",
		},
		{
			name: "invalid: stability inversion at later version",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Alpha}, {Version: v130, PreRelease: Beta}},
				fB: {{Version: v129, PreRelease: Alpha}, {Version: v130, PreRelease: Alpha, Default: true}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "BETA feature FeatureA cannot depend on ALPHA feature FeatureB at version 1.30",
		},
		{
			name: "invalid: stability inversion at earlier version",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Alpha}, {Version: v130, PreRelease: Beta}, {Version: v132, PreRelease: GA}},
				fB: {{Version: v129, PreRelease: Alpha}, {Version: v131, PreRelease: Beta}, {Version: v132, PreRelease: GA}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "BETA feature FeatureA cannot depend on ALPHA feature FeatureB at version 1.30",
		},
		{
			name: "valid: alpha feature depending on pre-alpha",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Alpha}, {Version: v131, PreRelease: Beta}},
				fB: {{Version: v130, PreRelease: Alpha}, {Version: v131, PreRelease: Beta}},
			},
			newDeps: map[Feature][]Feature{fA: {fB}},
		},
		{
			name: "valid: default-enabled depends on default-enabled",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Beta, Default: true}},
				fB: {{Version: v129, PreRelease: Beta, Default: true}},
			},
			newDeps: map[Feature][]Feature{fA: {fB}},
		},
		{
			name: "valid: default-disabled depends on default-enabled",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Beta, Default: false}},
				fB: {{Version: v129, PreRelease: Beta, Default: true}},
			},
			newDeps: map[Feature][]Feature{fA: {fB}},
		},
		{
			name: "invalid: default-enabled depends on default-disabled",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Beta, Default: true}},
				fB: {{Version: v129, PreRelease: Beta, Default: false}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "default-enabled feature FeatureA cannot depend on default-disabled feature FeatureB at version 1.29",
		},
		{
			name: "invalid: default-enabled depends on default-disabled at a later version",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Alpha, Default: false}, {Version: v130, PreRelease: Beta, Default: true}},
				fB: {{Version: v129, PreRelease: Alpha, Default: false}, {Version: v130, PreRelease: Beta, Default: false}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "default-enabled feature FeatureA cannot depend on default-disabled feature FeatureB at version 1.30",
		},
		{
			name: "invalid: default-enabled depends on default-disabled at an earlier version",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: Beta, Default: true}},
				fB: {{Version: v129, PreRelease: Beta, Default: false}, {Version: v130, PreRelease: Beta, Default: true}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "default-enabled feature FeatureA cannot depend on default-disabled feature FeatureB at version 1.29",
		},
		{
			name: "valid: locked depends on locked",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: GA, Default: true, LockToDefault: true}},
				fB: {{Version: v129, PreRelease: GA, Default: true, LockToDefault: true}},
			},
			newDeps: map[Feature][]Feature{fA: {fB}},
		},
		{
			name: "invalid: locked depends on unlocked",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: GA, Default: true, LockToDefault: true}},
				fB: {{Version: v129, PreRelease: GA, Default: true, LockToDefault: false}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "locked-to-default feature FeatureA cannot depend on unlocked feature FeatureB at version 1.29",
		},
		{
			name: "invalid: locked depends on unlocked at a later version",
			features: map[Feature]VersionedSpecs{
				fA: {{Version: v129, PreRelease: GA, Default: true, LockToDefault: false}, {Version: v130, PreRelease: GA, Default: true, LockToDefault: true}},
				fB: {{Version: v129, PreRelease: GA, Default: true, LockToDefault: false}},
			},
			newDeps:     map[Feature][]Feature{fA: {fB}},
			expectedErr: "locked-to-default feature FeatureA cannot depend on unlocked feature FeatureB at version 1.30",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFeatureGate()
			require.NoError(t, f.AddVersioned(tc.features))
			if tc.initialDeps != nil {
				require.NoError(t, f.AddDependencies(tc.initialDeps))
			}
			if tc.closeGate {
				f.Close()
			}

			err := f.AddDependencies(tc.newDeps)

			if tc.expectedErr != "" {
				assert.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)

				deps := f.Dependencies()
				finalDeps := tc.finalDeps
				if finalDeps == nil {
					finalDeps = tc.newDeps
				}
				assert.Equal(t, finalDeps, deps)

				// Verify that DeepCopy clones dependencies.
				clone := f.DeepCopy().Dependencies()
				assert.Equal(t, deps, clone, "dependencies should identical after DeepCopy")
			}
		})
	}
}

// TestValidateDependencies 验证 Set 时依赖一致性检查：启用的 gate 不能依赖
// 已禁用的 gate，检查沿依赖链递归进行，未启用的 gate 不产生错误。
func TestValidateDependencies(t *testing.T) {
	const (
		featureA Feature = "FeatureA"
		featureB Feature = "FeatureB"
		featureC Feature = "FeatureC"
		featureD Feature = "FeatureD"
	)

	features := map[Feature]FeatureSpec{
		featureA: {Default: false, PreRelease: Alpha},
		featureB: {Default: false, PreRelease: Alpha},
		featureC: {Default: false, PreRelease: Alpha},
		featureD: {Default: false, PreRelease: Alpha},
	}

	dependencies := map[Feature][]Feature{
		featureA: {featureB, featureC},
		featureB: {featureD},
	}

	testCases := []struct {
		name        string
		set         string
		expectedErr string
	}{
		{
			name: "all enabled",
			set:  "FeatureA=true,FeatureB=true,FeatureC=true,FeatureD=true",
		},
		{
			name:        "one dependency disabled",
			set:         "FeatureA=true,FeatureB=false,FeatureC=true,FeatureD=true",
			expectedErr: "FeatureA is enabled, but depends on features that are disabled: [FeatureB]",
		},
		{
			name:        "another dependency disabled",
			set:         "FeatureA=true,FeatureB=true,FeatureC=false,FeatureD=true",
			expectedErr: "FeatureA is enabled, but depends on features that are disabled: [FeatureC]",
		},
		{
			name:        "multiple dependencies disabled",
			set:         "FeatureA=true,FeatureB=false,FeatureC=false,FeatureD=true",
			expectedErr: "FeatureA is enabled, but depends on features that are disabled: [FeatureB FeatureC]",
		},
		{
			name:        "transitive dependency disabled",
			set:         "FeatureA=true,FeatureB=true,FeatureC=true,FeatureD=false",
			expectedErr: "FeatureB is enabled, but depends on features that are disabled: [FeatureD]",
		},
		{
			name: "feature disabled",
			set:  "FeatureA=false,FeatureB=false,FeatureC=true,FeatureD=true",
		},
		{
			name: "all disabled",
			set:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFeatureGate()
			require.NoError(t, f.Add(features))
			require.NoError(t, f.AddDependencies(dependencies))

			err := f.Set(tc.set)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}
