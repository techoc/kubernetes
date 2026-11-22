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

package compatibility

import (
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

const (
	// testComponent 是测试中模拟的第二个组件名，与 "kube" 区分。
	testComponent = "test"
)

// TestEffectiveVersionRegistry 验证注册/查询的幂等边界：未注册返回 nil，
// 重复注册同一组件必须失败且不覆盖已有版本。
func TestEffectiveVersionRegistry(t *testing.T) {
	r := NewComponentGlobalsRegistry()
	ver1 := NewEffectiveVersionFromString("1.31", "", "")
	ver2 := NewEffectiveVersionFromString("1.28", "", "")

	if r.EffectiveVersionFor(testComponent) != nil {
		t.Fatalf("expected nil EffectiveVersion initially")
	}
	if err := r.Register(testComponent, ver1, nil); err != nil {
		t.Fatalf("expected no error to register new component, but got err: %v", err)
	}
	if !r.EffectiveVersionFor(testComponent).EqualTo(ver1) {
		t.Fatalf("expected EffectiveVersionFor to return the version registered")
	}
	// overwrite
	if err := r.Register(testComponent, ver2, nil); err == nil {
		t.Fatalf("expected error to register existing component when override is false")
	}
	if !r.EffectiveVersionFor(testComponent).EqualTo(ver1) {
		t.Fatalf("expected EffectiveVersionFor to return the version overridden")
	}
}

// testRegistry 构造一个含 kube 与 test 两个组件的注册表。
// 两个组件的版本线（1.x 与 2.x）刻意不同，便于验证标志与门控按组件隔离。
func testRegistry(t *testing.T) *componentGlobalsRegistry {
	r := NewComponentGlobalsRegistry()
	verKube := NewEffectiveVersionFromString("1.31.1-beta.0.353", "1.31", "1.30")
	fgKube := featuregate.NewVersionedFeatureGate(version.MustParse("0.0"))
	err := fgKube.AddVersioned(map[featuregate.Feature]featuregate.VersionedSpecs{
		"kubeA": {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Beta},
			{Version: version.MustParse("1.31"), Default: true, LockToDefault: true, PreRelease: featuregate.GA},
		},
		"kubeB": {
			{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		},
		"commonC": {
			{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.29"), Default: true, PreRelease: featuregate.Beta},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	verTest := NewEffectiveVersionFromString("2.8", "2.8", "2.7")
	fgTest := featuregate.NewVersionedFeatureGate(version.MustParse("0.0"))
	err = fgTest.AddVersioned(map[featuregate.Feature]featuregate.VersionedSpecs{
		"testA": {
			{Version: version.MustParse("2.7"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("2.8"), Default: false, PreRelease: featuregate.Beta},
			{Version: version.MustParse("2.10"), Default: true, PreRelease: featuregate.GA},
		},
		"testB": {
			{Version: version.MustParse("2.9"), Default: false, PreRelease: featuregate.Alpha},
		},
		"commonC": {
			{Version: version.MustParse("2.7"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("2.9"), Default: true, PreRelease: featuregate.Beta},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	utilruntime.Must(r.Register(DefaultKubeComponent, verKube, fgKube))
	utilruntime.Must(r.Register(testComponent, verTest, fgTest))
	return r
}

// TestVersionFlagOptions 验证帮助文本中的版本取值区间按组件正确生成。
func TestVersionFlagOptions(t *testing.T) {
	r := testRegistry(t)
	emuVers := strings.Join(r.unsafeVersionFlagOptions(true), ",")
	expectedEmuVers := "kube=1.31..1.31(default:1.31),test=2.8..2.8(default:2.8)"
	if emuVers != expectedEmuVers {
		t.Errorf("wanted emulation version flag options to be: %s, got %s", expectedEmuVers, emuVers)
	}
	minCompVers := strings.Join(r.unsafeVersionFlagOptions(false), ",")
	expectedMinCompVers := "kube=1.30..1.31(default:1.30),test=2.7..2.8(default:2.7)"
	if minCompVers != expectedMinCompVers {
		t.Errorf("wanted min compatibility version flag options to be: %s, got %s", expectedMinCompVers, minCompVers)
	}
}

// TestVersionFlagOptionsWithMapping 验证被映射接管的组件不再出现在标志取值提示中，
// 因为它已不能通过命令行设置。
func TestVersionFlagOptionsWithMapping(t *testing.T) {
	r := testRegistry(t)
	utilruntime.Must(r.SetVersionMapping(testComponent, DefaultKubeComponent,
		func(from *version.Version) *version.Version { return version.MajorMinor(1, from.Minor()+23) }))
	emuVers := strings.Join(r.unsafeVersionFlagOptions(true), ",")
	expectedEmuVers := "test=2.8..2.8(default:2.8)"
	if emuVers != expectedEmuVers {
		t.Errorf("wanted emulation version flag options to be: %s, got %s", expectedEmuVers, emuVers)
	}
	minCompVers := strings.Join(r.unsafeVersionFlagOptions(false), ",")
	expectedMinCompVers := "test=2.7..2.8(default:2.7)"
	if minCompVers != expectedMinCompVers {
		t.Errorf("wanted min compatibility version flag options to be: %s, got %s", expectedMinCompVers, minCompVers)
	}
}

// TestVersionedFeatureGateFlags 验证 --feature-gates 帮助文本会按组件排序汇总特性，
// 且只展示当前模拟版本下可见的特性（如 testB 在 2.7 之前不可见）。
func TestVersionedFeatureGateFlags(t *testing.T) {
	r := testRegistry(t)
	known := strings.Join(r.unsafeKnownFeatures(), "\n")
	expectedKnown := "kube:AllAlpha=true|false (ALPHA - default=false)\n" +
		"kube:AllBeta=true|false (BETA - default=false)\n" +
		"kube:commonC=true|false (BETA - default=true)\n" +
		"kube:kubeB=true|false (ALPHA - default=false)\n" +
		"test:AllAlpha=true|false (ALPHA - default=false)\n" +
		"test:AllBeta=true|false (BETA - default=false)\n" +
		"test:commonC=true|false (ALPHA - default=false)\n" +
		"test:testA=true|false (BETA - default=false)"
	if known != expectedKnown {
		t.Errorf("wanted min compatibility version flag options to be:\n%s, got:\n%s", expectedKnown, known)
	}
}

// TestFlags 覆盖三个标志的端到端解析与生效：
// 版本默认前缀、重复项、补丁号、未注册组件、门控前缀混用、锁定/未知特性，
// 以及多次 AddFlags 之间配置是否被意外重置。
func TestFlags(t *testing.T) {
	tests := []struct {
		name                                string
		setupRegistry                       func(r *componentGlobalsRegistry) error
		flags                               []string
		parseError                          string
		expectedKubeEmulationVersion        string
		expectedTestEmulationVersion        string
		expectedKubeMinCompatibilityVersion string
		expectedTestMinCompatibilityVersion string
		expectedKubeFeatureValues           map[featuregate.Feature]bool
		expectedTestFeatureValues           map[featuregate.Feature]bool
	}{
		{
			name:                                "setting kube emulation version",
			flags:                               []string{"--emulated-version=kube=1.30", "--min-compatibility-version=kube=1.28"},
			expectedKubeEmulationVersion:        "1.30",
			expectedKubeMinCompatibilityVersion: "1.28",
		},
		{
			name: "setting kube emulation version twice",
			flags: []string{
				"--emulated-version=kube=1.30",
				"--emulated-version=kube=1.32",
			},
			parseError: "duplicate version flag, kube=1.30 and kube=1.32",
		},
		{
			name: "setting min compatibility version twice",
			flags: []string{
				"--min-compatibility-version=kube=1.30",
				"--min-compatibility-version=kube=1.29",
			},
			parseError: "duplicate version flag, kube=1.30 and kube=1.29",
		},
		{
			name:                                "prefix v ok",
			flags:                               []string{"--emulated-version=kube=v1.30", "--min-compatibility-version=kube=v1.28"},
			expectedKubeEmulationVersion:        "1.30",
			expectedKubeMinCompatibilityVersion: "1.28",
		},
		{
			name:       "patch version not ok",
			flags:      []string{"--emulated-version=kube=1.30.2"},
			parseError: "patch version not allowed, got: kube=1.30.2",
		},
		{
			name:       "patch min compatibility version not ok",
			flags:      []string{"--min-compatibility-version=kube=1.30.2"},
			parseError: "patch version not allowed, got: kube=1.30.2",
		},
		{
			name:                                "setting test emulation version",
			flags:                               []string{"--emulated-version=test=2.7", "--min-compatibility-version=test=v2.5"},
			expectedKubeEmulationVersion:        "1.31",
			expectedTestEmulationVersion:        "2.7",
			expectedKubeMinCompatibilityVersion: "1.30",
			expectedTestMinCompatibilityVersion: "2.5",
		},
		{
			name:                                "version missing component default to kube",
			flags:                               []string{"--emulated-version=1.30", "--min-compatibility-version=v1.28"},
			expectedKubeEmulationVersion:        "1.30",
			expectedKubeMinCompatibilityVersion: "1.28",
		},
		{
			name:       "version missing component default to kube with duplicate",
			flags:      []string{"--emulated-version=1.30", "--emulated-version=kube=1.30"},
			parseError: "duplicate version flag, kube=1.30 and kube=1.30",
		},
		{
			name:       "version unregistered component",
			flags:      []string{"--emulated-version=test3=1.31"},
			parseError: "component not registered: test3",
		},
		{
			name:       "invalid version",
			flags:      []string{"--emulated-version=test=1.foo"},
			parseError: "illegal version string \"1.foo\"",
		},
		{
			name:       "min compatibility version missing component default to kube with duplicate",
			flags:      []string{"--min-compatibility-version=1.30", "--min-compatibility-version=kube=1.30"},
			parseError: "duplicate version flag, kube=1.30 and kube=1.30",
		},
		{
			name:       "min compatibility version unregistered component",
			flags:      []string{"--min-compatibility-version=test3=1.31"},
			parseError: "component not registered: test3",
		},
		{
			name:       "invalid min compatibility version",
			flags:      []string{"--min-compatibility-version=test=1.foo"},
			parseError: "illegal version string \"1.foo\"",
		},
		{
			name: "setting test feature flag",
			flags: []string{
				"--emulated-version=test=2.7",
				"--feature-gates=test:testA=true",
			},
			expectedKubeEmulationVersion: "1.31",
			expectedTestEmulationVersion: "2.7",
			expectedKubeFeatureValues:    map[featuregate.Feature]bool{"kubeA": true, "kubeB": false, "commonC": true},
			expectedTestFeatureValues:    map[featuregate.Feature]bool{"testA": true, "testB": false, "commonC": false},
		},
		{
			name: "setting future test feature flag",
			flags: []string{
				"--emulated-version=test=2.7",
				"--feature-gates=test:testA=true,test:testB=true",
			},
			parseError: "cannot set feature gate testB to true, feature is PreAlpha at emulated version 2.7",
		},
		{
			name: "setting kube feature flag",
			flags: []string{
				"--emulated-version=test=2.7",
				"--emulated-version=kube=1.30",
				"--feature-gates=kubeB=false,test:commonC=true",
				"--feature-gates=commonC=false,kubeB=true",
			},
			expectedKubeEmulationVersion: "1.30",
			expectedTestEmulationVersion: "2.7",
			expectedKubeFeatureValues:    map[featuregate.Feature]bool{"kubeA": false, "kubeB": true, "commonC": false},
			expectedTestFeatureValues:    map[featuregate.Feature]bool{"testA": false, "testB": false, "commonC": true},
		},
		{
			name: "setting kube feature flag with different prefix",
			flags: []string{
				"--emulated-version=test=2.7",
				"--emulated-version=kube=1.30",
				"--feature-gates=kube:kubeB=false,test:commonC=true",
				"--feature-gates=commonC=false,kubeB=true",
			},
			parseError: "set kube feature gates with default empty prefix or kube: prefix consistently, do not mix use",
		},
		{
			name: "setting locked kube feature flag",
			flags: []string{
				"--emulated-version=test=2.7",
				"--feature-gates=kubeA=false",
			},
			parseError: "cannot set feature gate kubeA to false, feature is locked to true",
		},
		{
			name: "setting unknown test feature flag",
			flags: []string{
				"--emulated-version=test=2.7",
				"--feature-gates=test:testD=true",
			},
			parseError: "unrecognized feature gate: testD",
		},
		{
			name: "setting unknown component feature flag",
			flags: []string{
				"--emulated-version=test=2.7",
				"--feature-gates=test3:commonC=true",
			},
			parseError: "component not registered: test3",
		},
		{
			name: "feature gates config should accumulate across multiple flag sets",
			// 同一注册表被两次挂到不同 FlagSet 上解析，配置应累积而不是被清空。
			setupRegistry: func(r *componentGlobalsRegistry) error {
				fs := pflag.NewFlagSet("setupTestflag", pflag.ContinueOnError)
				r.AddFlags(fs)
				return fs.Parse([]string{"--feature-gates=test:commonC=true"})
			},
			flags: []string{
				"--feature-gates=test:testA=true",
			},
			expectedTestFeatureValues: map[featuregate.Feature]bool{"testA": true, "commonC": true},
		},
		{
			name: "feature gates config should be overridden when set multiple times one the same feature",
			// 同一特性后出现的标志值覆盖先前的值。
			setupRegistry: func(r *componentGlobalsRegistry) error {
				fs := pflag.NewFlagSet("setupTestflag", pflag.ContinueOnError)
				r.AddFlags(fs)
				return fs.Parse([]string{"--feature-gates=test:testA=false"})
			},
			flags: []string{
				"--feature-gates=test:testA=true",
			},
			expectedTestFeatureValues: map[featuregate.Feature]bool{"testA": true},
		},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := pflag.NewFlagSet("testflag", pflag.ContinueOnError)
			r := testRegistry(t)
			if test.setupRegistry != nil {
				if err := test.setupRegistry(r); err != nil {
					t.Fatalf("failed to setup registry: %v", err)
				}
			}
			r.AddFlags(fs)
			err := fs.Parse(test.flags)
			if err == nil {
				// AddFlags again to check whether there is no resetting on the config.
				// 第二次 AddFlags 不得清空已解析的配置。
				fs = pflag.NewFlagSet("testflag2", pflag.ContinueOnError)
				r.AddFlags(fs)
				err = r.Set()
			}
			if test.parseError != "" {
				if err == nil || !strings.Contains(err.Error(), test.parseError) {
					t.Fatalf("%d: Parse() expected: %v, got: %v", i, test.parseError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%d: Parse() expected: nil, got: %v", i, err)
			}
			if len(test.expectedKubeEmulationVersion) > 0 {
				assertVersionEqualTo(t, r.EffectiveVersionFor(DefaultKubeComponent).EmulationVersion(), test.expectedKubeEmulationVersion)
			}
			if len(test.expectedTestEmulationVersion) > 0 {
				assertVersionEqualTo(t, r.EffectiveVersionFor(testComponent).EmulationVersion(), test.expectedTestEmulationVersion)
			}
			if len(test.expectedKubeMinCompatibilityVersion) > 0 {
				assertVersionEqualTo(t, r.EffectiveVersionFor(DefaultKubeComponent).MinCompatibilityVersion(), test.expectedKubeMinCompatibilityVersion)
			}
			if len(test.expectedTestMinCompatibilityVersion) > 0 {
				assertVersionEqualTo(t, r.EffectiveVersionFor(testComponent).MinCompatibilityVersion(), test.expectedTestMinCompatibilityVersion)
			}
			for f, v := range test.expectedKubeFeatureValues {
				if r.FeatureGateFor(DefaultKubeComponent).Enabled(f) != v {
					t.Errorf("%d: expected kube feature Enabled(%s)=%v", i, f, v)
				}
			}
			for f, v := range test.expectedTestFeatureValues {
				if r.FeatureGateFor(testComponent).Enabled(f) != v {
					t.Errorf("%d: expected test feature Enabled(%s)=%v", i, f, v)
				}
			}
		})
	}
}

// TestVersionMapping 验证映射链 test1→test2→test3 的初始推导，
// 以及之后通过标志修改链头版本时，下游组件如何重新计算。
func TestVersionMapping(t *testing.T) {
	r := NewComponentGlobalsRegistry()
	ver1 := NewEffectiveVersionFromString("0.58", "", "")
	ver2 := NewEffectiveVersionFromString("1.28", "", "")
	ver3 := NewEffectiveVersionFromString("2.10", "", "")

	utilruntime.Must(r.Register("test1", ver1, nil))
	utilruntime.Must(r.Register("test2", ver2, nil))
	utilruntime.Must(r.Register("test3", ver3, nil))

	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").EmulationVersion(), "0.58")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").MinCompatibilityVersion(), "0.57")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").EmulationVersion(), "1.28")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").MinCompatibilityVersion(), "1.27")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").EmulationVersion(), "2.10")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").MinCompatibilityVersion(), "2.9")

	utilruntime.Must(r.SetVersionMapping("test2", "test3",
		func(from *version.Version) *version.Version {
			return version.MajorMinor(from.Major()+1, from.Minor()-19)
		}))
	utilruntime.Must(r.SetVersionMapping("test1", "test2",
		func(from *version.Version) *version.Version {
			return version.MajorMinor(from.Major()+1, from.Minor()-28)
		}))
	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").EmulationVersion(), "0.58")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").MinCompatibilityVersion(), "0.57")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").EmulationVersion(), "1.30")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").MinCompatibilityVersion(), "1.29")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").EmulationVersion(), "2.11")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").MinCompatibilityVersion(), "2.10")

	fs := pflag.NewFlagSet("testflag", pflag.ContinueOnError)
	r.AddFlags(fs)

	if err := fs.Parse([]string{"--emulated-version=test1=0.56", "--min-compatibility-version=test1=0.54"}); err != nil {
		t.Fatal(err)
		return
	}
	if err := r.Set(); err != nil {
		t.Fatal(err)
		return
	}
	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").EmulationVersion(), "0.56")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").MinCompatibilityVersion(), "0.54")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").EmulationVersion(), "1.28")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").MinCompatibilityVersion(), "1.26")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").EmulationVersion(), "2.9")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").MinCompatibilityVersion(), "2.7")
}

// TestVersionMappingWithMultipleDependency 验证一个组件只允许一个上游映射，
// 第二次为 test2 设置不同来源必须报错。
func TestVersionMappingWithMultipleDependency(t *testing.T) {
	r := NewComponentGlobalsRegistry()
	ver1 := NewEffectiveVersionFromString("0.58", "", "")
	ver2 := NewEffectiveVersionFromString("1.28", "", "")
	ver3 := NewEffectiveVersionFromString("2.10", "", "")

	utilruntime.Must(r.Register("test1", ver1, nil))
	utilruntime.Must(r.Register("test2", ver2, nil))
	utilruntime.Must(r.Register("test3", ver3, nil))

	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").EmulationVersion(), "0.58")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").EmulationVersion(), "1.28")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").EmulationVersion(), "2.10")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").MinCompatibilityVersion(), "0.57")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").MinCompatibilityVersion(), "1.27")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").MinCompatibilityVersion(), "2.9")

	utilruntime.Must(r.SetVersionMapping("test1", "test2",
		func(from *version.Version) *version.Version {
			return version.MajorMinor(from.Major()+1, from.Minor()-28)
		}))
	err := r.SetVersionMapping("test3", "test2",
		func(from *version.Version) *version.Version {
			return version.MajorMinor(from.Major()-1, from.Minor()+19)
		})
	if err == nil {
		t.Errorf("expect error when setting 2nd mapping to test2")
	}
}

// TestVersionMappingWithCyclicDependency 验证形成环的映射（test3→test1）会被拒绝，
// 否则版本传播时将无限循环。
func TestVersionMappingWithCyclicDependency(t *testing.T) {
	r := NewComponentGlobalsRegistry()
	ver1 := NewEffectiveVersionFromString("0.58", "", "")
	ver2 := NewEffectiveVersionFromString("1.28", "", "")
	ver3 := NewEffectiveVersionFromString("2.10", "", "")

	utilruntime.Must(r.Register("test1", ver1, nil))
	utilruntime.Must(r.Register("test2", ver2, nil))
	utilruntime.Must(r.Register("test3", ver3, nil))

	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").EmulationVersion(), "0.58")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").EmulationVersion(), "1.28")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").EmulationVersion(), "2.10")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test1").MinCompatibilityVersion(), "0.57")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test2").MinCompatibilityVersion(), "1.27")
	assertVersionEqualTo(t, r.EffectiveVersionFor("test3").MinCompatibilityVersion(), "2.9")

	utilruntime.Must(r.SetVersionMapping("test1", "test2",
		func(from *version.Version) *version.Version {
			return version.MajorMinor(from.Major()+1, from.Minor()-28)
		}))
	utilruntime.Must(r.SetVersionMapping("test2", "test3",
		func(from *version.Version) *version.Version {
			return version.MajorMinor(from.Major()+1, from.Minor()-19)
		}))
	err := r.SetVersionMapping("test3", "test1",
		func(from *version.Version) *version.Version {
			return version.MajorMinor(from.Major()-2, from.Minor()+48)
		})
	if err == nil {
		t.Errorf("expect cyclic version mapping error")
	}
}

// TestAddMetrics 验证 version_info 指标包含 binary/emulation/min_compat 三个标签。
// 使用 legacyregistry 默认 gatherer，因此断言的是全局指标输出。
func TestAddMetrics(t *testing.T) {
	r := NewComponentGlobalsRegistry()
	ver1 := NewEffectiveVersionFromString("0.58", "", "")
	ver2 := NewEffectiveVersionFromString("1.2", "1.1", "")

	if err := r.Register("comp1", ver1, nil); err != nil {
		t.Fatalf("expected no error to register new component, but got err: %v", err)
	}
	if err := r.Register("comp2", ver2, nil); err != nil {
		t.Fatalf("expected no error to register new component, but got err: %v", err)
	}
	r.AddMetrics()

	expectedOutput := `# HELP version_info [ALPHA] Provides the compatibility version info of the component. The component label is the name of the component, usually kube, but is relevant for aggregated-apiservers.
    # TYPE version_info gauge
    version_info{binary="0.58",component="comp1",emulation="0.58",min_compat="0.57"} 1
    version_info{binary="1.2",component="comp2",emulation="1.2",min_compat="1.1"} 1
`
	testedMetrics := []string{"version_info"}
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedOutput), testedMetrics...); err != nil {
		t.Fatal(err)
	}
}

// assertVersionEqualTo 断言版本相等；失败时输出期望值与实际值，方便定位映射公式问题。
func assertVersionEqualTo(t *testing.T, ver *version.Version, expectedVer string) {
	if ver.EqualTo(version.MustParse(expectedVer)) {
		return
	}
	t.Errorf("expected: %s, got %s", expectedVer, ver.String())
}

// Test_enabledAlphaFeatures 只返回“处于 Alpha 且已启用”的特性，
// beta 特性与显式关闭的 alpha 特性都不应出现在结果中。
func Test_enabledAlphaFeatures(t *testing.T) {
	features := map[featuregate.Feature]featuregate.FeatureSpec{
		"myFeat": {
			PreRelease: featuregate.Alpha,
		},
		"myOtherFeat": {
			PreRelease: featuregate.Beta,
		},
		"otherFeatDisabled": {
			PreRelease: featuregate.Alpha,
		},
	}

	alphaGate := featuregate.NewFeatureGate()
	if err := alphaGate.Add(features); err != nil {
		t.Fatalf("Unable to add features, %s", err)
	}

	err := alphaGate.SetFromMap(
		map[string]bool{
			"myFeat":            true,
			"myOtherFeat":       true,
			"otherFeatDisabled": false,
		},
	)
	if err != nil {
		t.Fatalf("Unable to set feature gate, %s", err)
	}

	globals := &ComponentGlobals{
		featureGate: alphaGate,
	}

	want := []string{
		"myFeat",
	}

	if got := enabledAlphaFeatures(features, globals); !reflect.DeepEqual(got, want) {
		t.Errorf("enabledAlphaFeatures() = %v, want %v", got, want)
	}
}
