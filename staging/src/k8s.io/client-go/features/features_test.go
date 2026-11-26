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

package features

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/version"
)

// TestAddFeaturesToExistingFeatureGates 验证默认特性门控能加入测试用门控注册表。
//
// 验证非版本化注册路径：期望注册表收到的是“最新规格 + 清空版本”的视图。
func TestAddFeaturesToExistingFeatureGates(t *testing.T) {
	fakeFeatureGates := &fakeRegistry{}
	require.NoError(t, AddFeaturesToExistingFeatureGates(fakeFeatureGates))
	require.Equal(t, unversionedFeatureGates(defaultVersionedKubernetesFeatureGates), fakeFeatureGates.specs)
}

// TestAddVersionedFeaturesToExistingFeatureGates 验证默认特性门控能加入带版本的测试门控注册表。
//
// 验证版本化注册路径：完整的 VersionedSpecs 应原样传给注册表。
func TestAddVersionedFeaturesToExistingFeatureGates(t *testing.T) {
	fakeFeatureGates := &fakeVersionedRegistry{}
	require.NoError(t, AddVersionedFeaturesToExistingFeatureGates(fakeFeatureGates))
	require.Equal(t, defaultVersionedKubernetesFeatureGates, fakeFeatureGates.specs)
}

// TestUnversionedFeatureGates 校验降级逻辑：多版本特性应只取最后一条，
// 并丢弃 Version 字段；单版本特性保持原样。
func TestUnversionedFeatureGates(t *testing.T) {
	testCases := []struct {
		name         string
		featureGates map[Feature]VersionedSpecs
		expected     map[Feature]FeatureSpec
	}{
		{
			name: "multiple features",
			featureGates: map[Feature]VersionedSpecs{
				"AlphaFeature": {
					{Version: version.MustParse("1.30"), Default: false, PreRelease: Alpha},
				},
				"BetaFeature": {
					{Version: version.MustParse("1.28"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.30"), Default: true, PreRelease: Beta},
				},
				"GAFeature": {
					{Version: version.MustParse("1.25"), Default: false, PreRelease: Alpha},
					{Version: version.MustParse("1.27"), Default: true, PreRelease: Beta},
					{Version: version.MustParse("1.29"), Default: true, PreRelease: GA, LockToDefault: true},
				},
			},
			expected: map[Feature]FeatureSpec{
				"AlphaFeature": {Default: false, PreRelease: Alpha},
				"BetaFeature":  {Default: true, PreRelease: Beta},
				"GAFeature":    {Default: true, PreRelease: GA, LockToDefault: true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := unversionedFeatureGates(tc.featureGates)
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("unversionedFeatureGates() = %v, want %v", actual, tc.expected)
			}
		})
	}
}

// TestReplaceFeatureGatesWithWarningIndicator 验证：默认实现已经读取过门控值之后再替换，
// 返回值必须指示需要告警，避免调用方在错误的初始化顺序下静默覆盖。
func TestReplaceFeatureGatesWithWarningIndicator(t *testing.T) {
	defaultFeatureGates := FeatureGates()
	require.Panics(t, func() { defaultFeatureGates.Enabled("Foo") }, "reading an unregistered feature gate Foo should panic")

	if !replaceFeatureGatesWithWarningIndicator(defaultFeatureGates) {
		t.Error("replacing the default feature gates after reading a value hasn't produced a warning")
	}
}

// fakeRegistry 是最小化的非版本化注册表，仅记录最后一次 Add 的内容。
type fakeRegistry struct {
	specs map[Feature]FeatureSpec
}

func (f *fakeRegistry) Add(specs map[Feature]FeatureSpec) error {
	f.specs = specs
	return nil
}

// fakeVersionedRegistry 是最小化的版本化注册表，仅记录最后一次 AddVersioned 的内容。
type fakeVersionedRegistry struct {
	specs map[Feature]VersionedSpecs
}

func (f *fakeVersionedRegistry) AddVersioned(specs map[Feature]VersionedSpecs) error {
	f.specs = specs
	return nil
}
