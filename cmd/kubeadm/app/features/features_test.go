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

package features

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
)

func TestKnownFeatures(t *testing.T) {
	var someFeatures = FeatureList{
		"feature2": {FeatureSpec: featuregate.FeatureSpec{Default: true, PreRelease: featuregate.Alpha}},
		"feature1": {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.Beta}},
		"feature3": {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.GA}},
		"hidden":   {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.GA}, HiddenInHelpText: true},
	}

	r := KnownFeatures(&someFeatures)

	if len(r) != 3 {
		t.Errorf("KnownFeatures returned %d values, expected 3", len(r))
	}

	// 检查第一个值是 feature1（列表应已排序）；应显示 prerelease 和默认值
	f1 := "feature1=true|false (BETA - default=false)"
	if r[0] != f1 {
		t.Errorf("KnownFeatures returned %s values, expected %s", r[0], f1)
	}
	// 检查第二个值是 feature2；应显示 prerelease 和默认值
	f2 := "feature2=true|false (ALPHA - default=true)"
	if r[1] != f2 {
		t.Errorf("KnownFeatures returned %s values, expected %s", r[1], f2)
	}
	// 检查第二个值是 feature3；GA 特性不显示 prerelease；应显示默认值
	f3 := "feature3=true|false (default=false)"
	if r[2] != f3 {
		t.Errorf("KnownFeatures returned %s values, expected %s", r[2], f3)
	}
}

func TestNewFeatureGate(t *testing.T) {
	var someFeatures = FeatureList{
		"feature1":   {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.Beta}},
		"feature2":   {FeatureSpec: featuregate.FeatureSpec{Default: true, PreRelease: featuregate.Alpha}},
		"deprecated": {FeatureSpec: featuregate.FeatureSpec{Default: true, PreRelease: featuregate.Deprecated}},
	}

	var tests = []struct {
		value                string
		expectedError        bool
		expectedFeaturesGate map[string]bool
	}{
		{ //无效值（缺少 =）
			value:         "invalidValue",
			expectedError: true,
		},
		{ //无效值（缺少 =）
			value:         "feature1=true,invalidValue",
			expectedError: true,
		},
		{ //无效值（非布尔值）
			value:         "feature1=notABoolean",
			expectedError: true,
		},
		{ //无效值（非布尔值）
			value:         "feature1=true,feature2=notABoolean",
			expectedError: true,
		},
		{ //无法识别的 feature-gate 键
			value:         "unknownFeature=false",
			expectedError: true,
		},
		{ //无法识别的 feature-gate 键
			value:         "feature1=true,unknownFeature=false",
			expectedError: true,
		},
		{ //已弃用的 feature-gate 键
			value:                "deprecated=true",
			expectedError:        false,
			expectedFeaturesGate: map[string]bool{"deprecated": true},
		},
		{ //一个特性
			value:                "feature1=true",
			expectedError:        false,
			expectedFeaturesGate: map[string]bool{"feature1": true},
		},
		{ //两个特性
			value:                "feature1=true,feature2=false",
			expectedError:        false,
			expectedFeaturesGate: map[string]bool{"feature1": true, "feature2": false},
		},
	}

	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			r, err := NewFeatureGate(&someFeatures, test.value)

			if !test.expectedError && err != nil {
				t.Errorf("NewFeatureGate failed when not expected: %v", err)
				return
			} else if test.expectedError && err == nil {
				t.Error("NewFeatureGate didn't failed when expected")
				return
			}

			if !reflect.DeepEqual(r, test.expectedFeaturesGate) {
				t.Errorf("NewFeatureGate returned a unexpected value")
			}
		})
	}
}

func TestValidateVersion(t *testing.T) {
	var someFeatures = FeatureList{
		"feature1": {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.Beta}},
		"feature2": {FeatureSpec: featuregate.FeatureSpec{Default: true, PreRelease: featuregate.Alpha}, MinimumVersion: version.MustParseSemantic("v1.17.0").WithPreRelease("alpha.1")},
	}

	var tests = []struct {
		name              string
		requestedVersion  string
		requestedFeatures map[string]bool
		expectedError     bool
	}{
		{
			name:              "no min version",
			requestedFeatures: map[string]bool{"feature1": true},
			expectedError:     false,
		},
		{
			name:              "min version but correct value given",
			requestedFeatures: map[string]bool{"feature2": true},
			requestedVersion:  "v1.17.0",
			expectedError:     false,
		},
		{
			name:              "min version and incorrect value given",
			requestedFeatures: map[string]bool{"feature2": true},
			requestedVersion:  "v1.11.2",
			expectedError:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateVersion(someFeatures, test.requestedFeatures, test.requestedVersion)
			if !test.expectedError && err != nil {
				t.Errorf("ValidateVersion failed when not expected: %v", err)
				return
			} else if test.expectedError && err == nil {
				t.Error("ValidateVersion didn't failed when expected")
				return
			}
		})
	}
}

// TestEnabledDefaults 测试当未指定任何特性门控时，Enabled 会返回
// 每个特性门控的默认值。
func TestEnabledDefaults(t *testing.T) {
	for featureName, feature := range InitFeatureGates {
		featureList := make(map[string]bool)

		enabled := Enabled(featureList, featureName)
		if enabled != feature.Default {
			t.Errorf("Enabled returned %v instead of default value %v for feature %s", enabled, feature.Default, featureName)
		}
	}
}

func TestCheckDeprecatedFlags(t *testing.T) {
	dummyMessage := "dummy message"
	var someFeatures = FeatureList{
		"feature1":   {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.Beta}},
		"deprecated": {FeatureSpec: featuregate.FeatureSpec{Default: true, PreRelease: featuregate.Deprecated}, DeprecationMessage: dummyMessage},
	}

	var tests = []struct {
		name        string
		features    map[string]bool
		expectedMsg map[string]string
	}{
		{
			name:        "deprecated feature",
			features:    map[string]bool{"deprecated": true},
			expectedMsg: map[string]string{"deprecated": dummyMessage},
		},
		{
			name:        "valid feature",
			features:    map[string]bool{"feature1": true},
			expectedMsg: map[string]string{},
		},
		{
			name:        "invalid feature",
			features:    map[string]bool{"feature2": true},
			expectedMsg: map[string]string{"feature2": "Unknown feature gate flag: feature2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := CheckDeprecatedFlags(&someFeatures, test.features)
			if !reflect.DeepEqual(test.expectedMsg, msg) {
				t.Errorf("CheckDeprecatedFlags() = %v, want %v", msg, test.expectedMsg)
			}
		})
	}
}

func TestSupports(t *testing.T) {
	var someFeatures = FeatureList{
		"feature1": {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.Beta}},
	}
	tests := []struct {
		name        string
		featureName string
		want        bool
	}{
		{
			name:        "the feature is not supported",
			featureName: "foo",
			want:        false,
		},
		{
			name:        "the feature is supported",
			featureName: "feature1",
			want:        true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Supports(someFeatures, test.featureName); got != test.want {
				t.Errorf("Supports() = %v, want %v", got, test.want)
			}
		})
	}
}
