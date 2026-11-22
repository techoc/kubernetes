/*
Copyright 2018 The Kubernetes Authors.

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

package version

import (
	"testing"
)

// TestCompareKubeAwareVersionStrings 覆盖比较规则的各个分支：主版本号、alpha/beta/GA
// 成熟度、次版本号的数值（而非字典序）比较，以及非 kube 风格版本串的兜底排序。
func TestCompareKubeAwareVersionStrings(t *testing.T) {
	tests := []*struct {
		v1, v2          string
		expectedGreater bool
	}{
		{"v1", "v2", false},
		{"v2", "v1", true},
		{"v10", "v2", true},      // 主版本号按数值比较
		{"v1", "v2alpha1", true}, // GA 优先于 alpha
		{"v1", "v2beta1", true},  // GA 优先于 beta
		{"v1alpha2", "v1alpha1", true},
		{"v1beta1", "v2alpha3", true},   // beta 整体优先于 alpha
		{"v1alpha10", "v1alpha2", true}, // 次版本号按数值比较
		{"v1beta10", "v1beta2", true},
		{"foo", "v1beta2", false}, // 不可识别版本排在可识别版本之后
		{"bar", "foo", true},
		{"version1", "version2", true},  // 非 kube 风格版本按字典序排序
		{"version1", "version10", true}, // 非 kube 风格版本按字典序排序
	}

	for _, tc := range tests {
		if e, a := tc.expectedGreater, CompareKubeAwareVersionStrings(tc.v1, tc.v2) > 0; e != a {
			if e {
				t.Errorf("expected %s to be greater than %s", tc.v1, tc.v2)
			} else {
				t.Errorf("expected %s to be less than %s", tc.v1, tc.v2)
			}
		}
	}
}

// Test_parseKubeVersion 覆盖版本解析的正例与反例：v1.1、v1alpha1.1 等非法格式必须被拒绝，
// alpha/beta/GA 三类合法版本要正确拆出主/次版本号与成熟度。
func Test_parseKubeVersion(t *testing.T) {
	tests := []struct {
		name             string
		v                string
		wantMajorVersion int
		wantVType        versionType
		wantMinorVersion int
		wantOk           bool
	}{
		{
			name:             "invalid version for ga",
			v:                "v1.1",
			wantMajorVersion: 0,
			wantVType:        0,
			wantMinorVersion: 0,
			wantOk:           false,
		},
		{
			name:             "invalid version for alpha",
			v:                "v1alpha1.1",
			wantMajorVersion: 0,
			wantVType:        0,
			wantMinorVersion: 0,
			wantOk:           false,
		},
		{
			name:             "alpha version",
			v:                "v1alpha1",
			wantMajorVersion: 1,
			wantVType:        0,
			wantMinorVersion: 1,
			wantOk:           true,
		},
		{
			name:             "beta version",
			v:                "v2beta10",
			wantMajorVersion: 2,
			wantVType:        1,
			wantMinorVersion: 10,
			wantOk:           true,
		},
		{
			name:             "ga version",
			v:                "v3",
			wantMajorVersion: 3,
			wantVType:        2,
			wantMinorVersion: 0,
			wantOk:           true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMajorVersion, gotVType, gotMinorVersion, gotOk := parseKubeVersion(tt.v)
			if gotMajorVersion != tt.wantMajorVersion {
				t.Errorf("parseKubeVersion() gotMajorVersion = %v, want %v", gotMajorVersion, tt.wantMajorVersion)
			}
			if gotVType != tt.wantVType {
				t.Errorf("parseKubeVersion() gotVType = %v, want %v", gotVType, tt.wantVType)
			}
			if gotMinorVersion != tt.wantMinorVersion {
				t.Errorf("parseKubeVersion() gotMinorVersion = %v, want %v", gotMinorVersion, tt.wantMinorVersion)
			}
			if gotOk != tt.wantOk {
				t.Errorf("parseKubeVersion() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
