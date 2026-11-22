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
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
	basecompatibility "k8s.io/component-base/compatibility"
)

// TestValidateKubeEffectiveVersion 覆盖 emulation/minCompatibility 版本组合的校验：
// 只有 emulation >= minCompatibility >= binary 的版本下限时才算合法。
func TestValidateKubeEffectiveVersion(t *testing.T) {
	tests := []struct {
		name                    string
		emulationVersion        string
		minCompatibilityVersion string
		expectErr               bool
	}{
		{
			name:                    "valid versions",
			emulationVersion:        "v1.32.0",
			minCompatibilityVersion: "v1.31.0",
			expectErr:               false,
		},
		{
			name:                    "emulationVersion too low",
			emulationVersion:        "v1.30.0",
			minCompatibilityVersion: "v1.31.0",
			expectErr:               true,
		},
		{
			name:                    "minCompatibilityVersion too low",
			emulationVersion:        "v1.31.0",
			minCompatibilityVersion: "v1.30.0",
			expectErr:               true,
		},
		{
			name:                    "both versions too low",
			emulationVersion:        "v1.30.0",
			minCompatibilityVersion: "v1.29.0",
			expectErr:               true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 以 1.33 作为二进制版本，得到对应的版本下限；随后显式设置
			// 仿真版本与最低兼容版本，交由 Validate 检查区间关系。
			binaryVersion := version.MustParseGeneric("1.33")
			versionFloor := kubeEffectiveVersionFloors(binaryVersion)
			effective := basecompatibility.NewEffectiveVersion(binaryVersion, false, versionFloor, versionFloor)
			effective.SetEmulationVersion(version.MustParseGeneric(test.emulationVersion))
			effective.SetMinCompatibilityVersion(version.MustParseGeneric(test.minCompatibilityVersion))

			err := effective.Validate()
			if test.expectErr && err == nil {
				t.Error("expected error, but got nil")
			}
			if !test.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
