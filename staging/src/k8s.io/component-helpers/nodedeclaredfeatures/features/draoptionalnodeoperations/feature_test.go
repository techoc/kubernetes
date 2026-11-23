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

package draoptionalnodeoperations

import (
	"testing"

	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// TestDiscover 验证 Discover 的结果完全由 feature gate 决定，
// 因此开/关两个取值各测一次，确保开关能直接反映到节点的已声明特性上。
func TestDiscover(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		want    bool
	}{
		{"gate enabled", true, true},
		{"gate disabled", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &types.NodeConfiguration{
				FeatureGates: types.FeatureGateMap{DRAOptionalNodeOperationsFeatureGate: tt.enabled},
			}
			if got := Feature.Discover(cfg); got != tt.want {
				t.Errorf("Discover() = %v, want %v", got, tt.want)
			}
		})
	}
}
