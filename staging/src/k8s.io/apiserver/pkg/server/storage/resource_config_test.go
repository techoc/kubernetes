/*
Copyright 2015 The Kubernetes Authors.

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

package storage

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestDisabledVersion 验证组版本级开关：先禁用再显式启用，最终以启用为准；
// 未被提及的版本同样视为禁用。
func TestDisabledVersion(t *testing.T) {
	g1v1 := schema.GroupVersion{Group: "group1", Version: "version1"}
	g1v2 := schema.GroupVersion{Group: "group1", Version: "version2"}
	g2v1 := schema.GroupVersion{Group: "group2", Version: "version1"}

	config := NewResourceConfig()

	config.DisableVersions(g1v1)
	config.EnableVersions(g1v2, g2v1)

	if config.versionEnabled(g1v1) {
		t.Errorf("expected disabled for %v, from %v", g1v1, config)
	}
	if !config.versionEnabled(g1v2) {
		t.Errorf("expected enabled for %v, from %v", g1v1, config)
	}
	if !config.versionEnabled(g2v1) {
		t.Errorf("expected enabled for %v, from %v", g1v1, config)
	}
}

// TestDisabledResource 验证资源级覆盖规则与版本级的优先级关系：
//   - 版本被禁用时，未显式启用的资源随之禁用，但显式启用的资源仍然生效；
//   - 版本启用时，未指定的资源默认启用，显式禁用的资源保持禁用。
func TestDisabledResource(t *testing.T) {
	g1v1 := schema.GroupVersion{Group: "group1", Version: "version1"}
	g1v1rUnspecified := g1v1.WithResource("unspecified")
	g1v1rEnabled := g1v1.WithResource("enabled")
	g1v1rDisabled := g1v1.WithResource("disabled")
	g1v2 := schema.GroupVersion{Group: "group1", Version: "version2"}
	g1v2rUnspecified := g1v2.WithResource("unspecified")
	g1v2rEnabled := g1v2.WithResource("enabled")
	g1v2rDisabled := g1v2.WithResource("disabled")
	g2v1 := schema.GroupVersion{Group: "group2", Version: "version1"}
	g2v1rUnspecified := g2v1.WithResource("unspecified")
	g2v1rEnabled := g2v1.WithResource("enabled")
	g2v1rDisabled := g2v1.WithResource("disabled")

	config := NewResourceConfig()

	config.DisableVersions(g1v1)
	config.EnableVersions(g1v2, g2v1)

	config.EnableResources(g1v1rEnabled, g1v2rEnabled, g2v1rEnabled)
	config.DisableResources(g1v1rDisabled, g1v2rDisabled, g2v1rDisabled)

	// g1v1 下未显式启用的资源全部禁用，因为该 group-version 已禁用
	if config.ResourceEnabled(g1v1rUnspecified) {
		t.Errorf("expected disabled for %v, from %v", g1v1rUnspecified, config)
	}
	if !config.ResourceEnabled(g1v1rEnabled) {
		t.Errorf("expected enabled for %v, from %v", g1v1rEnabled, config)
	}
	if config.ResourceEnabled(g1v1rDisabled) {
		t.Errorf("expected disabled for %v, from %v", g1v1rDisabled, config)
	}
	if config.ResourceEnabled(g1v1rUnspecified) {
		t.Errorf("expected disabled for %v, from %v", g1v1rUnspecified, config)
	}

	// 已启用 group-version 中显式禁用的资源保持禁用
	if config.ResourceEnabled(g1v2rDisabled) {
		t.Errorf("expected disabled for %v, from %v", g1v2rDisabled, config)
	}
	if config.ResourceEnabled(g2v1rDisabled) {
		t.Errorf("expected disabled for %v, from %v", g2v1rDisabled, config)
	}

	// 已启用 group-version 中未指定与显式启用的资源均为启用
	if !config.ResourceEnabled(g1v2rUnspecified) {
		t.Errorf("expected enabled for %v, from %v", g1v2rUnspecified, config)
	}
	if !config.ResourceEnabled(g1v2rEnabled) {
		t.Errorf("expected enabled for %v, from %v", g1v2rEnabled, config)
	}
	if !config.ResourceEnabled(g2v1rUnspecified) {
		t.Errorf("expected enabled for %v, from %v", g2v1rUnspecified, config)
	}
	if !config.ResourceEnabled(g2v1rEnabled) {
		t.Errorf("expected enabled for %v, from %v", g2v1rEnabled, config)
	}
}

// TestAnyVersionForGroupEnabled 验证 AnyResourceForGroupEnabled 在多种配置形态下的判定：
// 组内任一版本被启用，或任一资源被显式启用，都应返回 true；空配置与全禁用返回 false。
func TestAnyVersionForGroupEnabled(t *testing.T) {
	tests := []struct {
		name      string
		creator   func() APIResourceConfigSource
		testGroup string

		expectedResult bool
	}{
		{
			name: "empty",
			creator: func() APIResourceConfigSource {
				return NewResourceConfig()
			},
			testGroup: "one",

			expectedResult: false,
		},
		{
			name: "present, but disabled",
			creator: func() APIResourceConfigSource {
				ret := NewResourceConfig()
				ret.DisableVersions(schema.GroupVersion{Group: "one", Version: "version1"})
				return ret
			},
			testGroup: "one",

			expectedResult: false,
		},
		{
			name: "present, and one version enabled",
			creator: func() APIResourceConfigSource {
				ret := NewResourceConfig()
				ret.DisableVersions(schema.GroupVersion{Group: "one", Version: "version1"})
				ret.EnableVersions(schema.GroupVersion{Group: "one", Version: "version2"})
				return ret
			},
			testGroup: "one",

			expectedResult: true,
		},
		{
			name: "present, and one resource enabled",
			creator: func() APIResourceConfigSource {
				ret := NewResourceConfig()
				ret.DisableVersions(schema.GroupVersion{Group: "one", Version: "version1"})
				ret.EnableResources(schema.GroupVersionResource{Group: "one", Version: "version2", Resource: "foo"})
				return ret
			},
			testGroup: "one",

			expectedResult: true,
		},
		{
			name: "present, and one resource under disabled version enabled",
			creator: func() APIResourceConfigSource {
				ret := NewResourceConfig()
				ret.DisableVersions(schema.GroupVersion{Group: "one", Version: "version1"})
				ret.EnableResources(schema.GroupVersionResource{Group: "one", Version: "version1", Resource: "foo"})
				return ret
			},
			testGroup: "one",

			expectedResult: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if e, a := tc.expectedResult, tc.creator().AnyResourceForGroupEnabled(tc.testGroup); e != a {
				t.Errorf("expected %v, got %v", e, a)
			}
		})
	}
}
