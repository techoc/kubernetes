/*
Copyright The Kubernetes Authors.

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

package routes

import (
	"reflect"
	"testing"

	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TestGroupVersionFromPath 验证从 OpenAPI 路径前缀解析 group/version 的规则：
// 只接受恰好两段（api/v1 或 apis/<group>/<version>），多一段或少一段都视为无效，
// 否则会误把资源路径当成版本路径。
func TestGroupVersionFromPath(t *testing.T) {
	tests := []struct {
		path        string
		wantGroup   string
		wantVersion string
		wantOK      bool
	}{
		{"api/v1", "", "v1", true},
		{"apis/apps/v1", "apps", "v1", true},
		{"apis/networking.k8s.io/v1", "networking.k8s.io", "v1", true},
		{"apis/batch/v1", "batch", "v1", true},
		{"api", "", "", false},
		{"apis/apps", "", "", false},
		{"api/v1/extra", "", "", false},
		{"apis/apps/v1/extra", "", "", false},
		{"", "", "", false},
	}
	for _, tt := range tests {
		group, version, ok := groupVersionFromPath(tt.path)
		if group != tt.wantGroup || version != tt.wantVersion || ok != tt.wantOK {
			t.Errorf("groupVersionFromPath(%q) = (%q, %q, %v), want (%q, %q, %v)", tt.path, group, version, ok, tt.wantGroup, tt.wantVersion, tt.wantOK)
		}
	}
}

// gvk 构造 x-kubernetes-group-version-kind 扩展中的单条记录，保持测试数据与 JSON 结构一致。
func gvk(group, version, kind string) map[string]interface{} {
	return map[string]interface{}{"group": group, "version": version, "kind": kind}
}

// TestFilterScopedGVKs 验证按 group/version 裁剪共享类型的 GVK 扩展。
//
// 背景：同一 Go 类型（如 DeleteOptions）会被多个 group 引用，但每个 group 的 OpenAPI
// 文档只应声明自己那一条，否则客户端会看到不属于该组的 kind。核心组（空 group）
// 的条目总是保留，因为它是跨组通用的元类型。
func TestFilterScopedGVKs(t *testing.T) {
	tests := []struct {
		name     string
		gvks     []interface{}
		group    string
		version  string
		wantGVKs []interface{}
	}{
		{
			name: "DeleteOptions keeps local and core/v1",
			gvks: []interface{}{
				gvk("", "v1", "DeleteOptions"),
				gvk("apps", "v1", "DeleteOptions"),
				gvk("batch", "v1", "DeleteOptions"),
				gvk("autoscaling", "v1", "DeleteOptions"),
			},
			group:   "apps",
			version: "v1",
			wantGVKs: []interface{}{
				gvk("", "v1", "DeleteOptions"),
				gvk("apps", "v1", "DeleteOptions"),
			},
		},
		{
			name: "WatchEvent keeps local and core/v1",
			gvks: []interface{}{
				gvk("", "v1", "WatchEvent"),
				gvk("apps", "v1", "WatchEvent"),
				gvk("batch", "v1", "WatchEvent"),
			},
			group:   "batch",
			version: "v1",
			wantGVKs: []interface{}{
				gvk("", "v1", "WatchEvent"),
				gvk("batch", "v1", "WatchEvent"),
			},
		},
		{
			name: "DeleteOptions with core group keeps only core/v1",
			gvks: []interface{}{
				gvk("", "v1", "DeleteOptions"),
				gvk("apps", "v1", "DeleteOptions"),
				gvk("batch", "v1", "DeleteOptions"),
			},
			group:    "",
			version:  "v1",
			wantGVKs: []interface{}{gvk("", "v1", "DeleteOptions")},
		},
		{
			name: "single GVK unchanged",
			gvks: []interface{}{
				gvk("apps", "v1", "Deployment"),
			},
			group:    "apps",
			version:  "v1",
			wantGVKs: []interface{}{gvk("apps", "v1", "Deployment")},
		},
		{
			name: "non-meta cross-group type unchanged",
			gvks: []interface{}{
				gvk("autoscaling", "v1", "Scale"),
				gvk("apps", "v1", "Scale"),
			},
			group:   "apps",
			version: "v1",
			wantGVKs: []interface{}{
				gvk("autoscaling", "v1", "Scale"),
				gvk("apps", "v1", "Scale"),
			},
		},
		{
			name: "DeleteOptions no match removes extension",
			gvks: []interface{}{
				gvk("apps", "v1", "DeleteOptions"),
				gvk("batch", "v1", "DeleteOptions"),
			},
			group:    "networking.k8s.io",
			version:  "v1",
			wantGVKs: nil,
		},
		{
			name: "multi-version same group unchanged",
			gvks: []interface{}{
				gvk("autoscaling", "v1", "HorizontalPodAutoscaler"),
				gvk("autoscaling", "v2", "HorizontalPodAutoscaler"),
			},
			group:   "autoscaling",
			version: "v2",
			wantGVKs: []interface{}{
				gvk("autoscaling", "v1", "HorizontalPodAutoscaler"),
				gvk("autoscaling", "v2", "HorizontalPodAutoscaler"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &spec3.OpenAPI{
				Components: &spec3.Components{
					Schemas: map[string]*spec.Schema{
						"TestType": {
							VendorExtensible: spec.VendorExtensible{
								Extensions: spec.Extensions{
									"x-kubernetes-group-version-kind": tt.gvks,
								},
							},
						},
					},
				},
			}
			filterScopedGVKs(s, tt.group, tt.version)
			got, exists := s.Components.Schemas["TestType"].Extensions["x-kubernetes-group-version-kind"]
			if tt.wantGVKs == nil {
				if exists {
					t.Errorf("expected extension to be removed, got %v", got)
				}
			} else if !reflect.DeepEqual(got, tt.wantGVKs) {
				t.Errorf("got %v, want %v", got, tt.wantGVKs)
			}
		})
	}
}
