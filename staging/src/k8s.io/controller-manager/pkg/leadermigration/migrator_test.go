/*
Copyright 2021 The Kubernetes Authors.

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

package leadermigration

import (
	"testing"

	internal "k8s.io/controller-manager/config"
)

// 表驱动测试 FilterFunc：分别用“迁移来源”“迁移目标”“通配符”三种配置，
// 在 KCM 和 CCM 两个组件视角下验证每个控制器的启动策略。
func TestLeaderMigratorFilterFunc(t *testing.T) {
	// fromConfig：控制器原本在 KCM，正迁移到 CCM。
	fromConfig := &internal.LeaderMigrationConfiguration{
		ResourceLock: "leases",
		LeaderName:   "cloud-provider-extraction-migration",
		ControllerLeaders: []internal.ControllerLeaderConfiguration{
			{
				Name:      "route-controller",
				Component: "kube-controller-manager",
			}, {
				Name:      "service-controller",
				Component: "kube-controller-manager",
			}, {
				Name:      "cloud-node-lifecycle-controller",
				Component: "kube-controller-manager",
			},
		},
	}
	// toConfig：控制器迁移的目标为 CCM。
	toConfig := &internal.LeaderMigrationConfiguration{
		ResourceLock: "leases",
		LeaderName:   "cloud-provider-extraction-migration",
		ControllerLeaders: []internal.ControllerLeaderConfiguration{
			{
				Name:      "route-controller",
				Component: "cloud-controller-manager",
			}, {
				Name:      "service-controller",
				Component: "cloud-controller-manager",
			}, {
				Name:      "cloud-node-lifecycle-controller",
				Component: "cloud-controller-manager",
			},
		},
	}
	// wildcardConfig：控制器归属设为 "*"，两边都允许接管。
	wildcardConfig := &internal.LeaderMigrationConfiguration{
		ResourceLock: "leases",
		LeaderName:   "cloud-provider-extraction-migration",
		ControllerLeaders: []internal.ControllerLeaderConfiguration{
			{
				Name:      "route-controller",
				Component: "*",
			}, {
				Name:      "service-controller",
				Component: "*",
			}, {
				Name:      "cloud-node-lifecycle-controller",
				Component: "*",
			},
		},
	}
	for _, tc := range []struct {
		name         string
		config       *internal.LeaderMigrationConfiguration
		component    string
		migrated     bool
		expectResult map[string]FilterResult
	}{
		{
			name:      "from config, kcm",
			config:    fromConfig,
			component: "kube-controller-manager",
			expectResult: map[string]FilterResult{
				"deployment-controller":           ControllerNonMigrated,
				"route-controller":                ControllerMigrated,
				"service-controller":              ControllerMigrated,
				"cloud-node-lifecycle-controller": ControllerMigrated,
			},
		},
		{
			name:      "from config, ccm",
			config:    fromConfig,
			component: "cloud-controller-manager",
			expectResult: map[string]FilterResult{
				"cloud-node":                      ControllerNonMigrated,
				"route-controller":                ControllerUnowned,
				"service-controller":              ControllerUnowned,
				"cloud-node-lifecycle-controller": ControllerUnowned,
			},
		},
		{
			name:      "to config, kcm",
			config:    toConfig,
			component: "kube-controller-manager",
			expectResult: map[string]FilterResult{
				"deployment-controller":           ControllerNonMigrated,
				"route-controller":                ControllerUnowned,
				"service-controller":              ControllerUnowned,
				"cloud-node-lifecycle-controller": ControllerUnowned,
			},
		},
		{
			name:      "to config, ccm",
			config:    toConfig,
			component: "cloud-controller-manager",
			expectResult: map[string]FilterResult{
				"cloud-node-controller":           ControllerNonMigrated,
				"route-controller":                ControllerMigrated,
				"service-controller":              ControllerMigrated,
				"cloud-node-lifecycle-controller": ControllerMigrated,
			},
		},
		{
			name:      "wildcard config, kcm",
			config:    wildcardConfig,
			component: "kube-controller-manager",
			expectResult: map[string]FilterResult{
				"deployment-controller":           ControllerNonMigrated, // KCM only
				"route-controller":                ControllerMigrated,
				"service-controller":              ControllerMigrated,
				"cloud-node-lifecycle-controller": ControllerMigrated,
			},
		},
		{
			name:      "wildcard config, ccm",
			config:    wildcardConfig,
			component: "cloud-controller-manager",
			expectResult: map[string]FilterResult{
				"cloud-node-controller":           ControllerNonMigrated, // CCM only
				"route-controller":                ControllerMigrated,
				"service-controller":              ControllerMigrated,
				"cloud-node-lifecycle-controller": ControllerMigrated,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			migrator := NewLeaderMigrator(tc.config, tc.component)
			// 用当前 case 的配置与组件构造迁移器，再逐条核对期望结果。
			for name, expected := range tc.expectResult {
				if result := migrator.FilterFunc(name); expected != result {
					t.Errorf("controller %s, expect %v, got %v", name, expected, result)
				}
			}
		})
	}
}
