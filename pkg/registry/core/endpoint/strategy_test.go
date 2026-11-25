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

package endpoint

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// Test_endpointsWarning 验证手工创建对象会对非法 IP 发出警告，而带有 Endpoints Controller 管理标签的对象跳过该扫描。
func Test_endpointsWarning(t *testing.T) {
	tests := []struct {
		name      string
		endpoints *api.Endpoints
		warnings  []string
	}{
		{
			name:      "empty Endpoints",
			endpoints: &api.Endpoints{},
			warnings:  nil,
		},
		{
			name: "valid Endpoints",
			endpoints: &api.Endpoints{
				Subsets: []api.EndpointSubset{
					{
						Addresses: []api.EndpointAddress{
							{IP: "1.2.3.4"},
							{IP: "fd00::1234"},
						},
					},
				},
			},
			warnings: nil,
		},
		{
			name: "bad Endpoints",
			endpoints: &api.Endpoints{
				Subsets: []api.EndpointSubset{
					{
						Addresses: []api.EndpointAddress{
							{IP: "fd00::1234"},
							{IP: "01.02.03.04"},
						},
					},
					{
						Addresses: []api.EndpointAddress{
							{IP: "::ffff:1.2.3.4"},
						},
					},
					{
						Addresses: []api.EndpointAddress{
							{IP: "1.2.3.4"},
						},
						NotReadyAddresses: []api.EndpointAddress{
							{IP: "::ffff:1.2.3.4"},
						},
					},
				},
			},
			warnings: []string{
				"subsets[0].addresses[1].ip",
				"subsets[1].addresses[0].ip",
				"subsets[2].notReadyAddresses[0].ip",
			},
		},
		{
			// 这种情况并非真的要放行非法 IP；这里的重点是：
			// 我们信任 Endpoints controller 不会产生这种取值，
			// 而要测试的是相关检查被正确地跳过。
			name: "bad Endpoints ignored because of label",
			endpoints: &api.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"endpoints.kubernetes.io/managed-by": "endpoint-controller",
					},
				},
				Subsets: []api.EndpointSubset{
					{
						Addresses: []api.EndpointAddress{
							{IP: "fd00::1234"},
							{IP: "01.02.03.04"},
						},
					},
				},
			},
			warnings: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			warnings := endpointsWarnings(test.endpoints)
			ok := len(warnings) == len(test.warnings)
			if ok {
				for i := range warnings {
					if !strings.HasPrefix(warnings[i], test.warnings[i]) {
						ok = false
						break
					}
				}
			}
			if !ok {
				t.Errorf("Expected warnings for %v, got %v", test.warnings, warnings)
			}
		})
	}
}
