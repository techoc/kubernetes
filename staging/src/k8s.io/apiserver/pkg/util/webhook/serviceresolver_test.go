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

package webhook

import (
	"fmt"
	"testing"
)

// TestDefaultServiceResolver 验证 service 引用的解析格式：
// 需要同时提供 namespace 与 name，端口来自参数，结果为集群内 DNS 形式。
func TestDefaultServiceResolver(t *testing.T) {
	scenarios := []struct {
		serviceName      string
		serviceNamespace string
		port             int32
		expectedOutput   string
		expectError      bool
	}{
		// 场景 1：服务名带命名空间，应能解析。
		{serviceName: "ross", serviceNamespace: "andromeda", port: 443, expectedOutput: "https://ross.andromeda.svc:443"},
		// 场景 2：服务名不带命名空间，不应解析。
		{serviceName: "ross", expectError: true},
		// 场景 3：空服务名无法解析。
		{serviceNamespace: "andromeda", expectError: true},
		// 场景 1：服务名带命名空间且指定不同端口，应能解析。
		{serviceName: "ross", serviceNamespace: "andromeda", port: 1002, expectedOutput: "https://ross.andromeda.svc:1002"},
	}

	// act
	for index, scenario := range scenarios {
		t.Run(fmt.Sprintf("scenario %d", index), func(t *testing.T) {
			target := defaultServiceResolver{}
			serviceURL, err := target.ResolveEndpoint(scenario.serviceNamespace, scenario.serviceName, scenario.port)

			if err != nil && !scenario.expectError {
				t.Errorf("unexpected error has occurred = %v", err)
			}
			if err == nil && scenario.expectError {
				t.Error("expected an error but got nothing")
			}
			if !scenario.expectError {
				if serviceURL.String() != scenario.expectedOutput {
					t.Errorf("expected = %s, got = %s", scenario.expectedOutput, serviceURL.String())
				}
			}
		})
	}
}
