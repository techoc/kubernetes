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

package v1

import (
	"testing"

	v1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
)

// TestServiceNameConstants 验证服务名常量与生成的 gRPC 代码
// 中声明的名字一致。插件注册时广播的就是这些常量，
// kubelet 也按它们匹配，必须与 gRPC 服务定义保持同步。
// kubelet 也按它们匹配，必须与 gRPC 服务定义保持同步。
//
// 常量若与生成代码不一致，插件注册时的版本匹配会静默失败，
// 这类问题在真机上表现为健康检查“没有数据”，很难定位。
func TestServiceNameConstants(t *testing.T) {
	if DRAResourceHealthService != DRAResourceHealth_ServiceDesc.ServiceName {
		t.Errorf("v1.DRAResourceHealthService = %q, want generated service name %q", DRAResourceHealthService, DRAResourceHealth_ServiceDesc.ServiceName)
	}
	if v1alpha1.DRAResourceHealthService != v1alpha1.DRAResourceHealth_ServiceDesc.ServiceName {
		t.Errorf("v1alpha1.DRAResourceHealthService = %q, want generated service name %q", v1alpha1.DRAResourceHealthService, v1alpha1.DRAResourceHealth_ServiceDesc.ServiceName)
	}
}
