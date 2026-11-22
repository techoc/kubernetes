/*
Copyright 2014 The Kubernetes Authors.

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

package securitycontext

import (
	"k8s.io/api/core/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// ValidSecurityContextWithContainerDefaults 创建一个有效的 v1.SecurityContext，用于测试。
// 使用空的容器默认值：非特权模式、空 Capabilities、默认 ProcMount。
// 测试中需要非 nil 的 SecurityContext 时调用此函数，避免空指针。
func ValidSecurityContextWithContainerDefaults() *v1.SecurityContext {
	priv := false
	defProcMount := v1.DefaultProcMount
	return &v1.SecurityContext{
		Capabilities: &v1.Capabilities{},
		Privileged:   &priv,
		ProcMount:    &defProcMount,
	}
}

// ValidInternalSecurityContextWithContainerDefaults 创建一个有效的内部 api.SecurityContext，用于测试。
// 与 ValidSecurityContextWithContainerDefaults 类似，但返回内部版本而非外部 v1 版本。
func ValidInternalSecurityContextWithContainerDefaults() *api.SecurityContext {
	priv := false
	dpm := api.DefaultProcMount
	return &api.SecurityContext{
		Capabilities: &api.Capabilities{},
		Privileged:   &priv,
		ProcMount:    &dpm,
	}
}
