/*
Copyright 2022 The Kubernetes Authors.

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

	componentconfigtesting "k8s.io/component-base/config/testing"
)

// TestComponentConfigSetup 用 component-base 的通用校验器检查 v1 外部配置包，
// 确保 scheme 注册、类型转换与默认值符合 component config 的约定。
func TestComponentConfigSetup(t *testing.T) {
	pkginfo := &componentconfigtesting.ComponentConfigPackage{
		ComponentName:      "kubelet",
		GroupName:          GroupName,
		SchemeGroupVersion: SchemeGroupVersion,
		AddToScheme:        AddToScheme,
	}

	if err := componentconfigtesting.VerifyExternalTypePackage(pkginfo); err != nil {
		t.Fatal(err)
	}
}
