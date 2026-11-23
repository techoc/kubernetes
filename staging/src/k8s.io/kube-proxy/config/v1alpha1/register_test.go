/*
Copyright 2019 The Kubernetes Authors.

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

package v1alpha1

import (
	"testing"

	componentconfigtesting "k8s.io/component-base/config/testing"
)

// TestComponentConfigSetup 校验本包符合 component-base 对组件配置 API 包的约定。
//
// 这类配置包的结构高度模板化：组名、组版本、SchemeBuilder/AddToScheme、已知类型，
// 漏掉任何一项都可能在运行时才表现为「无法解码配置」。把约定写成测试，
// 可以在改动注册代码时尽早发现问题，而不必等到 kube-proxy 启动。
func TestComponentConfigSetup(t *testing.T) {
	pkginfo := &componentconfigtesting.ComponentConfigPackage{
		ComponentName:      "kube-proxy",
		GroupName:          GroupName,
		SchemeGroupVersion: SchemeGroupVersion,
		AddToScheme:        AddToScheme,
	}

	if err := componentconfigtesting.VerifyExternalTypePackage(pkginfo); err != nil {
		t.Errorf("failed TestComponentConfigSetup: %v", err)
	}
}
