/*
Copyright 2023 The Kubernetes Authors.

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

package app

import (
	"regexp"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cloud-provider/names"
)

// TestCloudControllerNamesConsistency 校验所有注册控制器的名称都符合
// “小写字母/连字符、-controller 后缀” 的命名约定，避免名称漂移破坏 flag 与别名解析。
func TestCloudControllerNamesConsistency(t *testing.T) {
	controllerNameRegexp := regexp.MustCompile("^[a-z]([-a-z]*[a-z])?$")

	for name := range DefaultInitFuncConstructors {
		// 名称会出现在命令行 flag 和事件中，必须先满足正则约束。
		if !controllerNameRegexp.MatchString(name) {
			t.Errorf("name consistency check failed: controller %q must consist of lower case alphabetic characters or '-', and must start and end with an alphabetic character", name)
		}
		if !strings.HasSuffix(name, "-controller") {
			t.Errorf("name consistency check failed: controller %q must have \"-controller\" suffix", name)
		}
	}
}

// TestCloudControllerNamesDeclaration 确保每个注册的控制器名称都在 names 包中
// 声明为常量；新增控制器时若忘记更新 controller_names.go，该测试会失败。
func TestCloudControllerNamesDeclaration(t *testing.T) {
	declaredControllers := sets.New(
		names.CloudNodeController,
		names.ServiceLBController,
		names.NodeRouteController,
		names.CloudNodeLifecycleController,
	)

	for name := range DefaultInitFuncConstructors {
		if !declaredControllers.Has(name) {
			t.Errorf("name declaration check failed: controller name %q should be declared in  \"controller_names.go\" and added to this test", name)
		}
	}
}
