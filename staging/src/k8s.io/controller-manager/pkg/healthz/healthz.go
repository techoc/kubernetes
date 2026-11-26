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

package healthz

import (
	"net/http"

	"k8s.io/apiserver/pkg/server/healthz"
)

// 返回一个带名字的“永远健康”检查器（ping），常用于占位或测试。
func NamedPingChecker(name string) healthz.HealthChecker {
	return NamedHealthChecker(name, healthz.PingHealthz)
}

// 给未命名检查器套上名字，使其符合 apiserver healthz 的 HealthChecker 接口。
// 控制器通常只实现 Check 方法，名字由 manager 统一指定。
func NamedHealthChecker(name string, check UnnamedHealthChecker) healthz.HealthChecker {
	return healthz.NamedCheck(name, check.Check)
}

// 未命名健康检查器：控制器只需实现检查逻辑，健康检查的名字由 controller-manager 决定。
type UnnamedHealthChecker interface {
	Check(req *http.Request) error
}

// 编译期断言：HealthChecker 一定满足 UnnamedHealthChecker（都含 Check 方法），
// 这样任何标准健康Checker 都能直接作为 UnnamedHealthChecker 使用。
var _ UnnamedHealthChecker = (healthz.HealthChecker)(nil)
