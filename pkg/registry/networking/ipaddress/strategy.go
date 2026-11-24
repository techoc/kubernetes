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

package ipaddress

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/apis/networking/validation"
)

// ipAddressStrategy 定义 IPAddress 的集群级 REST 策略；对象名就是 IP 字符串，因此 NameGenerator 必须原样返回 base。
type ipAddressStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// noopNameGenerator 不生成随机名称后缀，直接返回调用方提供的 base。
type noopNameGenerator struct{}

// IPAddress 不能追加随机后缀，否则会改变其所代表的真实 IP；生成名接口必须直接返回原地址。
func (noopNameGenerator) GenerateName(base string) string {
	return base
}

// Strategy 是创建、更新和删除 IPAddress 对象时使用的默认策略。
var Strategy = ipAddressStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, noopNameGenerator{}}

// 确保 Strategy 实现 rest.RESTCreateStrategy 接口。
var _ rest.RESTCreateStrategy = Strategy

// 确保 Strategy 实现 rest.RESTUpdateStrategy 接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 返回 false，因为 IPAddress 是集群级资源。
func (ipAddressStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建前执行 IPAddress 字段预处理；当前不修改对象内容。
func (ipAddressStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	_ = obj.(*networking.IPAddress)

}

// PrepareForUpdate 预留更新前的字段处理入口；当前不重置或改写字段。
func (ipAddressStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIPAddress := obj.(*networking.IPAddress)
	oldIPAddress := old.(*networking.IPAddress)

	_, _ = newIPAddress, oldIPAddress
}

// Validate 校验新 IPAddress 的名称、地址和 ParentRef 等字段。
func (ipAddressStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ipAddress := obj.(*networking.IPAddress)
	return validation.ValidateIPAddress(ipAddress)
}

// Canonicalize 在校验后规范化 IPAddress；当前无需额外处理。
func (ipAddressStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，IPAddress 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (ipAddressStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对照旧对象校验 IPAddress 更新是否合法，并保护地址身份等不可变字段。
// IPAddress 的地址身份不可随意更换；更新时既执行完整对象校验，也执行旧新对象之间的不可变字段校验。
func (ipAddressStrategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newIPAddress := new.(*networking.IPAddress)
	oldIPAddress := old.(*networking.IPAddress)
	errList := validation.ValidateIPAddress(newIPAddress)
	errList = append(errList, validation.ValidateIPAddressUpdate(newIPAddress, oldIPAddress)...)
	return errList
}

// AllowUnconditionalUpdate 返回 true，允许 IPAddress 更新请求不携带 ResourceVersion。
func (ipAddressStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// WarningsOnCreate 返回创建 IPAddress 时的非阻断性警告；当前不产生额外警告。
func (ipAddressStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// WarningsOnUpdate 返回更新 IPAddress 时的非阻断性警告；当前不产生额外警告。
func (ipAddressStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
