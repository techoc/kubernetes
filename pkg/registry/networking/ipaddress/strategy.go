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

// noopNameGenerator does not generate names, it just returns the base.
type noopNameGenerator struct{}

// IPAddress 不能追加随机后缀，否则会改变其所代表的真实 IP；生成名接口必须直接返回原地址。
func (noopNameGenerator) GenerateName(base string) string {
	return base
}

// Strategy is the default logic that applies when creating and updating Replication IPAddress objects.
var Strategy = ipAddressStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, noopNameGenerator{}}

// Strategy should implement rest.RESTCreateStrategy
var _ rest.RESTCreateStrategy = Strategy

// Strategy should implement rest.RESTUpdateStrategy
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped returns false because all IPAddresses is cluster scoped.
func (ipAddressStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate clears the status of an IPAddress before creation.
// 创建阶段不补默认值或改名；IPAddress 的地址和 ParentRef 由分配控制器按服务地址池规则写入。
func (ipAddressStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	_ = obj.(*networking.IPAddress)

}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
func (ipAddressStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIPAddress := obj.(*networking.IPAddress)
	oldIPAddress := old.(*networking.IPAddress)

	_, _ = newIPAddress, oldIPAddress
}

// Validate validates a new IPAddress.
// Validate 检查对象名称是否为合法 IP 地址，以及 ParentRef/地址族等字段是否符合 API 约束。
func (ipAddressStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ipAddress := obj.(*networking.IPAddress)
	return validation.ValidateIPAddress(ipAddress)
}

// Canonicalize normalizes the object after validation.
func (ipAddressStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate is false for IPAddress; this means POST is needed to create one.
func (ipAddressStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate is the default update validation for an end user.
// IPAddress 的地址身份不可随意更换；更新时既执行完整对象校验，也执行旧新对象之间的不可变字段校验。
func (ipAddressStrategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newIPAddress := new.(*networking.IPAddress)
	oldIPAddress := old.(*networking.IPAddress)
	errList := validation.ValidateIPAddress(newIPAddress)
	errList = append(errList, validation.ValidateIPAddressUpdate(newIPAddress, oldIPAddress)...)
	return errList
}

// AllowUnconditionalUpdate is the default update policy for IPAddress objects.
func (ipAddressStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (ipAddressStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// WarningsOnUpdate returns warnings for the given update.
func (ipAddressStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
