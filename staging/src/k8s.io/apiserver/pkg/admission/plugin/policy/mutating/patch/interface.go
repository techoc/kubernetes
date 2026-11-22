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

package patch

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
)

// Patcher provides a patch function to perform a mutation to an object in the admission chain.
type Patcher interface {
	// Patch returns a copy of the object in the request, modified to change specified by the patch.
	// The original object in the request MUST NOT be modified in-place.
	//
	// 返回副本而非原地修改，是因为同一对象可能被后续插件、审计与响应序列化共享；
	// 原地修改会让失败回滚变得不可能。runtimeCELCostBudget 限制 CEL 求值开销，
	// 防止恶意/低效表达式拖垮 API server。
	Patch(ctx context.Context, request Request, runtimeCELCostBudget int64) (runtime.Object, error)
}

// Request defines the arguments required by a patcher.
type Request struct {
	// MatchedResource 是策略匹配到的资源，用于构造 apply 配置与字段管理所需的信息。
	MatchedResource schema.GroupVersionResource
	// VersionedAttributes 携带请求对象、旧对象与操作类型，补丁在版本化视图上计算。
	VersionedAttributes *admission.VersionedAttributes
	// ObjectInterfaces 提供类型转换能力，用于在版本化对象与内部对象之间转换。
	ObjectInterfaces admission.ObjectInterfaces
	// OptionalVariables 是可选的 CEL 绑定（如 params、authorizer），由匹配阶段传入。
	OptionalVariables cel.OptionalVariableBindings
	// Namespace 为请求所在命名空间对象，用于命名空间作用域的策略参数。
	Namespace *v1.Namespace
	// TypeConverter 用于在服务器端 apply 语义下解析字段所有权。
	TypeConverter managedfields.TypeConverter
}
