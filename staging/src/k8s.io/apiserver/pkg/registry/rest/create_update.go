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

package rest

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// RESTCreateUpdateStrategy 是 RESTUpdateStrategy 与 RESTCreateStrategy 的并集，
// 它定义了创建和更新符合 Kubernetes API 约定的对象所需的最小校验、
// 可接受输入与名称生成行为。
//
// 传给这些方法的 context 可用于通过 request.RequestInfoFrom(ctx) 获取请求信息，
// 并根据请求中的 APIVersion 返回不同响应。
// 根据请求中的 APIVersion 返回不同响应。
// request.
//
// 该接口把 create 与 update 的默认化/校验策略合并，便于同时支持两者的资源
// （如大多数 CRD）只实现一套逻辑；同时实现 RESTUpdateStrategy 也意味着
// 需要单独处理 status 子资源等更新路径。
type RESTCreateUpdateStrategy interface {
	RESTCreateStrategy
	// AllowCreateOnUpdate 表示能否通过 PUT 创建对象。
	// 返回 true 时，PUT 到不存在的对象会走 create 路径（upsert 语义）。
	AllowCreateOnUpdate(ctx context.Context) bool
	// PrepareForUpdate 在更新校验前调用，用于归一化对象，
	// 例如删除不应持久化的字段、对顺序无关的列表字段排序等。
	// 但不得删除那些若缺失会被判定为校验错误的字段。
	// 若删除这些字段会被判定为校验错误，则不得删除。
	//
	// 调用顺序保证：先 PrepareForUpdate 再 ValidateUpdate，因此校验看到的是
	// 归一化后的对象；若在校验中做归一化会导致同一请求的校验结果不一致。
	PrepareForUpdate(ctx context.Context, obj, old runtime.Object)
	// ValidateUpdate 在对象默认值填充之后、持久化之前调用。此方法不得修改对象。
	// 填充之后、持久化之前调用。此方法不得修改对象。
	//
	ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList
	// WarningsOnUpdate 返回要发给执行更新操作的客户端的告警。
	// 调用时机在默认值填充与 ValidateUpdate 通过之后、Canonicalize 与持久化之前。
	//
	// 此方法不得修改任一对象。
	//
	// 告警应简短，尽量不超过 120 个字符。
	// 消息中不要包含 "Warning:" 前缀（客户端输出时会自行添加）。
	// 针对具体字段的告警应格式化为 "path.to.field: message"。
	// 例如：`spec.imagePullSecrets[0].name: invalid empty name ""`
	//
	// 告警用于描述发起 API 请求的客户端应当修正或知晓的问题。
	// 例如：
	// - 使用了将在未来版本停止工作的已弃用字段/标签/注解
	// - 使用了已失效的字段/标签/注解
	// - 格式错误或非法的规约，导致提交的对象无法被成功处理，
	//   但出于兼容原因未被校验拒绝
	//
	// 对调用方无法修正的字段不应返回告警。
	// 例如，不要在 status 更新中对 spec 字段发出告警。
	//
	// 警告在 ValidateUpdate 通过之后、Canonicalize 之前产生，因此可以基于
	// 已默认化的字段给出提示；且不得修改对象。
	WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string
	// AllowUnconditionalUpdate 表示当对象未指定 resource version 时，
	// 是否允许无条件更新（不检查最新资源版本）。
	//
	//
	// 这是危险的兼容性开关：关闭时，缺少 resourceVersion 的更新必须报冲突，
	// 以避免并发写互相覆盖。
	AllowUnconditionalUpdate(ctx context.Context) bool
}

// 确保 RESTCreateUpdateStrategy 扩展了 RESTCreateStrategy
var _ RESTCreateStrategy = (RESTCreateUpdateStrategy)(nil)

// 确保 RESTCreateUpdateStrategy 扩展了 RESTUpdateStrategy
var _ RESTUpdateStrategy = (RESTCreateUpdateStrategy)(nil)
