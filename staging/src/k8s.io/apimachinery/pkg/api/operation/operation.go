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

// Package operation 定义校验时所需的请求上下文：操作类型（创建/更新）、
// 生效的校验选项以及子资源路径。这些信息由资源策略填充，供生成的校验代码读取。
package operation

import (
	"strings"
)

// Operation 描述一次校验请求的上下文及被校验的 API 操作。
// 该类型专供生成的校验代码使用，未来可能扩展更多校验所需的信息。
type Operation struct {
	// Type 是被校验操作的类别。它不区分 PUT 与 PATCH 等 HTTP 动词，
	// 而是把它们合并为单一的 "Update" 类别。
	Type Type

	// Options 是本操作生效的校验选项，映射“选项名 -> 是否启用”。
	// 选项名通常与特性门控一致，但选项可能在其门控关闭时仍处于启用状态——
	// 例如该特性已被正在更新的对象使用。由资源策略设置，校验期间只读。
	//
	// 校验标签引用的每个选项都必须由策略在此定义；未定义的选项属于编程错误
	// （参见 HasOption）。
	Options map[string]bool

	// Request 提供被校验请求的相关信息。
	Request Request
}

// HasOption 返回命名选项是否启用，以及它是否由策略定义。
// 校验标签引用的每个选项都必须被定义；调用方会把未定义的选项当作内部错误
// （参见 validate.IfOption），而不是静默地按“未启用”处理。
func (o Operation) HasOption(option string) (enabled, defined bool) {
	enabled, defined = o.Options[option]
	return
}

// Request 提供被校验请求的相关信息。
type Request struct {
	// Subresources 标识请求的子资源路径分段。例如访问
	// `/api/v1/pods/my-pod/status` 时，Subresources 为 `["status"]`；
	// 访问 `/api/v1/widget/my-widget/x/y/z` 时为 `["x", "y", "z"]`；
	// 访问根资源（`/api/v1/pods/my-pod`）时为空切片。
	//
	// 只有当某字段的校验规则因“主资源”与“特定子资源”不同而不同时，
	// 校验逻辑才应查询该字段。例如：
	//
	// 更新 Pod（`/`）时通常不能在创建后修改容器资源 requests/limits（不可变）；
	// 但访问 Pod 的 "resize" 子资源（`/resize`）时，这些特定字段允许修改。
	// 因此对 `spec.container[*].resources` 的校验逻辑必须检查 `Subresources`，
	// 仅在请求目标为 "resize" 子资源时放行修改。
	//
	// 注意：该字段不应被用来控制子资源操作允许写哪些字段，那是“字段擦除
	// （field wiping）”的职责。字段擦除应由资源策略在对象进入校验前修改对象完成。
	Subresources []string
}

// SubresourcePath 返回以斜杠分隔的子资源名列表。
// 例如 `/status`、`/resize` 或 `/x/y/z`；根资源返回 `/`。
func (r Request) SubresourcePath() string {
	if len(r.Subresources) == 0 {
		return "/"
	}
	return "/" + strings.Join(r.Subresources, "/")
}

// Type 是被校验的请求操作。
type Type uint32

const (
	// Create 表示被校验的请求是资源创建操作。
	Create Type = iota

	// Update 表示被校验的请求是资源更新操作。
	Update
)
