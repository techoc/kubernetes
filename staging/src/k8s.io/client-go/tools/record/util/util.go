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

package util

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
)

// ValidateEventType 检查 eventtype 是否为预期的事件类型。
//
// 仅 Normal 与 Warning 是合法的 Event 类型，其余一律拒绝，
// 防止写入无法被 Event 消费者正确分类的记录。
func ValidateEventType(eventtype string) bool {
	switch eventtype {
	case v1.EventTypeNormal, v1.EventTypeWarning:
		return true
	}
	return false
}

// IsKeyNotFoundError 是检查错误是否为 not found 错误的工具函数。
//
// 只识别 *errors.StatusError 且 HTTP 码为 404 的情况；其他错误形态
// （如普通 error 或非 Status 的包装错误）都视为非 NotFound。
func IsKeyNotFoundError(err error) bool {
	statusErr, _ := err.(*errors.StatusError)

	return statusErr != nil && statusErr.Status().Code == http.StatusNotFound
}

// GenerateEventName 由引用对象名和传入的 UNIX 时间戳生成合法的 Event 名。
// 引用对象的名称可能不是合法的 Event 名称、导致 Event 创建失败，
// 这种情况下需要重新生成一个名字。
// 参见：https://issues.k8s.io/127594
//
// GenerateEventName 用“引用对象名.十六进制纳秒时间戳”构造 Event 名；
// Event 名同样是 DNS 子域名，若引用对象名含非法字符则退化为随机 UUID，
// 保证 Event 一定能创建成功（代价是名字不再可读）。
func GenerateEventName(refName string, unixNano int64) string {
	name := fmt.Sprintf("%s.%x", refName, unixNano)
	if errs := apimachineryvalidation.NameIsDNSSubdomain(name, false); len(errs) > 0 {
		// 使用 UUID 可以同时保证唯一性和合法性
		name = uuid.New().String()
	}
	return name
}
