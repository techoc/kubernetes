/*
Copyright 2014 The Kubernetes Authors.

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

package responsewriters

import (
	"fmt"
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// statusError 是可以转换为 metav1.Status 的对象
//
// statusError 是所有能自描述为 API Status 的错误的最小接口；
// *apierrors.StatusError 等类型天然满足它。
type statusError interface {
	Status() metav1.Status
}

// ErrorToAPIStatus 将错误转换为 metav1.Status 对象。
//
// ErrorToAPIStatus 保证 HTTP 响应体始终是合法且字段完整的 metav1.Status：
// 缺失的 Status/Code 会补默认值，非 Status 错误统一转 500（冲突除外），
// 避免客户端收到无法解析或含义模糊的错误。
func ErrorToAPIStatus(err error) *metav1.Status {
	switch t := err.(type) {
	case statusError:
		status := t.Status()
		// Status 字段为空时按失败处理，不允许把未标注的错误当作成功返回。
		if len(status.Status) == 0 {
			status.Status = metav1.StatusFailure
		}
		switch status.Status {
		case metav1.StatusSuccess:
			if status.Code == 0 {
				status.Code = http.StatusOK
			}
		case metav1.StatusFailure:
			if status.Code == 0 {
				status.Code = http.StatusInternalServerError
			}
		default:
			// 非法 Status 值属于实现错误，记录后仍返回合法的 500。
			runtime.HandleError(fmt.Errorf("apiserver received an error with wrong status field : %#+v", err))
			if status.Code == 0 {
				status.Code = http.StatusInternalServerError
			}
		}
		// 序列化契约要求固定的 Kind/APIVersion，客户端依赖它们反序列化。
		status.Kind = "Status"
		status.APIVersion = "v1"
		// TODO：检查无效的响应
		return &status
	default:
		// 未包装为 Status 的错误通常意味着未使用 pkg/api/errors，
		// 默认 500；存储层冲突是少数可映射为 409 的例外。
		status := http.StatusInternalServerError
		switch {
		// TODO：用 NewConflictErr 替换这里
		case storage.IsConflict(err):
			status = http.StatusConflict
		}
		// 记录未被 REST 存储层转换为错误状态的错误：这些通常表明
		// 未使用 pkg/api/errors 的编程错误，或意料之外的失败场景。
		runtime.HandleError(fmt.Errorf("apiserver received an error that is not an metav1.Status: %#+v: %v", err, err))
		return &metav1.Status{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Status",
				APIVersion: "v1",
			},
			Status:  metav1.StatusFailure,
			Code:    int32(status),
			Reason:  metav1.StatusReasonUnknown,
			Message: err.Error(),
		}
	}
}
