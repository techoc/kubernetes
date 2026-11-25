/*
Copyright 2017 The Kubernetes Authors.

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

package errors

import (
	"fmt"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ToStatusErr 返回带有 webhook 插件相关信息的 StatusError。
//
// 入参 result 可能来自不受信任的 webhook，因此这里会把状态码提升到 4xx
// 以上、把状态强制为 Failure，并统一加上 “denied the request” 前缀，
// 防止 webhook 用 2xx/3xx 表达拒绝而误导客户端。
func ToStatusErr(webhookName string, result *metav1.Status) *apierrors.StatusError {
	deniedBy := fmt.Sprintf("admission webhook %q denied the request", webhookName)
	const noExp = "without explanation"

	// webhook 未给出状态时，按失败处理。
	if result == nil {
		result = &metav1.Status{Status: metav1.StatusFailure}
	}

	// 确保拒绝请求时不会返回 < 400 的状态码
	if result.Code < http.StatusBadRequest {
		result.Code = http.StatusBadRequest
	}
	// 确保拒绝请求时不会返回 "" 或 "Success" 状态
	if result.Status == "" || result.Status == metav1.StatusSuccess {
		result.Status = metav1.StatusFailure
	}

	// 保留 webhook 自带的说明，但强制加上来源前缀，便于排查是哪个 webhook 拒绝的。
	switch {
	case len(result.Message) > 0:
		result.Message = fmt.Sprintf("%s: %s", deniedBy, result.Message)
	case len(result.Reason) > 0:
		result.Message = fmt.Sprintf("%s: %s", deniedBy, result.Reason)
	default:
		result.Message = fmt.Sprintf("%s %s", deniedBy, noExp)
	}

	return &apierrors.StatusError{
		ErrStatus: *result,
	}
}

// NewDryRunUnsupportedErr 返回带有 webhook 插件相关信息的 StatusError。
//
// dry-run 请求不会真正持久化，webhook 声明不支持 dry-run 时必须直接返回 400，
// 而不是静默放行，否则会掩盖 dry-run 的副作用检查语义。
func NewDryRunUnsupportedErr(webhookName string) *apierrors.StatusError {
	reason := fmt.Sprintf("admission webhook %q does not support dry run", webhookName)
	return apierrors.NewBadRequest(reason)
}
