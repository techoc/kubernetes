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

package webhook

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ErrCallingWebhook 用于调用 webhook 时的传输层错误。它
// 表示无法与 webhook 通信，而非 webhook 拒绝了
// 请求。
//
// 区分“调用失败”和“webhook 明确拒绝”很重要：前者受 failurePolicy 影响，
// 后者是正常的准入决策。
type ErrCallingWebhook struct {
	WebhookName string
	Reason      error
	Status      *apierrors.StatusError
}

// Error 返回包含 webhook 名称的错误信息；没有底层原因时明确说明，
// 避免日志出现空洞的 "failed calling webhook"。
func (e *ErrCallingWebhook) Error() string {
	if e.Reason != nil {
		return fmt.Sprintf("failed calling webhook %q: %v", e.WebhookName, e.Reason)
	}
	return fmt.Sprintf("failed calling webhook %q; no further details available", e.WebhookName)
}

// ErrWebhookRejection 表示 webhook 正常拒绝了请求。
type ErrWebhookRejection struct {
	Status *apierrors.StatusError
}

// Error 直接透传 webhook 返回的状态信息，保留其原始 reason/message。
func (e *ErrWebhookRejection) Error() string {
	return e.Status.Error()
}
