/*
Copyright 2018 The Kubernetes Authors.

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

// Package util 提供 webhook 准入插件共用的小工具，目前只包含 AdmissionReview
// 版本协商的判断逻辑。
package util

import (
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
)

// HasAdmissionReviewVersion check whether a version is accepted by a given webhook.
//
// 版本协商是双向的：apiserver 发送的 AdmissionReview 版本必须是 webhook 在
// admissionReviewVersions 中声明支持的版本。webhook 配置可能包含多个版本，
// 调用方据此挑选一个双方都支持的版本；都不支持时请求无法发送。
func HasAdmissionReviewVersion(a string, w webhook.WebhookAccessor) bool {
	for _, b := range w.GetAdmissionReviewVersions() {
		if b == a {
			return true
		}
	}
	return false
}
