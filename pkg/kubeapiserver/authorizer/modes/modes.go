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

package modes

import "k8s.io/apimachinery/pkg/util/sets"

const (
	// ModeAlwaysAllow 表示放行所有请求。
	ModeAlwaysAllow string = "AlwaysAllow"
	// ModeAlwaysDeny 表示拒绝所有请求。
	ModeAlwaysDeny string = "AlwaysDeny"
	// ModeABAC 表示使用基于属性的访问控制（ABAC）进行授权。
	ModeABAC string = "ABAC"
	// ModeWebhook 表示通过调用外部 webhook 进行授权。
	ModeWebhook string = "Webhook"
	// ModeRBAC 表示使用基于角色的访问控制（RBAC）进行授权。
	ModeRBAC string = "RBAC"
	// ModeNode 是用于授权 kubelet 发出的 API 请求的授权模式。
	ModeNode string = "Node"
)

// AuthorizationModeChoices 列出所有受支持的授权模式。
var AuthorizationModeChoices = []string{ModeAlwaysAllow, ModeAlwaysDeny, ModeABAC, ModeWebhook, ModeRBAC, ModeNode}

// IsValidAuthorizationMode 判断给定的授权模式是否为 apiserver 支持的有效模式。
func IsValidAuthorizationMode(authzMode string) bool {
	return sets.NewString(AuthorizationModeChoices...).Has(authzMode)
}
