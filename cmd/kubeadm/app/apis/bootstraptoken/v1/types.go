/*
Copyright 2021 The Kubernetes Authors.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BootstrapToken 描述存储在集群 Secret 中的一个引导令牌。
// +k8s:deepcopy-gen=true
type BootstrapToken struct {
	// Token 用于在节点与控制平面之间建立双向信任，并供节点加入集群。
	Token *BootstrapTokenString `json:"token" datapolicy:"token"`
	// Description 设置易于理解的说明，描述令牌存在的原因和用途，便于其他管理员了解。
	// +optional
	Description string `json:"description,omitempty"`
	// TTL 定义令牌的有效期，默认为 24 小时。
	// Expires 与 TTL 互斥。
	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty"`
	// Expires 指定令牌到期的时间戳。默认根据 TTL 在运行时动态设置。
	// Expires 与 TTL 互斥。
	// +optional
	Expires *metav1.Time `json:"expires,omitempty"`
	// Usages 描述令牌的可用方式。默认可用于建立双向信任，可在此修改。
	// +optional
	Usages []string `json:"usages,omitempty"`
	// Groups 指定令牌用于认证时所属的额外组。
	// +optional
	Groups []string `json:"groups,omitempty"`
}

// BootstrapTokenString 是格式为 abcdef.abcdef0123456789 的短期令牌。
// 加入集群的节点既用它验证 API Server 的可用性，也在 "kubeadm join" 引导阶段用它进行认证。
type BootstrapTokenString struct {
	// ID 是令牌中点号前的标识部分，不参与 JSON 序列化。
	ID string `json:"-"`
	// Secret 是令牌中点号后的秘密部分，不序列化且必须按敏感数据处理。
	Secret string `json:"-" datapolicy:"token"`
}
