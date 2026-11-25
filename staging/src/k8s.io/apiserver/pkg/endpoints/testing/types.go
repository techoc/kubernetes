/*
Copyright 2015 The Kubernetes Authors.

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

package testing

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Simple 是最小的命名空间级测试资源，仅含元数据、一个字符串字段与标签。
type Simple struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata"`
	// +optional
	Other string `json:"other,omitempty"`
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SimpleRoot 与 Simple 字段相同，但用于测试集群级（root）资源的 handler 路径。
type SimpleRoot struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata"`
	// +optional
	Other string `json:"other,omitempty"`
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SimpleGetOptions 用于测试 GET 查询参数解析；Path 的 JSON 名刻意与字段名
// 不同，以覆盖按 tag 取参数的逻辑。
type SimpleGetOptions struct {
	metav1.TypeMeta `json:""`
	Param1          string `json:"param1"`
	Param2          string `json:"param2"`
	Path            string `json:"atAPath"`
}

// SwaggerDoc 返回手写的参数描述，测试 OpenAPI 文档生成是否合并该结果。
func (SimpleGetOptions) SwaggerDoc() map[string]string {
	return map[string]string{
		"param1": "description for param1",
		"param2": "description for param2",
	}
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SimpleList 是 Simple 的列表形态，用于测试 list 响应的编码与分页元数据。
type SimpleList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,inline"`
	// +optional
	Items []Simple `json:"items,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SimpleXGSubresource 是跨组子资源（cross group subresource），即子资源不属于其父资源所在的 API 组。
// 该类型用于验证“子资源与父资源属于不同 API 组”时的路由与鉴权。
type SimpleXGSubresource struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata"`
	SubresourceInfo   string            `json:"subresourceInfo,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
}
