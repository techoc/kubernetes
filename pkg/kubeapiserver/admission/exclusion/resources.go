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

package exclusion

import (
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// included 列出基于表达式的准入控制器应拦截的资源。
// 此处省略版本，同一 GroupResource 的所有版本一视同仁。
// 瞬态资源（即不落盘存储的资源）必须明确出现在 included 或 excluded 列表中，
// 否则拦截器无法确定是否应处理它。
var included = []schema.GroupResource{
	{Group: "", Resource: "bindings"},
	{Group: "", Resource: "pods/attach"},
	{Group: "", Resource: "pods/binding"},
	{Group: "", Resource: "pods/eviction"},
	{Group: "", Resource: "pods/exec"},
	{Group: "", Resource: "pods/portforward"},

	// ref: https://github.com/kubernetes/kubernetes/issues/122205#issuecomment-1927390823
	{Group: "", Resource: "serviceaccounts/token"},
}

// excluded 列出基于表达式的准入控制器应忽略的资源。
// 此处省略版本，同一 GroupResource 的所有版本一视同仁。
var excluded = []schema.GroupResource{
	// BEGIN interception of these non-persisted resources may break the cluster
	{Group: "authentication.k8s.io", Resource: "selfsubjectreviews"},
	{Group: "authentication.k8s.io", Resource: "tokenreviews"},
	{Group: "authorization.k8s.io", Resource: "localsubjectaccessreviews"},
	{Group: "authorization.k8s.io", Resource: "selfsubjectaccessreviews"},
	{Group: "authorization.k8s.io", Resource: "selfsubjectrulesreviews"},
	{Group: "authorization.k8s.io", Resource: "subjectaccessreviews"},
	// END interception of these non-persisted resources may break the cluster
}

// Included 返回应被基于表达式的准入控制器拦截的资源列表副本。
func Included() []schema.GroupResource {
	return slices.Clone(included)
}

// Excluded 返回应被基于表达式的准入控制器忽略的资源列表副本。
func Excluded() []schema.GroupResource {
	return slices.Clone(excluded)
}
