/*
Copyright 2016 The Kubernetes Authors.

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

package install

import (
	eventv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/apiserver/pkg/server/storage"
	"k8s.io/client-go/informers"
	"k8s.io/kubernetes/pkg/apis/authentication"
	"k8s.io/kubernetes/pkg/apis/authorization"
	"k8s.io/kubernetes/pkg/quota/v1/evaluator/core"
)

// NewQuotaConfigurationForAdmission 创建用于 Admission 控制的配额配置。
// Admission 控制器在对象创建/更新时检查配额，不需要 lister 函数（传入 nil）。
// apiResourceConfig 用于判断 API 资源是否启用。
func NewQuotaConfigurationForAdmission(i informers.SharedInformerFactory, apiResourceConfig storage.APIResourceConfigSource) (quota.Configuration, error) {
	var isEnabled func(schema.GroupVersionResource) bool
	if apiResourceConfig != nil {
		isEnabled = apiResourceConfig.ResourceEnabled
	}
	evaluators, err := core.NewEvaluators(nil, i, isEnabled)
	if err != nil {
		return nil, err
	}
	return generic.NewConfiguration(evaluators, DefaultIgnoredResources()), nil
}

// NewQuotaConfigurationForControllers 创建用于 Controllers 的配额配置。
// Controllers 需要 lister 函数来列出当前资源，计算聚合使用量。
func NewQuotaConfigurationForControllers(f quota.ListerForResourceFunc, i informers.SharedInformerFactory) (quota.Configuration, error) {
	evaluators, err := core.NewEvaluators(f, i, nil)
	if err != nil {
		return nil, err
	}
	return generic.NewConfiguration(evaluators, DefaultIgnoredResources()), nil
}

// ignoredResources 是配额系统默认忽略的资源列表。
var ignoredResources = map[schema.GroupResource]struct{}{
	// 虚拟资源，不存储在 etcd 中，不应计入配额
	{Group: "", Resource: "bindings"}:          {},
	{Group: "", Resource: "componentstatuses"}: {},
	// 认证和授权资源，是请求型的，不存储
	{Group: authentication.GroupName, Resource: "tokenreviews"}:             {},
	{Group: authentication.GroupName, Resource: "selfsubjectreviews"}:       {},
	{Group: authorization.GroupName, Resource: "subjectaccessreviews"}:      {},
	{Group: authorization.GroupName, Resource: "selfsubjectaccessreviews"}:  {},
	{Group: authorization.GroupName, Resource: "localsubjectaccessreviews"}: {},
	{Group: authorization.GroupName, Resource: "selfsubjectrulesreviews"}:   {},

	// 事件资源，历史原因不计入配额
	{Group: "", Resource: "events"}:                {},
	{Group: eventv1.GroupName, Resource: "events"}: {},
}

// DefaultIgnoredResources 返回配额系统默认忽略的资源集合。
// 导出此函数以便下游集成者可以访问和扩展忽略列表。
func DefaultIgnoredResources() map[schema.GroupResource]struct{} {
	return ignoredResources
}
