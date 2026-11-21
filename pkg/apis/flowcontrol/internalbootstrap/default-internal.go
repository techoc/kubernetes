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

package internalbootstrap

import (
	flowcontrolv1 "k8s.io/api/flowcontrol/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	"k8s.io/kubernetes/pkg/apis/flowcontrol/install"
)

// MandatoryFlowSchemas 保存强制 flow schema 的内部类型（无版本）表示。
// 这个 map 的 key 是 schema 名称，value 是对应的 `*FlowSchema`。
// 任何人都不能修改这个 map 中可达的任何对象，因为这些对象是共享的只读配置。
var MandatoryFlowSchemas = internalizeFSes(bootstrap.MandatoryFlowSchemas)

// MandatoryPriorityLevelConfigurations 保存强制优先级级别配置的内部类型（无版本）表示。
// 这个 map 的 key 是对象名称，value 是对应的 `*PriorityLevelConfiguration`。
// 任何人都不能修改这个 map 中可达的任何对象，因为这些对象是共享的只读配置。
var MandatoryPriorityLevelConfigurations = internalizePLs(bootstrap.MandatoryPriorityLevelConfigurations)

// NewAPFScheme 构造并返回一个配置好的 Scheme，
// 用于处理配置 API 优先级和公平性（APF）所需的 API 对象类型。
func NewAPFScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	install.Install(scheme)
	return scheme
}

// internalizeFSes 把版本化的 FlowSchema 列表转换为内部类型，
// 并以对象名称为 key 返回一个 map，供内部代码只读使用。
func internalizeFSes(exts []*flowcontrolv1.FlowSchema) map[string]*flowcontrol.FlowSchema {
	ans := make(map[string]*flowcontrol.FlowSchema, len(exts))
	scheme := NewAPFScheme()
	for _, ext := range exts {
		var untyped flowcontrol.FlowSchema
		// 转换失败意味着 bootstrap 数据与内部类型不兼容，属于编程错误，直接 panic。
		if err := scheme.Convert(ext, &untyped, nil); err != nil {
			panic(err)
		}
		ans[ext.Name] = &untyped
	}
	return ans
}

// internalizePLs 把版本化的 PriorityLevelConfiguration 列表转换为内部类型，
// 并以对象名称为 key 返回一个 map，供内部代码只读使用。
func internalizePLs(exts []*flowcontrolv1.PriorityLevelConfiguration) map[string]*flowcontrol.PriorityLevelConfiguration {
	ans := make(map[string]*flowcontrol.PriorityLevelConfiguration, len(exts))
	scheme := NewAPFScheme()
	for _, ext := range exts {
		var untyped flowcontrol.PriorityLevelConfiguration
		// 转换失败意味着 bootstrap 数据与内部类型不兼容，属于编程错误，直接 panic。
		if err := scheme.Convert(ext, &untyped, nil); err != nil {
			panic(err)
		}
		ans[ext.Name] = &untyped
	}
	return ans
}
