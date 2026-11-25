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

// Package app 组装 Kubernetes API 服务器所需的 API、控制平面和服务端基础设施。
// 调用方既可直接配置和运行，也可通过 hyperkube 框架接入。
package app

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
)

// 让聚合器感知各后端服务器的 API Group 及组内版本顺序，理想做法是让
// genericapiserver.DelegationTarget 携带按已安装 API 计算的优先级列表。这会要求
// APIGroupInfo 引入优先级概念并同步更新核心存储映射，影响面较大；因此 1.7
// 仍在此显式维护优先级，而不是在存储初始化时动态生成。
var apiVersionPriorities = merge(controlplaneapiserver.DefaultGenericAPIServicePriorities(), map[schema.GroupVersion]controlplaneapiserver.APIServicePriority{
	{Group: "", Version: "v1"}: {Group: 18000, Version: 1},
	// 据我所知，以下优先级不会与默认配置冲突。
	{Group: "apps", Version: "v1"}:                    {Group: 17800, Version: 15},
	{Group: "autoscaling", Version: "v1"}:             {Group: 17500, Version: 15},
	{Group: "autoscaling", Version: "v2"}:             {Group: 17500, Version: 30},
	{Group: "autoscaling", Version: "v2beta1"}:        {Group: 17500, Version: 9},
	{Group: "autoscaling", Version: "v2beta2"}:        {Group: 17500, Version: 1},
	{Group: "batch", Version: "v1"}:                   {Group: 17400, Version: 15},
	{Group: "batch", Version: "v1beta1"}:              {Group: 17400, Version: 9},
	{Group: "batch", Version: "v2alpha1"}:             {Group: 17400, Version: 9},
	{Group: "networking.k8s.io", Version: "v1"}:       {Group: 17200, Version: 15},
	{Group: "networking.k8s.io", Version: "v1beta1"}:  {Group: 17200, Version: 9},
	{Group: "policy", Version: "v1"}:                  {Group: 17100, Version: 15},
	{Group: "policy", Version: "v1beta1"}:             {Group: 17100, Version: 9},
	{Group: "storage.k8s.io", Version: "v1"}:          {Group: 16800, Version: 15},
	{Group: "storage.k8s.io", Version: "v1beta1"}:     {Group: 16800, Version: 9},
	{Group: "storage.k8s.io", Version: "v1alpha1"}:    {Group: 16800, Version: 1},
	{Group: "scheduling.k8s.io", Version: "v1"}:       {Group: 16600, Version: 15},
	{Group: "scheduling.k8s.io", Version: "v1beta1"}:  {Group: 16600, Version: 9},
	{Group: "scheduling.k8s.io", Version: "v1alpha3"}: {Group: 16600, Version: 3},
	{Group: "node.k8s.io", Version: "v1"}:             {Group: 16300, Version: 15},
	{Group: "node.k8s.io", Version: "v1alpha1"}:       {Group: 16300, Version: 1},
	{Group: "node.k8s.io", Version: "v1beta1"}:        {Group: 16300, Version: 9},
	{Group: "resource.k8s.io", Version: "v1"}:         {Group: 16200, Version: 21},
	{Group: "resource.k8s.io", Version: "v1beta2"}:    {Group: 16200, Version: 15},
	{Group: "resource.k8s.io", Version: "v1beta1"}:    {Group: 16200, Version: 9},
	{Group: "resource.k8s.io", Version: "v1alpha3"}:   {Group: 16200, Version: 1},
	{Group: "lifecycle.k8s.io", Version: "v1alpha1"}:  {Group: 15700, Version: 1},
	// 不确定优先级时追加到列表末尾；新 Group 可先用 min(existing group)-100。
	// 新 Group 的 Version 可先设为 9，为后续版本预留空间。
})

func merge(a, b map[schema.GroupVersion]controlplaneapiserver.APIServicePriority) map[schema.GroupVersion]controlplaneapiserver.APIServicePriority {
	for k, v := range b {
		a[k] = v
	}
	return a
}
