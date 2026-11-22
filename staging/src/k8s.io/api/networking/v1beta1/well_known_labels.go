/*
Copyright 2023 The Kubernetes Authors.

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

package v1beta1

const (

	// TODO: 改用 IPFamily 字段并支持字段选择器，其值在创建时根据名称设定且不可变
	//
	// LabelIPAddressFamily 用于标识 Kubernetes IPAddress 的 IP 地址族
	// 该标签简化双栈客户端操作，可按地址族过滤获取 IP 地址列表
	//
	LabelIPAddressFamily = "ipaddress.kubernetes.io/ip-family"
	// LabelManagedBy 用于标识管理某个 IPAddress 的控制器或实体
	// 该标签使同一集群中的不同 IPAddress 对象可以由不同的控制器或实体管理。
	//
	// 强烈建议为所有 IPAddress 对象设置该标签
	//
	LabelManagedBy = "ipaddress.kubernetes.io/managed-by"
)
