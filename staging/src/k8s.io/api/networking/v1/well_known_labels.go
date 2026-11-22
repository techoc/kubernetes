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

package v1

const (

	// TODO: Use IPFamily as field with a field selector,And the value is set based on
	// the name at create time and immutable.
	// LabelIPAddressFamily 用于标识 Kubernetes IPAddress 的 IP 地址族。
	// 该标签简化双栈客户端操作，允许按地址族过滤获取 IP 地址列表。
	LabelIPAddressFamily = "ipaddress.kubernetes.io/ip-family"
	// LabelManagedBy 用于标识管理某个 IPAddress 的控制器或实体。
	// 该标签旨在让同一集群内的不同 IPAddress 对象由不同的控制器或实体管理。
	// 强烈建议为所有 IPAddress 对象配置此标签。
	LabelManagedBy = "ipaddress.kubernetes.io/managed-by"
)
