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

package cache

import (
	"fmt"

	"k8s.io/client-go/tools/cache"
)

// Conflict 表示两个 Pod 使用同一卷但 SELinux 标签或策略不同而产生的一次冲突。
// 应当为双方 Pod 都发送相应事件。
type Conflict struct {
	// PropertyName 是冲突属性的可读名称，同时作为 selinux_volume_conflict 指标 "property" 标签的取值。
	PropertyName string
	// EventReason 是事件原因，用作 Event.Reason 字段。
	EventReason string

	// Pod 是要为其生成事件的目标 Pod。
	Pod           cache.ObjectName
	PropertyValue string
	// OtherPod 与 OtherPropertyValue 仅用于日志与消息文案。
	OtherPod           cache.ObjectName
	OtherPropertyValue string
}

// EventMessage 生成描述该冲突的可读消息。
func (c *Conflict) EventMessage() string {
	// 给属性值加引号，提升可读性。
	value := "\"" + c.PropertyValue + "\""
	otherValue := "\"" + c.OtherPropertyValue + "\""
	if c.Pod.Namespace == c.OtherPod.Namespace {
		// 同一命名空间内可以直接点明对方 Pod 名称，信息足够具体。
		return fmt.Sprint(c.PropertyName, " ", value, " conflicts with pod ", c.OtherPod.Name, " that uses the same volume as this pod with ", c.PropertyName, " ", otherValue, ". If both pods land on the same node, only one of them may access the volume.")
	}
	// 两个 Pod 分属不同命名空间，不能泄露对方的命名空间或 Pod 名称。
	return fmt.Sprint(c.PropertyName, value, " conflicts with another pod that uses the same volume as this pod with a different ", c.PropertyName, ". If both pods land on the same node, only one of them may access the volume.")
}
