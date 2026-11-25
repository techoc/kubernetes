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

// Package experimental 提供 DRA 实验版结构化分配器的核心实现。
// 本文件定义约束匹配所需的属性访问接口，以及“属性互异”约束的实现。
package experimental

import (
	"fmt"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/klog/v2"
)

// attributeProvider 定义约束如何检索设备属性：给定请求、设备与属性名，返回属性值或错误。
type attributeProvider interface {
	// lookupAttribute 在给定请求与设备上按属性名查找属性值，失败时返回错误。
	lookupAttribute(request *requestData, device *draapi.Device, deviceID DeviceID, attributeName resourceapi.FullyQualifiedName) (*resourceapi.DeviceAttribute, error)
}

// distinctAttributeConstraint 要求所有相关设备的指定属性值互不相同：设备集合为空时任何拥有该属性的设备都可加入，此后只允许属性值不与已有设备重复的设备加入。
// 无需记录具体是哪些设备，只记录已接纳设备的属性值（其数量）。
// 字段：logger 为预置日志器；requestNames 为受影响的请求名集合（空表示全部）；attributeName 为属性全名。
// features 决定使用列表还是标量比较语义；attributeProvider 用于解析属性值；attributes 为已接纳设备的属性值列表。
type distinctAttributeConstraint struct {
	logger            klog.Logger // 已包含名称与属性名，日志消息中无需重复。
	requestNames      sets.Set[string]
	attributeName     resourceapi.FullyQualifiedName
	features          Features
	attributeProvider attributeProvider

	attributes []*resourceapi.DeviceAttribute
}

// add 在分配一个设备前检查其是否满足“属性互异”约束：属性缺失或与其它设备重复则拒绝，否则记录该属性值并返回成功。
func (m *distinctAttributeConstraint) add(request *requestData, device *draapi.Device, deviceID DeviceID) (bool, error) {
	// 若约束仅针对特定请求而当前请求不在其中，则该设备不受约束影响，直接接纳。
	if m.requestNames.Len() > 0 && !m.matches(request) {
		return true, nil
	}

	// 通过 provider 解析属性值（可能是静态属性或 CEL 派生属性）；解析失败直接返回错误。
	attribute, err := m.attributeProvider.lookupAttribute(request, device, deviceID, m.attributeName)
	if err != nil {
		return false, err
	}
	// 设备没有该属性，视为不满足约束。
	if attribute == nil {
		m.logger.V(7).Info("Constraint not satisfied, attribute not set")
		return false, nil
	}

	// 属性值与已接纳设备重复（或与已有集合有交集），不满足约束。
	if !m.matchesAttribute(*attribute) {
		m.logger.V(7).Info("Constraint not satisfied, has some duplicated attributes")
		return false, nil
	}
	// 满足约束，记录属性值并返回成功。
	m.attributes = append(m.attributes, attribute)
	m.logger.V(7).Info("Constraint satisfied by device", "device", deviceID, "numDevices", len(m.attributes))
	return true, nil
}

// remove 撤销 add 对约束状态的修改；调用方保证每次成功的 add 都有一次参数完全相同的 remove。
func (m *distinctAttributeConstraint) remove(request *requestData, device *draapi.Device, deviceID DeviceID) {
	// 不受约束影响的请求无需回滚。
	if m.requestNames.Len() > 0 && !m.matches(request) {
		return
	}

	// 弹出最近一次记录的属性值，恢复约束状态。
	m.attributes = m.attributes[:len(m.attributes)-1]
	m.logger.V(7).Info("Device removed from constraint set", "device", deviceID, "numDevices", len(m.attributes))
}

// matches 判断给定请求是否受此约束影响：子请求匹配父请求名或“父/子”全名。
func (m *distinctAttributeConstraint) matches(request *requestData) bool {
	if request.parentRequest != nil {
		requestName := request.parentRequest.name()
		subRequestName := request.request.name()
		fullSubRequestName := fmt.Sprintf("%s/%s", requestName, subRequestName)
		return m.requestNames.Has(requestName) || m.requestNames.Has(fullSubRequestName)
	} else {
		return m.requestNames.Has(request.request.name())
	}
}

// matchesAttribute 判断新属性值是否与已接纳的属性值冲突：列表类型按集合不相交语义，标量按精确相等语义。
func (m *distinctAttributeConstraint) matchesAttribute(attribute resourceapi.DeviceAttribute) bool {
	// 启用列表属性特性时，采用“两两不相交”的集合语义。
	if m.features.ListTypeAttributes {
		// 无法转换为集合（未知属性类型），拒绝。
		newSet := attributeAsSet(&attribute)
		if newSet == nil {
			m.logger.V(7).Info("Unknown attribute type")
			return false
		}

		// 与每个已有设备的属性集合求交，出现交集即违反互异约束。
		for _, attr := range m.attributes {
			existingSet := attributeAsSet(attr)
			if existingSet == nil {
				continue
			}
			// 与某已有设备存在公共元素，违反互异约束。
			if newSet.intersection(existingSet) != nil {
				m.logger.V(7).Info("Constraint not satisfied, devices have common elements")
				return false
			}
		}

		// 与所有已有设备均不相交，满足约束。
		m.logger.V(7).Info("Constraint satisfied, new device is disjoint from all existing devices")
		return true
	}

	// 未启用列表特性时，按标量精确相等逐一比较；重复则拒绝。
	for _, attr := range m.attributes {
		// 按属性值的具体类型分别比较。
		switch {
		case attribute.StringValue != nil:
			if attr.StringValue != nil && *attribute.StringValue == *attr.StringValue {
				m.logger.V(7).Info("String values duplicated")
				return false
			}
		case attribute.IntValue != nil:
			if attr.IntValue != nil && *attribute.IntValue == *attr.IntValue {
				m.logger.V(7).Info("Int values duplicated")
				return false
			}
		case attribute.BoolValue != nil:
			if attr.BoolValue != nil && *attribute.BoolValue == *attr.BoolValue {
				m.logger.V(7).Info("Bool values duplicated")
				return false
			}
		// semver 2.0.0 要求最小规范形式（尤其禁止前导零），因此版本值可直接做字符串相等比较。
		case attribute.VersionValue != nil:
			if attr.VersionValue != nil && *attribute.VersionValue == *attr.VersionValue {
				m.logger.V(7).Info("Version values duplicated")
				return false
			}
		default:
			// 未知值类型无法匹配；正常应在 CEL 编译阶段已被拒绝。
			m.logger.V(7).Info("Distinct attribute type unknown")
			return false
		}
	}
	// 与所有已有属性值均不重复，满足约束。
	return true
}
