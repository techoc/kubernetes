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

package incubating

import (
	"fmt"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/klog/v2"
)

// distinctAttributeConstraint 实现 distinctAttribute 约束：约束内所有设备在某属性上的取值必须互不相同；
// 集合为空时任意带该属性的设备都可加入，此后只允许取值未出现过的设备。
type distinctAttributeConstraint struct {
	// 字段说明：logger 记录约束名与属性名；requestNames 为受约束的请求名集合；
	// attributeName 为待比较的属性全限定名。三者均为构造后不再变化的配置。
	logger        klog.Logger // 已包含名称与属性名，日志消息中无需重复。
	requestNames  sets.Set[string]
	attributeName resourceapi.FullyQualifiedName

	// attributes 按加入顺序保存已接受设备的属性值，仅需数量与取值，无需记录具体设备。
	attributes []*resourceapi.DeviceAttribute
}

// add 在设备加入前校验：不受约束影响的请求直接通过；缺少属性或取值重复则拒绝；
// 通过后把属性值追加到 attributes 中。
func (m *distinctAttributeConstraint) add(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) bool {
	if m.requestNames.Len() > 0 && !m.matches(requestName, subRequestName) {
		// 该请求不在约束范围内，设备不受影响。
		return true
	}

	// 按属性名从设备上查找属性。
	attribute := lookupAttribute(device, deviceID, m.attributeName)
	// 设备没有该属性，约束不满足。
	if attribute == nil {
		m.logger.V(7).Info("Constraint not satisfied, attribute not set")
		return false
	}

	// 与已接受属性值比较，出现重复则不满足。
	if !m.matchesAttribute(*attribute) {
		m.logger.V(7).Info("Constraint not satisfied, has some duplicated attributes")
		return false
	}

	// 通过校验，记录该属性值。
	m.attributes = append(m.attributes, attribute)
	// 记录成功加入的设备数与当前集合大小。
	m.logger.V(7).Info("Constraint satisfied by device", "device", deviceID, "numDevices", len(m.attributes))
	return true

}

// remove 撤销一次成功的 add，仅截短属性值切片；不受约束影响的请求直接返回。
func (m *distinctAttributeConstraint) remove(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) {
	if m.requestNames.Len() > 0 && !m.matches(requestName, subRequestName) {
		// 该请求不受约束影响，无需回滚。
		return
	}

	// 按加入顺序弹出最后一条属性值。
	m.attributes = m.attributes[:len(m.attributes)-1]
	m.logger.V(7).Info("Device removed from constraint set", "device", deviceID, "numDevices", len(m.attributes))
}

// matches 判断请求名或子请求全名是否落在约束的作用范围内。
func (m *distinctAttributeConstraint) matches(requestName, subRequestName string) bool {
	// 普通请求直接按请求名匹配。
	if subRequestName == "" {
		return m.requestNames.Has(requestName)
	} else {
		// 子请求同时匹配请求名与“父请求/子请求”全名。
		fullSubRequestName := fmt.Sprintf("%s/%s", requestName, subRequestName)
		return m.requestNames.Has(requestName) || m.requestNames.Has(fullSubRequestName)
	}
}

// matchesAttribute 检查新属性值是否与已接受的任一取值重复；
// 按属性实际类型分别比较，字符串/整数/布尔/版本均要求严格相等。
func (m *distinctAttributeConstraint) matchesAttribute(attribute resourceapi.DeviceAttribute) bool {
	// 逐个比较已接受的属性值。
	for _, attr := range m.attributes {
		// 按属性值的实际类型分派比较逻辑。
		switch {
		// 字符串值比较。
		case attribute.StringValue != nil:
			if attr.StringValue != nil && *attribute.StringValue == *attr.StringValue {
				m.logger.V(7).Info("String values duplicated")
				return false
			}
		// 整数值比较。
		case attribute.IntValue != nil:
			if attr.IntValue != nil && *attribute.IntValue == *attr.IntValue {
				m.logger.V(7).Info("Int values duplicated")
				return false
			}
		// 布尔值比较。
		case attribute.BoolValue != nil:
			if attr.BoolValue != nil && *attribute.BoolValue == *attr.BoolValue {
				m.logger.V(7).Info("Bool values duplicated")
				return false
			}
		// 版本值比较：semver 2.0.0 要求最小形式，故可直接做字符串相等比较。
		case attribute.VersionValue != nil:
			// 版本字符串相同则视为重复。
			if attr.VersionValue != nil && *attribute.VersionValue == *attr.VersionValue {
				m.logger.V(7).Info("Version values duplicated")
				return false
			}
		// 未知值类型无法匹配；正常流程中此类值会在 CEL 编译阶段被拒绝。
		default:
			// 记录未知类型并拒绝该设备。
			m.logger.V(7).Info("Distinct attribute type unknown")
			return false
		}
	}
	// 与所有已接受取值均不重复，约束满足。
	return true
}
