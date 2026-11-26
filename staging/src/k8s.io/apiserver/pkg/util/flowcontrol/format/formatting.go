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

// Package format 提供惰性格式化工具：只有真正打印时才展开大型 APF 对象，
// 避免低日志级别下的序列化开销。
package format

import (
	"bytes"
	"encoding/json"
	"fmt"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// 本文件提供一种便捷方式，把值标记为在需要时用 `%s`
// 完整格式化输出，但若值未被打印，则不花费大量
// CPU 或内存。API Priority and Fairness 的 API 对象
// 格式化为 JSON，这里的其他类型
// 格式化为 Go 源码。

// Stringer 标记给定值由本包进行自定义格式化；仅保存引用，不做任何拷贝或序列化，因此构造本身几乎零成本。
type Stringer struct{ val interface{} }

// Fmt 把给定值标记为由本包自定义格式化。
func Fmt(val interface{}) Stringer {
	return Stringer{val}
}

// String 以完整细节将值格式化为字符串：APF API 对象输出 JSON（便于复制进 issue），其他类型输出 Go 源码形式。
func (sr Stringer) String() string {
	if sr.val == nil {
		return "nil"
	}
	switch typed := sr.val.(type) {
	case *flowcontrol.FlowSchema,
		flowcontrol.FlowSchema,
		flowcontrol.FlowSchemaSpec,
		flowcontrol.FlowDistinguisherMethod,
		*flowcontrol.FlowDistinguisherMethod,
		*flowcontrol.PolicyRulesWithSubjects,
		flowcontrol.PolicyRulesWithSubjects,
		flowcontrol.Subject,
		flowcontrol.ResourcePolicyRule,
		flowcontrol.NonResourcePolicyRule,
		flowcontrol.FlowSchemaCondition,
		*flowcontrol.PriorityLevelConfiguration,
		flowcontrol.PriorityLevelConfiguration,
		flowcontrol.PriorityLevelConfigurationSpec,
		*flowcontrol.LimitedPriorityLevelConfiguration,
		flowcontrol.LimitedPriorityLevelConfiguration,
		flowcontrol.LimitResponse,
		*flowcontrol.QueuingConfiguration,
		flowcontrol.QueuingConfiguration:
		return ToJSON(sr.val)
	case []user.Info:
		return FmtUsers(typed)
	case []*request.RequestInfo:
		return FmtRequests(typed)
	default:
		return fmt.Sprintf("%#+v", sr.val)
	}
}

// ToJSON 用 encoding/json 转换，并通过格式化来处理错误
func ToJSON(val interface{}) string {
	bs, err := json.Marshal(val)
	str := string(bs)
	if err != nil {
		str = str + "<" + err.Error() + ">"
	}
	return str
}

// FmtPriorityLevelConfiguration 返回与给定值等价的 Go 源码表达式。
// 输出可直接粘贴进测试代码，用于构造期望值。
func FmtPriorityLevelConfiguration(pl *flowcontrol.PriorityLevelConfiguration) string {
	if pl == nil {
		return "nil"
	}
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("&flowcontrolv1.PriorityLevelConfiguration{ObjectMeta: %#+v, Spec: ",
		pl.ObjectMeta))
	BufferPriorityLevelConfigurationSpec(&buf, &pl.Spec)
	buf.WriteString(fmt.Sprintf(", Status: %#+v}", pl.Status))
	return buf.String()
}

// FmtPriorityLevelConfigurationSpec 返回与给定值等价的 Go 源码表达式
func FmtPriorityLevelConfigurationSpec(plSpec *flowcontrol.PriorityLevelConfigurationSpec) string {
	var buf bytes.Buffer
	BufferPriorityLevelConfigurationSpec(&buf, plSpec)
	return buf.String()
}

// BufferPriorityLevelConfigurationSpec 把给定值的 Go 源码表达式写入给定 buffer
func BufferPriorityLevelConfigurationSpec(buf *bytes.Buffer, plSpec *flowcontrol.PriorityLevelConfigurationSpec) {
	buf.WriteString(fmt.Sprintf("flowcontrolv1.PriorityLevelConfigurationSpec{Type: %#v", plSpec.Type))
	if plSpec.Limited != nil {
		buf.WriteString(fmt.Sprintf(", Limited: &flowcontrol.LimitedPriorityLevelConfiguration{NominalConcurrencyShares:%d, LimitResponse:flowcontrol.LimitResponse{Type:%#v", plSpec.Limited.NominalConcurrencyShares, plSpec.Limited.LimitResponse.Type))
		if plSpec.Limited.LimitResponse.Queuing != nil {
			buf.WriteString(fmt.Sprintf(", Queuing:&%#+v", *plSpec.Limited.LimitResponse.Queuing))
		}
		buf.WriteString(" } }")
	}
	buf.WriteString("}")
}

// FmtFlowSchema 生成该值的 Go 源码表达式。
func FmtFlowSchema(fs *flowcontrol.FlowSchema) string {
	if fs == nil {
		return "nil"
	}
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("&flowcontrolv1.FlowSchema{ObjectMeta: %#+v, Spec: ",
		fs.ObjectMeta))
	BufferFlowSchemaSpec(&buf, &fs.Spec)
	buf.WriteString(fmt.Sprintf(", Status: %#+v}", fs.Status))
	return buf.String()
}

// FmtFlowSchemaSpec 生成与给定 spec 等价的 Go 源码表达式。
func FmtFlowSchemaSpec(fsSpec *flowcontrol.FlowSchemaSpec) string {
	var buf bytes.Buffer
	BufferFlowSchemaSpec(&buf, fsSpec)
	return buf.String()
}

// BufferFlowSchemaSpec 将给定值对应的 Go 源码表达式写入给定 buffer。
// 手写拼接而非反射，保证输出稳定且不触发未导出字段的格式化问题。
func BufferFlowSchemaSpec(buf *bytes.Buffer, fsSpec *flowcontrol.FlowSchemaSpec) {
	buf.WriteString(fmt.Sprintf("flowcontrolv1.FlowSchemaSpec{PriorityLevelConfiguration: %#+v, MatchingPrecedence: %d, DistinguisherMethod: ",
		fsSpec.PriorityLevelConfiguration,
		fsSpec.MatchingPrecedence))
	if fsSpec.DistinguisherMethod == nil {
		buf.WriteString("nil")
	} else {
		buf.WriteString(fmt.Sprintf("&%#+v", *fsSpec.DistinguisherMethod))
	}
	buf.WriteString(", Rules: []flowcontrol.PolicyRulesWithSubjects{")
	for idx, rule := range fsSpec.Rules {
		if idx > 0 {
			buf.WriteString(", ")
		}
		BufferFmtPolicyRulesWithSubjectsSlim(buf, rule)
	}
	buf.WriteString("}}")
}

// FmtPolicyRulesWithSubjects 生成该值的 Go 源码表达式。
func FmtPolicyRulesWithSubjects(rule flowcontrol.PolicyRulesWithSubjects) string {
	return "flowcontrolv1.PolicyRulesWithSubjects" + FmtPolicyRulesWithSubjectsSlim(rule)
}

// FmtPolicyRulesWithSubjectsSlim 生成该值的 Go 源码表达式，
// 但不包含前面的类型名。上文有示例说明
// 何时需要这种形式。
func FmtPolicyRulesWithSubjectsSlim(rule flowcontrol.PolicyRulesWithSubjects) string {
	var buf bytes.Buffer
	BufferFmtPolicyRulesWithSubjectsSlim(&buf, rule)
	return buf.String()
}

// BufferFmtPolicyRulesWithSubjectsSlim 把给定值的 Go 源码表达式写入给定 buffer，
// 但不包含前面的类型名。
func BufferFmtPolicyRulesWithSubjectsSlim(buf *bytes.Buffer, rule flowcontrol.PolicyRulesWithSubjects) {
	buf.WriteString("{Subjects: []flowcontrolv1.Subject{")
	for jdx, subj := range rule.Subjects {
		if jdx > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("{Kind: %q", subj.Kind))
		if subj.User != nil {
			buf.WriteString(fmt.Sprintf(", User: &%#+v", *subj.User))
		}
		if subj.Group != nil {
			buf.WriteString(fmt.Sprintf(", Group: &%#+v", *subj.Group))
		}
		if subj.ServiceAccount != nil {
			buf.WriteString(fmt.Sprintf(", ServiceAccount: &%#+v", *subj.ServiceAccount))
		}
		buf.WriteString("}")
	}
	buf.WriteString(fmt.Sprintf("}, ResourceRules: %#+v, NonResourceRules: %#+v}", rule.ResourceRules, rule.NonResourceRules))
}

// FmtUsers 生成该值的 Go 源码表达式，用于在日志中展示请求主体集合。
func FmtUsers(list []user.Info) string {
	var buf bytes.Buffer
	buf.WriteString("[]user.Info{")
	for idx, member := range list {
		if idx > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%#+v", member))
	}
	buf.WriteString("}")
	return buf.String()
}

// FmtRequests 生成该值的 Go 源码表达式。
func FmtRequests(list []*request.RequestInfo) string {
	var buf bytes.Buffer
	buf.WriteString("[]*request.RequestInfo{")
	for idx, member := range list {
		if idx > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%#+v", member))
	}
	buf.WriteString("}")
	return buf.String()
}
