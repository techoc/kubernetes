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

package v1

import (
	"errors"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"

	"k8s.io/klog/v2"
)

// MatchToleration 判断本容忍是否与 tolerationToMatch 匹配。
// 容忍以 <key,effect,operator,value> 四元组唯一标识，四者都相同即视为匹配。
// TODO: 在 API 校验中加入容忍的唯一性检查。
func (t *Toleration) MatchToleration(tolerationToMatch *Toleration) bool {
	return t.Key == tolerationToMatch.Key &&
		t.Effect == tolerationToMatch.Effect &&
		t.Operator == tolerationToMatch.Operator &&
		t.Value == tolerationToMatch.Value
}

// ToleratesTaint 判断本容忍是否容忍该污点，匹配规则如下：
//
//  1. toleration.effect 为空时匹配所有污点 effect，否则要求两者 effect 相等。
//  2. toleration.operator 为 'Exists' 时匹配所有污点 value。
//  3. toleration.key 为空时匹配所有污点 key；此时 operator 必须为 'Exists'，
//     两者组合表示匹配任意 key 与任意 value。
//  4. toleration.operator 为 'Lt' 或 'Gt' 时，对 toleration.value 与 taint.value
//     做数值比较。
//  5. enableComparisonOperators 为 false 且容忍使用了 'Lt' 或 'Gt' 时，
//     该容忍不匹配（返回 false）。
func (t *Toleration) ToleratesTaint(logger klog.Logger, taint *Taint, enableComparisonOperators bool) bool {
	if len(t.Effect) > 0 && t.Effect != taint.Effect {
		return false
	}

	if len(t.Key) > 0 && t.Key != taint.Key {
		return false
	}

	// TODO: 等 Toleration 成为 PodSpec 字段后再使用正规的默认值填充机制。
	switch t.Operator {
	// 空 operator 等价于 Equal。
	case "", TolerationOpEqual:
		return t.Value == taint.Value
	case TolerationOpExists:
		return true
	case TolerationOpLt, TolerationOpGt:
		// 比较运算符特性关闭时该容忍直接不匹配。
		if !enableComparisonOperators {
			return false
		}
		return compareNumericValues(logger, t.Value, taint.Value, t.Operator)
	default:
		return false
	}
}

// compareNumericValues 将容忍值与污点值按 int64 解析后做数值比较。
// 任一值不是合法十进制整数时返回 false。
func compareNumericValues(logger klog.Logger, tolerationVal, taintVal string, op TolerationOperator) bool {

	errorMsgs := content.IsDecimalInteger(tolerationVal)
	if len(errorMsgs) > 0 {
		logger.Error(errors.New(strings.Join(errorMsgs, ",")), "failed to parse toleration value as int64", "toleration", tolerationVal)
		return false
	}
	tVal, err := strconv.ParseInt(tolerationVal, 10, 64)
	if err != nil {
		logger.Error(err, "failed to parse toleration value as int64", "toleration", tolerationVal)
		return false
	}

	errorMsgs = content.IsDecimalInteger(taintVal)
	if len(errorMsgs) > 0 {
		logger.Error(errors.New(strings.Join(errorMsgs, ",")), "failed to parse taint value as int64", "taint", taintVal)
		return false
	}
	tntVal, err := strconv.ParseInt(taintVal, 10, 64)
	if err != nil {
		logger.Error(err, "failed to parse taint value as int64", "taint", taintVal)
		return false
	}

	switch op {
	case TolerationOpLt:
		return tntVal < tVal
	case TolerationOpGt:
		return tntVal > tVal
	default:
		return false
	}
}
