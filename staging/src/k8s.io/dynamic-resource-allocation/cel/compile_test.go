/*
Copyright 2022 The Kubernetes Authors.

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

package cel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// maxElementsListTypeEnabled 取标量长度上限与列表长度上限中的较大者。
// maxElementsListTypeEnabled 是启用列表属性时属性类型采用的元素数上限（标量长度与列表长度上限的较大者）。
const maxElementsListTypeEnabled = uint64(max(resourceapi.DeviceAttributeMaxValueLength, resourceapi.ResourceSliceMaxAttributeValuesPerDevice))

// testcases 是选择器表达式的端到端表驱动用例集合，键为用例名。
var testcases = map[string]struct {
	// testcases 覆盖选择器表达式的编译、成本与求值行为。
	// envType 限定用例使用的环境类型；nil 表示存储表达式与新表达式都执行。
	envType *environment.Type
	// features 限定用例运行的特性组合；nil 表示遍历全部组合。
	features *Features

	// expression 是待编译的选择器表达式。
	// driver 是输入设备的驱动域名。
	// allowMultipleAllocations 模拟输入中是否允许同一设备多次分配。
	// attributes 是输入设备的属性集合。
	// capacity 是输入设备的容量集合。
	// expectCompileError 是期望的编译错误片段；非空表示期望编译失败。
	// expectMatchError 是期望的求值错误片段；非空表示期望求值失败。
	// expectMatch 是期望的匹配布尔结果。
	expression               string
	driver                   string
	allowMultipleAllocations *bool
	attributes               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute
	capacity                 map[resourceapi.QualifiedName]resourceapi.DeviceCapacity
	expectCompileError       string
	expectMatchError         string
	expectMatch              bool

	// expectCost 用固定数值守护成本估算不发生回归。
	// expectCost 用固定数值防止成本估算回归，也便于人工快速核对合理性。
	expectCost uint64
}{
	// 常量表达式：恒真。
	"true": {
		expression:  "true",
		expectMatch: true,
	},
	// 常量表达式：恒假。
	"false": {
		expression:  "false",
		expectMatch: false,
	},
	// 语法错误：期望编译失败。
	"syntax-error": {
		expression:         "?!",
		expectCompileError: "Syntax error",
	},
	// 输出类型错误：整数不是合法的选择器结果。
	"type-error": {
		expression:         `1`,
		expectCompileError: "must evaluate to bool or the unknown type, not int",
	},
	// 运行时错误：点号访问不存在的属性字段。
	"runtime-error-lookup-identifier": {
		expression:       `device.attributes["no-such-domain"].noSuchAttr.isGreaterThan(quantity("0"))`,
		expectMatchError: "no such key: noSuchAttr",
		expectCost:       6,
	},
	// 运行时错误：下标访问不存在的属性键。
	"runtime-error-lookup-map": {
		expression:       `device.attributes["no-such-domain"]["noSuchAttr"].isGreaterThan(quantity("0"))`,
		expectMatchError: "no such key: noSuchAttr",
		expectCost:       6,
	},
	// 域检查：不存在的域不在 attributes 映射中。
	"domain-check-negative": {
		expression:  `"no-such-domain" in device.attributes`,
		expectMatch: false,
		expectCost:  3,
	},
	// 域检查：已有属性所属域存在于 attributes 映射中。
	"domain-check-positive": {
		expression:  `"dra.example.com" in device.attributes`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"dra.example.com/something": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  3,
	},
	// 空驱动名：device.driver 默认为空字符串。
	"empty-driver-name": {
		expression:  `device.driver == ""`,
		expectMatch: true,
		expectCost:  2,
	},
	// 驱动名暴露：输入驱动名可在表达式中读取。
	"real-driver-name": {
		expression:  `device.driver == "dra.example.com"`,
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4,
	},
	// 无域名属性使用输入驱动名作为默认域（点号访问）。
	"driver-name-qualifier": {
		expression:  `device.attributes["dra.example.com"].name`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  4,
	},
	// 同上，但使用下标访问。
	"driver-name-qualifier-map-lookup": {
		expression:  `device.attributes["dra.example.com"]["name"]`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  4,
	},
	// cel.bind 把同域映射绑定到局部变量，简化连续访问。
	"bind": {
		expression:  `cel.bind(dra, device.attributes["dra.example.com"], dra.name)`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  15,
	},
	// 带显式域名的属性可跨驱动域访问。
	"qualified-attribute-name": {
		expression:  `device.attributes["other.example.com"].name`,
		driver:      "dra.example.com",
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"other.example.com/name": {BoolValue: ptr.To(true)}},
		expectMatch: true,
		expectCost:  4,
	},
	// 标量属性：布尔值直接作为匹配结果。
	"bool": {
		expression:  `device.attributes["dra.example.com"].name`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: ptr.To(true)}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4,
	},
	// 标量属性：整数参与比较。
	"int": {
		expression:  `device.attributes["dra.example.com"].name > 0`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: ptr.To(int64(1))}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  5,
	},
	// 标量属性：字符串参与相等比较。
	"string": {
		expression:  `device.attributes["dra.example.com"].name == "fish"`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: ptr.To("fish")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  5,
	},
	// 未启用列表属性时，exists 宏在新表达式中编译失败。
	"macro-compilation-error-new-expressions": {
		features:           &Features{EnableListTypeAttributes: false},
		envType:            new(environment.NewExpressions),
		expression:         `device.attributes["dra.example.com"].ids.exists(id, id == 1)`,
		driver:             "dra.example.com",
		expectCompileError: "compilation failed",
	},
	// 运行时错误：对整数标量调用 exists（期望可迭代类型），成本含循环上限。
	"macro-exists-on-int": {
		features:         &Features{EnableListTypeAttributes: true},
		expression:       `device.attributes["dra.example.com"].name.exists(x, x > 0)`,
		attributes:       map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: new(int64(1))}},
		driver:           "dra.example.com",
		expectMatchError: "got 'types.Int', expected iterable type",
		expectCost:       5 + ((3 + 3) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x > 0)")) * maxElementsListTypeEnabled */),
	},
	// 运行时错误：对布尔标量调用 exists。
	"macro-exists-on-bool": {
		features:         &Features{EnableListTypeAttributes: true},
		expression:       `device.attributes["dra.example.com"].name.exists(x, x == true)`,
		attributes:       map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: new(true)}},
		driver:           "dra.example.com",
		expectMatchError: "got 'types.Bool', expected iterable type",
		expectCost:       5 + ((3 + 3) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x > 0)")) * maxElementsListTypeEnabled */),
	},
	// 运行时错误：对字符串标量调用 exists。
	"macro-exists-on-string": {
		features:         &Features{EnableListTypeAttributes: true},
		expression:       `device.attributes["dra.example.com"].name.exists(x, x == "fish")`,
		attributes:       map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: new("fish")}},
		driver:           "dra.example.com",
		expectMatchError: "got 'types.String', expected iterable type",
		expectCost:       5 + ((3 + 3) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x > 0)")) * maxElementsListTypeEnabled */),
	},
	// 选择器返回列表无法转为 bool，期望求值错误。
	"list-result-type-causes-match-error": {
		expression:       `device.attributes["dra.example.com"].names`,
		attributes:       map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {StringValues: []string{"fish", "bird"}}},
		driver:           "dra.example.com",
		expectMatchError: "CEL result of type list could not be converted to bool",
		expectCost:       4,
	},
	// 启用列表属性后 includes 函数可编译且可求值。
	"includes-function-can-evaluate": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes("fish")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	// 多值属性通过 size() 读取长度：布尔列表。
	"list-of-bool": {
		expression:  `device.attributes["dra.example.com"].names.size() == 2`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {BoolValues: []bool{true, false}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	// 多值属性通过 size() 读取长度：整数列表。
	"list-of-int": {
		expression:  `device.attributes["dra.example.com"].names.size() == 2`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {IntValues: []int64{1, 2}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	// 多值属性通过 size() 读取长度：字符串列表。
	"list-of-string": {
		expression:  `device.attributes["dra.example.com"].names.size() == 2`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {StringValues: []string{"fish", "bird"}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	// 多值属性通过 size() 读取长度：semver 列表。
	"list-of-semver": {
		expression:  `device.attributes["dra.example.com"].names.size() == 2`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	// 列表属性上的 exists 宏：整数列表。
	"macro-on-list-of-int": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].names.exists(x, x > 0)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {IntValues: []int64{1, 2}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  5 + ((3 + 3) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x > 0)")) * maxElementsListTypeEnabled */),
	},
	// 列表属性上的 all 宏：字符串列表。
	"macro-on-list-of-string": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].names.all(x, x != "")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {StringValues: []string{"fish", "bird"}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  5 + ((2 + 2) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(accu)") + cost(loopStep=="accu && (x != "")")) * maxElementsListTypeEnabled */),
	},
	// 列表属性上的 all 宏：semver 列表，含比较函数调用。
	"macro-on-list-of-semver": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].names.all(x, x.isGreaterThan(semver("0.0.1")))`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  5 + ((2 + 4) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(accu)") + cost(loopStep=="accu && (x.isGreaterThan(semver("0.0.1"))")) * maxElementsListTypeEnabled */),
	},
	// 未启用列表属性且为新表达式时 includes 未声明，编译失败。
	"includes-function-compilation-error": {
		features:           &Features{EnableListTypeAttributes: false},
		envType:            ptr.To(environment.NewExpressions),
		expression:         `device.attributes["dra.example.com"].attr.includes("fish")`,
		driver:             "dra.example.com",
		expectCompileError: "undeclared reference to 'includes'",
	},
	// 启用列表属性后 includes 编译与求值均成功（字符串标量）。
	"includes-function-compilation-and-eval-success": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].attr.includes("fish")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"attr": {StringValue: new("fish")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	// includes 成员判断的正例与反例：覆盖标量/列表以及布尔、整数、字符串、semver 四类元素。
	"includes-function-on-bool-scalar-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(true)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: new(true)}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-bool-scalar-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		envType:     ptr.To(environment.NewExpressions),
		expression:  `device.attributes["dra.example.com"].name.includes(true)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValue: new(false)}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-bool-list-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(true)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{true, false}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-bool-list-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(false)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {BoolValues: []bool{true, true}}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-int-scalar-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(1)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: new(int64(1))}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-int-scalar-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(1)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValue: new(int64(2))}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-int-list-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(1)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{1, 2}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-int-list-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(3)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {IntValues: []int64{1, 2}}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-string-scalar-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes("fish")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: new("fish")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-string-scalar-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes("bird")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValue: new("fish")}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-string-list-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes("fish")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-string-list-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes("cat")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: []string{"fish", "bird"}}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	"includes-function-on-semver-scalar-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(semver("1.0.0"))`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: new("1.0.0")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64 /* 动态类型上 includes 的代价 */ + 1,
	},
	"includes-function-on-semver-scalar-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(semver("2.0.0"))`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: new("1.0.0")}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64 /* 动态类型上 includes 的代价 */ + 1,
	},
	"includes-function-on-semver-list-positive": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(semver("1.0.0"))`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64 /* 动态类型上 includes 的代价 */ + 1,
	},
	"includes-function-on-semver-list-negative": {
		features:    &Features{EnableListTypeAttributes: true},
		expression:  `device.attributes["dra.example.com"].name.includes(semver("3.0.0"))`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValues: []string{"1.0.0", "2.0.0"}}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64 /* 动态类型上 includes 的代价 */ + 1,
	},
	// 超长列表上的 includes：验证边界规模下成本仍为动态类型调用成本。
	"includes-function-on-very-long-list-positive": {
		features:   &Features{EnableListTypeAttributes: true},
		expression: fmt.Sprintf(`device.attributes["dra.example.com"].name.includes("value-%d")`, resourceapi.ResourceSliceMaxAttributeValuesPerDevice),
		attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {StringValues: func() []string {
			values := make([]string, resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
			for i := range values {
				values[i] = fmt.Sprintf("value-%d", i)
			}
			return values
		}()}},
		driver:      "dra.example.com",
		expectMatch: false,
		expectCost:  4 + 64, /* 动态类型上 includes 的代价 */
	},
	// 文档用途：对比 in 操作符与 includes 函数的成本估算方式。
	"in-operator-on-list": {
		expression:  `1 in device.attributes["dra.example.com"].names`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"names": {IntValues: []int64{1, 2, 3}}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  4 + 64, /* 启用 maxElementsListTypeEnabled 时 in 的代价 */
	},
	// semver 属性的比较函数。
	"version": {
		expression:  `device.attributes["dra.example.com"].name.isGreaterThan(semver("0.0.1"))`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: ptr.To("1.0.0")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	// semver 与 isSemver 的宽松解析，允许 v 前缀。
	"version_v": {
		expression:  `device.attributes["dra.example.com"].name.isGreaterThan(semver("v0.0.1", true)) && isSemver("v1.0.0", true)`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: ptr.To("1.0.0")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  7,
	},
	// 运行时错误：对 semver 标量调用 exists。
	"macro-exists-on-version": {
		features:         &Features{EnableListTypeAttributes: true},
		expression:       `device.attributes["dra.example.com"].name.exists(x, x.isGreaterThan(semver("0.0.1")))`,
		attributes:       map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"name": {VersionValue: new("1.0.0")}},
		driver:           "dra.example.com",
		expectMatchError: "got 'cel.Semver', expected iterable type",
		expectCost:       5 + ((3 + 4) * maxElementsListTypeEnabled /* (cost(loopCondition=="not_strictly_false(!accu)") + cost(loopStep=="accu && (x.isGreaterThan(semver("0.0.1"))")) * maxElementsListTypeEnabled */),
	},
	// 容量值使用 quantity 进行比较。
	"quantity": {
		expression:  `device.capacity["dra.example.com"].name.isGreaterThan(quantity("1Ki"))`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"name": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  6,
	},
	// 容量键存在且值大于阈值时匹配。
	"check-positive": {
		expression:  `"name" in device.capacity["dra.example.com"] && device.capacity["dra.example.com"].name.isGreaterThan(quantity("1Ki"))`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"name": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  10,
	},
	// 容量键不存在但短路或使整个表达式成立。
	"check-negative": {
		expression:  `!("name" in device.capacity["dra.example.com"]) || device.capacity["dra.example.com"].name.isGreaterThan(quantity("1Ki"))`,
		expectMatch: true,
		expectCost:  11,
	},
	// 综合用例：点号/下标访问四种属性与容量类型并逐项比较。
	"all": {
		expression: `
device.capacity["dra.example.com"].quantity.isGreaterThan(quantity("1Ki")) &&
device.attributes["dra.example.com"].bool &&
device.attributes["dra.example.com"].int > 0 &&
device.attributes["dra.example.com"].string == "fish" &&
device.attributes["dra.example.com"].version.isGreaterThan(semver("0.0.1")) &&
device.capacity["dra.example.com"]["quantity"].isGreaterThan(quantity("1Ki")) &&
device.attributes["dra.example.com"]["bool"] &&
device.attributes["dra.example.com"]["int"] > 0 &&
device.attributes["dra.example.com"]["string"] == "fish" &&
device.attributes["dra.example.com"]["version"].isGreaterThan(semver("0.0.1"))
`,

		attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"bool":    {BoolValue: ptr.To(true)},
			"int":     {IntValue: ptr.To(int64(1))},
			"string":  {StringValue: ptr.To("fish")},
			"version": {VersionValue: ptr.To("1.0.0")},
		},
		capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"quantity": {Value: resource.MustParse("1Mi")},
		},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  52,
	},
	// 多个布尔属性串联，验证成本累加。
	"many": {
		expression: `device.attributes["dra.example.com"].a && device.attributes["dra.example.com"].b && device.attributes["dra.example.com"].c`,
		attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"a": {BoolValue: ptr.To(true)},
			"b": {BoolValue: ptr.To(true)},
			"c": {BoolValue: ptr.To(true)},
		},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  12,
	},
	// 遍历属性域名集合（exists_one）。
	"check_attribute_domains": {
		expression:  `device.attributes.exists_one(x, x == "dra.example.com")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("some_example_value")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  164,
	},
	// 遍历单个域内的属性 ID 集合。
	"check_attribute_ids": {
		expression:  `device.attributes["dra.example.com"].exists_one(x, x == "services")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("some_example_value")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  133,
	},
	// 字符串属性上的 split/size 库函数。
	"split_attribute": {
		expression:  `device.attributes["dra.example.com"].services.split("example").size() >= 2`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("some_example_value")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  19,
	},
	// 字符串属性上的正则匹配。
	"regexp_attribute": {
		expression:  `device.attributes["dra.example.com"].services.matches("[^a]?sym")`,
		attributes:  map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"services": {StringValue: ptr.To("asymetric")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  18,
	},
	// 遍历容量域名集合。
	"check_capacity_domains": {
		expression:  `device.capacity.exists_one(x, x == "dra.example.com")`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"memory": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  164,
	},
	// 遍历单域内的容量 ID 集合。
	"check_capacity_ids": {
		expression:  `device.capacity["dra.example.com"].exists_one(x, x == "memory")`,
		capacity:    map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"memory": {Value: resource.MustParse("1Mi")}},
		driver:      "dra.example.com",
		expectMatch: true,
		expectCost:  133,
	},
	// 构造接近规模上限的属性集合与深层嵌套循环，期望超出实际成本上限。
	"expensive": {
		attributes: func() map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
			attributes := make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute)
			prefix := "dra.example.com/"
			attribute := resourceapi.DeviceAttribute{
				StringValue: ptr.To("abc"),
			}
			for i := 0; i < resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice; i++ {
				suffix := fmt.Sprintf("-%d", i)
				name := prefix + strings.Repeat("x", resourceapi.DeviceMaxIDLength-len(suffix)) + suffix
				attributes[resourceapi.QualifiedName(name)] = attribute
			}
			return attributes
		}(),
		// 表达式改编自 https://github.com/kubernetes/kubernetes/blob/50fc400f178d2078d0ca46aee955ee26375fc437/test/integration/apiserver/cel/validatingadmissionpolicy_test.go#L2150
		expression:       `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))`,
		driver:           "dra.example.com",
		expectMatchError: "actual cost limit exceeded",
		expectCost:       85555551, // 超出限制！
	},
	// allowMultipleAllocations 用例组：验证字段可见性与 nil/true/false 三态语义。
	"allow_multiple_allocations": {
		features:                 &Features{EnableConsumableCapacity: true},
		expression:               `device.allowMultipleAllocations == true`,
		allowMultipleAllocations: ptr.To(true),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	// 输入未设置该字段时按 false 求值。
	"allow_multiple_allocations_default": {
		features:                 &Features{EnableConsumableCapacity: true},
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: nil,
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	// 输入显式 false。
	"allow_multiple_allocations_false": {
		features:                 &Features{EnableConsumableCapacity: true},
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: ptr.To(false),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	// 新表达式环境下的显式 false。
	"allow_multiple_allocations_new": {
		features:                 &Features{EnableConsumableCapacity: true},
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: ptr.To(false),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	// 特性开启时的默认情况。
	"allow_multiple_allocations_enabled": {
		features:                 &Features{EnableConsumableCapacity: true},
		expression:               `device.allowMultipleAllocations == false`,
		allowMultipleAllocations: ptr.To(false),
		driver:                   "dra.example.com",
		expectMatch:              true,
		expectCost:               3,
	},
	// 特性关闭且为新表达式时字段未定义，期望编译失败。
	"allow_multiple_allocations_disabled": {
		envType:            ptr.To(environment.NewExpressions),
		features:           &Features{EnableConsumableCapacity: false},
		expression:         `device.allowMultipleAllocations == false`,
		driver:             "dra.example.com",
		expectCompileError: `undefined field 'allowMultipleAllocations'`,
	},
}

// TestCEL 遍历选择器用例，在适用的特性组合与环境类型下验证编译、成本与求值结果。
func TestCEL(t *testing.T) {
	// TestCEL 是选择器表达式的主表驱动测试。
	// 遍历所有表驱动用例。
	for name, scenario := range testcases {
		// run 封装单个组合下的编译与断言流程。
		run := func(t *testing.T, features Features, envType environment.Type) {
			// 创建带清理函数的测试上下文。
			_, ctx := ktesting.NewTestContext(t)
			// 按特性与环境类型编译表达式。
			result := GetCompiler(features).CompileCELExpression(scenario.expression, Options{EnvType: &envType})
			// 期望编译失败但实际成功。
			if scenario.expectCompileError != "" && result.Error == nil {
				t.Fatalf("FAILURE: expected compile error %q, got none", scenario.expectCompileError)
			}
			// 实际发生编译错误时的分支。
			if result.Error != nil {
				// 非预期的编译错误。
				if scenario.expectCompileError == "" {
					t.Fatalf("FAILURE: unexpected compile error: %v", result.Error)
				}
				// 编译错误文本必须包含期望片段。
				if !strings.Contains(result.Error.Error(), scenario.expectCompileError) {
					t.Fatalf("FAILURE: expected compile error to contain %q, but got instead: %v", scenario.expectCompileError, result.Error)
				}
				// 编译错误符合预期，无需继续求值。
				return
			}
			// 期望编译失败却编译成功。
			if scenario.expectCompileError != "" {
				t.Fatalf("FAILURE: expected compile error %q, got none", scenario.expectCompileError)
			}
			// 校验估算成本与期望值一致。
			if expect, actual := scenario.expectCost, result.MaxCost; expect != actual {
				t.Errorf("ERROR: expected CEL cost %d, got %d instead (%.0f%% of limit %d)", expect, actual, float64(actual)*100.0/float64(resourceapi.CELSelectorExpressionMaxCost), resourceapi.CELSelectorExpressionMaxCost)
			}

			// 用用例输入求值，取得匹配结果与求值细节。
			match, details, err := result.DeviceMatches(ctx, Device{
				AllowMultipleAllocations: scenario.allowMultipleAllocations, Attributes: scenario.attributes, Capacity: scenario.capacity, Driver: scenario.driver,
			})
			// 读取实际成本，细节可能为 nil。
			actualCost := ptr.Deref(details.ActualCost(), 0)
			// 记录实际成本与最坏估算的对比。
			if scenario.expectCost > 0 {
				t.Logf("actual cost %d, %d%% of worst-case estimate %d", actualCost, actualCost*100/scenario.expectCost, scenario.expectCost)
			} else {
				t.Logf("actual cost %d, expected zero costs", actualCost)
			}
			// 实际成本不得超出估算上限。
			if actualCost > result.MaxCost {
				t.Errorf("ERROR: cost estimate %d underestimated the evaluation cost of %d", result.MaxCost, actualCost)
			}

			// 求值错误分支。
			if err != nil {
				if scenario.expectMatchError == "" {
					t.Fatalf("FAILURE: unexpected evaluation error: %v", err)
				}
				// 求值错误文本必须包含期望片段。
				if !strings.Contains(err.Error(), scenario.expectMatchError) {
					t.Fatalf("FAILURE: expected evaluation error to contain %q, but got instead: %v", scenario.expectMatchError, err)
				}
				// 求值错误符合预期，返回。
				return
			}
			// 期望求值失败却未失败。
			if scenario.expectMatchError != "" {
				t.Fatalf("FAILURE: expected match error %q, got none", scenario.expectMatchError)
			}
			// 最终校验匹配结果。
			if match != scenario.expectMatch {
				t.Fatalf("FAILURE: expected result %v, got %v", scenario.expectMatch, match)
			}
		}

		// 按用例约束展开特性组合与环境类型。
		t.Run(name, func(t *testing.T) {
			// 默认遍历列表属性特性的开关状态。
			stateListType := []bool{true, false}
			// 默认遍历可分配容量特性的开关状态。
			stateConsumable := []bool{true, false}
			// 用例指定了特性时只跑该组合。
			if scenario.features != nil {
				stateConsumable = []bool{scenario.features.EnableConsumableCapacity}
				stateListType = []bool{scenario.features.EnableListTypeAttributes}
			}
			// 默认存储表达式与新表达式都执行。
			envTypes := []environment.Type{environment.NewExpressions, environment.StoredExpressions}
			// 用例指定了环境类型时只跑该类型。
			if scenario.envType != nil {
				envTypes = []environment.Type{*scenario.envType}
			}

			// 两层特性循环，内层再遍历环境类型并运行断言。
			var features Features
			for _, features.EnableListTypeAttributes = range stateListType {
				t.Run(fmt.Sprintf("list-type-attributes=%v", features.EnableListTypeAttributes), func(t *testing.T) {
					for _, features.EnableConsumableCapacity = range stateConsumable {
						t.Run(fmt.Sprintf("consumable-capacity=%v", features.EnableConsumableCapacity), func(t *testing.T) {
							for _, envType := range envTypes {
								t.Run(string(envType), func(t *testing.T) {
									run(t, features, envType)
								})
							}
						})
					}
				})
			}
		})
	}
}

// TestInterrupt 验证上下文超时/截止/取消能中断 CEL 求值，并把真实上下文原因透传给调用方。
func TestInterrupt(t *testing.T) {
	// TestInterrupt 覆盖超时、截止与取消三种中断方式。
	// 三种中断触发方式。
	for _, name := range []string{"timeout", "deadline", "cancel"} {
		t.Run(name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			// 表达式改编自 https://github.com/kubernetes/kubernetes/blob/e0859f91b7d269bb7e2f43e23d202ccccaf34c0c/staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/validation_test.go#L3006
			// 使用 map/filter/exists 组合表达式保证求值足够耗时。
			expression := `device.attributes["dra.example.com"].map(key, device.attributes["dra.example.com"][key] * 20).filter(e, e > 50).exists(e, e == 60)`
			// 编译表达式。
			result := GetCompiler(Features{}).CompileCELExpression(expression, Options{})
			// 编译失败直接终止。
			if result.Error != nil {
				t.Fatalf("unexpected compile error: %v", result.Error)
			}
			// 按场景构造已结束或即将结束的上下文。
			switch name {
			case "timeout":
				c, cancel := context.WithTimeout(ctx, time.Nanosecond)
				defer cancel()
				ctx = c
			case "deadline":
				c, cancel := context.WithDeadline(ctx, time.Now())
				defer cancel()
				ctx = c
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			// 构造含 1000 个整数属性的设备以延长求值时间。
			device := Device{
				Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute),
			}
			// 填充设备属性。
			for i := int64(0); i < 1000; i++ {
				device.Attributes[resourceapi.QualifiedName(fmt.Sprintf("dra.example.com/attr%d", i))] = resourceapi.DeviceAttribute{
					IntValue: ptr.To(i),
				}
			}
			// 执行求值并检查错误链是否包含上下文错误。
			_, _, err := result.DeviceMatches(ctx, device)
			// 上下文已结束：错误必须能 errors.Is 到 ctx.Err()。
			if ctx.Err() != nil {
				if !errors.Is(err, ctx.Err()) {
					t.Fatalf("expected %v, got error: %v", ctx.Err(), err)
				}
				// 上下文未结束：不应产生错误。
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}
		})
	}
}

// BenchmarkDeviceMatches 复用选择器用例，在存储表达式环境下基准测试 DeviceMatches 的求值性能。
func BenchmarkDeviceMatches(b *testing.B) {
	// BenchmarkDeviceMatches 是 DeviceMatches 的基准测试。
	// 跳过编译预期失败的用例。
	for name, scenario := range testcases {
		if scenario.expectCompileError != "" {
			continue
		}

		// run 先编译表达式，再在循环中反复求值。
		run := func(b *testing.B, features Features, envType environment.Type) {
			_, ctx := ktesting.NewTestContext(b)
			// 编译表达式。
			result := GetCompiler(features).CompileCELExpression(scenario.expression, Options{EnvType: &envType})
			if result.Error != nil {
				b.Fatalf("unexpected compile error: %s", result.Error.Error())
			}

			// 重复求值 b.N 次。
			for i := 0; i < b.N; i++ {
				// 只测量求值本身，准备工作的耗时不在计时范围内。
				match, _, err := result.DeviceMatches(ctx, Device{
					AllowMultipleAllocations: scenario.allowMultipleAllocations, Attributes: scenario.attributes, Capacity: scenario.capacity, Driver: scenario.driver,
				})
				// 求值错误断言与测试保持一致。
				if err != nil {
					if scenario.expectMatchError == "" {
						b.Fatalf("unexpected evaluation error: %v", err)
					}
					if !strings.Contains(err.Error(), scenario.expectMatchError) {
						b.Fatalf("expected evaluation error to contain %q, but got instead: %v", scenario.expectMatchError, err)
					}
					return
				}
				if scenario.expectMatchError != "" {
					b.Fatalf("expected match error %q, got none", scenario.expectMatchError)
				}
				// 校验匹配结果。
				if match != scenario.expectMatch {
					b.Fatalf("expected result %v, got %v", scenario.expectMatch, match)
				}
			}
		}

		b.Run(name, func(b *testing.B) {
			// 性能不依赖特性开关，固定使用存储表达式。
			run(b, Features{ /* 因使用已存储表达式而忽略 */ }, environment.StoredExpressions)
		})
	}
}

// TestCompileDerivedAttributes 验证派生属性的输出类型校验：合法标量及标量列表通过，非法类型报错。
func TestCompileDerivedAttributes(t *testing.T) {
	// TestCompileDerivedAttributes 只校验编译期的输出类型约束。
	// 表驱动用例：表达式、列表特性开关、环境类型与期望编译错误。
	// name 是用例名。
	// expression 是待编译的派生属性表达式。
	// listTypeAttributesEnabled 控制列表属性特性是否开启。
	// envType 为 nil 时默认 environment.NewExpressions。
	// expectCompileError 是期望的编译错误片段。
	testCases := []struct {
		name                      string
		expression                string
		listTypeAttributesEnabled bool
		envType                   *environment.Type // 默认使用 environment.NewExpressions
		expectCompileError        string
	}{
		// 合法标量字面量及其列表形式；列表需要开启特性。
		{
			name:                      "valid-bool",
			expression:                "true",
			listTypeAttributesEnabled: false,
		},
		{
			name:                      "valid-bool-list-when-listType-enabled",
			expression:                `[true, false]`,
			listTypeAttributesEnabled: true,
		},
		{
			name:                      "valid-int",
			expression:                "1",
			listTypeAttributesEnabled: false,
		},
		{
			name:                      "valid-int-list-when-listType-enabled",
			expression:                `[1, 2, 3]`,
			listTypeAttributesEnabled: true,
		},
		{
			name:                      "valid-string",
			expression:                `"fish"`,
			listTypeAttributesEnabled: false,
		},
		{
			name:                      "valid-string-list-when-listType-enabled",
			expression:                `["a", "b"]`,
			listTypeAttributesEnabled: true,
		},
		{
			name:                      "valid-semver",
			expression:                `semver("1.0.0")`,
			listTypeAttributesEnabled: false,
		},
		{
			name:                      "valid-semver-list-when-listType-enabled",
			expression:                `[semver("1.0.0"), semver("2.0.0")]`,
			listTypeAttributesEnabled: true,
		},
		{
			// any / dyn 类型及其列表形式。
			name:                      "valid-any",
			expression:                `device.attributes["dra.example.com"]["numa"]`,
			listTypeAttributesEnabled: false,
		},
		{
			name:                      "valid-any-list-when-listType-enabled",
			expression:                `[device.attributes["dra.example.com"]["numa"]]`,
			listTypeAttributesEnabled: true,
		},
		{
			name:                      "valid-dyn",
			expression:                `dyn("foo")`,
			listTypeAttributesEnabled: false,
		},
		{
			name:                      "valid-dyn-list-when-listType-enabled",
			expression:                `[dyn("foo"), dyn(1)]`,
			listTypeAttributesEnabled: true,
		},
		// 存储表达式即使特性关闭也允许整数列表。
		{
			name:                      "valid-int-list-in-stored-expression-despite-disabled-feature-gate",
			expression:                `[1, 2, 3]`,
			listTypeAttributesEnabled: false,
			envType:                   new(environment.StoredExpressions),
		},
		// 非法输出类型：map、特性关闭时的列表、元素为 map 的列表、null。
		{
			name:                      "invalid-map",
			expression:                `device.attributes`,
			listTypeAttributesEnabled: false,
			expectCompileError:        "must evaluate to a primitive scalar (string, integer, boolean, semver), not map(string, map(string, google.protobuf.Any))",
		},
		{
			name:                      "invalid-int-list-when-listType-disabled",
			expression:                `[1, 2, 3]`,
			listTypeAttributesEnabled: false,
			expectCompileError:        "must evaluate to a primitive scalar (string, integer, boolean, semver), not list(int)",
		},
		{
			name:                      "invalid-list-of-maps",
			expression:                `[device.attributes]`,
			listTypeAttributesEnabled: true,
			expectCompileError:        "must evaluate to a primitive scalar (string, integer, boolean, semver) or a list of these scalars, not list(map(string, map(string, dyn)))",
		},
		{
			name:                      "invalid-null-literal",
			expression:                `null`,
			listTypeAttributesEnabled: true,
			expectCompileError:        "must evaluate to a primitive scalar (string, integer, boolean, semver) or a list of these scalars, not null_type",
		},
	}

	// 遍历用例，按设置编译并校验错误。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 按用例配置列表属性特性。
			compiler := GetCompiler(Features{
				EnableListTypeAttributes: tc.listTypeAttributesEnabled,
			})
			// 默认使用新表达式环境。
			envType := environment.NewExpressions
			if tc.envType != nil {
				envType = *tc.envType
			}
			// 以 DerivedAttribute 模式编译。
			result := compiler.CompileCELExpression(tc.expression, Options{
				EnvType:          new(envType),
				DerivedAttribute: true,
			})

			// 期望编译失败时校验错误文本。
			if tc.expectCompileError != "" {
				if result.Error == nil {
					t.Fatalf("expected compile error %q, got none", tc.expectCompileError)
				}
				if !strings.Contains(result.Error.Error(), tc.expectCompileError) {
					t.Fatalf("expected compile error to contain %q, but got: %v", tc.expectCompileError, result.Error)
				}
				// 不期望错误时确认编译成功。
			} else if result.Error != nil {
				t.Fatalf("unexpected compile error: %v", result.Error)
			}
		})
	}
}
func TestEvaluateDerivedAttributes(t *testing.T) {
	// TestEvaluateDerivedAttributes 校验求值结果与错误分类。
	// 模拟设备：包含四种标量属性与四种列表属性，驱动域名为 driver-a。
	mockDevice := Device{
		Driver: "driver-a",
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"bool-attr":      {BoolValue: new(true)},
			"int-attr":       {IntValue: new(int64(42))},
			"str-attr":       {StringValue: new("hello")},
			"ver-attr":       {VersionValue: new("1.0.0")},
			"bool-list-attr": {BoolValues: []bool{true, false}},
			"int-list-attr":  {IntValues: []int64{1, 2, 3}},
			"str-list-attr":  {StringValues: []string{"hello", "world"}},
			"ver-list-attr":  {VersionValues: []string{"1.0.0", "2.0.0"}},
		},
	}

	// 表驱动用例：表达式、输入设备、期望属性与期望错误。
	// name 是用例名。
	// expression 是待求值的派生属性表达式。
	// device 是求值输入，零值表示不使用设备属性。
	// expectAttr 是期望得到的 DeviceAttribute。
	// expectEvalError 是期望的求值错误片段。
	testCases := []struct {
		name            string
		expression      string
		device          Device
		expectAttr      *resourceapi.DeviceAttribute
		expectEvalError string
	}{
		// 标量字面量用例组。
		// 标量字面量：期望转换为对应的单值字段。
		{
			name:       "bool-scalar-literal",
			expression: "true",
			expectAttr: &resourceapi.DeviceAttribute{BoolValue: new(true)},
		},
		{
			name:       "int-scalar-literal",
			expression: "42",
			expectAttr: &resourceapi.DeviceAttribute{IntValue: new(int64(42))},
		},
		{
			name:       "string-scalar-literal",
			expression: `"hello"`,
			expectAttr: &resourceapi.DeviceAttribute{StringValue: new("hello")},
		},
		{
			name:       "semver-scalar-literal",
			expression: `semver("1.0.0")`,
			expectAttr: &resourceapi.DeviceAttribute{VersionValue: new("1.0.0")},
		},

		// 从设备读取标量的用例组。
		// 从设备属性读取标量：验证域名/ID 查找与类型转换。
		{
			name:       "bool-scalar-device",
			expression: `device.attributes["driver-a"]["bool-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{BoolValue: new(true)},
		},
		{
			name:       "int-scalar-device",
			expression: `device.attributes["driver-a"]["int-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{IntValue: new(int64(42))},
		},
		{
			name:       "string-scalar-device",
			expression: `device.attributes["driver-a"]["str-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{StringValue: new("hello")},
		},
		{
			name:       "semver-scalar-device",
			expression: `device.attributes["driver-a"]["ver-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{VersionValue: new("1.0.0")},
		},

		// 列表字面量用例组。
		// 列表字面量：期望转换为对应的多值字段。
		{
			name:       "bool-list-literal",
			expression: `[true, false]`,
			expectAttr: &resourceapi.DeviceAttribute{BoolValues: []bool{true, false}},
		},
		{
			name:       "int-list-literal",
			expression: `[1, 2, 3]`,
			expectAttr: &resourceapi.DeviceAttribute{IntValues: []int64{1, 2, 3}},
		},
		{
			name:       "string-list-literal",
			expression: `["hello", "world"]`,
			expectAttr: &resourceapi.DeviceAttribute{StringValues: []string{"hello", "world"}},
		},
		{
			name:       "semver-list-literal",
			expression: `[semver("1.0.0"), semver("2.0.0")]`,
			expectAttr: &resourceapi.DeviceAttribute{VersionValues: []string{"1.0.0", "2.0.0"}},
		},

		// 从设备读取列表的用例组。
		// 从设备属性读取列表。
		{
			name:       "bool-list-device",
			expression: `device.attributes["driver-a"]["bool-list-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{BoolValues: []bool{true, false}},
		},
		{
			name:       "int-list-device",
			expression: `device.attributes["driver-a"]["int-list-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{IntValues: []int64{1, 2, 3}},
		},
		{
			name:       "string-list-device",
			expression: `device.attributes["driver-a"]["str-list-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{StringValues: []string{"hello", "world"}},
		},
		{
			name:       "semver-list-device",
			expression: `device.attributes["driver-a"]["ver-list-attr"]`,
			device:     mockDevice,
			expectAttr: &resourceapi.DeviceAttribute{VersionValues: []string{"1.0.0", "2.0.0"}},
		},

		// 运行时错误用例组。
		// 运行时错误：异构列表、缺键、类型不匹配与 null。
		{
			name:            "runtime-error-heterogeneous-list-semver",
			expression:      `[dyn(device.attributes["driver-a"]["ver-attr"]), dyn(device.attributes["driver-a"]["str-attr"])]`,
			device:          mockDevice,
			expectEvalError: "expected list element at index 1 to be semver, got string",
		},
		{
			name:            "runtime-error-heterogeneous-list-bool",
			expression:      `[dyn(device.attributes["driver-a"]["bool-attr"]), dyn(device.attributes["driver-a"]["str-attr"])]`,
			device:          mockDevice,
			expectEvalError: "expected list element at index 1 to be bool, got string",
		},
		{
			name:            "runtime-error-heterogeneous-list-int",
			expression:      `[dyn(device.attributes["driver-a"]["int-attr"]), dyn(device.attributes["driver-a"]["str-attr"])]`,
			device:          mockDevice,
			expectEvalError: "expected list element at index 1 to be int, got string",
		},
		{
			name:            "runtime-error-heterogeneous-list-string",
			expression:      `[dyn(device.attributes["driver-a"]["str-attr"]), dyn(device.attributes["driver-a"]["int-attr"])]`,
			device:          mockDevice,
			expectEvalError: "expected list element at index 1 to be string, got int64",
		},
		{
			name:            "runtime-error-attribute-not-found",
			expression:      `device.attributes["driver-a"]["non-existent-attr"]`,
			device:          mockDevice,
			expectEvalError: "no such key",
		},
		{
			name:            "runtime-error-type-mismatch-op",
			expression:      `device.attributes["driver-a"]["int-attr"] + "hello"`, // 尝试将 int 与 string 相加应当失败。
			device:          mockDevice,
			expectEvalError: "no such overload",
		},
		{
			name:            "runtime-error-null-dynamic",
			expression:      `dyn(null)`,
			device:          mockDevice,
			expectEvalError: "unsupported CEL return type: structpb.NullValue",
		},
	}

	// 遍历用例：开启列表属性特性后编译并求值。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 开启列表属性特性，允许列表表达式。
			compiler := GetCompiler(Features{
				EnableListTypeAttributes: true,
			})
			// 以派生属性模式编译。
			result := compiler.CompileCELExpression(tc.expression, Options{
				DerivedAttribute: true,
			})
			// 编译失败直接终止。
			if result.Error != nil {
				t.Fatalf("unexpected compile error: %v", result.Error)
			}

			// 求值并得到 DeviceAttribute。
			attr, _, err := result.EvaluateDerivedAttribute(context.Background(), tc.device)
			// 期望错误时校验错误文本。
			if tc.expectEvalError != "" {
				if err == nil {
					t.Fatalf("expected evaluation error %q, got none", tc.expectEvalError)
				}
				if !strings.Contains(err.Error(), tc.expectEvalError) {
					t.Fatalf("expected evaluation error to contain %q, but got: %v", tc.expectEvalError, err)
				}
				// 否则比对返回属性与期望值。
			} else {
				if err != nil {
					t.Fatalf("unexpected evaluation error: %v", err)
				}
				if diff := cmp.Diff(tc.expectAttr, attr); diff != "" {
					t.Fatalf("unexpected attribute (-want +got):\n%s", diff)
				}
			}
		})
	}
}
