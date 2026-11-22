/*
Copyright 2021 The Kubernetes Authors.

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
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/utils/ptr"

	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/yaml"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/common"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/warning"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

// TestValidationExpressions 测试 CEL 与自定义资源值及 OpenAPIv3 的集成。
func TestValidationExpressions(t *testing.T) {
	tests := []struct {
		name          string
		schema        *schema.Structural
		oldSchema     *schema.Structural
		obj           interface{}
		oldObj        interface{}
		valid         []string
		errors        map[string]string // rule -> 错误信息必须包含的字符串
		costBudget    int64
		isRoot        bool
		expectSkipped bool
		expectedCost  int64
	}{
		// val1 与 val2 相等但 val3 不同的测试用例
		// 相等、比较以及类型相关函数
		{name: "integers",
			// 第 1 个 obj 和 schema 参数对应 "self.val1" 字段，第 2 个对应 "self.val2"，依此类推。
			obj: objs(int64(math.MaxInt64), int64(math.MaxInt64), int32(math.MaxInt32), int32(math.MaxInt32),
				int64(math.MaxInt64), int64(math.MaxInt64)),
			schema: schemas(integerType, integerType, int32Type, int32Type, int64Type, int64Type),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", fmt.Sprintf("%d", int64(math.MaxInt64))),
				ValsEqualThemselvesAndDataLiteral("self.val3", "self.val4", fmt.Sprintf("%d", math.MaxInt32)),
				ValsEqualThemselvesAndDataLiteral("self.val5", "self.val6", fmt.Sprintf("%d", int64(math.MaxInt64))),
				"self.val1 == self.val6", // 未指定 format 的 integer 等同于 int64
				"type(self.val1) == int",
				fmt.Sprintf("self.val3 + 1 == %d + 1", math.MaxInt32), // CEL 的整数是 64 位
				"type(self.val3) == int",
				"type(self.val5) == int",
			},
			errors: map[string]string{
				"self.val1 + 1 == 0": "integer overflow",
				"self.val5 + 1 == 0": "integer overflow",
				"1 / 0 == 1 / 0":     "division by zero",
			},
		},
		{name: "numbers",
			obj: objs(float64(math.MaxFloat64), float64(math.MaxFloat64), float32(math.MaxFloat32), float32(math.MaxFloat32),
				float64(math.MaxFloat64), float64(math.MaxFloat64), int64(1)),
			schema: schemas(numberType, numberType, floatType, floatType, doubleType, doubleType, doubleType),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", fmt.Sprintf("%f", math.MaxFloat64)),
				ValsEqualThemselvesAndDataLiteral("self.val3", "self.val4", fmt.Sprintf("%f", math.MaxFloat32)),
				ValsEqualThemselvesAndDataLiteral("self.val5", "self.val6", fmt.Sprintf("%f", math.MaxFloat64)),
				"self.val1 == self.val6", // 未指定 format 的 number 等同于 float64
				"type(self.val1) == double",
				"type(self.val3) == double",
				"type(self.val5) == double",

				// 对 number 类型的 OpenAPI schema 使用 int64 值，因为整数的浮点表示
				// （如 1.0、0.0）在 yaml 转 json 时可能变成整数表示（如 1、0），
				// 随后被解析为 int64。
				"type(self.val7) == double",
				"self.val7 == 1.0",
			},
		},
		{name: "numeric comparisons",
			obj: objs(
				int64(5),      // val1，integer 类型，整数值
				int64(10),     // val2，integer 类型，整数值
				int64(15),     // val3，integer 类型，整数值
				float64(10.0), // val4，number 类型，由小数面量解析
				float64(10.0), // val5，float 类型，由小数面量解析
				float64(10.0), // val6，double 类型，由小数面量解析
				int64(10),     // val7，number 类型，由整数面量解析
				int64(10),     // val8，float 类型，由整数面量解析
				int64(10),     // val9，double 类型，由整数面量解析
			),
			schema: schemas(integerType, integerType, integerType, numberType, floatType, doubleType, numberType, floatType, doubleType),
			valid: []string{
				// 参考：https://github.com/google/cel-spec/wiki/proposal-210

				// 整数与所有浮点类型比较
				"double(self.val1) < self.val4",
				"double(self.val1) <= self.val4",
				"double(self.val2) <= self.val4",
				"double(self.val2) == self.val4",
				"double(self.val2) >= self.val4",
				"double(self.val3) > self.val4",
				"double(self.val3) >= self.val4",

				"self.val1 < int(self.val4)",
				"self.val2 == int(self.val4)",
				"self.val3 > int(self.val4)",

				"double(self.val1) < self.val5",
				"double(self.val2) == self.val5",
				"double(self.val3) > self.val5",

				"self.val1 < int(self.val5)",
				"self.val2 == int(self.val5)",
				"self.val3 > int(self.val5)",

				"double(self.val1) < self.val6",
				"double(self.val2) == self.val6",
				"double(self.val3) > self.val6",

				"self.val1 < int(self.val6)",
				"self.val2 == int(self.val6)",
				"self.val3 > int(self.val6)",

				// 也与由整数值表示的浮点类型比较，
				// 这是自定义资源从 JSON 解析整数字面量的形式。
				"double(self.val1) < self.val7",
				"double(self.val2) == self.val7",
				"double(self.val3) > self.val7",

				"self.val1 < int(self.val7)",
				"self.val2 == int(self.val7)",
				"self.val3 > int(self.val7)",

				"double(self.val1) < self.val8",
				"double(self.val2) == self.val8",
				"double(self.val3) > self.val8",

				"self.val1 < int(self.val8)",
				"self.val2 == int(self.val8)",
				"self.val3 > int(self.val8)",

				"double(self.val1) < self.val9",
				"double(self.val2) == self.val9",
				"double(self.val3) > self.val9",

				"self.val1 < int(self.val9)",
				"self.val2 == int(self.val9)",
				"self.val3 > int(self.val9)",

				// 比较整数字面量与浮点字面量
				"double(5) < 10.0",
				"double(10) == 10.0",
				"double(15) > 10.0",

				"5 < int(10.0)",
				"10 == int(10.0)",
				"15 > int(10.0)",

				// 整数与浮点字面量比较
				"double(self.val1) < 10.0",
				"double(self.val2) == 10.0",
				"double(self.val3) > 10.0",

				// 跨类型数值比较：整数与所有浮点类型
				"self.val1 < self.val4",
				"self.val1 <= self.val4",
				"self.val2 <= self.val4",
				"self.val2 >= self.val4",
				"self.val3 > self.val4",
				"self.val3 >= self.val4",

				"self.val1 < self.val4",
				"self.val3 > self.val4",

				"self.val1 < self.val5",
				"self.val3 > self.val5",

				"self.val1 < self.val5",
				"self.val3 > self.val5",

				"self.val1 < self.val6",
				"self.val3 > self.val6",

				"self.val1 < self.val6",
				"self.val3 > self.val6",

				// 跨类型数值比较：由整数值表示的浮点类型，
				// 这是自定义资源从 JSON 解析整数字面量的形式。
				"self.val1 < self.val7",
				"self.val3 > self.val7",

				"self.val1 < int(self.val7)",
				"self.val3 > int(self.val7)",

				"self.val1 < self.val8",
				"self.val3 > self.val8",

				"self.val1 < self.val8",
				"self.val3 > self.val8",

				"self.val1 < self.val9",
				"self.val3 > self.val9",

				"self.val1 < self.val9",
				"self.val3 > self.val9",

				// 跨类型数值比较：整数字面量与浮点字面量
				"5 < 10.0",
				"15 > 10.0",

				"5 < 10.0",
				"15 > 10.0",

				// 跨类型数值比较：整数与浮点字面量
				"self.val1 < 10.0",
				"self.val3 > 10.0",
			},
		},
		{name: "unicode strings",
			obj:    objs("Rook takes 👑", "Rook takes 👑"),
			schema: schemas(stringType, stringType),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", "'Rook takes 👑'"),
				"self.val1.startsWith('Rook')",
				"!self.val1.startsWith('knight')",
				"self.val1.contains('takes')",
				"!self.val1.contains('gives')",
				"self.val1.endsWith('👑')",
				"!self.val1.endsWith('pawn')",
				"self.val1.matches('^[^0-9]*$')",
				"!self.val1.matches('^[0-9]*$')",
				"type(self.val1) == string",
				"size(self.val1) == 12",

				// 字符串函数（https://github.com/google/cel-go/blob/v0.9.0/ext/strings.go）
				"self.val1.charAt(3) == 'k'",
				"self.val1.indexOf('o') == 1",
				"self.val1.indexOf('o', 2) == 2",
				"self.val1.replace(' ', 'x') == 'Rookxtakesx👑'",
				"self.val1.replace(' ', 'x', 1) == 'Rookxtakes 👑'",
				"self.val1.split(' ') == ['Rook', 'takes', '👑']",
				"self.val1.split(' ', 2) == ['Rook', 'takes 👑']",
				"self.val1.substring(5) == 'takes 👑'",
				"self.val1.substring(0, 4) == 'Rook'",
				"self.val1.substring(4, 10).trim() == 'takes'",
				"self.val1.upperAscii() == 'ROOK TAKES 👑'",
				"self.val1.lowerAscii() == 'rook takes 👑'",

				"'%d %s %f %s %s'.format([1, 'abc', 1.0, duration('1m'), timestamp('2000-01-01T00:00:00.000Z')]) == '1 abc 1.000000 60s 2000-01-01T00:00:00Z'",
				"'%e'.format([3.14]) == '3.140000×10⁰⁰'",
				"'%o %o %o'.format([7, 8, 9]) == '7 10 11'",
				"'%b %b %b'.format([7, 8, 9]) == '111 1000 1001'",
			},
			errors: map[string]string{
				// 字符串常量正则模式的非法正则会成为编译期错误
				"self.val1.matches(')')": "compile error: compilation failed: ERROR: <input>:1:19: invalid matches argument",
			},
		},
		{name: "escaped strings",
			obj:    objs("l1\nl2", "l1\nl2"),
			schema: schemas(stringType, stringType),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", "'l1\\nl2'"),
				"self.val1 == '''l1\nl2'''",
			},
		},
		{name: "bytes",
			obj:    objs("QUI=", "QUI="),
			schema: schemas(byteType, byteType),
			valid: []string{
				"self.val1 == self.val2",
				"self.val1 == b'AB'",
				"self.val1 == bytes('AB')",
				"self.val1 == b'\\x41\\x42'",
				"type(self.val1) == bytes",
				"size(self.val1) == 2",
			},
		},
		{name: "booleans",
			obj:    objs(true, true, false, false),
			schema: schemas(booleanType, booleanType, booleanType, booleanType),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", "true"),
				ValsEqualThemselvesAndDataLiteral("self.val3", "self.val4", "false"),
				"self.val1 != self.val4",
				"type(self.val1) == bool",
			},
		},
		{name: "duration format",
			obj:    objs("1h2m3s4ms", "1h2m3s4ms"),
			schema: schemas(durationFormat, durationFormat),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", "duration('1h2m3s4ms')"),
				"self.val1 == duration('1h2m') + duration('3s4ms')",
				"self.val1.getHours() == 1",
				"self.val1.getMinutes() == 62",
				"self.val1.getSeconds() == 3723",
				"self.val1.getMilliseconds() == 3723004",
				"type(self.val1) == google.protobuf.Duration",
			},
			errors: map[string]string{
				"duration('1')":                      "compilation failed: ERROR: <input>:1:10: invalid duration argument",
				"duration('1d')":                     "compilation failed: ERROR: <input>:1:10: invalid duration argument",
				"duration('1us') < duration('1nns')": "compilation failed: ERROR: <input>:1:28: invalid duration argument",
			},
		},
		{name: "date format",
			obj:    objs("1997-07-16", "1997-07-16"),
			schema: schemas(dateFormat, dateFormat),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", "timestamp('1997-07-16T00:00:00.000Z')"),
				"self.val1.getDate() == 16",
				"self.val1.getMonth() == 06", // 索引从 0 开始
				"self.val1.getFullYear() == 1997",
				"type(self.val1) == google.protobuf.Timestamp",
			},
		},
		{name: "date-time format",
			obj:    objs("2011-08-18T19:03:37.010000000+01:00", "2011-08-18T19:03:37.010000000+01:00"),
			schema: schemas(dateTimeFormat, dateTimeFormat),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", "timestamp('2011-08-18T19:03:37.010+01:00')"),
				"self.val1 == timestamp('2011-08-18T00:00:00.000+01:00') + duration('19h3m37s10ms')",
				"self.val1.getDate('01:00') == 18",
				"self.val1.getMonth('01:00') == 7", // 索引从 0 开始
				"self.val1.getFullYear('01:00') == 2011",
				"self.val1.getHours('01:00') == 19",
				"self.val1.getMinutes('01:00') == 03",
				"self.val1.getSeconds('01:00') == 37",
				"self.val1.getMilliseconds('01:00') == 10",
				"self.val1.getHours('UTC') == 18", // 字符串中的时区比 UTC 早 1 小时
				"type(self.val1) == google.protobuf.Timestamp",
			},
			errors: map[string]string{
				"timestamp('1000-00-00T00:00:00Z')":  "compilation failed: ERROR: <input>:1:11: invalid timestamp",
				"timestamp('1000-01-01T00:00:00ZZ')": "compilation failed: ERROR: <input>:1:11: invalid timestamp",
				"timestamp(-62135596801)":            "compilation failed: ERROR: <input>:1:11: invalid timestamp",
			},
		},
		{name: "enums",
			obj: map[string]interface{}{"enumStr": "Pending"},
			schema: objectTypePtr(map[string]schema.Structural{"enumStr": {
				Generic: schema.Generic{
					Type: "string",
				},
				ValueValidation: &schema.ValueValidation{
					Enum: []schema.JSON{
						{Object: "Pending"},
						{Object: "Available"},
						{Object: "Bound"},
						{Object: "Released"},
						{Object: "Failed"},
					},
				},
			}}),
			valid: []string{
				"self.enumStr == 'Pending'",
				"self.enumStr in ['Pending', 'Available']",
			},
		},
		{name: "conversions",
			obj:    objs(int64(10), 10.0, 10.49, 10.5, true, "10", "MTA=", "3723.004s", "1h2m3s4ms", "2011-08-18T19:03:37.01+01:00", "2011-08-18T19:03:37.01+01:00", "2011-08-18T00:00:00Z", "2011-08-18"),
			schema: schemas(integerType, numberType, numberType, numberType, booleanType, stringType, byteType, stringType, durationFormat, stringType, dateTimeFormat, stringType, dateFormat),
			valid: []string{
				"int(self.val2) == self.val1",
				"int(self.val3) == self.val1",
				"int(self.val4) == self.val1",
				"int(self.val6) == self.val1",
				"double(self.val1) == self.val2",
				"double(self.val6) == self.val2",
				"bytes(self.val6) == self.val7",
				"string(self.val1) == self.val6",
				"string(self.val2) == '10'",
				"string(self.val3) == '10.49'",
				"string(self.val4) == '10.5'",
				"string(self.val5) == 'true'",
				"string(self.val7) == self.val6",
				"duration(self.val8) == self.val9",
				"string(self.val9) == self.val8",
				"timestamp(self.val10) == self.val11",
				"string(self.val11) == self.val10",
				"timestamp(self.val12) == self.val13",
				"string(self.val13) == self.val12",
			},
		},
		{name: "lists",
			obj:    objs([]interface{}{1, 2, 3}, []interface{}{1, 2, 3}),
			schema: schemas(listType(&integerType), listType(&integerType)),
			valid: []string{
				ValsEqualThemselvesAndDataLiteral("self.val1", "self.val2", "[1, 2, 3]"),
				"1 in self.val1",
				"self.val2[0] in self.val1",
				"!(0 in self.val1)",
				"self.val1 + self.val2 == [1, 2, 3, 1, 2, 3]",
				"self.val1 + [4, 5] == [1, 2, 3, 4, 5]",
			},
			errors: map[string]string{
				// 由于启用了 HomogeneousAggregateLiterals，不允许混合类型列表
				"[1, 'a', false].filter(x, string(x) == 'a')": "compilation failed: ERROR: <input>:1:5: expected type 'int' but found 'string'",
			},
		},
		{name: "ext lists version 3",
			obj:    objs([]interface{}{1, 2, 3}, []interface{}{1, 2, 3}),
			schema: schemas(listType(&integerType), listType(&integerType)),
			valid: []string{
				`lists.range(4) == [0,1,2,3]`,
				`lists.range(0) == []`,
				`[5,1,2,3].reverse() == [3,2,1,5]`,
				`[].reverse() == []`,
				`[1].reverse() == [1]`,
				`['are', 'you', 'as', 'bored', 'as', 'I', 'am'].reverse() == ['am', 'I', 'as', 'bored', 'as', 'you', 'are']`,
				`[false, true, true].reverse().reverse() == [false, true, true]`,
				`[1,2,3,4].slice(0, 4) == [1,2,3,4]`,
				`[1,2,3,4].slice(0, 0) == []`,
				`[1,2,3,4].slice(1, 1) == []`,
				`[1,2,3,4].slice(4, 4) == []`,
				`[1,2,3,4].slice(1, 3) == [2, 3]`,
				`dyn([]).flatten() == []`,
				`dyn([1,2,3,4]).flatten() == [1,2,3,4]`,
				`[].sort() == []`,
				`[1].sort() == [1]`,
				`[4, 3, 2, 1].sort() == [1, 2, 3, 4]`,
				`["d", "a", "b", "c"].sort() == ["a", "b", "c", "d"]`,
				`[].sortBy(e, e) == []`,
				`["a"].sortBy(e, e) == ["a"]`,
				`[-3, 1, -5, -2, 4].sortBy(e, -(e * e)) == [-5, 4, -3, -2, 1]`,
				`[-3, 1, -5, -2, 4].map(e, e * 2).sortBy(e, -(e * e)) == [-10, 8, -6, -4, 2]`,
				`lists.range(3).sortBy(e, -e) == [2, 1, 0]`,
				`["a", "c", "b", "first"].sortBy(e, e == "first" ? "" : e) == ["first", "a", "b", "c"]`,
				`[{'name': 'foo'}, {'name': 'bar'}, {'name': 'baz'}].sortBy(e, e.name) == [{'name': 'bar'}, {'name': 'baz'}, {'name': 'foo'}]`,
				`[].distinct() == []`,
				`[1].distinct() == [1]`,
				`[-2, 5, -2, 1, 1, 5, -2, 1].distinct() == [-2, 5, 1]`,
				`['c', 'a', 'a', 'b', 'a', 'b', 'c', 'c'].distinct() == ['c', 'a', 'b']`,
				`[[1], [1], [2]].distinct() == [[1], [2]]`,
				`[{'name': 'a'}, {'name': 'b'}, {'name': 'a'}].distinct() == [{'name': 'a'}, {'name': 'b'}]`,
				`[[1],[2],[3,4]].flatten() == [1,2,3,4]`,
			},
			errors: map[string]string{
				`[1,2,3,4].slice(3, 0) != [1,2]`:                  "cannot slice(3, 0), start index must be less than or equal to end index",
				`[1,2,3,4].slice(0, 10) != [1,2]`:                 "cannot slice(0, 10), list is length 4",
				`[1,2,3,4].slice(-5, 10) != [1,2]`:                "cannot slice(-5, 10), negative indexes not supported",
				`[1,2,3,4].slice(-5, -3) != [1,2]`:                "cannot slice(-5, -3), negative indexes not supported",
				`[[1],[2],[3,4]].flatten(-1) == [1,2,3,4]`:        "level must be non-negative evaluating rule: [[1],[2],[3,4]].flatten(-1) == [1,2,3,4]",
				`["d", 3, 2, "c"].sort() == ["a", "b", "c", "d"]`: "expected type 'string' but found 'int'",
			},
		},
		{name: "string lists",
			obj:    objs([]interface{}{"a", "b", "c"}),
			schema: schemas(listType(&stringType)),
			valid: []string{
				// join 函数
				"self.val1.join('-') == 'a-b-c'",
				"['a', 'b', 'c'].join('-') == 'a-b-c'",
				"self.val1.join() == 'abc'",
				"['a', 'b', 'c'].join() == 'abc'",
			},
		},
		{name: "listSets",
			obj:    objs([]interface{}{"a", "b", "c"}, []interface{}{"a", "c", "b"}),
			schema: schemas(listSetType(&stringType), listSetType(&stringType)),
			valid: []string{
				// 顺序不同但相等
				"self.val1 == ['c', 'b', 'a']",
				"self.val1 == self.val2",
				"'a' in self.val1",
				"self.val2[0] in self.val1",
				"!('x' in self.val1)",
				"self.val1 + self.val2 == ['a', 'b', 'c']",
				"self.val1 + ['c', 'd'] == ['a', 'b', 'c', 'd']",

				// join 函数
				"self.val1.join('-') == 'a-b-c'",
				"['a', 'b', 'c'].join('-') == 'a-b-c'",
				"self.val1.join() == 'abc'",
				"['a', 'b', 'c'].join() == 'abc'",

				// CEL 集合函数
				"sets.contains(['a', 'b'], [])",
				"sets.contains(['a', 'b'], ['b'])",
				"!sets.contains(['a', 'b'], ['c'])",
				"sets.equivalent([], [])",
				"sets.equivalent(['c', 'b', 'a'], ['b', 'c', 'a'])",
				"!sets.equivalent(['a', 'b'], ['b', 'c'])",
				"sets.intersects(['a', 'b'], ['b', 'c'])",
				"!sets.intersects([], [])",
				"!sets.intersects(['a', 'b'], [])",
				"!sets.intersects(['a', 'b'], ['c', 'd'])",

				"sets.contains([1, 2], [2])",
				"sets.contains([true, false], [false])",
				"sets.contains([1.25, 1.5], [1.5])",
				"sets.contains([{'a': 1}, {'b': 2}], [{'b': 2}])",
				"sets.contains([[1, 2], [3, 4]], [[3, 4]])",
				"sets.contains([timestamp('2000-01-01T00:00:00.000+01:00'), timestamp('2012-01-01T00:00:00.000+01:00')], [timestamp('2012-01-01T00:00:00.000+01:00')])",
				"sets.contains([duration('1h'), duration('2h')], [duration('2h')])",

				"sets.equivalent([1, 2], [1, 2])",
				"sets.equivalent([true, false], [true, false])",
				"sets.equivalent([1.25, 1.5], [1.25, 1.5])",
				"sets.equivalent([{'a': 1}, {'b': 2}], [{'a': 1}, {'b': 2}])",
				"sets.equivalent([[1, 2], [3, 4]], [[1, 2], [3, 4]])",
				"sets.equivalent([timestamp('2012-01-01T00:00:00.000+01:00')], [timestamp('2012-01-01T00:00:00.000+01:00')])",
				"sets.equivalent([duration('1h'), duration('2h')], [duration('1h'), duration('2h')])",

				"sets.intersects([1, 2], [2])",
				"sets.intersects([true, false], [false])",
				"sets.intersects([1.25, 1.5], [1.5])",
				"sets.intersects([{'a': 1}, {'b': 2}], [{'b': 2}])",
				"sets.intersects([[1, 2], [3, 4]], [[3, 4]])",
				"sets.intersects([timestamp('2000-01-01T00:00:00.000+01:00'), timestamp('2012-01-01T00:00:00.000+01:00')], [timestamp('2012-01-01T00:00:00.000+01:00')])",
				"sets.intersects([duration('1h'), duration('2h')], [duration('2h')])",
			},
		},
		{name: "listMaps",
			obj: map[string]interface{}{
				"objs": []interface{}{
					[]interface{}{
						map[string]interface{}{"k": "a", "v": "1"},
						map[string]interface{}{"k": "b", "v": "2"},
					},
					[]interface{}{
						map[string]interface{}{"k": "b", "v": "2"},
						map[string]interface{}{"k": "a", "v": "1"},
					},
					[]interface{}{
						map[string]interface{}{"k": "b", "v": "3"},
						map[string]interface{}{"k": "a", "v": "1"},
					},
					[]interface{}{
						map[string]interface{}{"k": "c", "v": "4"},
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"objs": listType(listMapTypePtr([]string{"k"}, objectTypePtr(map[string]schema.Structural{
					"k": stringType,
					"v": stringType,
				}))),
			}),
			valid: []string{
				"self.objs[0] == self.objs[1]",                // 顺序不同但相等
				"self.objs[0] + self.objs[2] == self.objs[2]", // 右侧覆盖左侧的值
				"self.objs[2] + self.objs[0] == self.objs[0]",

				"self.objs[0] == [self.objs[0][0], self.objs[0][1]]", // 与声明的列表相等
				"self.objs[0] == [self.objs[0][1], self.objs[0][0]]",

				"self.objs[2] + [self.objs[0][0], self.objs[0][1]] == self.objs[0]", // 与声明的列表拼接
				"size(self.objs[0] + [self.objs[3][0]]) == 3",
			},
			errors: map[string]string{
				"self.objs[0] == {'k': 'a', 'v': '1'}": "no matching overload for '_==_'", // 对象不能与数据字面量 map 比较
			},
		},
		{name: "maps",
			obj:    objs(map[string]interface{}{"k1": "a", "k2": "b"}, map[string]interface{}{"k2": "b", "k1": "a"}),
			schema: schemas(mapType(&stringType), mapType(&stringType)),
			valid: []string{
				"self.val1 == self.val2", // 顺序不同但相等
				"'k1' in self.val1",
				"!('k3' in self.val1)",
				"self.val1 == {'k1': 'a', 'k2': 'b'}",
			},
			errors: map[string]string{
				// 由于启用了 HomogeneousAggregateLiterals，不允许混合类型 map
				"{'k1': 'a', 'k2': 1, 'k2': false}": "expected type 'string' but found 'int'",
			},
		},
		{name: "objects",
			obj: map[string]interface{}{
				"objs": []interface{}{
					map[string]interface{}{"f1": "a", "f2": "b"},
					map[string]interface{}{"f1": "a", "f2": "b"},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"objs": listType(objectTypePtr(map[string]schema.Structural{
					"f1": stringType,
					"f2": stringType,
				})),
			}),
			valid: []string{
				"self.objs[0] == self.objs[1]",
			},
			errors: map[string]string{
				"self.objs[0] == {'f1': 'a', 'f2': 'b'}": "found no matching overload for '_==_'", // 对象不能与数据字面量 map 比较
			},
		},
		{name: "object access",
			obj: map[string]interface{}{
				"a": map[string]interface{}{
					"b": 1,
					"d": nil,
				},
				"a1": map[string]interface{}{
					"b1": map[string]interface{}{
						"c1": 4,
					},
				},
				"a3": map[string]interface{}{},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"a": objectType(map[string]schema.Structural{
					"b": integerType,
					"c": integerType,
					"d": withNullable(true, integerType),
				}),
				"a1": objectType(map[string]schema.Structural{
					"b1": objectType(map[string]schema.Structural{
						"c1": integerType,
					}),
					"d2": objectType(map[string]schema.Structural{
						"e2": integerType,
					}),
				}),
			}),
			// 参考：https://github.com/google/cel-spec/blob/master/doc/langdef.md#field-selection
			valid: []string{
				"has(self.a.b)",
				"!has(self.a.c)",
				"!has(self.a.d)", // 在 CEL 中，null 值字段与缺失字段同等对待
				"has(self.a1.b1.c1)",
				"!(has(self.a1.d2) && has(self.a1.d2.e2))", // 必须检查中间的可选字段（见下文 d2 的 no such key 错误）
				"!has(self.a1.d2)",
			},
			errors: map[string]string{
				"has(self.a.z)":      "undefined field 'z'",             // 不能引用未定义字段，即使通过 has 也不行
				"self.a['b'] == 1":   "no matching overload for '_[_]'", // 仅允许用于 map，不允许用于 object
				"has(self.a1.d2.e2)": "no such key: d2",                 // has 只检查路径最后一段，值中缺少 d2 时会报错
			},
		},
		{name: "map access",
			obj: map[string]interface{}{
				"val": map[string]interface{}{
					"b": 1,
					"d": 2,
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"val": mapType(&integerType),
			}),
			valid: []string{
				// 惯用的 map 访问方式
				"!('a' in self.val)",
				"'b' in self.val",
				"!('c' in self.val)",
				"'d' in self.val",
				// 若 map 键是合法的 CEL 标识符，也可用字段选择访问
				"!has(self.val.a)",
				"has(self.val.b)",
				"!has(self.val.c)",
				"has(self.val.d)",
				"self.val.all(k, self.val[k] > 0)",
				"self.val.exists(k, self.val[k] > 1)",
				"self.val.exists_one(k, self.val[k] == 2)",
				"!self.val.exists(k, self.val[k] > 2)",
				"!self.val.exists_one(k, self.val[k] > 0)",
				"size(self.val) == 2",
				"self.val.map(k, self.val[k]).exists(v, v == 1)",
				"size(self.val.filter(k, self.val[k] > 1)) == 1",

				// 双变量推导式
				"self.val.all(k, v, v > 0)",
				"self.val.exists(k, v, v == 2)",
				"self.val.existsOne(k, v, v == 2)",
				"self.val.transformMap(k, v, v > 1, v + 1).size() == 1",
				"self.val.transformMap(k, v, v + 1).size() == 2",
				"self.val.transformMapEntry(k, v, v > 1, {k + '2': v + 1}).size() == 1",
				"self.val.transformMapEntry(k, v, {k + '2': v + 1}).size() == 2",
			},
			errors: map[string]string{
				"self.val['c'] == 1": "no such key: c",
			},
		},
		{name: "listMap access",
			obj: map[string]interface{}{
				"listMap": []interface{}{
					map[string]interface{}{"k": "a1", "v": "b1"},
					map[string]interface{}{"k": "a2", "v": "b2"},
					map[string]interface{}{"k": "a3", "v": "b3", "v2": "z"},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"listMap": listMapType([]string{"k"}, objectTypePtr(map[string]schema.Structural{
					"k":  stringType,
					"v":  stringType,
					"v2": stringType,
				})),
			}),
			valid: []string{
				"has(self.listMap[0].v)",
				"self.listMap.all(m, m.k.startsWith('a'))",
				"self.listMap.all(m, !has(m.v2) || m.v2 == 'z')",
				"self.listMap.exists(m, m.k.endsWith('1'))",
				"self.listMap.exists_one(m, m.k == 'a3')",
				"!self.listMap.all(m, m.k.endsWith('1'))",
				"!self.listMap.exists(m, m.v == 'x')",
				"!self.listMap.exists_one(m, m.k.startsWith('a'))",
				"size(self.listMap.filter(m, m.k == 'a1')) == 1",
				"self.listMap.exists(m, m.k == 'a1' && m.v == 'b1')",
				"self.listMap.map(m, m.v).exists(v, v == 'b1')",

				// 测试谓词中使用的字段在除一个元素外均未设置时的推导式：
				// - 使用 has 检查：

				"self.listMap.exists(m, has(m.v2) && m.v2 == 'z')",
				"!self.listMap.all(m, has(m.v2) && m.v2 != 'z')",
				"self.listMap.exists_one(m, has(m.v2) && m.v2 == 'z')",
				"self.listMap.filter(m, has(m.v2) && m.v2 == 'z').size() == 1",
				// map 的未文档化重载，接受过滤参数，等同于 .filter().map()
				"self.listMap.map(m, has(m.v2) && m.v2 == 'z', m.v2).size() == 1",
				"self.listMap.filter(m, has(m.v2) && m.v2 == 'z').map(m, m.v2).size() == 1",
				// - 不使用 has 检查：

				// 只要条件至少对一个元素成立，all() 与 exists() 宏会忽略谓词产生的错误
				"self.listMap.exists(m, m.v2 == 'z')",
				"!self.listMap.all(m, m.v2 != 'z')",

				// 双变量推导式
				"!self.listMap.all(i, m, has(m.v2) && m.v2 != 'z')",
				"self.listMap.exists(i, m, has(m.v2) && m.v2 == 'z')",
				"self.listMap.existsOne(i, m, has(m.v2) && m.v2 == 'z')",
				"self.listMap.transformList(i, m, has(m.v2) && m.v2 == 'z', m.v2).size() == 1",
				"self.listMap.transformList(i, m, m.v).size() == 3",
			},
			errors: map[string]string{
				// 测试谓词字段在除一个元素外均未设置时的推导式：（错误用例）
				// - 不使用 has 检查：

				// 若 all() 谓词对所有元素都为 false 或出错，则抛出遇到的错误
				"self.listMap.all(m, m.v2 == 'z')": "no such key: v2",
				// exists_one() 比 map() 或 exists() 更严格：它要求恰好一个谓词为 true、其余为 false，
				// 遇到任何错误都会抛出
				"self.listMap.exists_one(m, m.v2 == 'z')": "no such key: v2",
				// filter 与 map 会抛出遇到的任何错误
				"self.listMap.filter(m, m.v2 == 'z').size() == 1": "no such key: v2",
				"self.listMap.map(m, m.v2).size() == 1":           "no such key: v2",
				// map 的未文档化重载，接受过滤参数，等同于 .filter().map()
				"self.listMap.map(m, m.v2 == 'z', m.v2).size() == 1": "no such key: v2",
			},
		},
		{name: "list access",
			obj: map[string]interface{}{
				"array": []interface{}{1, 1, 2, 2, 3, 3, 4, 5},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"array": listType(&integerType),
			}),
			valid: []string{
				"2 in self.array",
				"self.array.all(e, e > 0)",
				"self.array.exists(e, e > 2)",
				"self.array.exists_one(e, e > 4)",
				"!self.array.all(e, e < 2)",
				"!self.array.exists(e, e < 0)",
				"!self.array.exists_one(e, e == 2)",
				"self.array.all(e, e < 100)",
				"size(self.array.filter(e, e%2 == 0)) == 3",
				"self.array.map(e, e * 20).filter(e, e > 50).exists(e, e == 60)",
				"size(self.array) == 8",

				// 双变量推导式
				"self.array.all(i, e, e > 0)",
				"self.array.exists(i, e, e > 2)",
				"self.array.existsOne(i, e, e > 4)",
				"self.array.transformList(i, e, e > 2, e * 2).size() > 0",
				"self.array.transformList(i, e, e * 2).size() > 0",
			},
			errors: map[string]string{
				"self.array[100] == 0": "index out of bounds: 100",
			},
		},
		{name: "listSet access",
			obj: map[string]interface{}{
				"set": []interface{}{1, 2, 3, 4, 5},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"set": listType(&integerType),
			}),
			valid: []string{
				"3 in self.set",
				"self.set.all(e, e > 0)",
				"self.set.exists(e, e > 3)",
				"self.set.exists_one(e, e == 3)",
				"!self.set.all(e, e < 3)",
				"!self.set.exists(e, e < 0)",
				"!self.set.exists_one(e, e > 3)",
				"self.set.all(e, e < 10)",
				"size(self.set.filter(e, e%2 == 0)) == 2",
				"self.set.map(e, e * 20).filter(e, e > 50).exists_one(e, e == 60)",
				"size(self.set) == 5",
			},
		},
		{name: "typemeta and objectmeta access specified",
			obj: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"name":         "foo",
					"generateName": "pickItForMe",
					"namespace":    "xyz",
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"kind":       stringType,
				"apiVersion": stringType,
				"metadata": objectType(map[string]schema.Structural{
					"name":         stringType,
					"generateName": stringType,
				}),
			}),
			valid: []string{
				"self.kind == 'Pod'",
				"self.apiVersion == 'v1'",
				"self.metadata.name == 'foo'",
				"self.metadata.generateName == 'pickItForMe'",
			},
			errors: map[string]string{
				"has(self.metadata.namespace)": "undefined field 'namespace'",
			},
		},
		{name: "typemeta and objectmeta access not specified",
			isRoot: true,
			obj: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"name":         "foo",
					"generateName": "pickItForMe",
					"namespace":    "xyz",
				},
				"spec": map[string]interface{}{
					"field1": "a",
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"spec": objectType(map[string]schema.Structural{
					"field1": stringType,
				}),
			}),
			valid: []string{
				"self.kind == 'Pod'",
				"self.apiVersion == 'v1'",
				"self.metadata.name == 'foo'",
				"self.metadata.generateName == 'pickItForMe'",
				"self.spec.field1 == 'a'",
			},
			errors: map[string]string{
				"has(self.metadata.namespace)": "undefined field 'namespace'",
			},
		},

		// Kubernetes 特殊类型
		{name: "embedded object",
			obj: map[string]interface{}{
				"embedded": map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]interface{}{
						"name":         "foo",
						"generateName": "pickItForMe",
						"namespace":    "xyz",
					},
					"spec": map[string]interface{}{
						"field1": "a",
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"embedded": {
					Generic: schema.Generic{Type: "object"},
					Extensions: schema.Extensions{
						XEmbeddedResource: true,
					},
				},
			}),
			valid: []string{
				// 'kind'、'apiVersion'、'metadata.name' 与 'metadata.generateName' 始终可访问，
				// 即使未在 schema 中声明。
				"self.embedded.kind == 'Pod'",
				"self.embedded.apiVersion == 'v1'",
				"self.embedded.metadata.name == 'foo'",
				"self.embedded.metadata.generateName == 'pickItForMe'",
			},
			// 只有 schema 中声明的字段才能在 CEL 表达式中被字段选择
			errors: map[string]string{
				"has(self.embedded.metadata.namespace)": "undefined field 'namespace'",
				"has(self.embedded.spec)":               "undefined field 'spec'",
			},
		},
		{name: "embedded object with properties",
			obj: map[string]interface{}{
				"embedded": map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]interface{}{
						"name":         "foo",
						"generateName": "pickItForMe",
						"namespace":    "xyz",
					},
					"spec": map[string]interface{}{
						"field1": "a",
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"embedded": {
					Generic: schema.Generic{Type: "object"},
					Extensions: schema.Extensions{
						XEmbeddedResource: true,
					},
					Properties: map[string]schema.Structural{
						"kind":       stringType,
						"apiVersion": stringType,
						"metadata": objectType(map[string]schema.Structural{
							"name":         stringType,
							"generateName": stringType,
						}),
						"spec": objectType(map[string]schema.Structural{
							"field1": stringType,
						}),
					},
				},
			}),
			valid: []string{
				// 此例中 'kind'、'apiVersion'、'metadata.name' 与 'metadata.generateName' 已在 schema 中声明，
				// 但即使未声明它们也可访问
				"self.embedded.kind == 'Pod'",
				"self.embedded.apiVersion == 'v1'",
				"self.embedded.metadata.name == 'foo'",
				"self.embedded.metadata.generateName == 'pickItForMe'",
				// 已声明的内嵌字段可访问
				"self.embedded.spec.field1 == 'a'",
			},
			errors: map[string]string{
				// metadata 上只有 name 与 generateName 可访问
				"has(self.embedded.metadata.namespace)": "undefined field 'namespace'",
			},
		},
		{name: "embedded object with usage of reserved keywords",
			obj: map[string]interface{}{
				"embedded": map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]interface{}{
						"name":         "foo",
						"generateName": "pickItForMe",
						"namespace":    "reserved_keyword_namespace",
					},
					"spec": map[string]interface{}{
						"if": "reserved_keyword_if",
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"embedded": {
					Generic: schema.Generic{Type: "object"},
					Extensions: schema.Extensions{
						XEmbeddedResource: true,
					},
					Properties: map[string]schema.Structural{
						"kind":       stringType,
						"apiVersion": stringType,
						"metadata": objectType(map[string]schema.Structural{
							"name":         stringType,
							"generateName": stringType,
							"namespace":    stringType,
						}),
						"spec": objectType(map[string]schema.Structural{
							"if": stringType,
						}),
					},
				},
			}),
			valid: []string{
				"has(self.embedded.metadata.namespace)",
				"self.embedded.metadata.namespace == 'reserved_keyword_namespace'",
				"has(self.embedded.spec.if)",
				"self.embedded.spec.if == 'reserved_keyword_if'",
			},
		},
		{name: "embedded object with preserve unknown",
			obj: map[string]interface{}{
				"embedded": map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]interface{}{
						"name":         "foo",
						"generateName": "pickItForMe",
						"namespace":    "xyz",
					},
					"spec": map[string]interface{}{
						"field1": "a",
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"embedded": {
					Generic: schema.Generic{Type: "object"},
					Extensions: schema.Extensions{
						XPreserveUnknownFields: true,
						XEmbeddedResource:      true,
					},
				},
			}),
			valid: []string{
				// 'kind'、'apiVersion'、'metadata.name' 与 'metadata.generateName' 始终可访问，
				// 即使未在 schema 中声明，也与是否设置 x-kubernetes-preserve-unknown-fields 无关。
				"self.embedded.kind == 'Pod'",
				"self.embedded.apiVersion == 'v1'",
				"self.embedded.metadata.name == 'foo'",
				"self.embedded.metadata.generateName == 'pickItForMe'",

				// 对象存在
				"has(self.embedded)",
			},
			// 只有 schema 中声明的字段才能在 CEL 表达式中被字段选择，
			// 与是否设置 x-kubernetes-preserve-unknown-fields 无关。
			errors: map[string]string{
				"has(self.embedded.spec)": "undefined field 'spec'",
			},
		},
		{name: "string in intOrString",
			obj: map[string]interface{}{
				"something": "25%",
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"something": intOrStringType(),
			}),
			valid: []string{
				// 在 Kubernetes 1.24 及以后，int-or-string 与其他类型比较时 CEL 类型返回 false，
				// 因此可以安全地编写如下校验规则：
				"self.something == '25%'",
				"self.something != 1",
				"self.something == 1 || self.something == '25%'",
				"self.something == '25%' || self.something == 1",

				// 在 Kubernetes 1.23 及以前，所有 int-or-string 访问都必须先用类型检查保护，
				// 以避免字符串与整数类型做相等比较时出现运行时错误。
				"type(self.something) == string && self.something == '25%'",
				"type(self.something) == int ? self.something == 1 : self.something == '25%'",

				// 由于该类型是动态的，没有类型检查，与其他类型比较时
				// 在运行时求值为 false。
				"self.something != ['anything']",
			},
		},
		{name: "int in intOrString",
			obj: map[string]interface{}{
				"something": int64(1),
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"something": intOrStringType(),
			}),
			valid: []string{
				// 在 Kubernetes 1.24 及以后，int-or-string 与其他类型比较时 CEL 类型返回 false，
				// 因此可以安全地编写如下校验规则：
				"self.something == 1",
				"self.something != 'some string'",
				"self.something == 1 || self.something == '25%'",
				"self.something == '25%' || self.something == 1",

				// 在 Kubernetes 1.23 及以前，所有 int-or-string 访问都必须先用类型检查保护，
				// 以避免字符串与整数类型做相等比较时出现运行时错误。
				"type(self.something) == int && self.something == 1",
				"type(self.something) == int ? self.something == 1 : self.something == '25%'",

				// 由于该类型是动态的，没有类型检查，与其他类型比较时
				// 在运行时求值为 false。
				"self.something != ['anything']",
			},
		},
		{name: "null in intOrString",
			obj: map[string]interface{}{
				"something": nil,
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"something": withNullable(true, intOrStringType()),
			}),
			valid: []string{
				"!has(self.something)",
			},
			errors: map[string]string{
				"type(self.something) == int": "no such key",
			},
		},
		{name: "percent comparison using intOrString",
			obj: map[string]interface{}{
				"min":       "50%",
				"current":   5,
				"available": 10,
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"min":       intOrStringType(),
				"current":   integerType,
				"available": integerType,
			}),
			valid: []string{
				// 校验 'min' 为字符串时必须是百分比
				`type(self.min) == string && self.min.matches(r'(\d+(\.\d+)?%)')`,
				// 校验 'min' 可以是最小精确值，也可以是 'available' 的百分比最小值
				"type(self.min) == int ? self.current <= self.min : double(self.current) / double(self.available) >= double(self.min.replace('%', '')) / 100.0",
			},
		},
		{name: "preserve unknown fields",
			obj: map[string]interface{}{
				"withUnknown": map[string]interface{}{
					"field1": "a",
					"field2": "b",
				},
				"withUnknownList": []interface{}{
					map[string]interface{}{
						"field1": "a",
						"field2": "b",
					},
					map[string]interface{}{
						"field1": "x",
						"field2": "y",
					},
					map[string]interface{}{
						"field1": "x",
						"field2": "y",
					},
					map[string]interface{}{},
					map[string]interface{}{},
				},
				"withUnknownFieldList": []interface{}{
					map[string]interface{}{
						"fieldOfUnknownType": "a",
					},
					map[string]interface{}{
						"fieldOfUnknownType": 1,
					},
					map[string]interface{}{
						"fieldOfUnknownType": 1,
					},
				},
				"anyvalList":   []interface{}{"a", 2},
				"anyvalMap":    map[string]interface{}{"k": "1"},
				"anyvalField1": 1,
				"anyvalField2": "a",
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"withUnknown": {
					Generic: schema.Generic{Type: "object"},
					Extensions: schema.Extensions{
						XPreserveUnknownFields: true,
					},
				},
				"withUnknownList": listType(&schema.Structural{
					Generic: schema.Generic{Type: "object"},
					Extensions: schema.Extensions{
						XPreserveUnknownFields: true,
					},
				}),
				"withUnknownFieldList": listType(&schema.Structural{
					Generic: schema.Generic{Type: "object"},
					Properties: map[string]schema.Structural{
						"fieldOfUnknownType": {
							Extensions: schema.Extensions{
								XPreserveUnknownFields: true,
							},
						},
					},
				}),
				"anyvalList": listType(&schema.Structural{
					Extensions: schema.Extensions{
						XPreserveUnknownFields: true,
					},
				}),
				"anyvalMap": mapType(&schema.Structural{
					Extensions: schema.Extensions{
						XPreserveUnknownFields: true,
					},
				}),
				"anyvalField1": {
					Extensions: schema.Extensions{
						XPreserveUnknownFields: true,
					},
				},
				"anyvalField2": {
					Extensions: schema.Extensions{
						XPreserveUnknownFields: true,
					},
				},
			}),
			valid: []string{
				"has(self.withUnknown)",
				"self.withUnknownList.size() == 5",
				// 因未在对象 schema 中声明而未知的字段也会参与相等性检查
				"self.withUnknownList[0] != self.withUnknownList[1]",
				"self.withUnknownList[1] == self.withUnknownList[2]",
				"self.withUnknownList[3] == self.withUnknownList[4]",

				// 对象 schema 中已声明、但字段 schema 未知的字段同样参与相等性检查
				"self.withUnknownFieldList[0] != self.withUnknownFieldList[1]",
				"self.withUnknownFieldList[1] == self.withUnknownFieldList[2]",
			},
			errors: map[string]string{
				// 未知字段不能被字段选择
				"has(self.withUnknown.field1)": "undefined field 'field1'",
				// 若列表的 items 类型或 map 的 additionalProperties 类型未知，则整个列表或 map 视为未知
				// 若某属性类型未知，则它在 CEL 中不可访问
				"has(self.anyvalList)":   "undefined field 'anyvalList'",
				"has(self.anyvalMap)":    "undefined field 'anyvalMap'",
				"has(self.anyvalField1)": "undefined field 'anyvalField1'",
				"has(self.anyvalField2)": "undefined field 'anyvalField2'",
			},
		},
		{name: "known and unknown fields",
			obj: map[string]interface{}{
				"withUnknown": map[string]interface{}{
					"known":   1,
					"unknown": "a",
				},
				"withUnknownList": []interface{}{
					map[string]interface{}{
						"known":   1,
						"unknown": "a",
					},
					map[string]interface{}{
						"known":   1,
						"unknown": "b",
					},
					map[string]interface{}{
						"known":   1,
						"unknown": "b",
					},
					map[string]interface{}{
						"known": 1,
					},
					map[string]interface{}{
						"known": 1,
					},
					map[string]interface{}{
						"known": 2,
					},
				},
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"withUnknown": {
						Generic: schema.Generic{Type: "object"},
						Extensions: schema.Extensions{
							XPreserveUnknownFields: true,
						},
						Properties: map[string]schema.Structural{
							"known": integerType,
						},
					},
					"withUnknownList": listType(&schema.Structural{
						Generic: schema.Generic{Type: "object"},
						Extensions: schema.Extensions{
							XPreserveUnknownFields: true,
						},
						Properties: map[string]schema.Structural{
							"known": integerType,
						},
					}),
				},
			},
			valid: []string{
				"self.withUnknown.known == 1",
				"self.withUnknownList[0] != self.withUnknownList[1]",
				// 若未知字段相同，则相等
				"self.withUnknownList[1] == self.withUnknownList[2]",

				// 若未知字段不同，则不相等
				"self.withUnknownList[0] != self.withUnknownList[1]",
				"self.withUnknownList[0] != self.withUnknownList[3]",
				"self.withUnknownList[0] != self.withUnknownList[5]",

				// 若所有字段已知，相等性与常规一致
				"self.withUnknownList[3] == self.withUnknownList[4]",
				"self.withUnknownList[4] != self.withUnknownList[5]",
			},
			// 只有 schema 中声明的字段才能在 CEL 表达式中被字段选择
			errors: map[string]string{
				"has(self.withUnknown.unknown)": "undefined field 'unknown'",
			},
		},
		{name: "field nullability",
			obj: map[string]interface{}{
				"setPlainStr":          "v1",
				"setDefaultedStr":      "v2",
				"setNullableStr":       "v3",
				"setToNullNullableStr": nil,

				// 本测试套件不运行默认值填充（依赖它会引入循环），
				// 所以这里伪造一下 :(
				"unsetDefaultedStr": "default value",
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"unsetPlainStr":     stringType,
				"unsetDefaultedStr": withDefault("default value", stringType),
				"unsetNullableStr":  withNullable(true, stringType),

				"setPlainStr":          stringType,
				"setDefaultedStr":      withDefault("default value", stringType),
				"setNullableStr":       withNullable(true, stringType),
				"setToNullNullableStr": withNullable(true, stringType),
			}),
			valid: []string{
				"!has(self.unsetPlainStr)",
				"has(self.unsetDefaultedStr) && self.unsetDefaultedStr == 'default value'",
				"!has(self.unsetNullableStr)",

				"has(self.setPlainStr) && self.setPlainStr == 'v1'",
				"has(self.setDefaultedStr) && self.setDefaultedStr == 'v2'",
				"has(self.setNullableStr) && self.setNullableStr == 'v3'",
				// 我们将 null 字段视为缺失字段，而非 null 值字段。
				// 注意这与可空列表项或 map 值的处理方式不同。
				"type(self.setNullableStr) != null_type",

				// 在校验规则中，设为 null 的字段与缺失字段同等对待
				"!has(self.setToNullNullableStr)",
			},
			errors: map[string]string{
				// 校验规则使用的类型不与 CEL 的 Null 类型集成，因此
				// 所有与 null 比较的尝试都会在编译期被类型检查捕获
				"self.unsetPlainStr == null":     "no matching overload for '_==_' applied to '(string, null)",
				"self.unsetDefaultedStr != null": "no matching overload for '_!=_' applied to '(string, null)",
				"self.unsetNullableStr == null":  "no matching overload for '_==_' applied to '(string, null)",
				"self.setPlainStr != null":       "no matching overload for '_!=_' applied to '(string, null)",
				"self.setDefaultedStr != null":   "no matching overload for '_!=_' applied to '(string, null)",
				"self.setNullableStr != null":    "no matching overload for '_!=_' applied to '(string, null)",
			},
		},
		{name: "null values in container types",
			obj: map[string]interface{}{
				"m": map[string]interface{}{
					"a": nil,
					"b": "not-nil",
				},
				"l": []interface{}{
					nil, "not-nil",
				},
				"s": []interface{}{
					nil, "not-nil",
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"m": mapType(withNullablePtr(true, stringType)),
				"l": listType(withNullablePtr(true, stringType)),
				"s": listSetType(withNullablePtr(true, stringType)),
			}),
			valid: []string{
				"self.m.size() == 2",
				"'a' in self.m",
				"type(self.m['a']) == null_type", // 使用运行时类型检查判断 null
				//"self.m['a'] == null",

				"self.l.size() == 2",
				"type(self.l[0]) == null_type",
				//"self.l[0] == null",

				"self.s.size() == 2",
				"type(self.s[0]) == null_type",
				//"self.s[0] == null",
			},
			errors: map[string]string{
				// TODO(jpbetz): 类型检查器不支持 (<type>, Null) 的联合类型。
				// 待 cel-go 支持 "heterogeneous type" 后即可实现。
				// 在此之前，另一种选择是对可空类型使用动态类型，但这会
				// 禁用类型检查。我们计划等待 "heterogeneous type" 支持。
				//"self.m['a'] == null": "found no matching overload for '_==_' applied to '(string, null)",
				//"self.l[0] == null": "found no matching overload for '_==_' applied to '(string, null)",
				//"self.s[0] == null": "found no matching overload for '_==_' applied to '(string, null)",
			},
		},
		{name: "escaping",
			obj: map[string]interface{}{
				// CEL 词法分析器定义的 RESERVED 符号
				"true": 1, "false": 2, "null": 3, "in": 4, "as": 5,
				"break": 6, "const": 7, "continue": 8, "else": 9,
				"for": 10, "function": 11, "if": 12, "import": 13,
				"let": 14, "loop": 15, "package": 16, "namespace": 17,
				"return": 18, "var": 19, "void": 20, "while": 21,
				// CEL 语言自身的标识符
				"int": 101, "uint": 102, "double": 103, "bool": 104,
				"string": 105, "bytes": 106, "list": 107, "map": 108,
				"null_type": 109, "type": 110,
				// 校验表达式的保留标识符
				"self": 201,
				// CEL 内建函数与宏名称的标识符
				"getDate": 202,
				"all":     203,
				"size":    "204",
				// 含 _s 的标识符
				"_true": 301,
				// 含需转义字符的标识符
				"dot.dot":                            401,
				"dash-dash":                          402,
				"slash/slash":                        403,
				"underscore_underscore":              404, // 好吧，这不算太奇怪，但我们希望有一个测试用例覆盖它
				"doubleunderscore__doubleunderscore": 405,
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"true": integerType, "false": integerType, "null": integerType, "in": integerType, "as": integerType,
				"break": integerType, "const": integerType, "continue": integerType, "else": integerType,
				"for": integerType, "function": integerType, "if": integerType, "import": integerType,
				"let": integerType, "loop": integerType, "package": integerType, "namespace": integerType,
				"return": integerType, "var": integerType, "void": integerType, "while": integerType,

				"int": integerType, "uint": integerType, "double": integerType, "bool": integerType,
				"string": integerType, "bytes": integerType, "list": integerType, "map": integerType,
				"null_type": integerType, "type": integerType,

				"self": integerType,

				"getDate": integerType,
				"all":     integerType,
				"size":    stringType,

				"_true": integerType,

				"dot.dot":                            integerType,
				"dash-dash":                          integerType,
				"slash/slash":                        integerType,
				"underscore_underscore":              integerType, // 好吧，这不算太奇怪，但我们希望有一个测试用例覆盖它
				"doubleunderscore__doubleunderscore": integerType,
			}),
			valid: []string{
				// CEL 词法分析器的 RESERVED 关键字必须转义
				"self.__true__ == 1", "self.__false__ == 2", "self.__null__ == 3", "self.__in__ == 4", "self.__as__ == 5",
				"self.__break__ == 6", "self.__const__ == 7", "self.__continue__ == 8", "self.__else__ == 9",
				"self.__for__ == 10", "self.__function__ == 11", "self.__if__ == 12", "self.__import__ == 13",
				"self.__let__ == 14", "self.__loop__ == 15", "self.__package__ == 16", "self.__namespace__ == 17",
				"self.__return__ == 18", "self.__var__ == 19", "self.__void__ == 20", "self.__while__ == 21", "self.__true__ == 1",

				// CEL 语言标识符无需转义，但若作为根变量名绑定会与内建语言标识符冲突，
				// 因此只能作为 'self' 的字段被选择。
				"self.int == 101", "self.uint == 102", "self.double == 103", "self.bool == 104",
				"self.string == 105", "self.bytes == 106", "self.list == 107", "self.map == 108",
				"self.null_type == 109", "self.type == 110",

				// 若属性名为 'self'，可通过 'self.self' 字段选择访问（但不能只写 'self'，
				// 因为该变量名绑定到局部作用域的表达式值）。
				"self.self == 201",
				// CEL 宏与函数名无需转义，因为解析器能将它们与函数及宏标识符区分开。
				// 以及宏标识符。
				"self.getDate == 202",
				"self.all == 203",
				"self.size == '204'",
				// _ 不转义
				"self._true == 301",
				"self.__true__ != 301",

				"self.dot__dot__dot == 401",
				"self.dash__dash__dash == 402",
				"self.slash__slash__slash == 403",
				"self.underscore_underscore == 404",
				"self.doubleunderscore__underscores__doubleunderscore == 405",
			},
			errors: map[string]string{
				// 'true' 是布尔字面量，不是字段名
				"self.true == 1": "mismatched input 'true' expecting IDENTIFIER",
				// 'self' 是局部作用域的表达式值
				"self == 201": "found no matching overload for '_==_'",
				// 使用不可转义标识符的尝试会被编译器捕获，因为我们
				// 没有为它们注册声明
				"self.__illegal__ == 301": "undefined field '__illegal__'",
			},
		},
		{name: "map keys are not escaped",
			obj: map[string]interface{}{
				"m": map[string]interface{}{
					"@":   1,
					"9":   2,
					"int": 3,
					"./-": 4,
					"👑":   5,
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{"m": mapType(&integerType)}),
			valid: []string{
				"self.m['@'] == 1",
				"self.m['9'] == 2",
				"self.m['int'] == 3",
				"self.m['./-'] == 4",
				"self.m['👑'] == 5",
			},
		},
		{name: "object types are not accessible",
			obj: map[string]interface{}{
				"nestedInMap": map[string]interface{}{
					"k1": map[string]interface{}{
						"inMapField": 1,
					},
					"k2": map[string]interface{}{
						"inMapField": 2,
					},
				},
				"nestedInList": []interface{}{
					map[string]interface{}{
						"inListField": 1,
					},
					map[string]interface{}{
						"inListField": 2,
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"nestedInMap": mapType(objectTypePtr(map[string]schema.Structural{
					"inMapField": integerType,
				})),
				"nestedInList": listType(objectTypePtr(map[string]schema.Structural{
					"inListField": integerType,
				})),
			}),
			valid: []string{
				// 我们不暴露 self 变量的稳定类型，即使它是 CEL
				// 认为的命名类型。开发者唯一应能对该类型执行的操作是
				// 相等性检查。
				"type(self) == type(self)",
				"type(self.nestedInMap['k1']) == type(self.nestedInMap['k2'])",
				"type(self.nestedInList[0]) == type(self.nestedInList[1])",
			},
			errors: map[string]string{
				// 注意：如下错误确实会打印类型名，但每次更新 CRD 时类型名都会变化。
				// 下面错误中打印的类型名形如 "<uuid>.nestedInList.@idx"。

				// 开发者不能将变量的类型转换为字符串：
				"string(type(self.nestedInList[0])).endsWith('.nestedInList.@idx')": "found no matching overload for 'string' applied to '(type",
			},
		},
		{name: "listMaps with unsupported identity characters in property names",
			obj: map[string]interface{}{
				"objs": []interface{}{
					[]interface{}{
						map[string]interface{}{"k!": "a", "k.": "1"},
						map[string]interface{}{"k!": "b", "k.": "2"},
					},
					[]interface{}{
						map[string]interface{}{"k!": "b", "k.": "2"},
						map[string]interface{}{"k!": "a", "k.": "1"},
					},
					[]interface{}{
						map[string]interface{}{"k!": "b", "k.": "2"},
						map[string]interface{}{"k!": "c", "k.": "1"},
					},
					[]interface{}{
						map[string]interface{}{"k!": "b", "k.": "2"},
						map[string]interface{}{"k!": "a", "k.": "3"},
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"objs": listType(listMapTypePtr([]string{"k!", "k."}, objectTypePtr(map[string]schema.Structural{
					"k!": stringType,
					"k.": stringType,
				}))),
			}),
			valid: []string{
				"self.objs[0] == self.objs[1]",    // 顺序不同但相等
				"self.objs[0][0].k__dot__ == '1'", // '.' 是标识符支持的字符，但会被转义
				"self.objs[0] != self.objs[2]",    // 即使差异在不受支持的字符上，也不相等
				"self.objs[0] != self.objs[3]",    // 即使差异在已转义字符上，也不相等
			},
			errors: map[string]string{
				// '!' 不是标识符支持的字符，无法访问该字段
				"self.objs[0][0].k! == '1'": "Syntax error: mismatched input '!' expecting",
			},
		},
		{name: "container type composition",
			obj: map[string]interface{}{
				"obj": map[string]interface{}{
					"field": "a",
				},
				"mapOfMap": map[string]interface{}{
					"x": map[string]interface{}{
						"y": "b",
					},
				},
				"mapOfObj": map[string]interface{}{
					"k": map[string]interface{}{
						"field2": "c",
					},
				},
				"mapOfListMap": map[string]interface{}{
					"o": []interface{}{
						map[string]interface{}{
							"k": "1",
							"v": "d",
						},
					},
				},
				"mapOfList": map[string]interface{}{
					"l": []interface{}{"e"},
				},
				"listMapOfObj": []interface{}{
					map[string]interface{}{
						"k2": "2",
						"v2": "f",
					},
				},
				"listOfMap": []interface{}{
					map[string]interface{}{
						"z": "g",
					},
				},
				"listOfObj": []interface{}{
					map[string]interface{}{
						"field3": "h",
					},
				},
				"listOfListMap": []interface{}{
					[]interface{}{
						map[string]interface{}{
							"k3": "3",
							"v3": "i",
						},
					},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"obj": objectType(map[string]schema.Structural{
					"field": stringType,
				}),
				"mapOfMap": mapType(mapTypePtr(&stringType)),
				"mapOfObj": mapType(objectTypePtr(map[string]schema.Structural{
					"field2": stringType,
				})),
				"mapOfListMap": mapType(listMapTypePtr([]string{"k"}, objectTypePtr(map[string]schema.Structural{
					"k": stringType,
					"v": stringType,
				}))),
				"mapOfList": mapType(listTypePtr(&stringType)),
				"listMapOfObj": listMapType([]string{"k2"}, objectTypePtr(map[string]schema.Structural{
					"k2": stringType,
					"v2": stringType,
				})),
				"listOfMap": listType(mapTypePtr(&stringType)),
				"listOfObj": listType(objectTypePtr(map[string]schema.Structural{
					"field3": stringType,
				})),
				"listOfListMap": listType(listMapTypePtr([]string{"k3"}, objectTypePtr(map[string]schema.Structural{
					"k3": stringType,
					"v3": stringType,
				}))),
			}),
			valid: []string{
				"self.obj.field == 'a'",
				"self.mapOfMap['x']['y'] == 'b'",
				"self.mapOfObj['k'].field2 == 'c'",
				"self.mapOfListMap['o'].exists(e, e.k == '1' && e.v == 'd')",
				"self.mapOfList['l'][0] == 'e'",
				"self.listMapOfObj.exists(e, e.k2 == '2' && e.v2 == 'f')",
				"self.listOfMap[0]['z'] == 'g'",
				"self.listOfObj[0].field3 == 'h'",
				"self.listOfListMap[0].exists(e, e.k3 == '3' && e.v3 == 'i')",
			},
			errors: map[string]string{},
		},
		{name: "invalid data",
			obj: map[string]interface{}{
				"o":           []interface{}{},
				"m":           []interface{}{},
				"l":           map[string]interface{}{},
				"s":           map[string]interface{}{},
				"lm":          map[string]interface{}{},
				"intorstring": true,
				"nullable":    []interface{}{nil},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"o": objectType(map[string]schema.Structural{
					"field": stringType,
				}),
				"m": mapType(&stringType),
				"l": listType(&stringType),
				"s": listSetType(&stringType),
				"lm": listMapType([]string{"k"}, objectTypePtr(map[string]schema.Structural{
					"k": stringType,
					"v": stringType,
				})),
				"intorstring": intOrStringType(),
				"nullable":    listType(&stringType),
			}),
			errors: map[string]string{
				// 数据在 CEL 求值前已校验，因此这些错误不应暴露给最终用户
				"has(self.o)":             "invalid data, expected a map for the provided schema with type=object",
				"has(self.m)":             "invalid data, expected a map for the provided schema with type=object",
				"has(self.l)":             "invalid data, expected an array for the provided schema with type=array",
				"has(self.s)":             "invalid data, expected an array for the provided schema with type=array",
				"has(self.lm)":            "invalid data, expected an array for the provided schema with type=array",
				"has(self.intorstring)":   "invalid data, expected XIntOrString value to be either a string or integer",
				"self.nullable[0] == 'x'": "invalid data, got null for schema with nullable=false",
				// TODO: 还需要找到方法来测试以下情况返回的错误：无 items 的数组、无 properties 或 additionalProperties 的对象、非法的 listType 以及非法 type。
			},
		},
		{name: "stdlib list functions",
			obj: map[string]interface{}{
				"ints":         []interface{}{int64(1), int64(2), int64(2), int64(3)},
				"unsortedInts": []interface{}{int64(2), int64(1)},
				"emptyInts":    []interface{}{},

				"doubles":         []interface{}{float64(1), float64(2), float64(2), float64(3)},
				"unsortedDoubles": []interface{}{float64(2), float64(1)},
				"emptyDoubles":    []interface{}{},

				"intBackedDoubles":          []interface{}{int64(1), int64(2), int64(2), int64(3)},
				"unsortedIntBackedDDoubles": []interface{}{int64(2), int64(1)},
				"emptyIntBackedDDoubles":    []interface{}{},

				"durations":         []interface{}{"1s", "1m", "1m", "1h"},
				"unsortedDurations": []interface{}{"1m", "1s"},
				"emptyDurations":    []interface{}{},

				"strings":         []interface{}{"a", "b", "b", "c"},
				"unsortedStrings": []interface{}{"b", "a"},
				"emptyStrings":    []interface{}{},

				"dates":         []interface{}{"2000-01-01", "2000-02-01", "2000-02-01", "2010-01-01"},
				"unsortedDates": []interface{}{"2000-02-01", "2000-01-01"},
				"emptyDates":    []interface{}{},

				"objs": []interface{}{
					map[string]interface{}{"f1": "a", "f2": "a"},
					map[string]interface{}{"f1": "a", "f2": "b"},
					map[string]interface{}{"f1": "a", "f2": "b"},
					map[string]interface{}{"f1": "a", "f2": "c"},
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"ints":         listType(&integerType),
				"unsortedInts": listType(&integerType),
				"emptyInts":    listType(&integerType),

				"doubles":         listType(&doubleType),
				"unsortedDoubles": listType(&doubleType),
				"emptyDoubles":    listType(&doubleType),

				"intBackedDoubles":          listType(&doubleType),
				"unsortedIntBackedDDoubles": listType(&doubleType),
				"emptyIntBackedDDoubles":    listType(&doubleType),

				"durations":         listType(&durationFormat),
				"unsortedDurations": listType(&durationFormat),
				"emptyDurations":    listType(&durationFormat),

				"strings":         listType(&stringType),
				"unsortedStrings": listType(&stringType),
				"emptyStrings":    listType(&stringType),

				"dates":         listType(&dateFormat),
				"unsortedDates": listType(&dateFormat),
				"emptyDates":    listType(&dateFormat),

				"objs": listType(objectTypePtr(map[string]schema.Structural{
					"f1": stringType,
					"f2": stringType,
				})),
			}),
			valid: []string{
				"self.ints.sum() == 8",
				"self.ints.min() == 1",
				"self.ints.max() == 3",
				"self.emptyInts.sum() == 0",
				"self.ints.isSorted()",
				"self.emptyInts.isSorted()",
				"self.unsortedInts.isSorted() == false",
				"self.ints.indexOf(2) == 1",
				"self.ints.lastIndexOf(2) == 2",
				"self.ints.indexOf(10) == -1",
				"self.ints.lastIndexOf(10) == -1",

				"self.doubles.sum() == 8.0",
				"self.doubles.min() == 1.0",
				"self.doubles.max() == 3.0",
				"self.emptyDoubles.sum() == 0.0",
				"self.doubles.isSorted()",
				"self.emptyDoubles.isSorted()",
				"self.unsortedDoubles.isSorted() == false",
				"self.doubles.indexOf(2.0) == 1",
				"self.doubles.lastIndexOf(2.0) == 2",
				"self.doubles.indexOf(10.0) == -1",
				"self.doubles.lastIndexOf(10.0) == -1",

				"self.intBackedDoubles.sum() == 8.0",
				"self.intBackedDoubles.min() == 1.0",
				"self.intBackedDoubles.max() == 3.0",
				"self.emptyIntBackedDDoubles.sum() == 0.0",
				"self.intBackedDoubles.isSorted()",
				"self.emptyDoubles.isSorted()",
				"self.unsortedIntBackedDDoubles.isSorted() == false",
				"self.intBackedDoubles.indexOf(2.0) == 1",
				"self.intBackedDoubles.lastIndexOf(2.0) == 2",
				"self.intBackedDoubles.indexOf(10.0) == -1",
				"self.intBackedDoubles.lastIndexOf(10.0) == -1",

				"self.durations.sum() == duration('1h2m1s')",
				"self.durations.min() == duration('1s')",
				"self.durations.max() == duration('1h')",
				"self.emptyDurations.sum() == duration('0')",
				"self.durations.isSorted()",
				"self.emptyDurations.isSorted()",
				"self.unsortedDurations.isSorted() == false",
				"self.durations.indexOf(duration('1m')) == 1",
				"self.durations.lastIndexOf(duration('1m')) == 2",
				"self.durations.indexOf(duration('2m')) == -1",
				"self.durations.lastIndexOf(duration('2m')) == -1",

				"self.strings.min() == 'a'",
				"self.strings.max() == 'c'",
				"self.strings.isSorted()",
				"self.emptyStrings.isSorted()",
				"self.unsortedStrings.isSorted() == false",
				"self.strings.indexOf('b') == 1",
				"self.strings.lastIndexOf('b') == 2",
				"self.strings.indexOf('x') == -1",
				"self.strings.lastIndexOf('x') == -1",

				"self.dates.min() == timestamp('2000-01-01T00:00:00.000Z')",
				"self.dates.max() == timestamp('2010-01-01T00:00:00.000Z')",
				"self.dates.isSorted()",
				"self.emptyDates.isSorted()",
				"self.unsortedDates.isSorted() == false",
				"self.dates.indexOf(timestamp('2000-02-01T00:00:00.000Z')) == 1",
				"self.dates.lastIndexOf(timestamp('2000-02-01T00:00:00.000Z')) == 2",
				"self.dates.indexOf(timestamp('2005-02-01T00:00:00.000Z')) == -1",
				"self.dates.lastIndexOf(timestamp('2005-02-01T00:00:00.000Z')) == -1",

				// array、map 与 object 类型使用结构相等（即“深度相等”）
				"[[1], [2]].indexOf([1]) == 0",
				"[{'a': 1}, {'b': 2}].lastIndexOf({'b': 2}) == 1",
				"self.objs.indexOf(self.objs[1]) == 1",
				"self.objs.lastIndexOf(self.objs[1]) == 2",

				// 通过追加可接受的默认最小值，避免 min 和 max 对空列表报错
				"([0] + self.emptyInts).min() == 0",

				// 正确处理 CEL 的动态分派（对空列表做特殊处理）
				"dyn([]).sum() == 0",
				"dyn([1, 2]).sum() == 3",
				"dyn([1.0, 2.0]).sum() == 3.0",

				"[].sum() == 0", // 空列表返回整数 0
			},
			errors: map[string]string{
				// 对空列表调用 min/max 返回错误
				"self.emptyInts.min() == 1":      "min called on empty list",
				"self.emptyInts.max() == 3":      "max called on empty list",
				"self.emptyDoubles.min() == 1.0": "min called on empty list",
				"self.emptyDoubles.max() == 3.0": "max called on empty list",
				"self.emptyStrings.min() == 'a'": "min called on empty list",
				"self.emptyStrings.max() == 'c'": "max called on empty list",

				// 仅允许对数值类型和 duration 求和
				"['a', 'b'].sum() == 'c'": "found no matching overload for 'sum' applied to 'list(string).()", // 编译器类型检查错误

				// 仅允许对可比较类型使用 min/max/indexOf/lastIndexOf
				"[[1], [2]].min() == [1]":                "found no matching overload for 'min' applied to 'list(list(int)).()",        // 编译器类型检查错误
				"[{'a': 1}, {'b': 2}].max() == {'b': 2}": "found no matching overload for 'max' applied to 'list(map(string, int)).()", // 编译器类型检查错误
			},
		},
		{name: "stdlib regex functions",
			obj: map[string]interface{}{
				"str": "this is a 123 string 456",
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"str": stringType,
			}),
			valid: []string{
				"self.str.find('[0-9]+') == '123'",
				"self.str.find('[0-9]+') != '456'",
				"self.str.find('xyz') == ''",

				"self.str.findAll('[0-9]+') == ['123', '456']",
				"self.str.findAll('[0-9]+', 0) == []",
				"self.str.findAll('[0-9]+', 1) == ['123']",
				"self.str.findAll('[0-9]+', 2) == ['123', '456']",
				"self.str.findAll('[0-9]+', 3) == ['123', '456']",
				"self.str.findAll('[0-9]+', -1) == ['123', '456']",
				"self.str.findAll('xyz') == []",
				"self.str.findAll('xyz', 1) == []",
			},
			errors: map[string]string{
				// 字符串常量正则模式的非法正则会成为编译期错误
				"self.str.find(')') == ''":       "compile error: program instantiation failed: error parsing regexp: unexpected ): `)`",
				"self.str.findAll(')') == []":    "compile error: program instantiation failed: error parsing regexp: unexpected ): `)`",
				"self.str.findAll(')', 1) == []": "compile error: program instantiation failed: error parsing regexp: unexpected ): `)`",
				"self.str.matches('x++')":        "invalid matches argument",
			},
		},
		{name: "URL parsing",
			obj: map[string]interface{}{
				"url": "https://user:pass@kubernetes.io:80/docs/home?k1=a&k2=b&k2=c#anchor",
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"url": stringType,
			}),
			valid: []string{
				"url('/path').getScheme() == ''",
				"url('https://example.com/').getScheme() == 'https'",
				"url('https://example.com:80/').getHost() == 'example.com:80'",
				"url('https://example.com/').getHost() == 'example.com'",
				"url('https://[::1]:80/').getHost() == '[::1]:80'",
				"url('https://[::1]/').getHost() == '[::1]'",
				"url('/path').getHost() == ''",
				"url('https://example.com:80/').getHostname() == 'example.com'",
				"url('https://127.0.0.1/').getHostname() == '127.0.0.1'",
				"url('https://[::1]/').getHostname() == '::1'",
				"url('/path').getHostname() == ''",
				"url('https://example.com:80/').getPort() == '80'",
				"url('https://example.com/').getPort() == ''",
				"url('/path').getPort() == ''",
				"url('https://example.com/path').getEscapedPath() == '/path'",
				"url('https://example.com/with space/').getEscapedPath() == '/with%20space/'",
				"url('https://example.com').getEscapedPath() == ''",
				"url('https://example.com/path?k1=a&k2=b&k2=c').getQuery() == { 'k1': ['a'], 'k2': ['b', 'c']}",
				"url('https://example.com/path?key with spaces=value with spaces').getQuery() == { 'key with spaces': ['value with spaces']}",
				"url('https://example.com/path?').getQuery() == {}",
				"url('https://example.com/path').getQuery() == {}",

				// 使用字符串输入测试
				"url(self.url).getScheme() == 'https'",
				"url(self.url).getHost() == 'kubernetes.io:80'",
				"url(self.url).getHostname() == 'kubernetes.io'",
				"url(self.url).getPort() == '80'",
				"url(self.url).getEscapedPath() == '/docs/home'",
				"url(self.url).getQuery() == {'k1': ['a'], 'k2': ['b', 'c']}",

				"isURL('https://user:pass@example.com:80/path?query=val#fragment')",
				"isURL('/path') == true",
				"isURL('https://a:b:c/') == false",
				"isURL('../relative-path') == false",
			},
		},
		{name: "transition rules",
			obj: map[string]interface{}{
				"v": "new",
			},
			oldObj: map[string]interface{}{
				"v": "old",
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"v": stringType,
			}),
			valid: []string{
				"oldSelf.v != self.v",
				"oldSelf.v == 'old' && self.v == 'new'",
			},
		},
		{name: "skipped transition rule for nil old primitive",
			expectSkipped: true,
			obj:           "exists",
			oldObj:        nil,
			schema:        &stringType,
			valid: []string{
				"oldSelf == self",
			},
		},
		{name: "skipped transition rule for nil old array",
			expectSkipped: true,
			obj:           []interface{}{},
			oldObj:        nil,
			schema:        listTypePtr(&stringType),
			valid: []string{
				"oldSelf == self",
			},
		},
		{name: "skipped transition rule for nil old object",
			expectSkipped: true,
			obj:           map[string]interface{}{"f": "exists"},
			oldObj:        nil,
			schema: objectTypePtr(map[string]schema.Structural{
				"f": stringType,
			}),
			valid: []string{
				"oldSelf.f == self.f",
			},
		},
		{name: "skipped transition rule for old with non-nil interface but nil value",
			expectSkipped: true,
			obj:           []interface{}{},
			oldObj:        nilInterfaceOfStringSlice(),
			schema:        listTypePtr(&stringType),
			valid: []string{
				"oldSelf == self",
			},
		},
		{name: "authorizer is not supported for CRD Validation Rules",
			obj:    []interface{}{},
			oldObj: []interface{}{},
			schema: objectTypePtr(map[string]schema.Structural{}),
			errors: map[string]string{
				"authorizer.path('/healthz').check('get').allowed()": "undeclared reference to 'authorizer'",
			},
		},
		{name: "optionals", // 参考：https://github.com/google/cel-spec/wiki/proposal-246
			obj: map[string]interface{}{
				"presentObj": map[string]interface{}{
					"presentStr": "value",
				},
				"m": map[string]interface{}{"k": "v"},
				"l": []interface{}{"a"},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"presentObj": objectType(map[string]schema.Structural{
					"presentStr": stringType,
				}),
				"absentObj": objectType(map[string]schema.Structural{
					"absentStr": stringType,
				}),
				"m": mapType(&stringType),
				"l": listType(&stringType),
			}),
			valid: []string{
				"self.?presentObj.?presentStr == optional.of('value')",
				"self.presentObj.?presentStr == optional.of('value')",
				"self.presentObj.?presentStr.or(optional.of('nope')) == optional.of('value')",
				"self.presentObj.?presentStr.orValue('') == 'value'",
				"self.presentObj.?presentStr.hasValue() == true",
				"self.presentObj.?presentStr.optMap(v, v == 'value').hasValue()",
				"self.?absentObj.?absentStr == optional.none()",
				"self.?absentObj.?absentStr.or(optional.of('nope')) == optional.of('nope')",
				"self.?absentObj.?absentStr.orValue('nope') == 'nope'",
				"self.?absentObj.?absentStr.hasValue() == false",
				"self.?absentObj.?absentStr.optMap(v, v == 'value').hasValue() == false",

				"self.m[?'k'] == optional.of('v')",
				"self.m[?'k'].or(optional.of('nope')) == optional.of('v')",
				"self.m[?'k'].orValue('') == 'v'",
				"self.m[?'k'].hasValue() == true",
				"self.m[?'k'].optMap(v, v == 'v').hasValue()",
				"self.m[?'x'] == optional.none()",
				"self.m[?'x'].or(optional.of('nope')) == optional.of('nope')",
				"self.m[?'x'].orValue('nope') == 'nope'",
				"self.m[?'x'].hasValue() == false",
				"self.m[?'x'].hasValue() == false",

				"self.l[?0] == optional.of('a')",
				"self.l[?1] == optional.none()",
				"self.l[?0].orValue('') == 'a'",
				"self.l[?0].hasValue() == true",
				"self.l[?0].optMap(v, v == 'a').hasValue()",
				"self.l[?1] == optional.none()",
				"self.l[?1].or(optional.of('nope')) == optional.of('nope')",
				"self.l[?1].orValue('nope') == 'nope'",
				"self.l[?1].hasValue() == false",
				"self.l[?1].hasValue() == false",

				"optional.ofNonZeroValue(1).hasValue()",
				"optional.ofNonZeroValue(uint(1)).hasValue()",
				"optional.ofNonZeroValue(1.1).hasValue()",
				"optional.ofNonZeroValue('a').hasValue()",
				"optional.ofNonZeroValue(true).hasValue()",
				"optional.ofNonZeroValue(['a']).hasValue()",
				"optional.ofNonZeroValue({'k': 'v'}).hasValue()",
				"optional.ofNonZeroValue(timestamp('2011-08-18T00:00:00.000+01:00')).hasValue()",
				"optional.ofNonZeroValue(duration('19h3m37s10ms')).hasValue()",
				"optional.ofNonZeroValue(null) == optional.none()",
				"optional.ofNonZeroValue(0) == optional.none()",
				"optional.ofNonZeroValue(uint(0)) == optional.none()",
				"optional.ofNonZeroValue(0.0) == optional.none()",
				"optional.ofNonZeroValue('') == optional.none()",
				"optional.ofNonZeroValue(false) == optional.none()",
				"optional.ofNonZeroValue([]) == optional.none()",
				"optional.ofNonZeroValue({}) == optional.none()",
				"optional.ofNonZeroValue(timestamp('0001-01-01T00:00:00.000+00:00')) == optional.none()",
				"optional.ofNonZeroValue(duration('0s')) == optional.none()",

				"{?'k': optional.none(), 'k2': 'v2'} == {'k2': 'v2'}",
				"{?'k': optional.of('v'), 'k2': 'v2'} == {'k': 'v', 'k2': 'v2'}",
				"['a', ?optional.none(), 'c'] == ['a', 'c']",
				"['a', ?optional.of('v'), 'c'] == ['a', 'v', 'c']",
			},
			errors: map[string]string{
				"self.absentObj.?absentStr == optional.none()": "no such key: absentObj", // 首次解引用缺少 ?. 运算符会报错
			},
		},
		{name: "quantity",
			obj:    objs("20", "200M"),
			schema: schemas(stringType, stringType),
			valid: []string{
				"isQuantity(self.val1)",
				"isQuantity(self.val2)",
				`isQuantity("20Mi")`,
				`quantity(self.val2) == quantity("0.2G") && quantity("0.2G") == quantity("200M")`,
				`quantity("2M") == quantity("0.002G") && quantity("2000k") == quantity("2M") && quantity("0.002G") == quantity("2000k")`,
				`quantity(self.val1).isLessThan(quantity("100M"))`,
				`quantity(self.val2).isGreaterThan(quantity("50M"))`,
				`quantity(self.val2).compareTo(quantity("0.2G")) == 0`,
				`quantity("50k").add(quantity(self.val1)) == quantity("50.02k")`,
				`quantity("50k").sub(quantity(self.val1)) == quantity("49980")`,
				`quantity(self.val1).isInteger()`,
			},
		},
		{name: "cost for extended lib calculated correctly: isSorted",
			obj:    objs("20", "200M"),
			schema: schemas(stringType, stringType),
			valid: []string{
				"[1,2,3,4].isSorted()",
			},
			expectedCost: 4,
		},
		{name: "cost for extended lib calculated correctly: url",
			obj:    objs("20", "200M"),
			schema: schemas(stringType, stringType),
			valid: []string{
				"url('https:://kubernetes.io/').getHostname() != 'test'",
			},
			expectedCost: 4,
		},
		{name: "cost for extended lib calculated correctly: split",
			obj:    objs("20", "200M"),
			schema: schemas(stringType, stringType),
			valid: []string{
				"size('abc 123 def 123'.split(' ')) > 0",
			},
			expectedCost: 5,
		},
		{name: "cost for extended lib calculated correctly: join",
			obj:    objs("20", "200M"),
			schema: schemas(stringType, stringType),
			valid: []string{
				"size(['aa', 'bb', 'cc', 'd', 'e', 'f', 'g', 'h', 'i', 'j'].join(' ')) > 0",
			},
			expectedCost: 7,
		},
		{name: "IP and CIDR",
			obj:    objs("20", "200M"),
			schema: schemas(stringType, stringType),
			valid: []string{
				`isIP("192.168.0.1")`,
				`ip.isCanonical("127.0.0.1")`,
				`ip("192.168.0.1").family() > 0`,
				`ip("0.0.0.0").isUnspecified()`,
				`ip("127.0.0.1").isLoopback()`,
				`ip("224.0.0.1").isLinkLocalMulticast()`,
				`ip("192.168.0.1").isGlobalUnicast()`,
				`type(ip("192.168.0.1")) == net.IP`,
				`cidr("192.168.0.0/24").containsIP(ip("192.168.0.1"))`,
				`cidr("192.168.0.0/24").containsCIDR("192.168.0.0/25")`,
				`cidr("2001:db8::/32").containsCIDR(cidr("2001:db8::/33"))`,
				`type(cidr("2001:db8::/32").ip()) == net.IP`,
				`cidr('192.168.0.0/24') == cidr('192.168.0.0/24').masked()`,
				`cidr('192.168.0.0/16').prefixLength() == 16`,
				`cidr('::1/128').ip().family() == 6`,
			},
		},
		{name: "format",
			obj:    objs("20", "200M"),
			schema: schemas(stringType, stringType),
			valid: []string{
				`format.dns1123Label().validate("my-label-name") == optional.none()`,
				`format.dns1123Subdomain().validate("apiextensions.k8s.io") == optional.none()`,
				`format.qualifiedName().validate("apiextensions.k8s.io/v1beta1") == optional.none()`,
				`format.dns1123LabelPrefix().validate("my-label-prefix-") == optional.none()`,
				`format.dns1123SubdomainPrefix().validate("mysubdomain.prefix.-") == optional.none()`,
				`format.dns1035LabelPrefix().validate("my-label-prefix-") == optional.none()`,
				`format.uri().validate("http://example.com") == optional.none()`,
				`format.uuid().validate("123e4567-e89b-12d3-a456-426614174000") == optional.none()`,
				`format.byte().validate("aGVsbG8=") == optional.none()`,
				`format.date().validate("2021-01-01") == optional.none()`,
				`format.datetime().validate("2021-01-01T00:00:00Z") == optional.none()`,
				`format.named("dns1123Label").value().validate("my-name") == optional.none()`,
				`format.dns1123Label().validate("contains a space").value()[0] == "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or \'-\', and must start and end with an alphanumeric character (e.g. \'my-name\',  or \'123-abc\', regex used for validation is \'[a-z0-9]([-a-z0-9]*[a-z0-9])?\')"`,
				`!format.named("unknown").hasValue()`,
			},
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			t.Parallel()
			tt := tests[i]
			if tt.costBudget == 0 {
				tt.costBudget = celconfig.RuntimeCELCostBudget
			}
			ctx := context.TODO()
			for j := range tt.valid {
				validRule := tt.valid[j]
				testName := validRule
				if len(testName) > 127 {
					testName = testName[:127]
				}
				t.Run(testName, func(t *testing.T) {
					t.Parallel()
					s := withRule(*tt.schema, validRule)
					celValidator := validator(&s, &s, tt.isRoot, model.SchemaDeclType(&s, tt.isRoot), celconfig.PerCallLimit)
					if celValidator == nil {
						t.Fatal("expected non nil validator")
					}
					errs, remainingBudget := celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, tt.oldObj, tt.costBudget)
					for _, err := range errs {
						t.Errorf("unexpected error: %v", err)
					}

					if tt.expectedCost != 0 {
						if remainingBudget != tt.costBudget-tt.expectedCost {
							t.Errorf("expected cost to be %d, but got %d", tt.expectedCost, tt.costBudget-remainingBudget)
						}
						return
					}
					if tt.expectSkipped {
						// 跳过的校验不应产生开销。这里唯一可能的误报是 CEL 表达式 'true'。
						if remainingBudget != tt.costBudget {
							t.Errorf("expected no cost expended for skipped validation, but got %d remaining from %d budget", remainingBudget, tt.costBudget)
						}
						return
					}
				})
			}
			for rule, expectErrToContain := range tt.errors {
				testName := rule
				if len(testName) > 127 {
					testName = testName[:127]
				}
				t.Run(testName, func(t *testing.T) {
					s := withRule(*tt.schema, rule)
					celValidator := NewValidator(&s, true, celconfig.PerCallLimit)
					if celValidator == nil {
						t.Fatal("expected non nil validator")
					}
					errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, tt.oldObj, tt.costBudget)
					if len(errs) == 0 {
						t.Error("expected validation errors but got none")
					}
					for _, err := range errs {
						if err.Type != field.ErrorTypeInvalid || !strings.Contains(err.Error(), expectErrToContain) {
							t.Errorf("expected error to contain '%s', but got: %v", expectErrToContain, err)
						}
					}
				})
			}
		})
	}
}

// TestValidationExpressionsInSchema 测试 CEL 与自定义资源值及 OpenAPIv3 的集成，
// 其中校验规则可定义在 schema 的任意层级。
func TestValidationExpressionsAtSchemaLevels(t *testing.T) {
	tests := []struct {
		name   string
		schema *schema.Structural
		obj    interface{}
		oldObj interface{}
		errors []string // 错误信息必须包含的字符串
	}{
		{name: "invalid rule under array items",
			obj: map[string]interface{}{
				"f": []interface{}{1},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"f": listType(cloneWithRule(&integerType, "self == 'abc'")),
			}),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "invalid rule under array items, parent has rule",
			obj: map[string]interface{}{
				"f": []interface{}{1},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"f": withRule(listType(cloneWithRule(&integerType, "self == 'abc'")), "1 == 1"),
			}),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "invalid rule under additionalProperties",
			obj: map[string]interface{}{
				"f": map[string]interface{}{"k": 1},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"f": mapType(cloneWithRule(&integerType, "self == 'abc'")),
			}),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "invalid rule under additionalProperties, parent has rule",
			obj: map[string]interface{}{
				"f": map[string]interface{}{"k": 1},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"f": withRule(mapType(cloneWithRule(&integerType, "self == 'abc'")), "1 == 1"),
			}),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "invalid rule under unescaped field name",
			obj: map[string]interface{}{
				"f": map[string]interface{}{
					"m": 1,
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"f": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 'abc'"),
			}),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "invalid rule under unescaped field name, parent has rule",
			obj: map[string]interface{}{
				"f": map[string]interface{}{
					"m": 1,
				},
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"f": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 'abc'"),
			}), "1 == 1"),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		// 检查转义字段名不影响 CEL 规则校验
		{name: "invalid rule under escaped field name",
			obj: map[string]interface{}{
				"f/2": map[string]interface{}{
					"m": 1,
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"f/2": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 'abc'"),
			}),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "invalid rule under escaped field name, parent has rule",
			obj: map[string]interface{}{
				"f/2": map[string]interface{}{
					"m": 1,
				},
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"f/2": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 'abc'"),
			}), "1 == 1"),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "failing rule under escaped field name",
			obj: map[string]interface{}{
				"f/2": map[string]interface{}{
					"m": 1,
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"f/2": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 2"),
			}),
			errors: []string{"Invalid value: failed rule: self.m == 2"},
		},
		// CEL 规则未访问的不可转义字段名是允许的，且不应影响 CEL 规则校验
		{name: "invalid rule under unescapable field name",
			obj: map[string]interface{}{
				"a@b": map[string]interface{}{
					"m": 1,
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"a@b": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 'abc'"),
			}),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "invalid rule under unescapable field name, parent has rule",
			obj: map[string]interface{}{
				"f@2": map[string]interface{}{
					"m": 1,
				},
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"f@2": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 'abc'"),
			}), "1 == 1"),
			errors: []string{"found no matching overload for '_==_' applied to '(int, string)"},
		},
		{name: "failing rule under unescapable field name",
			obj: map[string]interface{}{
				"a@b": map[string]interface{}{
					"m": 1,
				},
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"a@b": withRule(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 2"),
			}),
			errors: []string{"Invalid value: failed rule: self.m == 2"},
		},
		{name: "matchExpressions - 'values' must be specified when 'operator' is 'In' or 'NotIn'",
			obj: map[string]interface{}{
				"matchExpressions": []interface{}{
					map[string]interface{}{
						"key":      "tier",
						"operator": "In",
						"values":   []interface{}{},
					},
				},
			},
			schema: genMatchSelectorSchema(`self.matchExpressions.all(rule, (rule.operator != "In" && rule.operator != "NotIn") || ((has(rule.values) && size(rule.values) > 0)))`),
			errors: []string{"failed rule"},
		},
		{name: "matchExpressions - 'values' may not be specified when 'operator' is 'Exists' or 'DoesNotExist'",
			obj: map[string]interface{}{
				"matchExpressions": []interface{}{
					map[string]interface{}{
						"key":      "tier",
						"operator": "Exists",
						"values":   []interface{}{"somevalue"},
					},
				},
			},
			schema: genMatchSelectorSchema(`self.matchExpressions.all(rule, (rule.operator != "Exists" && rule.operator != "DoesNotExist") || ((!has(rule.values) || size(rule.values) == 0)))`),
			errors: []string{"failed rule"},
		},
		{name: "matchExpressions - invalid selector operator",
			obj: map[string]interface{}{
				"matchExpressions": []interface{}{
					map[string]interface{}{
						"key":      "tier",
						"operator": "badop",
						"values":   []interface{}{},
					},
				},
			},
			schema: genMatchSelectorSchema(`self.matchExpressions.all(rule, rule.operator == "In" || rule.operator == "NotIn" || rule.operator == "DoesNotExist")`),
			errors: []string{"failed rule"},
		},
		{name: "matchExpressions - invalid label value",
			obj: map[string]interface{}{
				"matchExpressions": []interface{}{
					map[string]interface{}{
						"key":      "badkey!",
						"operator": "Exists",
						"values":   []interface{}{},
					},
				},
			},
			schema: genMatchSelectorSchema(`self.matchExpressions.all(rule, size(rule.key) <= 63 && rule.key.matches("^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$"))`),
			errors: []string{"failed rule"},
		},
		{
			name: "allOf rule",
			errors: []string{
				`key must be 'value' and key2 must be 'value2'`,
				`key must not be equal to key2`,
			},
			obj: map[string]interface{}{
				"key":  "value",
				"key2": "value",
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"key":  stringType,
					"key2": stringType,
				},
				ValueValidation: &schema.ValueValidation{
					AllOf: []schema.NestedValueValidation{
						{
							ValidationExtensions: schema.ValidationExtensions{
								XValidations: []apiextensions.ValidationRule{
									{
										Rule:    "self.key == 'value' && self.key2 == 'value2'",
										Message: "key must be 'value' and key2 must be 'value2'",
									},
								},
							},
						},
						{
							ValidationExtensions: schema.ValidationExtensions{
								XValidations: []apiextensions.ValidationRule{
									{
										Rule:    "self.key != self.key2",
										Message: "key must not be equal to key2",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			// anyOf 未实现，因为正确实现需要
			// SchemaValidator 调用 CEL。本测试
			// 表明一条本应报错的规则会忽略这些
			// 字段（此时 CRD 校验会报错）
			name: "anyOf rule ignored",
			obj: map[string]interface{}{
				"key":  "value",
				"key2": "value",
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"key":  stringType,
					"key2": stringType,
				},
				ValidationExtensions: schema.ValidationExtensions{
					XValidations: []apiextensions.ValidationRule{
						{
							Rule: "true",
						},
					},
				},
				ValueValidation: &schema.ValueValidation{
					AnyOf: []schema.NestedValueValidation{
						{
							ValidationExtensions: schema.ValidationExtensions{
								XValidations: []apiextensions.ValidationRule{
									{
										Rule:    "self.key == 'value' && self.key2 == 'value2'",
										Message: "key must be 'value' and key2 must be 'value2'",
									},
								},
							},
						},
						{
							ValidationExtensions: schema.ValidationExtensions{
								XValidations: []apiextensions.ValidationRule{
									{
										Rule:    "self.key != self.key2",
										Message: "key must not be equal to key2",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			// OneOf 未实现，因为正确实现需要
			// SchemaValidator 调用 CEL。本测试
			// 表明一条本应报错的规则会忽略这些
			// 字段（此时 CRD 校验会报错）
			name: "oneOf rule ignored",
			obj: map[string]interface{}{
				"key":  "value",
				"key2": "value",
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"key":  stringType,
					"key2": stringType,
				},
				ValidationExtensions: schema.ValidationExtensions{
					XValidations: []apiextensions.ValidationRule{
						{
							Rule: "true",
						},
					},
				},
				ValueValidation: &schema.ValueValidation{
					OneOf: []schema.NestedValueValidation{
						{
							ValidationExtensions: schema.ValidationExtensions{
								XValidations: []apiextensions.ValidationRule{
									{
										Rule:    "self.key == 'value' && self.key2 == 'value2'",
										Message: "key must be 'value' and key2 must be 'value2'",
									},
								},
							},
						},
						{
							ValidationExtensions: schema.ValidationExtensions{
								XValidations: []apiextensions.ValidationRule{
									{
										Rule:    "self.key != self.key2",
										Message: "key must not be equal to key2",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			// Not 未实现，因为正确实现需要
			// SchemaValidator 调用 CEL。本测试
			// 表明一条本应报错的规则会忽略这些
			// 字段（此时 CRD 校验会报错）
			name: "not rule ignored",
			obj: map[string]interface{}{
				"key":  "value",
				"key2": "value",
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"key":  stringType,
					"key2": stringType,
				},
				ValidationExtensions: schema.ValidationExtensions{
					XValidations: []apiextensions.ValidationRule{
						{
							Rule: "true",
						},
					},
				},
				ValueValidation: &schema.ValueValidation{
					Not: &schema.NestedValueValidation{
						ValidationExtensions: schema.ValidationExtensions{
							XValidations: []apiextensions.ValidationRule{
								{
									Rule:    "self.key == 'value' && self.key2 == 'value2'",
									Message: "key must be 'value' and key2 must be 'value2'",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "allOf.items",
			obj: map[string]interface{}{
				"myList": []interface{}{"value", "value2"},
			},
			errors: []string{
				`must be value2 or not value`,
				`len must be 5`,
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"myList": {
						Generic: schema.Generic{
							Type: "array",
						},
						Items: &stringType,
						ValueValidation: &schema.ValueValidation{
							AllOf: []schema.NestedValueValidation{
								{
									Items: &schema.NestedValueValidation{
										ValidationExtensions: schema.ValidationExtensions{
											XValidations: []apiextensions.ValidationRule{
												{
													Rule:    "self.size() == 5",
													Message: "len must be 5",
												},
											},
										},
									},
								},
								{
									Items: &schema.NestedValueValidation{
										ValidationExtensions: schema.ValidationExtensions{
											XValidations: []apiextensions.ValidationRule{
												{
													Rule:    `self == "value2" || self != "value"`,
													Message: "must be value2 or not value",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "allOf.additionalProperties",
			obj: map[string]interface{}{
				"myProperty": map[string]interface{}{
					"key":  "value",
					"key2": "value2",
				},
			},
			errors: []string{
				`root.myProperty[key]: Invalid value: "value": must be value2 or not value`,
				`root.myProperty[key2]: Invalid value: "value2": len must be 5`,
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				// 由于 WithTypeAndObjectMeta 强制所有顶层 schema 为结构体，
				// 需要把所有测试放进 Properties
				Properties: map[string]schema.Structural{
					"myProperty": {
						Generic: schema.Generic{
							Type: "object",
						},
						AdditionalProperties: &schema.StructuralOrBool{Structural: &stringType},
						ValueValidation: &schema.ValueValidation{
							AllOf: []schema.NestedValueValidation{
								{
									AdditionalProperties: &schema.NestedValueValidation{
										ValidationExtensions: schema.ValidationExtensions{
											XValidations: []apiextensions.ValidationRule{
												{
													Rule:    `self == "value2" || self != "value"`,
													Message: "must be value2 or not value",
												},
												{
													Rule:    "self.size() == 5",
													Message: "len must be 5",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "allOf.properties",
			obj: map[string]interface{}{
				"key":  "value",
				"key2": "value2",
			},
			errors: []string{
				`root.key: Invalid value: "value": must be value2 or not value`,
				`root.key2: Invalid value: "value2": len must be 5`,
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"key":  stringType,
					"key2": stringType,
				},
				ValueValidation: &schema.ValueValidation{
					AllOf: []schema.NestedValueValidation{
						{
							Properties: map[string]schema.NestedValueValidation{
								"key": {
									ValidationExtensions: schema.ValidationExtensions{
										XValidations: []apiextensions.ValidationRule{
											{
												Rule:    `self == "value2" || self != "value"`,
												Message: "must be value2 or not value",
											},
										},
									},
								},
								"key2": {
									ValidationExtensions: schema.ValidationExtensions{
										XValidations: []apiextensions.ValidationRule{
											{
												Rule:    "self.size() == 5",
												Message: "len must be 5",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "allOf.items.allOf",
			obj:  map[string]interface{}{"myList": []interface{}{"value", "value2"}},
			errors: []string{
				`must be value2 or not value`,
				`len must be 5`,
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"myList": {
						Generic: schema.Generic{
							Type: "array",
						},
						Items: &stringType,
						ValueValidation: &schema.ValueValidation{
							AllOf: []schema.NestedValueValidation{
								{
									Items: &schema.NestedValueValidation{
										ValueValidation: schema.ValueValidation{
											AllOf: []schema.NestedValueValidation{
												{
													ValidationExtensions: schema.ValidationExtensions{
														XValidations: []apiextensions.ValidationRule{
															{
																Rule:    "self.size() == 5",
																Message: "len must be 5",
															},
														},
													},
												},
											},
										},
									},
								},
								{
									Items: &schema.NestedValueValidation{
										ValueValidation: schema.ValueValidation{
											AllOf: []schema.NestedValueValidation{
												{
													ValidationExtensions: schema.ValidationExtensions{
														XValidations: []apiextensions.ValidationRule{
															{
																Rule:    `self == "value2" || self != "value"`,
																Message: "must be value2 or not value",
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "properties.allOf.additionalProperties.allOf.properties",
			obj: map[string]interface{}{
				"myProperty": map[string]interface{}{
					"randomKey": map[string]interface{}{
						"key":  "value",
						"key2": "value2",
					},
				},
			},
			errors: []string{
				`must be value2 or not value`,
				`len must be 5`,
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"myProperty": {
						Generic: schema.Generic{
							Type: "object",
						},
						AdditionalProperties: &schema.StructuralOrBool{Structural: objectTypePtr(map[string]schema.Structural{
							"key":  stringType,
							"key2": stringType,
						})},
					},
				},
				ValueValidation: &schema.ValueValidation{
					AllOf: []schema.NestedValueValidation{
						{
							Properties: map[string]schema.NestedValueValidation{
								"myProperty": {
									AdditionalProperties: &schema.NestedValueValidation{
										ValueValidation: schema.ValueValidation{
											AllOf: []schema.NestedValueValidation{
												{
													Properties: map[string]schema.NestedValueValidation{
														"key": {
															ValidationExtensions: schema.ValidationExtensions{
																XValidations: []apiextensions.ValidationRule{
																	{
																		Rule:    `self == "value2" || self != "value"`,
																		Message: "must be value2 or not value",
																	},
																},
															},
														},
														"key2": {
															ValidationExtensions: schema.ValidationExtensions{
																XValidations: []apiextensions.ValidationRule{
																	{
																		Rule:    "self.size() == 5",
																		Message: "len must be 5",
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.TODO()
			celValidator := validator(tt.schema, tt.schema, true, model.SchemaDeclType(tt.schema, true), celconfig.PerCallLimit)
			if celValidator == nil {
				t.Fatal("expected non nil validator")
			}
			errs, _ := celValidator.Validate(ctx, field.NewPath("root"), tt.schema, tt.obj, tt.oldObj, math.MaxInt)
			unmatched := map[string]struct{}{}
			for _, e := range tt.errors {
				unmatched[e] = struct{}{}
			}
			for _, err := range errs {
				if err.Type != field.ErrorTypeInvalid {
					t.Errorf("expected only ErrorTypeInvalid errors, but got: %v", err)
					continue
				}
				matched := false
				for expected := range unmatched {
					if strings.Contains(err.Error(), expected) {
						delete(unmatched, expected)
						matched = true
						break
					}
				}
				if !matched {
					t.Errorf("expected error to contain one of %v, but got: %v", unmatched, err)
				}
			}
			if len(unmatched) > 0 {
				t.Errorf("expected errors %v", unmatched)
			}
		})
	}
}

func genMatchSelectorSchema(rule string) *schema.Structural {
	s := withRule(objectType(map[string]schema.Structural{
		"matchExpressions": listType(objectTypePtr(map[string]schema.Structural{
			"key":      stringType,
			"operator": stringType,
			"values":   listType(&stringType),
		})),
	}), rule)
	return &s
}

// TestCELValidationLimit 测试 CEL 校验的成本预算与单次调用限制。
func TestCELValidationLimit(t *testing.T) {
	tests := []struct {
		name   string
		schema *schema.Structural
		obj    interface{}
		valid  []string
	}{
		{
			name:   "test limit",
			obj:    objs(int64(math.MaxInt64)),
			schema: schemas(integerType),
			valid: []string{
				"self.val1 > 0",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			for j := range tt.valid {
				validRule := tt.valid[j]
				t.Run(validRule, func(t *testing.T) {
					t.Parallel()
					s := withRule(*tt.schema, validRule)
					celValidator := validator(&s, &s, false, model.SchemaDeclType(&s, false), celconfig.PerCallLimit)

					// 测试超出成本预算
					errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, nil, 0)
					var found bool
					for _, err := range errs {
						if err.Type == field.ErrorTypeInvalid && strings.Contains(err.Error(), "validation failed due to running out of cost budget, no further validation rules will be run") {
							found = true
						} else {
							t.Errorf("unexpected err: %v", err)
						}
					}
					if !found {
						t.Errorf("expect cost limit exceed err but did not find")
					}
					if len(errs) > 1 {
						t.Errorf("expect to only return cost budget exceed err once but got: %v", len(errs))
					}

					// 测试超出 PerCallLimit
					found = false
					celValidator = NewValidator(&s, true, 0)
					if celValidator == nil {
						t.Fatal("expected non nil validator")
					}
					errs, _ = celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, nil, celconfig.RuntimeCELCostBudget)
					for _, err := range errs {
						if err.Type == field.ErrorTypeInvalid && strings.Contains(err.Error(), "no further validation rules will be run due to call cost exceeds limit for rule") {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expect PerCostLimit exceed err but did not find")
					}
				})
			}
		})
	}

}

// TestCELValidationContextCancellation 测试上下文取消时 CEL 校验的中断行为。
func TestCELValidationContextCancellation(t *testing.T) {
	items := make([]interface{}, 1000)
	for i := int64(0); i < 1000; i++ {
		items[i] = i
	}
	tests := []struct {
		name   string
		schema *schema.Structural
		obj    map[string]interface{}
		rule   string
	}{
		{name: "test cel validation with context cancellation",
			obj: map[string]interface{}{
				"array": items,
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"array": listType(&integerType),
			}),
			rule: "self.array.map(e, e * 20).filter(e, e > 50).exists(e, e == 60)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			s := withRule(*tt.schema, tt.rule)
			celValidator := NewValidator(&s, true, celconfig.PerCallLimit)
			if celValidator == nil {
				t.Fatal("expected non nil validator")
			}
			errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, nil, celconfig.RuntimeCELCostBudget)
			for _, err := range errs {
				t.Errorf("unexpected error: %v", err)
			}

			// 测试上下文取消
			found := false
			evalCtx, cancel := context.WithTimeout(ctx, time.Microsecond)
			cancel()
			errs, _ = celValidator.Validate(evalCtx, field.NewPath("root"), &s, tt.obj, nil, celconfig.RuntimeCELCostBudget)
			for _, err := range errs {
				if err.Type == field.ErrorTypeInvalid && strings.Contains(err.Error(), "operation interrupted") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expect operation interrupted err but did not find")
			}
		})
	}
}

// 这是表达式可容纳的递归操作数的上限。
// 随着语法或 ANTLR 栈的改进，该数值可能增大，但绝不应减小，否则原本合法的表达式会被视为非法。
const maxValidDepth = 250

// TestCELMaxRecursionDepth 测试 CEL 的 maxRecursionDepth 设置。
func TestCELMaxRecursionDepth(t *testing.T) {
	tests := []struct {
		name          string
		schema        *schema.Structural
		obj           interface{}
		oldObj        interface{}
		valid         []string
		errors        map[string]string // rule -> 错误信息必须包含的字符串
		costBudget    int64
		isRoot        bool
		expectSkipped bool
	}{
		{name: "test CEL maxRecursionDepth",
			obj:    objs(true),
			schema: schemas(booleanType),
			valid: []string{
				strings.Repeat("self.val1"+" == ", maxValidDepth-1) + "self.val1",
			},
			errors: map[string]string{
				strings.Repeat("self.val1"+" == ", maxValidDepth) + "self.val1": "max recursion depth exceeded",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.costBudget = celconfig.RuntimeCELCostBudget
			ctx := context.TODO()
			for j := range tt.valid {
				validRule := tt.valid[j]
				testName := validRule
				t.Run(testName, func(t *testing.T) {
					t.Parallel()
					s := withRule(*tt.schema, validRule)
					celValidator := validator(&s, &s, tt.isRoot, model.SchemaDeclType(&s, tt.isRoot), celconfig.PerCallLimit)
					if celValidator == nil {
						t.Fatal("expected non nil validator")
					}
					errs, remainingBudget := celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, tt.oldObj, tt.costBudget)
					for _, err := range errs {
						t.Errorf("unexpected error: %v", err)
					}
					if tt.expectSkipped {
						// 跳过的校验不应产生开销。这里唯一可能的误报是 CEL 表达式 'true'。
						if remainingBudget != tt.costBudget {
							t.Errorf("expected no cost expended for skipped validation, but got %d remaining from %d budget", remainingBudget, tt.costBudget)
						}
						return
					}
				})
			}
			for rule, expectErrToContain := range tt.errors {
				testName := rule
				if len(testName) > 127 {
					testName = testName[:127]
				}
				t.Run(testName, func(t *testing.T) {
					s := withRule(*tt.schema, rule)
					celValidator := NewValidator(&s, true, celconfig.PerCallLimit)
					if celValidator == nil {
						t.Fatal("expected non nil validator")
					}
					errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, tt.oldObj, tt.costBudget)
					if len(errs) == 0 {
						t.Error("expected validation errors but got none")
					}
					for _, err := range errs {
						if err.Type != field.ErrorTypeInvalid || !strings.Contains(err.Error(), expectErrToContain) {
							t.Errorf("expected error to contain '%s', but got: %v", expectErrToContain, err)
						}
					}
				})
			}
		})
	}
}

// TestMessageExpression 测试 CEL 校验规则中 messageExpression 的求值。
func TestMessageExpression(t *testing.T) {
	klog.LogToStderr(false)
	klog.InitFlags(nil)
	setDefaultVerbosity(2)
	defer klog.LogToStderr(true)
	tests := []struct {
		name                    string
		costBudget              int64
		perCallLimit            uint64
		message                 string
		messageExpression       string
		expectedLogErr          string
		expectedValidationErr   string
		expectedRemainingBudget int64
	}{
		{
			name:                    "no cost error expected",
			messageExpression:       `"static string"`,
			expectedValidationErr:   "static string",
			costBudget:              300,
			expectedRemainingBudget: 300,
		},
		{
			name:                  "messageExpression takes precedence over message",
			message:               "invisible",
			messageExpression:     `"this is messageExpression"`,
			costBudget:            celconfig.RuntimeCELCostBudget,
			expectedValidationErr: "this is messageExpression",
		},
		{
			name:                    "default rule message used if messageExpression does not eval to string",
			messageExpression:       `true`,
			costBudget:              celconfig.RuntimeCELCostBudget,
			expectedValidationErr:   "failed rule",
			expectedRemainingBudget: celconfig.RuntimeCELCostBudget,
		},
		{
			name:                    "limit exceeded",
			messageExpression:       `"string 1" + "string 2" + "string 3"`,
			costBudget:              1,
			expectedValidationErr:   "messageExpression evaluation failed due to running out of cost budget",
			expectedRemainingBudget: -1,
		},
		{
			name:                    "messageExpression budget (str concat)",
			messageExpression:       `"str1 " + self.str`,
			costBudget:              50,
			expectedValidationErr:   "str1 a string",
			expectedRemainingBudget: 46,
		},
		{
			name:                    "runtime cost preserved if messageExpression fails during evaluation",
			message:                 "message not messageExpression",
			messageExpression:       `"str1 " + ["a", "b", "c", "d"][4]`,
			costBudget:              50,
			expectedLogErr:          "messageExpression evaluation failed due to: index out of bounds: 4",
			expectedValidationErr:   "message not messageExpression",
			expectedRemainingBudget: 47,
		},
		{
			name:                    "runtime cost preserved if messageExpression fails during evaluation (no message set)",
			messageExpression:       `"str1 " + ["a", "b", "c", "d"][4]`,
			costBudget:              50,
			expectedLogErr:          "messageExpression evaluation failed due to: index out of bounds: 4",
			expectedValidationErr:   "failed rule",
			expectedRemainingBudget: 47,
		},
		{
			name:                    "per-call limit exceeded during messageExpression execution",
			messageExpression:       `"string 1" + "string 2" + "string 3"`,
			costBudget:              celconfig.RuntimeCELCostBudget,
			perCallLimit:            1,
			expectedValidationErr:   "call cost exceeds limit for messageExpression",
			expectedRemainingBudget: -1,
		},
		{
			name:                  "messageExpression is not allowed to generate a string with newlines",
			message:               "message not messageExpression",
			messageExpression:     `"str with \na newline"`,
			costBudget:            celconfig.RuntimeCELCostBudget,
			expectedLogErr:        "messageExpression should not contain line breaks",
			expectedValidationErr: "message not messageExpression",
		},
		{
			name:                  "messageExpression is not allowed to generate messages >5000 characters",
			message:               "message not messageExpression",
			messageExpression:     fmt.Sprintf(`"%s"`, genString(5121, 'a')),
			costBudget:            celconfig.RuntimeCELCostBudget,
			expectedLogErr:        "messageExpression beyond allowable length of 5120",
			expectedValidationErr: "message not messageExpression",
		},
		{
			name:                  "messageExpression is not allowed to generate an empty string",
			message:               "message not messageExpression",
			messageExpression:     `string("")`,
			costBudget:            celconfig.RuntimeCELCostBudget,
			expectedLogErr:        "messageExpression should evaluate to a non-empty string",
			expectedValidationErr: "message not messageExpression",
		},
		{
			name:                  "messageExpression is not allowed to generate a string with only spaces",
			message:               "message not messageExpression",
			messageExpression:     `string("     ")`,
			costBudget:            celconfig.RuntimeCELCostBudget,
			expectedLogErr:        "messageExpression should evaluate to a non-empty string",
			expectedValidationErr: "message not messageExpression",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputBuffer := strings.Builder{}
			klog.SetOutput(&outputBuffer)

			ctx := context.TODO()
			var s schema.Structural
			if tt.message != "" {
				s = withRuleMessageAndMessageExpression(objectType(map[string]schema.Structural{
					"str": stringType}), "false", tt.message, tt.messageExpression)
			} else {
				s = withRuleAndMessageExpression(objectType(map[string]schema.Structural{
					"str": stringType}), "false", tt.messageExpression)
			}
			obj := map[string]interface{}{
				"str": "a string",
			}

			callLimit := uint64(celconfig.PerCallLimit)
			if tt.perCallLimit != 0 {
				callLimit = tt.perCallLimit
			}
			celValidator := NewValidator(&s, false, callLimit)
			if celValidator == nil {
				t.Fatal("expected non nil validator")
			}
			errs, remainingBudget := celValidator.Validate(ctx, field.NewPath("root"), &s, obj, nil, tt.costBudget)
			klog.Flush()

			if len(errs) != 1 {
				t.Fatalf("expected 1 error, got %d", len(errs))
			}

			if tt.expectedLogErr != "" {
				if !strings.Contains(outputBuffer.String(), tt.expectedLogErr) {
					t.Fatalf("did not find expected log error message: %q\n%q", tt.expectedLogErr, outputBuffer.String())
				}
			} else if tt.expectedLogErr == "" && outputBuffer.String() != "" {
				t.Fatalf("expected no log output, got: %q", outputBuffer.String())
			}

			if tt.expectedValidationErr != "" {
				if !strings.Contains(errs[0].Error(), tt.expectedValidationErr) {
					t.Fatalf("did not find expected validation error message: %q", tt.expectedValidationErr)
				}
			}

			if tt.expectedRemainingBudget != 0 {
				if tt.expectedRemainingBudget != remainingBudget {
					t.Fatalf("expected %d cost left, got %d", tt.expectedRemainingBudget, remainingBudget)
				}
			}
		})
	}
}

// TestReasonAndFldPath 测试 CEL 校验失败时的 reason 与 fieldPath 输出。
func TestReasonAndFldPath(t *testing.T) {
	forbiddenReason := func() *apiextensions.FieldValueErrorReason {
		r := apiextensions.FieldValueForbidden
		return &r
	}()
	tests := []struct {
		name   string
		schema *schema.Structural
		obj    interface{}
		errors field.ErrorList
	}{
		{name: "Return error based on input reason",
			obj: map[string]interface{}{
				"f": map[string]interface{}{
					"m": 1,
				},
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"f": withReasonAndFldPath(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 2", "", forbiddenReason),
			}), "1 == 1"),
			errors: field.ErrorList{
				{
					Type:  field.ErrorTypeForbidden,
					Field: "root.f",
				},
			},
		},
		{name: "Return error default is invalid",
			obj: map[string]interface{}{
				"f": map[string]interface{}{
					"m": 1,
				},
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"f": withReasonAndFldPath(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 2", "", nil),
			}), "1 == 1"),
			errors: field.ErrorList{
				{
					Type:  field.ErrorTypeInvalid,
					Field: "root.f",
				},
			},
		},
		{name: "Return error based on input fieldPath",
			obj: map[string]interface{}{
				"f": map[string]interface{}{
					"m": 1,
				},
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"f": withReasonAndFldPath(objectType(map[string]schema.Structural{"m": integerType}), "self.m == 2", ".m", forbiddenReason),
			}), "1 == 1"),
			errors: field.ErrorList{
				{
					Type:  field.ErrorTypeForbidden,
					Field: "root.f.m",
				},
			},
		},
		{
			name: "multiple rules with custom reason and field path",
			obj: map[string]interface{}{
				"field1": "value1",
				"field2": "value2",
				"field3": "value3",
			},
			schema: &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"field1": stringType,
					"field2": stringType,
					"field3": stringType,
				},
				ValidationExtensions: schema.ValidationExtensions{
					XValidations: apiextensions.ValidationRules{
						{
							Rule:      `self.field2 != "value2"`,
							Reason:    ptr.To(apiextensions.FieldValueDuplicate),
							FieldPath: ".field2",
						},
						{
							Rule:      `self.field3 != "value3"`,
							Reason:    ptr.To(apiextensions.FieldValueRequired),
							FieldPath: ".field3",
						},
						{
							Rule:      `self.field1 != "value1"`,
							Reason:    ptr.To(apiextensions.FieldValueForbidden),
							FieldPath: ".field1",
						},
					},
				},
			},
			errors: field.ErrorList{
				{
					Type:  field.ErrorTypeDuplicate,
					Field: "root.field2",
				},
				{
					Type:  field.ErrorTypeRequired,
					Field: "root.field3",
				},
				{
					Type:  field.ErrorTypeForbidden,
					Field: "root.field1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			celValidator := validator(tt.schema, tt.schema, true, model.SchemaDeclType(tt.schema, true), celconfig.PerCallLimit)
			if celValidator == nil {
				t.Fatal("expected non nil validator")
			}
			errs, _ := celValidator.Validate(ctx, field.NewPath("root"), tt.schema, tt.obj, nil, celconfig.RuntimeCELCostBudget)

			for i := range errs {
				// 本测试忽略未检查的字段
				errs[i].Detail = ""
				errs[i].BadValue = nil
			}

			require.ElementsMatch(t, tt.errors, errs)
		})
	}
}

// TestValidateFieldPath 测试 CEL 校验规则中 fieldPath 的校验。
func TestValidateFieldPath(t *testing.T) {
	sts := schema.Structural{
		Generic: schema.Generic{
			Type: "object",
		},
		Properties: map[string]schema.Structural{
			"foo": {
				Generic: schema.Generic{
					Type: "object",
				},
				AdditionalProperties: &schema.StructuralOrBool{
					Structural: &schema.Structural{
						Generic: schema.Generic{
							Type: "object",
						},
						Properties: map[string]schema.Structural{
							"subAdd": {
								Generic: schema.Generic{
									Type: "number",
								},
							},
						},
					},
				},
			},
			"white space": {
				Generic: schema.Generic{
					Type: "number",
				},
			},
			"'foo'bar": {
				Generic: schema.Generic{
					Type: "number",
				},
			},
			"a": {
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"foo's": {
						Generic: schema.Generic{
							Type: "number",
						},
					},
					"test\a": {
						Generic: schema.Generic{
							Type: "number",
						},
					},
					"bb[b": {
						Generic: schema.Generic{
							Type: "number",
						},
					},
					"bbb": {
						Generic: schema.Generic{
							Type: "object",
						},
						Properties: map[string]schema.Structural{
							"c": {
								Generic: schema.Generic{
									Type: "number",
								},
							},
							"34": {
								Generic: schema.Generic{
									Type: "number",
								},
							},
						},
					},
					"bbb.c": {
						Generic: schema.Generic{
							Type: "object",
						},
						Properties: map[string]schema.Structural{
							"a-b.3'4": {
								Generic: schema.Generic{
									Type: "number",
								},
							},
						},
					},
				},
			},
			"list": {
				Generic: schema.Generic{
					Type: "array",
				},
				Items: &schema.Structural{
					Generic: schema.Generic{
						Type: "object",
					},
					Properties: map[string]schema.Structural{
						"a": {
							Generic: schema.Generic{
								Type: "number",
							},
						},
						"a-b.3'4": {
							Generic: schema.Generic{
								Type: "number",
							},
						},
					},
				},
			},
		},
	}

	path := field.NewPath("")

	tests := []struct {
		name            string
		fieldPath       string
		pathOfFieldPath *field.Path
		schema          *schema.Structural
		errDetail       string
		validFieldPath  *field.Path
	}{
		{
			name:            "Valid .a",
			fieldPath:       ".a",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("a"),
		},
		{
			name:            "Valid 'foo'bar",
			fieldPath:       "['\\'foo\\'bar']",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("'foo'bar"),
		},
		{
			name:            "Invalid 'foo'bar",
			fieldPath:       ".\\'foo\\'bar",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "Invalid with whitespace",
			fieldPath:       ". a",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "Valid with whitespace inside field",
			fieldPath:       ".white space",
			pathOfFieldPath: path,
			schema:          &sts,
		},
		{
			name:            "Valid with whitespace inside field",
			fieldPath:       "['white space']",
			pathOfFieldPath: path,
			schema:          &sts,
		},
		{
			name:            "invalid dot annotation",
			fieldPath:       ".a.bb[b",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "valid with .",
			fieldPath:       ".a['bbb.c']",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("a", "bbb.c"),
		},
		{
			name:            "Unclosed ]",
			fieldPath:       ".a['bbb.c'",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "unexpected end of JSON path",
		},
		{
			name:            "Unexpected end of JSON path",
			fieldPath:       ".",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "unexpected end of JSON path",
		},
		{
			name:            "Valid map syntax .a.bbb",
			fieldPath:       ".a['bbb.c']",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("a").Child("bbb.c"),
		},
		{
			name:            "Valid map key",
			fieldPath:       ".foo.subAdd",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("foo").Key("subAdd"),
		},
		{
			name:            "Valid map key",
			fieldPath:       ".foo['subAdd']",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("foo").Key("subAdd"),
		},
		{
			name:            "Invalid",
			fieldPath:       ".a.foo's",
			pathOfFieldPath: path,
			schema:          &sts,
		},
		{
			name:            "Escaping",
			fieldPath:       ".a['foo\\'s']",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("a").Child("foo's"),
		},
		{
			name:            "Escaping",
			fieldPath:       ".a['test\\a']",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("a").Child("test\a"),
		},

		{
			name:            "Invalid map key",
			fieldPath:       ".a.foo",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "Malformed map key",
			fieldPath:       ".a.bbb[0]",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "expected single quoted string but got 0",
		},
		{
			name:            "Valid refer for name has number",
			fieldPath:       ".a.bbb.34",
			pathOfFieldPath: path,
			schema:          &sts,
		},
		{
			name:            "Map syntax for special field names",
			fieldPath:       ".a.bbb['34']",
			pathOfFieldPath: path,
			schema:          &sts,
			//errDetail:       "does not refer to a valid field",
		},
		{
			name:            "Valid .list",
			fieldPath:       ".list",
			pathOfFieldPath: path,
			schema:          &sts,
			validFieldPath:  path.Child("list"),
		},
		{
			name:            "Invalid list index",
			fieldPath:       ".list[0]",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "expected single quoted string but got 0",
		},
		{
			name:            "Invalid list reference",
			fieldPath:       ".list. a",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "Invalid .list.a",
			fieldPath:       ".list['a']",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "Missing leading dot",
			fieldPath:       "a",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "expected [ or . but got: a",
		},
		{
			name:            "Nonexistent field",
			fieldPath:       ".c",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "Duplicate dots",
			fieldPath:       ".a..b",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
		{
			name:            "object of array",
			fieldPath:       ".list.a-b.34",
			pathOfFieldPath: path,
			schema:          &sts,
			errDetail:       "does not refer to a valid field",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			validField, _, err := ValidFieldPath(tc.fieldPath, tc.schema)

			if err == nil && tc.errDetail != "" {
				t.Errorf("expected err contains: %v but get nil", tc.errDetail)
			}
			if err != nil && tc.errDetail == "" {
				t.Errorf("unexpected error: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), tc.errDetail) {
				t.Errorf("expected error to contain: %v, but get: %v", tc.errDetail, err)
			}
			if tc.validFieldPath != nil && tc.validFieldPath.String() != path.Child(validField.String()).String() {
				t.Errorf("expected %v, got %v", tc.validFieldPath, path.Child(validField.String()))
			}
		})
	}
}

// FixTabsOrDie 统计给定 yaml 对象首行之前的制表符数量，
// 并从每一行移除相同数量的制表符。
// 若某行的制表符少于首行则 panic（这是测试函数）。
//
// 目的是让测试更易读。
func FixTabsOrDie(in string) string {
	lines := bytes.Split([]byte(in), []byte{'\n'})
	if len(lines[0]) == 0 && len(lines) > 1 {
		lines = lines[1:]
	}
	// 构造要移除的制表符前缀。
	var prefix []byte
	for _, c := range lines[0] {
		if c != '\t' {
			break
		}
		prefix = append(prefix, byte('\t'))
	}
	// 从所有行移除该前缀，否则失败。
	for i := range lines {
		line := lines[i]
		// 最后一行可以为空（尾部 \n）
		if i == len(lines)-1 && len(line) <= len(prefix) && bytes.TrimSpace(line) == nil {
			lines[i] = []byte{}
			break
		}
		if !bytes.HasPrefix(line, prefix) {
			minRange := i - 5
			maxRange := i + 5
			if minRange < 0 {
				minRange = 0
			}
			if maxRange > len(lines) {
				maxRange = len(lines)
			}
			panic(fmt.Errorf("line %d doesn't start with expected number (%d) of tabs (%v-%v):\n%v", i, len(prefix), minRange, maxRange, string(bytes.Join(lines[minRange:maxRange], []byte{'\n'}))))
		}
		lines[i] = line[len(prefix):]
	}

	joined := string(bytes.Join(lines, []byte{'\n'}))

	// 把剩余的制表符转换为空格，因为 yaml 不喜欢制表符
	// （假设按 2 空格对齐）
	return strings.ReplaceAll(joined, "\t", "  ")
}

// 通过解码给定 YAML 创建 *spec.Schema，出错则 panic
func mustSchema(source string) *schema.Structural {
	source = FixTabsOrDie(source)
	d := yaml.NewYAMLOrJSONDecoder(strings.NewReader(source), 4096)
	props := &apiextensions.JSONSchemaProps{}
	if err := d.Decode(props); err != nil {
		panic(err)
	}
	convertedProps := &apiextensionsinternal.JSONSchemaProps{}
	if err := apiextensions.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(props, convertedProps, nil); err != nil {
		panic(err)
	}

	res, err := schema.NewStructural(convertedProps)
	if err != nil {
		panic(err)
	}
	return res
}

// 通过解码给定 YAML 创建 *unstructured，出错则 panic
func mustUnstructured(source string) interface{} {
	source = FixTabsOrDie(source)
	d := yaml.NewYAMLOrJSONDecoder(strings.NewReader(source), 4096)
	var res interface{}
	if err := d.Decode(&res); err != nil {
		panic(err)
	}
	return res
}

type warningRecorder struct {
	mu       sync.Mutex
	warnings []string
}

// AddWarning 向 recorder 添加一条警告。
func (r *warningRecorder) AddWarning(agent, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warnings = append(r.warnings, text)
}

func (r *warningRecorder) Warnings() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	warnings := make([]string, len(r.warnings))
	copy(warnings, r.warnings)
	return warnings
}

// TestRatcheting 测试 CEL 校验错误的 ratcheting（棘轮）行为。
func TestRatcheting(t *testing.T) {
	cases := []struct {
		name   string
		schema *schema.Structural
		oldObj interface{}
		newObj interface{}

		// 启用 ratcheting 特性时求值该操作应出现的错误
		errors []string

		// 禁用 ratcheting 特性时求值该操作应出现的错误
		// 启用 ratcheting 时，这些错误应作为警告抛出
		warnings []string

		runtimeCostBudget int64
	}{
		{
			name: "normal CEL expression",
			schema: mustSchema(`
				type: object
				properties:
					foo:
						type: string
						x-kubernetes-validations:
						- rule: self == "bar"
						  message: "gotta be bar"
				`),
			oldObj: mustUnstructured(`
				foo: baz
			`),
			newObj: mustUnstructured(`
				foo: baz
			`),
			warnings: []string{
				`root.foo: Invalid value: "baz": gotta be bar`,
			},
		},
		{
			name: "normal CEL expression thats a descendent of an atomic array whose parent is totally unchanged",
			schema: mustSchema(`
				type: array
				x-kubernetes-list-type: atomic
				items:
					type: object
					properties:
						bar:
							type: string
							x-kubernetes-validations:
							- rule: self == "baz"
							  message: "gotta be baz"
				`),
			// CEL 错误来自 schema 的不可关联部分，
			// 但由于它是未变更的可关联节点的后代，
			// 仍应被 ratchet
			oldObj: mustUnstructured(`
				- bar: bar
			`),
			newObj: mustUnstructured(`
				- bar: bar
			`),
			warnings: []string{
				`root[0].bar: Invalid value: "bar": gotta be baz`,
			},
		},
		{
			name: "normal CEL expression thats a descendent of a set whose parent is totally unchanged",
			schema: mustSchema(`
				type: array
				x-kubernetes-list-type: set
				items:
					type: number
					x-kubernetes-validations:
					- rule: int(self) % 2 == 1
					  message: "gotta be odd"
				`),
			// CEL 错误来自 schema 的不可关联部分，
			// 但由于它是未变更的可关联节点的后代，
			// 仍应被 ratchet
			oldObj: mustUnstructured(`
				- 1
				- 2
			`),
			newObj: mustUnstructured(`
				- 1
				- 2
			`),
			warnings: []string{
				`root[1]: Invalid value: 2: gotta be odd`,
			},
		},
		{
			name: "normal CEL expression thats a descendent of a set and one of its siblings has changed",
			schema: mustSchema(`
				type: object
				properties:
					stringField:
						type: string
					setArray:
						type: array
						x-kubernetes-list-type: set
						items:
							type: number
							x-kubernetes-validations:
							- rule: int(self) % 2 == 1
							  message: "gotta be odd"
				`),
			oldObj: mustUnstructured(`
				stringField: foo
				setArray:
				- 1
				- 3
				- 2
			`),
			newObj: mustUnstructured(`
				stringField: changed but ratcheted
				setArray:
				- 1
				- 3
				- 2
			`),
			warnings: []string{
				`root.setArray[2]: Invalid value: 2: gotta be odd`,
			},
		},
		{
			name: "descendent of a map list whose parent is unchanged",
			schema: mustSchema(`
				type: array
				x-kubernetes-list-type: map
				x-kubernetes-list-map-keys: ["key"]
				items:
					type: object
					properties:
						key:
							type: string
						value:
							type: string
							x-kubernetes-validations:
							- rule: self == "baz"
							  message: "gotta be baz"
			`),
			oldObj: mustUnstructured(`
				- key: foo
				  value: notbaz
				- key: bar
				  value: notbaz
			`),
			newObj: mustUnstructured(`
				- key: foo
				  value: notbaz
				- key: bar
				  value: notbaz
				- key: baz
				  value: baz
			`),
			warnings: []string{
				`root[0].value: Invalid value: "notbaz": gotta be baz`,
				`root[1].value: Invalid value: "notbaz": gotta be baz`,
			},
		},
		{
			name: "descendent of a map list whose siblings have changed",
			schema: mustSchema(`
				type: array
				x-kubernetes-list-type: map
				x-kubernetes-list-map-keys: ["key"]
				items:
					type: object
					properties:
						key:
							type: string
						value:
							type: string
							x-kubernetes-validations:
							- rule: self == "baz"
							  message: "gotta be baz"
			`),
			oldObj: mustUnstructured(`
				- key: foo
				  value: notbaz
				- key: bar
				  value: notbaz
			`),
			newObj: mustUnstructured(`
				- key: foo
				  value: baz
				- key: bar
				  value: notbaz
			`),
			warnings: []string{
				`root[1].value: Invalid value: "notbaz": gotta be baz`,
			},
		},
		{
			name: "descendent of a map whose parent is totally unchanged",
			schema: mustSchema(`
				type: object
				properties:
					stringField:
						type: string
					mapField:
						type: object
						properties:
							foo:
								type: string
								x-kubernetes-validations:
								- rule: self == "baz"
								  message: "gotta be baz"
							mapField:
								type: object
								properties:
									bar:
										type: string
										x-kubernetes-validations:
										- rule: self == "baz"
										  message: "gotta be nested baz"
				`),
			oldObj: mustUnstructured(`
				stringField: foo
				mapField:
					foo: notbaz
					mapField:
						bar: notbaz
			`),
			newObj: mustUnstructured(`
				stringField: foo
				mapField:
					foo: notbaz
					mapField:
						bar: notbaz
			`),
			warnings: []string{
				`root.mapField.foo: Invalid value: "notbaz": gotta be baz`,
				`root.mapField.mapField.bar: Invalid value: "notbaz": gotta be nested baz`,
			},
		},
		{
			name: "descendent of a map whose siblings have changed",
			schema: mustSchema(`
				type: object
				properties:
					stringField:
						type: string
					mapField:
						type: object
						properties:
							foo:
								type: string
								x-kubernetes-validations:
								- rule: self == "baz"
								  message: "gotta be baz"
							mapField:
								type: object
								properties:
									bar:
										type: string
										x-kubernetes-validations:
										- rule: self == "baz"
										  message: "gotta be baz"
									otherBar:
										type: string
										x-kubernetes-validations:
										- rule: self == "otherBaz"
										  message: "gotta be otherBaz"
				`),
			oldObj: mustUnstructured(`
				stringField: foo
				mapField:
					foo: baz
					mapField:
						bar: notbaz
						otherBar: nototherBaz
			`),
			newObj: mustUnstructured(`
				stringField: foo
				mapField:
					foo: notbaz
					mapField:
						bar: notbaz
						otherBar: otherBaz
			`),
			errors: []string{
				// 未 ratchet，因为值从 baz 改成了 notbaz
				`root.mapField.foo: Invalid value: "notbaz": gotta be baz`,
			},
			warnings: []string{
				// 已 ratchet，因为值未变，尽管它无效
				`root.mapField.mapField.bar: Invalid value: "notbaz": gotta be baz`,
			},
		},
		{
			name: "normal CEL expression thats a descendent of an atomic array whose siblings has changed",
			schema: mustSchema(`
				type: object
				properties:
					stringField:
						type: string
					atomicArray:
						type: array
						x-kubernetes-list-type: atomic
						items:
							type: object
							properties:
								bar:
									type: string
									x-kubernetes-validations:
									- rule: self == "baz"
									  message: "gotta be baz"
				`),
			oldObj: mustUnstructured(`
				stringField: foo
				atomicArray:
				- bar: bar
			`),
			newObj: mustUnstructured(`
				stringField: changed but ratcheted
				atomicArray:
				- bar: bar
			`),
			warnings: []string{
				`root.atomicArray[0].bar: Invalid value: "bar": gotta be baz`,
			},
		},
		{
			name: "we can't ratchet a normal CEL expression from an uncorrelatable part of the schema whose parent nodes has changed",
			schema: mustSchema(`
				type: array
				x-kubernetes-list-type: atomic
				items:
					type: object
					properties:
						bar:
							type: string
							x-kubernetes-validations:
							- rule: self == "baz"
							  message: "gotta be baz"
			`),
			// CEL 错误来自 schema 的不可关联部分，
			// 但由于它是未变更的可关联节点的后代，
			// 或未变更的可关联节点的后代
			oldObj: mustUnstructured(`
				- bar: bar
			`),
			newObj: mustUnstructured(`
				- bar: bar
				- bar: baz
			`),
			errors: []string{
				`root[0].bar: Invalid value: "bar": gotta be baz`,
			},
		},
		{
			name: "transition rules never ratchet for correlatable schemas",
			schema: mustSchema(`
				type: object
				properties:
					foo:
						type: string
						x-kubernetes-validations:
						- rule: oldSelf != "bar" && self == "baz"
						  message: gotta be baz
			`),
			oldObj: mustUnstructured(`
				foo: bar
			`),
			newObj: mustUnstructured(`
				foo: bar
			`),
			errors: []string{
				`root.foo: Invalid value: "bar": gotta be baz`,
			},
		},
		{
			name: "changing field path does not change ratcheting logic",
			schema: mustSchema(`
				type: object
				x-kubernetes-validations:
				- rule: self.foo == "baz"
				  message: gotta be baz
				  fieldPath: ".foo"
				properties:
					bar:
						type: string
					foo:
						type: string
			`),
			oldObj: mustUnstructured(`
				foo: bar
			`),
			// 字段路径位于未变更字段 `foo` 上，但由于规则位于
			// 已变更的父对象，仍会报错
			newObj: mustUnstructured(`
				foo: bar
				bar: invalid
			`),
			errors: []string{
				`root.foo: Invalid value: gotta be baz`,
			},
		},
		{
			name: "cost budget errors are not ratcheted",
			schema: mustSchema(`
				type: string
				minLength: 5
				x-kubernetes-validations:
				- rule: self == "baz"
				  message: gotta be baz
			`),
			oldObj:            "unchanged",
			newObj:            "unchanged",
			runtimeCostBudget: 1,
			errors: []string{
				`validation failed due to running out of cost budget, no further validation rules will be run`,
			},
		},
		{
			name: "compile errors are not ratcheted",
			schema: mustSchema(`
				type: string
				x-kubernetes-validations:
				- rule: asdausidyhASDNJm
				  message: gotta be baz
			`),
			oldObj: "unchanged",
			newObj: "unchanged",
			errors: []string{
				`rule compile error: compilation failed: ERROR: <input>:1:1: undeclared reference to 'asdausidyhASDNJm'`,
			},
		},
		{
			name: "typemeta fields are not ratcheted",
			schema: mustSchema(`
				type: object
				properties:
					apiVersion:
						type: string
						x-kubernetes-validations:
						- rule: self == "v1"
					kind:
						type: string
						x-kubernetes-validations:
						- rule: self == "Pod"
			`),
			oldObj: mustUnstructured(`
				apiVersion: v2
				kind: Baz
			`),
			newObj: mustUnstructured(`
				apiVersion: v2
				kind: Baz
			`),
			errors: []string{
				`root.apiVersion: Invalid value: "v2": failed rule: self == "v1"`,
				`root.kind: Invalid value: "Baz": failed rule: self == "Pod"`,
			},
		},
		{
			name: "nested typemeta fields may still be ratcheted",
			schema: mustSchema(`
				type: object
				properties:
					list:
						type: array
						x-kubernetes-list-type: map
						x-kubernetes-list-map-keys: ["name"]
						maxItems: 2
						items:
							type: object
							properties:
								name:
									type: string
								apiVersion:
									type: string
									x-kubernetes-validations:
									- rule: self == "v1"
								kind:
									type: string
									x-kubernetes-validations:
									- rule: self == "Pod"
					subField:
						type: object
						properties:
							apiVersion:
								type: string
								x-kubernetes-validations:
								- rule: self == "v1"
							kind:
								type: string
								x-kubernetes-validations:
								- rule: self == "Pod"
							otherField:
								type: string
			`),
			oldObj: mustUnstructured(`
				subField:
					apiVersion: v2
					kind: Baz
				list:	
				- name: entry1
				  apiVersion: v2
				  kind: Baz
				- name: entry2
				  apiVersion: v3
				  kind: Bar
			`),
			newObj: mustUnstructured(`
				subField:
					apiVersion: v2
					kind: Baz
					otherField: newValue
				list:	
				- name: entry1
				  apiVersion: v2
				  kind: Baz
				  otherField: newValue2
				- name: entry2
				  apiVersion: v3
				  kind: Bar
				  otherField: newValue3
			`),
			warnings: []string{
				`root.subField.apiVersion: Invalid value: "v2": failed rule: self == "v1"`,
				`root.subField.kind: Invalid value: "Baz": failed rule: self == "Pod"`,
				`root.list[0].apiVersion: Invalid value: "v2": failed rule: self == "v1"`,
				`root.list[0].kind: Invalid value: "Baz": failed rule: self == "Pod"`,
				`root.list[1].apiVersion: Invalid value: "v3": failed rule: self == "v1"`,
				`root.list[1].kind: Invalid value: "Bar": failed rule: self == "Pod"`,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			validator := NewValidator(c.schema, false, celconfig.PerCallLimit)
			require.NotNil(t, validator)
			recorder := &warningRecorder{}
			ctx := warning.WithWarningRecorder(context.TODO(), recorder)
			budget := c.runtimeCostBudget
			if budget == 0 {
				budget = celconfig.RuntimeCELCostBudget
			}
			errs, _ := validator.Validate(
				ctx,
				field.NewPath("root"),
				c.schema,
				c.newObj,
				c.oldObj,
				budget,
				WithRatcheting(common.NewCorrelatedObject(c.newObj, c.oldObj, &model.Structural{Structural: c.schema})),
			)

			require.Len(t, errs, len(c.errors), "must have expected number of errors")
			require.Len(t, recorder.Warnings(), len(c.warnings), "must have expected number of warnings")

			// 检查已抛出预期错误
			for _, expectedErr := range c.errors {
				found := false
				for _, err := range errs {
					if strings.Contains(err.Error(), expectedErr) {
						found = true
						break
					}
				}

				assert.True(t, found, "expected error %q not found", expectedErr)
			}

			// 检查禁用 ratcheting 时的错误已作为警告抛出
			for _, expectedWarning := range c.warnings {
				found := false
				for _, warning := range recorder.Warnings() {
					if warning == expectedWarning {
						found = true
						break
					}
				}
				assert.True(t, found, "expected warning %q not found", expectedWarning)
			}

		})
	}
}

// 在 schema 上设置 OptionalOldSelf 为 true 运行转换规则用例。
func TestOptionalOldSelf(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, apiextensionsfeatures.CRDValidationRatcheting, true)

	tests := []struct {
		name   string
		schema *schema.Structural
		obj    interface{}
		oldObj interface{}
		errors []string // 错误信息必须包含的字符串
	}{
		{
			name: "allow new value if old value is null",
			obj: map[string]interface{}{
				"foo": "bar",
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"foo": stringType,
			}), "self.foo == 'not bar' || !oldSelf.hasValue()"),
		},
		{
			name: "block new value if old value is not null",
			obj: map[string]interface{}{
				"foo": "invalid",
			},
			oldObj: map[string]interface{}{
				"foo": "bar",
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"foo": stringType,
			}), "self.foo == 'valid' || !oldSelf.hasValue()"),
			errors: []string{"failed rule"},
		},
		{
			name: "allow invalid new value if old value is also invalid",
			obj: map[string]interface{}{
				"foo": "invalid again",
			},
			oldObj: map[string]interface{}{
				"foo": "invalid",
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"foo": stringType,
			}), "self.foo == 'valid' || (oldSelf.hasValue() && oldSelf.value().foo != 'valid')"),
		},
		{
			name: "allow invalid new value if old value is also invalid with chained optionals",
			obj: map[string]interface{}{
				"foo": "invalid again",
			},
			oldObj: map[string]interface{}{
				"foo": "invalid",
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"foo": stringType,
			}), "self.foo == 'valid' || oldSelf.foo.orValue('') != 'valid'"),
		},
		{
			name: "block invalid new value if old value is valid",
			obj: map[string]interface{}{
				"foo": "invalid",
			},
			oldObj: map[string]interface{}{
				"foo": "valid",
			},
			schema: withRulePtr(objectTypePtr(map[string]schema.Structural{
				"foo": stringType,
			}), "self.foo == 'valid' || (oldSelf.hasValue() && oldSelf.value().foo != 'valid')"),
			errors: []string{"failed rule"},
		},
		{
			name:   "create: new min or allow higher than oldValue",
			obj:    10,
			schema: cloneWithRule(&integerType, "self >= 10 || (oldSelf.hasValue() && oldSelf.value() <= self)"),
		},
		{
			name: "block create: new min or allow higher than oldValue",
			obj:  9,
			// 因为类型是 integer 且没有重载，不能用 != null
			// 改为比较类型，但有点取巧
			schema: cloneWithRule(&integerType, "self >= 10 || (oldSelf.hasValue() && oldSelf.value() <= self)"),
			errors: []string{"failed rule"},
		},
		{
			name:   "update: new min or allow higher than oldValue",
			obj:    10,
			oldObj: 5,
			schema: cloneWithRule(&integerType, "self >= 10 || (oldSelf.hasValue() && oldSelf.value() <= self)"),
		},
		{
			name:   "ratchet update: new min or allow higher than oldValue",
			obj:    9,
			oldObj: 5,
			schema: cloneWithRule(&integerType, "self >= 10 || (oldSelf.hasValue() && oldSelf.value() <= self)"),
		},
		{
			name:   "ratchet noop update: new min or allow higher than oldValue",
			obj:    5,
			oldObj: 5,
			schema: cloneWithRule(&integerType, "self >= 10 || (oldSelf.hasValue() && oldSelf.value() <= self)"),
		},
		{
			name:   "block update: new min or allow higher than oldValue",
			obj:    4,
			oldObj: 5,
			schema: cloneWithRule(&integerType, "self >= 10 || (oldSelf.hasValue() && oldSelf.value() <= self)"),
			errors: []string{"failed rule"},
		},
	}

	for _, tt := range tests {
		tp := true
		for i := range tt.schema.XValidations {
			tt.schema.XValidations[i].OptionalOldSelf = &tp
		}

		t.Run(tt.name, func(t *testing.T) {
			// t.Parallel()

			ctx := context.TODO()
			celValidator := validator(tt.schema, tt.schema, true, model.SchemaDeclType(tt.schema, false), celconfig.PerCallLimit)
			if celValidator == nil {
				t.Fatal("expected non nil validator")
			}
			errs, _ := celValidator.Validate(ctx, field.NewPath("root"), tt.schema, tt.obj, tt.oldObj, math.MaxInt)
			unmatched := map[string]struct{}{}
			for _, e := range tt.errors {
				unmatched[e] = struct{}{}
			}
			for _, err := range errs {
				if err.Type != field.ErrorTypeInvalid {
					t.Errorf("expected only ErrorTypeInvalid errors, but got: %v", err)
					continue
				}
				matched := false
				for expected := range unmatched {
					if strings.Contains(err.Error(), expected) {
						delete(unmatched, expected)
						matched = true
						break
					}
				}
				if !matched {
					t.Errorf("expected error to contain one of %v, but got: %v", unmatched, err)
				}
			}
			if len(unmatched) > 0 {
				t.Errorf("expected errors %v", unmatched)
			}
		})
	}
}

// 表明 type(oldSelf) == null_type 对所有受支持的 OpenAPI 类型都有效，
// 无论 oldSelf 为 null 还是非 null
func TestOptionalOldSelfCheckForNull(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, apiextensionsfeatures.CRDValidationRatcheting, true)

	tests := []struct {
		name   string
		schema schema.Structural
		obj    interface{}
		oldObj interface{}
	}{
		{
			name: "object",
			obj: map[string]interface{}{
				"foo": "bar",
			},
			oldObj: map[string]interface{}{
				"foo": "baz",
			},
			schema: withRule(objectType(map[string]schema.Structural{
				"foo": stringType,
			}), `!oldSelf.hasValue() || self.foo == "bar"`),
		},
		{
			name: "object - conditional field",
			obj: map[string]interface{}{
				"foo": "bar",
			},
			oldObj: map[string]interface{}{
				"foo": "baz",
			},
			schema: withRule(objectType(map[string]schema.Structural{
				"foo": stringType,
			}), `self.foo != "bar" || oldSelf.?foo.orValue("baz") == "baz"`),
		},
		{
			name:   "string",
			obj:    "bar",
			oldObj: "baz",
			schema: withRule(stringType, `
				!oldSelf.hasValue() || self == "bar"
			`),
		},
		{
			name:   "integer",
			obj:    1,
			oldObj: 2,
			schema: withRule(integerType, `
				!oldSelf.hasValue() || self == 1
			`),
		},
		{
			name:   "number",
			obj:    1.1,
			oldObj: 2.2,
			schema: withRule(numberType, `
				!oldSelf.hasValue() || self == 1.1
			`),
		},
		{
			name:   "boolean",
			obj:    true,
			oldObj: false,
			schema: withRule(booleanType, `
				!oldSelf.hasValue() || self == true
			`),
		},
		{
			name:   "array",
			obj:    []interface{}{"bar"},
			oldObj: []interface{}{"baz"},
			schema: withRule(arrayType("", nil, &stringSchema), `
				!oldSelf.hasValue() || self[0] == "bar"
			`),
		},
		{
			name: "array - conditional index",
			obj:  []interface{}{},
			oldObj: []interface{}{
				"baz",
			},
			schema: withRule(arrayType("", nil, &stringSchema), `
				self.size() > 0 || oldSelf[?0].orValue("baz") == "baz"
			`),
		},
		{
			name:   "set-array",
			obj:    []interface{}{"bar"},
			oldObj: []interface{}{"baz"},
			schema: withRule(arrayType("set", nil, &stringSchema), `
				!oldSelf.hasValue() || self[0] == "bar"
			`),
		},
		{
			name: "map-array",
			obj: []interface{}{map[string]interface{}{
				"key":   "foo",
				"value": "bar",
			}},
			oldObj: []interface{}{map[string]interface{}{
				"key":   "foo",
				"value": "baz",
			}},
			schema: withRule(arrayType("map", []string{"key"}, objectTypePtr(map[string]schema.Structural{
				"key":   stringType,
				"value": stringType,
			})), `
				!oldSelf.hasValue() || self[0].value == "bar"
			`),
		},
	}

	for _, tt := range tests {
		tp := true
		for i := range tt.schema.XValidations {
			tt.schema.XValidations[i].OptionalOldSelf = &tp
		}

		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			celValidator := validator(&tt.schema, &tt.schema, false, model.SchemaDeclType(&tt.schema, false), celconfig.PerCallLimit)
			if celValidator == nil {
				t.Fatal("expected non nil validator")
			}

			t.Run("null old", func(t *testing.T) {
				errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &tt.schema, tt.obj, nil, math.MaxInt)
				if len(errs) != 0 {
					t.Errorf("expected no errors, but got: %v", errs)
				}
			})

			t.Run("non-null old", func(t *testing.T) {
				errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &tt.schema, tt.obj, tt.oldObj, math.MaxInt)
				if len(errs) != 0 {
					t.Errorf("expected no errors, but got: %v", errs)
				}
			})
		})
	}
}

// 表明不能把 oldSelf 当作已解包的值直接使用
func TestOptionalOldSelfIsOptionalType(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, apiextensionsfeatures.CRDValidationRatcheting, true)

	cases := []struct {
		name   string
		schema schema.Structural
		obj    interface{}
		errors []string
	}{
		{
			name: "forbid direct usage of optional integer",
			schema: withRule(integerType, `
				oldSelf + self > 5
			`),
			obj:    5,
			errors: []string{"no matching overload for '_+_' applied to '(optional_type(int), int)"},
		},
		{
			name: "forbid direct usage of optional string",
			schema: withRule(stringType, `
				oldSelf == "foo"
			`),
			obj:    "bar",
			errors: []string{"no matching overload for '_==_' applied to '(optional_type(string), string)"},
		},
		{
			name: "forbid direct usage of optional array",
			schema: withRule(arrayType("", nil, &stringSchema), `
				oldSelf.all(x, x == x)
			`),
			obj:    []interface{}{"bar"},
			errors: []string{"expression of type 'optional_type(list(string))' cannot be range of a comprehension"},
		},
		{
			name: "forbid direct usage of optional array element",
			schema: withRule(arrayType("", nil, &stringSchema), `
				oldSelf[0] == "foo"
			`),
			obj:    []interface{}{"bar"},
			errors: []string{"found no matching overload for '_==_' applied to '(optional_type(string), string)"},
		},
		{
			name: "forbid direct usage of optional struct",
			schema: withRule(arrayType("map", []string{"key"}, objectTypePtr(map[string]schema.Structural{
				"key":   stringType,
				"value": stringType,
			})), `oldSelf.key == "foo"`),
			obj: []interface{}{map[string]interface{}{
				"key":   "bar",
				"value": "baz",
			}},
			errors: []string{"does not support field selection"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()

			for i := range tt.schema.XValidations {
				tt.schema.XValidations[i].OptionalOldSelf = ptr.To(true)
			}

			celValidator := validator(&tt.schema, &tt.schema, false, model.SchemaDeclType(&tt.schema, false), celconfig.PerCallLimit)
			if celValidator == nil {
				t.Fatal("expected non nil validator")
			}
			errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &tt.schema, tt.obj, tt.obj, math.MaxInt)
			unmatched := map[string]struct{}{}
			for _, e := range tt.errors {
				unmatched[e] = struct{}{}
			}
			for _, err := range errs {
				if err.Type != field.ErrorTypeInvalid {
					t.Errorf("expected only ErrorTypeInvalid errors, but got: %v", err)
					continue
				}
				matched := false
				for expected := range unmatched {
					if strings.Contains(err.Error(), expected) {
						delete(unmatched, expected)
						matched = true
						break
					}
				}
				if !matched {
					t.Errorf("expected error to contain one of %v, but got: %v", unmatched, err)
				}
			}

			if len(unmatched) > 0 {
				t.Errorf("expected errors %v", unmatched)
			}
		})
	}
}

func genString(n int, c rune) string {
	b := strings.Builder{}
	for i := 0; i < n; i++ {
		_, err := b.WriteRune(c)
		if err != nil {
			panic(err)
		}
	}
	return b.String()
}

func setDefaultVerbosity(v int) {
	f := flag.CommandLine.Lookup("v")
	_ = f.Value.Set(fmt.Sprintf("%d", v))
}

// BenchmarkCELValidationWithContext 测量带上下文的 CEL 校验开销。
func BenchmarkCELValidationWithContext(b *testing.B) {
	items := make([]interface{}, 1000)
	for i := int64(0); i < 1000; i++ {
		items[i] = i
	}
	tests := []struct {
		name   string
		schema *schema.Structural
		obj    map[string]interface{}
		rule   string
	}{
		{name: "benchmark for cel validation with context",
			obj: map[string]interface{}{
				"array": items,
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"array": listType(&integerType),
			}),
			rule: "self.array.map(e, e * 20).filter(e, e > 50).exists(e, e == 60)",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ctx := context.TODO()
			s := withRule(*tt.schema, tt.rule)
			celValidator := NewValidator(&s, true, celconfig.PerCallLimit)
			if celValidator == nil {
				b.Fatal("expected non nil validator")
			}
			for i := 0; i < b.N; i++ {
				errs, _ := celValidator.Validate(ctx, field.NewPath("root"), &s, tt.obj, nil, celconfig.RuntimeCELCostBudget)
				for _, err := range errs {
					b.Fatalf("validation failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkCELValidationWithCancelledContext 测量上下文已取消时 CEL 校验的开销。
func BenchmarkCELValidationWithCancelledContext(b *testing.B) {
	items := make([]interface{}, 1000)
	for i := int64(0); i < 1000; i++ {
		items[i] = i
	}
	tests := []struct {
		name   string
		schema *schema.Structural
		obj    map[string]interface{}
		rule   string
	}{
		{name: "benchmark for cel validation with context",
			obj: map[string]interface{}{
				"array": items,
			},
			schema: objectTypePtr(map[string]schema.Structural{
				"array": listType(&integerType),
			}),
			rule: "self.array.map(e, e * 20).filter(e, e > 50).exists(e, e == 60)",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ctx := context.TODO()
			s := withRule(*tt.schema, tt.rule)
			celValidator := NewValidator(&s, true, celconfig.PerCallLimit)
			if celValidator == nil {
				b.Fatal("expected non nil validator")
			}
			for i := 0; i < b.N; i++ {
				evalCtx, cancel := context.WithTimeout(ctx, time.Microsecond)
				cancel()
				errs, _ := celValidator.Validate(evalCtx, field.NewPath("root"), &s, tt.obj, nil, celconfig.RuntimeCELCostBudget)
				//found := false
				//for _, err := range errs {
				//	if err.Type == field.ErrorTypeInvalid && strings.Contains(err.Error(), "operation interrupted") {
				//		found = true
				//		break
				//	}
				//}
				if len(errs) == 0 {
					b.Errorf("expect operation interrupted err but did not find")
				}
			}
		})
	}
}

// BenchmarkCELValidationWithAndWithoutOldSelfReference 衡量求值
// 引用 "oldSelf" 的校验规则带来的额外开销。
func BenchmarkCELValidationWithAndWithoutOldSelfReference(b *testing.B) {
	for _, rule := range []string{
		"self.getMonth() >= 0",
		"oldSelf.getMonth() >= 0",
	} {
		b.Run(rule, func(b *testing.B) {
			obj := map[string]interface{}{
				"datetime": time.Time{}.Format(strfmt.ISO8601LocalTime),
			}
			s := &schema.Structural{
				Generic: schema.Generic{
					Type: "object",
				},
				Properties: map[string]schema.Structural{
					"datetime": {
						Generic: schema.Generic{
							Type: "string",
						},
						ValueValidation: &schema.ValueValidation{
							Format: "date-time",
						},
						ValidationExtensions: schema.ValidationExtensions{
							XValidations: []apiextensions.ValidationRule{
								{Rule: rule},
							},
						},
					},
				},
			}
			validator := NewValidator(s, true, celconfig.PerCallLimit)
			if validator == nil {
				b.Fatal("expected non nil validator")
			}

			ctx := context.TODO()
			root := field.NewPath("root")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				errs, _ := validator.Validate(ctx, root, s, obj, obj, celconfig.RuntimeCELCostBudget)
				for _, err := range errs {
					b.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func primitiveType(typ, format string) schema.Structural {
	result := schema.Structural{
		Generic: schema.Generic{
			Type: typ,
		},
	}
	if len(format) != 0 {
		result.ValueValidation = &schema.ValueValidation{
			Format: format,
		}
	}
	return result
}

var (
	integerType = primitiveType("integer", "")
	int32Type   = primitiveType("integer", "int32")
	int64Type   = primitiveType("integer", "int64")
	numberType  = primitiveType("number", "")
	floatType   = primitiveType("number", "float")
	doubleType  = primitiveType("number", "double")
	stringType  = primitiveType("string", "")
	byteType    = primitiveType("string", "byte")
	booleanType = primitiveType("boolean", "")

	durationFormat = primitiveType("string", "duration")
	dateFormat     = primitiveType("string", "date")
	dateTimeFormat = primitiveType("string", "date-time")
)

func listType(items *schema.Structural) schema.Structural {
	return arrayType("atomic", nil, items)
}

func listTypePtr(items *schema.Structural) *schema.Structural {
	l := listType(items)
	return &l
}

func listSetType(items *schema.Structural) schema.Structural {
	return arrayType("set", nil, items)
}

func listMapType(keys []string, items *schema.Structural) schema.Structural {
	return arrayType("map", keys, items)
}

func listMapTypePtr(keys []string, items *schema.Structural) *schema.Structural {
	l := listMapType(keys, items)
	return &l
}

func arrayType(listType string, keys []string, items *schema.Structural) schema.Structural {
	result := schema.Structural{
		Generic: schema.Generic{
			Type: "array",
		},
		Extensions: schema.Extensions{
			XListType: &listType,
		},
		Items: items,
	}
	if len(keys) > 0 && listType == "map" {
		result.Extensions.XListMapKeys = keys
	}
	return result
}

// ValsEqualThemselvesAndDataLiteral 返回断言两个字段彼此相等且等于给定数据字面量的 CEL 表达式。
func ValsEqualThemselvesAndDataLiteral(val1, val2 string, dataLiteral string) string {
	return fmt.Sprintf("%s == %s && %s == %s && %s == %s", val1, dataLiteral, dataLiteral, val1, val1, val2)
}

func objs(val ...interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(val))
	for i, v := range val {
		result[fmt.Sprintf("val%d", i+1)] = v
	}
	return result
}

func schemas(valSchema ...schema.Structural) *schema.Structural {
	result := make(map[string]schema.Structural, len(valSchema))
	for i, v := range valSchema {
		result[fmt.Sprintf("val%d", i+1)] = v
	}
	return objectTypePtr(result)
}

func objectType(props map[string]schema.Structural) schema.Structural {
	return schema.Structural{
		Generic: schema.Generic{
			Type: "object",
		},
		Properties: props,
	}
}

func objectTypePtr(props map[string]schema.Structural) *schema.Structural {
	o := objectType(props)
	return &o
}

func mapType(valSchema *schema.Structural) schema.Structural {
	result := schema.Structural{
		Generic: schema.Generic{
			Type: "object",
		},
		AdditionalProperties: &schema.StructuralOrBool{Bool: true, Structural: valSchema},
	}
	return result
}

func mapTypePtr(valSchema *schema.Structural) *schema.Structural {
	m := mapType(valSchema)
	return &m
}

func intOrStringType() schema.Structural {
	return schema.Structural{
		Extensions: schema.Extensions{
			XIntOrString: true,
		},
	}
}

func withRule(s schema.Structural, rule string) schema.Structural {
	s.XValidations = apiextensions.ValidationRules{
		{
			Rule: rule,
		},
	}
	return s
}

func withRuleMessageAndMessageExpression(s schema.Structural, rule, message, messageExpression string) schema.Structural {
	s.XValidations = apiextensions.ValidationRules{
		{
			Rule:              rule,
			Message:           message,
			MessageExpression: messageExpression,
		},
	}
	return s
}

func withReasonAndFldPath(s schema.Structural, rule, jsonPath string, reason *apiextensions.FieldValueErrorReason) schema.Structural {
	s.XValidations = apiextensions.ValidationRules{
		{
			Rule:      rule,
			FieldPath: jsonPath,
			Reason:    reason,
		},
	}
	return s
}

func withRuleAndMessageExpression(s schema.Structural, rule, messageExpression string) schema.Structural {
	s.XValidations = apiextensions.ValidationRules{
		{
			Rule:              rule,
			MessageExpression: messageExpression,
		},
	}
	return s
}

func withRulePtr(s *schema.Structural, rule string) *schema.Structural {
	s.XValidations = apiextensions.ValidationRules{
		{
			Rule: rule,
		},
	}
	return s
}

func cloneWithRule(s *schema.Structural, rule string) *schema.Structural {
	s = s.DeepCopy()
	return withRulePtr(s, rule)
}

func withMaxLength(s schema.Structural, maxLength *int64) schema.Structural {
	if s.ValueValidation == nil {
		s.ValueValidation = &schema.ValueValidation{}
	}
	s.ValueValidation.MaxLength = maxLength
	return s
}

func withMaxItems(s schema.Structural, maxItems *int64) schema.Structural {
	if s.ValueValidation == nil {
		s.ValueValidation = &schema.ValueValidation{}
	}
	s.ValueValidation.MaxItems = maxItems
	return s
}

func withMaxProperties(s schema.Structural, maxProperties *int64) schema.Structural {
	if s.ValueValidation == nil {
		s.ValueValidation = &schema.ValueValidation{}
	}
	s.ValueValidation.MaxProperties = maxProperties
	return s
}

func withDefault(dflt interface{}, s schema.Structural) schema.Structural {
	s.Generic.Default = schema.JSON{Object: dflt}
	return s
}

func withNullable(nullable bool, s schema.Structural) schema.Structural {
	s.Generic.Nullable = nullable
	return s
}

func withNullablePtr(nullable bool, s schema.Structural) *schema.Structural {
	s.Generic.Nullable = nullable
	return &s
}

func nilInterfaceOfStringSlice() []interface{} {
	var slice []interface{} = nil
	return slice
}
