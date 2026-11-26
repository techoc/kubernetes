/*
Copyright 2015 The Kubernetes Authors.

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

// 这些测试放在独立包中，以打破测试里的循环依赖：Unstructured 依赖转换器包，
// 而测试又要验证转换器如何处理 Unstructured，因此必须同时导入两者。

package runtime_test

import (
	encodingjson "encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/randfill"
)

// simpleEquality 把 time.Time 归一化到 UTC 后比较，避免时区差异导致误报。
var simpleEquality = conversion.EqualitiesOrDie(
	func(a, b time.Time) bool {
		return a.UTC() == b.UTC()
	},
)

// 以下测试类型刻意覆盖不同形态：基本类型、嵌套结构体、内嵌结构体、
// interface{} 切片、[]byte、自定义 MarshalJSON 等。
type A struct {
	A int    `json:"aa,omitempty"`
	B string `json:"ab,omitempty"`
	C bool   `json:"ac,omitempty"`
	D uint   `json:"ad,omitempty"`
}

type B struct {
	A A                 `json:"ba"`
	B string            `json:"bb"`
	C map[string]string `json:"bc"`
	D []string          `json:"bd"`
}

// C 内嵌 B，覆盖匿名内联字段的展开。
type C struct {
	A []A `json:"ca"`
	B `json:""`
	C string         `json:"cc"`
	D *int64         `json:"cd"`
	E map[string]int `json:"ce"`
	F []bool         `json:"cf"`
	G []int          `json:"cg"`
	H float32        `json:"ch"`
	I []interface{}  `json:"ci"`
}

// D 覆盖 interface{} 切片的各种内容。
type D struct {
	A []interface{} `json:"da"`
}

// E 覆盖顶层 interface{} 字段。
type E struct {
	A interface{} `json:"ea"`
}

// F 覆盖各类切片的合法/非法输入。
type F struct {
	A string            `json:"fa"`
	B map[string]string `json:"fb"`
	C []A               `json:"fc"`
	D int               `json:"fd"`
	E float32           `json:"fe"`
	F []string          `json:"ff"`
	G []int             `json:"fg"`
	H []bool            `json:"fh"`
	I []float32         `json:"fi"`
	J []byte            `json:"fj"`
}

// G 同时覆盖值接收者与指针接收者的自定义 JSON marshaller。
type G struct {
	CustomValue1   CustomValue    `json:"customValue1"`
	CustomValue2   *CustomValue   `json:"customValue2"`
	CustomPointer1 CustomPointer  `json:"customPointer1"`
	CustomPointer2 *CustomPointer `json:"customPointer2"`
}

// H 覆盖多层内嵌。
type H struct {
	A A `json:"ha"`
	C `json:""`
}

// I 覆盖三层内嵌 + 深层未知字段检测。
type I struct {
	A A `json:"ia"`
	H `json:""`

	UL1 UnknownLevel1 `json:"ul1"`
}

// UnknownLevel1/2/3 与 InlinedAA/AAA/BB/BBB/CC/CCC 构造多层内联结构，
// 用于验证未知字段路径能穿透任意深度的内联。
type UnknownLevel1 struct {
	A          int64 `json:"a"`
	InlinedAA  `json:""`
	InlinedAAA `json:""`
}
type InlinedAA struct {
	AA int64 `json:"aa"`
}
type InlinedAAA struct {
	AAA   int64         `json:"aaa"`
	Child UnknownLevel2 `json:"child"`
}

type UnknownLevel2 struct {
	B          int64 `json:"b"`
	InlinedBB  `json:""`
	InlinedBBB `json:""`
}
type InlinedBB struct {
	BB int64 `json:"bb"`
}
type InlinedBBB struct {
	BBB   int64         `json:"bbb"`
	Child UnknownLevel3 `json:"child"`
}

type UnknownLevel3 struct {
	C          int64 `json:"c"`
	InlinedCC  `json:""`
	InlinedCCC `json:""`
}
type InlinedCC struct {
	CC int64 `json:"cc"`
}
type InlinedCCC struct {
	CCC int64 `json:"ccc"`
}

// CustomValue 以值接收者实现 MarshalJSON；CustomPointer 以指针接收者实现，
// 两者在 ToUnstructured 中必须都被识别为自定义转换。
type CustomValue struct {
	data []byte
}

// MarshalJSON 在该类型上使用值接收者。
func (c CustomValue) MarshalJSON() ([]byte, error) {
	return c.data, nil
}

type CustomPointer struct {
	data []byte
}

// MarshalJSON 在该类型上使用指针接收者。
func (c *CustomPointer) MarshalJSON() ([]byte, error) {
	return c.data, nil
}

// doRoundTrip 依次执行：JSON 往返 -> ToUnstructured -> DeepCopy -> FromUnstructured，
// 每一步之后对象都必须与原始值相等。这是转换器最核心的正确性检查。
func doRoundTrip(t *testing.T, item interface{}) {
	data, err := json.Marshal(item)
	if err != nil {
		t.Errorf("Error when marshaling object: %v", err)
		return
	}

	unstr := make(map[string]interface{})
	err = json.Unmarshal(data, &unstr)
	if err != nil {
		t.Errorf("Error when unmarshaling to unstructured: %v", err)
		return
	}

	data, err = json.Marshal(unstr)
	if err != nil {
		t.Errorf("Error when marshaling unstructured: %v", err)
		return
	}
	unmarshalledObj := reflect.New(reflect.TypeOf(item).Elem()).Interface()
	err = json.Unmarshal(data, unmarshalledObj)
	if err != nil {
		t.Errorf("Error when unmarshaling to object: %v", err)
		return
	}
	if !reflect.DeepEqual(item, unmarshalledObj) {
		t.Errorf("Object changed during JSON operations, diff: %v", cmp.Diff(item, unmarshalledObj))
		return
	}

	// TODO: 本应使用不匹配检测，但会因另一个错误而失败。
	newUnstr, err := runtime.DefaultUnstructuredConverter.ToUnstructured(item)
	if err != nil {
		t.Errorf("ToUnstructured failed: %v", err)
		return
	}

	// 先深拷贝再反序列化，确保 FromUnstructured 不会意外复用/修改原 map。
	copiedNewUnstr := runtime.DeepCopyJSONValue(newUnstr)
	if value, ok := copiedNewUnstr.(map[string]interface{}); ok {
		newUnstr = value
	} else {
		t.Errorf("DeepCopyJSONValue return unexpected type %T", copiedNewUnstr)
		return
	}

	newObj := reflect.New(reflect.TypeOf(item).Elem()).Interface()
	err = runtime.NewTestUnstructuredConverter(simpleEquality).FromUnstructured(newUnstr, newObj)
	if err != nil {
		t.Errorf("FromUnstructured failed: %v", err)
		return
	}

	if !reflect.DeepEqual(item, newObj) {
		t.Errorf("Object changed, diff: %v", cmp.Diff(item, newObj))
	}
}

// TestRoundTrip 覆盖 nil/空/非空容器、内嵌结构体、interface{} 切片、
// uint 以及 Unstructured 自身的往返。
func TestRoundTrip(t *testing.T) {
	intVal := int64(42)
	testCases := []struct {
		obj interface{}
	}{
		{
			obj: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind": "List",
				},
				// 不测试 Items 为 nil 的列表：items 是必填字段，总会被序列化成
				// 空数组，反序列化后不等于 nil，必然失败，这是预期行为。
				Items: []unstructured.Unstructured{},
			},
		},
		{
			obj: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind": "List",
				},
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"kind": "Pod",
						},
					},
				},
			},
		},
		{
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
				},
			},
		},
		{
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name": "foo1",
					},
				},
			},
		},
		{
			// 该用例（及其它用例）覆盖 nil map、slice 和指针。
			obj: &C{
				C: "ccc",
			},
		},
		{
			// 该用例（及其它用例）覆盖空 map 和 slice。
			obj: &C{
				A: []A{},
				C: "ccc",
				E: map[string]int{},
				I: []interface{}{},
			},
		},
		{
			obj: &C{
				A: []A{
					{
						A: 1,
						B: "11",
						C: true,
					},
					{
						A: 2,
						B: "22",
						C: false,
					},
				},
				B: B{
					A: A{
						A: 3,
						B: "33",
					},
					B: "bbb",
					C: map[string]string{
						"k1": "v1",
						"k2": "v2",
					},
					D: []string{"s1", "s2"},
				},
				C: "ccc",
				D: &intVal,
				E: map[string]int{
					"k1": 1,
					"k2": 2,
				},
				F: []bool{true, false, false},
				G: []int{1, 2, 5},
				H: 3.3,
				I: []interface{}{nil, nil, nil},
			},
		},
		{
			// 测试包含空切片的 interface{} 切片。
			obj: &D{
				A: []interface{}{[]interface{}{}, []interface{}{}},
			},
		},
		{
			// 测试包含不同类型值的 interface{} 切片。
			obj: &D{
				A: []interface{}{float64(3.5), int64(4), "3.0", nil},
			},
		},
		{
			// 测试 uint 值。
			obj: &A{
				D: 1,
			},
		},
	}

	for i := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			doRoundTrip(t, testCases[i].obj)
		})
	}
}

// TestUnknownFields 检查来自未知字段各种可能位置（如结构体字段、内联结构体、切片等）
// 收集到的未知字段错误集合。
//
// 未知字段错误必须带完整路径（含数组下标与内联展开），且各层级互不串扰。
func TestUnknownFields(t *testing.T) {
	// simples 检查字段、子字段与切片中的基本未知字段能否被发现，
	// 覆盖结构体字段、切片元素中的未知字段场景。
	var simplesData = `{
"ca": [
	{
		"aa": 1,
		"ab": "ab",
		"ac": true,
		"unknown1": 24
	}
],
"cc": "ccstring",
"unknown2": "foo"
}`

	var simplesErrs = []string{
		`unknown field "ca[0].unknown1"`,
		`unknown field "unknown2"`,
	}

	// 同名不同层：验证高层级已匹配的字段不会残留在低层级的未识别字段中，反之亦然，
	// 即匹配集合按层级隔离。本例中 "cc" 存在于根层但不存在于嵌套字段 ul1 中，
	// 若匹配集合被错误保留，ul1 内的未知 "cc" 就不会被报告；"aaa" 则相反，
	// 它存在于嵌套层级而不在根层。
	var sameNameDiffLevelData = `
	{
		"cc": "foo",
		"aaa": 1,
		"ul1": {
			"aa": 1,
			"aaa": 1,
			"cc": 1

		}
}`
	var sameNameDiffLevelErrs = []string{
		`unknown field "aaa"`,
		`unknown field "ul1.cc"`,
	}

	// inlined-inlined 确认能识别双层嵌套内联的字段、并拒绝未识别的键：
	// 已知键应被识别，未知键仍要报错。
	var inlinedInlinedData = `{
		"bb": "foo",
		"bc": {
			"foo": "bar"
		},
		"bd": ["d1", "d2"],
		"aa": 1
}`

	var inlinedInlinedErrs = []string{
		`unknown field "aa"`,
	}

	// combined 综合场景：多层内联 + 切片 + 根层未知字段同时出现。
	var combinedData = `
	{
		"ia": {
			"aa": 1,
			"ab": "ab",
			"unknownI": "foo"
		},
		"ha": {
			"aa": 2,
			"ab": "ab2",
			"unknownH": "foo"
		},
		"ca":[
			{
				"aa":1,
				"ab":"11",
				"ac":true
			},
			{
				"aa":2,
				"ab":"22",
				"unknown1": "foo"
			},
			{
				"aa":3,
				"ab":"33",
				"unknown2": "foo"
			}
		],
		"ba":{
			"aa":3,
			"ab":"33",
			"ac": true,
			"unknown3": 26,
			"unknown4": "foo"
		},
		"unknown5": "foo",
		"bb":"bbb",
		"bc":{
			"k1":"v1",
			"k2":"v2"
		},
		"bd":[
			"s1",
			"s2"
		],
		"cc":"ccc",
		"cd":42,
		"ce":{
			"k1":1,
			"k2":2
		},
		"cf":[
			true,
			false,
			false
		],
		"cg":
		[
			1,
			2,
			5
		],
		"ch":3.3,
		"ci":[
			null,
			null,
			null
		],
		"ul1": {
			"a": 1,
			"aa": 1,
			"aaa": 1,
			"b": 1,
			"bb": 1,
			"bbb": 1,
			"c": 1,
			"cc": 1,
			"ccc": 1,
			"child": {
				"a": 1,
				"aa": 1,
				"aaa": 1,
				"b": 1,
				"bb": 1,
				"bbb": 1,
				"c": 1,
				"cc": 1,
				"ccc": 1,
				"child": {
					"a": 1,
					"aa": 1,
					"aaa": 1,
					"b": 1,
					"bb": 1,
					"bbb": 1,
					"c": 1,
					"cc": 1,
					"ccc": 1
				}
			}
		}
}`

	var combinedErrs = []string{
		`unknown field "ca[1].unknown1"`,
		`unknown field "ca[2].unknown2"`,
		`unknown field "ba.unknown3"`,
		`unknown field "ba.unknown4"`,
		`unknown field "unknown5"`,
		`unknown field "ha.unknownH"`,
		`unknown field "ia.unknownI"`,

		`unknown field "ul1.b"`,
		`unknown field "ul1.bb"`,
		`unknown field "ul1.bbb"`,
		`unknown field "ul1.c"`,
		`unknown field "ul1.cc"`,
		`unknown field "ul1.ccc"`,

		`unknown field "ul1.child.a"`,
		`unknown field "ul1.child.aa"`,
		`unknown field "ul1.child.aaa"`,
		`unknown field "ul1.child.c"`,
		`unknown field "ul1.child.cc"`,
		`unknown field "ul1.child.ccc"`,

		`unknown field "ul1.child.child.a"`,
		`unknown field "ul1.child.child.aa"`,
		`unknown field "ul1.child.child.aaa"`,
		`unknown field "ul1.child.child.b"`,
		`unknown field "ul1.child.child.bb"`,
		`unknown field "ul1.child.child.bbb"`,
	}

	testCases := []struct {
		jsonData            string
		obj                 interface{}
		returnUnknownFields bool
		expectedErrs        []string
	}{
		{
			jsonData:            simplesData,
			obj:                 &C{},
			returnUnknownFields: true,
			expectedErrs:        simplesErrs,
		},
		{
			jsonData:            simplesData,
			obj:                 &C{},
			returnUnknownFields: false,
		},
		{
			jsonData:            sameNameDiffLevelData,
			obj:                 &I{},
			returnUnknownFields: true,
			expectedErrs:        sameNameDiffLevelErrs,
		},
		{
			jsonData:            sameNameDiffLevelData,
			obj:                 &I{},
			returnUnknownFields: false,
		},
		{
			jsonData:            inlinedInlinedData,
			obj:                 &I{},
			returnUnknownFields: true,
			expectedErrs:        inlinedInlinedErrs,
		},
		{
			jsonData:            inlinedInlinedData,
			obj:                 &I{},
			returnUnknownFields: false,
		},
		{
			jsonData:            combinedData,
			obj:                 &I{},
			returnUnknownFields: true,
			expectedErrs:        combinedErrs,
		},
		{
			jsonData:            combinedData,
			obj:                 &I{},
			returnUnknownFields: false,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			unstr := make(map[string]interface{})
			err := json.Unmarshal([]byte(tc.jsonData), &unstr)
			if err != nil {
				t.Errorf("Error when unmarshaling to unstructured: %v", err)
				return
			}
			err = runtime.NewTestUnstructuredConverterWithValidation(simpleEquality).FromUnstructuredWithValidation(unstr, tc.obj, tc.returnUnknownFields)
			if len(tc.expectedErrs) == 0 && err != nil {
				t.Errorf("unexpected err: %v", err)
			}
			var errString string
			if err != nil {
				errString = err.Error()
			}
			// 逐条消解期望错误：先确认都存在，再把已匹配的文本从错误串中移除；
			// 最后剩余的文本必须是空，才能证明没有多报错误。
			missedErrs := []string{}
			failed := false
			for _, expected := range tc.expectedErrs {
				if !strings.Contains(errString, expected) {
					failed = true
					missedErrs = append(missedErrs, expected)
				} else {
					errString = strings.Replace(errString, expected, "", 1)
				}
			}
			if failed {
				for _, e := range missedErrs {
					t.Errorf("missing err: %v\n", e)
				}
			}
			leftoverErrors := strings.TrimSpace(strings.TrimPrefix(strings.ReplaceAll(errString, ",", ""), "strict decoding error:"))
			if leftoverErrors != "" {
				t.Errorf("found unexpected errors: %s", leftoverErrors)
			}
		})
	}
}

// BenchmarkFromUnstructuredWithValidation 对使用各种校验指令
// （Ignore、Warn、Strict）执行 FromUnstructured 所需的耗时与内存做基准测试。
//
// 对比开启/关闭未知字段检测的分配与耗时，防止检测逻辑造成性能回退。
func BenchmarkFromUnstructuredWithValidation(b *testing.B) {
	re := regexp.MustCompile("^I$")
	f := randfill.NewWithSeed(1).NilChance(0.1).SkipFieldsWithPattern(re)
	iObj := &I{}
	f.Fill(&iObj)

	unstr, err := runtime.DefaultUnstructuredConverter.ToUnstructured(iObj)
	if err != nil {
		b.Fatalf("ToUnstructured failed: %v", err)
		return
	}
	for _, shouldReturn := range []bool{false, true} {
		b.Run(fmt.Sprintf("shouldReturn=%t", shouldReturn), func(b *testing.B) {
			newObj := reflect.New(reflect.TypeOf(iObj).Elem()).Interface()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err = runtime.NewTestUnstructuredConverterWithValidation(simpleEquality).FromUnstructuredWithValidation(unstr, newObj, shouldReturn); err != nil {
					b.Fatalf("FromUnstructured failed: %v", err)
					return
				}
			}
		})
	}
}

// 验证「序列化 JSON -> 对象」与「JSON -> map[string]interface{} -> 对象」
// 两条路径产生相同的对象；转换器与标准库 JSON 对同一输入产生相同对象
// （含错误行为一致）。
func doUnrecognized(t *testing.T, jsonData string, item interface{}, expectedErr error) {
	unmarshalledObj := reflect.New(reflect.TypeOf(item).Elem()).Interface()
	err := json.Unmarshal([]byte(jsonData), unmarshalledObj)
	if (err != nil) != (expectedErr != nil) {
		t.Errorf("Unexpected error when unmarshaling to object: %v, expected: %v", err, expectedErr)
		return
	}

	unstr := make(map[string]interface{})
	err = json.Unmarshal([]byte(jsonData), &unstr)
	if err != nil {
		t.Errorf("Error when unmarshaling to unstructured: %v", err)
		return
	}
	newObj := reflect.New(reflect.TypeOf(item).Elem()).Interface()
	err = runtime.NewTestUnstructuredConverter(simpleEquality).FromUnstructured(unstr, newObj)
	if (err != nil) != (expectedErr != nil) {
		t.Errorf("Unexpected error in FromUnstructured: %v, expected: %v", err, expectedErr)
	}

	if expectedErr == nil && !reflect.DeepEqual(unmarshalledObj, newObj) {
		t.Errorf("Object changed, diff: %v", cmp.Diff(unmarshalledObj, newObj))
	}
}

// TestUnrecognized 表驱动地验证各类“类型不匹配”输入：合法转换必须与
// 标准库结果一致，非法转换必须产生与标准库同类的错误。
func TestUnrecognized(t *testing.T) {
	testCases := []struct {
		data string
		obj  interface{}
		err  error
	}{
		{
			data: "{\"da\":[3.5,4,\"3.0\",null]}",
			obj:  &D{},
		},
		{
			data: "{\"ea\":[3.5,4,\"3.0\",null]}",
			obj:  &E{},
		},
		{
			data: "{\"ea\":[null,null,null]}",
			obj:  &E{},
		},
		{
			data: "{\"ea\":[[],[null]]}",
			obj:  &E{},
		},
		{
			data: "{\"ea\":{\"a\":[],\"b\":null}}",
			obj:  &E{},
		},
		{
			data: "{\"fa\":\"fa\",\"fb\":{\"a\":\"a\"}}",
			obj:  &F{},
		},
		{
			data: "{\"fa\":\"fa\",\"fb\":{\"a\":null}}",
			obj:  &F{},
		},
		{
			data: "{\"fc\":[null]}",
			obj:  &F{},
		},
		{
			data: "{\"fc\":[{\"aa\":123,\"ab\":\"bbb\"}]}",
			obj:  &F{},
		},
		{
			// 仅包含未知字段
			data: "{\"fx\":[{\"aa\":123,\"ab\":\"bbb\"}],\"fz\":123}",
			obj:  &F{},
		},
		{
			data: "{\"fc\":[{\"aa\":\"aaa\",\"ab\":\"bbb\"}]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal string into Go value of type int"),
		},
		{
			data: "{\"fd\":123,\"fe\":3.5}",
			obj:  &F{},
		},
		{
			data: "{\"ff\":[\"abc\"],\"fg\":[123],\"fh\":[true,false]}",
			obj:  &F{},
		},
		{
			data: "{\"fj\":\"\"}",
			obj:  &F{},
		},
		{
			// 非法字符串数据
			data: "{\"fa\":123}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type string"),
		},
		{
			// 非法字符串数据
			data: "{\"fa\":13.5}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type string"),
		},
		{
			// 非法字符串数据
			data: "{\"fa\":true}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal bool into Go value of type string"),
		},
		{
			// 非法 []string 数据
			data: "{\"ff\":123}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type []string"),
		},
		{
			// 非法 []string 数据
			data: "{\"ff\":3.5}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type []string"),
		},
		{
			// 非法 []string 数据
			data: "{\"ff\":[123,345]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type string"),
		},
		{
			// 非法 []int 数据
			data: "{\"fg\":123}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type []int"),
		},
		{
			// 非法 []int 数据
			data: "{\"fg\":\"abc\"}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal string into Go value of type []int"),
		},
		{
			// 非法 []int 数据
			data: "{\"fg\":[\"abc\"]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal string into Go value of type int"),
		},
		{
			// 非法 []int 数据
			data: "{\"fg\":[3.5]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number 3.5 into Go value of type int"),
		},
		{
			// 非法 []int 数据
			data: "{\"fg\":[true,false]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number 3.5 into Go value of type int"),
		},
		{
			// 非法 []bool 数据
			data: "{\"fh\":123}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type []bool"),
		},
		{
			// 非法 []bool 数据
			data: "{\"fh\":\"abc\"}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal string into Go value of type []bool"),
		},
		{
			// 非法 []bool 数据
			data: "{\"fh\":[\"abc\"]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal string into Go value of type bool"),
		},
		{
			// 非法 []bool 数据
			data: "{\"fh\":[3.5]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type bool"),
		},
		{
			// 非法 []bool 数据
			data: "{\"fh\":[123]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type bool"),
		},
		{
			// 非法 []float 数据
			data: "{\"fi\":123}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal number into Go value of type []float32"),
		},
		{
			// 非法 []float 数据
			data: "{\"fi\":\"abc\"}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal string into Go value of type []float32"),
		},
		{
			// 非法 []float 数据
			data: "{\"fi\":[\"abc\"]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal string into Go value of type float32"),
		},
		{
			// 非法 []float 数据
			data: "{\"fi\":[true]}",
			obj:  &F{},
			err:  fmt.Errorf("json: cannot unmarshal bool into Go value of type float32"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.data, func(t *testing.T) {
			doUnrecognized(t, tc.data, tc.obj, tc.err)
		})
	}
}

// TestDeepCopyJSON 验证所有 JSON 原生类型（含 json.Number 与 nil）的深拷贝。
func TestDeepCopyJSON(t *testing.T) {
	src := map[string]interface{}{
		"a": nil,
		"b": int64(123),
		"c": map[string]interface{}{
			"a": "b",
		},
		"d": []interface{}{
			int64(1), int64(2),
		},
		"e": "estr",
		"f": true,
		"g": encodingjson.Number("123"),
	}
	deepCopy := runtime.DeepCopyJSON(src)
	assert.Equal(t, src, deepCopy)
}

// TestFloatIntConversion 验证整数形式的 float64（如 3.0）可无损转换为 int。
func TestFloatIntConversion(t *testing.T) {
	unstr := map[string]interface{}{"fd": float64(3)}

	var obj F
	if err := runtime.NewTestUnstructuredConverter(simpleEquality).FromUnstructured(unstr, &obj); err != nil {
		t.Errorf("Unexpected error in FromUnstructured: %v", err)
	}

	data, err := json.Marshal(unstr)
	if err != nil {
		t.Fatalf("Error when marshaling unstructured: %v", err)
	}
	var unmarshalled F
	if err := json.Unmarshal(data, &unmarshalled); err != nil {
		t.Fatalf("Error when unmarshaling to object: %v", err)
	}

	if !reflect.DeepEqual(obj, unmarshalled) {
		t.Errorf("Incorrect conversion, diff: %v", cmp.Diff(obj, unmarshalled))
	}
}

// TestIntFloatConversion 验证 int64 可无损转换为 float32 字段。
func TestIntFloatConversion(t *testing.T) {
	unstr := map[string]interface{}{"ch": int64(3)}

	var obj C
	if err := runtime.NewTestUnstructuredConverter(simpleEquality).FromUnstructured(unstr, &obj); err != nil {
		t.Errorf("Unexpected error in FromUnstructured: %v", err)
	}

	data, err := json.Marshal(unstr)
	if err != nil {
		t.Fatalf("Error when marshaling unstructured: %v", err)
	}
	var unmarshalled C
	if err := json.Unmarshal(data, &unmarshalled); err != nil {
		t.Fatalf("Error when unmarshaling to object: %v", err)
	}

	if !reflect.DeepEqual(obj, unmarshalled) {
		t.Errorf("Incorrect conversion, diff: %v", cmp.Diff(obj, unmarshalled))
	}
}

// TestCustomToUnstructured 验证值/指针接收者的 MarshalJSON 在所有字段形态
// （值、指针、可寻址/不可寻址）下都被正确调用。
func TestCustomToUnstructured(t *testing.T) {
	testcases := []struct {
		Data     string
		Expected interface{}
	}{
		{Data: `null`, Expected: nil},
		{Data: `true`, Expected: true},
		{Data: `false`, Expected: false},
		{Data: `[]`, Expected: []interface{}{}},
		{Data: `[1]`, Expected: []interface{}{int64(1)}},
		{Data: `{}`, Expected: map[string]interface{}{}},
		{Data: `{"a":1}`, Expected: map[string]interface{}{"a": int64(1)}},
		{Data: `0`, Expected: int64(0)},
		{Data: `0.0`, Expected: float64(0)},
	}

	for _, tc := range testcases {
		t.Run(tc.Data, func(t *testing.T) {
			t.Parallel()
			result, err := runtime.NewTestUnstructuredConverter(simpleEquality).ToUnstructured(&G{
				CustomValue1:   CustomValue{data: []byte(tc.Data)},
				CustomValue2:   &CustomValue{data: []byte(tc.Data)},
				CustomPointer1: CustomPointer{data: []byte(tc.Data)},
				CustomPointer2: &CustomPointer{data: []byte(tc.Data)},
			})
			require.NoError(t, err)
			for field, fieldResult := range result {
				assert.Equal(t, tc.Expected, fieldResult, field)
			}
		})
	}
}

// TestCustomToUnstructuredTopLevel 验证顶层对象直接使用自定义 MarshalJSON。
func TestCustomToUnstructuredTopLevel(t *testing.T) {
	// 顶层只支持对象。
	topLevelCases := []interface{}{
		&CustomValue{data: []byte(`{"a":1}`)},
		&CustomPointer{data: []byte(`{"a":1}`)},
	}
	expected := map[string]interface{}{"a": int64(1)}
	for i, obj := range topLevelCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			result, err := runtime.NewTestUnstructuredConverter(simpleEquality).ToUnstructured(obj)
			require.NoError(t, err)
			assert.Equal(t, expected, result)
		})
	}
}

// OmitemptyNameField 的 json tag 名恰好是 "omitempty"：它应被当作字段名，
// 而不是 omitempty 选项。
type OmitemptyNameField struct {
	I int `json:"omitempty"`
}

// TestOmitempty 验证 tag 中的 "omitempty" 位于名字位置时不会被误解析为选项。
func TestOmitempty(t *testing.T) {
	expected := `{"omitempty":0}`

	o := &OmitemptyNameField{}
	jsonData, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := expected, string(jsonData); e != a {
		t.Fatalf("expected\n%s\ngot\n%s", e, a)
	}

	unstr, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&o)
	if err != nil {
		t.Fatal(err)
	}
	jsonUnstrData, err := json.Marshal(unstr)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := expected, string(jsonUnstrData); e != a {
		t.Fatalf("expected\n%s\ngot\n%s", e, a)
	}
}

// InlineTestPrimitive 覆盖非结构体字段上的空名/内联/omitempty 组合。
type InlineTestPrimitive struct {
	NoNameTagPrimitive          int64 `json:""`
	NoNameTagInlinePrimitive    int64 `json:",inline"`
	NoNameTagOmitemptyPrimitive int64 `json:",omitempty"`
}

// InlineTestAnonymous 覆盖匿名内嵌字段在不同 tag 下的展开行为。
type InlineTestAnonymous struct {
	NoTag
	NoNameTag          `json:""`
	NameTag            `json:"nameTagEmbedded"`
	NoNameTagInline    `json:",inline"`
	NoNameTagEmbed     `json:",embed"` //nolint:staticcheck // SA5008 intentionally exercising a tag not present until Go 1.27
	NoNameTagOmitempty `json:",omitempty"`
}

// InlineTestNamed 与 InlineTestAnonymous 相同，但字段是具名的，验证具名与
// 匿名字段在 tag 语义上的差异。
type InlineTestNamed struct {
	NoTag              NoTag
	NoNameTag          NoNameTag          `json:""`
	NameTag            NameTag            `json:"nameTagEmbedded"`
	NoNameTagInline    NoNameTagInline    `json:",inline"`
	NoNameTagEmbed     NoNameTagEmbed     `json:",embed"` //nolint:staticcheck // intentionally exercising a tag not present until Go 1.27
	NoNameTagOmitempty NoNameTagOmitempty `json:",omitempty"`
}
type NoTag struct {
	Data0 int `json:"data0"`
}
type NameTag struct {
	Data1 int `json:"data1"`
}
type NoNameTag struct {
	Data2 int `json:"data2"`
}
type NoNameTagInline struct {
	Data3 int `json:"data3"`
}
type NoNameTagOmitempty struct {
	Data4 int `json:"data4"`
}
type NoNameTagEmbed struct {
	Data5 int `json:"data5"`
}

// TestInline 用标准库 encoding/json 的结果作为基准，验证 ToUnstructured
// 对所有内联 tag 组合的处理完全一致；同时捕获转换器可能出现的 panic。
func TestInline(t *testing.T) {
	testcases := []struct {
		name   string
		obj    any
		expect map[string]any
	}{
		{
			name: "primitive-zero",
			obj:  &InlineTestPrimitive{},
			expect: map[string]any{
				"NoNameTagPrimitive":       int64(0),
				"NoNameTagInlinePrimitive": int64(0),
			},
		},
		{
			name: "primitive-set",
			obj: &InlineTestPrimitive{
				NoNameTagPrimitive:          1,
				NoNameTagInlinePrimitive:    2,
				NoNameTagOmitemptyPrimitive: 3,
			},
			expect: map[string]any{
				"NoNameTagPrimitive":          int64(1),
				"NoNameTagInlinePrimitive":    int64(2),
				"NoNameTagOmitemptyPrimitive": int64(3),
			},
		},
		{
			name: "anonymous-zero",
			obj:  &InlineTestAnonymous{},
			expect: map[string]any{
				"data0":           int64(0),
				"data2":           int64(0),
				"data3":           int64(0),
				"data4":           int64(0),
				"data5":           int64(0),
				"nameTagEmbedded": map[string]any{"data1": int64(0)},
			},
		},
		{
			name: "anonymous-set",
			obj:  &InlineTestAnonymous{},
			expect: map[string]any{
				"data0":           int64(0),
				"data2":           int64(0),
				"data3":           int64(0),
				"data4":           int64(0),
				"data5":           int64(0),
				"nameTagEmbedded": map[string]any{"data1": int64(0)},
			},
		},
		{
			name: "named-zero",
			obj:  &InlineTestNamed{},
			expect: func() map[string]any {
				m := map[string]any{
					"NoTag":              map[string]any{"data0": int64(0)},
					"nameTagEmbedded":    map[string]any{"data1": int64(0)},
					"NoNameTag":          map[string]any{"data2": int64(0)},
					"NoNameTagInline":    map[string]any{"data3": int64(0)},
					"NoNameTagOmitempty": map[string]any{"data4": int64(0)},
				}
				if stdlibSupportsEmbedTag {
					m["data5"] = int64(0)
				} else {
					m["NoNameTagEmbed"] = map[string]any{"data5": int64(0)}
				}
				return m
			}(),
		},
		{
			name: "named-set",
			obj: &InlineTestNamed{
				NoTag:              NoTag{Data0: 10},
				NameTag:            NameTag{Data1: 11},
				NoNameTag:          NoNameTag{Data2: 12},
				NoNameTagInline:    NoNameTagInline{Data3: 13},
				NoNameTagOmitempty: NoNameTagOmitempty{Data4: 14},
				NoNameTagEmbed:     NoNameTagEmbed{Data5: 15},
			},
			expect: func() map[string]any {
				m := map[string]any{
					"NoTag":              map[string]any{"data0": int64(10)},
					"nameTagEmbedded":    map[string]any{"data1": int64(11)},
					"NoNameTag":          map[string]any{"data2": int64(12)},
					"NoNameTagInline":    map[string]any{"data3": int64(13)},
					"NoNameTagOmitempty": map[string]any{"data4": int64(14)},
				}
				if stdlibSupportsEmbedTag {
					m["data5"] = int64(15)
				} else {
					m["NoNameTagEmbed"] = map[string]any{"data5": int64(15)}
				}
				return m
			}(),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				// 捕获 panic。
				if err := recover(); err != nil {
					t.Fatal(err)
				}
			}()

			// 第一步：先确认期望值本身与标准库一致，避免测试基准错误。
			jsonData, err := json.Marshal(tc.obj)
			if err != nil {
				t.Fatal(err)
			}
			jsonUnstr := map[string]any{}
			if err := json.Unmarshal(jsonData, &jsonUnstr); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tc.expect, jsonUnstr) {
				t.Fatal(cmp.Diff(tc.expect, jsonUnstr))
			}

			// 用 DefaultUnstructuredConverter.ToUnstructured 核对预期结果。
			unstr, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tc.obj)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tc.expect, unstr) {
				t.Fatal(cmp.Diff(tc.expect, unstr))
			}
		})
	}
}
