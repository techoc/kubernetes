//go:build go1.8

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

package json

import (
	gojson "encoding/json"

	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// TestEvaluateTypes 验证本包 Unmarshal/Marshal 对 JSON 标量、整数/浮点数
// 边界、字符串、数组与 map 的类型推断与输出规范化：
// 整数在 int64 范围内保持整数，溢出后退化为 float64；浮点数按最短表示输出。
func TestEvaluateTypes(t *testing.T) {
	testCases := []struct {
		In   string
		Data interface{}
		Out  string
		Err  bool
	}{
		//无效语法
		{
			In:  `x`,
			Err: true,
		},
		{
			In:  ``,
			Err: true,
		},

		//空值
		{
			In:   `null`,
			Data: nil,
			Out:  `null`,
		},
		//布尔值
		{
			In:   `true`,
			Data: true,
			Out:  `true`,
		},
		{
			In:   `false`,
			Data: false,
			Out:  `false`,
		},

		//整数
		{
			In:   `0`,
			Data: int64(0),
			Out:  `0`,
		},
		{
			In:   `-0`,
			Data: int64(-0),
			Out:  `0`,
		},
		{
			In:   `1`,
			Data: int64(1),
			Out:  `1`,
		},
		{
			In:   `2147483647`,
			Data: int64(math.MaxInt32),
			Out:  `2147483647`,
		},
		{
			In:   `-2147483648`,
			Data: int64(math.MinInt32),
			Out:  `-2147483648`,
		},
		{
			In:   `9223372036854775807`,
			Data: int64(math.MaxInt64),
			Out:  `9223372036854775807`,
		},
		{
			In:   `-9223372036854775808`,
			Data: int64(math.MinInt64),
			Out:  `-9223372036854775808`,
		},

		//整数溢出
		{
			In:   `9223372036854775808`, //MaxInt64 + 1（溢出）
			Data: float64(9223372036854775808),
			Out:  `9223372036854776000`,
		},
		{
			In:   `-9223372036854775809`, //MinInt64 - 1（溢出）
			Data: float64(math.MinInt64),
			Out:  `-9223372036854776000`,
		},

		//浮点数
		{
			In:   `0.0`,
			Data: float64(0),
			Out:  `0`,
		},
		{
			In:   `-0.0`,
			Data: float64(-0.0), //nolint:staticcheck // SA4026：Go 中浮点字面量 '-0.0' 与 '0.0' 相同。
			Out:  `-0`,
		},
		{
			In:   `0.5`,
			Data: float64(0.5),
			Out:  `0.5`,
		},
		{
			In:   `1e3`,
			Data: float64(1e3),
			Out:  `1000`,
		},
		{
			In:   `1.5`,
			Data: float64(1.5),
			Out:  `1.5`,
		},
		{
			In:   `-0.3`,
			Data: float64(-.3),
			Out:  `-0.3`,
		},
		{
			//float32 可表示的最大值
			In:   `3.40282346638528859811704183484516925440e+38`,
			Data: float64(math.MaxFloat32),
			Out:  strconv.FormatFloat(math.MaxFloat32, 'g', -1, 64),
		},
		{
			//不损失精度的最小 float32 值
			In:   `1.175494351e-38`,
			Data: float64(1.175494351e-38),
			Out:  `1.175494351e-38`,
		},
		{
			//最接近零的 float32 值
			In:   `1.401298464324817070923729583289916131280e-45`,
			Data: float64(math.SmallestNonzeroFloat32),
			Out:  strconv.FormatFloat(math.SmallestNonzeroFloat32, 'g', -1, 64),
		},
		{
			//float64 可表示的最大值
			In:   `1.797693134862315708145274237317043567981e+308`,
			Data: float64(math.MaxFloat64),
			Out:  strconv.FormatFloat(math.MaxFloat64, 'g', -1, 64),
		},
		{
			//不损失精度的最接近零值
			In:   `2.2250738585072014e-308`,
			Data: float64(2.2250738585072014e-308),
			Out:  `2.2250738585072014e-308`,
		},

		{
			//最接近零的 float64 值
			In:   `4.940656458412465441765687928682213723651e-324`,
			Data: float64(math.SmallestNonzeroFloat64),
			Out:  strconv.FormatFloat(math.SmallestNonzeroFloat64, 'g', -1, 64),
		},

		{
			//math.MaxFloat64 + 2 溢出
			In:  `1.7976931348623159e+308`,
			Err: true,
		},

		//字符串
		{
			In:   `""`,
			Data: string(""),
			Out:  `""`,
		},
		{
			In:   `"0"`,
			Data: string("0"),
			Out:  `"0"`,
		},
		{
			In:   `"A"`,
			Data: string("A"),
			Out:  `"A"`,
		},
		{
			In:   `"Iñtërnâtiônàlizætiøn"`,
			Data: string("Iñtërnâtiônàlizætiøn"),
			Out:  `"Iñtërnâtiônàlizætiøn"`,
		},

		//数组
		{
			In:   `[]`,
			Data: []interface{}{},
			Out:  `[]`,
		},
		{
			In: `[` + strings.Join([]string{
				`null`,
				`true`,
				`false`,
				`0`,
				`9223372036854775807`,
				`0.0`,
				`0.5`,
				`1.0`,
				`1.797693134862315708145274237317043567981e+308`,
				`"0"`,
				`"A"`,
				`"Iñtërnâtiônàlizætiøn"`,
				`[null,true,1,1.0,1.5]`,
				`{"boolkey":true,"floatkey":1.0,"intkey":1,"nullkey":null}`,
			}, ",") + `]`,
			Data: []interface{}{
				nil,
				true,
				false,
				int64(0),
				int64(math.MaxInt64),
				float64(0.0),
				float64(0.5),
				float64(1.0),
				float64(math.MaxFloat64),
				string("0"),
				string("A"),
				string("Iñtërnâtiônàlizætiøn"),
				[]interface{}{nil, true, int64(1), float64(1.0), float64(1.5)},
				map[string]interface{}{"nullkey": nil, "boolkey": true, "intkey": int64(1), "floatkey": float64(1.0)},
			},
			Out: `[` + strings.Join([]string{
				`null`,
				`true`,
				`false`,
				`0`,
				`9223372036854775807`,
				`0`,
				`0.5`,
				`1`,
				strconv.FormatFloat(math.MaxFloat64, 'g', -1, 64),
				`"0"`,
				`"A"`,
				`"Iñtërnâtiônàlizætiøn"`,
				`[null,true,1,1,1.5]`,
				`{"boolkey":true,"floatkey":1,"intkey":1,"nullkey":null}`, //经 Marshal 后按字母顺序排列
			}, ",") + `]`,
		},

		//映射
		{
			In:   `{}`,
			Data: map[string]interface{}{},
			Out:  `{}`,
		},
		{
			In:   `{"boolkey":true,"floatkey":1.0,"intkey":1,"nullkey":null}`,
			Data: map[string]interface{}{"nullkey": nil, "boolkey": true, "intkey": int64(1), "floatkey": float64(1.0)},
			Out:  `{"boolkey":true,"floatkey":1,"intkey":1,"nullkey":null}`, //经 Marshal 后按字母顺序排列
		},
	}

	// 每个用例分别以 map 成员、数组元素和独立对象三种形态解码，
	// 以覆盖不同上下文中的类型推断路径。
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d_map", i), func(t *testing.T) {
			// 把输入作为 map 的一个条目解码
			inputJSON := fmt.Sprintf(`{"data":%s}`, tc.In)
			expectedJSON := fmt.Sprintf(`{"data":%s}`, tc.Out)
			m := map[string]interface{}{}
			err := Unmarshal([]byte(inputJSON), &m)
			if tc.Err && err != nil {
				// 期望的错误
				return
			}
			if err != nil {
				t.Fatalf("%s: error decoding: %v", tc.In, err)
			}
			if tc.Err {
				t.Fatalf("%s: expected error, got none", tc.In)
			}
			data, ok := m["data"]
			if !ok {
				t.Fatalf("%s: decoded object missing data key: %#v", tc.In, m)
			}
			if !reflect.DeepEqual(tc.Data, data) {
				t.Fatalf("%s: expected\n\t%#v (%v), got\n\t%#v (%v)", tc.In, tc.Data, reflect.TypeOf(tc.Data), data, reflect.TypeOf(data))
			}

			outputJSON, err := Marshal(m)
			if err != nil {
				t.Fatalf("%s: error encoding: %v", tc.In, err)
			}

			if expectedJSON != string(outputJSON) {
				t.Fatalf("%s: expected\n\t%s, got\n\t%s", tc.In, expectedJSON, string(outputJSON))
			}
		})

		t.Run(fmt.Sprintf("%d_slice", i), func(t *testing.T) {
			// 把输入作为数组元素解码
			inputJSON := fmt.Sprintf(`[0,%s]`, tc.In)
			expectedJSON := fmt.Sprintf(`[0,%s]`, tc.Out)
			m := []interface{}{}
			err := Unmarshal([]byte(inputJSON), &m)
			if tc.Err && err != nil {
				// 期望的错误
				return
			}
			if err != nil {
				t.Fatalf("%s: error decoding: %v", tc.In, err)
			}
			if tc.Err {
				t.Fatalf("%s: expected error, got none", tc.In)
			}
			if len(m) != 2 {
				t.Fatalf("%s: decoded object wasn't the right length: %#v", tc.In, m)
			}
			data := m[1]
			if !reflect.DeepEqual(tc.Data, data) {
				t.Fatalf("%s: expected\n\t%#v (%v), got\n\t%#v (%v)", tc.In, tc.Data, reflect.TypeOf(tc.Data), data, reflect.TypeOf(data))
			}

			outputJSON, err := Marshal(m)
			if err != nil {
				t.Fatalf("%s: error encoding: %v", tc.In, err)
			}

			if expectedJSON != string(outputJSON) {
				t.Fatalf("%s: expected\n\t%s, got\n\t%s", tc.In, expectedJSON, string(outputJSON))
			}
		})

		t.Run(fmt.Sprintf("%d_raw", i), func(t *testing.T) {
			// 把输入作为独立对象解码
			inputJSON := tc.In
			expectedJSON := tc.Out
			var m interface{}
			err := Unmarshal([]byte(inputJSON), &m)
			if tc.Err && err != nil {
				// 期望的错误
				return
			}
			if err != nil {
				t.Fatalf("%s: error decoding: %v", tc.In, err)
			}
			if tc.Err {
				t.Fatalf("%s: expected error, got none", tc.In)
			}
			data := m
			if !reflect.DeepEqual(tc.Data, data) {
				t.Fatalf("%s: expected\n\t%#v (%v), got\n\t%#v (%v)", tc.In, tc.Data, reflect.TypeOf(tc.Data), data, reflect.TypeOf(data))
			}

			outputJSON, err := Marshal(m)
			if err != nil {
				t.Fatalf("%s: error encoding: %v", tc.In, err)
			}

			if expectedJSON != string(outputJSON) {
				t.Fatalf("%s: expected\n\t%s, got\n\t%s", tc.In, expectedJSON, string(outputJSON))
			}
		})
	}
}

// TestUnmarshalNil 验证向 nil 指针 Unmarshal 时的错误与标准库 encoding/json 一致，
// 避免包装层悄悄改变错误语义。
func TestUnmarshalNil(t *testing.T) {
	{
		var v *interface{}
		err := Unmarshal([]byte(`0`), v)
		goerr := gojson.Unmarshal([]byte(`0`), v)
		if err == nil || goerr == nil || err.Error() != goerr.Error() {
			t.Fatalf("expected error matching stdlib, got %v, %v", err, goerr)
		} else {
			t.Log(err)
		}
	}

	{
		var v *[]interface{}
		err := Unmarshal([]byte(`[]`), v)
		goerr := gojson.Unmarshal([]byte(`[]`), v)
		if err == nil || goerr == nil || err.Error() != goerr.Error() {
			t.Fatalf("expected error matching stdlib, got %v, %v", err, goerr)
		} else {
			t.Log(err)
		}
	}

	{
		var v *map[string]interface{}
		err := Unmarshal([]byte(`{}`), v)
		goerr := gojson.Unmarshal([]byte(`{}`), v)
		if err == nil || goerr == nil || err.Error() != goerr.Error() {
			t.Fatalf("expected error matching stdlib, got %v, %v", err, goerr)
		} else {
			t.Log(err)
		}
	}
}
