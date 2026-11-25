/*
Copyright 2025 The Kubernetes Authors.

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

// 这些测试放在单独的包中，以打破测试里的循环依赖。
// Unstructured 类型依赖 unstructured converter 包，但我们又想测试 converter 如何处理
// Unstructured 类型，因此需要同时导入两者。
//
// 本文件专门测试 omitzero 标签在 Unstructured 转换与 JSON 序列化
// 之间的一致性——两条路径对“零值”的判断必须相同，否则同一条数据在不同
// 编码下会以不同字段集出现。

package runtime

import (
	encodingjson "encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/json"

	"github.com/google/go-cmp/cmp"
)

// ZeroParent 是所有字段都带 omitzero 的父结构，覆盖值/指针、内置/自定义
// 类型、可比较/不可比较类型的组合。
type ZeroParent struct {
	Int               int                    `json:"int,omitzero"`
	IntP              *int                   `json:"intP,omitzero"`
	String            string                 `json:"string,omitzero"`
	StringP           *string                `json:"stringP,omitzero"`
	Bool              bool                   `json:"bool,omitzero"`
	BoolP             bool                   `json:"boolP,omitzero"`
	Slice             []int                  `json:"slice,omitzero"`
	SliceP            *[]int                 `json:"sliceP,omitzero"`
	Map               map[string]int         `json:"map,omitzero"`
	MapP              *map[string]int        `json:"mapP,omitzero"`
	Struct            ZeroChild              `json:"struct,omitzero"`
	StructP           *ZeroChild             `json:"structP,omitzero"`
	CustomPrimitive   ZeroCustomPrimitive    `json:"customPrimitive,omitzero"`
	CustomPrimitiveP  *ZeroCustomPrimitiveP  `json:"customPrimitiveP,omitzero"`
	CustomStruct      ZeroCustomStruct       `json:"customStruct,omitzero"`
	CustomStructP     *ZeroCustomStructP     `json:"customStructP,omitzero"`
	CustomPPrimitive  ZeroCustomPPrimitive   `json:"customPPrimitive,omitzero"`
	CustomPPrimitiveP *ZeroCustomPPrimitiveP `json:"customPPrimitiveP,omitzero"`
	CustomPStruct     ZeroCustomPStruct      `json:"customPStruct,omitzero"`
	CustomPStructP    *ZeroCustomPStructP    `json:"customPStructP,omitzero"`
}

// ZeroChild 是普通嵌套结构，没有自定义 IsZero，因此只有零值字段才被省略。
type ZeroChild struct {
	Data int `json:"data"`
}

// 以下自定义类型把 42 定义为“零值”，用于验证 omitzero 会尊重其 IsZero
// 方法，而不是简单比较字段是否为零。

type ZeroCustomPrimitive int

func (z ZeroCustomPrimitive) IsZero() bool {
	return z == 42
}

type ZeroCustomPrimitiveP int

func (z ZeroCustomPrimitiveP) IsZero() bool {
	return z == 42
}

type ZeroCustomStruct struct {
	Data int `json:"data"`
}

func (z ZeroCustomStruct) IsZero() bool {
	return z.Data == 42
}

type ZeroCustomStructP struct {
	Data int `json:"data"`
}

// ZeroCustomStructP 的 IsZero 定义在指针接收者上，覆盖“非指针字段但方法
// 需要取地址”的情形。
func (z ZeroCustomStructP) IsZero() bool {
	return z.Data == 42
}

type ZeroCustomPPrimitive int

func (z *ZeroCustomPPrimitive) IsZero() bool {
	return *z == 42
}

type ZeroCustomPPrimitiveP int

func (z *ZeroCustomPPrimitiveP) IsZero() bool {
	return *z == 42
}

type ZeroCustomPStruct struct {
	Data int `json:"data"`
}

func (z *ZeroCustomPStruct) IsZero() bool {
	return z.Data == 42
}

type ZeroCustomPStructP struct {
	Data int `json:"data"`
}

func (z *ZeroCustomPStructP) IsZero() bool {
	return z.Data == 42
}

// TestOmitZero2 验证 JSON 与 ToUnstructured 对 omitzero 的判定一致：普通
// 零值字段被省略，而“零值=42”的自定义类型（初始值 0 并非其零值）被保留。
func TestOmitZero2(t *testing.T) {
	testcases := []struct {
		name   string
		obj    any
		expect map[string]any
	}{
		{
			name: "emptyzero",
			obj:  &ZeroParent{},
			expect: map[string]any{
				"customPPrimitive": int64(0),
				"customPStruct":    map[string]any{"data": int64(0)},
				"customPrimitive":  int64(0),
				"customStruct":     map[string]any{"data": int64(0)},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			jsonData, err := json.Marshal(tc.obj)
			if err != nil {
				t.Fatal(err)
			}
			jsonUnstructured := map[string]any{}
			if err := json.Unmarshal(jsonData, &jsonUnstructured); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(jsonUnstructured, tc.expect) {
				t.Fatal(cmp.Diff(tc.expect, jsonUnstructured))
			}

			unstr, err := DefaultUnstructuredConverter.ToUnstructured(tc.obj)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(unstr, tc.expect) {
				t.Fatal(cmp.Diff(tc.expect, unstr))
			}
		})
	}
}

// NonZeroStruct 永远报告“非零”，用于验证 omitzero 不会省略它。
type NonZeroStruct struct{}

func (nzs NonZeroStruct) IsZero() bool {
	return false
}

// NoPanicStruct 的 IsZero 在指针接收者上解引用字段；nil 指针调用时必须
// 被安全处理（不 panic），这是 omitzero 实现的边界条件。
type NoPanicStruct struct {
	Int int `json:"int,omitzero"`
}

func (nps *NoPanicStruct) IsZero() bool {
	return nps.Int != 0
}

type isZeroer interface {
	IsZero() bool
}

// OptionalsZero 集中覆盖 omitzero 的各种组合：内联/显式标签、切片、map、
// 浮点、接口、nil 指针与具体类型。字段名即断言中的 JSON key。
type OptionalsZero struct {
	Sr string `json:"sr"`
	So string `json:"so,omitzero"`
	Sw string `json:"-"`

	Ir int `json:"omitzero"` // actually named omitzero, not an option
	Io int `json:"io,omitzero"`

	Slr       []string `json:"slr,random"` //nolint:staticcheck // SA5008
	Slo       []string `json:"slo,omitzero"`
	SloNonNil []string `json:"slononnil,omitzero"`

	Mr  map[string]any `json:"mr"`
	Mo  map[string]any `json:",omitzero"`
	Moo map[string]any `json:"moo,omitzero"`

	Fr   float64    `json:"fr"`
	Fo   float64    `json:"fo,omitzero"`
	Foo  float64    `json:"foo,omitzero"`
	Foo2 [2]float64 `json:"foo2,omitzero"`

	Br bool `json:"br"`
	Bo bool `json:"bo,omitzero"`

	Ur uint `json:"ur"`
	Uo uint `json:"uo,omitzero"`

	Str struct{} `json:"str"`
	Sto struct{} `json:"sto,omitzero"`

	Time      time.Time     `json:"time,omitzero"`
	TimeLocal time.Time     `json:"timelocal,omitzero"`
	Nzs       NonZeroStruct `json:"nzs,omitzero"`

	NilIsZeroer    isZeroer       `json:"niliszeroer,omitzero"`    // nil interface
	NonNilIsZeroer isZeroer       `json:"nonniliszeroer,omitzero"` // non-nil interface
	NoPanicStruct0 isZeroer       `json:"nps0,omitzero"`           // non-nil interface with nil pointer
	NoPanicStruct1 isZeroer       `json:"nps1,omitzero"`           // non-nil interface with non-nil pointer
	NoPanicStruct2 *NoPanicStruct `json:"nps2,omitzero"`           // nil pointer
	NoPanicStruct3 *NoPanicStruct `json:"nps3,omitzero"`           // non-nil pointer
	NoPanicStruct4 NoPanicStruct  `json:"nps4,omitzero"`           // concrete type
}

// TestOmitZero 验证 omitzero 与 omitempty 的差异：omitzero 只按“零值”判断，
// 因而 -0、空但非 nil 的切片、非 nil 的空 map、空 struct 等仍会被输出
// （如 "fr": 0、"slononnil": []、"Mo": {}），而 nil 切片输出 null。
func TestOmitZero(t *testing.T) {
	const want = `{
 "Mo": {},
 "br": false,
 "fr": 0,
 "mr": {},
 "nps1": {},
 "nps3": {},
 "nps4": {},
 "nzs": {},
 "omitzero": 0,
 "slononnil": [],
 "slr": null,
 "sr": "",
 "str": {},
 "ur": 0
}`
	var o OptionalsZero
	o.Sw = "something"
	o.SloNonNil = make([]string, 0)
	o.Mr = map[string]any{}
	o.Mo = map[string]any{}

	o.Foo = -0
	o.Foo2 = [2]float64{+0, -0}

	o.TimeLocal = time.Time{}.Local()

	o.NonNilIsZeroer = time.Time{}
	o.NoPanicStruct0 = (*NoPanicStruct)(nil)
	o.NoPanicStruct1 = &NoPanicStruct{}
	o.NoPanicStruct3 = &NoPanicStruct{}

	unstr, err := DefaultUnstructuredConverter.ToUnstructured(&o)
	if err != nil {
		t.Fatal(err)
	}

	got, err := encodingjson.MarshalIndent(unstr, "", " ")
	if err != nil {
		t.Fatalf("MarshalIndent error: %v", err)
	}
	if got := string(got); got != want {
		t.Errorf("MarshalIndent:\n\tgot:  %s\n\twant: %s\n", got, want)
	}
}

// TestOmitZeroMap 验证 omitzero 在 map value（而非指针）上的行为：map 中的
// 结构体是按值复制后转换的，零值字段同样应被省略。
func TestOmitZeroMap(t *testing.T) {
	const want = `{
 "foo": {
  "br": false,
  "fr": 0,
  "mr": null,
  "nps4": {},
  "nzs": {},
  "omitzero": 0,
  "slr": null,
  "sr": "",
  "str": {},
  "ur": 0
 }
}`

	m := map[string]OptionalsZero{"foo": {}}

	unstr, err := DefaultUnstructuredConverter.ToUnstructured(&m)
	if err != nil {
		t.Fatal(err)
	}

	got, err := encodingjson.MarshalIndent(unstr, "", " ")
	if err != nil {
		t.Fatalf("MarshalIndent error: %v", err)
	}
	if got := string(got); got != want {
		fmt.Println(got)
		t.Errorf("MarshalIndent:\n\tgot:  %s\n\twant: %s\n", got, want)
	}
}

// OptionalsEmptyZero 同时带 omitempty 与 omitzero，用于确定两者的组合语义：
// omitzero 优先判断零值，omitzero 不认为零值时再由 omitempty 决定。
type OptionalsEmptyZero struct {
	Sr string `json:"sr"`
	So string `json:"so,omitempty,omitzero"`
	Sw string `json:"-"`

	Io int `json:"io,omitempty,omitzero"`

	Slr       []string `json:"slr,random"` //nolint:staticcheck // SA5008
	Slo       []string `json:"slo,omitempty,omitzero"`
	SloNonNil []string `json:"slononnil,omitempty,omitzero"`

	Mr map[string]any `json:"mr"`
	Mo map[string]any `json:",omitempty,omitzero"`

	Fr float64 `json:"fr"`
	Fo float64 `json:"fo,omitempty,omitzero"`

	Br bool `json:"br"`
	Bo bool `json:"bo,omitempty,omitzero"`

	Ur uint `json:"ur"`
	Uo uint `json:"uo,omitempty,omitzero"`

	Str struct{} `json:"str"`
	Sto struct{} `json:"sto,omitempty,omitzero"`

	Time time.Time     `json:"time,omitempty,omitzero"`
	Nzs  NonZeroStruct `json:"nzs,omitempty,omitzero"`
}

// TestOmitEmptyZero 验证 omitempty+omitzero 组合：空但非 nil 的 map 因非零值
// 被保留（"mr": {}），空切片则被 omitempty 省略。
func TestOmitEmptyZero(t *testing.T) {
	const want = `{
 "br": false,
 "fr": 0,
 "mr": {},
 "nzs": {},
 "slr": null,
 "sr": "",
 "str": {},
 "ur": 0
}`
	var o OptionalsEmptyZero
	o.Sw = "something"
	o.SloNonNil = make([]string, 0)
	o.Mr = map[string]any{}
	o.Mo = map[string]any{}

	unstr, err := DefaultUnstructuredConverter.ToUnstructured(&o)
	if err != nil {
		t.Fatal(err)
	}

	got, err := encodingjson.MarshalIndent(unstr, "", " ")
	if err != nil {
		t.Fatalf("MarshalIndent error: %v", err)
	}
	if got := string(got); got != want {
		t.Errorf("MarshalIndent:\n\tgot:  %s\n\twant: %s\n", got, want)
	}
}
