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

package roundtrip

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// defaultFillFuncs 返回各特殊类型的自定义填充函数。
// 这些类型没有可直接按字段递归填充的普通结构，或需要规避序列化差异。
func defaultFillFuncs() map[reflect.Type]FillFunc {
	funcs := map[reflect.Type]FillFunc{}
	funcs[reflect.TypeOf(&runtime.RawExtension{})] = func(s string, i int, obj interface{}) {
		// 生成规范化形式的原始对象；
		// TODO: 测试非规范化数据的往返；YAMLToJSON 会规范化数据，导致逐字节比较失败。
		obj.(*runtime.RawExtension).Raw = []byte(`{"apiVersion":"example.com/v1","kind":"CustomType","spec":{"replicas":1},"status":{"available":1}}`)
	}
	funcs[reflect.TypeOf(&metav1.TypeMeta{})] = func(s string, i int, obj interface{}) {
		// 并非所有格式都会序列化 APIVersion 与 Kind（尤其是 protobuf），因此默认清空以便跨格式比较。
		obj.(*metav1.TypeMeta).APIVersion = ""
		obj.(*metav1.TypeMeta).Kind = ""
	}
	funcs[reflect.TypeOf(&metav1.FieldsV1{})] = func(s string, i int, obj interface{}) {
		obj.(*metav1.FieldsV1).SetRawString(`{}`)
	}
	funcs[reflect.TypeOf(&metav1.Time{})] = func(s string, i int, obj interface{}) {
		// 用整数作为相对年份的偏移，保证同一字段的取值确定且可复现。
		obj.(*metav1.Time).Time = time.Date(2000+i, 1, 1, 1, 1, 1, 0, time.UTC)
	}
	funcs[reflect.TypeOf(&metav1.MicroTime{})] = func(s string, i int, obj interface{}) {
		// 用整数作为相对年份的偏移，同时作为微秒部分，以覆盖微秒精度的序列化。
		obj.(*metav1.MicroTime).Time = time.Date(2000+i, 1, 1, 1, 1, 1, i*int(time.Microsecond), time.UTC)
	}
	funcs[reflect.TypeOf(&intstr.IntOrString{})] = func(s string, i int, obj interface{}) {
		// 使用字符串作为字符串值，避免测试对 IntOrString 的多态分支产生依赖。
		obj.(*intstr.IntOrString).Type = intstr.String
		obj.(*intstr.IntOrString).StrVal = s + "Value"
	}
	return funcs
}

// CompatibilityTestObject 按指定 GVK 构造一个确定性填充的对象，供兼容性/往返测试使用。
func CompatibilityTestObject(scheme *runtime.Scheme, gvk schema.GroupVersionKind, fillFuncs map[reflect.Type]FillFunc) (runtime.Object, error) {
	// 构造对象
	obj, err := scheme.New(gvk)
	if err != nil {
		return nil, err
	}

	fill("", 0, reflect.TypeOf(obj), reflect.ValueOf(obj), fillFuncs, map[reflect.Type]bool{})

	// 设置 kind 与 apiVersion
	if typeAcc, err := apimeta.TypeAccessor(obj); err != nil {
		return nil, err
	} else {
		typeAcc.SetKind(gvk.Kind)
		typeAcc.SetAPIVersion(gvk.GroupVersion().String())
	}

	return obj, nil
}

// fill 递归地把确定性的测试数据写入 v。
// dataString/dataInt 携带字段的 JSON 名与 protobuf 序号，用于生成稳定可复现的取值；
// filledTypes 记录当前递归路径上已处理的类型，防止自引用结构导致无限递归。
func fill(dataString string, dataInt int, t reflect.Type, v reflect.Value, fillFuncs map[reflect.Type]FillFunc, filledTypes map[reflect.Type]bool) {
	if filledTypes[t] {
		// 该类型已在当前路径上填充过，避免无限递归
		return
	}
	filledTypes[t] = true
	defer delete(filledTypes, t)

	// 若为 nil 指针，用底层类型的零值实例填充
	if t.Kind() == reflect.Pointer && v.IsNil() {
		if v.CanSet() {
			v.Set(reflect.New(t.Elem()))
		} else if v.IsNil() {
			panic(fmt.Errorf("unsettable nil pointer of type %v in field %s", t, dataString))
		}
	}

	if f, ok := fillFuncs[t]; ok {
		// 使用该类型的自定义填充函数
		f(dataString, dataInt, v.Interface())
		return
	}

	switch t.Kind() {
	case reflect.Slice:
		// 填充单元素切片
		v.Set(reflect.MakeSlice(t, 1, 1))
		// 递归填充该元素，并保留数据上下文
		if t.Elem().Kind() == reflect.Pointer {
			fill(dataString, dataInt, t.Elem(), v.Index(0), fillFuncs, filledTypes)
		} else {
			fill(dataString, dataInt, reflect.PointerTo(t.Elem()), v.Index(0).Addr(), fillFuncs, filledTypes)
		}

	case reflect.Map:
		// 构造 key，必须是 string 类型，也可能是 string 的类型别名
		key := reflect.ValueOf(dataString + "Key").Convert(t.Key())
		// 构造零值 item
		item := reflect.New(t.Elem())
		// 递归填充 item，并保留数据上下文
		fill(dataString, dataInt, t.Elem(), item.Elem(), fillFuncs, filledTypes)
		// 写入 map
		v.Set(reflect.MakeMap(t))
		v.SetMapIndex(key, item.Elem())

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)

			if !field.IsExported() {
				continue
			}

			// 使用 JSON 字段名，它在不同版本间必须保持稳定
			jsonTag, _ := field.Tag.Lookup("json")
			dataString = strings.Split(jsonTag, ",")[0]
			if dataString == "-" {
				// 不参与序列化的字段无需填充
				continue
			}
			if len(dataString) == 0 {
				// 没有 JSON 字段名时回退到结构体字段名
				dataString = "<no json tag> " + field.Name
			}

			// 使用 protobuf 序号，它同样必须保持稳定
			dataInt := 0
			if protobufTagParts := strings.Split(field.Tag.Get("protobuf"), ","); len(protobufTagParts) > 1 {
				if tag, err := strconv.Atoi(protobufTagParts[1]); err != nil {
					panic(err)
				} else {
					dataInt = tag
				}
			}
			if dataInt == 0 {
				// 回退到 dataString 的长度作为备用
				dataInt = -len(dataString)
			}

			fieldType := field.Type
			fieldValue := v.Field(i)

			fill(dataString, dataInt, reflect.PointerTo(fieldType), fieldValue.Addr(), fillFuncs, filledTypes)
		}

	case reflect.Pointer:
		fill(dataString, dataInt, t.Elem(), v.Elem(), fillFuncs, filledTypes)

	case reflect.String:
		// 借助 Convert 正确写入 string 的别名类型
		v.Set(reflect.ValueOf(dataString + "Value").Convert(t))

	case reflect.Bool:
		// 置为 true，确保带 omitempty 的字段会被序列化出来
		v.Set(reflect.ValueOf(true).Convert(t))

	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// 借助 Convert 正确写入整型别名类型及不同位宽
		v.Set(reflect.ValueOf(dataInt).Convert(t))

	case reflect.Float32, reflect.Float64:
		// 加 0.5 以得到一个非整数的小数值，并用 Convert 写入浮点类型
		v.Set(reflect.ValueOf(float32(dataInt) + 0.5).Convert(t))

	default:
		panic(fmt.Errorf("unhandled type %v in field %s", t, dataString))
	}
}
