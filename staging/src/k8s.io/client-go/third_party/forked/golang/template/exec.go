// This package is copied from Go library text/template.
// The original private functions indirect and printableValue
// are exported as public functions.
//
// 该包复制自标准库 text/template，只把原本私有的 indirect 与 printableValue
// 导出，供 Kubernetes 的模板工具（如 kubectl 的一些输出模板）复用，
// 从而避免依赖标准库内部实现。修改时应尽量保持与上游语义一致。
package template

import (
	"fmt"
	"reflect"
)

// errorType/fmtStringerType 预先缓存反射类型，避免每次格式化时重复查询接口。
var (
	errorType       = reflect.TypeOf((*error)(nil)).Elem()
	fmtStringerType = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
)

// Indirect returns the item at the end of indirection, and a bool to indicate if it's nil.
// We indirect through pointers and empty interfaces (only) because
// non-empty interfaces have methods we might need.
//
// 只穿透指针与空接口；遇到带方法的非空接口即停止，避免丢失调用方法所需的具体类型。
func Indirect(v reflect.Value) (rv reflect.Value, isNil bool) {
	for ; v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface; v = v.Elem() {
		if v.IsNil() {
			return v, true
		}
		if v.Kind() == reflect.Interface && v.NumMethod() > 0 {
			break
		}
	}
	return v, false
}

// PrintableValue returns the, possibly indirected, interface value inside v that
// is best for a call to formatted printer.
//
// 优先返回能实现 error/fmt.Stringer 的值（含指针接收者方法），
// 否则退化为普通取值；chan/func 无法直接打印为有意义内容，返回 ok=false。
func PrintableValue(v reflect.Value) (interface{}, bool) {
	if v.Kind() == reflect.Pointer {
		v, _ = Indirect(v) // fmt.Fprint handles nil.
	}
	if !v.IsValid() {
		return "<no value>", true
	}

	if !v.Type().Implements(errorType) && !v.Type().Implements(fmtStringerType) {
		if v.CanAddr() && (reflect.PointerTo(v.Type()).Implements(errorType) || reflect.PointerTo(v.Type()).Implements(fmtStringerType)) {
			v = v.Addr()
		} else {
			switch v.Kind() {
			case reflect.Chan, reflect.Func:
				return nil, false
			}
		}
	}
	return v.Interface(), true
}
