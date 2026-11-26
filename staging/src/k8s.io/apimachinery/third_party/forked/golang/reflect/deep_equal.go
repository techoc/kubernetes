// Copyright 2009 The Go Authors. All rights reserved.
// 本源码的使用受 BSD 风格许可证约束，详见 LICENSE 文件。

// Package reflect 是 Go 标准库 reflect 包的一个分支，
// 支持使用自定义的相等函数进行深度比较。
package reflect

import (
	"fmt"
	"reflect"
	"strings"
)

// Equalities 是从类型到用于比较该类型两个值的函数的映射。
type Equalities map[reflect.Type]reflect.Value

// 为方便起见，出错时直接 panic
func EqualitiesOrDie(funcs ...interface{}) Equalities {
	e := Equalities{}
	if err := e.AddFuncs(funcs...); err != nil {
		panic(err)
	}
	return e
}

// AddFuncs 是多次调用 AddFunc 的快捷方式。
func (e Equalities) AddFuncs(funcs ...interface{}) error {
	for _, f := range funcs {
		if err := e.AddFunc(f); err != nil {
			return err
		}
	}
	return nil
}

// AddFunc 将 func 用作相等函数：它必须接受
// 两个同类型的参数，并返回一个布尔值。
func (e Equalities) AddFunc(eqFunc interface{}) error {
	fv := reflect.ValueOf(eqFunc)
	ft := fv.Type()
	if ft.Kind() != reflect.Func {
		return fmt.Errorf("expected func, got: %v", ft)
	}
	if ft.NumIn() != 2 {
		return fmt.Errorf("expected two 'in' params, got: %v", ft)
	}
	if ft.NumOut() != 1 {
		return fmt.Errorf("expected one 'out' param, got: %v", ft)
	}
	if ft.In(0) != ft.In(1) {
		return fmt.Errorf("expected arg 1 and 2 to have same type, but got %v", ft)
	}
	var forReturnType bool
	boolType := reflect.TypeOf(forReturnType)
	if ft.Out(0) != boolType {
		return fmt.Errorf("expected bool return, got: %v", ft)
	}
	e[ft.In(0)] = fv
	return nil
}

// 以下内容分支自 Go 的 reflect/deepequal.go

// 在 deepValueEqual 执行期间，必须记录正在进行的检查。
// 比较算法在再次遇到这些检查时，假定它们的结果均为 true。
// 已访问的比较记录在一个以 visit 为键的 map 中。
type visit struct {
	a1  uintptr
	a2  uintptr
	typ reflect.Type
}

// 当对含有非导出类型的对象使用 DeepEqual 时，会抛出 unexportedTypePanic。
// 这表示程序员犯了错误，运行时本不应发生，因此它不公开，也就无法被捕获。
type unexportedTypePanic []reflect.Type

func (u unexportedTypePanic) Error() string { return u.String() }
func (u unexportedTypePanic) String() string {
	strs := make([]string, len(u))
	for i, t := range u {
		strs[i] = fmt.Sprintf("%v", t)
	}
	return "an unexported field was encountered, nested like this: " + strings.Join(strs, " -> ")
}

func makeUsefulPanic(v reflect.Value) {
	if x := recover(); x != nil {
		if u, ok := x.(unexportedTypePanic); ok {
			u = append(unexportedTypePanic{v.Type()}, u...)
			x = u
		}
		panic(x)
	}
}

// 通过反射类型测试深度相等。map 参数用于记录已经比较过的对象，
// 从而在递归类型上实现短路。
// equateNilAndEmpty 控制空的 map/slice 是否等价于 nil
func (e Equalities) deepValueEqual(v1, v2 reflect.Value, visited map[visit]bool, equateNilAndEmpty bool, depth int) bool {
	// 将非导出字段触发的 panic 转成带注释的 panic，
	// 以便完整的访问路径出现在堆栈跟踪中。
	defer makeUsefulPanic(v1)

	if !v1.IsValid() || !v2.IsValid() {
		// 无效的 Value 表示 nil 接口；两个 nil 相等。
		return v1.IsValid() == v2.IsValid()
	}
	if v1.Type() != v2.Type() {
		return false
	}
	// 已注册的相等函数优先于结构化比较。
	if fv, ok := e[v1.Type()]; ok {
		return fv.Call([]reflect.Value{v1, v2})[0].Bool()
	}

	hard := func(k reflect.Kind) bool {
		// 只有类似容器的 Kind 才可能递归，需要跟踪环。
		switch k {
		case reflect.Array, reflect.Map, reflect.Slice, reflect.Struct:
			return true
		}
		return false
	}

	if v1.CanAddr() && v2.CanAddr() && hard(v1.Kind()) {
		addr1 := v1.UnsafeAddr()
		addr2 := v2.UnsafeAddr()
		if addr1 > addr2 {
			// 规范化顺序，减少 visited 中的条目数。
			addr1, addr2 = addr2, addr1
		}

		// 若引用相同则短路 ...
		if addr1 == addr2 {
			return true
		}

		// ... 或已经比较过
		// 对结构良好的图而言，假定进行中的比较相等是安全的，
		// 也是在环上终止比较的必要条件。
		typ := v1.Type()
		v := visit{addr1, addr2, typ}
		if visited[v] {
			return true
		}

		// 记录下来供后续使用。
		visited[v] = true
	}

	switch v1.Kind() {
	case reflect.Array:
		// 这里无需检查长度，因为长度是数组类型的一部分，
		// 而类型已在前面过滤过。
		for i := 0; i < v1.Len(); i++ {
			if !e.deepValueEqual(v1.Index(i), v2.Index(i), visited, equateNilAndEmpty, depth+1) {
				return false
			}
		}
		return true
	case reflect.Slice:
		if equateNilAndEmpty {
			if (v1.IsNil() || v1.Len() == 0) != (v2.IsNil() || v2.Len() == 0) {
				return false
			}

			if v1.IsNil() || v1.Len() == 0 {
				return true
			}
		} else {
			if v1.IsNil() != v2.IsNil() {
				return false
			}

			// 优化 nil 和空的情况
			// 两个都为 nil 的列表相等
			// 上面已得出 v1.IsNil == v2.IsNil，无需再检查 v2 是否为 nil
			if v1.IsNil() {
				return true
			}

			// 两个都为空且都非 nil 的列表相等
			if v1.Len() == 0 && v2.Len() == 0 {
				return true
			}
		}
		if v1.Len() != v2.Len() {
			return false
		}
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		for i := 0; i < v1.Len(); i++ {
			if !e.deepValueEqual(v1.Index(i), v2.Index(i), visited, equateNilAndEmpty, depth+1) {
				return false
			}
		}
		return true
	case reflect.Interface:
		if v1.IsNil() || v2.IsNil() {
			return v1.IsNil() == v2.IsNil()
		}
		return e.deepValueEqual(v1.Elem(), v2.Elem(), visited, equateNilAndEmpty, depth+1)
	case reflect.Pointer:
		return e.deepValueEqual(v1.Elem(), v2.Elem(), visited, equateNilAndEmpty, depth+1)
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			if !e.deepValueEqual(v1.Field(i), v2.Field(i), visited, equateNilAndEmpty, depth+1) {
				return false
			}
		}
		return true
	case reflect.Map:
		if equateNilAndEmpty {
			if (v1.IsNil() || v1.Len() == 0) != (v2.IsNil() || v2.Len() == 0) {
				return false
			}
			if v1.IsNil() || v1.Len() == 0 {
				return true
			}
		} else {
			if v1.IsNil() != v2.IsNil() {
				return false
			}

			// 优化 nil 和空的情况
			// 两个都为 nil 的 map 相等
			// 上面已得出 v1.IsNil == v2.IsNil，无需再检查 v2 是否为 nil
			if v1.IsNil() {
				return true
			}

			// 两个都为空且都非 nil 的 map 相等
			if v1.Len() == 0 && v2.Len() == 0 {
				return true
			}
		}
		if v1.Len() != v2.Len() {
			return false
		}
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		for _, k := range v1.MapKeys() {
			if !e.deepValueEqual(v1.MapIndex(k), v2.MapIndex(k), visited, equateNilAndEmpty, depth+1) {
				return false
			}
		}
		return true
	case reflect.Func:
		if v1.IsNil() && v2.IsNil() {
			return true
		}
		// 只能做到这样：
		return false
	default:
		// 普通的相等比较即可
		if !v1.CanInterface() || !v2.CanInterface() {
			panic(unexportedTypePanic{})
		}
		return v1.Interface() == v2.Interface()
	}
}

// DeepEqual 类似于 reflect.DeepEqual，但关注的是语义相等，
// 而非内存上的相等。
//
// 如果找到匹配的类型，它会使用 e 中的相等函数。
//
// 在我们的语义里，空 slice *确实*等于 nil slice；map 也是如此。
//
// 非导出的字段成员无法比较，会触发带有说明信息的 panic；
// 你必须为这些类型添加 Equality 函数。
func (e Equalities) DeepEqual(a1, a2 interface{}) bool {
	return e.deepEqual(a1, a2, true)
}

func (e Equalities) DeepEqualWithNilDifferentFromEmpty(a1, a2 interface{}) bool {
	return e.deepEqual(a1, a2, false)
}

// deepEqual 是接口层面的入口；它将参数展开为 reflect.Value，
// 并初始化用于环检测的 map。
func (e Equalities) deepEqual(a1, a2 interface{}, equateNilAndEmpty bool) bool {
	if a1 == nil || a2 == nil {
		return a1 == a2
	}
	v1 := reflect.ValueOf(a1)
	v2 := reflect.ValueOf(a2)
	if v1.Type() != v2.Type() {
		return false
	}
	return e.deepValueEqual(v1, v2, make(map[visit]bool), equateNilAndEmpty, 0)
}

func (e Equalities) deepValueDerive(v1, v2 reflect.Value, visited map[visit]bool, depth int) bool {
	// 与 deepValueEqual 使用相同的 panic 注释处理。
	defer makeUsefulPanic(v1)

	if !v1.IsValid() || !v2.IsValid() {
		return v1.IsValid() == v2.IsValid()
	}
	if v1.Type() != v2.Type() {
		return false
	}
	if fv, ok := e[v1.Type()]; ok {
		return fv.Call([]reflect.Value{v1, v2})[0].Bool()
	}

	hard := func(k reflect.Kind) bool {
		switch k {
		case reflect.Array, reflect.Map, reflect.Slice, reflect.Struct:
			return true
		}
		return false
	}

	if v1.CanAddr() && v2.CanAddr() && hard(v1.Kind()) {
		addr1 := v1.UnsafeAddr()
		addr2 := v2.UnsafeAddr()
		if addr1 > addr2 {
			// 规范化顺序，减少 visited 中的条目数。
			addr1, addr2 = addr2, addr1
		}

		// 若引用相同则短路 ...
		if addr1 == addr2 {
			return true
		}

		// ... 或已经比较过
		typ := v1.Type()
		v := visit{addr1, addr2, typ}
		if visited[v] {
			return true
		}

		// 记录下来供后续使用。
		visited[v] = true
	}

	switch v1.Kind() {
	case reflect.Array:
		// 这里无需检查长度，因为长度是数组类型的一部分，
		// 而类型已在前面过滤过。
		for i := 0; i < v1.Len(); i++ {
			if !e.deepValueDerive(v1.Index(i), v2.Index(i), visited, depth+1) {
				return false
			}
		}
		return true
	case reflect.Slice:
		// 在 derive 语义下，为空或 nil 的 "wanted" slice 不构成
		// 任何约束；它与任意实际 slice 都匹配。
		if v1.IsNil() || v1.Len() == 0 {
			return true
		}
		if v1.Len() > v2.Len() {
			return false
		}
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		for i := 0; i < v1.Len(); i++ {
			if !e.deepValueDerive(v1.Index(i), v2.Index(i), visited, depth+1) {
				return false
			}
		}
		return true
	case reflect.String:
		// 空字符串被视为未设置，因此与任意值匹配。
		if v1.Len() == 0 {
			return true
		}
		if v1.Len() > v2.Len() {
			return false
		}
		return v1.String() == v2.String()
	case reflect.Interface:
		if v1.IsNil() {
			return true
		}
		return e.deepValueDerive(v1.Elem(), v2.Elem(), visited, depth+1)
	case reflect.Pointer:
		if v1.IsNil() {
			return true
		}
		return e.deepValueDerive(v1.Elem(), v2.Elem(), visited, depth+1)
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			if !e.deepValueDerive(v1.Field(i), v2.Field(i), visited, depth+1) {
				return false
			}
		}
		return true
	case reflect.Map:
		if v1.IsNil() || v1.Len() == 0 {
			return true
		}
		if v1.Len() > v2.Len() {
			return false
		}
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		for _, k := range v1.MapKeys() {
			if !e.deepValueDerive(v1.MapIndex(k), v2.MapIndex(k), visited, depth+1) {
				return false
			}
		}
		return true
	case reflect.Func:
		if v1.IsNil() && v2.IsNil() {
			return true
		}
		// 只能做到这样：
		return false
	default:
		// 普通的相等比较即可
		if !v1.CanInterface() || !v2.CanInterface() {
			panic(unexportedTypePanic{})
		}
		return v1.Interface() == v2.Interface()
	}
}

// DeepDerivative 与 DeepEqual 类似，区别在于 a1 中未设置的字段会被
// 忽略（不参与比较），这样就能专注于对语义比较有意义的字段。
//
// 未设置的字段包括 nil 指针和空字符串。
func (e Equalities) DeepDerivative(a1, a2 interface{}) bool {
	// nil 的 "wanted" 对象与一切匹配，包括 nil 本身。
	if a1 == nil {
		return true
	}
	v1 := reflect.ValueOf(a1)
	v2 := reflect.ValueOf(a2)
	if v1.Type() != v2.Type() {
		return false
	}
	return e.deepValueDerive(v1, v2, make(map[visit]bool), 0)
}
