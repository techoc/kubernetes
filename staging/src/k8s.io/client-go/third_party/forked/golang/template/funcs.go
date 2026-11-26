// 本包复制自 Go 标准库 text/template，把原本私有的 eq、ge、gt、le、lt、ne
// 函数导出为公开函数。
package template

import (
	"errors"
	"reflect"
)

var (
	// 与标准库 text/template 相同的错误文本，保持模板求值的行为一致。
	errBadComparisonType = errors.New("invalid type for comparison")
	errBadComparison     = errors.New("incompatible types for comparison")
	errNoComparison      = errors.New("missing argument for comparison")
)

// kind 把 Go 的多种数值类型归并为可比较的类别。
type kind int

const (
	invalidKind kind = iota
	boolKind
	complexKind
	intKind
	floatKind
	integerKind
	stringKind
	uintKind
)

// basicKind 将 reflect.Kind 映射为比较类别；不支持的类型返回 errBadComparisonType。
func basicKind(v reflect.Value) (kind, error) {
	switch v.Kind() {
	case reflect.Bool:
		return boolKind, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intKind, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintKind, nil
	case reflect.Float32, reflect.Float64:
		return floatKind, nil
	case reflect.Complex64, reflect.Complex128:
		return complexKind, nil
	case reflect.String:
		return stringKind, nil
	}
	return invalidKind, errBadComparisonType
}

// Equal 计算 a == b || a == c || ...。
//
// 对应模板中的 eq 函数：任一参数与 arg1 相等即返回 true。
// 无符号与有符号整数可跨符号比较（负数视为不相等/更小），其余类型不匹配则报错。
func Equal(arg1 interface{}, arg2 ...interface{}) (bool, error) {
	v1 := reflect.ValueOf(arg1)
	k1, err := basicKind(v1)
	if err != nil {
		return false, err
	}
	if len(arg2) == 0 {
		return false, errNoComparison
	}
	for _, arg := range arg2 {
		v2 := reflect.ValueOf(arg)
		k2, err := basicKind(v2)
		if err != nil {
			return false, err
		}
		truth := false
		if k1 != k2 {
			// 特例：整数值可以跨符号类型比较。
			// 负数先单独判断，避免 uint64 转换后产生回绕导致误判。
			switch {
			case k1 == intKind && k2 == uintKind:
				truth = v1.Int() >= 0 && uint64(v1.Int()) == v2.Uint()
			case k1 == uintKind && k2 == intKind:
				truth = v2.Int() >= 0 && v1.Uint() == uint64(v2.Int())
			default:
				return false, errBadComparison
			}
		} else {
			switch k1 {
			case boolKind:
				truth = v1.Bool() == v2.Bool()
			case complexKind:
				truth = v1.Complex() == v2.Complex()
			case floatKind:
				truth = v1.Float() == v2.Float()
			case intKind:
				truth = v1.Int() == v2.Int()
			case stringKind:
				truth = v1.String() == v2.String()
			case uintKind:
				truth = v1.Uint() == v2.Uint()
			default:
				// basicKind 已排除其它类型，走到这里说明类别表与 switch 不一致。
				panic("invalid kind")
			}
		}
		if truth {
			return true, nil
		}
	}
	return false, nil
}

// NotEqual 计算 a != b。
func NotEqual(arg1, arg2 interface{}) (bool, error) {
	// != 是 == 的逆运算。
	// 出错时仍返回取反后的值，保持与标准库一致的“错误值无意义”约定。
	equal, err := Equal(arg1, arg2)
	return !equal, err
}

// Less 计算 a < b。
//
// 对应模板的 lt：bool 与 complex 不可排序；有符号/无符号整数可跨符号比较。
func Less(arg1, arg2 interface{}) (bool, error) {
	v1 := reflect.ValueOf(arg1)
	k1, err := basicKind(v1)
	if err != nil {
		return false, err
	}
	v2 := reflect.ValueOf(arg2)
	k2, err := basicKind(v2)
	if err != nil {
		return false, err
	}
	truth := false
	if k1 != k2 {
		// 特例：整数值可以跨符号类型比较。
		switch {
		case k1 == intKind && k2 == uintKind:
			truth = v1.Int() < 0 || uint64(v1.Int()) < v2.Uint()
		case k1 == uintKind && k2 == intKind:
			truth = v2.Int() >= 0 && v1.Uint() < uint64(v2.Int())
		default:
			return false, errBadComparison
		}
	} else {
		switch k1 {
		case boolKind, complexKind:
			return false, errBadComparisonType
		case floatKind:
			truth = v1.Float() < v2.Float()
		case intKind:
			truth = v1.Int() < v2.Int()
		case stringKind:
			truth = v1.String() < v2.String()
		case uintKind:
			truth = v1.Uint() < v2.Uint()
		default:
			panic("invalid kind")
		}
	}
	return truth, nil
}

// LessEqual 计算 a <= b。
func LessEqual(arg1, arg2 interface{}) (bool, error) {
	// <= is < or ==.
	lessThan, err := Less(arg1, arg2)
	if lessThan || err != nil {
		return lessThan, err
	}
	return Equal(arg1, arg2)
}

// Greater 计算 a > b。
func Greater(arg1, arg2 interface{}) (bool, error) {
	// > 是 <= 的逆运算。
	lessOrEqual, err := LessEqual(arg1, arg2)
	if err != nil {
		return false, err
	}
	return !lessOrEqual, nil
}

// GreaterEqual 计算 a >= b。
func GreaterEqual(arg1, arg2 interface{}) (bool, error) {
	// >= 是 < 的逆运算。
	lessThan, err := Less(arg1, arg2)
	if err != nil {
		return false, err
	}
	return !lessThan, nil
}
