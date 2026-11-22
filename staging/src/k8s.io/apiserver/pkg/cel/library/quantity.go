/*
Copyright 2023 The Kubernetes Authors.

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

package library

import (
	"errors"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"k8s.io/apimachinery/pkg/api/resource"
	apiservercel "k8s.io/apiserver/pkg/cel"
)

// Quantity provides a CEL function library extension of Kubernetes
// resource.Quantity parsing functions. See `resource.Quantity`
// documentation for more detailed information about the format itself:
// https://pkg.go.dev/k8s.io/apimachinery/pkg/api/resource#Quantity
//
// Quantity 把字符串解析为资源数量，并提供比较、四则运算与标量转换。
// 所有函数都遵循 Quantity 的规范语义（如 1Gi == 1024Mi），
// 解析失败会作为 CEL 错误返回而不是产生零值。
//
// quantity
//
// Converts a string to a Quantity or results in an error if the string is not a valid Quantity. Refer
// to resource.Quantity documentation for information on accepted patterns.
//
//	quantity(<string>) <Quantity>
//
// Examples:
//
//	quantity('1.5G') // returns a Quantity
//	quantity('200k') // returns a Quantity
//	quantity('200K') // error
//	quantity('Three') // error
//	quantity('Mi') // error
//
// isQuantity
//
// Returns true if a string is a valid Quantity. isQuantity returns true if and
// only if quantity does not result in error.
//
//	isQuantity( <string>) <bool>
//
// Examples:
//
//	isQuantity('1.3G') // returns true
//	isQuantity('1.3Gi') // returns true
//	isQuantity('1,3G') // returns false
//	isQuantity('10000k') // returns true
//	isQuantity('200K') // returns false
//	isQuantity('Three') // returns false
//	isQuantity('Mi') // returns false
//
// Conversion to Scalars:
//
//   - isInteger: returns true if and only if asInteger is safe to call without an error
//
//   - asInteger: returns a representation of the current value as an int64 if
//     possible or results in an error if conversion would result in overflow
//     or loss of precision.
//
//   - asApproximateFloat: returns a float64 representation of the quantity which may
//     lose precision. If the value of the quantity is outside the range of a float64
//     +Inf/-Inf will be returned.
//
//     <Quantity>.isInteger() <bool>
//     <Quantity>.asInteger() <int>
//     <Quantity>.asApproximateFloat() <float>
//
// Examples:
//
//	quantity("50000000G").isInteger() // returns true
//	quantity("50k").isInteger() // returns true
//	quantity("9999999999999999999999999999999999999G").asInteger() // error: cannot convert value to integer
//	quantity("9999999999999999999999999999999999999G").isInteger() // returns false
//	quantity("50k").asInteger() == 50000 // returns true
//	quantity("50k").sub(20000).asApproximateFloat() == 30000 // returns true
//
// Arithmetic
//
//   - sign: Returns `1` if the quantity is positive, `-1` if it is negative. `0` if it is zero
//
//   - add: Returns sum of two quantities or a quantity and an integer
//
//   - sub: Returns difference between two quantities or a quantity and an integer
//
//     <Quantity>.sign() <int>
//     <Quantity>.add(<quantity>) <quantity>
//     <Quantity>.add(<integer>) <quantity>
//     <Quantity>.sub(<quantity>) <quantity>
//     <Quantity>.sub(<integer>) <quantity>
//
// Examples:
//
//	quantity("50k").add("20k") == quantity("70k") // returns true
//	quantity("50k").add(20) == quantity("50020") // returns true
//	quantity("50k").sub("20k") == quantity("30k") // returns true
//	quantity("50k").sub(20000) == quantity("30k") // returns true
//	quantity("50k").add(20).sub(quantity("100k")).sub(-50000) == quantity("20") // returns true
//
// Comparisons
//
//   - isGreaterThan: Returns true if and only if the receiver is greater than the operand
//
//   - isLessThan: Returns true if and only if the receiver is less than the operand
//
//   - compareTo: Compares receiver to operand and returns 0 if they are equal, 1 if the receiver is greater, or -1 if the receiver is less than the operand
//
//     <Quantity>.isLessThan(<quantity>) <bool>
//     <Quantity>.isGreaterThan(<quantity>) <bool>
//     <Quantity>.compareTo(<quantity>) <int>
//
// Examples:
//
//	quantity("200M").compareTo(quantity("0.2G")) // returns 0
//	quantity("50M").compareTo(quantity("50Mi")) // returns -1
//	quantity("50Mi").compareTo(quantity("50M")) // returns 1
//	quantity("150Mi").isGreaterThan(quantity("100Mi")) // returns true
//	quantity("50Mi").isGreaterThan(quantity("100Mi")) // returns false
//	quantity("50M").isLessThan(quantity("100M")) // returns true
//	quantity("100M").isLessThan(quantity("50M")) // returns false
func Quantity() cel.EnvOption {
	return cel.Lib(quantityLib)
}

// quantity 是 quantityLib 的单例实现；cel.Lib 要求库可安全地重复注册，
// 因此这里用无状态空结构体。
var quantityLib = &quantity{}

type quantity struct{}

// LibraryName 返回 CEL 库名，用于环境构建时去重与诊断。
func (*quantity) LibraryName() string {
	return "kubernetes.quantity"
}

// Types 声明该库引入的自定义类型（Quantity），编译器据此做类型推断。
func (*quantity) Types() []*cel.Type {
	return []*cel.Type{apiservercel.QuantityType}
}

// declarations 返回库中声明的函数表，供 cel.Lib 机制统一注册。
func (*quantity) declarations() map[string][]cel.FunctionOpt {
	return quantityLibraryDecls
}

// quantityLibraryDecls 列出所有 CEL 函数重载。
// add/sub 各有 quantity 与 int 两个重载，是为了让表达式既能与另一个
// Quantity 相加，也能与整数字面量相加，避免用户手动包装。
var quantityLibraryDecls = map[string][]cel.FunctionOpt{
	"quantity": {
		cel.Overload("string_to_quantity", []*cel.Type{cel.StringType}, apiservercel.QuantityType, cel.UnaryBinding((stringToQuantity))),
	},
	"isQuantity": {
		cel.Overload("is_quantity_string", []*cel.Type{cel.StringType}, cel.BoolType, cel.UnaryBinding(isQuantity)),
	},
	"sign": {
		cel.Overload("quantity_sign", []*cel.Type{apiservercel.QuantityType}, cel.IntType, cel.UnaryBinding(quantityGetSign)),
	},
	"isGreaterThan": {
		cel.MemberOverload("quantity_is_greater_than", []*cel.Type{apiservercel.QuantityType, apiservercel.QuantityType}, cel.BoolType, cel.BinaryBinding(quantityIsGreaterThan)),
	},
	"isLessThan": {
		cel.MemberOverload("quantity_is_less_than", []*cel.Type{apiservercel.QuantityType, apiservercel.QuantityType}, cel.BoolType, cel.BinaryBinding(quantityIsLessThan)),
	},
	"compareTo": {
		cel.MemberOverload("quantity_compare_to", []*cel.Type{apiservercel.QuantityType, apiservercel.QuantityType}, cel.IntType, cel.BinaryBinding(quantityCompareTo)),
	},
	"asApproximateFloat": {
		cel.MemberOverload("quantity_get_float", []*cel.Type{apiservercel.QuantityType}, cel.DoubleType, cel.UnaryBinding(quantityGetApproximateFloat)),
	},
	"asInteger": {
		cel.MemberOverload("quantity_get_int", []*cel.Type{apiservercel.QuantityType}, cel.IntType, cel.UnaryBinding(quantityGetValue)),
	},
	"isInteger": {
		cel.MemberOverload("quantity_is_integer", []*cel.Type{apiservercel.QuantityType}, cel.BoolType, cel.UnaryBinding(quantityCanValue)),
	},
	"add": {
		cel.MemberOverload("quantity_add", []*cel.Type{apiservercel.QuantityType, apiservercel.QuantityType}, apiservercel.QuantityType, cel.BinaryBinding(quantityAdd)),
		cel.MemberOverload("quantity_add_int", []*cel.Type{apiservercel.QuantityType, cel.IntType}, apiservercel.QuantityType, cel.BinaryBinding(quantityAddInt)),
	},
	"sub": {
		cel.MemberOverload("quantity_sub", []*cel.Type{apiservercel.QuantityType, apiservercel.QuantityType}, apiservercel.QuantityType, cel.BinaryBinding(quantitySub)),
		cel.MemberOverload("quantity_sub_int", []*cel.Type{apiservercel.QuantityType, cel.IntType}, apiservercel.QuantityType, cel.BinaryBinding(quantitySubInt)),
	},
}

// CompileOptions 把每个函数名及其重载注册进 CEL 环境。
func (*quantity) CompileOptions() []cel.EnvOption {
	options := make([]cel.EnvOption, 0, len(quantityLibraryDecls))
	for name, overloads := range quantityLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 返回空：所有实现都是纯函数绑定，不需要额外的运行期选项。
func (*quantity) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// isQuantity 判断字符串能否被解析为 Quantity；解析失败只返回 false，
// 因为该函数的语义就是“是否合法”，而不是传播错误。
func isQuantity(arg ref.Val) ref.Val {
	str, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	_, err := resource.ParseQuantity(str)
	if err != nil {
		return types.Bool(false)
	}

	return types.Bool(true)
}

// stringToQuantity 解析字符串并包装为 CEL 的 Quantity 值；
// 解析错误会作为 CEL 运行期错误返回，使策略评估失败而不是静默得到零值。
func stringToQuantity(arg ref.Val) ref.Val {
	str, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q, err := resource.ParseQuantity(str)
	if err != nil {
		return types.WrapErr(err)
	}

	return apiservercel.Quantity{Quantity: &q}
}

// quantityGetApproximateFloat 把 Quantity 转成 float64，可能损失精度；
// 超出 float64 范围时由 resource 包返回 ±Inf，而不是报错。
func quantityGetApproximateFloat(arg ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	return types.Double(q.AsApproximateFloat64())
}

// quantityCanValue 判断 Quantity 能否无损转换为 int64；与 asInteger 共享
// 同一底层判断，保证 isInteger 为 true 时 asInteger 一定不会报错。
func quantityCanValue(arg ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	_, success := q.AsInt64()
	return types.Bool(success)
}

// quantityGetValue 把 Quantity 精确转换为 int64；溢出或存在小数部分时返回错误，
// 而不是截断，避免策略静默接受错误的数值。
func quantityGetValue(arg ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	v, success := q.AsInt64()
	if !success {
		return types.WrapErr(errors.New("cannot convert value to integer"))
	}
	return types.Int(v)
}

// quantityGetSign 返回 -1/0/1，对应 Quantity 的符号。
func quantityGetSign(arg ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	return types.Int(q.Sign())
}

// quantityIsGreaterThan 使用 Quantity 的规范比较，正确处理不同后缀
// （如 1Gi 与 1024Mi 视为相等）。
func quantityIsGreaterThan(arg ref.Val, other ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2, ok := other.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(q.Cmp(*q2) == 1)
}

// quantityIsLessThan 与 isGreaterThan 对称，基于 Quantity.Cmp 的规范比较。
func quantityIsLessThan(arg ref.Val, other ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2, ok := other.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(q.Cmp(*q2) == -1)
}

// quantityCompareTo 返回 -1/0/1，语义与 CEL 的 compareTo 约定一致。
func quantityCompareTo(arg ref.Val, other ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2, ok := other.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Int(q.Cmp(*q2))
}

// quantityAdd 先 DeepCopy 再相加：Quantity 内部带有缓存，直接修改参数会污染
// 调用方持有的值（CEL 值可能被多个表达式共享）。
func quantityAdd(arg ref.Val, other ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2, ok := other.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	copy := q.DeepCopy()
	copy.Add(*q2)
	return &apiservercel.Quantity{
		Quantity: &copy,
	}
}

// quantityAddInt 把 int64 按十进制指数形式转换为 Quantity 后相加，
// 十进制指数保证整数值的精确表示，不引入二进制舍入误差。
func quantityAddInt(arg ref.Val, other ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2, ok := other.Value().(int64)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2Converted := *resource.NewQuantity(q2, resource.DecimalExponent)

	copy := q.DeepCopy()
	copy.Add(q2Converted)
	return &apiservercel.Quantity{
		Quantity: &copy,
	}
}

// quantitySub 与 add 相同，同样先复制以避免修改接收者。
func quantitySub(arg ref.Val, other ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2, ok := other.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	copy := q.DeepCopy()
	copy.Sub(*q2)
	return &apiservercel.Quantity{
		Quantity: &copy,
	}
}

// quantitySubInt 是 quantitySub 的整数重载，转换方式与 quantityAddInt 一致。
func quantitySubInt(arg ref.Val, other ref.Val) ref.Val {
	q, ok := arg.Value().(*resource.Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2, ok := other.Value().(int64)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	q2Converted := *resource.NewQuantity(q2, resource.DecimalExponent)

	copy := q.DeepCopy()
	copy.Sub(q2Converted)
	return &apiservercel.Quantity{
		Quantity: &copy,
	}
}
