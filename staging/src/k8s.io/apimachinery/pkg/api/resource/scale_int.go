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

// Package resource 提供用于表示和运算资源数量的类型（如 Quantity），
// 以及其内部使用的十进制缩放整数运算。
package resource

import (
	"math"
	"math/big"
	"sync"
)

var (
	// 一个 sync.Pool，用于减少 big.Int 的重复分配。
	intPool  sync.Pool
	maxInt64 = big.NewInt(math.MaxInt64)
)

func init() {
	intPool.New = func() interface{} {
		return &big.Int{}
	}
}

// scaledValue 把给定未缩放值从 scale 换算到 newScale，并返回 int64。
// 缩小（scale down）时结果总是向上取整；最终结果可能溢出。
//
// scale 与 newScale 表示未缩放十进制数的标度。
// 该十进制数的数学值为 unscaled * 10**(-scale)。
func scaledValue(unscaled *big.Int, scale, newScale int) int64 {
	dif := scale - newScale
	if dif == 0 {
		return unscaled.Int64()
	}

	// 处理放大（scale up）
	// 这是简单情形，无需考虑舍入与溢出：
	// 任何中间运算若溢出，最终结果也会溢出。
	if dif < 0 {
		return unscaled.Int64() * int64(math.Pow10(-dif))
	}

	// 处理缩小（scale down）
	// 必须小心处理中间运算。

	// 当 unscaled < max.Int64 且 exp(10,dif) < max.Int64 时走快路径
	const log10MaxInt64 = 19
	if unscaled.Cmp(maxInt64) < 0 && dif < log10MaxInt64 {
		divide := int64(math.Pow10(dif))
		result := unscaled.Int64() / divide
		mod := unscaled.Int64() % divide
		if mod != 0 {
			// 有余数说明需要向上取整
			return result + 1
		}
		return result
	}

	// 仅在取得最终结果时才转回 int64，避免中间值溢出。
	divisor := intPool.Get().(*big.Int)
	exp := intPool.Get().(*big.Int)
	result := intPool.Get().(*big.Int)
	defer func() {
		intPool.Put(divisor)
		intPool.Put(exp)
		intPool.Put(result)
	}()

	// TODO: 若 Exp 开销过大，改用查找表。
	divisor.Exp(bigTen, exp.SetInt64(int64(dif)), nil)
	// 复用 exp 作为余数
	remainder := exp

	// result = unscaled / divisor（结果）
	// remainder = unscaled % divisor（余数）
	result.DivMod(unscaled, divisor, remainder)
	if remainder.Sign() != 0 {
		// 有余数，向上取整
		return result.Int64() + 1
	}

	return result.Int64()
}
