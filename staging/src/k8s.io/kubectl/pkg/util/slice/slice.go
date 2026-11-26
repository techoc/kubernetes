/*
Copyright 2017 The Kubernetes Authors.

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

package slice

import "slices"

// 按升序排列 []int64。
//
// Deprecated: Use slices.Sort instead.
//
//go:fix inline
func SortInts64(a []int64) { slices.Sort(a) }

// 判断切片是否包含给定元素。
// 提供修改函数时，比较前先对切片元素应用它。
//
// Deprecated: Use slices.Contains or slices.ContainsFunc instead.
func Contains[T comparable](slice []T, s T, modifier func(s T) T) bool {
	if slices.Contains(slice, s) {
		return true
	}
	if modifier != nil {
		return slices.ContainsFunc(slice, func(item T) bool { return modifier(item) == s })
	}
	return false
}

// 判断字符串切片是否包含给定字符串。
// 提供修改函数时，比较前先对切片元素应用它。
//
// Deprecated: Use slices.Contains or slices.ContainsFunc instead.
//
//go:fix inline
func ContainsString(slice []string, s string, modifier func(s string) string) bool {
	return Contains(slice, s, modifier)
}

// ToSet 合并一个或多个切片并去重；结果顺序
// 不作保证。
func ToSet[T comparable](slices ...[]T) []T {
	if len(slices) == 0 {
		return nil
	}
	m := map[T]struct{}{}
	for _, slice := range slices {
		for _, value := range slice {
			m[value] = struct{}{}
		}
	}
	result := []T{}
	for k := range m {
		result = append(result, k)
	}
	return result
}
