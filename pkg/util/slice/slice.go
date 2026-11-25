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

// Package slice 提供切片常用操作的工具函数。
package slice

import (
	"slices"
)

// CopyStrings 将指定字符串切片的内容复制到一个新切片中。
//
// Deprecated: 请改用 slices.Clone。
//
//go:fix inline
func CopyStrings(s []string) []string {
	return slices.Clone(s)
}

// SortStrings 原地排序指定的字符串切片。
// 返回传入的同一个切片，方便链式调用。
//
// Deprecated: 请改用 slices.Sort。
func SortStrings(s []string) []string {
	slices.Sort(s)
	return s
}

// ContainsString 检查给定的字符串切片是否包含指定的字符串。
// 如果提供了 modifier 函数，比较前会先用它转换切片中的元素。
//
// Deprecated: 请改用 slices.ContainsFunc 或 slices.Contains。
func ContainsString(slice []string, s string, modifier func(s string) string) bool {
	if slices.Contains(slice, s) {
		return true
	}
	if modifier != nil {
		return slices.ContainsFunc(slice, func(item string) bool { return modifier(item) == s })
	}
	return false
}

// RemoveString 返回一个新切片，包含 slice 中所有既不等于 s、
// 也不在经 modifier 转换后等于 s 的元素（若提供了 modifier）。
func RemoveString(slice []string, s string, modifier func(s string) string) []string {
	newSlice := make([]string, 0)
	for _, item := range slice {
		if item == s {
			continue
		}
		if modifier != nil && modifier(item) == s {
			continue
		}
		newSlice = append(newSlice, item)
	}
	if len(newSlice) == 0 {
		// 归一化为 nil，这样单测里就不必区分空切片和 nil。
		newSlice = nil
	}
	return newSlice
}
