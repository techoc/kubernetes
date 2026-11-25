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

package kubeadm

// GetArgValue 从后向前遍历参数切片，返回指定参数名的值及其索引。
// 参数不存在时返回空字符串和 -1。
// startIdx 指定遍历起点；若为负数或大于参数切片长度，则从最后一个元素开始。
func GetArgValue(args []Arg, name string, startIdx int) (string, int) {
	if startIdx < 0 || startIdx > len(args)-1 {
		startIdx = len(args) - 1
	}
	for i := startIdx; i >= 0; i-- {
		arg := args[i]
		if arg.Name == name {
			return arg.Value, i
		}
	}
	return "", -1
}

// SetArgValues 更新一个或多个同名参数的值；不存在时追加新参数。
// 函数从后向前遍历参数列表。
// nArgs 指定要更新的同名参数数量；小于 1 时更新全部。
func SetArgValues(args []Arg, name, value string, nArgs int) []Arg {
	var count int
	var found bool
	for i := len(args) - 1; i >= 0; i-- {
		if args[i].Name == name {
			found = true
			args[i].Value = value
			if nArgs < 1 {
				continue
			}
			count++
			if count >= nArgs {
				return args
			}
		}
	}
	if found {
		return args
	}
	args = append(args, Arg{Name: name, Value: value})
	return args
}

// MergeArgWithBase 根据参数的 MergeMethod 合并基础值和参数值；未指定合并方式时执行替换。
func MergeArgWithBase(base string, arg Arg) string {
	switch arg.MergeMethod {
	case ArgMergeMethodAppend:
		return base + arg.Value
	case ArgMergeMethodPrepend:
		return arg.Value + base
	default:
		return arg.Value
	}
}
