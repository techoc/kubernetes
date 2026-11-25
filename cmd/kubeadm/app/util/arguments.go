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

package util

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// ArgumentsToCommand 接收基础参数和可选覆盖参数两个 Arg 切片。返回值中基础参数在前。
// 根据 MergeMethod，覆盖参数可追加到基础参数之前或之后，也可替换同名基础参数。
func ArgumentsToCommand(base, overrides []kubeadmapi.Arg) []string {
	// 仅排序基础参数。
	sortArgsSlice(&base)

	// 收集 "replace" 类型的覆盖参数。
	overrideArgs := make(map[string]kubeadmapi.Arg, len(overrides))
	tail := make([]string, 0, len(overrides))
	for _, arg := range overrides {
		overrideArgs[arg.Name] = arg
		if arg.MergeMethod == "" {
			tail = append(tail, fmt.Sprintf("--%s=%s", arg.Name, arg.Value))
		}
	}

	command := make([]string, 0, len(base)+len(tail))
	for _, arg := range base {
		ov, ok := overrideArgs[arg.Name]
		switch {
		case !ok:
			// 基础参数不变。
		case ov.MergeMethod == "":
			continue
		default:
			arg.Value = kubeadmapi.MergeArgWithBase(arg.Value, ov)
		}
		command = append(command, fmt.Sprintf("--%s=%s", arg.Name, arg.Value))
	}

	return append(command, tail...)
}

// ArgumentsFromCommand 将 "--foo=bar" 形式的 CLI 命令解析为 Arg 切片。
// 主要用于解析 kubeadm-flags.env，但某些版本可能暂不使用。
func ArgumentsFromCommand(command []string) []kubeadmapi.Arg {
	args := []kubeadmapi.Arg{}
	for i, arg := range command {
		key, val, err := parseArgument(arg)

		// 首个参数不符合条件时忽略，它通常是二进制名称。
		// 其他情况仅警告而不报错；这通常意味着用户手工编辑过参数列表，可能清楚自己的修改。
		if err != nil {
			if i != 0 {
				klog.Warningf("[kubeadm] WARNING: The component argument %q could not be parsed correctly. The argument must be of the form %q. Skipping...\n", arg, "--")
			}
			continue
		}

		args = append(args, kubeadmapi.Arg{Name: key, Value: val})
	}

	sortArgsSlice(&args)

	return args
}

// parseArgument 将 "--foo=bar" 解析为 "foo" 和 "bar"。
func parseArgument(arg string) (string, string, error) {
	if !strings.HasPrefix(arg, "--") {
		return "", "", errors.New("the argument should start with '--'")
	}
	if !strings.Contains(arg, "=") {
		return "", "", errors.New("the argument should have a '=' between the flag and the value")
	}
	// 去掉开头的 --。
	arg = strings.TrimPrefix(arg, "--")
	// 按 = 分割；仅取前两段，因为只需要键和值，值本身也可包含 =。
	keyvalSlice := strings.SplitN(arg, "=", 2)

	// 确保键和值均存在。
	if len(keyvalSlice) != 2 {
		return "", "", errors.New("the argument must have both a key and a value")
	}
	if len(keyvalSlice[0]) == 0 {
		return "", "", errors.New("the argument must have a key")
	}

	return keyvalSlice[0], keyvalSlice[1], nil
}

// sortArgsSlice 按字母和数字顺序排序 Args 切片。
func sortArgsSlice(argsPtr *[]kubeadmapi.Arg) {
	args := *argsPtr
	sort.Slice(args, func(i, j int) bool {
		if args[i].Name == args[j].Name {
			return args[i].Value < args[j].Value
		}
		return args[i].Name < args[j].Name
	})
}
