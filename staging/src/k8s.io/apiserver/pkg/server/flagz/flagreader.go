/*
Copyright 2024 The Kubernetes Authors.

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

// Package flagz 把 apiserver 的命令行参数以只读 API 的形式暴露出来，
// 用于核对运行中实例的实际配置。
package flagz

import (
	"github.com/spf13/pflag"
	cliflag "k8s.io/component-base/cli/flag"
)

// Reader 抽象出“读取当前参数”的能力，便于测试替换数据源。
type Reader interface {
	GetFlagz() map[string]string
}

// NamedFlagSetsReader implements Reader for cliflag.NamedFlagSets
type NamedFlagSetsReader struct {
	FlagSets cliflag.NamedFlagSets
}

// GetFlagz 返回参数名到字符串值的映射。
func (n NamedFlagSetsReader) GetFlagz() map[string]string {
	return convertNamedFlagSetToFlags(&n.FlagSets)
}

// convertNamedFlagSetToFlags 遍历所有 flag set 收集参数值。
//
// 带 "classified" 注解的参数（如 token、证书路径等敏感值）一律替换为 "CLASSIFIED"，
// 避免通过 flagz API 泄漏凭据。
func convertNamedFlagSetToFlags(flagSets *cliflag.NamedFlagSets) map[string]string {
	flags := make(map[string]string)
	for _, fs := range flagSets.FlagSets {
		fs.VisitAll(func(flag *pflag.Flag) {
			if flag.Value != nil {
				value := flag.Value.String()
				if set, ok := flag.Annotations["classified"]; ok && len(set) > 0 {
					value = "CLASSIFIED"
				}
				flags[flag.Name] = value
			}
		})
	}

	return flags
}
