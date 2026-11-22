/*
Copyright 2014 The Kubernetes Authors.

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

package flag

import (
	goflag "flag"
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

// underscoreWarnings 记录已经告警过的下划线 flag，避免重复刷日志。
// pflag 在解析过程中可能多次触发 normalize，加缓存后每个 flag 只提示一次。
var underscoreWarnings = make(map[string]struct{})

// WordSepNormalizeFunc 把 flag 名中的 "_" 统一替换为 "-"。
// Kubernetes 约定使用连字符，但历史用户脚本可能仍写下划线，静默归一化以保持兼容。
func WordSepNormalizeFunc(f *pflag.FlagSet, name string) pflag.NormalizedName {
	if strings.Contains(name, "_") {
		return pflag.NormalizedName(strings.Replace(name, "_", "-", -1))
	}
	return pflag.NormalizedName(name)
}

// WarnWordSepNormalizeFunc 与 WordSepNormalizeFunc 相同地归一化下划线 flag，
// 但会额外告警，用于提示用户迁移到连字符写法。
func WarnWordSepNormalizeFunc(f *pflag.FlagSet, name string) pflag.NormalizedName {
	if strings.Contains(name, "_") {
		nname := strings.Replace(name, "_", "-", -1)
		if _, alreadyWarned := underscoreWarnings[name]; !alreadyWarned {
			klog.Warningf("using an underscore in a flag name is not supported. %s has been converted to %s.", name, nname)
			underscoreWarnings[name] = struct{}{}
		}

		return pflag.NormalizedName(nname)
	}
	return pflag.NormalizedName(name)
}

// InitFlags 规范化、解析并记录命令行 flag。
// 组件在 main 中调用一次：先把标准库 flag 合并进 pflag，再统一解析，
// 最后以 V(2) 级别打印实际值，便于排查启动参数问题。
func InitFlags() {
	pflag.CommandLine.SetNormalizeFunc(WordSepNormalizeFunc)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
	pflag.VisitAll(func(flag *pflag.Flag) {
		klog.V(2).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})
}

// PrintFlags 以 V(1) 级别记录给定 flag set 中的所有 flag。
// 用于只注册了部分 flag set、未走 InitFlags 的组件。
func PrintFlags(flags *pflag.FlagSet) {
	flags.VisitAll(func(flag *pflag.Flag) {
		klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})
}
