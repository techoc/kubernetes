/*
Copyright 2018 The Kubernetes Authors.

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

package options

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"

	// 提供参数注册函数的依赖库。
	"k8s.io/component-base/logs"
	"k8s.io/component-base/version/verflag"
)

// AddGlobalFlags 避免库的全局参数意外泄漏到 Kubelet 参数集合，并统一管理版本和日志参数。
func AddGlobalFlags(fs *pflag.FlagSet) {
	addCadvisorFlags(fs)
	addCredentialProviderFlags(fs)
	verflag.AddFlags(fs)
	logs.AddFlags(fs, logs.SkipLoggingConfigurationFlags())
}

// normalize 把下划线转成连字符；Kubelet 对外统一使用 kebab-case 参数名。
func normalize(s string) string {
	return strings.Replace(s, "_", "-", -1)
}

// register 从 Go flag.FlagSet 查找已有参数，并把同一个 Value 绑定到本地 pflag FlagSet。
func register(global *flag.FlagSet, local *pflag.FlagSet, globalName string) {
	if f := global.Lookup(globalName); f != nil {
		pflagFlag := pflag.PFlagFromGoFlag(f)
		pflagFlag.Name = normalize(pflagFlag.Name)
		local.AddFlag(pflagFlag)
	} else {
		panic(fmt.Sprintf("failed to find flag in global flagset (flag): %s", globalName))
	}
}

// addCredentialProviderFlags 为凭据提供程序预留独立参数集合，避免全局注册状态污染 Kubelet 参数。
func addCredentialProviderFlags(fs *pflag.FlagSet) {
	// 从全局 FlagSet 查找参数，再把 Value 重新绑定到本地参数集合。
	local := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)

	fs.AddFlagSet(local)
}
