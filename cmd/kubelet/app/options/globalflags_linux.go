//go:build linux

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
	"os"

	"github.com/spf13/pflag"

	"github.com/google/cadvisor/lib/cadvisorflags"

	// cAdvisor 将 housekeeping_interval 注册到 Go 全局 FlagSet。
	_ "github.com/google/cadvisor/lib/manager"
)

// addCadvisorFlags 只在 Linux 上注册 cAdvisor 的 housekeeping_interval 参数，供 Kubelet 兼容历史节点参数。
func addCadvisorFlags(fs *pflag.FlagSet) {
	// cAdvisor 把参数注册在 Go 全局 FlagSet；这里将同一 Value 重新绑定到 Kubelet 自己的 FlagSet。
	global := flag.CommandLine
	local := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)

	// 节点 e2e 测试依赖此参数。
	register(global, local, cadvisorflags.HousekeepingInterval)

	// 最后将 cAdvisor 参数加入调用方传入的 FlagSet。
	fs.AddFlagSet(local)
}
