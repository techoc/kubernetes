//go:build linux

/*
Copyright The Kubernetes Authors.

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
	"slices"
	"testing"

	"github.com/google/cadvisor/lib/cadvisorflags"
	"github.com/spf13/pflag"
)

// TestPinnedCadvisorFlagsResolve 检查 Kubelet 依赖的 cAdvisor 参数仍注册在库的全局 FlagSet，避免节点启动时 panic。
func TestPinnedCadvisorFlagsResolve(t *testing.T) {
	names := cadvisorflags.Kept()
	if len(names) == 0 {
		t.Fatal("cadvisorflags exposes no pinned flag names; expected at least housekeeping_interval")
	}
	for _, name := range names {
		if flag.CommandLine.Lookup(name) == nil {
			t.Errorf("cAdvisor flag %q is pinned by the kubelet (cadvisorflags) but is not registered on the global flag set; update github.com/google/cadvisor/lib or its cadvisorflags package", name)
		}
	}
}

// TestAddCadvisorFlagsDoesNotPanic 直接执行真实参数注册流程，确保 cAdvisor 参数迁移不会在启动时触发 panic。
func TestAddCadvisorFlagsDoesNotPanic(t *testing.T) {
	addCadvisorFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))
}

// TestKeptIncludesHousekeepingInterval 保证显式绑定的 housekeeping_interval 仍被 cAdvisor 列为兼容保留参数。
func TestKeptIncludesHousekeepingInterval(t *testing.T) {
	if !slices.Contains(cadvisorflags.Kept(), cadvisorflags.HousekeepingInterval) {
		t.Errorf("cadvisorflags.Kept() must include HousekeepingInterval (%q); addCadvisorFlags binds it as a normal kubelet flag", cadvisorflags.HousekeepingInterval)
	}
}
