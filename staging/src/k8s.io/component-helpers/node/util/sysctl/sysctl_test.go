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

package sysctl

import (
	"testing"
)

// TestConvertSysctlVariableToDotsSeparator 验证 NormalizeName 能把 sysctl 变量名
// 正确转换为以点号分隔的形式，无论输入本身采用哪种分隔风格。
func TestConvertSysctlVariableToDotsSeparator(t *testing.T) {
	// testCase 描述一组输入与期望输出：in 是原始名称，out 是归一化后的名称。
	type testCase struct {
		in  string
		out string
	}
	// 用例覆盖两种命名风格，尤其是接口名含斜杠或点号的情形，
	// 用来守住“不能盲目替换所有分隔符”这条归一化规则。
	valid := []testCase{
		// 已是点号写法：原样返回。
		{in: "kernel.shm_rmid_forced", out: "kernel.shm_rmid_forced"},
		// 斜杠写法：分隔符整体换成点号。
		{in: "kernel/shm_rmid_forced", out: "kernel.shm_rmid_forced"},
		// 点号写法且接口名本身含斜杠（eno2/100）：斜杠必须保留。
		{in: "net.ipv4.conf.eno2/100.rp_filter", out: "net.ipv4.conf.eno2/100.rp_filter"},
		// 斜杠写法且接口名本身含点号（eno2.100）：交换分隔符后点号应落在接口名里。
		{in: "net/ipv4/conf/eno2.100/rp_filter", out: "net.ipv4.conf.eno2/100.rp_filter"},
		// 普通路径写法：逐段转换。
		{in: "net/ipv4/ip_local_port_range", out: "net.ipv4.ip_local_port_range"},
		// 精确 sysctl 名的路径写法。
		{in: "kernel/msgmax", out: "kernel.msgmax"},
		{in: "kernel/sem", out: "kernel.sem"},
	}

	// 逐组执行归一化并比对；一旦转换错误，后续白名单判定会拿错误名称查表。
	for _, test := range valid {
		convertSysctlVal := NormalizeName(test.in)
		if test.out != convertSysctlVal {
			t.Errorf("The sysctl variable was not converted correctly. got: %s, want: %s", convertSysctlVal, test.out)
		}
	}
}
