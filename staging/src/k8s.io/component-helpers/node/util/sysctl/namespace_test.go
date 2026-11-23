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

// TestNamespacedOf 用几个代表性前缀锁住命名空间推断规则：
// 内核参数按前缀分流到 IPC/Net/UTS/User 命名空间，未知前缀必须落回 Unknown，
// 否则调用方会误以为它属于某个已知的 pod 级命名空间。
func TestNamespacedOf(t *testing.T) {
	// 各用例含义（按 map 中的键说明）：
	// kernel.shm_rmid_forced 是精确表条目，共享内存强制删除标记归 IPC 命名空间；
	// net.a.b.c 是前缀表条目，net.* 归网络命名空间；
	// fs.mqueue.a.b.c 是前缀表条目，POSIX 消息队列归 IPC 命名空间；
	// kernel.domainname 是精确表条目，域名归 UTS 命名空间；
	// user.max_user_namespaces 是前缀表条目，user.* 归 user 命名空间；
	// foo 是兜底场景：不在白名单中的前缀必须返回 Unknown，防止越权放行。
	tests := map[string]Namespace{
		"kernel.shm_rmid_forced":   IPCNamespace,
		"net.a.b.c":                NetNamespace,
		"fs.mqueue.a.b.c":          IPCNamespace,
		"kernel.domainname":        UTSNamespace,
		"user.max_user_namespaces": UserNamespace,
		"foo":                      UnknownNamespace,
	}

	// 逐项调用内部函数并核对命名空间；map 遍历顺序无关紧要，各用例彼此独立。
	for sysctl, ns := range tests {
		if got := namespaceOf(sysctl); got != ns {
			// 判定错误会直接影响 pod 级 sysctl 白名单的放行结果。
			t.Errorf("wrong namespace for %q: got=%s want=%s", sysctl, got, ns)
		}
	}
}
