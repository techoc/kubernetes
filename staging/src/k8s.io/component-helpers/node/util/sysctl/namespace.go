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

// Package sysctl 提供 sysctl 参数名的命名空间判定。
//
// 判定结果用于按命名空间白名单过滤 kubelet 允许设置的 sysctl，
// 避免把具备节点级作用域的参数下放给 pod 设置。
package sysctl

import (
	"strings"
)

// Namespace 表示 Linux 内核命名空间的名称。
//
// 采用字符串类型而非枚举常量集合，是为了能在 map 键、白名单和日志中
// 直接使用内核文档里的命名空间名（如 "Net"）。
type Namespace string

// 这里只列举与 pod 级 sysctl 安全判定相关的命名空间：
// 其他内核命名空间的参数一律不允许在 pod 级别设置，因此无需在此建模。
const (
	// IPCNamespace 对应 Linux IPC 命名空间，涵盖信号量、共享内存和消息队列类 sysctl。
	// 参考 https://man7.org/linux/man-pages/man7/ipc_namespaces.7.html
	IPCNamespace = Namespace("IPC")

	// NetNamespace 对应网络命名空间，涵盖 net.* 系列 sysctl。
	// 参考 https://man7.org/linux/man-pages/man7/network_namespaces.7.html
	NetNamespace = Namespace("Net")

	// UTSNamespace 对应 UTS（主机名/域名）命名空间，例如 kernel.domainname。
	// 参考 https://man7.org/linux/man-pages/man7/uts_namespaces.7.html
	UTSNamespace = Namespace("UTS")

	// UserNamespace 对应 user 命名空间，例如 user.max_user_namespaces。
	// 参考 https://man7.org/linux/man-pages/man7/user_namespaces.7.html
	UserNamespace = Namespace("User")

	// UnknownNamespace 表示“未知命名空间”的零值。
	// 零值天然表示未识别：未列入白名单的参数默认拿不到任何命名空间授权。
	UnknownNamespace = Namespace("")
)

// nameToNamespace 是精确匹配表：每个条目把一个具体的 sysctl 名称映射到已知命名空间。
// 之所以单独列出精确项，是因为它们不服从所在前缀的通用规则，必须优先判定。
var nameToNamespace = map[string]Namespace{
	// UTS 命名空间参数：主机名/域名可以按 pod 隔离。
	"kernel.domainname": UTSNamespace,
	// 内核信号量参数：SEMMSL、SEMMNS、SEMOPM、SEMMNI。
	"kernel.sem": IPCNamespace,
	// 内核共享内存限制：shmall、shmmax、shmmni、shm_rmid_forced。
	"kernel.shmall":          IPCNamespace,
	"kernel.shmmax":          IPCNamespace,
	"kernel.shmmni":          IPCNamespace,
	"kernel.shm_rmid_forced": IPCNamespace,
	// kernel.shm* 的整类前缀项，兼容旧客户端查询这类参数的命名空间。
	"kernel.shm": IPCNamespace,
	// 内核消息队列参数：msgmni、msgmax、msgmnb。
	"kernel.msgmax": IPCNamespace,
	"kernel.msgmnb": IPCNamespace,
	"kernel.msgmni": IPCNamespace,
	// kernel.msg* 的整类前缀项，兼容旧客户端查询这类参数的命名空间。
	"kernel.msg": IPCNamespace,
}

// prefixToNamespace 是前缀匹配表：按整类参数（如 net.*）判定命名空间，
// 避免逐个枚举内核中可能出现的具体键。
var prefixToNamespace = map[string]Namespace{
	// 所有 net.* 参数都属于网络命名空间。
	"net": NetNamespace,
	// fs.mqueue 文件系统提供实现 POSIX 消息队列 API 所需的内核特性，
	// 其 sysctl 同样受 IPC 命名空间约束。
	"fs.mqueue": IPCNamespace,
	// user 命名空间下的 sysctl。
	"user": UserNamespace,
}

// namespaceOf 返回给定 sysctl 所属的 Linux 内核命名空间；
// 对未知（非命名空间化）的参数返回 UnknownNamespace。
//
// 注意本函数只返回命名空间；是否按前缀匹配由调用方 GetNamespace 另行判定。
// 先查精确表再查前缀表：精确条目（如 kernel.sem）属于特例，
// 若改成前缀规则会误伤同前缀的其他参数。
func namespaceOf(val string) Namespace {
	if ns, found := nameToNamespace[val]; found {
		return ns
	}
	// 比较时补上 "."，避免 "net" 误匹配 "netfilter.*" 之类不相关的键。
	for p, ns := range prefixToNamespace {
		if strings.HasPrefix(val, p+".") {
			return ns
		}
	}
	return UnknownNamespace
}

// GetNamespace 从 sysctl 字符串中提取命名空间信息。
// 它返回三个值：ns 是参数所属的命名空间（IPC、Net、UTS、User 或 Unknown）；
// sysctlOrPrefix 是截掉第一个 '*' 及其后内容后的参数名（无 '*' 时即原名）；
// prefixed 表示参数是否使用了通配前缀（含 '*'），多数情况下 '*' 位于末尾。
//
// 例如输入 "net.ipv6.neigh.*"，返回 NetNamespace、"net.ipv6.neigh"、true；
// 输入 "net.ipv6.conf.all.disable_ipv6"，返回 NetNamespace、原名、false。
//
// 带 '*' 的参数必须先截掉通配部分再判命名空间：白名单按前缀授权，
// 判定依据应是匹配的前缀，而不是带通配符的原始串。
func GetNamespace(sysctl string) (ns Namespace, sysctlOrPrefix string, prefixed bool) {
	// 先归一化名称（如替换分隔符），保证后续匹配基于统一格式。
	sysctlOrPrefix = NormalizeName(sysctl)
	firstIndex := strings.IndexAny(sysctlOrPrefix, "*")
	if firstIndex != -1 {
		sysctlOrPrefix = sysctlOrPrefix[:firstIndex]
		prefixed = true
	}
	ns = namespaceOf(sysctlOrPrefix)
	return
}
