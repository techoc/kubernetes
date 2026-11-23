/*
Copyright 2015 The Kubernetes Authors.

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
	"os"
	"path"
	"strconv"
	"strings"
)

// 本文件集中定义 kubelet 启动时要写的系统参数。
// 选择这些值是为了让容器密集运行的节点不会因为内核默认限制而过早出问题；
// 常量集中放在这里，避免同一取值散落在代码各处后各自漂移。
const (
	// sysctlBase 是 procfs 中 sysctl 文件所在目录的前缀。
	sysctlBase = "/proc/sys"
	// VMOvercommitMemory 是内核内存过量提交（overcommit）策略对应的 sysctl 变量名。
	VMOvercommitMemory = "vm/overcommit_memory"
	// VMPanicOnOOM 是内核 OOM 行为对应的 sysctl 变量名。
	VMPanicOnOOM = "vm/panic_on_oom"
	// KernelPanic 是内核 panic 后重启等待秒数对应的 sysctl 变量名。
	KernelPanic = "kernel/panic"
	// KernelPanicOnOops 是内核遇到 oops 或 BUG 时行为对应的 sysctl 变量名。
	KernelPanicOnOops = "kernel/panic_on_oops"
	// RootMaxKeys 是 root 用户（root user namespace 中的 UID 0）可持有的 key 数量上限对应的 sysctl 变量名。
	RootMaxKeys = "kernel/keys/root_maxkeys"
	// RootMaxBytes 是 root 用户（root user namespace 中的 UID 0）所持 key 的载荷字节总数上限对应的 sysctl 变量名。
	RootMaxBytes = "kernel/keys/root_maxbytes"

	// VMOvercommitMemoryAlways 表示内核对内存过量提交不做任何限制（总是允许）。
	VMOvercommitMemoryAlways = 1
	// VMPanicOnOOMInvokeOOMKiller 表示发生 OOM 时内核调用 oom_killer 进行处理。
	VMPanicOnOOMInvokeOOMKiller = 0

	// KernelPanicOnOopsAlways 表示内核遇到 oops 时直接触发 panic。
	KernelPanicOnOopsAlways = 1
	// KernelPanicRebootTimeout 是内核 panic 后重启前的等待秒数。
	KernelPanicRebootTimeout = 10

	// RootMaxKeysSetting 是 root 用户可持有的 key 数量上限。
	// Docker 会为每个容器创建一个 key，内核默认额度很快会被耗尽，
	// 因此按“每容器一个 key”的规模预留充足余量。
	RootMaxKeysSetting = 1000000
	// RootMaxBytesSetting 是 root 用户所持 key 的载荷字节总数上限。
	// 按每个 key 平均 25 字节乘以 key 数量上限推算，
	// 保证提高 key 数量后总字节额度同步放大。
	RootMaxBytesSetting = RootMaxKeysSetting * 25
)

// Interface 抽象了 sysctl 的读写能力，便于注入。
// 调用方在测试中可替换为 mock 实现，而不必依赖真实的 /proc/sys 文件。
type Interface interface {
	// GetSysctl 读取指定 sysctl 项的整数值。
	GetSysctl(sysctl string) (int, error)
	// SetSysctl 把指定 sysctl 项修改为 newVal。
	SetSysctl(sysctl string, newVal int) error
}

// New 返回一个访问真实 sysctl 的 Interface 实现（直连 procfs）。
// 返回接口而非具体类型：生产代码默认拿到真实实现，测试时替换为 mock 即可，
// 调用方无需改动。
func New() Interface {
	return &procSysctl{}
}

// procSysctl 通过读写 /proc/sys 下的文件来实现 Interface。
type procSysctl struct {
}

// GetSysctl 从 /proc/sys 读取指定 sysctl 项并解析为整数。
// 文件读取或数值转换失败时返回 -1 和对应错误。
func (*procSysctl) GetSysctl(sysctl string) (int, error) {
	data, err := os.ReadFile(path.Join(sysctlBase, sysctl))
	if err != nil {
		return -1, err
	}
	// procfs 的读数带尾随换行，必须去掉后才能转成整数。
	val, err := strconv.Atoi(strings.Trim(string(data), " \n"))
	if err != nil {
		return -1, err
	}
	return val, nil
}

// SetSysctl 把整数值 newVal 写入 /proc/sys 下对应的 sysctl 文件。
func (*procSysctl) SetSysctl(sysctl string, newVal int) error {
	return os.WriteFile(path.Join(sysctlBase, sysctl), []byte(strconv.Itoa(newVal)), 0640)
}

// NormalizeName 把 sysctl 变量名统一成以 "." 分隔的写法。
// 输入也接受以 "/" 分隔的路径写法；先归一化再校验，避免两种写法产生分歧。
// 例如 "net/ipv4/ip_local_port_range" 会转成 "net.ipv4.ip_local_port_range"。
// 更多背景见：
//
//	https://man7.org/linux/man-pages/man8/sysctl.8.html
//	https://man7.org/linux/man-pages/man5/sysctl.d.5.html
func NormalizeName(val string) string {
	if val == "" {
		return val
	}
	firstSepIndex := strings.IndexAny(val, "./")
	// 用第一个分隔符决定命名风格：若已经是点号开头，说明名称遵循点号约定
	//（如 net.ipv4.conf.eno2/100.rp_filter），其中的斜杠属于接口名本身，
	// 不能替换，否则会改错对象；没有分隔符时也无需转换。
	if firstSepIndex == -1 || val[firstSepIndex] == '.' {
		return val
	}

	// 斜杠开头的名称来自 sysctl 文件路径写法，统一交换 "." 与 "/"；
	// 例如 net/ipv4/conf/eno2.100/rp_filter 转成 net.ipv4.conf.eno2/100.rp_filter。
	// 采用交换而非单向替换，才能同时保护接口名中的点号（如 eno2.100）。
	f := func(r rune) rune {
		switch r {
		case '.':
			return '/'
		case '/':
			return '.'
		}
		return r
	}
	return strings.Map(f, val)
}
