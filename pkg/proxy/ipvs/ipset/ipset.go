//go:build linux

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

package ipset

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
	netutils "k8s.io/utils/net"
)

// 包级哨兵错误，统一封装校验失败原因，便于在日志中检索。
var validationError = fmt.Errorf("failed to validate entry for ipset")

// Interface 是执行 ipset 命令的可注入接口。实现必须保证 goroutine 安全，
// 因为 kube-proxy 会从多个 sync 循环并发调用它。
type Interface interface {
	// FlushSet 删除指定集合中的所有条目。
	FlushSet(set string) error
	// DestroySet 删除指定的集合。
	DestroySet(set string) error
	// DestroyAllSets 删除内核中的全部 ipset 集合。
	DestroyAllSets() error
	// CreateSet 创建一个新集合；ignoreExistErr=true 时忽略“集合已存在”错误，
	// 这样重复调用创建逻辑时无需先查询集合是否存在。
	CreateSet(set *IPSet, ignoreExistErr bool) error
	// AddEntry 向指定集合添加条目；ignoreExistErr=true 时忽略“条目已存在”错误，
	// 使同步逻辑可以幂等地反复执行。
	AddEntry(entry string, set *IPSet, ignoreExistErr bool) error
	// DelEntry 从指定集合中删除一个条目。
	DelEntry(entry string, set string) error
	// TestEntry 检查指定条目是否存在于集合中。
	TestEntry(entry string, set string) (bool, error)
	// ListEntries 列出指定集合中的所有条目。
	ListEntries(set string) ([]string, error)
	// ListSets 列出内核中的所有集合名称。
	ListSets() ([]string, error)
	// GetVersion 返回 ipset 的版本字符串，格式为 "X.Y"。
	GetVersion() (string, error)
}

// IPSetCmd 是 ipset 工具的命令名，kube-proxy 通过执行命令而非 netlink 操作 ipset。
const IPSetCmd = "ipset"

// EntryMemberPattern 用于从 `ipset list {set}` 的输出中剥离头部信息，只保留成员列表。
// ipset 命令 `ipset list {set}` 的原始输出类似：
// Name: foobar
// Type: hash:ip,port
// Revision: 2
// Header: family inet hashsize 1024 maxelem 65536
// Size in memory: 16592
// References: 0
// Members:
// 192.168.1.2,tcp:8080
// 192.168.1.1,udp:53
// 该正则贪婪地吞掉从行首到 Members: 行的内容，便于剥离头部。
var EntryMemberPattern = "(?m)^(.*\n)*Members:\n"

// VersionPattern 用于从 ipset 版本输出（形如 "v6.10"）中提取版本号。
// 版本号仅提取主/次版本号，忽略前缀 v 和其余文本。
var VersionPattern = "v[0-9]+\\.[0-9]+"

// IPSet 描述一个 ipset 集合的配置。
type IPSet struct {
	// Name 是集合名称。
	Name string
	// SetType 指定 ipset 类型。
	SetType Type
	// HashFamily 指定集合中 IP 地址所属协议族，
	// 默认为 inet（即 IPv4）；若需 IPv6，用户必须显式指定 inet6。
	HashFamily string
	// HashSize 指定 ipset 的哈希表大小。
	HashSize int
	// MaxElem 指定 ipset 可容纳的最大元素数。
	MaxElem int
	// PortRange 指定 bitmap:port 类型集合的端口范围。
	PortRange string
	// Comment 是创建集合时附带的注释信息。
	Comment string
}

// Validate 校验集合配置是否合法，在创建集合前调用以避免把非法参数传给 ipset 命令。
func (set *IPSet) Validate() error {
	// 对于 hash:ip,port 等包含协议族的类型，先校验协议族是否被 ipset 支持。
	if set.SetType == HashIPPort || set.SetType == HashIPPortIP || set.SetType == HashIPPortNet {
		if err := validateHashFamily(set.HashFamily); err != nil {
			return err
		}
	}
	// 校验集合类型是否在支持的类型列表中。
	if err := validateIPSetType(set.SetType); err != nil {
		return err
	}
	// bitmap 类型必须提供合法的端口范围，否则 ipset 无法创建集合。
	if set.SetType == BitmapPort {
		if err := validatePortRange(set.PortRange); err != nil {
			return err
		}
	}
	// 哈希表大小必须为正，0 或负数会导致内核拒绝创建。
	if set.HashSize <= 0 {
		return fmt.Errorf("invalid HashSize: %d", set.HashSize)
	}
	// 最大元素数必须为正，否则内核无法分配容量。
	if set.MaxElem <= 0 {
		return fmt.Errorf("invalid MaxElem %d", set.MaxElem)
	}

	// 全部字段合法才可能被内核接受。
	return nil
}

// setIPSetDefaults 为未设置的字段填充默认值，
// 保证后续 Validate 和命令拼装拿到的是完整配置。
func (set *IPSet) setIPSetDefaults() {
	// 仅对未显式设置（零值）的字段填默认值，不覆盖用户配置。
	if set.HashSize == 0 {
		set.HashSize = 1024
	}
	if set.MaxElem == 0 {
		set.MaxElem = 65536
	}
	// 默认协议族为 IPv4。
	if set.HashFamily == "" {
		set.HashFamily = ProtocolFamilyIPV4
	}
	// 默认集合类型为 "hash:ip,port"。
	if len(set.SetType) == 0 {
		set.SetType = HashIPPort
	}
	if len(set.PortRange) == 0 {
		set.PortRange = DefaultPortRange
	}
}

// Entry 表示 ipset 中的一条成员记录。
type Entry struct {
	// IP 是条目的 IP 地址，其协议族须与所属 IPSet 的 HashFamily 一致；
	// 同一集合中所有条目的 IP 协议相同（IPv4 或 IPv6）。
	IP string
	// Port 是条目的端口号。
	Port int
	// Protocol 是条目的协议，同一集合中的协议一致，
	// 仅接受 TCP、UDP 和 SCTP。
	Protocol string
	// Net 是条目的 IP 网段；前缀长度为 0 的网络地址不能存入集合。
	Net string
	// IP2 是条目的第二个 IP；hash:ip,port,ip 类型集合要求其非空。
	IP2 string
	// SetType 是该条目所属 ipset 的类型，决定 String() 的输出格式。
	SetType Type
}

// Validate 校验条目是否合法；set 参数提供条目所属集合的配置（如端口范围）。
// 返回 false 表示条目不能被加入集合，调用方应跳过该条目。
func (e *Entry) Validate(set *IPSet) bool {
	if e.Port < 0 {
		klog.ErrorS(validationError, "port number should be >=0", "entry", e, "port", e.Port, "ipset", set)
		return false
	}
	switch e.SetType {
	case HashIP:
		// 校验 hash:ip 类型条目的 IP 是否合法。
		if valid := e.checkIP(set); !valid {
			return false
		}
	case HashIPPort:
		// 校验条目的 IP 和协议是否合法。
		if valid := e.checkIPandProtocol(set); !valid {
			return false
		}
	case HashIPPortIP:
		// 校验条目的 IP 和协议是否合法。
		if valid := e.checkIPandProtocol(set); !valid {
			return false
		}

		// hash:ip,port,ip 类型的 IP2 不能为空，否则无法表达第二个 IP。
		if netutils.ParseIPSloppy(e.IP2) == nil {
			klog.ErrorS(validationError, "error parsing second ip address", "entry", e, "ip", e.IP2, "ipset", set)
			return false
		}
	case HashIPPortNet:
		// 校验条目的 IP 和协议是否合法。
		if valid := e.checkIPandProtocol(set); !valid {
			return false
		}

		// hash:ip,port,net 类型的 Net 必须是合法 CIDR。
		if _, ipNet, err := netutils.ParseCIDRSloppy(e.Net); ipNet == nil {
			klog.ErrorS(err, "error parsing ip net", "entry", e, "net", e.Net, "set", set)
			return false
		}
	case BitmapPort:
		// 校验条目端口是否落在其 bitmap:port 集合声明的端口范围内。
		if set == nil {
			klog.ErrorS(validationError, "unable to reference ip set where the entry exists", "entry", e)
			return false
		}
		begin, end, err := parsePortRange(set.PortRange)
		if err != nil {
			klog.ErrorS(err, "failed to parse set port range", "ipset", set, "portRange", set.PortRange)
			return false
		}
		if e.Port < begin || e.Port > end {
			klog.ErrorS(validationError, "port number is not in the port range of its ipset", "entry", e, "port", e.Port, "portRange", set.PortRange, "ipset", set)
			return false
		}
	}

	return true
}

// String 将 ipset 条目按集合类型格式化为 ipset 命令可识别的字符串。
func (e *Entry) String() string {
	switch e.SetType {
	case HashIP:
		// Entry{192.168.1.1} -> 192.168.1.1
		return fmt.Sprintf("%s", e.IP)
	case HashIPPort:
		// Entry{192.168.1.1, udp, 53} -> 192.168.1.1,udp:53
		// Entry{192.168.1.2, tcp, 8080} -> 192.168.1.2,tcp:8080
		return fmt.Sprintf("%s,%s:%s", e.IP, e.Protocol, strconv.Itoa(e.Port))
	case HashIPPortIP:
		// Entry{192.168.1.1, udp, 53, 10.0.0.1} -> 192.168.1.1,udp:53,10.0.0.1
		// Entry{192.168.1.2, tcp, 8080, 192.168.1.2} -> 192.168.1.2,tcp:8080,192.168.1.2
		return fmt.Sprintf("%s,%s:%s,%s", e.IP, e.Protocol, strconv.Itoa(e.Port), e.IP2)
	case HashIPPortNet:
		// Entry{192.168.1.2, udp, 80, 10.0.1.0/24} -> 192.168.1.2,udp:80,10.0.1.0/24
		// Entry{192.168.2,25, tcp, 8080, 10.1.0.0/16} -> 192.168.2,25,tcp:8080,10.1.0.0/16
		return fmt.Sprintf("%s,%s:%s,%s", e.IP, e.Protocol, strconv.Itoa(e.Port), e.Net)
	case BitmapPort:
		// Entry{53} -> 53
		// Entry{8080} -> 8080
		return strconv.Itoa(e.Port)
	}
	return ""
}

// checkIPandProtocol 校验条目的 IP 和协议是否合法。
func (e *Entry) checkIPandProtocol(set *IPSet) bool {
	// 协议为空时默认使用 tcp，与内核 ipset 的默认行为保持一致。
	if len(e.Protocol) == 0 {
		e.Protocol = ProtocolTCP
	} else if !validateProtocol(e.Protocol) {
		return false
	}
	return e.checkIP(set)
}

// checkIP 校验条目的 IP 能否被解析为合法 IP 地址。
func (e *Entry) checkIP(set *IPSet) bool {
	if netutils.ParseIPSloppy(e.IP) == nil {
		klog.ErrorS(validationError, "error parsing ip address", "entry", e, "ip", e.IP, "ipset", set)
		return false
	}

	return true
}

type runner struct {
	exec utilexec.Interface
}

// New 返回一个通过执行 ipset 命令工作的 Interface 实现。
func New() Interface {
	return &runner{
		exec: utilexec.New(),
	}
}

// CreateSet 创建一个新集合；ignoreExistErr=true 时忽略“集合已存在”错误，
// 先填默认值再校验，避免无效配置进入命令层。
func (runner *runner) CreateSet(set *IPSet, ignoreExistErr bool) error {
	// 未显式设置的字段先填充默认值。
	set.setIPSetDefaults()

	// 创建前校验参数，避免把非法参数传给 ipset 命令后难以定位错误。
	if err := set.Validate(); err != nil {
		return err
	}
	return runner.createSet(set, ignoreExistErr)
}

// ignoreExistErr 为 true 时附加 -exist 选项：
// 当同名集合（集合名与创建参数完全一致）已存在时，ipset 会忽略原本报错。
func (runner *runner) createSet(set *IPSet, ignoreExistErr bool) error {
	args := []string{"create", set.Name, string(set.SetType)}
	if set.SetType == HashIPPortIP || set.SetType == HashIPPort || set.SetType == HashIPPortNet || set.SetType == HashIP {
		args = append(args,
			"family", set.HashFamily,
			"hashsize", strconv.Itoa(set.HashSize),
			"maxelem", strconv.Itoa(set.MaxElem),
		)
	}
	if set.SetType == BitmapPort {
		args = append(args, "range", set.PortRange)
	}
	if ignoreExistErr {
		args = append(args, "-exist")
	}
	if _, err := runner.exec.Command(IPSetCmd, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("error creating ipset %s, error: %v", set.Name, err)
	}
	return nil
}

// AddEntry 向指定集合添加条目；若指定 -exist 选项，
// 当相同条目已存在时 ipset 会忽略原本报错，使添加操作可幂等重试。
// ipset 的 -exist 选项会忽略条目已存在的错误。
func (runner *runner) AddEntry(entry string, set *IPSet, ignoreExistErr bool) error {
	args := []string{"add", set.Name, entry}
	if ignoreExistErr {
		args = append(args, "-exist")
	}
	if out, err := runner.exec.Command(IPSetCmd, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("error adding entry %s, error: %v (%s)", entry, err, out)
	}
	return nil
}

// DelEntry 从集合中删除指定条目。
func (runner *runner) DelEntry(entry string, set string) error {
	if out, err := runner.exec.Command(IPSetCmd, "del", set, entry).CombinedOutput(); err != nil {
		return fmt.Errorf("error deleting entry %s: from set: %s, error: %v (%s)", entry, set, err, out)
	}
	return nil
}

// TestEntry 判断指定条目是否存在于集合中。
func (runner *runner) TestEntry(entry string, set string) (bool, error) {
	if out, err := runner.exec.Command(IPSetCmd, "test", set, entry).CombinedOutput(); err == nil {
		reg, e := regexp.Compile("is NOT in set " + set)
		if e == nil && reg.MatchString(string(out)) {
			return false, nil
		} else if e == nil {
			return true, nil
		} else {
			return false, fmt.Errorf("error testing entry: %s, error: %v", entry, e)
		}
	} else {
		return false, fmt.Errorf("error testing entry %s: %v (%s)", entry, err, out)
	}
}

// FlushSet 清空指定集合中的所有条目，但保留集合本身。
func (runner *runner) FlushSet(set string) error {
	if _, err := runner.exec.Command(IPSetCmd, "flush", set).CombinedOutput(); err != nil {
		return fmt.Errorf("error flushing set: %s, error: %v", set, err)
	}
	return nil
}

// DestroySet 销毁（删除）指定的集合。
func (runner *runner) DestroySet(set string) error {
	if out, err := runner.exec.Command(IPSetCmd, "destroy", set).CombinedOutput(); err != nil {
		return fmt.Errorf("error destroying set %s, error: %v(%s)", set, err, out)
	}
	return nil
}

// DestroyAllSets 销毁内核中的所有集合。
func (runner *runner) DestroyAllSets() error {
	if _, err := runner.exec.Command(IPSetCmd, "destroy").CombinedOutput(); err != nil {
		return fmt.Errorf("error destroying all sets, error: %v", err)
	}
	return nil
}

// ListSets 列出内核中所有集合的名称。
func (runner *runner) ListSets() ([]string, error) {
	out, err := runner.exec.Command(IPSetCmd, "list", "-n").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("error listing all sets, error: %v", err)
	}
	return strings.Split(string(out), "\n"), nil
}

// ListEntries 列出指定集合中的所有条目。
func (runner *runner) ListEntries(set string) ([]string, error) {
	if len(set) == 0 {
		return nil, fmt.Errorf("set name can't be nil")
	}
	out, err := runner.exec.Command(IPSetCmd, "list", set).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("error listing set: %s, error: %v", set, err)
	}
	memberMatcher := regexp.MustCompile(EntryMemberPattern)
	list := memberMatcher.ReplaceAllString(string(out[:]), "")
	strs := strings.Split(list, "\n")
	results := make([]string, 0, len(strs))
	for i := range strs {
		if len(strs[i]) > 0 {
			results = append(results, strs[i])
		}
	}
	return results, nil
}

// GetVersion 返回 ipset 的版本字符串。
func (runner *runner) GetVersion() (string, error) {
	return getIPSetVersionString(runner.exec)
}

// getIPSetVersionString 执行 `ipset --version`，返回形如 "6.19" 的版本号。
func getIPSetVersionString(exec utilexec.Interface) (string, error) {
	cmd := exec.Command(IPSetCmd, "--version")
	cmd.SetStdin(bytes.NewReader([]byte{}))
	bytes, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	versionMatcher := regexp.MustCompile(VersionPattern)
	match := versionMatcher.FindStringSubmatch(string(bytes))
	if match == nil {
		return "", fmt.Errorf("no ipset version found in string: %s", bytes)
	}
	return match[0], nil
}

// validatePortRange 校验端口范围格式是否合法。
// 起始端口不必小于结束端口，ipset 接受 1-100 与 100-1 两种写法，
// 因此这里只校验两端都是非负整数，不比较大小。
func validatePortRange(portRange string) error {
	strs := strings.Split(portRange, "-")
	if len(strs) != 2 {
		return fmt.Errorf("invalid PortRange: %q", portRange)
	}
	for i := range strs {
		num, err := strconv.Atoi(strs[i])
		if err != nil {
			return fmt.Errorf("invalid PortRange: %q", portRange)
		}
		if num < 0 {
			return fmt.Errorf("invalid PortRange: %q", portRange)
		}
	}
	return nil
}

// validateIPSetType 校验给定的 ipset 类型是否在支持列表中。
func validateIPSetType(set Type) error {
	for _, valid := range ValidIPSetTypes {
		if set == valid {
			return nil
		}
	}
	return fmt.Errorf("unsupported SetType: %q", set)
}

// validateHashFamily 校验给定的协议族是否被 ipset 支持。
func validateHashFamily(family string) error {
	if family == ProtocolFamilyIPV4 || family == ProtocolFamilyIPV6 {
		return nil
	}
	return fmt.Errorf("unsupported HashFamily %q", family)
}

// IsNotFoundError 判断错误是否表示“对象不存在”。
// 通过匹配错误字符串中的已知关键字实现，虽不完美但在实践中可用。
func IsNotFoundError(err error) bool {
	es := err.Error()
	if strings.Contains(es, "does not exist") {
		// 集合不存在的错误文本；参见 ipset 源码 errcode.c 中的定义。
		// xref: https://github.com/Olipro/ipset/blob/master/lib/errcode.c#L32-L33
		return true
	}
	if strings.Contains(es, "element is missing") {
		// 条目不在集合中的错误文本；参见 ipset 源码 parse.c 中的相关处理。
		// xref: https://github.com/Olipro/ipset/blob/master/lib/parse.c#L1904
		// https://github.com/Olipro/ipset/blob/master/lib/parse.c#L1925
		return true
	}
	return false
}

// validateProtocol 校验条目使用的协议是否受支持。
func validateProtocol(protocol string) bool {
	if protocol == ProtocolTCP || protocol == ProtocolUDP || protocol == ProtocolSCTP {
		return true
	}
	klog.ErrorS(validationError, "invalid protocol", "protocol", protocol, "supportedProtocols", []string{ProtocolTCP, ProtocolUDP, ProtocolSCTP})
	return false
}

// parsePortRange 解析 "a-b" 格式的端口范围，返回值保证 beginPort <= endPort。
func parsePortRange(portRange string) (beginPort int, endPort int, err error) {
	if len(portRange) == 0 {
		portRange = DefaultPortRange
	}

	strs := strings.Split(portRange, "-")
	if len(strs) != 2 {
		// 端口 -1 表示解析失败，调用方据此识别错误。
		return -1, -1, fmt.Errorf("port range should be in the format of `a-b`")
	}
	for i := range strs {
		num, err := strconv.Atoi(strs[i])
		if err != nil {
			// 端口 -1 表示解析失败，调用方据此识别错误。
			return -1, -1, err
		}
		if num < 0 {
			// 端口 -1 表示解析失败，调用方据此识别错误。
			return -1, -1, fmt.Errorf("port number %d should be >=0", num)
		}
		if i == 0 {
			beginPort = num
			continue
		}
		endPort = num
		// 输入端口顺序颠倒时交换结果，与 ipset 接受 `100-1` 写法的行为保持一致。
		if beginPort > endPort {
			endPort = beginPort
			beginPort = num
		}
	}
	return beginPort, endPort, nil
}

var _ = Interface(&runner{})
