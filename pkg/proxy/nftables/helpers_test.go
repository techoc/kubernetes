//go:build linux

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

// 本文件为 nftables proxier 的测试辅助代码：对事务做规范化排序比较，
// 并以虚拟数据包追踪的方式校验规则链的最终行为。
package nftables

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/lithammer/dedent"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	netutils "k8s.io/utils/net"
	"sigs.k8s.io/knftables"
)

// getLine 返回调用方的文件名与行号（若可获取）。用例很多时，
// 报错信息带上位置才能快速定位到具体是哪一处断言失败。
func getLine() string {
	// Caller(1) 取调用者而非 getLine 自身的位置，才能指向真正的测试代码行。
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		return ""
	}
	return fmt.Sprintf(" (from %s:%d)", file, line)
}

// objectOrder 定义对象类型的排序权重（数值越大越靠前）。比较本身不依赖顺序，
// 但让规则/集合/映射排在链/元素之前能显著提升 Diff 的可读性。
var objectOrder = map[string]int{
	"table":   10,
	"chain":   9,
	"rule":    8,
	"set":     7,
	"map":     6,
	"element": 5,
}

// sortNFTablesTransaction 将 nftables 事务规范化为固定顺序，便于忽略无关顺序差异后比较。
func sortNFTablesTransaction(tx string) string {
	lines := strings.Split(tx, "\n")

	// 空行与注释对规则语义无影响，先剔除，避免干扰逐行排序。
	for i := 0; i < len(lines); {
		if lines[i] == "" || lines[i][0] == '#' {
			lines = append(lines[:i], lines[i+1:]...)
		} else {
			i++
		}
	}

	// 使用稳定排序以保留比较键相同的行之间的原始相对顺序。
	sort.SliceStable(lines, func(i, j int) bool {
		li := lines[i]
		wi := strings.Split(li, " ")
		lj := lines[j]
		wj := strings.Split(lj, " ")

		// 所有行都以 "add OBJECTTYPE ip kube-proxy" 开头；除 table 外都有对象名，
		// table 行则在表名后跟注释，因此每行至少 5 个词，可安全按索引取字段。
		if len(wi) < 5 || len(wj) < 5 {
			return false
		}

		// 先按对象类型分组；权重高的类型排前面。
		if wi[1] != wj[1] {
			return objectOrder[wi[1]] >= objectOrder[wj[1]]
		}

		// 同类型再按对象名排序，使相同对象的行彼此相邻。
		if wi[4] != wj[4] {
			return wi[4] < wj[4]
		}

		// 规则顺序决定匹配优先级，必须保持原始顺序，不能按文本重排。
		if wi[1] == "rule" {
			return false
		}

		// 类型与名称都相同（例如同一链上的元素）时再按整行排序，保证结果确定。
		return li < lj
	})
	return strings.Join(lines, "\n")
}

// diffNFTablesTransaction 是 assertNFTablesTransactionEqual 的可单测辅助函数，
// 返回规范化后两段事务的差异文本。
func diffNFTablesTransaction(expected, result string) string {
	expected = sortNFTablesTransaction(expected)
	result = sortNFTablesTransaction(result)

	return cmp.Diff(expected, result)
}

// assertNFTablesTransactionEqual 断言期望与实际事务相等，忽略排序、空行和注释等无关差异。
func assertNFTablesTransactionEqual(t *testing.T, line string, expected, result string) {
	diff := diffNFTablesTransaction(expected, result)
	if diff != "" {
		t.Errorf("tables do not match%s:\ndiff:\n%s\nfull result: %+v", line, diff, result)
	}
}

// diffNFTablesChain 是链比较的可单测辅助函数：取出 Fake 中链的规则文本，
// 去掉首尾空白后与期望文本比较并返回差异。
func diffNFTablesChain(nft *knftables.Fake, chain, expected string) string {
	// 期望文本允许首尾换行；链不存在时按空规则列表处理，而不是直接失败。
	expected = strings.TrimSpace(expected)
	result := ""
	if ch := nft.Table.Chains[chain]; ch != nil {
		for i, rule := range ch.Rules {
			if i > 0 {
				result += "\n"
			}
			result += rule.Rule
		}
	}

	return cmp.Diff(expected, result)
}

// nftablesTracer 保存虚拟追踪数据包穿过 nftables 规则链时的状态：
// Fake 表、节点 IP 集合、匹配记录、终点输出以及 masq/DNAT 标志。
type nftablesTracer struct {
	nft     *knftables.Fake
	nodeIPs sets.Set[string]
	t       *testing.T

	// matches 累计实际命中的规则，失败时打印可快速定位是哪条规则导致结果不符。
	matches []string

	// outputs 累计命中的终止目标（端点 IP:端口或 REJECT 等特殊目标），
	// 最终拼成 tracePacket 的返回值。
	outputs []string

	// 记录已命中规则的附加状态，供后续规则（如清除标记、hairpin 检查）判断条件。
	markMasq   bool
	masquerade bool
	dnat       bool
}

// newNFTablesTracer 创建追踪器。nodeIPs 视为本地节点 IP，
// 用于判断 "fib saddr type local" 与 "fib daddr type local" 规则是否命中。
func newNFTablesTracer(t *testing.T, nft *knftables.Fake, nodeIPs []string) *nftablesTracer {
	return &nftablesTracer{
		nft:     nft,
		nodeIPs: sets.New(nodeIPs...),
		t:       t,
	}
}

// addressMatches 判断规则中的字面 IP/CIDR 与给定 IP 的匹配结果是否符合预期；
// 格式错误直接在测试中报错，以免假阴性掩盖规则问题。
func (tracer *nftablesTracer) addressMatches(ipStr string, wantMatch bool, ruleAddress string) bool {
	ip := netutils.ParseIPSloppy(ipStr)
	if ip == nil {
		tracer.t.Fatalf("Bad IP in test case: %s", ipStr)
	}

	var match bool
	// 带斜杠按 CIDR 解析，否则按单个 IP 精确比较。
	if strings.Contains(ruleAddress, "/") {
		_, cidr, err := netutils.ParseCIDRSloppy(ruleAddress)
		if err != nil {
			tracer.t.Errorf("Bad CIDR in kube-proxy output: %v", err)
		}
		match = cidr.Contains(ip)
	} else {
		ip2 := netutils.ParseIPSloppy(ruleAddress)
		if ip2 == nil {
			tracer.t.Errorf("Bad IP/CIDR in kube-proxy output: %s", ruleAddress)
		}
		match = ip.Equal(ip2)
	}

	return match == wantMatch
}

// addressMatchesSet 判断 IP 与匿名集合中的每个地址是否都满足 wantMatch；
// 集合元素以逗号分隔，空格需先去掉。
func (tracer *nftablesTracer) addressMatchesSet(ipStr string, wantMatch bool, ruleAddress string) bool {
	ruleAddress = strings.ReplaceAll(ruleAddress, " ", "")
	addresses := strings.Split(ruleAddress, ",")
	var match bool
	for _, address := range addresses {
		match = tracer.addressMatches(ipStr, true, address)
		if match != wantMatch {
			return false
		}
	}
	return true
}

// matchDestIPOnly 在集合/映射中查找 key 仅含目的 IP 的条目，返回匹配的元素或 nil。
func (tracer *nftablesTracer) matchDestIPOnly(elements []*knftables.Element, destIP string) *knftables.Element {
	for _, element := range elements {
		if element.Key[0] == destIP {
			return element
		}
	}
	return nil
}

// matchDest 在集合/映射中查找由目的 IP、协议、目的端口组成的 key，返回匹配元素或 nil。
func (tracer *nftablesTracer) matchDest(elements []*knftables.Element, destIP, protocol, destPort string) *knftables.Element {
	for _, element := range elements {
		if element.Key[0] == destIP && element.Key[1] == protocol && element.Key[2] == destPort {
			return element
		}
	}
	return nil
}

// matchDestPort 在集合/映射中查找由协议与目的端口组成的 key，返回匹配元素或 nil。
func (tracer *nftablesTracer) matchDestPort(elements []*knftables.Element, protocol, destPort string) *knftables.Element {
	for _, element := range elements {
		if element.Key[0] == protocol && element.Key[1] == destPort {
			return element
		}
	}
	return nil
}

// 这里刻意不解析任意 nftables 规则：其语法复杂且与上下文相关（例如 "ip daddr"
// 既可能引出地址比较，也可能引出集合/映射查找）。只针对 proxier.go 生成的规则片段
// 写正则：先剔除 ignoredRegexp 匹配的部分，剩余内容必须命中 runChain 的某个分支，
// 否则报错。正则未以 $ 结尾时，runChain 会在匹配成功后继续处理剩余部分——例如
// "ip daddr 10.0.0.1 drop" 先命中 destAddrRegexp，再命中 verdictRegexp。

var destAddrRegexp = regexp.MustCompile(`^ip6* daddr (!= )?(\S+)`)
var destAddrLookupRegexp = regexp.MustCompile(`^ip6* daddr (!= )?\{([^}]*)\}`)
var destAddrLocalRegexp = regexp.MustCompile(`^fib daddr type local`)
var destPortRegexp = regexp.MustCompile(`^(tcp|udp|sctp) dport (\d+)`)
var destIPOnlyLookupRegexp = regexp.MustCompile(`^ip6* daddr @(\S+)`)

var destDispatchRegexp = regexp.MustCompile(`^ip6* daddr \. meta l4proto \. th dport vmap @(\S+)$`)
var destPortDispatchRegexp = regexp.MustCompile(`^meta l4proto \. th dport vmap @(\S+)$`)

var sourceAddrRegexp = regexp.MustCompile(`^ip6* saddr (!= )?(\S+)`)
var sourceAddrLookupRegexp = regexp.MustCompile(`^ip6* saddr (!= )?\{([^}]*)\}`)
var sourceAddrLocalRegexp = regexp.MustCompile(`^fib saddr type local`)

var endpointVMapRegexp = regexp.MustCompile(`^numgen random mod \d+ vmap \{(.*)\}$`)
var endpointVMapEntryRegexp = regexp.MustCompile(`\d+ : goto (\S+)`)
var endpointDNATRegexp = regexp.MustCompile(`^dnat ip6* addr \. port to numgen random mod \d+ map \{(.*)\}$`)
var endpointDNATEntryRegexp = regexp.MustCompile(`\d+ : (\S+) \. (\d+)`)
var singleEndpointDNATRegexp = regexp.MustCompile(`^dnat ip6* addr \. port to (\S+) \. (\d+)$`)

var masqMarkRegexp = regexp.MustCompile(`^mark set mark or 0x[[:xdigit:]]+$`)
var masqCheckRegexp = regexp.MustCompile(`^mark and 0x[[:xdigit:]]+ != 0 mark set mark xor 0x[[:xdigit:]]+`)
var masqueradeRegexp = regexp.MustCompile(`^masquerade fully-random$`)
var dnatCheckRegexp = regexp.MustCompile(`^ct status dnat`)
var hairpinCheckRegexp = regexp.MustCompile(`^ip6* saddr \. ip6* daddr \. meta l4proto \. th dport @hairpin-connections`)
var jumpRegexp = regexp.MustCompile(`^(jump|goto) (\S+)$`)
var returnRegexp = regexp.MustCompile(`^return$`)
var verdictRegexp = regexp.MustCompile(`^(drop|reject)$`)
var l4protoRegexp = regexp.MustCompile(`^meta l4proto (tcp|udp|sctp)`)
var dnatRegexp = regexp.MustCompile(`^dnat to (\S+)$`)

var ignoredRegexp = regexp.MustCompile(strings.Join(
	[]string{
		// 注释只出现在规则末尾，对匹配结果无影响。
		` *comment "[^"]*"$`,

		// 追踪测试只关心新连接，故此检查恒为真，可忽略。
		`^ct state new`,
	},
	"|",
))

// runChain 让给定数据包依次经过指定链的每条规则并更新追踪状态；
// 命中终止动作（DNAT、drop、reject 等）时返回 true。
func (tracer *nftablesTracer) runChain(chname, sourceIP, protocol, destIP, destPort string) bool {
	// 链不存在说明测试规则数据有误，报错并按终止处理，防止后续访问空链。
	ch := tracer.nft.Table.Chains[chname]
	if ch == nil {
		tracer.t.Errorf("unknown chain %q", chname)
		return true
	}

	for _, ruleObj := range ch.Rules {
		// 先剔除注释与恒真的连接状态检查，再逐段解析剩余规则文本。
		rule := ignoredRegexp.ReplaceAllLiteralString(ruleObj.Rule, "")
		for rule != "" {
			rule = strings.TrimLeft(rule, " ")

			// 部分正则互为前后缀（集合查找与字面量地址等），顺序错误会误匹配，不能随意调整。

			switch {
			case dnatCheckRegexp.MatchString(rule):
				// `^ct status dnat`
				// hairpin 检查的一部分：仅已 DNAT 的包才可能命中。
				match := dnatCheckRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				if !tracer.dnat {
					rule = ""
					break
				}

			case hairpinCheckRegexp.MatchString(rule):
				// `^ip6* saddr \. ip6* daddr \. meta l4proto \. th dport @hairpin-connections`
				// 判断包是否发回自身客户端（hairpin）。
				match := hairpinCheckRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				if sourceIP != destIP {
					rule = ""
					break
				}
				// 简化处理：不查 hairpin 连接表，假定源目的相同即为 hairpin。

			case destIPOnlyLookupRegexp.MatchString(rule):
				// `^ip6* daddr @(\S+)`
				// 判断目的 IP 是否属于指定集合。
				match := destIPOnlyLookupRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				set := match[1]
				if tracer.matchDestIPOnly(tracer.nft.Table.Sets[set].Elements, destIP) == nil {
					rule = ""
					break
				}

			case destDispatchRegexp.MatchString(rule):
				// `^ip6* daddr \. meta l4proto \. th dport vmap @(\S+)$`
				// 以 "目的IP . 协议 . 端口" 查 verdict 映射，命中则执行对应动作。
				match := destDispatchRegexp.FindStringSubmatch(rule)
				mapName := match[1]
				element := tracer.matchDest(tracer.nft.Table.Maps[mapName].Elements, destIP, protocol, destPort)
				if element == nil {
					rule = ""
					break
				} else {
					rule = element.Value[0]
				}

			case destPortDispatchRegexp.MatchString(rule):
				// `^meta l4proto \. th dport vmap @(\S+)$`
				// 以 "协议 . 端口" 查 verdict 映射，命中则执行对应动作。
				match := destPortDispatchRegexp.FindStringSubmatch(rule)
				mapName := match[1]
				element := tracer.matchDestPort(tracer.nft.Table.Maps[mapName].Elements, protocol, destPort)
				if element == nil {
					rule = ""
					break
				} else {
					rule = element.Value[0]
				}

			case destAddrLookupRegexp.MatchString(rule):
				// `^ip6* daddr (!= )?\{([^}]*)\}`
				// 判断目的 IP 是否与匿名集合匹配（支持 != 取反）。
				match := destAddrLookupRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				wantMatch, set := match[1] != "!= ", match[2]
				if !tracer.addressMatchesSet(destIP, wantMatch, set) {
					rule = ""
					break
				}

			case destAddrRegexp.MatchString(rule):
				// `^ip6* daddr (!= )?(\S+)`
				// 判断目的 IP 是否与字面量地址相等（支持 != 取反）。
				match := destAddrRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				wantMatch, ip := match[1] != "!= ", match[2]
				if !tracer.addressMatches(destIP, wantMatch, ip) {
					rule = ""
					break
				}

			case destAddrLocalRegexp.MatchString(rule):
				// `^fib daddr type local`
				// 判断目的 IP 是否属于本地节点 IP。
				match := destAddrLocalRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				if !tracer.nodeIPs.Has(destIP) {
					rule = ""
					break
				}

			case destPortRegexp.MatchString(rule):
				// `^(tcp|udp|sctp) dport (\d+)`
				// 同时校验协议与端口，任一不符则不命中该规则。
				match := destPortRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				proto, port := match[1], match[2]
				if protocol != proto || destPort != port {
					rule = ""
					break
				}

			case sourceAddrLookupRegexp.MatchString(rule):
				// `^ip6* saddr (!= )?\{([^}]*)\}`
				// 判断源 IP 是否与匿名集合匹配（支持 != 取反）。
				match := sourceAddrLookupRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				wantMatch, set := match[1] != "!= ", match[2]
				if !tracer.addressMatchesSet(sourceIP, wantMatch, set) {
					rule = ""
					break
				}

			case sourceAddrRegexp.MatchString(rule):
				// `^ip6* saddr (!= )?(\S+)`
				// 判断源 IP 是否与字面量地址相等（支持 != 取反）。
				match := sourceAddrRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				wantMatch, ip := match[1] != "!= ", match[2]
				if !tracer.addressMatches(sourceIP, wantMatch, ip) {
					rule = ""
					break
				}

			case sourceAddrLocalRegexp.MatchString(rule):
				// `^fib saddr type local`
				// 判断源 IP 是否属于本地节点 IP。
				match := sourceAddrLocalRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				if !tracer.nodeIPs.Has(sourceIP) {
					rule = ""
					break
				}

			case l4protoRegexp.MatchString(rule):
				// `meta l4proto (tcp|udp|sctp)`
				// 校验数据包的四层协议。
				match := l4protoRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				if match[1] != protocol {
					rule = ""
					break
				}

			case masqMarkRegexp.MatchString(rule):
				// `^mark set mark or 0x[[:xdigit:]]+$`
				// 打上待伪装标记，后续规则据此决定是否 masquerade。
				match := masqMarkRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])

				tracer.matches = append(tracer.matches, ruleObj.Rule)
				tracer.markMasq = true

			case masqCheckRegexp.MatchString(rule):
				// `^mark and 0x[[:xdigit:]]+ != 0 mark set mark xor 0x[[:xdigit:]]+`
				// 检查并清除伪装标记：只有前面打过标记才会继续执行。
				match := masqCheckRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				if !tracer.markMasq {
					rule = ""
					break
				}
				tracer.markMasq = false

			case masqueradeRegexp.MatchString(rule):
				// ^masquerade fully-random$
				// 真正执行伪装，记录最终返回值使用的标志。
				match := masqueradeRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])

				tracer.matches = append(tracer.matches, ruleObj.Rule)
				tracer.masquerade = true

			case jumpRegexp.MatchString(rule):
				// `^(jump|goto) (\S+)$`
				// 跳转到另一条链；jump 可返回继续，goto 则不返回。
				match := jumpRegexp.FindStringSubmatch(rule)
				rule = strings.TrimPrefix(rule, match[0])
				action, destChain := match[1], match[2]

				tracer.matches = append(tracer.matches, ruleObj.Rule)
				// 被调链若已终止，则整条路径也终止。
				terminated := tracer.runChain(destChain, sourceIP, protocol, destIP, destPort)
				if terminated {
					return true
				} else if action == "goto" {
					// goto 不返回调用链：直接结束当前链的处理，
					// 与 jump 继续执行后续规则区分开。
					return false
				}

			case verdictRegexp.MatchString(rule):
				// `^(drop|reject)$`
				// 丢弃/拒绝包并终止处理，同时记入 outputs 供断言。
				match := verdictRegexp.FindStringSubmatch(rule)
				verdict := match[1]

				tracer.matches = append(tracer.matches, ruleObj.Rule)
				tracer.outputs = append(tracer.outputs, strings.ToUpper(verdict))
				return true

			case returnRegexp.MatchString(rule):
				// `^return$`
				// 返回调用链，未产生终止动作。
				tracer.matches = append(tracer.matches, ruleObj.Rule)
				return false

			case dnatRegexp.MatchString(rule):
				// `^dnat to (\S+)$`
				// 直接 DNAT 到端点地址并终止处理。
				match := dnatRegexp.FindStringSubmatch(rule)
				destEndpoint := match[1]

				tracer.matches = append(tracer.matches, ruleObj.Rule)
				tracer.outputs = append(tracer.outputs, destEndpoint)
				return true

			case endpointVMapRegexp.MatchString(rule):
				// `^numgen random mod \d+ vmap \{(.*)\}$`
				// 实际是随机选一个端点；追踪时覆盖所有端点以便断言全部候选结果。
				match := endpointVMapRegexp.FindStringSubmatch(rule)
				elements := match[1]

				for _, match = range endpointVMapEntryRegexp.FindAllStringSubmatch(elements, -1) {
					// `\d+ : goto (\S+)`
					destChain := match[1]

					tracer.matches = append(tracer.matches, ruleObj.Rule)
					// 每个端点链都以 dnat 终止，故忽略返回值，仅需收集所有端点。
					_ = tracer.runChain(destChain, sourceIP, protocol, destIP, destPort)
				}
				return true

			case endpointDNATRegexp.MatchString(rule):
				// `^dnat ip6* addr \. port to numgen random mod \d+ map \{(.*)\}$`
				// 实际随机选一个端点做 DNAT；此处收集所有候选以覆盖随机结果。
				match := endpointDNATRegexp.FindStringSubmatch(rule)
				elements := match[1]

				for _, match = range endpointDNATEntryRegexp.FindAllStringSubmatch(elements, -1) {
					// `\d+ : (\S+) \. (\d+)`
					endpointIP, endpointPort := match[1], match[2]

					tracer.matches = append(tracer.matches, ruleObj.Rule)
					tracer.outputs = append(tracer.outputs, net.JoinHostPort(endpointIP, endpointPort))
				}
				return true

			case singleEndpointDNATRegexp.MatchString(rule):
				// `^dnat ip6* addr \. port to (\S+) \. (\d+)$`
				// 直接 DNAT 到唯一端点并终止处理。
				match := singleEndpointDNATRegexp.FindStringSubmatch(rule)
				endpointIP, endpointPort := match[1], match[2]

				tracer.matches = append(tracer.matches, ruleObj.Rule)
				tracer.outputs = append(tracer.outputs, net.JoinHostPort(endpointIP, endpointPort))
				return true

			default:
				// 规则未被任何已知模式识别：要么规则格式变了，要么追踪器遗漏了分支。
				tracer.t.Errorf("unmatched rule: %s", ruleObj.Rule)
				rule = ""
			}
		}
	}

	return false
}

// tracePacket 模拟给定源/目的 IP 与端口的数据包在规则集中的走向。nodeIPs 为本地节点 IP，
// 用于匹配 "local" 规则；protocol 必须是 nftables 中的小写形式，而非 corev1 的大写值。
// 返回值为：命中的规则列表（便于调试）、最终目的地（逗号分隔的 IP 或 ACCEPT/DROP/REJECT
// 等特殊目标），以及数据包是否会被伪装。
func tracePacket(t *testing.T, nft *knftables.Fake, sourceIP, protocol, destIP, destPort string, nodeIPs []string) ([]string, string, bool) {
	var err error
	tracer := newNFTablesTracer(t, nft, nodeIPs)

	// 按 netfilter 实际钩子顺序：先 pre-DNAT 过滤链，命中终止动作则不再执行后续链。
	if tracer.runChain("filter-prerouting-pre-dnat", sourceIP, protocol, destIP, destPort) {
		return tracer.matches, strings.Join(tracer.outputs, ", "), tracer.markMasq
	}
	tracer.runChain("nat-prerouting", sourceIP, protocol, destIP, destPort)
	// prerouting 产生的 DNAT 会改变后续规则看到的目的 IP，故先提取并标记 dnat 状态。
	if len(tracer.outputs) != 0 {
		destIP, _, err = net.SplitHostPort(tracer.outputs[0])
		if err != nil {
			t.Errorf("failed to parse host port '%s': %s", tracer.outputs[0], err.Error())
		}
		tracer.dnat = true
	}

	if tracer.runChain("filter-forward", sourceIP, protocol, destIP, destPort) {
		return tracer.matches, strings.Join(tracer.outputs, ", "), tracer.markMasq
	}

	tracer.runChain("filter-input", sourceIP, protocol, destIP, destPort)

	// output 链在测试场景中与 prerouting 链完全冗余，跳过以简化追踪。

	// 最后跑 postrouting 链处理伪装；返回值取 masquerade 而非前面的 markMasq。
	tracer.runChain("nat-postrouting", sourceIP, protocol, destIP, destPort)

	return tracer.matches, strings.Join(tracer.outputs, ", "), tracer.masquerade
}

// packetFlowTest 描述一条数据包流向用例：源/目的、协议、端口及期望的输出与是否伪装。
type packetFlowTest struct {
	name     string
	sourceIP string
	protocol v1.Protocol
	destIP   string
	destPort int
	output   string
	masq     bool
}

// runPacketFlowTests 逐条执行数据包流向用例，以子测试报告结果；
// line 用于把调用位置附加到失败信息，便于定位用例来源。
func runPacketFlowTests(t *testing.T, line string, nft *knftables.Fake, nodeIPs []string, testCases []packetFlowTest) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 协议默认按 TCP 处理，与 nftables 规则中的小写形式保持一致。
			protocol := strings.ToLower(string(tc.protocol))
			if protocol == "" {
				protocol = "tcp"
			}
			matches, output, masq := tracePacket(t, nft, tc.sourceIP, protocol, tc.destIP, fmt.Sprintf("%d", tc.destPort), nodeIPs)
			// 收集全部差异后一次报错，避免反复运行用例才能看全问题。
			var errors []string
			if output != tc.output {
				errors = append(errors, fmt.Sprintf("wrong output: expected %q got %q", tc.output, output))
			}
			if masq != tc.masq {
				errors = append(errors, fmt.Sprintf("wrong masq: expected %v got %v", tc.masq, masq))
			}
			// 失败时连同命中的规则一起打印，方便判断是规则生成还是匹配逻辑出错。
			if errors != nil {
				t.Errorf("Test %q of a packet from %s to %s:%d%s got result:\n%s\n\nBy matching:\n%s\n\n",
					tc.name, tc.sourceIP, tc.destIP, tc.destPort, line, strings.Join(errors, "\n"), strings.Join(matches, "\n"))
			}
		})
	}
}

// helpers_test 单元测试的公共夹具：一份未排序的 nftables 事务及其规范化后的期望结果。

var testInput = dedent.Dedent(`
	add table ip testing { comment "rules for kube-proxy" ; }

	add chain ip testing masquerading
	add rule ip testing masquerading mark and 0x4000 != 0 mark set mark xor 0x4000 masquerade fully-random

	add set ip testing firewall { type ipv4_addr . inet_proto . inet_service ; comment "destinations that are subject to LoadBalancerSourceRanges" ; }
	add set ip testing firewall-allow { type ipv4_addr . inet_proto . inet_service . ipv4_addr ; flags interval ; comment "destinations+sources that are allowed by LoadBalancerSourceRanges" ; }
	add chain ip testing firewall-check
	add chain ip testing firewall-allow-check
	add rule ip testing firewall-allow-check ip daddr . meta l4proto . th dport . ip saddr @firewall-allow return
	add rule ip testing firewall-allow-check drop
	add rule ip testing firewall-check ip daddr . meta l4proto . th dport @firewall jump firewall-allow-check

	# svc1
	add chain ip testing service-ULMVA6XW-ns1/svc1/tcp/p80
	add rule ip testing service-ULMVA6XW-ns1/svc1/tcp/p80 ip daddr 172.30.0.41 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
	add rule ip testing service-ULMVA6XW-ns1/svc1/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 }

	add chain ip testing endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80
	add rule ip testing endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 ip saddr 10.180.0.1 mark set mark or 0x4000
	add rule ip testing endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 meta l4proto tcp dnat to 10.180.0.1:80

	add element ip testing service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }

	# svc2
	add chain ip testing service-42NFTM6N-ns2/svc2/tcp/p80
	add rule ip testing service-42NFTM6N-ns2/svc2/tcp/p80 ip daddr 172.30.0.42 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
	add rule ip testing service-42NFTM6N-ns2/svc2/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 }
	add chain ip testing external-42NFTM6N-ns2/svc2/tcp/p80
	add rule ip testing external-42NFTM6N-ns2/svc2/tcp/p80 ip saddr 10.0.0.0/8 goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit pod traffic"
	add rule ip testing external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local mark set mark or 0x4000 comment "masquerade local traffic"
	add rule ip testing external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit local traffic"
	add chain ip testing endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80
	add rule ip testing endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 ip saddr 10.180.0.2 mark set mark or 0x4000
	add rule ip testing endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 meta l4proto tcp dnat to 10.180.0.2:80

	add element ip testing service-ips { 172.30.0.42 . tcp . 80 : goto service-42NFTM6N-ns2/svc2/tcp/p80 }
	add element ip testing service-ips { 192.168.99.22 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
	add element ip testing service-ips { 1.2.3.4 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
	add element ip testing service-nodeports { tcp . 3001 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }

	add element ip testing no-endpoint-nodeports { tcp . 3001 comment "ns2/svc2:p80" : drop }
	add element ip testing no-endpoint-services { 1.2.3.4 . tcp . 80 comment "ns2/svc2:p80" : drop }
	add element ip testing no-endpoint-services { 192.168.99.22 . tcp . 80 comment "ns2/svc2:p80" : drop }
	`)

// testExpected 是 testInput 规范化排序后的期望文本，供排序与比较相关测试复用。
var testExpected = dedent.Dedent(`
	add table ip testing { comment "rules for kube-proxy" ; }
	add chain ip testing endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80
	add chain ip testing endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80
	add chain ip testing external-42NFTM6N-ns2/svc2/tcp/p80
	add chain ip testing firewall-allow-check
	add chain ip testing firewall-check
	add chain ip testing masquerading
	add chain ip testing service-42NFTM6N-ns2/svc2/tcp/p80
	add chain ip testing service-ULMVA6XW-ns1/svc1/tcp/p80
	add rule ip testing endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 ip saddr 10.180.0.1 mark set mark or 0x4000
	add rule ip testing endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 meta l4proto tcp dnat to 10.180.0.1:80
	add rule ip testing endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 ip saddr 10.180.0.2 mark set mark or 0x4000
	add rule ip testing endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 meta l4proto tcp dnat to 10.180.0.2:80
	add rule ip testing external-42NFTM6N-ns2/svc2/tcp/p80 ip saddr 10.0.0.0/8 goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit pod traffic"
	add rule ip testing external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local mark set mark or 0x4000 comment "masquerade local traffic"
	add rule ip testing external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit local traffic"
	add rule ip testing firewall-allow-check ip daddr . meta l4proto . th dport . ip saddr @firewall-allow return
	add rule ip testing firewall-allow-check drop
	add rule ip testing firewall-check ip daddr . meta l4proto . th dport @firewall jump firewall-allow-check
	add rule ip testing masquerading mark and 0x4000 != 0 mark set mark xor 0x4000 masquerade fully-random
	add rule ip testing service-42NFTM6N-ns2/svc2/tcp/p80 ip daddr 172.30.0.42 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
	add rule ip testing service-42NFTM6N-ns2/svc2/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 }
	add rule ip testing service-ULMVA6XW-ns1/svc1/tcp/p80 ip daddr 172.30.0.41 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
	add rule ip testing service-ULMVA6XW-ns1/svc1/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 }
	add set ip testing firewall { type ipv4_addr . inet_proto . inet_service ; comment "destinations that are subject to LoadBalancerSourceRanges" ; }
	add set ip testing firewall-allow { type ipv4_addr . inet_proto . inet_service . ipv4_addr ; flags interval ; comment "destinations+sources that are allowed by LoadBalancerSourceRanges" ; }
	add element ip testing no-endpoint-nodeports { tcp . 3001 comment "ns2/svc2:p80" : drop }
	add element ip testing no-endpoint-services { 1.2.3.4 . tcp . 80 comment "ns2/svc2:p80" : drop }
	add element ip testing no-endpoint-services { 192.168.99.22 . tcp . 80 comment "ns2/svc2:p80" : drop }
	add element ip testing service-ips { 1.2.3.4 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
	add element ip testing service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
	add element ip testing service-ips { 172.30.0.42 . tcp . 80 : goto service-42NFTM6N-ns2/svc2/tcp/p80 }
	add element ip testing service-ips { 192.168.99.22 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
	add element ip testing service-nodeports { tcp . 3001 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
	`)

// 测试 sortNFTablesTransaction：规范化后的事务应与 testExpected 完全一致。
func Test_sortNFTablesTransaction(t *testing.T) {
	output := sortNFTablesTransaction(testInput)
	// 常量原始文本带首尾换行，规范化结果不含，需先 TrimSpace 再比较。
	expected := strings.TrimSpace(testExpected)

	diff := cmp.Diff(expected, output)
	if diff != "" {
		t.Errorf("output does not match expected:\n%s", diff)
	}
}

// 测试 diffNFTablesTransaction：相同事务应无差异，删掉一行的变体应能被检出差异。
func Test_diffNFTablesTransaction(t *testing.T) {
	diff := diffNFTablesTransaction(testInput, testExpected)
	if diff != "" {
		t.Errorf("found diff in inputs that should have been equal:\n%s", diff)
	}

	// 去掉 testExpected 的前两行制造差异，验证 diff 函数确实能发现内容缺失。
	notExpected := strings.Join(strings.Split(testExpected, "\n")[2:], "\n")
	diff = diffNFTablesTransaction(testInput, notExpected)
	if diff == "" {
		t.Errorf("found no diff in inputs that should have been different")
	}
}

// 测试 diffNFTablesChain：覆盖规则完全匹配、尾部换行、规则缺失与空链等情形。
func Test_diffNFTablesChain(t *testing.T) {
	fake := knftables.NewFake(knftables.IPv4Family, "testing")
	tx := fake.NewTransaction()

	tx.Add(&knftables.Table{})
	tx.Add(&knftables.Chain{
		Name: "mark-masq-chain",
	})
	tx.Add(&knftables.Chain{
		Name: "masquerade-chain",
	})
	tx.Add(&knftables.Chain{
		Name: "empty-chain",
	})

	tx.Add(&knftables.Rule{
		Chain: "mark-masq-chain",
		Rule:  "mark set mark or 0x4000",
	})

	tx.Add(&knftables.Rule{
		Chain: "masquerade-chain",
		Rule:  "mark and 0x4000 == 0 return",
	})
	tx.Add(&knftables.Rule{
		Chain: "masquerade-chain",
		Rule:  "mark set mark xor 0x4000",
	})
	tx.Add(&knftables.Rule{
		Chain: "masquerade-chain",
		Rule:  "masquerade fully-random",
	})

	// 必须先把事务应用到 Fake，规则才会出现在链的 Dump 中供比较。
	err := fake.Run(context.Background(), tx)
	if err != nil {
		t.Fatalf("Unexpected error running transaction: %v", err)
	}

	diff := diffNFTablesChain(fake, "mark-masq-chain", "mark set mark or 0x4000")
	if diff != "" {
		t.Errorf("unexpected difference in mark-masq-chain:\n%s", diff)
	}
	diff = diffNFTablesChain(fake, "mark-masq-chain", "mark set mark or 0x4000\n")
	if diff != "" {
		t.Errorf("unexpected difference in mark-masq-chain with trailing newline:\n%s", diff)
	}

	diff = diffNFTablesChain(fake, "masquerade-chain", "mark and 0x4000 == 0 return\nmark set mark xor 0x4000\nmasquerade fully-random")
	if diff != "" {
		t.Errorf("unexpected difference in masquerade-chain:\n%s", diff)
	}
	// 故意漏掉第一条规则，确保差异检测不是恒为空。
	diff = diffNFTablesChain(fake, "masquerade-chain", "mark set mark xor 0x4000\nmasquerade fully-random")
	if diff == "" {
		t.Errorf("unexpected lack of difference in wrong masquerade-chain")
	}

	diff = diffNFTablesChain(fake, "empty-chain", "")
	if diff != "" {
		t.Errorf("unexpected difference in empty-chain:\n%s", diff)
	}
	diff = diffNFTablesChain(fake, "empty-chain", "\n")
	if diff != "" {
		t.Errorf("unexpected difference in empty-chain with trailing newline:\n%s", diff)
	}
}

// 基于静态规则数据测试 tracePacket，验证匹配行为符合预期。
// knftables.Fake 一次仅支持一种地址族，因此 IPv4/IPv6 分开测试；
// 测试数据取自 TestOverallNFTablesRules。
func TestTracePacketV4(t *testing.T) {
	rules := dedent.Dedent(`
		add table ip kube-proxy { comment "rules for kube-proxy" ; }
		
		add chain ip kube-proxy masquerading
		add chain ip kube-proxy services
		add chain ip kube-proxy firewall-check
		add chain ip kube-proxy endpoints-check
		add chain ip kube-proxy filter-prerouting-pre-dnat { type filter hook prerouting priority -110 ; }
		add chain ip kube-proxy filter-output-pre-dnat { type filter hook prerouting priority -110 ; }
		add chain ip kube-proxy filter-forward { type filter hook forward priority 0 ; }
		add chain ip kube-proxy filter-input { type filter hook input priority 0 ; }
		add chain ip kube-proxy filter-output { type filter hook output priority 0 ; }
		add chain ip kube-proxy nat-output { type nat hook output priority -100 ; }
		add chain ip kube-proxy nat-postrouting { type nat hook postrouting priority 100 ; }
		add chain ip kube-proxy nat-prerouting { type nat hook prerouting priority -100 ; }
		add chain ip kube-proxy reject-chain { comment "helper for @no-endpoint-services / @no-endpoint-nodeports" ; }
		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add chain ip kube-proxy endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80
		add chain ip kube-proxy service-42NFTM6N-ns2/svc2/tcp/p80
		add chain ip kube-proxy endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80
		add chain ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80
		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add chain ip kube-proxy endpoint-UEIP74TE-ns3/svc3/tcp/p80__10.180.0.3/80
		add chain ip kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80
		add chain ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add chain ip kube-proxy endpoint-UNZV3OEC-ns4/svc4/tcp/p80__10.180.0.4/80
		add chain ip kube-proxy endpoint-5RFCDDV7-ns4/svc4/tcp/p80__10.180.0.5/80
		add chain ip kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80
		add chain ip kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80
		add chain ip kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80
		add chain ip kube-proxy endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80
		add chain ip kube-proxy firewall-HVFWP5L3-ns5/svc5/tcp/p80

		add rule ip kube-proxy masquerading mark and 0x4000 != 0 mark set mark xor 0x4000 masquerade fully-random
		add rule ip kube-proxy filter-prerouting-pre-dnat ct state new jump firewall-check
		add rule ip kube-proxy filter-forward ct state new jump endpoints-check
		add rule ip kube-proxy filter-input ct state new jump endpoints-check
		add rule ip kube-proxy filter-output ct state new jump endpoints-check
		add rule ip kube-proxy filter-output-pre-dnat ct state new jump firewall-check
		add rule ip kube-proxy nat-output jump services
		add rule ip kube-proxy nat-postrouting jump masquerading
		add rule ip kube-proxy nat-prerouting jump services

		add map ip kube-proxy firewall-ips { type ipv4_addr . inet_proto . inet_service : verdict ; comment "destinations that are subject to LoadBalancerSourceRanges" ; }
		add rule ip kube-proxy firewall-check ip daddr . meta l4proto . th dport vmap @firewall-ips

		add rule ip kube-proxy reject-chain reject

		add map ip kube-proxy no-endpoint-services { type ipv4_addr . inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to services with no endpoints" ; }
		add map ip kube-proxy no-endpoint-nodeports { type inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to service nodeports with no endpoints" ; }

		add rule ip kube-proxy endpoints-check ip daddr . meta l4proto . th dport vmap @no-endpoint-services
		add rule ip kube-proxy endpoints-check fib daddr type local ip daddr != 127.0.0.0/8 meta l4proto . th dport vmap @no-endpoint-nodeports

		add map ip kube-proxy service-ips { type ipv4_addr . inet_proto . inet_service : verdict ; comment "ClusterIP, ExternalIP and LoadBalancer IP traffic" ; }
		add map ip kube-proxy service-nodeports { type inet_proto . inet_service : verdict ; comment "NodePort traffic" ; }
		add rule ip kube-proxy services ip daddr . meta l4proto . th dport vmap @service-ips
		add rule ip kube-proxy services fib daddr type local ip daddr != 127.0.0.0/8 meta l4proto . th dport vmap @service-nodeports

		# svc1
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 ip daddr 172.30.0.41 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 }

		add rule ip kube-proxy endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 ip saddr 10.180.0.1 mark set mark or 0x4000
		add rule ip kube-proxy endpoint-5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80 meta l4proto tcp dnat to 10.180.0.1:80

		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }

		# svc2
		add rule ip kube-proxy service-42NFTM6N-ns2/svc2/tcp/p80 ip daddr 172.30.0.42 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
		add rule ip kube-proxy service-42NFTM6N-ns2/svc2/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 }
		add rule ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 ip saddr 10.0.0.0/8 goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit pod traffic"
		add rule ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local mark set mark or 0x4000 comment "masquerade local traffic"
		add rule ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit local traffic"
		add rule ip kube-proxy endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 ip saddr 10.180.0.2 mark set mark or 0x4000
		add rule ip kube-proxy endpoint-SGOXE6O3-ns2/svc2/tcp/p80__10.180.0.2/80 meta l4proto tcp dnat to 10.180.0.2:80

		add element ip kube-proxy service-ips { 172.30.0.42 . tcp . 80 : goto service-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip kube-proxy service-ips { 192.168.99.22 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip kube-proxy service-ips { 1.2.3.4 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip kube-proxy service-nodeports { tcp . 3001 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }

		add element ip kube-proxy no-endpoint-nodeports { tcp . 3001 comment "ns2/svc2:p80" : drop }
		add element ip kube-proxy no-endpoint-services { 1.2.3.4 . tcp . 80 comment "ns2/svc2:p80" : drop }
		add element ip kube-proxy no-endpoint-services { 192.168.99.22 . tcp . 80 comment "ns2/svc2:p80" : drop }

		# svc3
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 ip daddr 172.30.0.43 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-UEIP74TE-ns3/svc3/tcp/p80__10.180.0.3/80 }
		add rule ip kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80 mark set mark or 0x4000
		add rule ip kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80 goto service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy endpoint-UEIP74TE-ns3/svc3/tcp/p80__10.180.0.3/80 ip saddr 10.180.0.3 mark set mark or 0x4000
		add rule ip kube-proxy endpoint-UEIP74TE-ns3/svc3/tcp/p80__10.180.0.3/80 meta l4proto tcp dnat to 10.180.0.3:80

		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy service-nodeports { tcp . 3003 : goto external-4AT6LBPK-ns3/svc3/tcp/p80 }

		# svc4
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 ip daddr 172.30.0.44 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 numgen random mod 2 vmap { 0 : goto endpoint-UNZV3OEC-ns4/svc4/tcp/p80__10.180.0.4/80 , 1 : goto endpoint-5RFCDDV7-ns4/svc4/tcp/p80__10.180.0.5/80 }
		add rule ip kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80 mark set mark or 0x4000
		add rule ip kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80 goto service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy endpoint-5RFCDDV7-ns4/svc4/tcp/p80__10.180.0.5/80 ip saddr 10.180.0.5 mark set mark or 0x4000
		add rule ip kube-proxy endpoint-5RFCDDV7-ns4/svc4/tcp/p80__10.180.0.5/80 meta l4proto tcp dnat to 10.180.0.5:80
		add rule ip kube-proxy endpoint-UNZV3OEC-ns4/svc4/tcp/p80__10.180.0.4/80 ip saddr 10.180.0.4 mark set mark or 0x4000
		add rule ip kube-proxy endpoint-UNZV3OEC-ns4/svc4/tcp/p80__10.180.0.4/80 meta l4proto tcp dnat to 10.180.0.4:80

		add element ip kube-proxy service-ips { 172.30.0.44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy service-ips { 192.168.99.33 . tcp . 80 : goto external-LAUZTJTB-ns4/svc4/tcp/p80 }

		# svc5
		add set ip kube-proxy affinity-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 { type ipv4_addr ; flags dynamic,timeout ; timeout 10800s ; }
		add rule ip kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80 ip daddr 172.30.0.45 tcp dport 80 ip saddr != 10.0.0.0/8 mark set mark or 0x4000
		add rule ip kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80 ip saddr @affinity-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 goto endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80
		add rule ip kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 }
		add rule ip kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80 mark set mark or 0x4000
		add rule ip kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80 goto service-HVFWP5L3-ns5/svc5/tcp/p80

		add rule ip kube-proxy endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 ip saddr 10.180.0.3 mark set mark or 0x4000
		add rule ip kube-proxy endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 update @affinity-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 { ip saddr }
		add rule ip kube-proxy endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 meta l4proto tcp dnat to 10.180.0.3:80

		add rule ip kube-proxy firewall-HVFWP5L3-ns5/svc5/tcp/p80 ip saddr != { 203.0.113.0/25 } drop

		add element ip kube-proxy service-ips { 172.30.0.45 . tcp . 80 : goto service-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip kube-proxy service-ips { 5.6.7.8 . tcp . 80 : goto external-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip kube-proxy service-nodeports { tcp . 3002 : goto external-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip kube-proxy firewall-ips { 5.6.7.8 . tcp . 80 comment "ns5/svc5:p80" : goto firewall-HVFWP5L3-ns5/svc5/tcp/p80 }

		# svc6
		add element ip kube-proxy no-endpoint-services { 172.30.0.46 . tcp . 80 comment "ns6/svc6:p80" : goto reject-chain }
		`)

	nft := knftables.NewFake(knftables.IPv4Family, "kube-proxy")
	err := nft.ParseDump(rules)
	if err != nil {
		t.Fatalf("failed to parse given nftables rules: %v", err)
	}
	// 先确认规则被正确解析：否则后续追踪失败无法区分是解析问题还是规则逻辑问题。
	assertNFTablesTransactionEqual(t, getLine(), rules, nft.Dump())
	runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
		// 目的 IP 不在任何映射中，应保持无输出。
		{
			name:     "no match",
			sourceIP: "10.0.0.2",
			destIP:   "10.0.0.3",
			destPort: 80,
			output:   "",
		},
		{
			name:     "single endpoint",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80",
		},
		// svc4 有两个端点，追踪器应把随机选择的两种结果都收集到。
		{
			name:     "multiple endpoints",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.44",
			destPort: 80,
			output:   "10.180.0.4:80, 10.180.0.5:80",
		},
		// 来自节点自身的流量应打上伪装标记。
		{
			name:     "local, mark for masquerade",
			sourceIP: testNodeIP,
			destIP:   "192.168.99.22",
			destPort: 80,
			output:   "10.180.0.2:80",
			masq:     true,
		},
		// 外部客户端被 LoadBalancerSourceRanges 拦截，应命中 DROP。
		{
			name:     "DROP",
			sourceIP: testExternalClient,
			destIP:   "192.168.99.22",
			destPort: 80,
			output:   "DROP",
		},
		// svc6 无端点且配置为 reject，应命中 REJECT。
		{
			name:     "REJECT",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.46",
			destPort: 80,
			output:   "REJECT",
		},
		{
			name:     "blocked external to loadbalancer IP",
			sourceIP: testExternalClientBlocked,
			destIP:   "5.6.7.8",
			destPort: 80,
			output:   "DROP",
		},
		{
			name:     "pod to nodePort",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: 3001,
			output:   "10.180.0.2:80",
		},
	})
}

// 与 TestTracePacketV4 同理，用 IPv6 静态规则验证追踪逻辑；
// 数据取自 TestNodePorts 的 IPv6 "basic tests"，因 Fake 只支持单地址族而独立成用例。
func TestTracePacketV6(t *testing.T) {
	rules := dedent.Dedent(`
		add table ip6 kube-proxy { comment "rules for kube-proxy" ; }
		add chain ip6 kube-proxy cluster-ips-check
		add chain ip6 kube-proxy endpoint-2CRNCTTE-ns1/svc1/tcp/p80__fd00.10.180..2.1/80
		add chain ip6 kube-proxy endpoint-ZVRFLKHO-ns1/svc1/tcp/p80__fd00.10.180..1/80
		add chain ip6 kube-proxy external-ULMVA6XW-ns1/svc1/tcp/p80
		add chain ip6 kube-proxy filter-prerouting-pre-dnat { type filter hook prerouting priority -110 ; }
		add chain ip6 kube-proxy filter-output-pre-dnat { type filter hook output priority -110 ; }
		add chain ip6 kube-proxy filter-forward { type filter hook forward priority 0 ; }
		add chain ip6 kube-proxy filter-input { type filter hook input priority 0 ; }
		add chain ip6 kube-proxy filter-output { type filter hook output priority 0 ; }
		add chain ip6 kube-proxy filter-output-post-dnat { type filter hook output priority -90 ; }
		add chain ip6 kube-proxy firewall-check
		add chain ip6 kube-proxy masquerading
		add chain ip6 kube-proxy nat-output { type nat hook output priority -100 ; }
		add chain ip6 kube-proxy nat-postrouting { type nat hook postrouting priority 100 ; }
		add chain ip6 kube-proxy nat-prerouting { type nat hook prerouting priority -100 ; }
		add chain ip6 kube-proxy nodeport-endpoints-check
		add chain ip6 kube-proxy reject-chain { comment "helper for @no-endpoint-services / @no-endpoint-nodeports" ; }
		add chain ip6 kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add chain ip6 kube-proxy service-endpoints-check
		add chain ip6 kube-proxy services
		add set ip6 kube-proxy cluster-ips { type ipv6_addr ; comment "Active ClusterIPs" ; }
		add set ip6 kube-proxy nodeport-ips { type ipv6_addr ; comment "IPs that accept NodePort traffic" ; }
		add map ip6 kube-proxy firewall-ips { type ipv6_addr . inet_proto . inet_service : verdict ; comment "destinations that are subject to LoadBalancerSourceRanges" ; }
		add map ip6 kube-proxy no-endpoint-nodeports { type inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to service nodeports with no endpoints" ; }
		add map ip6 kube-proxy no-endpoint-services { type ipv6_addr . inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to services with no endpoints" ; }
		add map ip6 kube-proxy service-ips { type ipv6_addr . inet_proto . inet_service : verdict ; comment "ClusterIP, ExternalIP and LoadBalancer IP traffic" ; }
		add map ip6 kube-proxy service-nodeports { type inet_proto . inet_service : verdict ; comment "NodePort traffic" ; }
		add rule ip6 kube-proxy cluster-ips-check ip6 daddr @cluster-ips reject comment "Reject traffic to invalid ports of ClusterIPs"
		add rule ip6 kube-proxy cluster-ips-check ip6 daddr { fd00:10:96::/112 } drop comment "Drop traffic to unallocated ClusterIPs"
		add rule ip6 kube-proxy endpoint-2CRNCTTE-ns1/svc1/tcp/p80__fd00.10.180..2.1/80 ip6 saddr fd00:10:180::2:1 mark set mark or 0x4000
		add rule ip6 kube-proxy endpoint-2CRNCTTE-ns1/svc1/tcp/p80__fd00.10.180..2.1/80 meta l4proto tcp dnat to [fd00:10:180::2:1]:80
		add rule ip6 kube-proxy endpoint-ZVRFLKHO-ns1/svc1/tcp/p80__fd00.10.180..1/80 ip6 saddr fd00:10:180::1 mark set mark or 0x4000
		add rule ip6 kube-proxy endpoint-ZVRFLKHO-ns1/svc1/tcp/p80__fd00.10.180..1/80 meta l4proto tcp dnat to [fd00:10:180::1]:80
		add rule ip6 kube-proxy external-ULMVA6XW-ns1/svc1/tcp/p80 mark set mark or 0x4000
		add rule ip6 kube-proxy external-ULMVA6XW-ns1/svc1/tcp/p80 goto service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip6 kube-proxy filter-forward ct state new jump service-endpoints-check
		add rule ip6 kube-proxy filter-forward ct state new jump cluster-ips-check
		add rule ip6 kube-proxy filter-input ct state new jump nodeport-endpoints-check
		add rule ip6 kube-proxy filter-input ct state new jump service-endpoints-check
		add rule ip6 kube-proxy filter-output ct state new jump service-endpoints-check
		add rule ip6 kube-proxy filter-output-pre-dnat ct state new jump firewall-check
		add rule ip6 kube-proxy filter-output-post-dnat ct state new jump cluster-ips-check
		add rule ip6 kube-proxy filter-prerouting-pre-dnat ct state new jump firewall-check
		add rule ip6 kube-proxy firewall-check ip6 daddr . meta l4proto . th dport vmap @firewall-ips
		add rule ip6 kube-proxy masquerading mark and 0x4000 != 0 mark set mark xor 0x4000 masquerade fully-random
		add rule ip6 kube-proxy nat-output jump services
		add rule ip6 kube-proxy nat-postrouting jump masquerading
		add rule ip6 kube-proxy nat-prerouting jump services
		add rule ip6 kube-proxy nodeport-endpoints-check ip6 daddr @nodeport-ips meta l4proto . th dport vmap @no-endpoint-nodeports
		add rule ip6 kube-proxy reject-chain reject
		add rule ip6 kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 ip6 daddr fd00:172:30::41 tcp dport 80 ip6 saddr != fd00:10::/64 mark set mark or 0x4000
		add rule ip6 kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 numgen random mod 2 vmap { 0 : goto endpoint-ZVRFLKHO-ns1/svc1/tcp/p80__fd00.10.180..1/80 , 1 : goto endpoint-2CRNCTTE-ns1/svc1/tcp/p80__fd00.10.180..2.1/80 }
		add rule ip6 kube-proxy service-endpoints-check ip6 daddr . meta l4proto . th dport vmap @no-endpoint-services
		add rule ip6 kube-proxy services ip6 daddr . meta l4proto . th dport vmap @service-ips
		add rule ip6 kube-proxy services ip6 daddr @nodeport-ips meta l4proto . th dport vmap @service-nodeports
		add element ip6 kube-proxy cluster-ips { fd00:172:30::41 }
		add element ip6 kube-proxy nodeport-ips { 2001:db8::1 }
		add element ip6 kube-proxy nodeport-ips { 2001:db8:1::2 }
		add element ip6 kube-proxy service-ips { fd00:172:30::41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip6 kube-proxy service-nodeports { tcp . 3001 : goto external-ULMVA6XW-ns1/svc1/tcp/p80 }
		`)

	nft := knftables.NewFake(knftables.IPv6Family, "kube-proxy")
	err := nft.ParseDump(rules)
	if err != nil {
		t.Fatalf("failed to parse given nftables rules: %v", err)
	}
	// 解析正确性先行校验；两个端点都收集到一个输出字符串中便于期望值书写。
	assertNFTablesTransactionEqual(t, getLine(), rules, nft.Dump())
	output := "[fd00:10:180::1]:80, [fd00:10:180::2:1]:80"

	runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP",
			sourceIP: "fd00:10::2",
			destIP:   "fd00:172:30::41",
			destPort: 80,
			output:   output,
			masq:     false,
		},
		// 外部访问 NodePort 需要伪装，确保跨节点回程地址正确。
		{
			name:     "external to nodePort",
			sourceIP: "2600:5200::1",
			destIP:   testNodeIPv6,
			destPort: 3001,
			output:   output,
			masq:     true,
		},
		// 节点自身访问本机 NodePort 同样走伪装路径；
		// V6 规则中无 hairpin 表查，靠源目的相同即可覆盖此路径。
		{
			name:     "node to nodePort",
			sourceIP: testNodeIPv6,
			destIP:   testNodeIPv6,
			destPort: 3001,
			output:   output,
			masq:     true,
		},
	})
}
