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

package testing

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/util/iptables"
)

// FakeIPTables 是 iptables.Interface 的内存实现，不执行任何真实 iptables 命令。
type FakeIPTables struct {
	protocol iptables.Protocol

	Dump *IPTablesDump
}

// NewFake 返回一个 IPv4 的空实现，预置了 nat、filter 和 mangle 表。
func NewFake() *FakeIPTables {
	f := &FakeIPTables{
		protocol: iptables.ProtocolIPv4,
		Dump: &IPTablesDump{
			Tables: []Table{
				{
					Name: iptables.TableNAT,
					Chains: []Chain{
						{Name: iptables.ChainPrerouting},
						{Name: iptables.ChainInput},
						{Name: iptables.ChainOutput},
						{Name: iptables.ChainPostrouting},
					},
				},
				{
					Name: iptables.TableFilter,
					Chains: []Chain{
						{Name: iptables.ChainInput},
						{Name: iptables.ChainForward},
						{Name: iptables.ChainOutput},
					},
				},
				{
					Name:   iptables.TableMangle,
					Chains: []Chain{},
				},
			},
		},
	}

	return f
}

// NewIPv6Fake 返回一个 IsIPv6() 为 true 的空实现。
func NewIPv6Fake() *FakeIPTables {
	f := NewFake()
	f.protocol = iptables.ProtocolIPv6
	return f
}

// EnsureChain 实现 iptables.Interface：确保链存在，返回是否为已存在的链。
func (f *FakeIPTables) EnsureChain(table iptables.Table, chain iptables.Chain) (bool, error) {
	t, err := f.Dump.GetTable(table)
	if err != nil {
		return false, err
	}
	if c, _ := f.Dump.GetChain(table, chain); c != nil {
		return true, nil
	}
	t.Chains = append(t.Chains, Chain{Name: chain})
	return false, nil
}

// FlushChain 实现 iptables.Interface：清空链中的规则。
func (f *FakeIPTables) FlushChain(table iptables.Table, chain iptables.Chain) error {
	if c, _ := f.Dump.GetChain(table, chain); c != nil {
		c.Rules = nil
	}
	return nil
}

// DeleteChain 实现 iptables.Interface：删除链。
func (f *FakeIPTables) DeleteChain(table iptables.Table, chain iptables.Chain) error {
	t, err := f.Dump.GetTable(table)
	if err != nil {
		return err
	}
	for i := range t.Chains {
		if t.Chains[i].Name == chain {
			t.Chains = append(t.Chains[:i], t.Chains[i+1:]...)
			return nil
		}
	}
	return nil
}

// ChainExists 实现 iptables.Interface：判断链是否存在。
func (f *FakeIPTables) ChainExists(table iptables.Table, chain iptables.Chain) (bool, error) {
	if _, err := f.Dump.GetTable(table); err != nil {
		return false, err
	}
	if c, _ := f.Dump.GetChain(table, chain); c != nil {
		return true, nil
	}
	return false, nil
}

// EnsureRule 实现 iptables.Interface：按插入位置确保规则存在，返回是否已存在。
func (f *FakeIPTables) EnsureRule(position iptables.RulePosition, table iptables.Table, chain iptables.Chain, args ...string) (bool, error) {
	c, err := f.Dump.GetChain(table, chain)
	if err != nil {
		return false, err
	}

	rule := "-A " + string(chain) + " " + strings.Join(args, " ")
	for _, r := range c.Rules {
		if r.Raw == rule {
			return true, nil
		}
	}

	parsed, err := ParseRule(rule, false)
	if err != nil {
		return false, err
	}

	if position == iptables.Append {
		c.Rules = append(c.Rules, parsed)
	} else {
		c.Rules = append([]*Rule{parsed}, c.Rules...)
	}
	return false, nil
}

// DeleteRule 实现 iptables.Interface：删除匹配的规则。
func (f *FakeIPTables) DeleteRule(table iptables.Table, chain iptables.Chain, args ...string) error {
	c, err := f.Dump.GetChain(table, chain)
	if err != nil {
		return err
	}

	rule := "-A " + string(chain) + " " + strings.Join(args, " ")
	for i, r := range c.Rules {
		if r.Raw == rule {
			c.Rules = append(c.Rules[:i], c.Rules[i+1:]...)
			break
		}
	}
	return nil
}

// IsIPv6 实现 iptables.Interface：返回是否为 IPv6 实现。
func (f *FakeIPTables) IsIPv6() bool {
	return f.protocol == iptables.ProtocolIPv6
}

// Protocol 实现 iptables.Interface：返回协议族。
func (f *FakeIPTables) Protocol() iptables.Protocol {
	return f.protocol
}

// saveTable 将指定表按 iptables-save 格式写入 buffer。
func (f *FakeIPTables) saveTable(table iptables.Table, buffer *bytes.Buffer) error {
	t, err := f.Dump.GetTable(table)
	if err != nil {
		return err
	}

	fmt.Fprintf(buffer, "*%s\n", table)
	for _, c := range t.Chains {
		fmt.Fprintf(buffer, ":%s - [%d:%d]\n", c.Name, c.Packets, c.Bytes)
	}
	for _, c := range t.Chains {
		for _, r := range c.Rules {
			fmt.Fprintf(buffer, "%s\n", r.Raw)
		}
	}
	fmt.Fprintf(buffer, "COMMIT\n")
	return nil
}

// SaveInto 实现 iptables.Interface：将规则转储写入 buffer。
func (f *FakeIPTables) SaveInto(table iptables.Table, buffer *bytes.Buffer) error {
	if table == "" {
		// 对 API 的隐式扩展：FakeIPTables 将 table="" 解释为“所有表”。
		for i := range f.Dump.Tables {
			err := f.saveTable(f.Dump.Tables[i].Name, buffer)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return f.saveTable(table, buffer)
}

// 内置 target 列表并不完整，但足以让单测通过。
var builtinTargets = sets.New("ACCEPT", "DROP", "RETURN", "REJECT", "DNAT", "SNAT", "MASQUERADE", "MARK")

// restoreTable 按 restore 参数将 newTable 合入当前转储，并校验所有跳转目标是否有效。
// 校验失败时回滚到合并前的链集合，避免留下半更新状态。
func (f *FakeIPTables) restoreTable(newDump *IPTablesDump, newTable *Table, flush iptables.FlushFlag, counters iptables.RestoreCountersFlag) error {
	oldTable, err := f.Dump.GetTable(newTable.Name)
	if err != nil {
		return err
	}

	backupChains := make([]Chain, len(oldTable.Chains))
	copy(backupChains, oldTable.Chains)

	// 先更新内部状态，稍后再统一校验跳转目标。
	if flush == iptables.FlushTables {
		oldTable.Chains = make([]Chain, 0, len(newTable.Chains))
	}
	for _, newChain := range newTable.Chains {
		oldChain, _ := f.Dump.GetChain(newTable.Name, newChain.Name)
		switch {
		case oldChain == nil && newChain.Deleted:
			// 删除本就不存在的链，无需操作。
		case oldChain == nil && !newChain.Deleted:
			oldTable.Chains = append(oldTable.Chains, newChain)
		case oldChain != nil && newChain.Deleted:
			_ = f.DeleteChain(newTable.Name, newChain.Name)
		case oldChain != nil && !newChain.Deleted:
			// 用新规则替换旧规则；计数器按标志决定是否同步。
			oldChain.Rules = newChain.Rules
			if counters == iptables.RestoreCounters {
				oldChain.Packets = newChain.Packets
				oldChain.Bytes = newChain.Bytes
			}
		}
	}

	// 检查新旧规则中所有跳转目标都有效：真实 iptables-restore 会拒绝无效跳转。
	for _, chain := range oldTable.Chains {
		for _, rule := range chain.Rules {
			if rule.Jump == nil {
				continue
			}
			if builtinTargets.Has(rule.Jump.Value) {
				continue
			}

			jumpedChain, _ := f.Dump.GetChain(oldTable.Name, iptables.Chain(rule.Jump.Value))
			if jumpedChain == nil {
				newChain, _ := newDump.GetChain(oldTable.Name, iptables.Chain(rule.Jump.Value))
				if newChain != nil {
					// 旧规则跳转到被本次 restore 删除的链。
					oldTable.Chains = backupChains
					return fmt.Errorf("deleted chain %q is referenced by existing rules", newChain.Name)
				} else {
					// 新规则跳转到既未创建也不存在的链。
					oldTable.Chains = backupChains
					return fmt.Errorf("rule %q jumps to a non-existent chain", rule.Raw)
				}
			}
		}
	}

	return nil
}

// Restore 实现 iptables.Interface：恢复单个表。
func (f *FakeIPTables) Restore(table iptables.Table, data []byte, flush iptables.FlushFlag, counters iptables.RestoreCountersFlag) error {
	dump, err := ParseIPTablesDump(string(data))
	if err != nil {
		return err
	}

	newTable, err := dump.GetTable(table)
	if err != nil {
		return err
	}

	return f.restoreTable(dump, newTable, flush, counters)
}

// RestoreAll 实现 iptables.Interface：恢复转储中的所有表。
func (f *FakeIPTables) RestoreAll(data []byte, flush iptables.FlushFlag, counters iptables.RestoreCountersFlag) error {
	dump, err := ParseIPTablesDump(string(data))
	if err != nil {
		return err
	}

	for i := range dump.Tables {
		err = f.restoreTable(dump, &dump.Tables[i], flush, counters)
		if err != nil {
			return err
		}
	}
	return nil
}

// Monitor 实现 iptables.Interface：空实现，不启动监控。
func (f *FakeIPTables) Monitor(canary iptables.Chain, tables []iptables.Table, reloadFunc func(), interval time.Duration, stopCh <-chan struct{}) {
}

// Present 实现 iptables.Interface：空实现，始终认为 iptables 可用。
func (f *FakeIPTables) Present() error {
	return nil
}

var _ = iptables.Interface(&FakeIPTables{})
