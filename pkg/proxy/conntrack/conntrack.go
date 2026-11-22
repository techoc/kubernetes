//go:build linux

/*
Copyright 2016 The Kubernetes Authors.

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

package conntrack

import (
	"fmt"

	"github.com/vishvananda/netlink"

	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/proxy/util"
)

// Interface 封装 conntrack（连接跟踪）表的查询与删除操作。
type Interface interface {
	ListEntries(ipFamily uint8) ([]*netlink.ConntrackFlow, error)
	// DeleteEntries 删除给定的 conntrack 表项。
	DeleteEntries(ipFamily uint8, flows []*netlink.ConntrackFlow) (int, error)
}

// netlinkHandler 抽象底层 netlink 调用，使生产代码使用真实实现，
// 测试时可以注入 mock 实现。
type netlinkHandler interface {
	ConntrackTableList(netlink.ConntrackTableType, netlink.InetFamily) ([]*netlink.ConntrackFlow, error)
	ConntrackDeleteFilters(netlink.ConntrackTableType, netlink.InetFamily, ...netlink.CustomConntrackFilter) (uint, error)
}

// conntracker 通过 netlink API 实现 Interface。
type conntracker struct {
	handler netlinkHandler
}

var _ Interface = &conntracker{}

// New 创建一个使用真实 netlink Handle 的 Interface 实现。
func New() Interface {
	return newConntracker(&netlink.Handle{})
}

// newConntracker 用给定的 netlinkHandler 构造 conntracker，主要用于在测试中注入 mock。
func newConntracker(handler netlinkHandler) Interface {
	return &conntracker{handler: handler}
}

// ListEntries 列出指定 IP 族的全部 conntrack 表项。
func (ct *conntracker) ListEntries(ipFamily uint8) (entries []*netlink.ConntrackFlow, err error) {
	// netlink 调用可能被信号中断（EINTR），需按统一策略重试。
	err = retry.OnError(util.MaxAttemptsEINTR, util.ShouldRetryOnEINTR, func() error {
		entries, err = ct.handler.ConntrackTableList(netlink.ConntrackTable, netlink.InetFamily(ipFamily))
		return err
	})
	return entries, err
}

// DeleteEntries 删除指定 IP 族中给定的 conntrack 表项。
// 返回实际删除的表项数量；出错时同时返回错误。
func (ct *conntracker) DeleteEntries(ipFamily uint8, flows []*netlink.ConntrackFlow) (int, error) {
	if len(flows) == 0 {
		// 没有待删除表项时直接返回，避免向 netlink 发送无意义的请求。
		return 0, nil
	}

	// 将多个流合并为一个过滤器，减少与 netlink 的交互次数。
	filter := newFlowFilter(flows)
	var n uint
	var err error
	// 删除同样可能因 EINTR 失败而重试，重试时累计已删除的数量。
	err = retry.OnError(util.MaxAttemptsEINTR, util.ShouldRetryOnEINTR, func() error {
		var count uint
		count, err = ct.handler.ConntrackDeleteFilters(netlink.ConntrackTable, netlink.InetFamily(ipFamily), filter)
		// 即使后续重试失败，之前已删除的表项也要计入总数。
		n += count
		return err
	})

	if err != nil {
		return int(n), fmt.Errorf("error deleting conntrack entries, error: %w", err)
	}
	return int(n), nil
}
