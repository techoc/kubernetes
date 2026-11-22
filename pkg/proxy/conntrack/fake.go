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
	"sync"

	"github.com/vishvananda/netlink"
)

// NewFake 创建一个基于内存 fakeHandler 的 Interface，供测试使用。
func NewFake() Interface {
	return newConntracker(
		&fakeHandler{
			entries: make([]*netlink.ConntrackFlow, 0),
		},
	)
}

var _ netlinkHandler = (*fakeHandler)(nil)

// fakeHandler 在内存中维护 conntrack 表项并统计 netlink 请求次数，
// 用于在不依赖真实内核 conntrack 的情况下测试 conntracker。
type fakeHandler struct {
	mu              sync.Mutex
	entries         []*netlink.ConntrackFlow
	netlinkRequests int // 统计 netlink 请求次数，用于估算真实实现的请求数量
}

// ConntrackTableList 返回当前内存中的全部表项，并记录一次 netlink 请求。
func (f *fakeHandler) ConntrackTableList(_ netlink.ConntrackTableType, _ netlink.InetFamily) ([]*netlink.ConntrackFlow, error) {
	// fake 也可能被并发调用，需加锁保护共享的表项和计数器。
	f.mu.Lock()
	defer f.mu.Unlock()
	f.netlinkRequests++
	return f.entries, nil
}

// ConntrackDeleteFilters 按过滤器删除匹配的表项，
// 并按真实 netlink 实现的行为记录请求次数。
func (f *fakeHandler) ConntrackDeleteFilters(tableType netlink.ConntrackTableType, family netlink.InetFamily, netlinkFilters ...netlink.CustomConntrackFilter) (uint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 转储整张表需要 1 次 netlink 请求。
	// https://github.com/vishvananda/netlink/blob/0af32151e72b990c271ef6268e8aadb7e015f2bd/conntrack_linux.go#L163
	f.netlinkRequests++
	var dataplaneFlows []*netlink.ConntrackFlow
	// 记录删除前的数量，据此计算实际删除的表项数。
	before := len(f.entries)

	for _, flow := range f.entries {
		var matched bool
		for _, filter := range netlinkFilters {
			matched = filter.MatchConntrackFlow(flow)
			if matched {
				// 删除每条流需要 1 次 netlink 请求。
				// https://github.com/vishvananda/netlink/blob/0af32151e72b990c271ef6268e8aadb7e015f2bd/conntrack_linux.go#L182
				f.netlinkRequests++
			}
		}
		// 没有过滤器匹配时保留该表项。
		if !matched {
			dataplaneFlows = append(dataplaneFlows, flow)
		}
	}
	f.entries = dataplaneFlows
	return uint(before - len(f.entries)), nil
}
