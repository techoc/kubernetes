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

package ipvs

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
)

const (
	rsCheckDeleteInterval = 1 * time.Minute
)

// listItem 保存真实服务器（real server）信息。
// 如果没有特殊情况发生，真实服务器会在处理完成后被删除。
// listItem 保存后端真实服务器（RS）的信息及其处理时间。
// 若无特殊情况，RS 会在处理时间到达后被删除。
type listItem struct {
	VirtualServer *utilipvs.VirtualServer
	RealServer    *utilipvs.RealServer
}

// String 返回唯一的真实服务器名称（包含虚拟服务器信息）。
// String 返回包含虚拟服务信息的 RS 唯一名称
func (g *listItem) String() string {
	return GetUniqueRSName(g.VirtualServer, g.RealServer)
}

// GetUniqueRSName 返回一个包含虚拟服务器（VS）信息的字符串类型的唯一 RS 名称。
// GetUniqueRSName 返回包含虚拟服务信息的字符串形式 RS 唯一名称
func GetUniqueRSName(vs *utilipvs.VirtualServer, rs *utilipvs.RealServer) string {
	return vs.String() + "/" + rs.String()
}

// graceTerminateRSList 是等待优雅终止的 RS 列表，lock 保护其并发访问。
type graceTerminateRSList struct {
	lock sync.Mutex
	list map[string]*listItem
}

// add 向 RS 列表添加一个新元素；如果元素已存在则返回 false。
// add 向 rsList 中加入一个新元素
func (q *graceTerminateRSList) add(rs *listItem) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	uniqueRS := rs.String()
	if _, ok := q.list[uniqueRS]; ok {
		return false
	}

	klog.V(5).InfoS("Adding real server to graceful delete real server list", "realServer", rs)
	q.list[uniqueRS] = rs
	return true
}

// remove 从 RS 列表中移除一个元素；如果元素不存在则返回 false。
// remove 从 rsList 中移除一个元素
func (q *graceTerminateRSList) remove(rs *listItem) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	uniqueRS := rs.String()
	if _, ok := q.list[uniqueRS]; ok {
		delete(q.list, uniqueRS)
		return true
	}
	return false
}

// len 返回列表中的元素数量。
func (q *graceTerminateRSList) len() int {
	q.lock.Lock()
	defer q.lock.Unlock()

	return len(q.list)
}

// flushList 遍历列表并对每个元素调用 handler 尝试删除；
// 只要有一次删除出错就返回 false，但不会中断对其余元素的处理。
// 调用方须持有 q.lock，避免处理过程中列表被并发修改。
func (q *graceTerminateRSList) flushList(handler func(rsToDelete *listItem) (bool, error)) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	success := true
	for name, rs := range q.list {
		deleted, err := handler(rs)
		if err != nil {
			klog.ErrorS(err, "Error in deleting real server", "realServer", name)
			success = false
		}
		if deleted {
			klog.InfoS("Removed real server from graceful delete real server list", "realServer", name)
			delete(q.list, rs.String())
		}
	}
	return success
}

// exist 检查指定的唯一 RS 名称是否在列表中，并返回对应元素。
// exist 检查指定的唯一 RS 是否在 rsList 中
func (q *graceTerminateRSList) exist(uniqueRS string) (*listItem, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if rs, ok := q.list[uniqueRS]; ok {
		return rs, true
	}
	return nil, false
}

// GracefulTerminationManager 管理 RS 的优雅终止信息并执行优雅终止工作。
// rsList 是等待优雅终止的 RS 列表，ipvs 是用于执行 IPVS 删除/更新操作的接口。
// GracefulTerminationManager 管理 RS 的优雅终止信息并执行优雅终止工作。
// rsList 是待优雅终止的 RS 列表，ipvs 接口用于执行实际的 IPVS 删除/更新操作。
type GracefulTerminationManager struct {
	rsList graceTerminateRSList
	ipvs   utilipvs.Interface
}

// NewGracefulTerminationManager 创建一个 gracefulTerminationManager，管理 IPVS RS 的优雅终止工作。
// NewGracefulTerminationManager 创建一个管理器，负责 IPVS RS 的优雅终止工作
func NewGracefulTerminationManager(ipvs utilipvs.Interface) *GracefulTerminationManager {
	l := make(map[string]*listItem)
	return &GracefulTerminationManager{
		rsList: graceTerminateRSList{
			list: l,
		},
		ipvs: ipvs,
	}
}

// InTerminationList 检查指定的唯一 RS 名称是否处于优雅终止列表中。
// InTerminationList 检查指定的 RS 唯一名称是否在优雅终止列表中
func (m *GracefulTerminationManager) InTerminationList(uniqueRS string) bool {
	_, exist := m.rsList.exist(uniqueRS)
	return exist
}

// GracefulDeleteRS 将 RS 权重更新为 0，并加入优雅终止列表。
// GracefulDeleteRS 将 RS 权重置为 0，并加入优雅终止列表
func (m *GracefulTerminationManager) GracefulDeleteRS(vs *utilipvs.VirtualServer, rs *utilipvs.RealServer) error {
	// 先尝试直接删除 RS；若仍有活跃连接，再将其加入优雅终止列表等待权重归零
	ele := &listItem{
		VirtualServer: vs,
		RealServer:    rs,
	}
	deleted, err := m.deleteRsFunc(ele)
	if err != nil {
		klog.ErrorS(err, "Error in deleting real server", "realServer", ele)
	}
	if deleted {
		return nil
	}
	rs.Weight = 0
	err = m.ipvs.UpdateRealServer(vs, rs)
	if err != nil {
		return err
	}
	klog.V(5).InfoS("Adding real server to graceful delete real server list", "realServer", ele)
	m.rsList.add(ele)
	return nil
}

// deleteRsFunc 尝试删除一个真实服务器：
// 当 RS 上仍有连接且协议需要优雅终止时跳过删除，否则立即删除。
func (m *GracefulTerminationManager) deleteRsFunc(rsToDelete *listItem) (bool, error) {
	klog.V(5).InfoS("Trying to delete real server", "realServer", rsToDelete)
	rss, err := m.ipvs.GetRealServers(rsToDelete.VirtualServer)
	if err != nil {
		return false, err
	}
	for _, rs := range rss {
		if rsToDelete.RealServer.Equal(rs) {
			// UDP 和 SCTP 无法优雅终止，因此立即删除 RS
			// （sysctlExpireNoDestConn=1，现有连接会在下一个报文到来时被清除）。
			// 其他协议则需等待所有连接过期后再删除。
			if utilipvs.IsRsGracefulTerminationNeeded(rsToDelete.VirtualServer.Protocol) && rs.ActiveConn+rs.InactiveConn != 0 {
				klog.V(5).InfoS("Skip deleting real server till all connection have expired", "realServer", rsToDelete, "activeConnection", rs.ActiveConn, "inactiveConnection", rs.InactiveConn)
				return false, nil
			}
			klog.V(5).InfoS("Deleting real server", "realServer", rsToDelete)
			err := m.ipvs.DeleteRealServer(rsToDelete.VirtualServer, rs)
			if err != nil {
				return false, fmt.Errorf("delete destination %q err: %w", rs.String(), err)
			}
			return true, nil
		}
	}
	return true, fmt.Errorf("failed to delete rs %q, can't find the real server", rsToDelete.String())
}

// tryDeleteRs 尝试删除优雅终止列表中的所有 RS。
func (m *GracefulTerminationManager) tryDeleteRs() {
	if !m.rsList.flushList(m.deleteRsFunc) {
		klog.ErrorS(nil, "Try flush graceful termination list error")
	}
}

// MoveRSOutofGracefulDeleteList 立即删除指定 RS 并从列表中移除。
// MoveRSOutofGracefulDeleteList 立即删除 RS 并将其从 rsList 中移除
func (m *GracefulTerminationManager) MoveRSOutofGracefulDeleteList(uniqueRS string) error {
	rsToDelete, find := m.rsList.exist(uniqueRS)
	if !find || rsToDelete == nil {
		return fmt.Errorf("failed to find rs: %q", uniqueRS)
	}
	err := m.ipvs.DeleteRealServer(rsToDelete.VirtualServer, rsToDelete.RealServer)
	if err != nil {
		return err
	}
	m.rsList.remove(rsToDelete)
	return nil
}

// Run 启动一个 goroutine，每隔一分钟尝试删除优雅终止列表中的 RS。
// Run 启动一个 goroutine，每分钟尝试清理优雅终止列表中的 RS
func (m *GracefulTerminationManager) Run() {
	go wait.Until(m.tryDeleteRs, rsCheckDeleteInterval, wait.NeverStop)
}
