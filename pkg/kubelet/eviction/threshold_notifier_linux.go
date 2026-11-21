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

package eviction

import (
	"context"
	"fmt"
	"sync"
	"time"

	libcontainercgroups "github.com/opencontainers/cgroups"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

const (
	// 一次成功读取 eventfd 返回的字节数，参见 eventfd(2)。
	eventSize = 8
	// 一次最多记录的事件数；EpollWait 返回更多事件时会被遗漏。
	numFdEvents = 6
)

// linuxCgroupNotifier 是 CgroupNotifier 的 Linux 实现，封装 eventfd/epoll。
type linuxCgroupNotifier struct {
	eventfd  int
	epfd     int
	stop     chan struct{}
	stopLock sync.Mutex
}

// 编译期断言：linuxCgroupNotifier 实现 CgroupNotifier 接口。
var _ CgroupNotifier = &linuxCgroupNotifier{}

// NewCgroupNotifier 返回 linuxCgroupNotifier，
// 负责建立从 cgroup 接收阈值跨越通知所需的控制结构。
func NewCgroupNotifier(logger klog.Logger, path, attribute string, threshold int64) (CgroupNotifier, error) {
	// cgroup v2 不支持通过 cgroup.event_control 监控内存阈值；
	// 长期方案应结合根 Pod cgroup 的 memory.low 与 memory.events/PSI 的 inotify 通知；
	// 目前 cgroup v2 下返回一个"禁用"的 notifier，参见 issue 106331。
	if libcontainercgroups.IsCgroup2UnifiedMode() {
		return &disabledThresholdNotifier{}, nil
	}

	// 打开被监控属性文件与 event_control 文件，并创建 eventfd/epoll。
	var watchfd, eventfd, epfd, controlfd int
	var err error
	watchfd, err = unix.Open(fmt.Sprintf("%s/%s", path, attribute), unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(watchfd)
	controlfd, err = unix.Open(fmt.Sprintf("%s/cgroup.event_control", path), unix.O_WRONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(controlfd)
	eventfd, err = unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		return nil, err
	}
	if eventfd < 0 {
		err = fmt.Errorf("eventfd call failed")
		return nil, err
	}
	defer func() {
		// 初始化后续步骤失败时关闭 eventfd，避免泄漏。
		if err != nil {
			unix.Close(eventfd)
		}
	}()
	epfd, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	if epfd < 0 {
		err = fmt.Errorf("EpollCreate1 call failed")
		return nil, err
	}
	defer func() {
		// 初始化后续步骤失败时关闭 epfd，避免泄漏。
		if err != nil {
			unix.Close(epfd)
		}
	}()
	// 把 eventfd、watchfd 与阈值写入 cgroup.event_control，完成注册。
	config := fmt.Sprintf("%d %d %d", eventfd, watchfd, threshold)
	_, err = unix.Write(controlfd, []byte(config))
	if err != nil {
		return nil, err
	}
	return &linuxCgroupNotifier{
		eventfd: eventfd,
		epfd:    epfd,
		stop:    make(chan struct{}),
	}, nil
}

// Start 把 eventfd 加入 epoll，并循环等待阈值事件。
func (n *linuxCgroupNotifier) Start(ctx context.Context, eventCh chan<- struct{}) {
	logger := klog.FromContext(ctx)
	// 把 eventfd 注册到 epoll，监听可读事件。
	err := unix.EpollCtl(n.epfd, unix.EPOLL_CTL_ADD, n.eventfd, &unix.EpollEvent{
		Fd:     int32(n.eventfd),
		Events: unix.EPOLLIN,
	})
	if err != nil {
		logger.Info("Eviction manager: error adding epoll eventfd", "err", err)
		return
	}
	buf := make([]byte, eventSize)
	// 循环等待事件，直到 Stop 关闭或出错。
	for {
		select {
		case <-n.stop:
			return
		default:
		}
		event, err := wait(n.epfd, n.eventfd, notifierRefreshInterval)
		if err != nil {
			logger.Info("Eviction manager: error while waiting for memcg events", "err", err)
			return
		} else if !event {
			// 等待超时属于正常情况，说明阈值未被跨越。
			continue
		}
		// 消费 eventfd 中的事件，避免重复触发。
		_, err = unix.Read(n.eventfd, buf)
		if err != nil {
			logger.Info("Eviction manager: error reading memcg events", "err", err)
			return
		}
		eventCh <- struct{}{}
	}
}

// wait 在 notifierRefreshInterval 内等待 eventfd 的 epoll 事件；
// 出错时返回错误，需要读取 eventfd 时返回 true。
func wait(epfd, eventfd int, timeout time.Duration) (bool, error) {
	events := make([]unix.EpollEvent, numFdEvents+1)
	timeoutMS := int(timeout / time.Millisecond)
	n, err := unix.EpollWait(epfd, events, timeoutMS)
	// EINTR 表示被信号中断，忽略即可。
	if n == -1 {
		if err == unix.EINTR {
			// Interrupt, ignore the error
			return false, nil
		}
		return false, err
	}
	// 超时。
	if n == 0 {
		// Timeout
		return false, nil
	}
	// 返回事件数超出预期，说明缓冲区不足，直接报错。
	if n > numFdEvents {
		return false, fmt.Errorf("epoll_wait returned more events than we know what to do with")
	}
	for _, event := range events[:n] {
		if event.Fd == int32(eventfd) {
			if event.Events&unix.EPOLLHUP != 0 || event.Events&unix.EPOLLERR != 0 || event.Events&unix.EPOLLIN != 0 {
				// EPOLLHUP：不应发生，但若发生则按唤醒处理。

				// EPOLLERR：文件描述符上有错误时，假装可读，让 unix.Read 取到错误。

				// EPOLLIN：有数据可读。
				return true, nil
			}
		}
	}
	// 发生了不关心的事件。
	return false, nil
}

// Stop 关闭 eventfd/epfd 并标记已停止；重复调用安全。
func (n *linuxCgroupNotifier) Stop() {
	n.stopLock.Lock()
	defer n.stopLock.Unlock()
	select {
	case <-n.stop:
		// notifier 已经停止。
		return
	default:
	}
	unix.Close(n.eventfd)
	unix.Close(n.epfd)
	close(n.stop)
}

// disabledThresholdNotifier 是假的"禁用"通知器，所有方法都是空操作。
type disabledThresholdNotifier struct{}

// Start 空操作。
func (*disabledThresholdNotifier) Start(context.Context, chan<- struct{}) {}

// Stop 空操作。
func (*disabledThresholdNotifier) Stop() {}
