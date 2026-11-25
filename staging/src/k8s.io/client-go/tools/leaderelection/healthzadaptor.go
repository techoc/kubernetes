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

package leaderelection

import (
	"net/http"
	"sync"
	"time"
)

// HealthzAdaptor 将 /healthz 端点与 LeaderElection 对象关联起来，
// 用于处理 /healthz 端点先于 LeaderElection 建立的场景。它作为领导者选举
// 代码与健康检查代码之间的适配器，提供领导者选举的健康状态，最典型的是
// leader 未能续约但进程仍未退出的情况——此时应报告不健康，依靠 kubelet 终止进程。
// healthz 检查可能先于 LeaderElector 完成绑定，因此用一个可延迟注入的
// 指针加锁保护；领导者丢失续约但仍存活时报告不健康，由 kubelet 重启进程，
// 避免出现两个进程都认为自己是 leader。
type HealthzAdaptor struct {
	// pointerLock 保护 le，允许健康检查与 SetLeaderElection 并发执行。
	pointerLock sync.Mutex
	le          *LeaderElector
	timeout     time.Duration
}

// Name 返回所实现的健康检查的名称。
func (l *HealthzAdaptor) Name() string {
	return "leaderElection"
}

// Check 由 healthz 端点处理器调用。
// 若我们持有租约却未能续约，则失败（返回错误）。
// 尚未绑定 LeaderElector 时视为健康，因为此时还没有参与选举。
func (l *HealthzAdaptor) Check(req *http.Request) error {
	l.pointerLock.Lock()
	defer l.pointerLock.Unlock()
	if l.le == nil {
		return nil
	}
	return l.le.Check(l.timeout)
}

// SetLeaderElection 将领导者选举对象绑定到 HealthzAdaptor。
// 加锁保证与 Check 的读写不产生数据竞争。
func (l *HealthzAdaptor) SetLeaderElection(le *LeaderElector) {
	l.pointerLock.Lock()
	defer l.pointerLock.Unlock()
	l.le = le
}

// NewLeaderHealthzAdaptor 创建一个用于监控领导者选举的基础 healthz 适配器。
// timeout 决定租约过期后仍允许超时的时长；租约过期后 timeout 期内的检查
// 仍会返回健康。timeout 是租约过期后的宽限期，用于吸收网络抖动或时钟偏差，
// 避免短暂续约失败就触发进程重启。
func NewLeaderHealthzAdaptor(timeout time.Duration) *HealthzAdaptor {
	result := &HealthzAdaptor{
		timeout: timeout,
	}
	return result
}
