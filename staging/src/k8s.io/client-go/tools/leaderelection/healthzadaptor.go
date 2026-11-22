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

// HealthzAdaptor associates the /healthz endpoint with the LeaderElection object.
// It helps deal with the /healthz endpoint being set up prior to the LeaderElection.
// This contains the code needed to act as an adaptor between the leader
// election code the health check code. It allows us to provide health
// status about the leader election. Most specifically about if the leader
// has failed to renew without exiting the process. In that case we should
// report not healthy and rely on the kubelet to take down the process.
//
// 中文说明：healthz 检查可能先于 LeaderElector 完成绑定，因此用一个可延迟注入的
// 指针加锁保护。领导者丢失续约但仍存活时报告不健康，由 kubelet 重启进程，
// 避免出现两个进程都认为自己是 leader。
type HealthzAdaptor struct {
	// pointerLock 保护 le，允许健康检查与 SetLeaderElection 并发执行。
	pointerLock sync.Mutex
	le          *LeaderElector
	timeout     time.Duration
}

// Name returns the name of the health check we are implementing.
func (l *HealthzAdaptor) Name() string {
	return "leaderElection"
}

// Check is called by the healthz endpoint handler.
// It fails (returns an error) if we own the lease but had not been able to renew it.
//
// 中文：尚未绑定 LeaderElector 时视为健康，因为此时还没有参与选举。
func (l *HealthzAdaptor) Check(req *http.Request) error {
	l.pointerLock.Lock()
	defer l.pointerLock.Unlock()
	if l.le == nil {
		return nil
	}
	return l.le.Check(l.timeout)
}

// SetLeaderElection ties a leader election object to a HealthzAdaptor
//
// 中文：加锁保证与 Check 的读写不产生数据竞争。
func (l *HealthzAdaptor) SetLeaderElection(le *LeaderElector) {
	l.pointerLock.Lock()
	defer l.pointerLock.Unlock()
	l.le = le
}

// NewLeaderHealthzAdaptor creates a basic healthz adaptor to monitor a leader election.
// timeout determines the time beyond the lease expiry to be allowed for timeout.
// checks within the timeout period after the lease expires will still return healthy.
//
// 中文说明：timeout 是租约过期后仍允许的宽限期，用于吸收网络抖动或时钟偏差，
// 避免短暂续约失败就触发进程重启。
func NewLeaderHealthzAdaptor(timeout time.Duration) *HealthzAdaptor {
	result := &HealthzAdaptor{
		timeout: timeout,
	}
	return result
}
