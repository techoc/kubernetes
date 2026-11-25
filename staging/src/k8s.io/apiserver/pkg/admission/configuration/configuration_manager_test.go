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

package configuration

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TestTolerateBootstrapFailure 验证启动阶段的“bootstrap 宽限期”行为：
// 在宽限期内，底层配置源持续失败也不应把错误暴露给调用方，只要随后恢复即可正常返回。
func TestTolerateBootstrapFailure(t *testing.T) {
	var fakeGetSucceed bool
	var fakeGetSucceedLock sync.RWMutex
	fakeGetFn := func() (runtime.Object, error) {
		fakeGetSucceedLock.RLock()
		defer fakeGetSucceedLock.RUnlock()
		if fakeGetSucceed {
			return nil, nil
		} else {
			return nil, fmt.Errorf("this error shouldn't be exposed to caller")
		}
	}
	poller := newPoller(fakeGetFn)
	// 把宽限期设得足够长，确保重试期间始终处于 bootstrap 阶段。
	poller.bootstrapGracePeriod = 100 * time.Second
	poller.bootstrapRetries = math.MaxInt32
	// 把 failureThreshold 设为 0，这样单次失败就会把 ready 置为 false。
	poller.failureThreshold = 0
	stopCh := make(chan struct{})
	defer close(stopCh)
	go poller.Run(stopCh)
	go func() {
		// 2 秒后模拟配置源恢复；即使调度延迟也不会导致测试偶发失败。
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		<-timer.C
		fakeGetSucceedLock.Lock()
		defer fakeGetSucceedLock.Unlock()
		fakeGetSucceed = true
	}()

	done := make(chan struct{})
	go func(t *testing.T) {
		// 调用方最终应当等到成功结果，而不是看到中间的重试错误。
		_, err := poller.configuration()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		close(done)
	}(t)
	<-done
}

// TestNotTolerateNonbootstrapFailure 验证一旦 bootstrap 阶段结束（已 bootstrapped），
// 后续失败必须原样返回给调用方，而不是继续被宽限期吞掉。
func TestNotTolerateNonbootstrapFailure(t *testing.T) {
	fakeGetFn := func() (runtime.Object, error) {
		return nil, fmt.Errorf("this error should be exposed to caller")
	}
	poller := newPoller(fakeGetFn)
	poller.bootstrapGracePeriod = 1 * time.Second
	poller.interval = 1 * time.Millisecond
	stopCh := make(chan struct{})
	defer close(stopCh)
	go poller.Run(stopCh)
	// 触发 bootstrap 计时器
	go poller.configuration()

	// 等待 poller 自己把 bootstrapped 置为 true。
	wait.PollInfinite(1*time.Second, func() (bool, error) {
		poller.lock.Lock()
		defer poller.lock.Unlock()
		return poller.bootstrapped, nil
	})

	_, err := poller.configuration()
	if err == nil {
		t.Errorf("unexpected no error")
	}
}
