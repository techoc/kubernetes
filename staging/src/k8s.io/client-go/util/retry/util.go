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

package retry

import (
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
)

// DefaultRetry 是多个客户端同时修改同一资源发生冲突时的推荐重试策略。
//
// 步数多、间隔短、近似固定间隔，适合“抢同一对象”的高频冲突：
// 尽快重读并重试通常比指数退避更有效。
var DefaultRetry = wait.Backoff{
	Steps:    5,
	Duration: 10 * time.Millisecond,
	Factor:   1.0,
	Jitter:   0.1,
}

// DefaultBackoff 是客户端可能修改正被一个或多个控制器管理的资源时的推荐退避策略。
//
// 步数少、间隔指数增长，适合与控制器“抢写”的场景：给控制器留出处理时间，
// 避免客户端持续制造冲突。
var DefaultBackoff = wait.Backoff{
	Steps:    4,
	Duration: 10 * time.Millisecond,
	Factor:   5.0,
	Jitter:   0.1,
}

// OnError 在 fn 返回的错误被给定函数判定为可重试时重试 fn。backoff 定义最大重试
// 次数与两次重试之间的等待间隔。
//
// 一旦遇到不可重试错误立即返回原错误；若重试次数耗尽，则返回最后一次
// 可重试错误，而不是 wait.ErrWaitTimeout，便于调用方判断根因。
func OnError(backoff wait.Backoff, retriable func(error) bool, fn func() error) error {
	var lastErr error
	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		err := fn()
		switch {
		case err == nil:
			return true, nil
		case retriable(err):
			lastErr = err
			return false, nil
		default:
			return false, err
		}
	})
	if wait.Interrupted(err) && lastErr != nil {
		err = lastErr
	}
	return err
}

// RetryOnConflict 用于更新可能被其他代码并发更新而冲突的资源。fn 应读取待修改资源、
// 做修改、尝试更新，并原样返回更新错误。更新成功返回 nil；遇到 Conflict 则按 backoff
// 等待后重试；遇到非 Conflict 错误或重试次数用尽则返回错误。
//
//	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
//
// 在这里读取资源；每次尝试都必须重新读取，因为上次更新冲突后需要拿到最新版本再改。
//
//	pod, err := c.Pods("mynamespace").Get(name, metav1.GetOptions{})
//	if err != nil {
//	    return err
//	}
//
// 对资源做所需的修改。
//
//	pod.Status.Phase = v1.PodFailed
//
// 尝试更新。
//
//	_, err = c.Pods("mynamespace").UpdateStatus(pod)
//
// 必须原样返回 err（不要包装），否则 RetryOnConflict 无法正确识别冲突。
//
//	    return err
//	})
//	if err != nil {
//
// 可能是重试次数用尽后的冲突，也可能是权限、网络等其他错误。
//
//	    return err
//	}
//	...
//
// TODO: 是否应把 Backoff 改为接口？
//
// RetryOnConflict 只在 errors.IsConflict 时重试；fn 必须在每次尝试中
// 重新读取对象的最新版本，否则会一直用旧 resourceVersion 触发冲突。
func RetryOnConflict(backoff wait.Backoff, fn func() error) error {
	return OnError(backoff, errors.IsConflict, fn)
}
