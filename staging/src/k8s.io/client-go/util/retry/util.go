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

// DefaultRetry is the recommended retry for a conflict where multiple clients
// are making changes to the same resource.
//
// 步数多、间隔短、近似固定间隔，适合“抢同一对象”的高频冲突：
// 尽快重读并重试通常比指数退避更有效。
var DefaultRetry = wait.Backoff{
	Steps:    5,
	Duration: 10 * time.Millisecond,
	Factor:   1.0,
	Jitter:   0.1,
}

// DefaultBackoff is the recommended backoff for a conflict where a client
// may be attempting to make an unrelated modification to a resource under
// active management by one or more controllers.
//
// 步数少、间隔指数增长，适合与控制器“抢写”的场景：给控制器留出处理时间，
// 避免客户端持续制造冲突。
var DefaultBackoff = wait.Backoff{
	Steps:    4,
	Duration: 10 * time.Millisecond,
	Factor:   5.0,
	Jitter:   0.1,
}

// OnError allows the caller to retry fn in case the error returned by fn is retriable
// according to the provided function. backoff defines the maximum retries and the wait
// interval between two retries.
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

// RetryOnConflict is used to make an update to a resource when you have to worry about
// conflicts caused by other code making unrelated updates to the resource at the same
// time. fn should fetch the resource to be modified, make appropriate changes to it, try
// to update it, and return (unmodified) the error from the update function. On a
// successful update, RetryOnConflict will return nil. If the update function returns a
// "Conflict" error, RetryOnConflict will wait some amount of time as described by
// backoff, and then try again. On a non-"Conflict" error, or if it retries too many times
// and gives up, RetryOnConflict will return an error to the caller.
//
//	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
//	    // Fetch the resource here; you need to refetch it on every try, since
//	    // if you got a conflict on the last update attempt then you need to get
//	    // the current version before making your own changes.
//	    pod, err := c.Pods("mynamespace").Get(name, metav1.GetOptions{})
//	    if err != nil {
//	        return err
//	    }
//
//	    // Make whatever updates to the resource are needed
//	    pod.Status.Phase = v1.PodFailed
//
//	    // Try to update
//	    _, err = c.Pods("mynamespace").UpdateStatus(pod)
//	    // You have to return err itself here (not wrapped inside another error)
//	    // so that RetryOnConflict can identify it correctly.
//	    return err
//	})
//	if err != nil {
//	    // May be conflict if max retries were hit, or may be something unrelated
//	    // like permissions or a network error
//	    return err
//	}
//	...
//
// TODO: Make Backoff an interface?
//
// RetryOnConflict 只在 errors.IsConflict 时重试；fn 必须在每次尝试中
// 重新读取对象的最新版本，否则会一直用旧 resourceVersion 触发冲突。
func RetryOnConflict(backoff wait.Backoff, fn func() error) error {
	return OnError(backoff, errors.IsConflict, fn)
}
