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
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TestRetryOnConflict 覆盖 RetryOnConflict 的四种典型路径：
// 一直冲突到耗尽步数、首次即成功、遇到非冲突错误立即返回、中途冲突后成功。
func TestRetryOnConflict(t *testing.T) {
	opts := wait.Backoff{Factor: 1.0, Steps: 3}
	conflictErr := errors.NewConflict(schema.GroupResource{Resource: "test"}, "other", nil)

	// never returns
	// 冲突错误会持续重试，步数耗尽后把最后一次冲突错误原样返回。
	err := RetryOnConflict(opts, func() error {
		return conflictErr
	})
	if err != conflictErr {
		t.Errorf("unexpected error: %v", err)
	}

	// returns immediately
	// 首次成功即停止，函数恰好只被调用一次。
	i := 0
	err = RetryOnConflict(opts, func() error {
		i++
		return nil
	})
	if err != nil || i != 1 {
		t.Errorf("unexpected error: %v", err)
	}

	// returns immediately on error
	// 非冲突错误不属于可重试范畴，必须立即透出。
	testErr := fmt.Errorf("some other error")
	err = RetryOnConflict(opts, func() error {
		return testErr
	})
	if err != testErr {
		t.Errorf("unexpected error: %v", err)
	}

	// keeps retrying
	// 前两次冲突、第三次成功，验证重试计数与最终 nil 结果。
	i = 0
	err = RetryOnConflict(opts, func() error {
		if i < 2 {
			i++
			return errors.NewConflict(schema.GroupResource{Resource: "test"}, "other", nil)
		}
		return nil
	})
	if err != nil || i != 2 {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestRetryOnConflictPreservesContextErrorWithoutLastError 验证当操作返回
// context.Canceled 时错误链未被包装替换，调用方仍可用 errors.Is 识别取消。
func TestRetryOnConflictPreservesContextErrorWithoutLastError(t *testing.T) {
	err := RetryOnConflict(DefaultBackoff, func() error {
		return context.Canceled
	})
	if !stderrors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
