/*
Copyright The Kubernetes Authors.

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

package runtime

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"
)

// HandleError logs an asynchronous error.
// 异步路径拿不到请求上下文，只能退回到 Background logger；调用方若手上有 ctx，
// 优先用 HandleErrorWithContext，日志才能带上 trace/请求 ID。
func HandleError(err error) {
	if err == nil {
		return
	}
	klog.Background().Error(err, "Unhandled Error")
}

// HandleErrorWithContext logs an asynchronous error with contextual logging when available.
// 这里的“when available”指 ctx 里是否挂了 logger；没有就退回默认 logger，
// 所以调用方传 context.Background() 也不会 panic。
func HandleErrorWithContext(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	if err == nil {
		return
	}
	klog.FromContext(ctx).Error(err, msg, keysAndValues...)
}

// HandleCrash recovers from panic and logs it.
// 仅在 defer 中调用才有意义；不在 defer 里调用时 recover() 恒为 nil，等于空操作。
func HandleCrash() {
	HandleCrashWithLogger(klog.Background())
}

// HandleCrashWithContext recovers from panic and logs it with the context logger.
// 附加处理器先跑，让调用方有机会在日志落盘前补充自己的现场信息（比如上报指标），
// 且它们拿到的是原始 panic 值，不做 fmt.Errorf 包装。
func HandleCrashWithContext(ctx context.Context, additionalHandlers ...func(context.Context, interface{})) {
	if r := recover(); r != nil {
		for _, fn := range additionalHandlers {
			fn(ctx, r)
		}
		klog.FromContext(ctx).Error(fmt.Errorf("%v", r), "Observed a panic")
	}
}

// HandleCrashWithLogger recovers from panic and logs it using the provided logger.
// 与 HandleCrashWithContext 的区别是不支持附加处理器；给已有明确 logger 的调用点省一次 ctx 查找。
func HandleCrashWithLogger(logger klog.Logger) {
	if r := recover(); r != nil {
		logger.Error(fmt.Errorf("%v", r), "Observed a panic")
	}
}
