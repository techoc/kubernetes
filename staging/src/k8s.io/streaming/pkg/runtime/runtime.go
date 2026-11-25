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

// HandleError 记录异步执行过程中发生的错误。
// 该接口不接收上下文，只能使用 klog.Background()；调用方若持有 ctx，
// 应改用 HandleErrorWithContext，让日志关联请求上下文。
func HandleError(err error) {
	if err == nil {
		return
	}
	klog.Background().Error(err, "Unhandled Error")
}

// HandleErrorWithContext 使用 ctx 中的日志记录器记录异步错误。
// ctx 未关联日志记录器时，klog.FromContext 会返回后台日志记录器，
// 因此传入 context.Background() 也不会 panic。
func HandleErrorWithContext(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	if err == nil {
		return
	}
	klog.FromContext(ctx).Error(err, msg, keysAndValues...)
}

// HandleCrash 从 panic 中恢复，并使用后台日志记录器记录异常。
// 必须直接由 defer 调用；普通调用中的 recover() 恒为 nil，等于空操作。
func HandleCrash() {
	HandleCrashWithLogger(klog.Background())
}

// HandleCrashWithContext 从 panic 中恢复，并使用上下文中的日志记录器记录异常。
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

// HandleCrashWithLogger 从 panic 中恢复，并使用调用方提供的日志记录器记录异常。
// 与 HandleCrashWithContext 的区别是不支持附加处理器；给已有明确 logger 的调用点省一次 ctx 查找。
func HandleCrashWithLogger(logger klog.Logger) {
	if r := recover(); r != nil {
		logger.Error(fmt.Errorf("%v", r), "Observed a panic")
	}
}
