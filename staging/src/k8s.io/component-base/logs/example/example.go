/*
Copyright 2023 The Kubernetes Authors.

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

// Package example shows how a library uses contextual logging.
// Package example 演示库代码如何正确使用上下文日志（contextual logging）：
// 从 context 取 logger 并携带键值，而不是依赖全局 logger。
package example

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"k8s.io/klog/v2"
)

func init() {
	// 故意演示错误用法：此时日志系统尚未初始化，日志可能丢失或输出到错误位置。
	klog.TODO().Info("Oops, I shouldn't be logging yet!")
}

// Run 演示各种日志 API 的用法。它接受 context 以便调用方注入带值/带名字的 logger。
func Run(ctx context.Context) {
	fmt.Println("This is normal output via stdout.")
	fmt.Fprintln(os.Stderr, "This is other output via stderr.")
	klog.Infof("Log using Infof, key: %s", "value")
	klog.InfoS("Log using InfoS", "key", "value")
	err := errors.New("fail")
	klog.Errorf("Log using Errorf, err: %v", err)
	klog.ErrorS(err, "Log using ErrorS")
	klog.V(1).Info("Log less important message")

	// 在没有 logger 或 context 时的兜底：TODO() 使用当前全局 logger，
	// 但仍应尽量改为显式传递参数。
	klog.TODO().Info("Now the default logger is set, but using the one from the context is still better.")

	logger := klog.FromContext(ctx)
	logger.V(5).Info("Log less important message at V=5 through context")

	// 这里故意重复同一个键。只有第二处能通过静态分析发现键值重复。
	klog.LoggerWithValues(klog.LoggerWithName(logger, "myname"), "duration", time.Hour).Info("runtime", "duration", time.Minute)
	logger.Info("another runtime", "duration", time.Hour, "duration", time.Minute)
}
