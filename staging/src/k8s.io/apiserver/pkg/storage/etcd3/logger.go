/*
Copyright 2018 The Kubernetes Authors.

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

package etcd3

import (
	"fmt"

	"google.golang.org/grpc/grpclog"
	"k8s.io/klog/v2"
)

func init() {
	// 接管 etcd client 的全局 grpc 日志，避免其直接写 stderr、绕过 klog 的格式与级别控制。
	grpclog.SetLoggerV2(klogWrapper{})
}

// klogWrapper 把 grpc 的日志接口适配到 klog。
type klogWrapper struct{}

// klogWrapperDepth 补偿 klogWrapper 与 grpc 调用栈之间的层数，
// 使日志中的文件:行号指向真正的调用点而非本包装器。
const klogWrapperDepth = 4

// Info 降级到 V(5)：grpc 的 Info 级别过于嘈杂，只在详细日志中输出。
func (klogWrapper) Info(args ...interface{}) {
	if klogV := klog.V(5); klogV.Enabled() {
		klogV.InfoSDepth(klogWrapperDepth, fmt.Sprint(args...))
	}
}

// Infoln 同 Info，使用 Sprintln 保留换行语义。
func (klogWrapper) Infoln(args ...interface{}) {
	if klogV := klog.V(5); klogV.Enabled() {
		klogV.InfoSDepth(klogWrapperDepth, fmt.Sprintln(args...))
	}
}

// Infof 同 Info，支持格式化参数。
func (klogWrapper) Infof(format string, args ...interface{}) {
	if klogV := klog.V(5); klogV.Enabled() {
		klog.V(5).InfoSDepth(klogWrapperDepth, fmt.Sprintf(format, args...))
	}
}

// Warning 映射到 klog 的 Warning 级别。
func (klogWrapper) Warning(args ...interface{}) {
	klog.WarningDepth(klogWrapperDepth, args...)
}

// Warningln 映射到 Warning，保留换行。
func (klogWrapper) Warningln(args ...interface{}) {
	klog.WarningDepth(klogWrapperDepth, fmt.Sprintln(args...))
}

// Warningf 映射到 Warning，支持格式化参数。
func (klogWrapper) Warningf(format string, args ...interface{}) {
	klog.WarningDepth(klogWrapperDepth, fmt.Sprintf(format, args...))
}

// Error 映射到 klog 的 Error 级别。
func (klogWrapper) Error(args ...interface{}) {
	klog.ErrorDepth(klogWrapperDepth, args...)
}

// Errorln 映射到 Error，保留换行。
func (klogWrapper) Errorln(args ...interface{}) {
	klog.ErrorDepth(klogWrapperDepth, fmt.Sprintln(args...))
}

// Errorf 映射到 Error，支持格式化参数。
func (klogWrapper) Errorf(format string, args ...interface{}) {
	klog.ErrorDepth(klogWrapperDepth, fmt.Sprintf(format, args...))
}

// Fatal 映射到 klog 的 Fatal，会触发进程退出。
func (klogWrapper) Fatal(args ...interface{}) {
	klog.FatalDepth(klogWrapperDepth, args...)
}

// Fatalln 映射到 Fatal，保留换行。
func (klogWrapper) Fatalln(args ...interface{}) {
	klog.FatalDepth(klogWrapperDepth, fmt.Sprintln(args...))
}

// Fatalf 映射到 Fatal，支持格式化参数。
func (klogWrapper) Fatalf(format string, args ...interface{}) {
	klog.FatalDepth(klogWrapperDepth, fmt.Sprintf(format, args...))
}

// V 把 grpc 的详细级别直接映射到 klog 的 -v 级别。
func (klogWrapper) V(l int) bool {
	return bool(klog.V(klog.Level(l)).Enabled())
}
