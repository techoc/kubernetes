//go:build race

/*
Copyright 2025 The Kubernetes Authors.

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

package cli

import (
	"k8s.io/klog/v2"
)

// logRaceDetection 在竞态检测构建下输出提示，便于运维识别带额外开销的二进制。
// 无 race 构建的对应实现见 withoutrace.go；只有内建竞态检测时才会调用本函数。
func logRaceDetection() {
	klog.Info("Data race detection enabled")
}
