//go:build !linux

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

package config

import (
	"fmt"

	"k8s.io/klog/v2"
)

// startWatch 在未支持文件监视的构建中只记录错误，不启动 watch。
func (s *sourceFile) startWatch(logger klog.Logger) {
	logger.Error(nil, "Watching source file is unsupported in this build")
}

// consumeWatchEvent 在未支持文件监视的构建中直接返回错误。
func (s *sourceFile) consumeWatchEvent(logger klog.Logger, e *watchEvent) error {
	return fmt.Errorf("consuming watch event is unsupported in this build")
}
