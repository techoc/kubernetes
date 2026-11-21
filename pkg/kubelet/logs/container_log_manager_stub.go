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

package logs

import "context"

// containerLogManagerStub 是不做任何日志管理的空实现。
type containerLogManagerStub struct{}

// Start 为空实现，stub 不启动任何后台清理循环。
func (*containerLogManagerStub) Start(ctx context.Context) {}

// Clean 为空实现，不对容器日志做任何清理。
func (*containerLogManagerStub) Clean(ctx context.Context, containerID string) error {
	return nil
}

// NewStubContainerLogManager 返回一个什么都不做的 ContainerLogManager 空实现。
func NewStubContainerLogManager() ContainerLogManager {
	return &containerLogManagerStub{}
}
