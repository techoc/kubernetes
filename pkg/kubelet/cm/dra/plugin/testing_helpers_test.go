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

package plugin

import (
	"context"

	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
)

// mockStreamHandler 是 StreamHandler 接口的 mock 实现，供本包所有测试共用。
type mockStreamHandler struct{}

var _ StreamHandler = &mockStreamHandler{}

// HandleWatchResourcesStream 阻塞直到 ctx 结束，用于模拟持续接收健康数据流的插件。
func (m *mockStreamHandler) HandleWatchResourcesStream(ctx context.Context, stream drahealthv1.DRAResourceHealth_NodeWatchResourcesClient, resourceName string) error {
	<-ctx.Done()
	return nil
}
