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

// StreamHandler 定义处理 DRA 健康数据流的接口。
// 该接口由 DRA Manager 实现，用于解耦 plugin 包与 manager 包，避免循环导入。
type StreamHandler interface {
	// HandleWatchResourcesStream 处理来自指定 DRA 插件数据流的健康状态更新。
	HandleWatchResourcesStream(ctx context.Context, stream drahealthv1.DRAResourceHealth_NodeWatchResourcesClient, resourceName string) error
}
