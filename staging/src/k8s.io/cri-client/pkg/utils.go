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

package cri

import (
	"fmt"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// maxMsgSize 是本客户端与容器运行时通信时允许的最大 gRPC 消息大小（16MB）。
// gRPC 库默认是 4MB，对于携带容器状态、日志等较大响应的 CRI 场景来说偏小，因此在这里调大。
const maxMsgSize = 1024 * 1024 * 16

// verifySandboxStatus 校验运行时返回的 PodSandboxStatus 是否包含所有必需字段。
// kubelet 依赖这些字段唯一标识沙箱并做状态同步，字段缺失说明运行时返回的数据不可信。
// 校验顺序（命中任意一项即返回错误）：
//  1. Id 非空——沙箱的唯一标识；
//  2. Metadata 非 nil；
//  3. Metadata 中的 Name、Namespace、Uid 均非空；
//  4. CreatedAt 非 0——纳秒级创建时间戳。
func verifySandboxStatus(status *runtimeapi.PodSandboxStatus) error {
	if status.Id == "" {
		return fmt.Errorf("status.Id is not set")
	}

	if status.Metadata == nil {
		return fmt.Errorf("status.Metadata is not set")
	}

	metadata := status.Metadata
	// 三个元数据字段必须同时存在，任一为空即视为无效状态。
	if metadata.Name == "" || metadata.Namespace == "" || metadata.Uid == "" {
		return fmt.Errorf("metadata.Name, metadata.Namespace or metadata.Uid is not in metadata %q", metadata)
	}

	if status.CreatedAt == 0 {
		return fmt.Errorf("status.CreatedAt is not set")
	}

	return nil
}

// verifyContainerStatus 校验运行时返回的 ContainerStatus 是否包含所有必需字段。
// 相比沙箱状态，容器状态还额外要求镜像信息完整（Image 与 ImageRef）。
// 校验顺序（命中任意一项即返回错误）：
//  1. Id 非空；
//  2. Metadata 非 nil 且 Name 非空（容器的 Attempt 允许为 0，故不校验）；
//  3. CreatedAt 非 0；
//  4. Image 非 nil 且其 Image 字段非空；
//  5. ImageRef 非空——镜像的实际引用（通常是 digest）。
func verifyContainerStatus(status *runtimeapi.ContainerStatus) error {
	if status.Id == "" {
		return fmt.Errorf("status.Id is not set")
	}

	if status.Metadata == nil {
		return fmt.Errorf("status.Metadata is not set")
	}

	metadata := status.Metadata
	// 这里只校验 Name：Attempt 为 0 是合法的首次尝试。
	if metadata.Name == "" {
		return fmt.Errorf("metadata.Name is not in metadata %q", metadata)
	}

	if status.CreatedAt == 0 {
		return fmt.Errorf("status.CreatedAt is not set")
	}

	// 镜像规格与镜像名都必须存在。
	if status.Image == nil || status.Image.Image == "" {
		return fmt.Errorf("status.Image is not set")
	}

	// ImageRef 是镜像的全局唯一引用，缺失会影响镜像 GC 与状态上报。
	if status.ImageRef == "" {
		return fmt.Errorf("status.ImageRef is not set")
	}

	return nil
}
