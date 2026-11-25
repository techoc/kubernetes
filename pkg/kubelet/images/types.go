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

package images

import (
	"context"
	"errors"

	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var (
	// ErrImagePullBackOff 表示镜像拉取失败，kubelet 正在退避重试。
	ErrImagePullBackOff = errors.New("ImagePullBackOff")

	// ErrImageInspect 表示无法检查镜像信息。
	ErrImageInspect = errors.New("ImageInspectError")

	// ErrImagePull 是通用的镜像拉取错误。
	ErrImagePull = errors.New("ErrImagePull")

	// ErrImageNeverPull 表示宿主机缺少所需镜像且拉取策略为 Never。
	ErrImageNeverPull = errors.New("ErrImageNeverPull")

	// ErrInvalidImageName 表示无法解析镜像名称。
	ErrInvalidImageName = errors.New("InvalidImageName")
)

// ImageManager 提供容器镜像生命周期管理的接口。
// 实现需负责拉取（下载）、管理与删除镜像，抽象底层运行时，并保证并发安全。
type ImageManager interface {
	// EnsureImageExists 确保 requestedImage 指定的镜像存在，必要时触发拉取。
	EnsureImageExists(ctx context.Context, objRef *v1.ObjectReference, pod *v1.Pod, requestedImage string, pullSecrets []v1.Secret, podSandboxConfig *runtimeapi.PodSandboxConfig, podRuntimeHandler string, pullPolicy v1.PullPolicy) (imageRef, message string, err error)

	// TODO(ronl): 整合该接口中的镜像管理与删除操作。
}
