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

package kuberuntime

import (
	"context"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	crededentialprovider "k8s.io/kubernetes/pkg/credentialprovider"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// PullImage 使用提供的凭据把镜像从网络拉取到本地存储。
func (m *kubeGenericRuntimeManager) PullImage(ctx context.Context, image kubecontainer.ImageSpec, credentials []crededentialprovider.TrackedAuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, *crededentialprovider.TrackedAuthConfig, error) {
	logger := klog.FromContext(ctx)
	img := image.Image
	imgSpec := ToRuntimeAPIImageSpec(image)

	// 没有凭据时只尝试一次匿名拉取。
	if len(credentials) == 0 {
		logger.V(3).Info("Pulling image without credentials", "image", img)

		imageRef, err := m.imageService.PullImage(ctx, imgSpec, nil, podSandboxConfig)
		if err != nil {
			logger.Error(err, "Failed to pull image", "image", img)
			return "", nil, err
		}

		return imageRef, nil, nil
	}

	// 依次尝试每组凭据；全部失败时聚合错误返回。
	var pullErrs []error
	for _, currentCreds := range credentials {
		auth := &runtimeapi.AuthConfig{
			Username:      currentCreds.Username,
			Password:      currentCreds.Password,
			Auth:          currentCreds.Auth,
			ServerAddress: currentCreds.ServerAddress,
			IdentityToken: currentCreds.IdentityToken,
			RegistryToken: currentCreds.RegistryToken,
		}

		imageRef, err := m.imageService.PullImage(ctx, imgSpec, auth, podSandboxConfig)
		// 未发生错误时返回成功。
		if err == nil {
			return imageRef, &currentCreds, nil
		}

		pullErrs = append(pullErrs, err)
	}

	return "", nil, utilerrors.NewAggregate(pullErrs)
}

// GetImageRef 返回本地已存在镜像的 ID；镜像不存在时返回 ("", nil)。
func (m *kubeGenericRuntimeManager) GetImageRef(ctx context.Context, image kubecontainer.ImageSpec) (string, error) {
	logger := klog.FromContext(ctx)
	resp, err := m.imageService.ImageStatus(ctx, ToRuntimeAPIImageSpec(image), false)
	if err != nil {
		logger.Error(err, "Failed to get image status", "image", image.Image)
		return "", err
	}
	if resp.Image == nil {
		return "", nil
	}
	return resp.Image.Id, nil
}

// GetImageSize 返回本地镜像的大小。
func (m *kubeGenericRuntimeManager) GetImageSize(ctx context.Context, image kubecontainer.ImageSpec) (uint64, error) {
	logger := klog.FromContext(ctx)
	resp, err := m.imageService.ImageStatus(ctx, ToRuntimeAPIImageSpec(image), false)
	if err != nil {
		logger.Error(err, "Failed to get image status", "image", image.Image)
		return 0, err
	}
	if resp.Image == nil {
		return 0, nil
	}
	return resp.Image.Size, nil
}

// ListImages 返回机器上当前的所有镜像。
func (m *kubeGenericRuntimeManager) ListImages(ctx context.Context) ([]kubecontainer.Image, error) {
	logger := klog.FromContext(ctx)
	var images []kubecontainer.Image

	allImages, err := m.imageService.ListImages(ctx, nil)
	if err != nil {
		logger.Error(err, "Failed to list images")
		return nil, err
	}

	// 把 CRI 镜像信息转换为 kubelet 内部结构。
	for _, img := range allImages {
		// 容器运行时可能未实现 KEP 4216 的改动；若未实现，ImageSpec 中的 runtimeHandler
		// 字段保持为空。因此启用 RuntimeClassInImageCriAPI 时，检测到该字段为空会记录警告。
		if utilfeature.DefaultFeatureGate.Enabled(features.RuntimeClassInImageCriAPI) {
			if img.Spec == nil || (img.Spec != nil && img.Spec.RuntimeHandler == "") {
				logger.V(2).Info("WARNING: RuntimeHandler is empty", "ImageID", img.Id)
			}
		}

		images = append(images, kubecontainer.Image{
			ID:          img.Id,
			Size:        int64(img.Size),
			RepoTags:    img.RepoTags,
			RepoDigests: img.RepoDigests,
			Spec:        ToKubeContainerImageSpec(img),
			Pinned:      img.Pinned,
		})
	}

	return images, nil
}

// RemoveImage 删除指定镜像。
func (m *kubeGenericRuntimeManager) RemoveImage(ctx context.Context, image kubecontainer.ImageSpec) error {
	logger := klog.FromContext(ctx)
	err := m.imageService.RemoveImage(ctx, &runtimeapi.ImageSpec{Image: image.Image})
	if err != nil {
		logger.Error(err, "Failed to remove image", "image", image.Image)
		return err
	}

	return nil
}

// ImageStats 返回镜像统计。
// 注意：当前逻辑对共享层的镜像（如 Docker 镜像）不准确，这是已知问题，
// 后续改为直接从 CRI 获取 imagefs 统计。
func (m *kubeGenericRuntimeManager) ImageStats(ctx context.Context) (*kubecontainer.ImageStats, error) {
	logger := klog.FromContext(ctx)
	allImages, err := m.imageService.ListImages(ctx, nil)
	if err != nil {
		logger.Error(err, "Failed to list images")
		return nil, err
	}
	// 累加所有镜像大小作为总存储占用。
	stats := &kubecontainer.ImageStats{}
	for _, img := range allImages {
		stats.TotalStorageBytes += img.Size
	}
	return stats, nil
}

// ImageFsInfo 返回镜像文件系统的信息。
func (m *kubeGenericRuntimeManager) ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	logger := klog.FromContext(ctx)
	allImages, err := m.imageService.ImageFsInfo(ctx)
	if err != nil {
		logger.Error(err, "Failed to get image filesystem")
		return nil, err
	}
	return allImages, nil
}
