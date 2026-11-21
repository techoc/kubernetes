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
	"fmt"

	"k8s.io/client-go/util/flowcontrol"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/credentialprovider"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// throttleImagePulling 包装 kubecontainer.ImageService，按给定 QPS 与 burst 限流镜像拉取。
// QPS 为 0 时不做任何限流。
func throttleImagePulling(imageService kubecontainer.ImageService, qps float32, burst int) kubecontainer.ImageService {
	if qps == 0.0 {
		return imageService
	}
	return &throttledImageService{
		ImageService: imageService,
		limiter:      flowcontrol.NewTokenBucketRateLimiter(qps, burst),
	}
}

// throttledImageService 在真实 ImageService 之上叠加令牌桶限流。
type throttledImageService struct {
	kubecontainer.ImageService
	limiter flowcontrol.RateLimiter
}

// PullImage 在令牌桶允许时执行拉取，否则返回 QPS 超限错误。
func (ts throttledImageService) PullImage(ctx context.Context, image kubecontainer.ImageSpec, credentials []credentialprovider.TrackedAuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, *credentialprovider.TrackedAuthConfig, error) {
	if ts.limiter.TryAccept() {
		return ts.ImageService.PullImage(ctx, image, credentials, podSandboxConfig)
	}
	return "", nil, fmt.Errorf("pull QPS exceeded")
}
