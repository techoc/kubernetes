//go:build windows

/*
Copyright The Kubernetes Authors.

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

package e2enodewindows

import (
	"context"
	"os/user"
	"sync"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	commontest "k8s.io/kubernetes/test/e2e/common"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	// 拉取镜像的尝试次数。
	maxImagePullRetries = 5
	// 两次拉取重试之间的等待时长。
	imagePullRetryDelay = time.Second
	// 并行拉取镜像的数量。
	maxParallelImagePullCount = 5
)

// NodePrePullImageList 是 Windows 节点 e2e 测试所用镜像的列表。
// 这些镜像会在测试运行前预拉取，避免正式测试中拉取镜像失败。
var NodePrePullImageList = sets.NewString(
	imageutils.GetE2EImage(imageutils.Agnhost),
	busyboxImage,
	imageutils.GetPauseImageName(),
)

// updateImageAllowList 用 Windows 测试镜像更新 e2epod.ImagePrePullList。
func updateImageAllowList(ctx context.Context) {
	e2epod.ImagePrePullList = NodePrePullImageList.Union(commontest.PrePulledImages)
}

// puller 表示通用的镜像拉取器
type puller interface {
	// Pull 按名称拉取镜像
	Pull(ctx context.Context, image string) ([]byte, error)
	// Name 返回具体拉取器实现的名称
	Name() string
}

type remotePuller struct {
	imageService internalapi.ImageManagerService
}

func (rp *remotePuller) Name() string {
	return "CRI"
}

func (rp *remotePuller) Pull(ctx context.Context, image string) ([]byte, error) {
	resp, err := rp.imageService.ImageStatus(ctx, &runtimeapi.ImageSpec{Image: image}, false)
	if err == nil && resp.GetImage() != nil {
		return nil, nil
	}
	_, err = rp.imageService.PullImage(ctx, &runtimeapi.ImageSpec{Image: image}, nil, nil)
	return nil, err
}

func getPuller(ctx context.Context) (puller, error) {
	_, is, err := getCRIClient(ctx)
	if err != nil {
		return nil, err
	}
	return &remotePuller{
		imageService: is,
	}, nil
}

// PrePullAllImages 预先拉取测试依赖的所有镜像，避免正式测试时失败。
func PrePullAllImages(ctx context.Context) error {
	puller, err := getPuller(ctx)
	if err != nil {
		return err
	}
	usr, err := user.Current()
	if err != nil {
		return err
	}
	images := e2epod.ImagePrePullList.List()
	klog.V(4).Infof("Pre-pulling images with %s %+v", puller.Name(), images)

	imageCh := make(chan int, len(images))
	for i := range images {
		imageCh <- i
	}
	close(imageCh)

	pullErrs := make([]error, len(images))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	parallelImagePullCount := maxParallelImagePullCount
	if len(images) < parallelImagePullCount {
		parallelImagePullCount = len(images)
	}

	var wg sync.WaitGroup
	wg.Add(parallelImagePullCount)
	for i := 0; i < parallelImagePullCount; i++ {
		go func() {
			defer wg.Done()

			for i := range imageCh {
				var (
					pullErr error
					output  []byte
				)
				for retryCount := range maxImagePullRetries {
					select {
					case <-ctx.Done():
						return
					default:
					}

					if retryCount > 0 {
						time.Sleep(imagePullRetryDelay)
					}
					if output, pullErr = puller.Pull(ctx, images[i]); pullErr == nil {
						break
					}
					klog.Warningf("Failed to pull %s as user %q, retrying in %s (%d of %d): %v",
						images[i], usr.Username, imagePullRetryDelay.String(), retryCount+1, maxImagePullRetries, pullErr)
				}
				if pullErr != nil {
					klog.Warningf("Could not pre-pull image %s %v output: %s", images[i], pullErr, output)
					pullErrs[i] = pullErr
					cancel()
					return
				}
			}
		}()
	}

	wg.Wait()
	return utilerrors.NewAggregate(pullErrs)
}
