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

package e2enode

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	"k8s.io/klog/v2"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	commontest "k8s.io/kubernetes/test/e2e/common"
	e2emanifest "k8s.io/kubernetes/test/e2e/framework/manifest"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	// 拉取镜像的尝试次数。
	maxImagePullRetries = 5
	// 镜像拉取重试之间的休眠时长。
	imagePullRetryDelay = time.Second
	// 并行拉取镜像的数量。
	maxParallelImagePullCount = 5
)

// NodePrePullImageList 是 node e2e 测试中使用的镜像列表，这些镜像会预先拉取，
// 以免实际测试中镜像拉取失败。
var NodePrePullImageList = sets.NewString(
	imageutils.GetE2EImage(imageutils.AgnhostPrev),
	imageutils.GetE2EImage(imageutils.Agnhost),
	"gcr.io/cadvisor/cadvisor:v0.47.2",
	busyboxImage,
	"registry.k8s.io/e2e-test-images/busybox@sha256:a9155b13325b2abef48e71de77bb8ac015412a566829f621d06bfae5c699b1b9",
	imageutils.GetE2EImage(imageutils.Nginx),
	imageutils.GetE2EImage(imageutils.Perl),
	imageutils.GetE2EImage(imageutils.Nonewprivs),
	imageutils.GetPauseImageName(),
	imageutils.GetE2EImage(imageutils.NodePerfNpbEp),
	imageutils.GetE2EImage(imageutils.NodePerfNpbIs),
	imageutils.GetE2EImage(imageutils.NodePerfPytorchWideDeep),
	imageutils.GetE2EImage(imageutils.Etcd),
)

// updateImageAllowList 用以下内容更新 e2epod.ImagePrePullList：
// 1. 硬编码的列表
// 2. 从 framework.TestContext.ExtraEnvs 传入的镜像
// 因此此函数必须在应用 extra env 之后调用。
func updateImageAllowList(ctx context.Context) {
	// 将 NodePrePullImageList 和 PrePulledImages 合并到 framework 的镜像预拉取列表。
	e2epod.ImagePrePullList = NodePrePullImageList.Union(commontest.PrePulledImages)
	// 来自 extra env 的镜像
	e2epod.ImagePrePullList.Insert(getNodeProblemDetectorImage())
	if sriovDevicePluginImage, err := getSRIOVDevicePluginImage(); err != nil {
		klog.Errorln(err)
	} else {
		e2epod.ImagePrePullList.Insert(sriovDevicePluginImage)
	}
	if samplePluginImage, err := getContainerImageFromE2ETestDaemonset(e2enode.SampleDevicePluginDSYAML); err != nil {
		klog.Errorln(err)
	} else {
		e2epod.ImagePrePullList.Insert(samplePluginImage)
	}
	if samplePluginImageCtrlReg, err := getContainerImageFromE2ETestDaemonset(e2enode.SampleDevicePluginControlRegistrationDSYAML); err != nil {
		klog.Errorln(err)
	} else {
		e2epod.ImagePrePullList.Insert(samplePluginImageCtrlReg)
	}
}

func getNodeProblemDetectorImage() string {
	const defaultImage string = "registry.k8s.io/node-problem-detector/node-problem-detector:v1.35.2"
	image := os.Getenv("NODE_PROBLEM_DETECTOR_IMAGE")
	if image == "" {
		image = defaultImage
	}
	return image
}

// puller 表示通用镜像拉取器
type puller interface {
	// Pull 按名称拉取镜像
	Pull(ctx context.Context, image string) ([]byte, error)
	// Remove 按名称删除镜像
	Remove(ctx context.Context, image string) error
	// Name 返回具体 puller 实现的名称
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

func (rp *remotePuller) Remove(ctx context.Context, image string) error {
	return rp.imageService.RemoveImage(ctx, &runtimeapi.ImageSpec{Image: image})
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

// PrePullAllImages 预取测试依赖的所有镜像，以免实际测试中失败。
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
	ctx, cancel := context.WithCancel(context.Background())
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

func RemoveImage(ctx context.Context, image string) error {
	puller, err := getPuller(ctx)
	if err != nil {
		return err
	}
	return puller.Remove(ctx, image)
}

func getContainerImageFromE2ETestDaemonset(dsYamlPath string) (string, error) {
	data, err := e2etestfiles.Read(dsYamlPath)
	if err != nil {
		return "", fmt.Errorf("failed to read the daemonset yaml: %w", err)
	}

	ds, err := e2emanifest.DaemonSetFromData(data)
	if err != nil {
		return "", fmt.Errorf("failed to parse daemonset yaml: %w", err)
	}

	if len(ds.Spec.Template.Spec.Containers) < 1 {
		return "", fmt.Errorf("failed to parse the container image: cannot extract the container from YAML")
	}
	return ds.Spec.Template.Spec.Containers[0].Image, nil
}

// getSRIOVDevicePluginImage 返回 SRIOV device plugin 的镜像。
func getSRIOVDevicePluginImage() (string, error) {
	data, err := e2etestfiles.Read(SRIOVDevicePluginDSYAML)
	if err != nil {
		return "", fmt.Errorf("failed to read the device plugin manifest: %w", err)
	}
	ds, err := e2emanifest.DaemonSetFromData(data)
	if err != nil {
		return "", fmt.Errorf("failed to parse the device plugin image: %w", err)
	}
	if ds == nil {
		return "", fmt.Errorf("failed to parse the device plugin image: the extracted DaemonSet is nil")
	}
	if len(ds.Spec.Template.Spec.Containers) < 1 {
		return "", fmt.Errorf("failed to parse the device plugin image: cannot extract the container from YAML")
	}
	return ds.Spec.Template.Spec.Containers[0].Image, nil
}
