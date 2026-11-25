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

// Package runtime 提供 kubeadm 容器运行时实现。
package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	criapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// defaultKnownCRISockets 保存已知 CRI 端点集合。
var defaultKnownCRISockets = []string{
	constants.CRISocketContainerd,
	constants.CRISocketCRIO,
	constants.CRISocketDocker,
}

// ContainerRuntime 是操作容器运行时的接口。
type ContainerRuntime interface {
	Connect() error
	Close(ctx context.Context)
	SetImpl(Impl)
	IsRunning() error
	ListKubeContainers() ([]string, error)
	RemoveContainers(containers []string) error
	PullImage(image string) error
	PullImagesInParallel(images []string, ifNotPresent bool) error
	ImageExists(image string) bool
	SandboxImage() (string, error)
	IsRuntimeConfigImplemented() (bool, error)
}

// CRIRuntime 是与 CRI 交互的结构。
type CRIRuntime struct {
	impl           Impl
	criSocket      string
	runtimeService criapi.RuntimeService
	imageService   criapi.ImageManagerService
}

// defaultTimeout 是 crictl 继承的默认超时。
const defaultTimeout = 2 * time.Second

// NewContainerRuntime 创建并返回 ContainerRuntime。
func NewContainerRuntime(criSocket string) ContainerRuntime {
	return &CRIRuntime{
		impl:      &defaultImpl{},
		criSocket: criSocket,
	}
}

// SetImpl 可设置内部实现，供测试使用。
func (runtime *CRIRuntime) SetImpl(impl Impl) {
	runtime.impl = impl
}

// Connect 建立与 CRI 运行时的连接。
func (runtime *CRIRuntime) Connect() error {
	runtimeService, err := runtime.impl.NewRemoteRuntimeService(context.Background(), runtime.criSocket, defaultTimeout)
	if err != nil {
		return errors.Wrap(err, "failed to create new CRI runtime service")
	}
	runtime.runtimeService = runtimeService

	imageService, err := runtime.impl.NewRemoteImageService(context.Background(), runtime.criSocket, defaultTimeout)
	if err != nil {
		return errors.Wrap(err, "failed to create new CRI image service")
	}
	runtime.imageService = imageService

	return nil
}

// Close 关闭运行时和镜像服务的连接。
func (runtime *CRIRuntime) Close(ctx context.Context) {
	if runtime.runtimeService != nil {
		if err := runtime.runtimeService.Close(ctx); err != nil {
			klog.Warningf("failed to close runtime service: %v", err)
		}
	}
	if runtime.imageService != nil {
		if err := runtime.imageService.Close(ctx); err != nil {
			klog.Warningf("failed to close image service: %v", err)
		}
	}
}

// IsRunning 检查运行时是否正在运行。
func (runtime *CRIRuntime) IsRunning() error {
	ctx, cancel := defaultContext()
	defer cancel()

	res, err := runtime.impl.Status(ctx, runtime.runtimeService, false)
	if err != nil {
		return errors.Wrap(err, "container runtime is not running")
	}

	for _, condition := range res.GetStatus().GetConditions() {
		if condition.GetType() == runtimeapi.RuntimeReady && // 有意不检查 NetworkReady
			!condition.GetStatus() {
			return errors.Errorf(
				"container runtime condition %q is not true. reason: %s, message: %s",
				condition.GetType(), condition.GetReason(), condition.GetMessage(),
			)
		}
	}

	return nil
}

// ListKubeContainers 列出运行中的 Kubernetes CRI Pod。
func (runtime *CRIRuntime) ListKubeContainers() ([]string, error) {
	ctx, cancel := defaultContext()
	defer cancel()

	sandboxes, err := runtime.impl.ListPodSandbox(ctx, runtime.runtimeService, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list pod sandboxes")
	}

	pods := []string{}
	for _, sandbox := range sandboxes {
		pods = append(pods, sandbox.GetId())
	}
	return pods, nil
}

// RemoveContainers 移除运行中的 Kubernetes Pod。
func (runtime *CRIRuntime) RemoveContainers(containers []string) error {
	errs := []error{}
	for _, container := range containers {
		var lastErr error
		for range constants.RemoveContainerRetry {
			klog.V(5).Infof("Attempting to remove container %v", container)

			ctx, cancel := defaultContext()
			if err := runtime.impl.StopPodSandbox(ctx, runtime.runtimeService, container); err != nil {
				lastErr = errors.Wrapf(err, "failed to stop running pod %s", container)
				cancel()
				continue
			}
			cancel()

			ctx, cancel = defaultContext()
			if err := runtime.impl.RemovePodSandbox(ctx, runtime.runtimeService, container); err != nil {
				lastErr = errors.Wrapf(err, "failed to remove pod %s", container)
				cancel()
				continue
			}
			cancel()

			lastErr = nil
			break
		}

		if lastErr != nil {
			errs = append(errs, lastErr)
		}
	}
	return errorsutil.NewAggregate(errs)
}

// PullImage 拉取镜像。
func (runtime *CRIRuntime) PullImage(image string) (err error) {
	for range constants.PullImageRetry {
		if _, err = runtime.impl.PullImage(context.Background(), runtime.imageService, &runtimeapi.ImageSpec{Image: image}, nil, nil); err == nil {
			return nil
		}
	}
	return errors.Wrapf(err, "failed to pull image %s", image)
}

// PullImagesInParallel 并行拉取镜像列表。
func (runtime *CRIRuntime) PullImagesInParallel(images []string, ifNotPresent bool) error {
	errs := pullImagesInParallelImpl(images, ifNotPresent, runtime.ImageExists, runtime.PullImage)
	return errorsutil.NewAggregate(errs)
}

func defaultContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultTimeout)
}

func pullImagesInParallelImpl(images []string, ifNotPresent bool,
	imageExistsFunc func(string) bool, pullImageFunc func(string) error) []error {

	var errs []error
	errChan := make(chan error, len(images))

	klog.V(1).Info("pulling all images in parallel")
	for _, image := range images {
		go func() {
			if ifNotPresent {
				exists := imageExistsFunc(image)
				if exists {
					klog.V(1).Infof("image exists: %s", image)
					errChan <- nil
					return
				}
			}
			err := pullImageFunc(image)
			if err != nil {
				err = errors.WithMessagef(err, "failed to pull image %s", image)
			} else {
				klog.V(1).Infof("done pulling: %s", image)
			}
			errChan <- err
		}()
	}

	for range images {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

// ImageExists 检查系统中是否已存在镜像。
func (runtime *CRIRuntime) ImageExists(image string) bool {
	ctx, cancel := defaultContext()
	defer cancel()
	resp, err := runtime.impl.ImageStatus(ctx, runtime.imageService, &runtimeapi.ImageSpec{Image: image}, false)
	if err != nil {
		klog.Warningf("Failed to get image status, image: %q, error: %v", image, err)
		return false
	}
	if resp == nil || resp.Image == nil {
		return false
	}
	return true
}

// detectCRISocketImpl 仅为测试拆分，请勿直接调用，应使用 DetectCRISocket。
func detectCRISocketImpl(isSocket func(string) bool, knownCRISockets []string) (string, error) {
	foundCRISockets := []string{}

	for _, socket := range knownCRISockets {
		if isSocket(socket) {
			foundCRISockets = append(foundCRISockets, socket)
		}
	}

	switch len(foundCRISockets) {
	case 0:
		// 未检测到 CRI 时回退到默认 socket；真正需要时可后续报错。
		return constants.DefaultCRISocket, nil
	case 1:
		// 恰好找到一个 CRI，使用它。
		return foundCRISockets[0], nil
	default:
		// 安装了多个 CRI？
		return "", errors.Errorf("found multiple CRI endpoints on the host. Please define which one do you wish "+
			"to use by setting the 'criSocket' field in the kubeadm configuration file: %s",
			strings.Join(foundCRISockets, ", "))
	}
}

// DetectCRISocket 从已知 CRI socket 中检测一个；发现多个或未发现时返回错误。
func DetectCRISocket() (string, error) {
	return detectCRISocketImpl(isExistingSocket, defaultKnownCRISockets)
}

// SandboxImage 返回容器运行时使用的 sandbox 镜像。
func (runtime *CRIRuntime) SandboxImage() (string, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	status, err := runtime.impl.Status(ctx, runtime.runtimeService, true)
	if err != nil {
		return "", errors.Wrap(err, "failed to get runtime status")
	}

	infoConfig, ok := status.GetInfo()["config"]
	if !ok {
		return "", errors.Errorf("no 'config' field in CRI info: %+v", status)
	}

	type config struct {
		SandboxImage string `json:"sandboxImage,omitempty"`
	}
	c := config{}

	if err := json.Unmarshal([]byte(infoConfig), &c); err != nil {
		return "", errors.Wrap(err, "failed to unmarshal CRI info config")
	}

	if c.SandboxImage == "" {
		return "", errors.New("no 'sandboxImage' field in CRI info config")
	}

	return c.SandboxImage, nil
}

// IsRuntimeConfigImplemented 检查容器运行时是否支持 RuntimeConfig gRPC 方法。
func (runtime *CRIRuntime) IsRuntimeConfigImplemented() (bool, error) {
	ctx, cancel := defaultContext()
	defer cancel()
	_, err := runtime.impl.RuntimeConfig(ctx, runtime.runtimeService)
	if err != nil {
		s, ok := status.FromError(err)
		if !ok || s.Code() != codes.Unimplemented {
			return false, errors.Wrap(err, "failed to call RuntimeConfig gRPC method")
		}
		return false, nil
	}
	return true, nil
}
