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
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/credentialprovider"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// pullResult 是一次镜像拉取的结果。
type pullResult struct {
	// imageRef 是拉取成功后的镜像引用。
	imageRef string
	// imageSize 是镜像大小（字节）。
	imageSize uint64
	// err 是拉取过程中的错误。
	err error
	// pullDuration 是拉取耗时。
	pullDuration time.Duration
	// credentialsUsed 是实际使用的凭据。
	credentialsUsed *credentialprovider.TrackedAuthConfig
}

// imagePuller 抽象镜像拉取行为，支持并行与串行两种实现。
type imagePuller interface {
	// pullImage 拉取镜像并通过 pullChan 返回结果。
	pullImage(context.Context, kubecontainer.ImageSpec, []credentialprovider.TrackedAuthConfig, chan<- pullResult, *runtimeapi.PodSandboxConfig)
}

// 编译期断言：两种拉取器都实现了 imagePuller 接口。
var _, _ imagePuller = &parallelImagePuller{}, &serialImagePuller{}

// parallelImagePuller 通过令牌通道限制并发数的镜像拉取器。
type parallelImagePuller struct {
	// imageService 是实际执行拉取的镜像服务。
	imageService kubecontainer.ImageService
	// tokens 是并发令牌；为 nil 时不限制并发。
	tokens chan struct{}
}

// newParallelImagePuller 按并发上限创建并行拉取器。
func newParallelImagePuller(imageService kubecontainer.ImageService, maxParallelImagePulls *int32) imagePuller {
	// 未配置有效上限时不使用令牌通道，即不限制并发。
	if maxParallelImagePulls == nil || *maxParallelImagePulls < 1 {
		return &parallelImagePuller{imageService, nil}
	}
	return &parallelImagePuller{imageService, make(chan struct{}, *maxParallelImagePulls)}
}

// pullImage 获取令牌后拉取镜像，并把结果写入 pullChan。
// podSandboxConfig 是 Pod 的 sandbox 配置。
func (pip *parallelImagePuller) pullImage(ctx context.Context, spec kubecontainer.ImageSpec, credentials []credentialprovider.TrackedAuthConfig, pullChan chan<- pullResult, podSandboxConfig *runtimeapi.PodSandboxConfig) {
	// 通过令牌通道限制同时进行的拉取数量。
	if pip.tokens != nil {
		pip.tokens <- struct{}{}
		defer func() { <-pip.tokens }()
	}
	startTime := time.Now()
	imageRef, creds, err := pip.imageService.PullImage(ctx, spec, credentials, podSandboxConfig)
	var size uint64
	if err == nil && imageRef != "" {
		// 尽力获取镜像大小，失败时忽略错误。
		size, _ = pip.imageService.GetImageSize(ctx, spec)
	}
	pullChan <- pullResult{
		imageRef:        imageRef,
		imageSize:       size,
		err:             err,
		pullDuration:    time.Since(startTime),
		credentialsUsed: creds,
	}
}

// maxImagePullRequests 是串行拉取器可排队的最大请求数。
const maxImagePullRequests = 10

// serialImagePuller 把拉取请求排队后逐个执行的拉取器。
type serialImagePuller struct {
	imageService kubecontainer.ImageService
	// pullRequests 是待处理拉取请求的队列。
	pullRequests chan *imagePullRequest
}

// newSerialImagePuller 创建串行拉取器并启动后台处理协程。
func newSerialImagePuller(imageService kubecontainer.ImageService) imagePuller {
	imagePuller := &serialImagePuller{imageService, make(chan *imagePullRequest, maxImagePullRequests)}
	// 后台协程按秒轮询处理排队中的拉取请求。
	go wait.Until(imagePuller.processImagePullRequests, time.Second, wait.NeverStop)
	return imagePuller
}

// imagePullRequest 描述一次排队中的镜像拉取请求。
type imagePullRequest struct {
	// ctx 是拉取使用的上下文。
	ctx context.Context
	// spec 是待拉取的镜像规格。
	spec kubecontainer.ImageSpec
	// credentials 是拉取镜像可用的凭据列表。
	credentials []credentialprovider.TrackedAuthConfig
	// pullChan 用于回传拉取结果。
	pullChan         chan<- pullResult
	podSandboxConfig *runtimeapi.PodSandboxConfig
}

// pullImage 把请求放入队列后立即返回，由后台协程串行执行。
func (sip *serialImagePuller) pullImage(ctx context.Context, spec kubecontainer.ImageSpec, credentials []credentialprovider.TrackedAuthConfig, pullChan chan<- pullResult, podSandboxConfig *runtimeapi.PodSandboxConfig) {
	sip.pullRequests <- &imagePullRequest{
		ctx:              ctx,
		spec:             spec,
		credentials:      credentials,
		pullChan:         pullChan,
		podSandboxConfig: podSandboxConfig,
	}
}

// processImagePullRequests 串行处理队列中的拉取请求。
func (sip *serialImagePuller) processImagePullRequests() {
	for pullRequest := range sip.pullRequests {
		startTime := time.Now()
		imageRef, creds, err := sip.imageService.PullImage(pullRequest.ctx, pullRequest.spec, pullRequest.credentials, pullRequest.podSandboxConfig)
		var size uint64
		if err == nil && imageRef != "" {
			// 尽力获取镜像大小，失败时忽略错误。
			size, _ = sip.imageService.GetImageSize(pullRequest.ctx, pullRequest.spec)
		}
		pullRequest.pullChan <- pullResult{
			imageRef:  imageRef,
			imageSize: size,
			err:       err,
			// 注意：pullDuration 包含获取镜像大小的耗时。
			pullDuration:    time.Since(startTime),
			credentialsUsed: creds,
		}
	}
}
