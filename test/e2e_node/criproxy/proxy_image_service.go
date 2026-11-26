/*
Copyright 2024 The Kubernetes Authors.

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

package criproxy

import (
	"context"

	kubeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	ListImages  = "ListImages"
	ImageStatus = "ImageStatus"
	PullImage   = "PullImage"
	RemoveImage = "RemoveImage"
	ImageFsInfo = "ImageFsInfo"
	// 流式 API
	StreamImages = "StreamImages"
	// 流式 API 的每次发送注入点，在每次 stream.Send() 前调用。
	StreamImagesSend = "StreamImagesSend"
)

// ListImages 列出现有镜像。
func (p *RemoteRuntime) ListImages(ctx context.Context, req *kubeapi.ListImagesRequest) (*kubeapi.ListImagesResponse, error) {
	if err := p.runInjectors(ListImages); err != nil {
		return nil, err
	}

	images, err := p.imageService.ListImages(ctx, req.Filter)
	if err != nil {
		return nil, err
	}
	return &kubeapi.ListImagesResponse{
		Images: images,
	}, nil
}

// ImageStatus 返回镜像状态。如果镜像
// 不存在，返回的响应中 ImageStatusResponse.Image 设为
// nil。
func (p *RemoteRuntime) ImageStatus(ctx context.Context, req *kubeapi.ImageStatusRequest) (*kubeapi.ImageStatusResponse, error) {
	if err := p.runInjectors(ImageStatus); err != nil {
		return nil, err
	}

	resp, err := p.imageService.ImageStatus(ctx, req.Image, false)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// PullImage 用认证配置拉取镜像。
func (p *RemoteRuntime) PullImage(ctx context.Context, req *kubeapi.PullImageRequest) (*kubeapi.PullImageResponse, error) {
	if err := p.runInjectors(PullImage); err != nil {
		return nil, err
	}

	image, err := p.imageService.PullImage(ctx, req.Image, req.Auth, req.SandboxConfig)
	if err != nil {
		return nil, err
	}
	return &kubeapi.PullImageResponse{
		ImageRef: image,
	}, nil
}

// RemoveImage 移除镜像。
// 此调用是幂等的，镜像已被移除时
// 不得返回错误。
func (p *RemoteRuntime) RemoveImage(ctx context.Context, req *kubeapi.RemoveImageRequest) (*kubeapi.RemoveImageResponse, error) {
	if err := p.runInjectors(RemoveImage); err != nil {
		return nil, err
	}

	err := p.imageService.RemoveImage(ctx, req.Image)
	if err != nil {
		return nil, err
	}
	return &kubeapi.RemoveImageResponse{}, nil
}

// ImageFsInfo 返回用于存储镜像的文件系统的信息。
func (p *RemoteRuntime) ImageFsInfo(ctx context.Context, req *kubeapi.ImageFsInfoRequest) (*kubeapi.ImageFsInfoResponse, error) {
	if err := p.runInjectors(ImageFsInfo); err != nil {
		return nil, err
	}

	resp, err := p.imageService.ImageFsInfo(ctx)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// StreamImages 返回镜像流。
func (p *RemoteRuntime) StreamImages(req *kubeapi.StreamImagesRequest, stream kubeapi.ImageService_StreamImagesServer) error {
	if err := p.runInjectors(StreamImages); err != nil {
		return err
	}

	images, err := p.imageService.ListImages(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	for _, image := range images {
		if err := p.runInjectors(StreamImagesSend); err != nil {
			return err
		}
		if err := stream.Send(&kubeapi.StreamImagesResponse{Images: []*kubeapi.Image{image}}); err != nil {
			return err
		}
	}
	return nil
}
