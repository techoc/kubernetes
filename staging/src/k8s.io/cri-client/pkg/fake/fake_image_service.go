/*
Copyright 2017 The Kubernetes Authors.

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

package fake

import (
	"context"

	kubeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// ListImages 列出假服务中已有的镜像，返回经请求 Filter 过滤后的结果。
func (f *RemoteRuntime) ListImages(ctx context.Context, req *kubeapi.ListImagesRequest) (*kubeapi.ListImagesResponse, error) {
	// 把请求中的过滤条件透传给假镜像服务，由后者在内存镜像集合上做筛选。
	images, err := f.ImageService.ListImages(ctx, req.Filter)
	if err != nil {
		return nil, err
	}

	// 将筛选出的镜像包装成 CRI 响应。
	return &kubeapi.ListImagesResponse{
		Images: images,
	}, nil
}

// ImageStatus 返回指定镜像的状态；镜像不存在时，假服务返回的响应中 Image 字段为 nil。
// 此处固定传入 verbose=false，只请求基础状态信息。
func (f *RemoteRuntime) ImageStatus(ctx context.Context, req *kubeapi.ImageStatusRequest) (*kubeapi.ImageStatusResponse, error) {
	// 第二个参数 false 表示不要求返回镜像的详细信息。
	resp, err := f.ImageService.ImageStatus(ctx, req.Image, false)
	if err != nil {
		return nil, err
	}

	// 假服务已经构造好完整响应，直接透传即可。
	return resp, nil
}

// PullImage 拉取镜像：把镜像规格、认证配置与 sandbox 配置一并交给假服务处理。
func (f *RemoteRuntime) PullImage(ctx context.Context, req *kubeapi.PullImageRequest) (*kubeapi.PullImageResponse, error) {
	// 假服务只记录拉取行为并返回一个镜像引用，不会真正下载镜像。
	image, err := f.ImageService.PullImage(ctx, req.Image, req.Auth, req.SandboxConfig)
	if err != nil {
		return nil, err
	}

	// 将假服务返回的镜像引用包装成 CRI 响应。
	return &kubeapi.PullImageResponse{
		ImageRef: image,
	}, nil
}

// RemoveImage 从假服务中移除指定镜像；该操作是幂等的，镜像不存在时也不应报错。
func (f *RemoteRuntime) RemoveImage(ctx context.Context, req *kubeapi.RemoveImageRequest) (*kubeapi.RemoveImageResponse, error) {
	// 移除本身没有返回数据，成功时只需返回空响应。
	err := f.ImageService.RemoveImage(ctx, req.Image)
	if err != nil {
		return nil, err
	}

	return &kubeapi.RemoveImageResponse{}, nil
}

// ImageFsInfo 返回存放镜像的文件系统信息，数据来自假服务中的预置用量信息。
func (f *RemoteRuntime) ImageFsInfo(ctx context.Context, req *kubeapi.ImageFsInfoRequest) (*kubeapi.ImageFsInfoResponse, error) {
	// 假服务的 ImageFsInfo 不依赖请求参数，直接返回预置的文件系统用量。
	resp, err := f.ImageService.ImageFsInfo(ctx)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
