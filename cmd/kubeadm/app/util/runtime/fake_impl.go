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

package runtime

import (
	"context"
	"time"

	cri "k8s.io/cri-api/pkg/apis"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// FakeImpl 是 impl 接口的伪实现。
type FakeImpl struct {
	runtimeConfigReturns struct {
		res *v1.RuntimeConfigResponse
		err error
	}
	imageStatusReturns struct {
		res *v1.ImageStatusResponse
		err error
	}
	listPodSandboxReturns struct {
		res []*v1.PodSandbox
		err error
	}
	newRemoteImageServiceReturns struct {
		res cri.ImageManagerService
		err error
	}
	newRemoteRuntimeServiceReturns struct {
		res cri.RuntimeService
		err error
	}
	pullImageReturns struct {
		res string
		err error
	}
	removePodSandboxReturns struct {
		res error
	}
	statusReturns struct {
		res *v1.StatusResponse
		err error
	}
	stopPodSandboxReturns struct {
		res error
	}
}

// ImageStatus 返回镜像状态。
func (fake *FakeImpl) ImageStatus(context.Context, cri.ImageManagerService, *v1.ImageSpec, bool) (*v1.ImageStatusResponse, error) {
	fakeReturns := fake.imageStatusReturns
	return fakeReturns.res, fakeReturns.err
}

// ImageStatusReturns 设置 ImageStatus 方法的返回值。
func (fake *FakeImpl) ImageStatusReturns(res *v1.ImageStatusResponse, err error) {
	fake.imageStatusReturns = struct {
		res *v1.ImageStatusResponse
		err error
	}{res, err}
}

// ListPodSandbox 返回 Pod sandbox 列表。
func (fake *FakeImpl) ListPodSandbox(context.Context, cri.RuntimeService, *v1.PodSandboxFilter) ([]*v1.PodSandbox, error) {
	fakeReturns := fake.listPodSandboxReturns
	return fakeReturns.res, fakeReturns.err
}

// ListPodSandboxReturns 设置 ListPodSandbox 方法的返回值。
func (fake *FakeImpl) ListPodSandboxReturns(res []*v1.PodSandbox, err error) {
	fake.listPodSandboxReturns = struct {
		res []*v1.PodSandbox
		err error
	}{res, err}
}

// NewRemoteImageService 返回新的远程镜像服务。
func (fake *FakeImpl) NewRemoteImageService(context.Context, string, time.Duration) (cri.ImageManagerService, error) {
	fakeReturns := fake.newRemoteImageServiceReturns
	return fakeReturns.res, fakeReturns.err
}

// NewRemoteImageServiceReturns 设置 NewRemoteImageService 方法的返回值。
func (fake *FakeImpl) NewRemoteImageServiceReturns(res cri.ImageManagerService, err error) {
	fake.newRemoteImageServiceReturns = struct {
		res cri.ImageManagerService
		err error
	}{res, err}
}

// NewRemoteRuntimeService 返回新的远程运行时服务。
func (fake *FakeImpl) NewRemoteRuntimeService(context.Context, string, time.Duration) (cri.RuntimeService, error) {
	fakeReturns := fake.newRemoteRuntimeServiceReturns
	return fakeReturns.res, fakeReturns.err
}

// NewRemoteRuntimeServiceReturns 设置 NewRemoteRuntimeService 方法的返回值。
func (fake *FakeImpl) NewRemoteRuntimeServiceReturns(res cri.RuntimeService, err error) {
	fake.newRemoteRuntimeServiceReturns = struct {
		res cri.RuntimeService
		err error
	}{res, err}
}

// PullImage 返回拉取结果。
func (fake *FakeImpl) PullImage(context.Context, cri.ImageManagerService, *v1.ImageSpec, *v1.AuthConfig, *v1.PodSandboxConfig) (string, error) {
	fakeReturns := fake.pullImageReturns
	return fakeReturns.res, fakeReturns.err
}

// PullImageReturns 设置 PullImage 方法的返回值。
func (fake *FakeImpl) PullImageReturns(res string, err error) {
	fake.pullImageReturns = struct {
		res string
		err error
	}{res, err}
}

// RemovePodSandbox 移除 Pod sandbox。
func (fake *FakeImpl) RemovePodSandbox(context.Context, cri.RuntimeService, string) error {
	fakeReturns := fake.removePodSandboxReturns
	return fakeReturns.res
}

// RemovePodSandboxReturns 设置 RemovePodSandbox 方法的返回值。
func (fake *FakeImpl) RemovePodSandboxReturns(res error) {
	fake.removePodSandboxReturns = struct {
		res error
	}{res}
}

// RuntimeConfig 返回运行时配置。
func (fake *FakeImpl) RuntimeConfig(context.Context, cri.RuntimeService) (*v1.RuntimeConfigResponse, error) {
	fakeReturns := fake.runtimeConfigReturns
	return fakeReturns.res, fakeReturns.err
}

// RuntimeConfigReturns 设置 RuntimeConfig 方法的返回值。
func (fake *FakeImpl) RuntimeConfigReturns(res *v1.RuntimeConfigResponse, err error) {
	fake.runtimeConfigReturns = struct {
		res *v1.RuntimeConfigResponse
		err error
	}{res, err}
}

// Status 返回运行时状态。
func (fake *FakeImpl) Status(context.Context, cri.RuntimeService, bool) (*v1.StatusResponse, error) {
	fakeReturns := fake.statusReturns
	return fakeReturns.res, fakeReturns.err
}

// StatusReturns 设置 Status 方法的返回值。
func (fake *FakeImpl) StatusReturns(res *v1.StatusResponse, err error) {
	fake.statusReturns = struct {
		res *v1.StatusResponse
		err error
	}{res, err}
}

// StopPodSandbox 停止 Pod sandbox。
func (fake *FakeImpl) StopPodSandbox(context.Context, cri.RuntimeService, string) error {
	fakeReturns := fake.stopPodSandboxReturns
	return fakeReturns.res
}

// StopPodSandboxReturns 设置 StopPodSandbox 方法的返回值。
func (fake *FakeImpl) StopPodSandboxReturns(res error) {
	fake.stopPodSandboxReturns = struct {
		res error
	}{res}
}
