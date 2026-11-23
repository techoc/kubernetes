/*
Copyright 2022 The Kubernetes Authors.

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

package kubeletplugin

import (
	"context"
	"fmt"
	"sync/atomic"

	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

// registrationServer implements the kubelet plugin registration gRPC interface.
// registrationServer 实现 kubelet 插件注册 gRPC 接口。
type registrationServer struct {
	// driverName 是驱动名；draEndpointPath 是 DRA 服务套接字路径；
	// supportedVersions 是所提供的服务/版本列表；status 记录 kubelet 的注册结果。
	driverName        string
	draEndpointPath   string
	supportedVersions []string
	status            *registerapi.RegistrationStatus

	// 下面两个原子指针可在运行时注入错误，用于测试或强制注册流程失败；
	// 存 nil 表示不注入错误。
	getInfoError                  atomic.Pointer[error]
	notifyRegistrationStatusError atomic.Pointer[error]

	// 内嵌 UnsafeRegistrationServer，便于接口未来扩展方法而不破坏编译。
	registerapi.UnsafeRegistrationServer
}

// 编译期断言：确保本类型实现注册服务接口。
var _ registerapi.RegistrationServer = &registrationServer{}

// GetInfo is the RPC invoked by plugin watcher.
// GetInfo 是 kubelet 插件监视器调用的 RPC，返回插件类型、名称、端点与支持的版本。
func (e *registrationServer) GetInfo(ctx context.Context, req *registerapi.InfoRequest) (*registerapi.PluginInfo, error) {
	// 若注入了错误则直接返回。
	if err := e.getGetInfoError(); err != nil {
		return nil, err
	}
	// 构造插件信息：类型为 DRA 插件，端点指向 DRA 服务路径。
	return &registerapi.PluginInfo{
		Type:              registerapi.DRAPlugin,
		Name:              e.driverName,
		Endpoint:          e.draEndpointPath,
		SupportedVersions: e.supportedVersions,
	}, nil
}

// NotifyRegistrationStatus is the RPC invoked by plugin watcher.
// NotifyRegistrationStatus 是插件监视器调用的 RPC，用于通知注册结果。
func (e *registrationServer) NotifyRegistrationStatus(ctx context.Context, status *registerapi.RegistrationStatus) (*registerapi.RegistrationStatusResponse, error) {
	// 若注入了错误则直接返回。
	if err := e.getNotifyRegistrationStatusError(); err != nil {
		return nil, err
	}
	// 记录 kubelet 返回的状态。
	e.status = status
	// 注册失败时把错误包装后返回。
	if !status.PluginRegistered {
		return nil, fmt.Errorf("failed registration process: %+v", status.Error)
	}

	// 注册成功返回空响应。
	return &registerapi.RegistrationStatusResponse{}, nil
}

// getGetInfoError 读取注入的 GetInfo 错误；未注入时返回 nil。
func (e *registrationServer) getGetInfoError() error {
	// 原子读取错误指针。
	errPtr := e.getInfoError.Load()
	// 指针为空表示没有注入错误。
	if errPtr == nil {
		return nil
	}
	return *errPtr
}

// getNotifyRegistrationStatusError 读取注入的 NotifyRegistrationStatus 错误；未注入时返回 nil。
func (e *registrationServer) getNotifyRegistrationStatusError() error {
	// 原子读取错误指针。
	errPtr := e.notifyRegistrationStatusError.Load()
	// 指针为空表示没有注入错误。
	if errPtr == nil {
		return nil
	}
	return *errPtr
}

// setGetInfoError sets the error to be returned by the GetInfo handler of the registration server.
// If a non-nil error is provided, subsequent GetInfo calls will return this error.
// Passing nil as the err argument will clear any previously set error, effectively disabling erroring.
// setGetInfoError 设置 GetInfo 返回的错误；传入 nil 会清除此前设置的错误。
func (e *registrationServer) setGetInfoError(err error) {
	// nil 表示关闭错误注入。
	if err == nil {
		e.getInfoError.Store(nil)
		return
	}
	// 非 nil 错误存入原子指针。
	e.getInfoError.Store(&err)
}

// setNotifyRegistrationStatusError sets the error to be returned by the NotifyRegistrationStatus handler
// of the registration server.
// If a non-nil error is provided, subsequent NotifyRegistrationStatus calls will return this error.
// Passing nil as the err argument will clear any previously set error, effectively disabling erroring.
// setNotifyRegistrationStatusError 设置 NotifyRegistrationStatus 返回的错误；传入 nil 清除。
func (e *registrationServer) setNotifyRegistrationStatusError(err error) {
	// nil 表示关闭错误注入。
	if err == nil {
		e.notifyRegistrationStatusError.Store(nil)
		return
	}
	// 非 nil 错误存入原子指针。
	e.notifyRegistrationStatusError.Store(&err)
}
