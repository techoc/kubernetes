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

package v1beta1

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	watcherapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

// Stub 是 DevicePlugin 的桩实现，主要用于测试设备插件相关逻辑。
type Stub struct {
	devs                       []*pluginapi.Device
	socket                     string
	resourceName               string
	preStartContainerFlag      bool
	getPreferredAllocationFlag bool

	stop   chan interface{}
	wg     sync.WaitGroup
	update chan []*pluginapi.Device

	server *grpc.Server

	// allocFunc 处理分配请求。
	allocFunc stubAllocFunc

	// getPreferredAllocFunc 处理首选分配请求。
	getPreferredAllocFunc stubGetPreferredAllocFunc

	// registerControlFunc 控制是否自动向 kubelet 注册。
	registerControlFunc stubRegisterControlFunc

	registrationStatus chan watcherapi.RegistrationStatus // 供测试使用。
	endpoint           string                             // 供测试使用。

	// kubeletRestartWatcher 监听 kubelet 套接字文件，用于检测 kubelet 重启。
	kubeletRestartWatcher *fsnotify.Watcher

	pluginapi.UnsafeDevicePluginServer
	watcherapi.UnsafeRegistrationServer
}

// stubGetPreferredAllocFunc 是收到 kubelet 首选分配请求时调用的函数。
type stubGetPreferredAllocFunc func(r *pluginapi.PreferredAllocationRequest, devs map[string]*pluginapi.Device) (*pluginapi.PreferredAllocationResponse, error)

// defaultGetPreferredAllocFunc 是首选分配请求的默认实现，返回空响应。
func defaultGetPreferredAllocFunc(r *pluginapi.PreferredAllocationRequest, devs map[string]*pluginapi.Device) (*pluginapi.PreferredAllocationResponse, error) {
	var response pluginapi.PreferredAllocationResponse

	return &response, nil
}

// stubAllocFunc 是收到 kubelet 分配请求时调用的函数。
type stubAllocFunc func(r *pluginapi.AllocateRequest, devs map[string]*pluginapi.Device) (*pluginapi.AllocateResponse, error)

// defaultAllocFunc 是分配请求的默认实现，返回空响应。
func defaultAllocFunc(r *pluginapi.AllocateRequest, devs map[string]*pluginapi.Device) (*pluginapi.AllocateResponse, error) {
	var response pluginapi.AllocateResponse

	return &response, nil
}

// stubRegisterControlFunc 决定是否响应 kubelet 的注册请求。
type stubRegisterControlFunc func() bool

// defaultRegisterControlFunc 默认始终允许注册。
func defaultRegisterControlFunc() bool {
	return true
}

// NewDevicePluginStub 返回初始化完成的 DevicePlugin Stub。
func NewDevicePluginStub(logger klog.Logger, devs []*pluginapi.Device, socket string, name string, preStartContainerFlag bool, getPreferredAllocationFlag bool) *Stub {

	// 创建 inotify watcher 以检测 kubelet 重启；失败时直接 panic，
	// 因为测试桩无法在没有该能力的情况下模拟插件生命周期。
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error(err, "Watcher creation failed")
		panic(err)
	}

	return &Stub{
		devs:                       devs,
		socket:                     socket,
		resourceName:               name,
		preStartContainerFlag:      preStartContainerFlag,
		getPreferredAllocationFlag: getPreferredAllocationFlag,
		registerControlFunc:        defaultRegisterControlFunc,

		stop:   make(chan interface{}),
		update: make(chan []*pluginapi.Device),

		allocFunc:             defaultAllocFunc,
		getPreferredAllocFunc: defaultGetPreferredAllocFunc,
		kubeletRestartWatcher: watcher,
	}
}

// SetGetPreferredAllocFunc 设置设备插件的首选分配处理函数。
func (m *Stub) SetGetPreferredAllocFunc(f stubGetPreferredAllocFunc) {
	m.getPreferredAllocFunc = f
}

// SetAllocFunc 设置设备插件的分配处理函数。
func (m *Stub) SetAllocFunc(f stubAllocFunc) {
	m.allocFunc = f
}

// SetRegisterControlFunc 设置设备插件的注册控制函数。
func (m *Stub) SetRegisterControlFunc(f stubRegisterControlFunc) {
	m.registerControlFunc = f
}

// Start 启动设备插件的 gRPC 服务端，只能调用一次。
func (m *Stub) Start(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.Info("Starting device plugin server")
	// 先清理可能残留的套接字文件，避免 Listen 因文件已存在而失败。
	err := m.cleanup()
	if err != nil {
		return err
	}

	sock, err := net.Listen("unix", m.socket)
	if err != nil {
		return err
	}

	m.wg.Add(1)
	m.server = grpc.NewServer([]grpc.ServerOption{}...)
	pluginapi.RegisterDevicePluginServer(m.server, m)
	watcherapi.RegisterRegistrationServer(m.server, m)

	err = m.kubeletRestartWatcher.Add(filepath.Dir(m.socket))
	if err != nil {
		logger.Error(err, "Failed to add watch", "devicePluginPath", pluginapi.DevicePluginPath)
		return err
	}

	go func() {
		defer m.wg.Done()
		if err = m.server.Serve(sock); err != nil {
			logger.Error(err, "Error while serving device plugin registration grpc server")
		}
	}()

	// 轮询确认服务端已可连接，保证返回后调用方可以立即访问套接字。
	var lastDialErr error
	wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		var conn *grpc.ClientConn
		_, conn, lastDialErr = dial(ctx, m.socket)
		if lastDialErr != nil {
			return false, nil
		}
		conn.Close()
		return true, nil
	})
	if lastDialErr != nil {
		return lastDialErr
	}

	logger.Info("Starting to serve on socket", "socket", m.socket)
	return nil
}

// Restart 停止并重新启动设备插件的 gRPC 服务端。
func (m *Stub) Restart(ctx context.Context) error {
	klog.FromContext(ctx).Info("Restarting Device Plugin server")
	if m.server == nil {
		return nil
	}

	m.server.Stop()
	m.server = nil

	return m.Start(ctx)
}

// Stop 停止 gRPC 服务端。可以在未调用 Start 的情况下调用，也可以重复调用。
// 但不能由多个 goroutine 并发调用！
func (m *Stub) Stop(logger klog.Logger) error {
	logger.Info("Stopping device plugin server")
	if m.server == nil {
		return nil
	}

	m.kubeletRestartWatcher.Close()

	m.server.Stop()
	m.wg.Wait()
	m.server = nil
	close(m.stop) // 关闭该通道可防止服务端被重新启动。

	return m.cleanup()
}

// Watch 监听 kubelet 套接字文件，检测到 kubelet 重启后重启并重新注册插件。
func (m *Stub) Watch(ctx context.Context, kubeletEndpoint, resourceName, pluginSockDir string) {
	logger := klog.FromContext(ctx)
	for {
		select {
		// 通过监听新创建的 kubelet 套接字文件来检测 kubelet 重启；
		// 一旦发现，就重启设备插件服务端并重新注册。
		case event := <-m.kubeletRestartWatcher.Events:
			if event.Name == kubeletEndpoint && event.Op&fsnotify.Create == fsnotify.Create {
				logger.Info("inotify: file created, restarting", "kubeletEndpoint", kubeletEndpoint)
				// 重启可能因端口/套接字竞争而短暂失败，因此带超时重试。
				var lastErr error

				err := wait.PollUntilContextTimeout(ctx, 10*time.Second, 2*time.Minute, false, func(context.Context) (done bool, err error) {
					restartErr := m.Restart(ctx)
					if restartErr == nil {
						return true, nil
					}
					logger.Error(restartErr, "Retrying after error")
					lastErr = restartErr
					return false, nil
				})
				if err != nil {
					logger.Error(err, "Unable to restart server: wait timed out", "lastErr", lastErr.Error())
					panic(err)
				}

				if ok := m.registerControlFunc(); ok {
					if err := m.Register(ctx, kubeletEndpoint, resourceName, pluginSockDir); err != nil {
						logger.Error(err, "Unable to register to kubelet")
						panic(err)
					}
				}
			}

		// 记录 inotify 的其他错误。
		case err := <-m.kubeletRestartWatcher.Errors:
			logger.Error(err, "inotify error")
		}
	}
}

// GetInfo 返回插件信息，供 kubelet 的插件管理器识别插件类型与版本。
func (m *Stub) GetInfo(ctx context.Context, req *watcherapi.InfoRequest) (*watcherapi.PluginInfo, error) {
	klog.FromContext(ctx).Info("GetInfo")
	return &watcherapi.PluginInfo{
		Type:              watcherapi.DevicePlugin,
		Name:              m.resourceName,
		Endpoint:          m.endpoint,
		SupportedVersions: []string{pluginapi.Version}}, nil
}

// NotifyRegistrationStatus 接收 watcher 发来的注册状态通知。
func (m *Stub) NotifyRegistrationStatus(ctx context.Context, status *watcherapi.RegistrationStatus) (*watcherapi.RegistrationStatusResponse, error) {
	logger := klog.FromContext(ctx)
	if m.registrationStatus != nil {
		m.registrationStatus <- *status
	}
	if !status.PluginRegistered {
		logger.Info("Registration failed", "err", status.Error)
	}
	return &watcherapi.RegistrationStatusResponse{}, nil
}

// Register 将设备插件以指定资源名注册到 kubelet。
func (m *Stub) Register(ctx context.Context, kubeletEndpoint, resourceName string, pluginSockDir string) error {
	logger := klog.FromContext(ctx)
	logger.Info("Register", "kubeletEndpoint", kubeletEndpoint, "resourceName", resourceName, "socket", pluginSockDir)

	// 存在 DEPRECATION 标记文件时跳过注册，用于兼容旧版 kubelet 的行为。
	if pluginSockDir != "" {
		if _, err := os.Stat(pluginSockDir + "DEPRECATION"); err == nil {
			logger.Info("Deprecation file found. Skip registration")
			return nil
		}
	}
	logger.Info("Deprecation file not found. Invoke registration")
	// 注册调用设置 10 秒超时，避免 kubelet 无响应时插件永久阻塞。
	ctxDial, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	//nolint:staticcheck // SA1019: grpc.DialContext is deprecated: use NewClient instead.
	conn, err := grpc.DialContext(ctxDial, kubeletEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     filepath.Base(m.socket),
		ResourceName: resourceName,
		Options: &pluginapi.DevicePluginOptions{
			PreStartRequired:                m.preStartContainerFlag,
			GetPreferredAllocationAvailable: m.getPreferredAllocationFlag,
		},
	}

	_, err = client.Register(ctx, reqt)
	if err != nil {
		// 注册失败时停止服务端，避免留下不可用的插件实例。
		m.server.Stop()
		logger.Error(err, "Client unable to register to kubelet")
		return err
	}
	logger.Info("Device Plugin registered with the Kubelet")
	return err
}

// GetDevicePluginOptions 返回设备插件的选项配置。
func (m *Stub) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	options := &pluginapi.DevicePluginOptions{
		PreStartRequired:                m.preStartContainerFlag,
		GetPreferredAllocationAvailable: m.getPreferredAllocationFlag,
	}
	return options, nil
}

// PreStartContainer 在容器启动前重置设备状态。
func (m *Stub) PreStartContainer(ctx context.Context, r *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	klog.FromContext(ctx).Info("PreStartContainer", "request", r)
	return &pluginapi.PreStartContainerResponse{}, nil
}

// ListAndWatch 向 kubelet 推送设备列表，并在 Update 被调用时推送更新。
func (m *Stub) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	// 这里使用 klog.TODO()：该方法必须与 api_grpc.pb.go 中生成的设备插件
	// gRPC 签名一致，而该签名不接受 logger 参数。
	klog.TODO().Info("ListAndWatch")

	s.Send(&pluginapi.ListAndWatchResponse{Devices: m.devs})

	for {
		select {
		case <-m.stop:
			return nil
		case updated := <-m.update:
			s.Send(&pluginapi.ListAndWatchResponse{Devices: updated})
		}
	}
}

// Update 允许设备插件通过 ListAndWatch 推送新的设备列表。
func (m *Stub) Update(devs []*pluginapi.Device) {
	m.update <- devs
}

// GetPreferredAllocation 从可用设备集合中选出首选的分配方案。
func (m *Stub) GetPreferredAllocation(ctx context.Context, r *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	klog.FromContext(ctx).Info("GetPreferredAllocation", "request", r)

	// 按设备 ID 建立索引，便于处理函数按 ID 查找设备。
	devs := make(map[string]*pluginapi.Device)

	for _, dev := range m.devs {
		devs[dev.ID] = dev
	}

	return m.getPreferredAllocFunc(r, devs)
}

// Allocate 执行模拟的设备分配。
func (m *Stub) Allocate(ctx context.Context, r *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	klog.FromContext(ctx).Info("Allocate", "request", r)

	// 按设备 ID 建立索引，便于分配处理函数按 ID 查找设备。
	devs := make(map[string]*pluginapi.Device)

	for _, dev := range m.devs {
		devs[dev.ID] = dev
	}

	return m.allocFunc(r, devs)
}

// cleanup 删除残留的套接字文件；文件不存在时视为成功。
func (m *Stub) cleanup() error {
	if err := os.Remove(m.socket); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
