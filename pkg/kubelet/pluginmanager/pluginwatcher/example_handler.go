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

package pluginwatcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"

	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/pluginwatcher/example_plugin_apis/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/pluginwatcher/example_plugin_apis/v1beta2"
)

// exampleHandler 是供测试使用的示例插件处理器，用事件通道观察插件生命周期。
type exampleHandler struct {
	SupportedVersions []string
	ExpectedNames     map[string]int

	eventChans map[string]chan examplePluginEvent // map[pluginName]eventChan

	m sync.Mutex

	permitDeprecatedDir bool
}

// examplePluginEvent 表示示例处理器观察到的事件类型。
type examplePluginEvent int

// 事件类型：校验、注册、注销。
const (
	exampleEventValidate   examplePluginEvent = 0
	exampleEventRegister   examplePluginEvent = 1
	exampleEventDeRegister examplePluginEvent = 2
)

// NewExampleHandler 创建示例插件处理器。
func NewExampleHandler(supportedVersions []string, permitDeprecatedDir bool) *exampleHandler {
	return &exampleHandler{
		SupportedVersions: supportedVersions,
		ExpectedNames:     make(map[string]int),

		eventChans:          make(map[string]chan examplePluginEvent),
		permitDeprecatedDir: permitDeprecatedDir,
	}
}

// ValidatePlugin 校验插件是否符合测试预期，并通知等待中的测试。
func (p *exampleHandler) ValidatePlugin(ctx context.Context, pluginName string, endpoint string, versions []string) error {
	p.SendEvent(ctx, pluginName, exampleEventValidate)

	// 名称不在预期集合中时返回错误，帮助测试尽早失败。
	n, ok := p.DecreasePluginCount(pluginName)
	if !ok && n > 0 {
		return fmt.Errorf("pluginName('%s') wasn't expected (count is %d)", pluginName, n)
	}

	// 版本列表必须与期望完全一致（顺序也一致）。
	if !reflect.DeepEqual(versions, p.SupportedVersions) {
		return fmt.Errorf("versions('%v') != supported versions('%v')", versions, p.SupportedVersions)
	}

	// 示例处理器要求 endpoint 非空。
	if len(endpoint) == 0 {
		return errors.New("expecting non empty endpoint")
	}

	return nil
}

// RegisterPlugin 验证插件 gRPC 服务已就绪且两个版本的 API 都可调用。
func (p *exampleHandler) RegisterPlugin(ctx context.Context, pluginName, endpoint string, versions []string) error {
	p.SendEvent(ctx, pluginName, exampleEventRegister)

	// 拨号成功即证明 gRPC server 已就绪。
	_, conn, err := dial(ctx, endpoint, time.Second)
	if err != nil {
		return fmt.Errorf("failed dialing endpoint (%s): %v", endpoint, err)
	}
	defer conn.Close()

	// 插件处理器应能使用任一已声明的服务 API 版本。
	v1beta1Client := v1beta1.NewExampleClient(conn)
	v1beta2Client := v1beta2.NewExampleClient(conn)

	// 测试 v1beta1 的 GetExampleInfo。
	_, err = v1beta1Client.GetExampleInfo(ctx, &v1beta1.ExampleRequest{})
	if err != nil {
		return fmt.Errorf("failed GetExampleInfo for v1beta2Client(%s): %v", endpoint, err)
	}

	// 测试 v1beta2 的 GetExampleInfo。
	_, err = v1beta2Client.GetExampleInfo(ctx, &v1beta2.ExampleRequest{})
	if err != nil {
		return fmt.Errorf("failed GetExampleInfo for v1beta2Client(%s): %v", endpoint, err)
	}

	return nil
}

// DeRegisterPlugin 发送注销事件。
func (p *exampleHandler) DeRegisterPlugin(ctx context.Context, pluginName string) {
	p.SendEvent(ctx, pluginName, exampleEventDeRegister)
}

// SendEvent 把事件写入该插件的事件通道，测试通过通道同步进度。
func (p *exampleHandler) SendEvent(ctx context.Context, pluginName string, event examplePluginEvent) {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Sending event for plugin", "pluginName", pluginName, "event", event, "channel", p.eventChans[pluginName])
	p.eventChans[pluginName] <- event
}

// DecreasePluginCount 查询插件名是否在预期集合中；不存在时返回 -1 与 false。
func (p *exampleHandler) DecreasePluginCount(pluginName string) (old int, ok bool) {
	p.m.Lock()
	defer p.m.Unlock()

	v, ok := p.ExpectedNames[pluginName]
	if !ok {
		v = -1
	}

	return v, ok
}

// dial 与探测到的插件 socket 建立 gRPC 通信，
// 参见 https://godoc.org/google.golang.org/grpc#Dial。
func dial(ctx context.Context, unixSocketPath string, timeout time.Duration) (registerapi.RegistrationClient, *grpc.ClientConn, error) {
	// 按调用方给定的超时拨号，避免测试挂起。
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 通过 unix socket 阻塞拨号，确保返回时连接已可用。
	c, err := grpc.DialContext(ctx, unixSocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to dial socket %s, err: %v", unixSocketPath, err)
	}

	return registerapi.NewRegistrationClient(c), c, nil
}
