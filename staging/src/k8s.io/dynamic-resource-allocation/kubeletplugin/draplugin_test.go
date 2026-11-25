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

package kubeletplugin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
)

// stubPlugin 实现最小化的 [DRAPlugin] 接口，并通过返回 ErrHealthNotSupported 退出健康上报。
type stubPlugin struct{}

// PrepareResourceClaims 返回空的准备结果。
func (s *stubPlugin) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]PrepareResult, error) {
	return nil, nil
}

// UnprepareResourceClaims 返回空的错误映射。
func (s *stubPlugin) UnprepareResourceClaims(ctx context.Context, claims []NamespacedObject) (map[types.UID]error, error) {
	return nil, nil
}

// HandleError 是空实现，不记录错误。
func (s *stubPlugin) HandleError(ctx context.Context, err error, msg string) {}

// WatchHealthStatus 直接返回 ErrHealthNotSupported，表示不支持健康上报。
func (s *stubPlugin) WatchHealthStatus(ctx context.Context, reports chan<- DeviceHealthReport) error {
	return ErrHealthNotSupported
}

// healthPlugin 在 stubPlugin 的基础上支持健康上报：依次发完固定的报告列表后返回。
type healthPlugin struct {
	stubPlugin
	// 内嵌桩实现，复用其余方法。
	// reports 是要依次发出的健康报告。
	reports []DeviceHealthReport
}

// WatchHealthStatus 顺序发送预置报告；上下文取消时返回 ctx.Err()。
func (p *healthPlugin) WatchHealthStatus(ctx context.Context, reports chan<- DeviceHealthReport) error {
	// 逐份发送预置报告。
	for _, r := range p.reports {
		// 发送过程中若上下文被取消则立即退出。
		select {
		// 上下文已取消。
		case <-ctx.Done():
			return ctx.Err()
		// 成功写入一份报告。
		case reports <- r:
		}
	}
	return nil
}

// TestStartWithoutHealth 验证不支持健康上报的驱动也能干净启动。
// 输入：stubPlugin + fake client，并关闭注册与 DRA 服务；断言 Start 无错误且 Stop 正常。
func TestStartWithoutHealth(t *testing.T) {
	// 测试上下文。
	ctx := t.Context()
	// 启动 Helper；不启动任何 socket，只关注装配是否成功。
	helper, err := Start(ctx, &stubPlugin{},
		DriverName("test-driver"),
		KubeClient(fake.NewClientset()),
		RegistrationService(false),
		DRAService(false),
	)
	// 断言启动成功。
	require.NoError(t, err)
	// 清理 Helper。
	helper.Stop()
}

// TestStartWithoutDRAAPI 验证关闭全部 DRA gRPC API 版本时 Start 立即失败，
// 而不是注册一个只提供健康附加服务、kubelet 无法使用的驱动。
// 输入：NodeV1 与 NodeV1beta1 均为 false；断言错误包含 "no supported DRA gRPC API"。
func TestStartWithoutDRAAPI(t *testing.T) {
	// 测试上下文。
	ctx := t.Context()
	// 关闭全部 DRA API 版本并尝试启动。
	_, err := Start(ctx, &stubPlugin{},
		DriverName("test-driver"),
		KubeClient(fake.NewClientset()),
		RegistrationService(false),
		DRAService(false),
		NodeV1(false),
		NodeV1beta1(false),
	)
	// 断言因没有受支持的 DRA gRPC API 而失败。
	require.ErrorContains(t, err, "no supported DRA gRPC API")
}

// fakeHealthStream 捕获辅助代码 gRPC 桥接器发送的响应，用于断言转换结果。
type fakeHealthStream struct {
	// 内嵌空流以满足接口；ctx 是流的上下文；sent 保存已发送的响应。
	grpc.ServerStream
	ctx  context.Context
	sent []*drahealthv1.NodeWatchResourcesResponse
}

// Context 返回测试中设置的上下文。
func (f *fakeHealthStream) Context() context.Context { return f.ctx }

// Send 把响应追加到 sent 列表，并始终返回 nil。
func (f *fakeHealthStream) Send(resp *drahealthv1.NodeWatchResourcesResponse) error {
	// 记录响应以便断言。
	f.sent = append(f.sent, resp)
	return nil
}

// TestHealthServerBridge 验证桥接器把版本无关的 WatchHealthStatus 报告转换为 v1 线路类型并发送。
// 输入：一份包含健康与不健康两台设备的报告；
// 断言发送恰好一份响应，且两台设备的标识、健康状态与消息正确。
func TestHealthServerBridge(t *testing.T) {
	// 驱动返回一份含两台设备的报告。
	plugin := &healthPlugin{
		reports: []DeviceHealthReport{
			{Devices: []DeviceHealth{
				// 第一台设备：健康且带消息。
				{PoolName: "pool", DeviceName: "dev0", Health: HealthStatusHealthy, Message: "ok"},
				// 第二台设备：不健康。
				{PoolName: "pool", DeviceName: "dev1", Health: HealthStatusUnhealthy},
			}},
		},
	}
	// 用假流捕获发送结果。
	stream := &fakeHealthStream{ctx: t.Context()}

	// 构造桥接器。
	bridge := &healthServerBridge{plugin: plugin}
	// 驱动桥接调用。
	err := bridge.NodeWatchResources(&drahealthv1.NodeWatchResourcesRequest{}, stream)
	// 断言无错误。
	require.NoError(t, err)

	// 断言恰好发送一份报告。
	require.Len(t, stream.sent, 1)
	// 取出报告中的设备列表。
	devices := stream.sent[0].GetDevices()
	// 断言列表中有两台设备。
	require.Len(t, devices, 2)

	// 断言第一台设备的池名、设备名、健康状态与消息。
	require.Equal(t, "pool", devices[0].GetDevice().GetPoolName())
	require.Equal(t, "dev0", devices[0].GetDevice().GetDeviceName())
	require.Equal(t, drahealthv1.HealthStatus_HEALTHY, devices[0].GetHealth())
	require.Equal(t, "ok", devices[0].GetMessage())

	// 断言第二台设备的设备名与健康状态。
	require.Equal(t, "dev1", devices[1].GetDevice().GetDeviceName())
	require.Equal(t, drahealthv1.HealthStatus_UNHEALTHY, devices[1].GetHealth())
}

// stalePlugin 先发送一份报告然后保持沉默，模拟健康监控卡住的驱动；
// 它把 HandleError 收到的错误记录到通道中供测试断言。
type stalePlugin struct {
	// 内嵌桩实现，复用其余方法；report 是唯一发送的一份报告；handleErr 记录错误。
	stubPlugin
	report    DeviceHealthReport
	handleErr chan error
}

// HandleError 非阻塞地把错误写入 handleErr（通道满时丢弃）。
func (p *stalePlugin) HandleError(ctx context.Context, err error, msg string) {
	// 非阻塞发送，避免测试卡住。
	select {
	// 记录错误。
	case p.handleErr <- err:
	default:
	}
}

// WatchHealthStatus 发送一份报告后一直等待上下文取消，模拟驱动不再重发。
func (p *stalePlugin) WatchHealthStatus(ctx context.Context, reports chan<- DeviceHealthReport) error {
	// 发送前先检查上下文是否已取消。
	select {
	case <-ctx.Done():
		return nil
		// 只发送一次报告。
	case reports <- p.report:
	}
	// 之后一直等待取消。
	<-ctx.Done()
	return nil
}

// TestHealthServerBridgeStaleWatchdog 验证驱动未在健康检查超时内重发报告时，桥接器通过 HandleError
// 上报可恢复错误（ErrRecoverable），且不会代驱动重发报告。
// 输入：把检查间隔缩短到 10ms、设备超时设为 20ms；
// 断言收到可恢复错误，且 kubelet 只收到驱动发出的那一份报告。
func TestHealthServerBridgeStaleWatchdog(t *testing.T) {
	// 保存原检查间隔以便恢复。
	origInterval := healthStaleCheckInterval
	// 缩短检查间隔以加快测试。
	healthStaleCheckInterval = 10 * time.Millisecond
	// 测试结束后恢复原值。
	defer func() { healthStaleCheckInterval = origInterval }()

	// 驱动只发一份带 20ms 超时的报告。
	plugin := &stalePlugin{
		report: DeviceHealthReport{Devices: []DeviceHealth{
			{PoolName: "pool", DeviceName: "dev0", Health: HealthStatusHealthy, HealthCheckTimeout: 20 * time.Millisecond},
		}},
		// 缓冲通道用于接收 HandleError 调用。
		handleErr: make(chan error, 1),
	}
	// 可取消的上下文，便于断言结束后让桥接调用退出。
	ctx, cancel := context.WithCancel(t.Context())
	// 确保测试退出时取消。
	defer cancel()
	// 假流捕获发送结果。
	stream := &fakeHealthStream{ctx: ctx}

	// 构造桥接器。
	bridge := &healthServerBridge{plugin: plugin}
	// 接收桥接调用的最终返回值。
	done := make(chan error, 1)
	// 在后台运行桥接调用，避免阻塞断言。
	go func() {
		done <- bridge.NodeWatchResources(&drahealthv1.NodeWatchResourcesRequest{}, stream)
	}()

	// 等待看门狗触发或超时。
	select {
	// 收到 HandleError 调用。
	case err := <-plugin.handleErr:
		// 断言错误带可恢复标记。
		require.ErrorIs(t, err, ErrRecoverable)
	// 超时保护，避免测试永久等待。
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the stale-health watchdog to fire")
	}

	// 断言桥接器没有代为重发：kubelet 只收到驱动的那一份报告。
	// 取消上下文让驱动的 WatchHealthStatus 退出。
	cancel()
	// 断言桥接调用正常结束。
	require.NoError(t, <-done)
	// 断言只发送了一份报告。
	require.Len(t, stream.sent, 1)
}

// TestHealthServerBridgeNotSupported 验证不支持健康上报的驱动会让 kubelet 的健康流以 Unimplemented 失败，
// 从而告知 kubelet 停止监听。
// 输入：stubPlugin；断言错误码为 Unimplemented 且没有发送任何响应。
func TestHealthServerBridgeNotSupported(t *testing.T) {
	// 假流捕获发送结果。
	stream := &fakeHealthStream{ctx: t.Context()}

	// 使用不支持健康上报的桩插件构造桥接器。
	bridge := &healthServerBridge{plugin: &stubPlugin{}}
	// 驱动桥接调用。
	err := bridge.NodeWatchResources(&drahealthv1.NodeWatchResourcesRequest{}, stream)
	// 断言返回 Unimplemented。
	require.Equal(t, codes.Unimplemented, status.Code(err))
	// 断言没有发送任何响应。
	require.Empty(t, stream.sent)
}

// TestMinHealthCheckTimeout 验证过期看门狗使用 kubelet 实际适用的最紧租约：
// 设备设置了超时则取其中最小值，否则用 kubelet 默认值；尤其当所有设备都要求更长租约时不应提前告警。
// 输入：多组报告；断言 minHealthCheckTimeout 的返回值与预期一致。
func TestMinHealthCheckTimeout(t *testing.T) {
	for name, tc := range map[string]struct {
		// report 是输入报告；want 是期望的最紧租约。
		report DeviceHealthReport
		want   time.Duration
	}{
		// 空报告：使用默认租约。
		"empty-report": {
			report: DeviceHealthReport{},
			want:   defaultKubeletHealthTimeout,
		},
		// 设备未设置超时：使用默认租约。
		"no-explicit-timeouts": {
			report: DeviceHealthReport{Devices: []DeviceHealth{{}, {}}},
			want:   defaultKubeletHealthTimeout,
		},
		// 存在比默认值更短的设备超时：取最小值。
		"shorter-than-default": {
			report: DeviceHealthReport{Devices: []DeviceHealth{
				{HealthCheckTimeout: 5 * time.Second},
				{HealthCheckTimeout: time.Minute},
			}},
			want: 5 * time.Second,
		},
		// 所有设备都要求更长租约：取其中最小值，而非默认值。
		"all-longer-than-default": {
			report: DeviceHealthReport{Devices: []DeviceHealth{
				{HealthCheckTimeout: 2 * time.Minute},
				{HealthCheckTimeout: 3 * time.Minute},
			}},
			want: 2 * time.Minute,
		},
		// 有一台设备未设置超时：最小值被压到默认值。
		"unset-device-caps-at-default": {
			report: DeviceHealthReport{Devices: []DeviceHealth{
				{HealthCheckTimeout: 2 * time.Minute},
				{},
			}},
			want: defaultKubeletHealthTimeout,
		},
		// 负超时按未设置处理：使用默认值。
		"negative-treated-as-default": {
			report: DeviceHealthReport{Devices: []DeviceHealth{
				{HealthCheckTimeout: -time.Second},
			}},
			want: defaultKubeletHealthTimeout,
		},
	} {
		// 逐用例执行。
		t.Run(name, func(t *testing.T) {
			// 断言计算出的租约与预期一致。
			require.Equal(t, tc.want, minHealthCheckTimeout(tc.report))
		})
	}
}
