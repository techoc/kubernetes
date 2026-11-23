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
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
)

// This file defines the version-neutral device health reporting API that DRA
// driver authors implement through [DRAPlugin.WatchHealthStatus]. The versioned
// DRAResourceHealth gRPC API is deliberately kept out of the Go package API:
// the helper translates between the hand-written types below and whatever gRPC
// version a kubelet supports. This means driver authors do not have to adapt
// their source code when the gRPC API is promoted to a newer version.
// 本文件定义了与版本无关的设备健康上报 API，DRA 驱动通过 [DRAPlugin.WatchHealthStatus] 实现它。
// 带版本的 DRAResourceHealth gRPC API 刻意不进入 Go 包 API：本辅助代码负责在下面的手写类型与
// kubelet 支持的任意 gRPC 版本之间做转换，因此驱动作者无需在 gRPC API 升级为新版本时修改源码。

// HealthStatus describes the health of a device as reported by a DRA driver.
// HealthStatus 表示 DRA 驱动上报的设备健康状态，底层类型为 string。
type HealthStatus string

const (
	// HealthStatusUnknown indicates that the health of the device cannot be
	// determined. This is also the default when no status is set.
	// HealthStatusUnknown 表示无法确定设备的健康状态；未设置状态时也按该值处理。
	HealthStatusUnknown HealthStatus = "Unknown"
	// HealthStatusHealthy indicates that the device is operating normally.
	// HealthStatusHealthy 表示设备运行正常。
	HealthStatusHealthy HealthStatus = "Healthy"
	// HealthStatusUnhealthy indicates that the device has reported a problem.
	// HealthStatusUnhealthy 表示设备报告了问题。
	HealthStatusUnhealthy HealthStatus = "Unhealthy"
)

// DeviceHealth describes the health of a single device managed by a DRA driver.
// DeviceHealth 描述由 DRA 驱动管理的一台设备的健康情况。
type DeviceHealth struct {
	// PoolName is the name of the pool which contains the device.
	// PoolName 是包含该设备的设备池名称。
	PoolName string
	// DeviceName is the name of the device within the pool.
	// DeviceName 是设备在该池内的名称。
	DeviceName string
	// Health is the current health status of the device. An empty value is
	// treated as [HealthStatusUnknown].
	// Health 是当前健康状态；空值按 [HealthStatusUnknown] 处理。
	Health HealthStatus
	// LastUpdated is when the driver last determined this status. The zero
	// value indicates that the time is unknown.
	// LastUpdated 是驱动最后一次判定该状态的时间；零值表示时间未知。
	LastUpdated time.Time
	// HealthCheckTimeout is the interval after which the kubelet should treat
	// the device's health as unknown if it has not received a fresh report.
	// If zero or negative, the kubelet uses a default. The value is truncated
	// to whole seconds on the wire.
	// HealthCheckTimeout 是 kubelet 在未收到新上报后将设备健康视为未知的间隔。
	// 零值或负值表示使用 kubelet 默认值；在线路上会截断为整秒。
	HealthCheckTimeout time.Duration
	// Message is an optional human-readable detail about the device's health.
	// Message 是关于设备健康的可选可读详情。
	Message string
}

// DeviceHealthReport is a snapshot of the health of devices managed by a DRA
// driver. It usually covers all of the driver's devices, but may cover a
// subset: the kubelet reconciles each report with its internal cache, and a
// device absent from a report keeps its previously reported health until that
// goes stale (see [DeviceHealth.HealthCheckTimeout]), after which the kubelet
// reports it as unknown.
//
// A driver with more than one health source (for example an event-driven
// hardware monitor plus a polling prober for devices the monitor cannot see)
// should send one report per source, each covering only that source's
// devices. Then a resend only vouches for devices which were actually
// verified, and the failure of one source decays exactly its devices to
// unknown.
// DeviceHealthReport 是驱动所管理设备健康状况的一份快照。
// 它通常覆盖驱动的全部设备，但也可以只覆盖一部分：kubelet 会把每份报告与内部缓存对账，
// 未出现在报告中的设备会保留此前上报的健康状态，直到该状态过期（见 [DeviceHealth.HealthCheckTimeout]），
// 之后 kubelet 会把它报告为未知。
//
// 拥有多个健康来源的驱动（例如事件驱动的硬件监控器加上用于监控器看不到设备的轮询探针）
// 应当每个来源发送一份报告，且每份只覆盖该来源的设备。这样重发只担保确实被核验过的设备，
// 某个来源失效也只会让它的设备衰减为未知。
type DeviceHealthReport struct {
	// Devices is the list of devices covered by this report and their
	// current health.
	// Devices 是本报告覆盖的设备列表及其当前健康状态。
	Devices []DeviceHealth
}

// ErrHealthNotSupported is returned by [DRAPlugin.WatchHealthStatus] to
// indicate that a driver does not support device health reporting. The helper
// then fails the kubelet's health stream with [codes.Unimplemented], which
// tells the kubelet to stop watching.
// ErrHealthNotSupported 由 [DRAPlugin.WatchHealthStatus] 返回，表示驱动不支持设备健康上报。
// 辅助代码随后以 [codes.Unimplemented] 让 kubelet 的健康流失败，告知 kubelet 停止监听。
var ErrHealthNotSupported = errors.New("device health reporting is not supported by this driver")

// defaultKubeletHealthTimeout mirrors the kubelet's default lease for device
// health data: entries which are not refreshed within this window are
// reported as unknown.
// defaultKubeletHealthTimeout 对应 kubelet 默认的设备健康数据租约：
// 在此窗口内未刷新的条目会被报告为未知。
const defaultKubeletHealthTimeout = 30 * time.Second

// healthStaleCheckInterval is how often the helper checks whether the driver
// has let its health reports go stale. A variable so tests can shorten it.
// healthStaleCheckInterval 是辅助代码检查驱动上报是否过期的频率；
// 设为变量以便测试缩短该间隔。
var healthStaleCheckInterval = 10 * time.Second

// healthServerBridge implements the versioned v1 DRAResourceHealth gRPC
// server by calling the version-neutral [DRAPlugin.WatchHealthStatus]
// implementation provided by the driver. The helper registers it (and, via a
// conversion wrapper, the older v1alpha1 server) so that the gRPC API version
// is fully hidden from driver authors.
// healthServerBridge 通过调用驱动提供的版本无关 [DRAPlugin.WatchHealthStatus] 实现带版本的 v1
// DRAResourceHealth gRPC 服务。辅助代码注册它（并通过转换包装注册更旧的 v1alpha1 服务），
// 从而对驱动作者完全隐藏 gRPC API 的版本差异。
type healthServerBridge struct {
	// 内嵌 UnsafeDRAResourceHealthServer，使未实现的方法也能满足接口。
	// plugin 是驱动提供的版本无关插件实现。
	drahealthv1.UnsafeDRAResourceHealthServer
	plugin DRAPlugin
}

// 编译期断言：确保桥接器实现了 v1 DRAResourceHealthServer 接口。
var _ drahealthv1.DRAResourceHealthServer = &healthServerBridge{}

// NodeWatchResources 实现 v1 DRAResourceHealth 服务的节点健康监听 RPC。
// 它在后台启动驱动的 WatchHealthStatus，把报告转换成 v1 线路类型后发送给 kubelet，
// 同时监控驱动是否让报告过期。
func (h *healthServerBridge) NodeWatchResources(_ *drahealthv1.NodeWatchResourcesRequest, stream drahealthv1.DRAResourceHealth_NodeWatchResourcesServer) error {
	// A derived context lets us tell the driver to stop producing reports when
	// sending to the kubelet fails or the stream is otherwise done.
	// 派生一个可取消的上下文：发送失败或流结束时通知驱动停止产生报告。
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// 驱动把报告写入该通道，桥接器从通道取出并发送给 kubelet。
	reports := make(chan DeviceHealthReport)
	// 缓冲为 1 的通道接收驱动实现返回的错误，避免生产 goroutine 阻塞。
	errCh := make(chan error, 1)
	// 在后台运行驱动的健康监听；退出时关闭 reports 并回报最终错误。
	go func() {
		defer close(reports)
		errCh <- h.plugin.WatchHealthStatus(ctx, reports)
	}()

	// The kubelet reports device health as unknown when it is not refreshed
	// within each device's health check timeout. Watch for drivers which let
	// their reports go stale and surface it as a (recoverable) error instead
	// of silently degrading. The helper deliberately does not re-send the
	// last report itself: resending is the driver's job, so that a wedged
	// monitor decays to unknown instead of being kept alive by the helper.
	// 用定时器检测报告是否过期：过期时通过 HandleError 上报可恢复错误，而不是静默降级。
	// 辅助代码刻意不代为重发上一份报告：重发是驱动的职责，否则卡住的监控器会被辅助代码续命。
	staleCheck := time.NewTicker(healthStaleCheckInterval)
	defer staleCheck.Stop()
	// 记录最近一次收到报告的时间，用于判断是否过期。
	lastReport := time.Now()
	// 过期阈值初始为 kubelet 默认值，收到报告后按报告内容调整。
	staleAfter := defaultKubeletHealthTimeout
	// 避免同一个过期事件被重复上报。
	staleReported := false

	// 主循环标签：报告通道关闭时跳出循环。
receive:
	// 在“接收报告”与“检查过期”之间循环等待。
	for {
		// 同时监听报告通道、过期定时器与驱动结束事件。
		select {
		// 收到驱动上报。
		case report, ok := <-reports:
			// 通道关闭说明驱动已退出，结束主循环。
			if !ok {
				break receive
			}
			// 刷新最近一次报告的时间。
			lastReport = time.Now()
			// 按本份报告中最紧的租约更新过期阈值。
			staleAfter = minHealthCheckTimeout(report)
			// 收到新报告后重置过期告警标志。
			staleReported = false
			// 转换为 v1 类型后发送给 kubelet。
			if err := stream.Send(deviceHealthReportToV1(report)); err != nil {
				// Stop the driver and drain remaining reports so that the
				// producing goroutine can observe the cancellation and exit.
				// 取消驱动，并把通道中剩余报告排空，让生产 goroutine 观察到取消后退出。
				cancel()
				// 排空剩余报告，避免驱动 goroutine 阻塞在发送上。
				for range reports { //nolint:revive // Intentionally draining.
				}
				<-errCh
				return err
			}
		// 定时检查上报是否已经过期。
		case <-staleCheck.C:
			// 已告警过或尚未超过阈值时跳过本次检查。
			if staleReported || time.Since(lastReport) <= staleAfter {
				continue
			}
			// 标记本次过期已上报，避免重复告警。
			staleReported = true
			// 构造带可恢复标记的错误，说明 kubelet 会把设备健康视为未知。
			err := fmt.Errorf("no device health report was sent for %s, the kubelet now reports device health as unknown: %w", time.Since(lastReport).Truncate(time.Second), ErrRecoverable)
			// 通过驱动的 HandleError 上报，提示必须在每个设备的超时内重发报告。
			h.plugin.HandleError(ctx, err, "device health reports are stale, WatchHealthStatus must re-send within each device's HealthCheckTimeout")
		}
	}

	// 主循环结束后读取驱动返回的最终错误。
	err := <-errCh
	// 驱动不支持健康上报。
	if errors.Is(err, ErrHealthNotSupported) {
		// The health service is always advertised and served; a driver
		// declines health reporting by returning ErrHealthNotSupported.
		// Reporting it as Unimplemented tells the kubelet to stop watching.
		// 健康服务始终注册；驱动以 ErrHealthNotSupported 拒绝上报时返回 Unimplemented，
		// 从而让 kubelet 停止监听。
		return status.Error(codes.Unimplemented, ErrHealthNotSupported.Error())
	}
	return err
}

// minHealthCheckTimeout 返回 kubelet 对该报告适用的最紧租约：所有设备中最小的有效健康检查超时，
// 未显式设置正超时的设备按 kubelet 默认值计算。
// 当所有设备都要求更长租约时，结果可以长于默认值；空报告使用默认值。
// minHealthCheckTimeout returns the tightest lease the kubelet applies to the
// report: the smallest effective per-device health check timeout, where a
// device without an explicit positive timeout gets the kubelet's default.
// This can be longer than the default when all devices ask for a longer
// lease. Empty reports use the default.
func minHealthCheckTimeout(report DeviceHealthReport) time.Duration {
	// 空报告没有设备级超时，直接返回默认值。
	if len(report.Devices) == 0 {
		return defaultKubeletHealthTimeout
	}
	// 用零值表示尚未确定最小值。
	var min time.Duration
	// 遍历设备求最紧的超时。
	for _, d := range report.Devices {
		// 取设备自身配置的租约。
		lease := d.HealthCheckTimeout
		// 非正数表示未配置，回退到 kubelet 默认值。
		if lease <= 0 {
			lease = defaultKubeletHealthTimeout
		}
		// 首台设备或遇到更紧的租约时更新最小值。
		if min == 0 || lease < min {
			min = lease
		}
	}
	return min
}

// deviceHealthReportToV1 把版本无关的 DeviceHealthReport 转换为 v1 线路类型。
func deviceHealthReportToV1(report DeviceHealthReport) *drahealthv1.NodeWatchResourcesResponse {
	// 预分配设备切片，容量与报告中的设备数一致。
	devices := make([]*drahealthv1.DeviceHealth, 0, len(report.Devices))
	// 逐台设备转换。
	for _, d := range report.Devices {
		// 填入设备标识、健康状态、更新时间、截断为整秒的超时以及消息。
		devices = append(devices, &drahealthv1.DeviceHealth{
			Device: &drahealthv1.DeviceIdentifier{
				PoolName:   d.PoolName,
				DeviceName: d.DeviceName,
			},
			Health:                    healthStatusToV1(d.Health),
			LastUpdatedTime:           lastUpdatedToUnix(d.LastUpdated),
			HealthCheckTimeoutSeconds: int64(d.HealthCheckTimeout / time.Second),
			Message:                   d.Message,
		})
	}
	// 封装为 v1 响应。
	return &drahealthv1.NodeWatchResourcesResponse{Devices: devices}
}

// lastUpdatedToUnix 把时间转换为 Unix 秒；零值时间返回 0，表示时间未知。
func lastUpdatedToUnix(t time.Time) int64 {
	// 零值表示未知，在线路上用 0 表示。
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// healthStatusToV1 把版本无关的健康状态映射为 v1 枚举；未知状态一律映射为 UNKNOWN。
func healthStatusToV1(s HealthStatus) drahealthv1.HealthStatus {
	// 仅“健康”和“不健康”有对应枚举，其余按未知处理。
	switch s {
	case HealthStatusHealthy:
		return drahealthv1.HealthStatus_HEALTHY
	case HealthStatusUnhealthy:
		return drahealthv1.HealthStatus_UNHEALTHY
	default:
		return drahealthv1.HealthStatus_UNKNOWN
	}
}
