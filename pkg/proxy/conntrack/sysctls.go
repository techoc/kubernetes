//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package conntrack

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"k8s.io/component-helpers/node/util/sysctl"
	"k8s.io/klog/v2"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/utils/cpuset"
)

// SetSysctls 按 kube-proxy 的配置设置 conntrack 相关的内核 sysctl。
// 这是供 kube-proxy 启动流程调用的入口，内部使用真实的 sysctl 读写实现。
func SetSysctls(ctx context.Context, config *kubeproxyconfig.KubeProxyConntrackConfiguration) error {
	return setSysctls(ctx, realConntrackConfigurer{sys: sysctl.New()}, config)
}

// conntrackConfigurer 是用于设置 conntrack sysctl 的可 mock 接口。
//
// 各 sysctl 字段的说明可参考：
// https://www.kernel.org/doc/Documentation/networking/nf_conntrack-sysctl.txt
type conntrackConfigurer interface {
	// GetMax 获取 nf_conntrack_max 的当前值。
	GetMax(ctx context.Context) (int, error)
	// SetMax 调整 nf_conntrack_max。
	SetMax(ctx context.Context, max int) error
	// SetTCPEstablishedTimeout 调整 nf_conntrack_tcp_timeout_established。
	SetTCPEstablishedTimeout(ctx context.Context, seconds int) error
	// SetTCPCloseWaitTimeout 调整 nf_conntrack_tcp_timeout_close_wait。
	SetTCPCloseWaitTimeout(ctx context.Context, seconds int) error
	// SetTCPBeLiberal 调整 nf_conntrack_tcp_be_liberal。
	SetTCPBeLiberal(ctx context.Context, value int) error
	// SetUDPTimeout 调整 nf_conntrack_udp_timeout。
	SetUDPTimeout(ctx context.Context, seconds int) error
	// SetUDPStreamTimeout 调整 nf_conntrack_udp_timeout_stream。
	SetUDPStreamTimeout(ctx context.Context, seconds int) error

	// GetHashsize 获取 conntrack 模块的 "hashsize" 参数
	GetHashsize(ctx context.Context) (int, error)
	// SetHashsize 设置 conntrack 模块的 "hashsize" 参数
	SetHashsize(ctx context.Context, value int) error

	// DetectNumCPU 返回系统中 CPU 核心的数量
	DetectNumCPU() int
}

// setSysctls 通过给定的 configurer 应用 conntrack 配置。
// 对 conntrack_max 只在当前值偏小时才上调，避免覆盖管理员设置的更大值；
// 各项超时与开关仅在配置显式给出有效值时才下发。
func setSysctls(ctx context.Context, ct conntrackConfigurer, config *kubeproxyconfig.KubeProxyConntrackConfiguration) error {
	max, err := getConntrackMax(ctx, config, ct.DetectNumCPU())
	if err != nil {
		return err
	}
	// max <= 0 表示配置未启用 conntrack 表大小调整，此时不改动相关参数。
	if max > 0 {
		if curMax, err := ct.GetMax(ctx); err != nil || curMax < max {
			err := ct.SetMax(ctx, max)
			if err != nil {
				return err
			}
		}

		// hashsize 太小会导致 conntrack 哈希表冲突加剧、查找变慢；
		// 按 max/4 的比例扩容，保证表项容量足够。
		hashsize, err := ct.GetHashsize(ctx)
		if err != nil {
			return err
		}
		if hashsize < max/4 {
			err = ct.SetHashsize(ctx, max/4)
			if err != nil {
				return err
			}
		}
	}

	if config.TCPEstablishedTimeout != nil && config.TCPEstablishedTimeout.Duration > 0 {
		timeout := int(config.TCPEstablishedTimeout.Duration / time.Second)
		if err := ct.SetTCPEstablishedTimeout(ctx, timeout); err != nil {
			return err
		}
	}

	if config.TCPCloseWaitTimeout != nil && config.TCPCloseWaitTimeout.Duration > 0 {
		timeout := int(config.TCPCloseWaitTimeout.Duration / time.Second)
		if err := ct.SetTCPCloseWaitTimeout(ctx, timeout); err != nil {
			return err
		}
	}

	if config.TCPBeLiberal {
		if err := ct.SetTCPBeLiberal(ctx, 1); err != nil {
			return err
		}
	}

	if config.UDPTimeout.Duration > 0 {
		timeout := int(config.UDPTimeout.Duration / time.Second)
		if err := ct.SetUDPTimeout(ctx, timeout); err != nil {
			return err
		}
	}

	if config.UDPStreamTimeout.Duration > 0 {
		timeout := int(config.UDPStreamTimeout.Duration / time.Second)
		if err := ct.SetUDPStreamTimeout(ctx, timeout); err != nil {
			return err
		}
	}

	return nil
}

// getConntrackMax 根据每核上限、CPU 核数与下限计算 conntrack_max。
// 计算出的值设有硬上限，防止高核数机器上 conntrack 表占用过多内存；
// 未配置每核上限时返回 0，表示不做调整。
func getConntrackMax(ctx context.Context, config *kubeproxyconfig.KubeProxyConntrackConfiguration, numCPU int) (int, error) {
	logger := klog.FromContext(ctx)
	if config.MaxPerCore != nil && *config.MaxPerCore > 0 {
		floor := 0
		if config.Min != nil {
			floor = int(*config.Min)
		}
		scaled := int(*config.MaxPerCore) * numCPU
		// 将上限封顶在 1M，避免高核数机器上按核数放大后占用过多内存。
		const maxLimit = 1048576
		if scaled > maxLimit {
			logger.V(3).Info("GetConntrackMax: capping scaled conntrack-max-per-core", "scaled", scaled, "limit", maxLimit)
			return maxLimit, nil
		}
		if scaled > floor {
			logger.V(3).Info("GetConntrackMax: using scaled conntrack-max-per-core")
			return scaled, nil
		}
		logger.V(3).Info("GetConntrackMax: using conntrack-min")
		return floor, nil
	}
	return 0, nil
}

// realConntrackConfigurer 使用真实的 sysctl 接口与内核交互。
type realConntrackConfigurer struct {
	sys sysctl.Interface
}

// DetectNumCPU 返回用于确定 nf_conntrack_max 大小的 CPU 数量。该限制是
// 主机级别的，因此必须基于节点的 CPU 核数，而不是 runtime.NumCPU()：
// runtime.NumCPU() 会遵循进程的 cpuset，当 kube-proxy 运行在静态 CPU 策略下时
// 会低估核数。cpuset.NumCPU() 改为从 sysfs 读取节点在线 CPU 的数量，
// 读取失败时回退到 runtime.NumCPU()。
func (rct realConntrackConfigurer) DetectNumCPU() int {
	if n, err := cpuset.NumCPU(); err == nil && n > 0 {
		return n
	}
	return runtime.NumCPU()
}

// GetMax 从 sysctl 读取当前 nf_conntrack_max 值。
func (rct realConntrackConfigurer) GetMax(_ context.Context) (int, error) {
	return rct.sys.GetSysctl("net/netfilter/nf_conntrack_max")
}

// SetMax 设置 nf_conntrack_max。
func (rct realConntrackConfigurer) SetMax(ctx context.Context, max int) error {
	return rct.setIntSysCtl(ctx, "nf_conntrack_max", max)
}

// SetTCPEstablishedTimeout 设置已建立 TCP 连接的 conntrack 超时时间。
func (rct realConntrackConfigurer) SetTCPEstablishedTimeout(ctx context.Context, seconds int) error {
	return rct.setIntSysCtl(ctx, "nf_conntrack_tcp_timeout_established", seconds)
}

// SetTCPCloseWaitTimeout 设置处于 CLOSE_WAIT 状态的 TCP 连接的 conntrack 超时时间。
func (rct realConntrackConfigurer) SetTCPCloseWaitTimeout(ctx context.Context, seconds int) error {
	return rct.setIntSysCtl(ctx, "nf_conntrack_tcp_timeout_close_wait", seconds)
}

// SetTCPBeLiberal 设置 nf_conntrack_tcp_be_liberal 开关。
func (rct realConntrackConfigurer) SetTCPBeLiberal(ctx context.Context, value int) error {
	return rct.setIntSysCtl(ctx, "nf_conntrack_tcp_be_liberal", value)
}

// SetUDPTimeout 设置 UDP 单播连接的 conntrack 超时时间。
func (rct realConntrackConfigurer) SetUDPTimeout(ctx context.Context, seconds int) error {
	return rct.setIntSysCtl(ctx, "nf_conntrack_udp_timeout", seconds)
}

// SetUDPStreamTimeout 设置 UDP 多播/广播等流式连接的 conntrack 超时时间。
func (rct realConntrackConfigurer) SetUDPStreamTimeout(ctx context.Context, seconds int) error {
	return rct.setIntSysCtl(ctx, "nf_conntrack_udp_timeout_stream", seconds)
}

// setIntSysCtl 在值不同时才写入 sysctl，避免无谓的写操作；
// 读取失败时 val 为 0，仍按"值不同"处理，从而确保配置能被应用。
func (rct realConntrackConfigurer) setIntSysCtl(ctx context.Context, name string, value int) error {
	logger := klog.FromContext(ctx)
	entry := "net/netfilter/" + name

	if val, _ := rct.sys.GetSysctl(entry); val != value {
		logger.Info("Set sysctl", "entry", entry, "value", value)
		if err := rct.sys.SetSysctl(entry, value); err != nil {
			return err
		}
	}
	return nil
}

// GetHashsize 从 sysfs 读取 conntrack 模块的 hashsize 参数。
func (rct realConntrackConfigurer) GetHashsize(_ context.Context) (int, error) {
	b, err := os.ReadFile("/sys/module/nf_conntrack/parameters/hashsize")
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

// SetHashsize 通过 sysfs 写入 conntrack 模块的 hashsize 参数。
func (rct realConntrackConfigurer) SetHashsize(ctx context.Context, value int) error {
	klog.FromContext(ctx).Info("Setting conntrack hashsize", "conntrackHashsize", value)
	return os.WriteFile("/sys/module/nf_conntrack/parameters/hashsize", []byte(strconv.Itoa(value)), 0640)
}
