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

package exec

import (
	"errors"
	"io/fs"
	"os/exec"
	"reflect"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/client-go/tools/metrics"
)

// 以下常量对应 Prometheus 指标实现中使用的特殊取值。
const (
	// noError 表示插件进程成功启动并以退出码 0 结束。
	noError = "no_error"
	// pluginExecutionError 表示插件进程成功启动但返回了非零退出码。
	pluginExecutionError = "plugin_execution_error"
	// pluginNotFoundError 表示找不到 exec 插件。
	pluginNotFoundError = "plugin_not_found_error"
	// clientInternalError 表示尝试启动插件进程但失败。
	clientInternalError = "client_internal_error"

	// successExitCode 表示插件调用成功。
	successExitCode = 0
	// failureExitCode 表示插件调用失败。插件未找到、客户端内部错误等场景都用它，
	// 便于统一监控所有失败的调用。
	failureExitCode = 1

	// pluginAllowed 表示插件调用被插件策略或 allowlist 允许。
	pluginAllowed = "allowed"
	// pluginDenied 表示插件调用被插件策略或 allowlist 拒绝。
	pluginDenied = "denied"
)

// certificateExpirationTracker 跟踪每个 exec 认证器提供的客户端证书过期时间。
//
// 指标只暴露全局最早过期时间，这样监控告警不会被尚未使用的认证器干扰；
// 因此需要维护每个认证器的值并重新求最小值。
type certificateExpirationTracker struct {
	mu        sync.RWMutex
	m         map[*Authenticator]time.Time
	metricSet func(*time.Time)
}

// expirationMetrics 是进程级的单例，直接写入 tools/metrics 注册的全局指标。
var expirationMetrics = &certificateExpirationTracker{
	m: map[*Authenticator]time.Time{},
	metricSet: func(e *time.Time) {
		metrics.ClientCertExpiry.Set(e)
	},
}

// set 记录该认证器的证书过期时间，并把指标更新为所有认证器中最早的过期时间。
//
// 零值表示该认证器尚未提供证书；若所有值都是零，指标设为 nil 表示未知。
func (c *certificateExpirationTracker) set(a *Authenticator, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[a] = t

	// 全量扫描而不是增量维护堆：认证器数量通常很少，简单实现不易出错。
	earliest := time.Time{}
	for _, t := range c.m {
		if t.IsZero() {
			continue
		}
		if earliest.IsZero() || earliest.After(t) {
			earliest = t
		}
	}
	if earliest.IsZero() {
		c.metricSet(nil)
	} else {
		c.metricSet(&earliest)
	}
}

// incrementCallsMetric 按退出码分类累加 exec 插件调用次数，err 应传入 exec.Cmd.Run() 的返回值。
//
// 退出码和错误类别分开统计：插件未找到与插件执行失败对运维的含义不同，
// 前者通常意味着环境配置问题，后者才是插件自身逻辑失败。
func incrementCallsMetric(err error) {
	execExitError := &exec.ExitError{}
	execError := &exec.Error{}
	pathError := &fs.PathError{}
	switch {
	case err == nil: // Binary execution succeeded.
		metrics.ExecPluginCalls.Increment(successExitCode, noError)

	case errors.As(err, &execExitError): // Binary execution failed (see "os/exec".Cmd.Run()).
		metrics.ExecPluginCalls.Increment(execExitError.ExitCode(), pluginExecutionError)

	case errors.As(err, &execError), errors.As(err, &pathError): // Binary does not exist (see exec.Error, fs.PathError).
		metrics.ExecPluginCalls.Increment(failureExitCode, pluginNotFoundError)

	default: // We don't know about this error type.
		klog.V(2).InfoS("unexpected exec plugin return error type", "type", reflect.TypeOf(err).String(), "err", err)
		metrics.ExecPluginCalls.Increment(failureExitCode, clientInternalError)
	}
}

// incrementPolicyMetric 记录一次 exec 插件策略检查的结果。
//
// err 非 nil 表示策略拒绝或 allowlist 不匹配，两者对调用方而言都是“被拒绝”。
func incrementPolicyMetric(err error) {
	if err != nil {
		metrics.ExecPluginPolicyCalls.Increment(pluginDenied)
		return
	}

	metrics.ExecPluginPolicyCalls.Increment(pluginAllowed)
}
