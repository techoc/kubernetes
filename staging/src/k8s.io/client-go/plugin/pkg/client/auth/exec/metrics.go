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

// The following constants shadow the special values used in the prometheus metrics implementation.
const (
	// noError indicates that the plugin process was successfully started and exited with an exit
	// code of 0.
	noError = "no_error"
	// pluginExecutionError indicates that the plugin process was successfully started and then
	// it returned a non-zero exit code.
	pluginExecutionError = "plugin_execution_error"
	// pluginNotFoundError indicates that we could not find the exec plugin.
	pluginNotFoundError = "plugin_not_found_error"
	// clientInternalError indicates that we attempted to start the plugin process, but failed
	// for some reason.
	clientInternalError = "client_internal_error"

	// successExitCode represents an exec plugin invocation that was successful.
	successExitCode = 0
	// failureExitCode represents an exec plugin invocation that was not successful. This code is
	// used in some failure modes (e.g., plugin not found, client internal error) so that someone
	// can more easily monitor all unsuccessful invocations.
	failureExitCode = 1

	// pluginAllowed represents an exec plugin invocation that was allowed by
	// the plugin policy and/or the allowlist
	pluginAllowed = "allowed"
	// pluginAllowed represents an exec plugin invocation that was denied by
	// the plugin policy and/or the allowlist
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

// set stores the given expiration time and updates the updates the certificate
// expiry metric to the earliest expiration time.
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

// incrementCallsMetric increments a global metrics counter for the number of calls to an exec
// plugin, partitioned by exit code. The provided err should be the return value from
// exec.Cmd.Run().
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
