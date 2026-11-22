/*
Copyright 2016 The Kubernetes Authors.

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

package server

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/utils/clock"
)

// healthMux is an interface describing the methods InstallHandler requires.
// 抽象成接口是为了让测试可以注入假 mux，而不必启动完整 HTTP 服务器。
type healthMux interface {
	Handle(pattern string, handler http.Handler)
}

// healthCheckRegistry 收集某个健康端点（healthz/livez/readyz）的检查项，
// 并在处理器安装后冻结集合——之后新增检查会被拒绝，避免运行期行为漂移。
type healthCheckRegistry struct {
	path            string
	lock            sync.Mutex
	checks          []healthz.HealthChecker
	checksInstalled bool
	clock           clock.Clock
}

// addHealthChecks 以零宽限期注册检查项。
func (reg *healthCheckRegistry) addHealthChecks(checks ...healthz.HealthChecker) error {
	return reg.addDelayedHealthChecks(0, checks...)
}

// addDelayedHealthChecks 注册检查项；delay > 0 时用 delayedHealthCheck 包装，
// 使检查在启动后的宽限期内始终返回健康。
func (reg *healthCheckRegistry) addDelayedHealthChecks(delay time.Duration, checks ...healthz.HealthChecker) error {
	// 延迟包装依赖时钟；未配置时钟时无法计算到期时间，属于编程错误。
	if delay > 0 && reg.clock == nil {
		return fmt.Errorf("nil clock in healthCheckRegistry for %s endpoint", reg.path)
	}
	reg.lock.Lock()
	defer reg.lock.Unlock()
	// 处理器一旦安装，检查集合即固定；再添加会改变已对外暴露的健康语义。
	if reg.checksInstalled {
		return fmt.Errorf("unable to add because the %s endpoint has already been created", reg.path)
	}
	if delay > 0 {
		for _, check := range checks {
			reg.checks = append(reg.checks, delayedHealthCheck(check, reg.clock, delay))
		}
	} else {
		reg.checks = append(reg.checks, checks...)
	}
	return nil
}

// installHandler 安装处理器，不触发“首次健康”回调。
func (reg *healthCheckRegistry) installHandler(mux healthMux) {
	reg.installHandlerWithHealthyFunc(mux, nil)
}

// installHandlerWithHealthyFunc 安装处理器并标记集合已冻结；
// firstTimeHealthy 只会在首次全部检查通过时调用一次（readyz 用它发出就绪信号）。
func (reg *healthCheckRegistry) installHandlerWithHealthyFunc(mux healthMux, firstTimeHealthy func()) {
	reg.lock.Lock()
	defer reg.lock.Unlock()
	reg.checksInstalled = true
	healthz.InstallPathHandlerWithHealthyFunc(mux, reg.path, firstTimeHealthy, reg.checks...)
}

// AddHealthChecks adds HealthCheck(s) to health endpoints (healthz, livez, readyz) but
// configures the liveness grace period to be zero, which means we expect this health check
// to immediately indicate that the apiserver is unhealthy.
// 适用于通用健康检查（如存储可用性），它们与启动序列无关，无需宽限期。
func (s *GenericAPIServer) AddHealthChecks(checks ...healthz.HealthChecker) error {
	// we opt for a delay of zero here, because this entrypoint adds generic health checks
	// and not health checks which are specifically related to kube-apiserver boot-sequences.
	return s.addHealthChecks(0, checks...)
}

// AddBootSequenceHealthChecks adds health checks to the old healthz endpoint (for backwards compatibility reasons)
// as well as livez and readyz. The livez grace period is defined by the value of the
// command-line flag --livez-grace-period; before the grace period elapses, the livez health checks
// will default to healthy. One may want to set a grace period in order to prevent the kubelet from restarting
// the kube-apiserver due to long-ish boot sequences. Readyz health checks, on the other hand, have no grace period,
// since readyz should fail until boot fully completes.
// 宽限期只作用于 livez：启动慢不应导致 kubelet 重启进程，但 readyz 必须诚实反映未就绪。
func (s *GenericAPIServer) AddBootSequenceHealthChecks(checks ...healthz.HealthChecker) error {
	return s.addHealthChecks(s.livezGracePeriod, checks...)
}

// addHealthChecks adds health checks to healthz, livez, and readyz. The delay passed in will set
// a corresponding grace period on livez.
// 同一个检查被注册到三个端点；只有 livez 使用延迟包装，healthz 与 readyz 立即生效。
func (s *GenericAPIServer) addHealthChecks(livezGracePeriod time.Duration, checks ...healthz.HealthChecker) error {
	if err := s.healthzRegistry.addHealthChecks(checks...); err != nil {
		return err
	}
	if err := s.livezRegistry.addDelayedHealthChecks(livezGracePeriod, checks...); err != nil {
		return err
	}
	return s.readyzRegistry.addHealthChecks(checks...)
}

// AddReadyzChecks allows you to add a HealthCheck to readyz.
func (s *GenericAPIServer) AddReadyzChecks(checks ...healthz.HealthChecker) error {
	return s.readyzRegistry.addHealthChecks(checks...)
}

// AddLivezChecks allows you to add a HealthCheck to livez.
// delay 为该检查的宽限期，期间 livez 会忽略其失败。
func (s *GenericAPIServer) AddLivezChecks(delay time.Duration, checks ...healthz.HealthChecker) error {
	return s.livezRegistry.addDelayedHealthChecks(delay, checks...)
}

// addReadyzShutdownCheck is a convenience function for adding a readyz shutdown check, so
// that we can register that the api-server is no longer ready while we attempt to gracefully
// shutdown.
func (s *GenericAPIServer) addReadyzShutdownCheck(stopCh <-chan struct{}) error {
	return s.AddReadyzChecks(healthz.NewShutdownHealthz(stopCh))
}

// installHealthz creates the healthz endpoint for this server
func (s *GenericAPIServer) installHealthz() {
	s.healthzRegistry.installHandler(s.Handler.NonGoRestfulMux)
}

// installReadyz creates the readyz endpoint for this server.
// 首次全部就绪时发出 HasBeenReady 信号，用于驱动依赖“服务已就绪”的启动逻辑。
func (s *GenericAPIServer) installReadyz() {
	s.readyzRegistry.installHandlerWithHealthyFunc(s.Handler.NonGoRestfulMux, func() {
		// note: installHandlerWithHealthyFunc guarantees that this is called only once
		s.lifecycleSignals.HasBeenReady.Signal()
	})
}

// installLivez creates the livez endpoint for this server.
func (s *GenericAPIServer) installLivez() {
	s.livezRegistry.installHandler(s.Handler.NonGoRestfulMux)
}

// delayedHealthCheck wraps a health check which will not fail until the explicitly defined delay has elapsed. This
// is intended for use primarily for livez health checks.
// 到期时间在包装时计算一次，因此宽限期从注册时刻起算，而非从第一次探测起算。
func delayedHealthCheck(check healthz.HealthChecker, clock clock.Clock, delay time.Duration) healthz.HealthChecker {
	return delayedLivezCheck{
		check,
		clock.Now().Add(delay),
		clock,
	}
}

// delayedLivezCheck 在宽限期内始终报告健康，之后委托给底层检查。
type delayedLivezCheck struct {
	check      healthz.HealthChecker
	startCheck time.Time
	clock      clock.Clock
}

// Name 透传底层检查名，保证端点上显示的名称与注册时一致。
func (c delayedLivezCheck) Name() string {
	return c.check.Name()
}

// Check 在宽限期结束前直接返回 nil，避免启动耗时导致 liveness 误判并重启进程。
func (c delayedLivezCheck) Check(req *http.Request) error {
	if c.clock.Now().After(c.startCheck) {
		return c.check.Check(req)
	}
	return nil
}
