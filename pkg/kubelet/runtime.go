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

package kubelet

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// runtimeState 保存 kubelet 对容器运行时、网络与存储状态的观测结果，
// 供节点状态上报与健康检查使用；并发访问由内嵌 RWMutex 保护。
type runtimeState struct {
	sync.RWMutex
	lastBaseRuntimeSync      time.Time
	baseRuntimeSyncThreshold time.Duration
	networkError             error
	runtimeError             error
	storageError             error
	cidr                     string
	healthChecks             []*healthCheck
	rtHandlers               []kubecontainer.RuntimeHandler
	rtFeatures               *kubecontainer.RuntimeFeatures
}

// healthCheckFnType 是运行时健康检查函数类型。
// 检查必须轻量且不能依赖外部组件（如容器运行时），否则检查本身会阻塞或形成递归依赖。
type healthCheckFnType func() (bool, error)

// healthCheck 把名称与检查函数绑定，便于在错误信息中定位失败的检查项。
type healthCheck struct {
	name string
	fn   healthCheckFnType
}

// addHealthCheck 注册一个命名健康检查。
func (s *runtimeState) addHealthCheck(name string, f healthCheckFnType) {
	s.Lock()
	defer s.Unlock()
	s.healthChecks = append(s.healthChecks, &healthCheck{name: name, fn: f})
}

// setRuntimeSync 记录最近一次基础运行时同步完成的时间，用于判断运行时是否存活。
func (s *runtimeState) setRuntimeSync(t time.Time) {
	s.Lock()
	defer s.Unlock()
	s.lastBaseRuntimeSync = t
}

// setNetworkState 更新网络错误状态。
func (s *runtimeState) setNetworkState(err error) {
	s.Lock()
	defer s.Unlock()
	s.networkError = err
}

// setRuntimeState 更新运行时错误状态。
func (s *runtimeState) setRuntimeState(err error) {
	s.Lock()
	defer s.Unlock()
	s.runtimeError = err
}

// setRuntimeHandlers 保存运行时 handler 列表。
func (s *runtimeState) setRuntimeHandlers(rtHandlers []kubecontainer.RuntimeHandler) {
	s.Lock()
	defer s.Unlock()
	// 复制并排序，保证 handler 顺序确定，避免顺序抖动导致 Node 状态被无谓更新。
	s.rtHandlers = append([]kubecontainer.RuntimeHandler(nil), rtHandlers...)
	sort.Slice(s.rtHandlers, func(i, j int) bool {
		return s.rtHandlers[i].Name < s.rtHandlers[j].Name
	})
}

// runtimeHandlers 返回当前 handler 列表（只读锁保护）。
func (s *runtimeState) runtimeHandlers() []kubecontainer.RuntimeHandler {
	s.RLock()
	defer s.RUnlock()
	return s.rtHandlers
}

// setRuntimeFeatures 更新运行时能力特性。
func (s *runtimeState) setRuntimeFeatures(features *kubecontainer.RuntimeFeatures) {
	s.Lock()
	defer s.Unlock()
	s.rtFeatures = features
}

// runtimeFeatures 返回运行时能力特性。
func (s *runtimeState) runtimeFeatures() *kubecontainer.RuntimeFeatures {
	s.RLock()
	defer s.RUnlock()
	return s.rtFeatures
}

// setStorageState 更新存储错误状态。
func (s *runtimeState) setStorageState(err error) {
	s.Lock()
	defer s.Unlock()
	s.storageError = err
}

// setPodCIDR 记录分配给节点的 Pod CIDR。
func (s *runtimeState) setPodCIDR(cidr string) {
	s.Lock()
	defer s.Unlock()
	s.cidr = cidr
}

// podCIDR 返回当前 Pod CIDR。
func (s *runtimeState) podCIDR() string {
	s.RLock()
	defer s.RUnlock()
	return s.cidr
}

// runtimeErrors 汇总运行时相关错误：基础同步超时、健康检查失败以及显式运行时错误。
func (s *runtimeState) runtimeErrors() error {
	s.RLock()
	defer s.RUnlock()
	errs := []error{}
	// 从未同步过说明启动尚未完成，此时不能直接判定运行时宕机。
	if s.lastBaseRuntimeSync.IsZero() {
		errs = append(errs, errors.New("container runtime status check may not have completed yet"))
	} else if !s.lastBaseRuntimeSync.Add(s.baseRuntimeSyncThreshold).After(time.Now()) {
		errs = append(errs, errors.New("container runtime is down"))
	}
	// 逐个执行轻量健康检查，任何一项失败都计入错误。
	for _, hc := range s.healthChecks {
		if ok, err := hc.fn(); !ok {
			errs = append(errs, fmt.Errorf("%s is not healthy: %v", hc.name, err))
		}
	}
	if s.runtimeError != nil {
		errs = append(errs, s.runtimeError)
	}

	return utilerrors.NewAggregate(errs)
}

// networkErrors 汇总网络错误。
func (s *runtimeState) networkErrors() error {
	s.RLock()
	defer s.RUnlock()
	errs := []error{}
	if s.networkError != nil {
		errs = append(errs, s.networkError)
	}
	return utilerrors.NewAggregate(errs)
}

// storageErrors 汇总存储错误。
func (s *runtimeState) storageErrors() error {
	s.RLock()
	defer s.RUnlock()
	errs := []error{}
	if s.storageError != nil {
		errs = append(errs, s.storageError)
	}
	return utilerrors.NewAggregate(errs)
}

// newRuntimeState 构造运行时状态。
// 网络错误初始为 ErrNetworkUnknown，避免把"尚未探测"误报为健康。
func newRuntimeState(runtimeSyncThreshold time.Duration) *runtimeState {
	return &runtimeState{
		lastBaseRuntimeSync:      time.Time{},
		baseRuntimeSyncThreshold: runtimeSyncThreshold,
		networkError:             ErrNetworkUnknown,
	}
}
