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

package audit

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/errors"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
)

// Union returns an audit Backend which logs events to a set of backends. The returned
// Sink implementation blocks in turn for each call to ProcessEvents.
//
// 单个后端时直接返回原对象，省掉一层无谓的包装与分配。
func Union(backends ...Backend) Backend {
	if len(backends) == 1 {
		return backends[0]
	}
	return union{backends}
}

// union 把多个审计后端聚合成一个；事件按顺序扇出。
type union struct {
	backends []Backend
}

// ProcessEvents 顺序投递事件；注意使用 success && 短路避免覆盖先前的失败，
// 即只要有一个后端拒绝接收，整体就返回 false 触发重试。
func (u union) ProcessEvents(events ...*auditinternal.Event) bool {
	success := true
	for _, backend := range u.backends {
		success = backend.ProcessEvents(events...) && success
	}
	return success
}

// Run 并发启动所有后端，聚合返回各自的错误，避免一个慢后端阻塞其他后端启动。
func (u union) Run(stopCh <-chan struct{}) error {
	var funcs []func() error
	for _, backend := range u.backends {
		funcs = append(funcs, func() error {
			return backend.Run(stopCh)
		})
	}
	return errors.AggregateGoroutines(funcs...)
}

// Shutdown 顺序关闭各后端；聚合体自身无额外资源。
func (u union) Shutdown() {
	for _, backend := range u.backends {
		backend.Shutdown()
	}
}

// String 生成 "union[a,b]" 形式的名字，用于日志与指标区分后端组合。
func (u union) String() string {
	var backendStrings []string
	for _, backend := range u.backends {
		backendStrings = append(backendStrings, fmt.Sprintf("%s", backend))
	}
	return fmt.Sprintf("union[%s]", strings.Join(backendStrings, ","))
}
