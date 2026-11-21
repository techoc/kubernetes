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

package statusupdater

import (
	"fmt"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/types"
)

// NewFakeNodeStatusUpdater 构造一个可配置返回错误的假节点状态更新器，
// returnError 为 true 时所有更新调用都会失败，用于测试错误分支。
func NewFakeNodeStatusUpdater(returnError bool) NodeStatusUpdater {
	return &fakeNodeStatusUpdater{
		returnError: returnError,
	}
}

// fakeNodeStatusUpdater 不访问 API Server，仅按 returnError 决定成功或失败，
// 让调用方的错误处理逻辑可被独立验证。
type fakeNodeStatusUpdater struct {
	returnError bool // 是否让所有更新调用返回假错误
}

// UpdateNodeStatuses 模拟批量更新节点状态；按配置返回成功或假错误。
func (fnsu *fakeNodeStatusUpdater) UpdateNodeStatuses(logger klog.Logger) error {
	if fnsu.returnError {
		return fmt.Errorf("fake error on update node status")
	}

	return nil
}

// UpdateNodeStatusForNode 模拟更新单个节点状态；按配置返回成功或假错误。
// nodeName 在此被忽略，因为 fake 不维护任何节点状态。
func (fnsu *fakeNodeStatusUpdater) UpdateNodeStatusForNode(logger klog.Logger, nodeName types.NodeName) error {
	if fnsu.returnError {
		return fmt.Errorf("fake error on update node status")
	}

	return nil
}
