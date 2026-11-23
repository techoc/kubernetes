/*
Copyright 2021 The Kubernetes Authors.

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

package util

import (
	"context"
	"encoding/json"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
)

// GetNodeCondition 从给定的节点状态中取出指定类型的条件及其在切片中的索引。
// 条件不存在时返回索引 -1 和 nil。
//
// 同时返回索引是为了让调用方能就地修改或替换切片中的条件；
// 返回指向切片元素的指针而不是副本，修改会直接反映到传入的 status 上。
func GetNodeCondition(status *v1.NodeStatus, conditionType v1.NodeConditionType) (int, *v1.NodeCondition) {
	// nil status 视为“没有条件”，让调用方无需先判空。
	if status == nil {
		return -1, nil
	}
	// 按类型逐个匹配；Node 不允许同类型条件重复，因此第一个匹配项即为结果。
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	// 遍历完仍未找到，按约定返回 -1 与 nil。
	return -1, nil
}

// SetNodeCondition 通过 patch 操作更新节点上指定类型的条件。
//
// 只 patch status 下的一条条件：Node 的状态由多个组件共同维护，
// 整对象 Update 会覆盖别人的改动或引发冲突。
func SetNodeCondition(c clientset.Interface, node types.NodeName, condition v1.NodeCondition) error {
	// 心跳时间必须在本地生成：状态更新方才知道自己“此刻”仍然存活，
	// 由服务端填充会让其他组件无法判断该条件是否仍在刷新。
	condition.LastHeartbeatTime = metav1.NewTime(time.Now())
	// 用 map 拼出最小 patch，只携带目标条件；serverside 会按 conditions 的
	// type 合并，未提及的其他条件保持不变。
	patch, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []v1.NodeCondition{condition},
		},
	})
	if err != nil {
		return err
	}
	// 只提交条件补丁，apiserver 会按 type 合并进现有 conditions 列表。
	_, err = c.CoreV1().Nodes().PatchStatus(context.TODO(), string(node), patch)
	return err
}
