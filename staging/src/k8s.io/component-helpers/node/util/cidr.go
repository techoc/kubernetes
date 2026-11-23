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

// Package util 提供节点相关的通用辅助函数。
//
// 这些函数被多个节点组件（如 kubelet、cloud-controller-manager）复用，
// 避免各自重复实现节点 CIDR 与状态条件的读写细节。
package util

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// 用最小化的结构体而不是完整 Node 对象来构造 patch：
// 只声明要修改的字段，避免把本地读到的旧值（如 resourceVersion）一并写回去。
// Spec 是补丁的顶层字段，CIDR 改动都挂在 spec 下。
type nodeForCIDRMergePatch struct {
	Spec nodeSpecForMergePatch `json:"spec"`
}

// PodCIDR 是旧的单段 CIDR 字段，PodCIDRs 是支持多段后的新字段。
// 两者必须同时设置，以兼容只读取旧字段的组件与读取新字段的组件。
// PodCIDR 取第一段 CIDR，供只认识旧字段的组件使用；
// PodCIDRs 是完整的 CIDR 列表，为空时省略，避免写出无意义的字段。
type nodeSpecForMergePatch struct {
	PodCIDR  string   `json:"podCIDR"`
	PodCIDRs []string `json:"podCIDRs,omitempty"`
}

// PatchNodeCIDRs 把节点的 PodCIDR 设为 cidrs[0]，并把 PodCIDRs 整体设为给定的列表。
//
// 用 StrategicMergePatch 同时更新新旧两个 CIDR 字段，而不是先 Get 再 Update：
// 后者需要完整对象且容易与其他组件的状态更新产生冲突。
// 调用方需保证 cidrs 非空，因为第一段 CIDR 还要写入兼容用的 PodCIDR 字段。
func PatchNodeCIDRs(ctx context.Context, c clientset.Interface, node types.NodeName, cidrs []string) error {
	// 同时写入新的 CIDR 列表和兼容用的旧 CIDR 字段。
	patch := nodeForCIDRMergePatch{
		Spec: nodeSpecForMergePatch{
			PodCIDR:  cidrs[0],
			PodCIDRs: cidrs,
		},
	}

	patchBytes, err := json.Marshal(&patch)
	if err != nil {
		return fmt.Errorf("failed to json.Marshal CIDR: %v", err)
	}
	// patch 内容只在高日志等级记录，避免正常运行时刷屏；排查 CIDR 分配问题时再打开。
	klog.FromContext(ctx).V(4).Info("cidrs patch bytes", "patchBytes", string(patchBytes))
	if _, err := c.CoreV1().Nodes().Patch(ctx, string(node), types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("failed to patch node CIDR: %v", err)
	}
	return nil
}
