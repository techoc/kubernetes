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

// Package fsquota 提供文件系统配额的探测与查询能力。
package fsquota

import (
	"k8s.io/mount-utils"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume/util/fsquota/common"
)

// Interface 定义配额机制的抽象接口。
type Interface interface {
	// GetQuotaOnDir 获取作用于该目录的配额 ID（若有）。
	GetQuotaOnDir(m mount.Interface, path string) (common.QuotaID, error)

	// SupportsQuotas 判断给定路径是否支持配额。
	SupportsQuotas(m mount.Interface, path string) (bool, error)
	// AssignQuota 为路径分配一个由配额机制选定的配额并返回。
	AssignQuota(m mount.Interface, path string, poduid types.UID, bytes *resource.Quantity) error

	// GetConsumption 获取该路径基于配额的存储使用量。
	GetConsumption(path string) (*resource.Quantity, error)

	// GetInodes 获取该路径基于配额的 inode 使用量。
	GetInodes(path string) (*resource.Quantity, error)

	// ClearQuota 移除路径上的配额。
	// 实现可假定配额覆盖的数据已被删除。
	ClearQuota(m mount.Interface, path string) error
}

// enabledQuotasForMonitoring 判断用于监控的配额特性是否启用。
func enabledQuotasForMonitoring() bool {
	return utilfeature.DefaultFeatureGate.Enabled(features.LocalStorageCapacityIsolationFSQuotaMonitoring)
}
