//go:build !linux

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

package fsquota

import (
	"errors"

	"k8s.io/kubernetes/pkg/volume/util/fsquota/common"
	"k8s.io/mount-utils"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

// 为不支持卷配额的系统提供的空实现。

var errNotImplemented = errors.New("not implemented")

// GetQuotaOnDir 返回无效配额 ID，表示该系统不支持配额查询。
func GetQuotaOnDir(_ mount.Interface, _ string) (common.QuotaID, error) {
	return common.BadQuotaID, errNotImplemented
}

// SupportsQuotas 为不支持配额的系统提供的空实现。
func SupportsQuotas(_ mount.Interface, _ string, _ bool) (bool, error) {
	return false, errNotImplemented
}

// AssignQuota 为不支持配额的系统提供的空实现。
func AssignQuota(_ mount.Interface, _ string, _ types.UID, _ *resource.Quantity, _ bool) error {
	return errNotImplemented
}

// GetConsumption 为不支持配额的系统提供的空实现。
func GetConsumption(_ string) (*resource.Quantity, error) {
	return nil, errNotImplemented
}

// GetInodes 为不支持配额的系统提供的空实现。
func GetInodes(_ string) (*resource.Quantity, error) {
	return nil, errNotImplemented
}

// ClearQuota 为不支持配额的系统提供的空实现。
func ClearQuota(_ mount.Interface, _ string, _ bool) error {
	return errNotImplemented
}
