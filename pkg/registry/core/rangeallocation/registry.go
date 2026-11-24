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

package rangeallocation

import (
	api "k8s.io/kubernetes/pkg/apis/core"
)

// RangeRegistry 定义 RangeAllocation 快照存储契约；ClusterIP、NodePort 等分配器通过它读取最新位图并以资源版本冲突保护写入。
type RangeRegistry interface {
	// Get returns the latest allocation, an empty object if no allocation has been made,
	// or an error if the allocation could not be retrieved.
	Get() (*api.RangeAllocation, error)
	// CreateOrUpdate should create or update the provide allocation, unless a conflict
	// has occurred since the item was last created.
	CreateOrUpdate(*api.RangeAllocation) error
}
