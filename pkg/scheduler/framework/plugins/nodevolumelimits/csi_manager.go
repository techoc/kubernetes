/*
Copyright 2025 The Kubernetes Authors.

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

package nodevolumelimits

import (
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// DefaultCSIManager 是 fwk.CSIManager 接口的默认实现。
// 它把 client-go 的 CSINodeLister 包装成框架侧接口，隔离存储 API 的细节。
type DefaultCSIManager struct {
	csiNodeLister *csiNodeListerWrapper
}

var _ fwk.CSIManager = &DefaultCSIManager{}

// NewCSIManager 基于 client-go 的 CSINodeLister 创建 DefaultCSIManager。
func NewCSIManager(csiNodeLister storagelisters.CSINodeLister) *DefaultCSIManager {
	return &DefaultCSIManager{csiNodeLister: NewCSINodeLister(csiNodeLister)}
}

// CSINodes 返回包装后的 CSINodeLister，供框架查询节点上的 CSI 驱动信息。
func (m *DefaultCSIManager) CSINodes() fwk.CSINodeLister {
	return m.csiNodeLister
}

// csiNodeListerWrapper 把 client-go 的 CSINodeLister 适配为 fwk.CSINodeLister。
type csiNodeListerWrapper struct {
	csiNodeLister storagelisters.CSINodeLister
}

var _ fwk.CSINodeLister = &csiNodeListerWrapper{}

// NewCSINodeLister 创建一个 csiNodeListerWrapper。
func NewCSINodeLister(csiNodeLister storagelisters.CSINodeLister) *csiNodeListerWrapper {
	return &csiNodeListerWrapper{csiNodeLister: csiNodeLister}
}

// List 返回所有 CSINode。框架关心全部节点，所以用 Everything 不过滤标签。
func (l *csiNodeListerWrapper) List() ([]*storagev1.CSINode, error) {
	return l.csiNodeLister.List(labels.Everything())
}

// Get 按名称返回单个 CSINode。
func (l *csiNodeListerWrapper) Get(name string) (*storagev1.CSINode, error) {
	return l.csiNodeLister.Get(name)
}
