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

package factory

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// DestroyFunc 用于一并销毁 Create() 返回的存储所占用的资源。
// 调用方必须把 Create 返回的 DestroyFunc 与存储实例绑定，在关闭时成对释放，
// 否则会泄漏 etcd 客户端连接与后台 goroutine。
type DestroyFunc func()

// Create 根据给定配置创建存储后端。
// 当前仅支持 etcd3（以及未显式配置时默认 etcd3）。etcd2 被显式拒绝而
// 不是静默回退，是为了让仍在使用旧配置的部署尽早暴露问题。
func Create(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3Storage(c, newFunc, newListFunc, resourcePrefix)
	default:
		return nil, nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// CreateHealthCheck 根据给定配置创建健康检查函数。
// 返回的函数用于存活探针；它与 ready check 分开，是因为 etcd 短暂不可用时
// 可能仍应保持进程存活，避免重启放大故障。
func CreateHealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3HealthCheck(c, stopCh)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// CreateReadyCheck 创建就绪检查函数。stopCh 关闭后检查应尽快返回，
// 避免关停期间探针阻塞优雅退出。
func CreateReadyCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ReadyCheck(c, stopCh)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// CreateProber 返回可直接调用的 etcd 探针（Probe/Close），
// 适合需要自行控制探测节奏的调用方。
func CreateProber(c storagebackend.Config) (Prober, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// CreateMonitor 返回 etcd 指标采集器；与 CreateProber 共用同一实现，
// 因为两者都需要建立独立于主存储连接的短连接来做探测与采集。
func CreateMonitor(c storagebackend.Config) (metrics.Monitor, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// Prober 是定义 Probe 函数的接口，用于执行 etcd 的就绪性与存活检查。
// Close 用于释放探针持有的连接，必须与创建配对调用。
type Prober interface {
	Probe(ctx context.Context) error
	Close() error
}
