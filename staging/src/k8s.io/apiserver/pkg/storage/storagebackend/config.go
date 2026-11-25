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

// Package storagebackend 定义存储后端的配置类型（连接、编码、加密、压缩等），
// 供 apiserver 在启动时构造 etcd3 存储客户端。
package storagebackend

import (
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/apiserver/pkg/storage/value"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
)

const (
	// StorageTypeUnset 表示未指定，等价于 etcd3。
	StorageTypeUnset = ""
	// StorageTypeETCD2 已废弃，仅为兼容旧配置保留字符串常量。
	StorageTypeETCD2 = "etcd2"
	// StorageTypeETCD3 是唯一受支持的存储后端。
	StorageTypeETCD3 = "etcd3"

	// DefaultCompactInterval 是默认的 etcd 压缩间隔；0 表示不压缩。
	DefaultCompactInterval = 5 * time.Minute
	// DefaultDBMetricPollInterval 是存储后端指标采集间隔。
	DefaultDBMetricPollInterval = 30 * time.Second
	// DefaultEventsHistoryWindow 是事件保留的最短历史窗口，须大于 etcd 压缩间隔，
	// 否则压缩可能删掉尚未被 watch 消费的事件。
	DefaultEventsHistoryWindow = 75 * time.Second
	// DefaultHealthcheckTimeout 是健康检查（livez）的存储探测超时。
	DefaultHealthcheckTimeout = 2 * time.Second
	// DefaultReadinessTimeout 是就绪检查（readyz）的存储探测超时。
	DefaultReadinessTimeout = 2 * time.Second
)

// TransportConfig holds all connection related info,  i.e. equal TransportConfig means equal servers we talk to.
//
// 可比较性是刻意设计的：上层以 TransportConfig 作为键来复用 etcd 客户端，
// 因此新增字段必须保持可比较（不要加入 slice/map/func 之外的不可比较类型）。
type TransportConfig struct {
	// ServerList is the list of storage servers to connect with.
	ServerList []string
	// TLS credentials
	KeyFile       string
	CertFile      string
	TrustedCAFile string
	// function to determine the egress dialer. (i.e. konnectivity server dialer)
	EgressLookup egressselector.Lookup
	// The TracerProvider can add tracing the connection
	TracerProvider oteltrace.TracerProvider
}

// Config 是创建存储后端所需的配置。
// 一个 Config 描述“同一组 etcd 服务器上的同一套编码/加密设置”，可被多个
// GroupResource 共享；按资源特化的部分见 ConfigForResource。
type Config struct {
	// Type defines the type of storage backend. Default ("") is "etcd3".
	Type string
	// Prefix is the prefix to all keys passed to storage.Interface methods.
	Prefix string
	// Transport holds all connection related info, i.e. equal TransportConfig means equal servers we talk to.
	Transport TransportConfig

	Codec runtime.Codec
	// EncodeVersioner is the same groupVersioner used to build the
	// storage encoder. Given a list of kinds the input object might belong
	// to, the EncodeVersioner outputs the gvk the object will be
	// converted to before persisted in etcd.
	EncodeVersioner runtime.GroupVersioner
	// Transformer allows the value to be transformed prior to persisting into etcd.
	Transformer value.Transformer

	// CompactionInterval 是 apiserver 向 etcd 发起压缩请求的间隔；为 0 表示不发起压缩。
	// 压缩是回收 etcd 历史版本空间的手段，缺失会导致 etcd 磁盘无限增长。
	CompactionInterval time.Duration
	// CountMetricPollPeriod specifies how often should count metric be updated
	CountMetricPollPeriod time.Duration
	// DBMetricPollInterval specifies how often should storage backend metric be updated.
	DBMetricPollInterval time.Duration
	// EventsHistoryWindow specifies minimum history duration that storage is keeping.
	EventsHistoryWindow time.Duration
	// HealthcheckTimeout specifies the timeout used when checking health
	HealthcheckTimeout time.Duration
	// ReadycheckTimeout specifies the timeout used when checking readiness
	ReadycheckTimeout time.Duration

	// LeaseManagerConfig 控制 etcd3 租约的复用与回收参数。
	LeaseManagerConfig etcd3.LeaseManagerConfig

	// StorageObjectCountTracker is used to keep track of the total
	// number of objects in the storage per resource.
	StorageObjectCountTracker flowcontrolrequest.StorageObjectCountTracker
}

// ConfigForResource is a Config specialized to a particular `schema.GroupResource`
type ConfigForResource struct {
	// Config is the resource-independent configuration
	Config

	// GroupResource is the relevant one
	GroupResource schema.GroupResource
}

// ForResource 把通用配置特化为给定资源的配置。
// 返回的是值拷贝：对返回值的修改不会影响原始 Config。
func (config *Config) ForResource(resource schema.GroupResource) *ConfigForResource {
	return &ConfigForResource{
		Config:        *config,
		GroupResource: resource,
	}
}

// NewDefaultConfig 返回一个填好生产默认值的 Config。
// 调用方仍需填写 Transport.ServerList；TracerProvider 默认是 no-op，
// 避免未配置 tracing 时产生额外开销。
func NewDefaultConfig(prefix string, codec runtime.Codec) *Config {
	return &Config{
		Prefix:               prefix,
		Codec:                codec,
		CompactionInterval:   DefaultCompactInterval,
		DBMetricPollInterval: DefaultDBMetricPollInterval,
		EventsHistoryWindow:  DefaultEventsHistoryWindow,
		HealthcheckTimeout:   DefaultHealthcheckTimeout,
		ReadycheckTimeout:    DefaultReadinessTimeout,
		LeaseManagerConfig:   etcd3.NewDefaultLeaseManagerConfig(),
		Transport:            TransportConfig{TracerProvider: noopoteltrace.NewTracerProvider()},
	}
}
