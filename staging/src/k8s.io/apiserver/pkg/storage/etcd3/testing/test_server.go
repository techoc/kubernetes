/*
Copyright 2015 The Kubernetes Authors.

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

// Package testing 为 etcd3 存储层测试启动本地 etcd 实例并构造对应的存储配置。
package testing

import (
	"testing"

	"go.etcd.io/etcd/client/v3/kubernetes"

	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// EtcdTestServer 封装启动本地测试实例所需的数据结构。
type EtcdTestServer struct {
	V3Client *kubernetes.Client
}

// Terminate 保留为空操作：etcd 进程的生命周期已交给 testing.TB 的 cleanup 管理，
// 由 t.Cleanup 保证测试结束时回收。
func (e *EtcdTestServer) Terminate(t testing.TB) {
	// 空操作：服务端终止已交给测试清理逻辑处理。
}

// NewUnsecuredEtcd3TestClientServer 为测试创建新的客户端与服务端。
//
// 返回未启用 TLS 的本地 etcd 客户端，以及指向该实例、带测试前缀的存储配置。
func NewUnsecuredEtcd3TestClientServer(t testing.TB) (*EtcdTestServer, *storagebackend.Config) {
	server := &EtcdTestServer{}
	server.V3Client = testserver.RunEtcd(t, nil)
	config := &storagebackend.Config{
		Type:   "etcd3",
		Prefix: PathPrefix(),
		Transport: storagebackend.TransportConfig{
			ServerList: server.V3Client.Endpoints(),
		},
	}
	return server, config
}
