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

package registrytest

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/server/options"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/kubernetes/pkg/kubeapiserver"
)

// NewEtcdStorage 为测试启动临时 etcd，并使用任意占位资源配置存储；调用方无需关心具体资源名称。
func NewEtcdStorage(t *testing.T, group string) (*storagebackend.ConfigForResource, *etcd3testing.EtcdTestServer) {
	return NewEtcdStorageForResource(t, schema.GroupResource{Group: group, Resource: "any"})
}

// NewEtcdStorageForResource 为指定 GroupResource 创建临时 etcd 测试服务，并返回对应的存储后端配置。
func NewEtcdStorageForResource(t *testing.T, resource schema.GroupResource) (*storagebackend.ConfigForResource, *etcd3testing.EtcdTestServer) {
	t.Helper()

	server, config := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)

	options := options.NewEtcdOptions(config)
	completedConfig := kubeapiserver.NewStorageFactoryConfig().Complete(options)
	completedConfig.APIResourceConfig = serverstorage.NewResourceConfig()
	factory, err := completedConfig.New()
	if err != nil {
		t.Fatalf("Error while making storage factory: %v", err)
	}
	resourceConfig, err := factory.NewConfig(resource, nil)
	if err != nil {
		t.Fatalf("Error while finding storage destination: %v", err)
	}
	return resourceConfig, server
}
