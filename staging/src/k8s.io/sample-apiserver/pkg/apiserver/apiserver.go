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

package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"

	"k8s.io/sample-apiserver/pkg/apis/wardle"
	"k8s.io/sample-apiserver/pkg/apis/wardle/install"
	wardleregistry "k8s.io/sample-apiserver/pkg/registry"
	fischerstorage "k8s.io/sample-apiserver/pkg/registry/wardle/fischer"
	flunderstorage "k8s.io/sample-apiserver/pkg/registry/wardle/flunder"
)

var (
	// 全局单例供 API 安装和各版本存储共用，改这里等于改所有请求看到的编解码规则。
	// 必须基于上面的 Scheme 构建，否则版本协商会拿到另一套类型信息。
	// 组件名同时用于事件与日志来源，和 API 组名保持一致以免排查时混淆。
	// Scheme defines methods for serializing and deserializing API objects.
	Scheme = runtime.NewScheme()
	// Codecs provides methods for retrieving codecs and serializers for specific
	// versions and content types.
	Codecs              = serializer.NewCodecFactory(Scheme)
	WardleComponentName = "wardle"
)

func init() {
	install.Install(Scheme)

	// we need to add the options to empty v1
	// TODO fix the server code to avoid this
	// 没有 Group 的 v1 也要带上 ListOptions 等元类型，否则列表请求解码会失败。
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// TODO: keep the generic API server from wanting this
	// 通用 apiserver 的发现接口依赖这些未分组类型，示例服务只能照注册。
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

// ExtraConfig holds custom apiserver config
// 示例里没有可配项，保留空壳是为了展示如何扩展配置结构。
type ExtraConfig struct {
	// Place you custom config here.
}

// Config defines the config for the apiserver
// 把通用配置和示例专属配置分开传，便于构造时各取所需。
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

// WardleServer contains state for a Kubernetes cluster master/api server.
// 这里只包一层通用 server，聚合 apiserver 的委托逻辑也由它承载。
type WardleServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

// 私有类型保证调用方只能走 Config.Complete，不会构造出半初始化的配置。
type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig embeds a private pointer that cannot be instantiated outside of this package.
// 对外暴露类型但不暴露字段，顺序约束靠包边界而不是文档约定。
type CompletedConfig struct {
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
// 直接改接收者而非拷贝，避免调用方误用未补全的配置。
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	return CompletedConfig{&c}
}

// New returns a new instance of WardleServer from the given config.
// 在通用 server 上按版本挂载存储：v1alpha1 有 Fischer 和 Flunder，v1beta1 只有 Flunder，
// 正好演示同一资源多版本共存时存储可以独立演进。
func (c completedConfig) New() (*WardleServer, error) {
	genericServer, err := c.GenericConfig.New("sample-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	s := &WardleServer{
		GenericAPIServer: genericServer,
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(wardle.GroupName, Scheme, metav1.ParameterCodec, Codecs)

	v1alpha1storage := map[string]rest.Storage{}
	v1alpha1storage["flunders"] = wardleregistry.RESTInPeace(flunderstorage.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter))
	v1alpha1storage["fischers"] = wardleregistry.RESTInPeace(fischerstorage.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter))
	apiGroupInfo.VersionedResourcesStorageMap["v1alpha1"] = v1alpha1storage

	v1beta1storage := map[string]rest.Storage{}
	v1beta1storage["flunders"] = wardleregistry.RESTInPeace(flunderstorage.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter))
	apiGroupInfo.VersionedResourcesStorageMap["v1beta1"] = v1beta1storage

	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	return s, nil
}
