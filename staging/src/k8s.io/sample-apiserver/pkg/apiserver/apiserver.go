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
	// Scheme 提供 API 对象的序列化与反序列化方法。
	// 全局单例供 API 安装和各版本存储共用，改这里等于改所有请求看到的编解码规则。
	Scheme = runtime.NewScheme()
	// Codecs 按版本和内容类型提供编解码器与序列化器。
	Codecs = serializer.NewCodecFactory(Scheme)
	// WardleComponentName 是 Wardle 组件的注册名，同时用于事件与日志来源。
	WardleComponentName = "wardle"
)

func init() {
	install.Install(Scheme)

	// 没有 Group 的 v1 也要带上 ListOptions 等元类型，否则列表请求解码会失败。
	// TODO：修正服务端代码，避免需要此注册。
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// TODO：阻止通用 API server 依赖这些未分组类型。
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

// ExtraConfig 保存示例 apiserver 的自定义配置。
// 示例里没有可配项，保留空壳是为了展示如何扩展配置结构。
type ExtraConfig struct {
	// 在此添加自定义配置。
}

// Config 定义构造 Wardle apiserver 所需的配置。
// 把通用配置和示例专属配置分开传，便于构造时各取所需。
type Config struct {
	// GenericConfig 承载通用 apiserver 的配置。
	GenericConfig *genericapiserver.RecommendedConfig
	// ExtraConfig 承载 Wardle 专属配置。
	ExtraConfig ExtraConfig
}

// WardleServer 保存 Wardle API 服务器的运行状态。
// 这里只包一层通用 server，聚合 apiserver 的委托逻辑也由它承载。
type WardleServer struct {
	// GenericAPIServer 是承载 HTTP 服务与 API 安装的通用服务器。
	GenericAPIServer *genericapiserver.GenericAPIServer
}

// 私有类型保证调用方只能走 Config.Complete，不会构造出半初始化的配置。
type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig 内嵌私有配置指针，包外无法直接实例化。
// 对外暴露类型但不暴露字段，顺序约束靠包边界而不是文档约定。
type CompletedConfig struct {
	*completedConfig
}

// Complete 补齐构造有效服务器所需的字段，并直接修改接收者。
// 直接改接收者而非拷贝，避免调用方误用未补全的配置。
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	return CompletedConfig{&c}
}

// New 根据已补全的配置创建 WardleServer。
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
