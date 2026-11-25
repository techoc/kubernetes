/*
Copyright 2023 The Kubernetes Authors.

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
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apiextensionsoptions "k8s.io/apiextensions-apiserver/pkg/cmd/server/options"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/informers"

	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
)

// CreateAPIExtensionsConfig 构造 CRD（apiextensions）服务器的配置。
func CreateAPIExtensionsConfig(
	kubeAPIServerConfig server.Config,
	kubeInformers informers.SharedInformerFactory,
	pluginInitializers []admission.PluginInitializer,
	commandOptions options.CompletedOptions,
	masterCount int,
	serviceResolver webhook.ServiceResolver,
	authResolverWrapper webhook.AuthenticationInfoResolverWrapper,
) (*apiextensionsapiserver.Config, error) {
	// 浅拷贝配置，仅调整与 apiextensions 服务器相关的少数项；清空钩子与 RESTOptionsGetter 避免重复安装。
	genericConfig := kubeAPIServerConfig
	genericConfig.PostStartHooks = map[string]server.PostStartHookConfigEntry{}
	genericConfig.RESTOptionsGetter = nil

	// 复制 etcd 选项避免修改原对象；假定其已完成，仅改动 StorageConfig 相关字段。
	etcdOptions := *commandOptions.Etcd
	// 真正的可解码版本由此处的 codec 决定。
	etcdOptions.StorageConfig.Codec = apiextensionsapiserver.Codecs.LegacyCodec(v1beta1.SchemeGroupVersion, v1.SchemeGroupVersion)
	// 优先使用更紧凑的 v1beta1 序列化存储，规避 v1 序列化过大而 v1beta1 可存储的问题（issue #82292）。
	etcdOptions.StorageConfig.EncodeVersioner = runtime.NewMultiGroupVersioner(v1beta1.SchemeGroupVersion, schema.GroupKind{Group: v1beta1.GroupName})
	etcdOptions.SkipHealthEndpoints = true // 避免重复接入健康检查
	if err := etcdOptions.ApplyTo(&genericConfig); err != nil {
		return nil, err
	}

	// 用 apiextensions 的默认资源配置与 scheme 覆盖 MergedResourceConfig。
	if err := commandOptions.APIEnablement.ApplyTo(
		&genericConfig,
		apiextensionsapiserver.DefaultAPIResourceConfigSource(),
		apiextensionsapiserver.Scheme); err != nil {
		return nil, err
	}
	apiextensionsConfig := &apiextensionsapiserver.Config{
		GenericConfig: &server.RecommendedConfig{
			Config:                genericConfig,
			SharedInformerFactory: kubeInformers,
		},
		ExtraConfig: apiextensionsapiserver.ExtraConfig{
			CRDRESTOptionsGetter: apiextensionsoptions.NewCRDRESTOptionsGetter(etcdOptions, genericConfig.ResourceTransformers, genericConfig.StorageObjectCountTracker),
			MasterCount:          masterCount,
			AuthResolverWrapper:  authResolverWrapper,
			ServiceResolver:      serviceResolver,
		},
	}

	// 清空启动后钩子，避免重复添加导致失败。
	apiextensionsConfig.GenericConfig.PostStartHooks = map[string]server.PostStartHookConfigEntry{}

	return apiextensionsConfig, nil
}
