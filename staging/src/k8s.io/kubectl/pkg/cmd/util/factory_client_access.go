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

// 本文件包含不依赖其他内容的 factory

package util

import (
	"errors"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	openapiclient "k8s.io/client-go/openapi"
	"k8s.io/client-go/openapi/cached"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/util/openapi"
	"k8s.io/kubectl/pkg/validation"
)

type factoryImpl struct {
	clientGetter genericclioptions.RESTClientGetter

	// 缓存 OpenAPI 文档及解析后的资源
	openAPIParser *openapi.CachedOpenAPIParser
	oapi          *openapi.CachedOpenAPIGetter
	parser        sync.Once
	getter        sync.Once
}

func NewFactory(clientGetter genericclioptions.RESTClientGetter) Factory {
	if clientGetter == nil {
		panic("attempt to instantiate client_access_factory with nil clientGetter")
	}
	f := &factoryImpl{
		clientGetter: clientGetter,
	}

	return f
}

func (f *factoryImpl) ToRESTConfig() (*restclient.Config, error) {
	return f.clientGetter.ToRESTConfig()
}

func (f *factoryImpl) ToRESTMapper() (meta.RESTMapper, error) {
	return f.clientGetter.ToRESTMapper()
}

func (f *factoryImpl) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	return f.clientGetter.ToDiscoveryClient()
}

func (f *factoryImpl) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	return f.clientGetter.ToRawKubeConfigLoader()
}

func (f *factoryImpl) KubernetesClientSet() (*kubernetes.Clientset, error) {
	clientConfig, err := f.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(clientConfig)
}

func (f *factoryImpl) DynamicClient() (dynamic.Interface, error) {
	clientConfig, err := f.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(clientConfig)
}

// NewBuilder 为结构化的 API 对象返回一个新的 resource builder。
func (f *factoryImpl) NewBuilder() *resource.Builder {
	return resource.NewBuilder(f.clientGetter)
}

func (f *factoryImpl) RESTClient() (*restclient.RESTClient, error) {
	clientConfig, err := f.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	setKubernetesDefaults(clientConfig)
	return restclient.RESTClientFor(clientConfig)
}

func (f *factoryImpl) ClientForMapping(mapping *meta.RESTMapping) (resource.RESTClient, error) {
	cfg, err := f.clientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	if err := setKubernetesDefaults(cfg); err != nil {
		return nil, err
	}
	gvk := mapping.GroupVersionKind
	switch gvk.Group {
	case corev1.GroupName:
		cfg.APIPath = "/api"
	default:
		cfg.APIPath = "/apis"
	}
	gv := gvk.GroupVersion()
	cfg.GroupVersion = &gv
	return restclient.RESTClientFor(cfg)
}

func (f *factoryImpl) UnstructuredClientForMapping(mapping *meta.RESTMapping) (resource.RESTClient, error) {
	cfg, err := f.clientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	if err := restclient.SetKubernetesDefaults(cfg); err != nil {
		return nil, err
	}
	cfg.APIPath = "/apis"
	if mapping.GroupVersionKind.Group == corev1.GroupName {
		cfg.APIPath = "/api"
	}
	gv := mapping.GroupVersionKind.GroupVersion()
	cfg.ContentConfig = resource.UnstructuredPlusDefaultContentConfig()
	cfg.GroupVersion = &gv
	return restclient.RESTClientFor(cfg)
}

func (f *factoryImpl) Validator(validationDirective string) (validation.Schema, error) {
	// 仅在 validationDirective 为 strict 时
	// 才执行客户端 schema 校验。
	// 如果指令为 warn，则依赖 ParamVerifyingSchema
	// 忽略客户端校验，并向用户发出警告：在不支持 SS 校验时
	// 尝试使用 warn 校验不会生效。
	if validationDirective == metav1.FieldValidationIgnore {
		return validation.NullSchema{}, nil
	}

	schema := validation.ConjunctiveSchema{
		validation.NewSchemaValidation(f),
		validation.NoDoubleKeySchema{},
	}

	dynamicClient, err := f.DynamicClient()
	if err != nil {
		return nil, err
	}
	// 创建 FieldValidationVerifier，供 ParamVerifyingSchema 使用。
	discoveryClient, err := f.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}
	// 将 OpenAPI V3 响应缓存在内存中。磁盘缓存行为由
	// discovery client 决定。
	oapiV3Client := cached.NewClient(discoveryClient.OpenAPIV3())
	queryParam := resource.QueryParamFieldValidation
	primary := newCachingVerifier(resource.NewQueryParamVerifierV3(dynamicClient, oapiV3Client, queryParam))
	secondary := resource.NewQueryParamVerifier(dynamicClient, f.openAPIGetter(), queryParam)
	fallback := resource.NewFallbackQueryParamVerifier(primary, secondary)
	return validation.NewParamVerifyingSchema(schema, fallback, string(validationDirective)), nil
}

// OpenAPISchema 返回有关 Kubernetes 对象定义的
// 元信息和结构信息。
func (f *factoryImpl) OpenAPISchema() (openapi.Resources, error) {
	openAPIGetter := f.openAPIGetter()
	if openAPIGetter == nil {
		return nil, errors.New("no openapi getter")
	}

	// 延迟初始化 OpenAPIParser，且只初始化一次
	f.parser.Do(func() {
		// 创建带缓存的 OpenAPIParser
		f.openAPIParser = openapi.NewOpenAPIParser(f.openAPIGetter())
	})

	// 委托给 OpenAPIParser 处理
	return f.openAPIParser.Parse()
}

func (f *factoryImpl) openAPIGetter() discovery.OpenAPISchemaInterface {
	discovery, err := f.clientGetter.ToDiscoveryClient()
	if err != nil {
		return nil
	}
	f.getter.Do(func() {
		f.oapi = openapi.NewOpenAPIGetter(discovery)
	})

	return f.oapi
}

func (f *factoryImpl) OpenAPIV3Client() (openapiclient.Client, error) {
	discovery, err := f.clientGetter.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}

	return cached.NewClient(discovery.OpenAPIV3()), nil
}
