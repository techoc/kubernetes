/*
Copyright 2014 The Kubernetes Authors.

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

package framework

import (
	"path"

	"github.com/google/uuid"

	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	utilopenapi "k8s.io/apiserver/pkg/util/openapi"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/generated/openapi"
)

const (
	UnprivilegedUserToken = "unprivileged-user"
)

// DefaultOpenAPIConfig 返回初始化为默认值的 openapicommon.Config。
func DefaultOpenAPIConfig() *openapicommon.Config {
	openAPIConfig := genericapiserver.DefaultOpenAPIConfig(openapi.GetOpenAPIDefinitions, openapinamer.NewDefinitionNamer(legacyscheme.Scheme))
	openAPIConfig.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:   "Kubernetes",
			Version: "unversioned",
		},
	}
	openAPIConfig.DefaultResponse = &spec.Response{
		ResponseProps: spec.ResponseProps{
			Description: "Default Response.",
		},
	}
	openAPIConfig.GetDefinitions = utilopenapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(openapi.GetOpenAPIDefinitions)

	return openAPIConfig
}

// DefaultOpenAPIV3Config 返回初始化为默认值的 openapicommon.Config。
func DefaultOpenAPIV3Config() *openapicommon.OpenAPIV3Config {
	openAPIConfig := genericapiserver.DefaultOpenAPIV3Config(openapi.GetOpenAPIDefinitions, openapinamer.NewDefinitionNamer(legacyscheme.Scheme))
	openAPIConfig.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:   "Kubernetes",
			Version: "unversioned",
		},
	}
	openAPIConfig.DefaultResponse = &spec3.Response{
		ResponseProps: spec3.ResponseProps{
			Description: "Default Response.",
		},
	}
	openAPIConfig.GetDefinitions = utilopenapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(openapi.GetOpenAPIDefinitions)

	return openAPIConfig
}

// DefaultEtcdOptions 是集成测试使用的默认 EtcdOptions。
func DefaultEtcdOptions() *options.EtcdOptions {
	// 这会让集成测试覆盖 etcd prefix 相关代码，
	// 因此除非有其他方式保证足够的覆盖率，
	// 否则请勿修改。
	etcdOptions := options.NewEtcdOptions(storagebackend.NewDefaultConfig(uuid.New().String(), nil))
	etcdOptions.StorageConfig.Transport.ServerList = []string{GetEtcdURL()}
	return etcdOptions
}

// SharedEtcd 为共享 etcd 实例创建一个带有唯一前缀的 storage config。
//
// 对于非安全连接，transport 的 CertFile/KeyFile/TrustedCAFile 将为空。
// 在这种情况下，不应使用任何 TLS 配置，因为 etcd 会尝试将其用于
// Unix Domain Socket（https://github.com/etcd-io/etcd/blob/5a8fba466087686fc15815f5bc041fb7eb1f23ea/client/v3/internal/endpoint/endpoint.go#L61-L66），
// 并因 TLS 配置不足而连接失败。对于 TCP 则没有问题，因为 http 禁用了 TLS 的使用。
func SharedEtcd() *storagebackend.Config {
	cfg := storagebackend.NewDefaultConfig(path.Join(uuid.New().String(), "registry"), nil)
	cfg.Transport.ServerList = []string{GetEtcdURL()}
	return cfg
}

// DefaultTestServerFlags 返回测试中用于运行 kube-apiserver 的默认 flag
func DefaultTestServerFlags() []string {
	return []string{
		"--endpoint-reconciler-type=none",            // 禁用 Endpoints Reconciler，避免它不断尝试使用 127.0.0.1 作为有效 Endpoint 而失败。
		"--disable-admission-plugins=ServiceAccount", // 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	}
}
