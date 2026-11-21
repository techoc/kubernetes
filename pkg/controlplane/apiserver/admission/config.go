/*
Copyright 2024 The Kubernetes Authors.

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

package admission

import (
	"net/http"

	"go.opentelemetry.io/otel/trace"

	"k8s.io/apiserver/pkg/admission"
	webhookinit "k8s.io/apiserver/pkg/admission/plugin/webhook/initializer"
	egressselector "k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/util/webhook"
	externalinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/kubeapiserver/admission/exclusion"
	quotainstall "k8s.io/kubernetes/pkg/quota/v1/install"
)

// Config 保存初始化准入插件所需的配置。
type Config struct {
	LoopbackClientConfig *rest.Config
	ExternalInformers    externalinformers.SharedInformerFactory
	APIResourceConfig    storage.APIResourceConfigSource
}

// New 设置准入所需的插件与准入启动钩子。
func (c *Config) New(proxyTransport *http.Transport, egressSelector *egressselector.EgressSelector, serviceResolver webhook.ServiceResolver, tp trace.TracerProvider) ([]admission.PluginInitializer, error) {
	// Webhook 插件需要独立的认证解析器，以便通过代理/egress 选择器访问外部服务。
	webhookAuthResolverWrapper := webhook.NewDefaultAuthenticationInfoResolverWrapper(proxyTransport, egressSelector, c.LoopbackClientConfig, tp)
	webhookPluginInitializer := webhookinit.NewPluginInitializer(webhookAuthResolverWrapper, serviceResolver)

	// 配额配置依赖外部 informer 与启用的 API 资源，必须先构造成功再创建插件初始化器。
	quotaConfiguration, err := quotainstall.NewQuotaConfigurationForAdmission(c.ExternalInformers, c.APIResourceConfig)
	if err != nil {
		return nil, err
	}
	kubePluginInitializer := NewPluginInitializer(
		quotaConfiguration,
		exclusion.Excluded(),
	)

	return []admission.PluginInitializer{webhookPluginInitializer, kubePluginInitializer}, nil
}
