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

package kubeapiserver

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serveroptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/server/resourceconfig"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/util/compatibility"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/apis/coordination"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/events"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/apis/policy"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/apis/storagemigration"
)

// SpecialDefaultResourcePrefixes 是编译进 Kubernetes 的特殊默认 etcd 键前缀。
// 保留这些历史前缀是为了兼容已有集群中存储的数据路径。
var SpecialDefaultResourcePrefixes = map[schema.GroupResource]string{
	{Group: "", Resource: "replicationcontrollers"}:     "controllers",
	{Group: "", Resource: "endpoints"}:                  "services/endpoints",
	{Group: "", Resource: "nodes"}:                      "minions",
	{Group: "", Resource: "services"}:                   "services/specs",
	{Group: "extensions", Resource: "ingresses"}:        "ingress",
	{Group: "networking.k8s.io", Resource: "ingresses"}: "ingress",
}

// DefaultWatchCacheSizes 定义默认应禁用 watch cache 的资源。
// event 数量大且生命周期短，缓存它们收益极低却会消耗大量内存。
func DefaultWatchCacheSizes() map[schema.GroupResource]int {
	return map[schema.GroupResource]int{
		{Resource: "events"}:                         0,
		{Group: "events.k8s.io", Resource: "events"}: 0,
	}
}

// NewStorageFactoryConfig 返回一个带有必要资源覆盖配置的新 StorageFactoryConfig。
func NewStorageFactoryConfig() *StorageFactoryConfig {
	return NewStorageFactoryConfigEffectiveVersion(compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent))
}

// NewStorageFactoryConfigEffectiveVersion 针对给定的 EffectiveVersion 返回一个
// 带有必要资源覆盖配置的新 StorageFactoryConfig。
func NewStorageFactoryConfigEffectiveVersion(effectiveVersion basecompatibility.EffectiveVersion) *StorageFactoryConfig {
	// 某些资源必须以非最新版本存储，以支持向还不支持新版本的老 apiserver 回滚一个版本。
	resources := []schema.GroupVersionResource{
		// If a resource has to be stored in a version that is not the
		// latest, then it can be listed here. Usually this is the case
		// when a new version for a resource gets introduced and a
		// downgrade to an older apiserver that doesn't know the new
		// version still needs to be supported for one release.
		//
		// Example from Kubernetes 1.24 where csistoragecapacities had just
		// graduated to GA:
		//
		// TODO (https://github.com/kubernetes/kubernetes/issues/108451): remove the override in 1.25.
		// apisstorage.Resource("csistoragecapacities").WithVersion("v1beta1"),
		coordination.Resource("leasecandidates").WithVersion("v1beta1"),
		certificates.Resource("clustertrustbundles").WithVersion("v1beta1"), // TODO: remove in 1.38
		certificates.Resource("podcertificaterequests").WithVersion("v1beta1"),
		storagemigration.Resource("storageversionmigrations").WithVersion("v1beta1"),
		resource.Resource("devicetaintrules").WithVersion("v1beta2"),
		resource.Resource("resourcepoolstatusrequests").WithVersion("v1alpha3"),
		scheduling.Resource("workloads").WithVersion("v1beta1"),
		scheduling.Resource("podgroups").WithVersion("v1beta1"),
		scheduling.Resource("compositepodgroups").WithVersion("v1alpha3"),
		lifecycle.Resource("evictions").WithVersion("v1alpha1"),
		lifecycle.Resource("evictionrequests").WithVersion("v1alpha1"),
	}
	return &StorageFactoryConfig{
		Serializer:                legacyscheme.Codecs,
		DefaultResourceEncoding:   serverstorage.NewDefaultResourceEncodingConfigForEffectiveVersion(legacyscheme.Scheme, effectiveVersion),
		ResourceEncodingOverrides: resources,
	}
}

// StorageFactoryConfig 是用于创建存储工厂的配置。
type StorageFactoryConfig struct {
	StorageConfig             storagebackend.Config
	APIResourceConfig         *serverstorage.ResourceConfig
	DefaultResourceEncoding   *serverstorage.DefaultResourceEncodingConfig
	DefaultStorageMediaType   string
	Serializer                runtime.StorageSerializer
	ResourceEncodingOverrides []schema.GroupVersionResource
	EtcdServersOverrides      []string
}

// Complete 用给定的 etcdOptions 补全 StorageFactoryConfig，返回 completedStorageFactoryConfig。
// 该方法会修改接收者，但绝不修改输入参数。
func (c *StorageFactoryConfig) Complete(etcdOptions *serveroptions.EtcdOptions) *completedStorageFactoryConfig {
	c.StorageConfig = etcdOptions.StorageConfig
	c.DefaultStorageMediaType = etcdOptions.DefaultStorageMediaType
	c.EtcdServersOverrides = etcdOptions.EtcdServersOverrides
	return &completedStorageFactoryConfig{c}
}

// completedStorageFactoryConfig 是经 etcd 选项补全后的 StorageFactoryConfig 包装。
//
// 注意：该结构体故意保持非导出，确保只能通过 StorageFactoryConfig.Complete 构造；
// 因此需要绕过 golint 对导出类型的要求。
type completedStorageFactoryConfig struct {
	*StorageFactoryConfig
}

// New 根据已补全的配置创建一个新的存储工厂。
func (c *completedStorageFactoryConfig) New() (*serverstorage.DefaultStorageFactory, error) {
	resourceEncodingConfig := resourceconfig.MergeResourceEncodingConfigs(c.DefaultResourceEncoding, c.ResourceEncodingOverrides)
	// 新旧 API 组中的同一资源共享底层存储，避免版本迁移时出现数据分裂。
	storageFactory := serverstorage.NewDefaultStorageFactory(
		c.StorageConfig,
		c.DefaultStorageMediaType,
		c.Serializer,
		resourceEncodingConfig,
		c.APIResourceConfig,
		SpecialDefaultResourcePrefixes)

	storageFactory.AddCohabitatingResources(networking.Resource("networkpolicies"), extensions.Resource("networkpolicies"))
	storageFactory.AddCohabitatingResources(apps.Resource("deployments"), extensions.Resource("deployments"))
	storageFactory.AddCohabitatingResources(apps.Resource("daemonsets"), extensions.Resource("daemonsets"))
	storageFactory.AddCohabitatingResources(apps.Resource("replicasets"), extensions.Resource("replicasets"))
	storageFactory.AddCohabitatingResources(api.Resource("events"), events.Resource("events"))
	storageFactory.AddCohabitatingResources(api.Resource("replicationcontrollers"), extensions.Resource("replicationcontrollers")) // 使 scale 子资源等价
	storageFactory.AddCohabitatingResources(policy.Resource("podsecuritypolicies"), extensions.Resource("podsecuritypolicies"))
	storageFactory.AddCohabitatingResources(networking.Resource("ingresses"), extensions.Resource("ingresses"))

	// 按资源覆盖 etcd 服务器地址，供多 etcd 集群拆分流量的场景使用。
	for _, override := range c.EtcdServersOverrides {
		tokens := strings.Split(override, "#")
		apiresource := strings.Split(tokens[0], "/")

		group := apiresource[0]
		resource := apiresource[1]
		groupResource := schema.GroupResource{Group: group, Resource: resource}

		servers := strings.Split(tokens[1], ";")
		storageFactory.SetEtcdLocation(groupResource, servers)
	}
	return storageFactory, nil
}
