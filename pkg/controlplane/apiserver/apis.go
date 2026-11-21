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
	"fmt"

	"k8s.io/apiserver/pkg/registry/generic"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/client-go/discovery"
	"k8s.io/klog/v2"
	svmrest "k8s.io/kubernetes/pkg/registry/storagemigration/rest"

	admissionregistrationrest "k8s.io/kubernetes/pkg/registry/admissionregistration/rest"
	apiserverinternalrest "k8s.io/kubernetes/pkg/registry/apiserverinternal/rest"
	authenticationrest "k8s.io/kubernetes/pkg/registry/authentication/rest"
	authorizationrest "k8s.io/kubernetes/pkg/registry/authorization/rest"
	certificatesrest "k8s.io/kubernetes/pkg/registry/certificates/rest"
	coordinationrest "k8s.io/kubernetes/pkg/registry/coordination/rest"
	corerest "k8s.io/kubernetes/pkg/registry/core/rest"
	eventsrest "k8s.io/kubernetes/pkg/registry/events/rest"
	flowcontrolrest "k8s.io/kubernetes/pkg/registry/flowcontrol/rest"
	rbacrest "k8s.io/kubernetes/pkg/registry/rbac/rest"
)

// RESTStorageProvider 是 REST 存储的工厂接口。
type RESTStorageProvider interface {
	// GroupName 返回该提供者对应的 API 组名。
	GroupName() string
	// NewRESTStorage 根据资源配置与 REST 选项构造 API 组信息。
	NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error)
}

// NewCoreGenericConfig 返回 core 组的 REST 通用配置。
func (c *CompletedConfig) NewCoreGenericConfig() *corerest.GenericConfig {
	return &corerest.GenericConfig{
		StorageFactory:              c.Extra.StorageFactory,
		EventTTL:                    c.Extra.EventTTL,
		LoopbackClientConfig:        c.Generic.LoopbackClientConfig,
		ServiceAccountIssuer:        c.Extra.ServiceAccountIssuer,
		ExtendExpiration:            c.Extra.ExtendExpiration,
		ServiceAccountMaxExpiration: c.Extra.ServiceAccountMaxExpiration,
		MaxExtendedExpiration:       c.Extra.ServiceAccountExtendedMaxExpiration,
		APIAudiences:                c.Generic.Authentication.APIAudiences,
		Informers:                   c.Extra.VersionedInformers,
	}
}

// GenericStorageProviders 返回通用控制面的一组 API 存储提供者，它们应是 kube-apiserver 所提供 API 的子集。
func (c *CompletedConfig) GenericStorageProviders(discovery discovery.DiscoveryInterface) ([]RESTStorageProvider, error) {
	// 列表顺序会被发现接口保留：当多个组存在同名资源时，顺序决定了非限定资源名优先归属的组。
	// 本地发现使用该顺序，聚合时会由 aggregator.go 中特定的优先级覆盖。
	// TODO: 将优先级下沉到 RESTStorageProviders 并贯穿各发现处理器。
	return []RESTStorageProvider{
		c.NewCoreGenericConfig(),
		apiserverinternalrest.StorageProvider{},
		authenticationrest.RESTStorageProvider{Authenticator: c.Generic.Authentication.Authenticator, APIAudiences: c.Generic.Authentication.APIAudiences},
		authorizationrest.RESTStorageProvider{Authorizer: c.Generic.Authorization.Authorizer, RuleResolver: c.Generic.RuleResolver},
		certificatesrest.RESTStorageProvider{Authorizer: c.Generic.Authorization.Authorizer, Generic: true},
		coordinationrest.RESTStorageProvider{},
		rbacrest.RESTStorageProvider{Authorizer: c.Generic.Authorization.Authorizer},
		svmrest.RESTStorageProvider{},
		flowcontrolrest.RESTStorageProvider{InformerFactory: c.Generic.SharedInformerFactory},
		admissionregistrationrest.RESTStorageProvider{Authorizer: c.Generic.Authorization.Authorizer, DiscoveryClient: discovery},
		eventsrest.RESTStorageProvider{TTL: c.EventTTL},
	}, nil
}

// InstallAPIs 安装所有已启用的 REST 存储提供者对应的 API。
func (s *Server) InstallAPIs(restStorageProviders ...RESTStorageProvider) error {
	nonLegacy := []*genericapiserver.APIGroupInfo{}

	// 该评估器用于过滤在当前版本已过期或尚未引入的资源。
	resourceExpirationEvaluatorOpts := genericapiserver.ResourceExpirationEvaluatorOptions{
		CurrentVersion:                          s.GenericAPIServer.EffectiveVersion.EmulationVersion(),
		Prerelease:                              s.GenericAPIServer.EffectiveVersion.BinaryVersion().PreRelease(),
		EmulationForwardCompatible:              s.GenericAPIServer.EmulationForwardCompatible,
		RuntimeConfigEmulationForwardCompatible: s.GenericAPIServer.RuntimeConfigEmulationForwardCompatible,
	}
	resourceExpirationEvaluator, err := genericapiserver.NewResourceExpirationEvaluatorFromOptions(resourceExpirationEvaluatorOpts)
	if err != nil {
		return err
	}

	for _, restStorageBuilder := range restStorageProviders {
		groupName := restStorageBuilder.GroupName()
		apiGroupInfo, err := restStorageBuilder.NewRESTStorage(s.APIResourceConfigSource, s.RESTOptionsGetter)
		if err != nil {
			return fmt.Errorf("problem initializing API group %q: %w", groupName, err)
		}
		if len(apiGroupInfo.VersionedResourcesStorageMap) == 0 {
			// 未配置任何资源存储意味着该 API 组实际被禁用（可能是整个组、某个版本或某开发阶段被禁用）。
			klog.Infof("API group %q is not enabled, skipping.", groupName)
			continue
		}

		// 移除当前版本已删除或尚未引入的 kind，避免提供没有资源或 openapi 信息的版本。
		// 放在此处统一处理，避免各 SIG 在构建存储处理器时遗漏检查。
		err = resourceExpirationEvaluator.RemoveUnavailableKinds(groupName, apiGroupInfo.Scheme, apiGroupInfo.VersionedResourcesStorageMap, s.APIResourceConfigSource)
		if err != nil {
			return err
		}
		if len(apiGroupInfo.VersionedResourcesStorageMap) == 0 {
			klog.V(1).Infof("Removing API group %v because it is time to stop serving it because it has no versions per APILifecycle.", groupName)
			continue
		}

		klog.V(1).Infof("Enabling API group %q.", groupName)

		if postHookProvider, ok := restStorageBuilder.(genericapiserver.PostStartHookProvider); ok {
			name, hook, err := postHookProvider.PostStartHook()
			if err != nil {
				return fmt.Errorf("error building PostStartHook: %w", err)
			}
			s.GenericAPIServer.AddPostStartHookOrDie(name, hook)
		}

		if len(groupName) == 0 {
			// core API 的 legacy 组比较特殊，需通过专用方法安装到 /api。
			if err := s.GenericAPIServer.InstallLegacyAPIGroup(genericapiserver.DefaultLegacyAPIPrefix, &apiGroupInfo); err != nil {
				return fmt.Errorf("error in registering legacy API: %w", err)
			}
		} else {
			// 其他组统一安装到 /apis。
			nonLegacy = append(nonLegacy, &apiGroupInfo)
		}
	}

	if err := s.GenericAPIServer.InstallAPIGroups(nonLegacy...); err != nil {
		return fmt.Errorf("error in registering group versions: %w", err)
	}
	return nil
}
