/*
Copyright 2017 The Kubernetes Authors.

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

package rest

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	schedulingclient "k8s.io/client-go/kubernetes/typed/scheduling/v1"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	schedulingapiv1 "k8s.io/kubernetes/pkg/apis/scheduling/v1"
	schedulingapiv1alpha3 "k8s.io/kubernetes/pkg/apis/scheduling/v1alpha3"
	schedulingapiv1beta1 "k8s.io/kubernetes/pkg/apis/scheduling/v1beta1"
	"k8s.io/kubernetes/pkg/features"
	compositepodgroupstore "k8s.io/kubernetes/pkg/registry/scheduling/compositepodgroup/storage"
	podgroupstore "k8s.io/kubernetes/pkg/registry/scheduling/podgroup/storage"
	priorityclassstore "k8s.io/kubernetes/pkg/registry/scheduling/priorityclass/storage"
	workloadstore "k8s.io/kubernetes/pkg/registry/scheduling/workload/storage"
)

const PostStartHookName = "scheduling/bootstrap-system-priority-classes"

type RESTStorageProvider struct{}

var _ genericapiserver.PostStartHookProvider = RESTStorageProvider{}

// NewRESTStorage 初始化 Scheduling API Group，并按资源启用配置注册各 API 版本的 REST Storage。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(scheduling.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[schedulingapiv1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1beta1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[schedulingapiv1beta1.SchemeGroupVersion.Version] = storageMap
	}

	if storageMap, err := p.v1alpha3Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[schedulingapiv1alpha3.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 构建 scheduling.k8s.io/v1 版本的资源存储映射。
func (p RESTStorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// 注册集群级 PriorityClass 资源。
	if resource := "priorityclasses"; apiResourceConfigSource.ResourceEnabled(schedulingapiv1.SchemeGroupVersion.WithResource(resource)) {
		if priorityClassStorage, err := priorityclassstore.NewREST(restOptionsGetter); err != nil {
			return nil, err
		} else {
			storage[resource] = priorityClassStorage
		}
	}

	return storage, nil
}

// v1beta1Storage 构建 scheduling.k8s.io/v1beta1 版本的资源存储映射；Workload 和 PodGroup 受 GenericWorkload 特性门控控制。
func (p RESTStorageProvider) v1beta1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "workloads"; apiResourceConfigSource.ResourceEnabled(schedulingapiv1beta1.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
			workloadStorage, err := workloadstore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = workloadStorage
		} else {
			klog.Warning("Workload storage is disabled because the GenericWorkload feature gate is disabled")
		}
	}

	if resource := "podgroups"; apiResourceConfigSource.ResourceEnabled(schedulingapiv1beta1.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
			podGroupStorage, podGroupStatusStorage, err := podgroupstore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = podGroupStorage
			storage[resource+"/status"] = podGroupStatusStorage
		} else {
			klog.Warning("PodGroup storage is disabled because the GenericWorkload feature gate is disabled")
		}
	}

	return storage, nil
}

// v1alpha3Storage 构建 scheduling.k8s.io/v1alpha3 版本的资源存储映射；CompositePodGroup 受独立特性门控控制。
func (p RESTStorageProvider) v1alpha3Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	if resource := "workloads"; apiResourceConfigSource.ResourceEnabled(schedulingapiv1alpha3.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
			workloadStorage, err := workloadstore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = workloadStorage
		} else {
			klog.Warning("Workload storage is disabled because the GenericWorkload feature gate is disabled")
		}
	}

	if resource := "podgroups"; apiResourceConfigSource.ResourceEnabled(schedulingapiv1alpha3.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
			podGroupStorage, podGroupStatusStorage, err := podgroupstore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = podGroupStorage
			storage[resource+"/status"] = podGroupStatusStorage
		} else {
			klog.Warning("PodGroup storage is disabled because the GenericWorkload feature gate is disabled")
		}
	}

	if resource := "compositepodgroups"; apiResourceConfigSource.ResourceEnabled(schedulingapiv1alpha3.SchemeGroupVersion.WithResource(resource)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
			cpgStorage, cpgStatusStorage, err := compositepodgroupstore.NewREST(restOptionsGetter)
			if err != nil {
				return nil, err
			}
			storage[resource] = cpgStorage
			storage[resource+"/status"] = cpgStatusStorage
		} else {
			klog.Warning("CompositePodGroup storage is disabled because the CompositePodGroup feature gate is disabled")
		}
	}

	return storage, nil
}

// PostStartHook 注册 API Server 启动后创建内置系统 PriorityClass 的 Hook。
func (p RESTStorageProvider) PostStartHook() (string, genericapiserver.PostStartHookFunc, error) {
	return PostStartHookName, AddSystemPriorityClasses(), nil
}

// AddSystemPriorityClasses 周期性检查并创建内置系统 PriorityClass；存储暂不可用时会重试，初始化超时则阻止 API Server 启动。
func AddSystemPriorityClasses() genericapiserver.PostStartHookFunc {
	return func(hookContext genericapiserver.PostStartHookContext) error {
		// 内置系统 PriorityClass 必须成功创建；如果缺失，多个关键系统组件可能无法正常运行，甚至导致集群不可用。
		err := wait.Poll(1*time.Second, 30*time.Second, func() (done bool, err error) {
			schedClientSet, err := schedulingclient.NewForConfig(hookContext.LoopbackClientConfig)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to initialize client: %v", err))
				return false, nil
			}

			for _, pc := range schedulingapiv1.SystemPriorityClasses() {
				_, err := schedClientSet.PriorityClasses().Get(context.TODO(), pc.Name, metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						_, err := schedClientSet.PriorityClasses().Create(context.TODO(), pc, metav1.CreateOptions{})
						if err == nil || apierrors.IsAlreadyExists(err) {
							klog.Infof("created PriorityClass %s with value %v", pc.Name, pc.Value)
							continue
						}
						// 存储版本升级阻塞 API Server 时，创建请求可能返回 ServiceUnavailable，需要等待后重试。
						if apierrors.IsServiceUnavailable(err) {
							klog.Infof("going to retry, unable to create PriorityClass %s: %v", pc.Name, err)
							return false, nil
						}
						return false, err
					} else {
						// 读取 PriorityClass 时发生非 NotFound 错误，暂时无法确认对象状态，记录警告并重试。
						klog.Warningf("unable to get PriorityClass %v: %v. Retrying...", pc.Name, err)
						return false, nil
					}
				}
			}
			klog.Infof("all system priority classes are created successfully or already exist.")
			return true, nil
		})
		// 如果在重试期限内始终无法完成系统 PriorityClass 初始化，则终止 API Server 启动。
		if err != nil {
			return fmt.Errorf("unable to add default system priority classes: %v", err)
		}
		return nil
	}
}

func (p RESTStorageProvider) GroupName() string {
	return scheduling.GroupName
}
