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

package rest

import (
	appsapiv1 "k8s.io/api/apps/v1"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apps"
	controllerrevisionsstore "k8s.io/kubernetes/pkg/registry/apps/controllerrevision/storage"
	daemonsetstore "k8s.io/kubernetes/pkg/registry/apps/daemonset/storage"
	deploymentstore "k8s.io/kubernetes/pkg/registry/apps/deployment/storage"
	replicasetstore "k8s.io/kubernetes/pkg/registry/apps/replicaset/storage"
	statefulsetstore "k8s.io/kubernetes/pkg/registry/apps/statefulset/storage"
)

// StorageProvider 负责装配 apps API Group 的 RESTStorage，并根据 APIResourceConfigSource 决定注册哪些资源。
type StorageProvider struct{}

// NewRESTStorage returns APIGroupInfo object.
// NewRESTStorage 创建 apps APIGroupInfo，并将已启用资源的 v1 REST 接口挂入版本化 StorageMap。
func (p StorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(apps.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 若在此处增加 API 版本，还必须在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将版本优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.v1Storage(apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[appsapiv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// v1Storage 按资源开关逐个创建 Storage；主资源、status、scale 等子资源分别使用独立 REST Handler。
func (p StorageProvider) v1Storage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	// Deployment 同时提供主资源、状态子资源和 scale 子资源；Rollback 作为历史兼容动作在该 Storage 内实现。
	if resource := "deployments"; apiResourceConfigSource.ResourceEnabled(appsapiv1.SchemeGroupVersion.WithResource(resource)) {
		deploymentStorage, err := deploymentstore.NewStorage(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = deploymentStorage.Deployment
		storage[resource+"/status"] = deploymentStorage.Status
		storage[resource+"/scale"] = deploymentStorage.Scale
	}

	// StatefulSet 提供主资源、状态和 scale 子资源，Replica 数量修改最终映射回 Spec.Replicas。
	if resource := "statefulsets"; apiResourceConfigSource.ResourceEnabled(appsapiv1.SchemeGroupVersion.WithResource(resource)) {
		statefulSetStorage, err := statefulsetstore.NewStorage(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = statefulSetStorage.StatefulSet
		storage[resource+"/status"] = statefulSetStorage.Status
		storage[resource+"/scale"] = statefulSetStorage.Scale
	}

	// DaemonSet 暴露主资源与状态子资源；当前该资源没有独立 scale Handler。
	if resource := "daemonsets"; apiResourceConfigSource.ResourceEnabled(appsapiv1.SchemeGroupVersion.WithResource(resource)) {
		daemonSetStorage, daemonSetStatusStorage, err := daemonsetstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = daemonSetStorage
		storage[resource+"/status"] = daemonSetStatusStorage
	}

	// ReplicaSet 提供主资源、状态和 scale 子资源。
	if resource := "replicasets"; apiResourceConfigSource.ResourceEnabled(appsapiv1.SchemeGroupVersion.WithResource(resource)) {
		replicaSetStorage, err := replicasetstore.NewStorage(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = replicaSetStorage.ReplicaSet
		storage[resource+"/status"] = replicaSetStorage.Status
		storage[resource+"/scale"] = replicaSetStorage.Scale
	}

	// ControllerRevision 仅注册历史快照主资源，不暴露 scale/status 子资源。
	if resource := "controllerrevisions"; apiResourceConfigSource.ResourceEnabled(appsapiv1.SchemeGroupVersion.WithResource(resource)) {
		historyStorage, err := controllerrevisionsstore.NewREST(restOptionsGetter)
		if err != nil {
			return storage, err
		}
		storage[resource] = historyStorage
	}

	return storage, nil
}

// GroupName 返回 apps API 组名称。
func (p StorageProvider) GroupName() string {
	return apps.GroupName
}
