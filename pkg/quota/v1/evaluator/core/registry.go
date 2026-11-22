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

package core

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/dynamic-resource-allocation/deviceclass/extendedresourcecache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/clock"
)

// legacyObjectCountAliases 是旧版对象计数配额的别名映射。
// 这些资源类型使用简单的对象计数评估器，不需要特殊的配额逻辑。
var legacyObjectCountAliases = map[schema.GroupVersionResource]corev1.ResourceName{
	corev1.SchemeGroupVersion.WithResource("configmaps"):             corev1.ResourceConfigMaps,
	corev1.SchemeGroupVersion.WithResource("resourcequotas"):         corev1.ResourceQuotas,
	corev1.SchemeGroupVersion.WithResource("replicationcontrollers"): corev1.ResourceReplicationControllers,
	corev1.SchemeGroupVersion.WithResource("secrets"):                corev1.ResourceSecrets,
}

// NewEvaluators 创建所有配额评估器的列表。
// 根据启用的 API 资源创建对应的评估器：
//   - Pod：特殊的资源计算和 scope 匹配逻辑
//   - Service：NodePort 和 LoadBalancer 计数
//   - PVC：存储配额，支持按 StorageClass 细分
//   - ResourceClaim：DRA 设备配额（需启用 DynamicResourceAllocation）
//   - ConfigMap/Secret/RC/ResourceQuota：简单的对象计数
func NewEvaluators(f quota.ListerForResourceFunc, i informers.SharedInformerFactory, isEnabled func(schema.GroupVersionResource) bool) ([]quota.Evaluator, error) {
	if isEnabled == nil {
		isEnabled = func(schema.GroupVersionResource) bool { return true }
	}

	// 具有特殊配额逻辑的评估器
	result := []quota.Evaluator{}
	if isEnabled(corev1.SchemeGroupVersion.WithResource("pods")) {
		result = append(result, NewPodEvaluator(f, clock.RealClock{}))
	}
	if isEnabled(corev1.SchemeGroupVersion.WithResource("services")) {
		result = append(result, NewServiceEvaluator(f))
	}
	if isEnabled(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")) {
		result = append(result, NewPersistentVolumeClaimEvaluator(f))
	}
	// ResourceClaim 评估器需要启用 DynamicResourceAllocation 特性
	if isEnabled(resourcev1.SchemeGroupVersion.WithResource("resourceclaims")) && utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		var claimGetter resourceClaimPodOwnerGetter
		var podLister corev1listers.PodLister
		var deviceClassMapping *extendedresourcecache.ExtendedResourceCache
		// DRAExtendedResource 特性需要额外的设备类映射和 Pod 验证
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
			podLister = i.Core().V1().Pods().Lister()
			logger := klog.FromContext(context.Background())
			deviceClassMapping = extendedresourcecache.NewExtendedResourceCache(logger)
			if _, err := i.Resource().V1().DeviceClasses().Informer().AddEventHandler(deviceClassMapping); err != nil {
				return nil, fmt.Errorf("failed to add device class informer event handler: %w", err)
			}
			var err error
			claimGetter, err = makeResourceClaimPodOwnerGetter(i.Resource().V1().ResourceClaims())
			if err != nil {
				return nil, err
			}
		}
		result = append(result, NewResourceClaimEvaluator(f, deviceClassMapping, podLister, claimGetter))
	}

	// 简单对象计数评估器，为向后兼容使用别名
	for gvr, alias := range legacyObjectCountAliases {
		result = append(result,
			generic.NewObjectCountEvaluator(gvr.GroupResource(), generic.ListResourceUsingListerFunc(f, gvr), alias))
	}
	return result, nil
}
