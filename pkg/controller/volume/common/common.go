/*
Copyright 2020 The Kubernetes Authors.

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

package common

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/storage/ephemeral"
)

const (
	// PodPVCIndex 是按 Pod 所用 PVC 建立索引的索引函数查找名。
	PodPVCIndex = "pod-pvc-index"
)

// PodPVCIndexFunc 创建索引函数，对给定 Pod 返回其 PVC 键（即 namespace/name）。
// 其中也包含可能为通用临时卷（generic ephemeral volume）创建的 PVC。
func PodPVCIndexFunc() func(obj interface{}) ([]string, error) {
	return func(obj interface{}) ([]string, error) {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			return []string{}, nil
		}
		keys := []string{}
		for _, podVolume := range pod.Spec.Volumes {
			claimName := ""
			if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
				claimName = pvcSource.ClaimName
			} else if podVolume.VolumeSource.Ephemeral != nil {
				claimName = ephemeral.VolumeClaimName(pod, &podVolume)
			}
			if claimName != "" {
				keys = append(keys, fmt.Sprintf("%s/%s", pod.Namespace, claimName))
			}
		}
		return keys, nil
	}
}

// AddPodPVCIndexerIfNotPresent 添加 PodPVCIndexFunc 索引器（若尚未添加）。
func AddPodPVCIndexerIfNotPresent(indexer cache.Indexer) error {
	return AddIndexerIfNotPresent(indexer, PodPVCIndex, PodPVCIndexFunc())
}

// AddIndexerIfNotPresent 在缓存索引器中不存在指定名称时，添加对应的索引函数。
func AddIndexerIfNotPresent(indexer cache.Indexer, indexName string, indexFunc cache.IndexFunc) error {
	indexers := indexer.GetIndexers()
	if _, ok := indexers[indexName]; ok {
		return nil
	}
	return indexer.AddIndexers(cache.Indexers{indexName: indexFunc})
}
