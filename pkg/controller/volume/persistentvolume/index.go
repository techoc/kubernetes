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

package persistentvolume

import (
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/storage/volume"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume/util"
)

// persistentVolumeOrderedIndex 是一个 cache.Store，按 AccessModes 对持久卷
// 建立索引。
type persistentVolumeOrderedIndex struct {
	store cache.Indexer
}

// newPersistentVolumeOrderedIndex 创建一个按访问模式索引的持久卷索引。
func newPersistentVolumeOrderedIndex() persistentVolumeOrderedIndex {
	return persistentVolumeOrderedIndex{cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{"accessmodes": accessModesIndexFunc})}
}

// accessModesIndexFunc 是索引函数，将持久卷的 AccessModes 转成字符串作为索引键。
func accessModesIndexFunc(obj interface{}) ([]string, error) {
	if pv, ok := obj.(*v1.PersistentVolume); ok {
		modes := v1helper.GetAccessModesAsString(pv.Spec.AccessModes)
		return []string{modes}, nil
	}
	return []string{""}, fmt.Errorf("object is not a persistent volume: %v", obj)
}

// listByAccessModes 返回具有给定 AccessModeTypes 集合的所有卷。
// 注意：返回的列表未经排序！
func (pvIndex *persistentVolumeOrderedIndex) listByAccessModes(modes []v1.PersistentVolumeAccessMode) ([]*v1.PersistentVolume, error) {
	pv := &v1.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			AccessModes: modes,
		},
	}

	objs, err := pvIndex.store.Index("accessmodes", pv)
	if err != nil {
		return nil, err
	}

	volumes := make([]*v1.PersistentVolume, len(objs))
	for i, obj := range objs {
		volumes[i] = obj.(*v1.PersistentVolume)
	}

	return volumes, nil
}

// findByClaim 从有序列表中返回与 PVC 最匹配的 PV，若无匹配则返回 nil。
func (pvIndex *persistentVolumeOrderedIndex) findByClaim(claim *v1.PersistentVolumeClaim, delayBinding bool) (*v1.PersistentVolume, error) {
	// PV 按其访问模式建立索引以便查找。每个索引是一组访问模式的字符串表示。
	// 可能的模式组合数量有限，每个 PV 只会落入其中一个索引（与其模式匹配的那个）。
	//
	// 资源请求总会指定期望的访问模式。匹配的 PV 必须至少包含请求的那些模式，
	// 也可以更多。例如用户请求 ReadWriteOnce，而可用的 GCEPD 支持
	// ReadWriteOnce+ReadOnlyMany。
	//
	// 由于查找按模式集合进行，除了完全匹配的模式外，还要尝试潜在匹配
	// （即上面的 GCEPD 情形）。
	allPossibleModes := pvIndex.allPossibleMatchingAccessModes(claim.Spec.AccessModes)

	for _, modes := range allPossibleModes {
		volumes, err := pvIndex.listByAccessModes(modes)
		if err != nil {
			return nil, err
		}

		bestVol, err := volume.FindMatchingVolume(claim, volumes, nil /* 用于拓扑绑定的节点 */, nil /* 排除映射 */, delayBinding, utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass))
		if err != nil {
			return nil, err
		}

		if bestVol != nil {
			return bestVol, nil
		}
	}
	return nil, nil
}

// findBestMatchForClaim 是便捷方法，根据 PVC 的 AccessModes 和存储请求查找卷。
func (pvIndex *persistentVolumeOrderedIndex) findBestMatchForClaim(claim *v1.PersistentVolumeClaim, delayBinding bool) (*v1.PersistentVolume, error) {
	return pvIndex.findByClaim(claim, delayBinding)
}

// allPossibleMatchingAccessModes 返回所有能满足用户请求模式的 AccessMode 组合。
//
// 关于索引的说明见上面 findByClaim 的注释。
//
// 本函数从索引中取出所有字符串化的访问模式，返回其中至少包含全部请求模式的
// 那些组合。
//
// 例如，假设索引中有两类 PV，其字符串化访问模式分别为：
//
// "RWO,ROX" —— 若干个 GCEPD
// "RWO,ROX,RWX" —— 若干个 NFS 卷
//
// 请求 RWO 时两组索引卷都能满足，因此 allPossibleMatchingAccessModes 返回：
//
//	[][]v1.PersistentVolumeAccessMode {
//	     []v1.PersistentVolumeAccessMode {
//				v1.ReadWriteOnce, v1.ReadOnlyMany,
//			},
//	     []v1.PersistentVolumeAccessMode {
//				v1.ReadWriteOnce, v1.ReadOnlyMany, v1.ReadWriteMany,
//			},
//	}
//
// 请求 RWX 时只有一组能满足，因此返回：
//
//	[][]v1.PersistentVolumeAccessMode {
//	     []v1.PersistentVolumeAccessMode {
//				v1.ReadWriteOnce, v1.ReadOnlyMany, v1.ReadWriteMany,
//			},
//	}
//
// 本函数按模式数量递增返回，以便给用户提供最接近其实际请求的模式组合。
func (pvIndex *persistentVolumeOrderedIndex) allPossibleMatchingAccessModes(requestedModes []v1.PersistentVolumeAccessMode) [][]v1.PersistentVolumeAccessMode {
	matchedModes := [][]v1.PersistentVolumeAccessMode{}
	keys := pvIndex.store.ListIndexFuncValues("accessmodes")
	for _, key := range keys {
		indexedModes := v1helper.GetAccessModesFromString(key)
		if util.ContainsAllAccessModes(indexedModes, requestedModes) {
			matchedModes = append(matchedModes, indexedModes)
		}
	}

	// 按每组模式的数量排序，模式最少的排在前面，这样优先用所需模式最少的
	// 候选去查找卷。
	sort.Sort(byAccessModes{matchedModes})
	return matchedModes
}

// byAccessModes 用于按访问模式组合的大小排序，模式最少的排在前面。
type byAccessModes struct {
	modes [][]v1.PersistentVolumeAccessMode
}

// Less 判断第 i 组模式数量是否小于第 j 组。
func (c byAccessModes) Less(i, j int) bool {
	return len(c.modes[i]) < len(c.modes[j])
}

// Swap 交换第 i 组与第 j 组模式。
func (c byAccessModes) Swap(i, j int) {
	c.modes[i], c.modes[j] = c.modes[j], c.modes[i]
}

// Len 返回模式组合的数量。
func (c byAccessModes) Len() int {
	return len(c.modes)
}

// claimToClaimKey 将 PVC 转换为 namespace/name 形式的键。
func claimToClaimKey(claim *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", claim.Namespace, claim.Name)
}

// claimrefToClaimKey 将 PVC 的 ObjectReference 转换为 namespace/name 形式的键。
func claimrefToClaimKey(claimref *v1.ObjectReference) string {
	return fmt.Sprintf("%s/%s", claimref.Namespace, claimref.Name)
}
