/*
Copyright 2025 The Kubernetes Authors.

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

package dynamicresources

import (
	"fmt"
	"iter"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
)

// claimStore 管理 Pod 的所有 ResourceClaim：包括用户直接创建的（ResourceClaim）、
// 通过模板间接创建的（ResourceClaimTemplate），以及调度器为扩展资源生成的特殊 Claim。
//
// 零值可用且表示空集合。
type claimStore struct {
	// claims 保存所有用户持有的 Claim，末尾可选地跟着扩展资源的特殊 Claim。
	claims []*resourceapi.ResourceClaim
	// bindTo 列出每个 Claim 应绑定到的对象（Pod 或其 PodGroup）。
	// PodGroup Claim 特性关闭时该列表始终为空。
	bindTo []resourceapi.ResourceClaimConsumerReference
	// numUserOwned 是用户持有的 Claim 数量，不含末尾的特殊 Claim。
	numUserOwned int
	// initialExtendedResourceClaimUID 是 PreFilter 阶段生成的扩展资源 Claim 的临时 UID。
	// PreBind 阶段把内存中的临时 Claim 写入 API 服务器后，API 服务器会分配新的 UID，
	// 因此两者不同。
	initialExtendedResourceClaimUID types.UID
}

// newClaimStore 保存用户持有的 Claim 列表以及可选的调度器持有的扩展资源 Claim。
func newClaimStore(claims []*resourceapi.ResourceClaim, extendedResourceClaim *resourceapi.ResourceClaim, claimBindings []resourceapi.ResourceClaimConsumerReference) claimStore {
	cs := claimStore{
		claims:                          claims,
		bindTo:                          claimBindings,
		numUserOwned:                    len(claims),
		initialExtendedResourceClaimUID: "",
	}
	if extendedResourceClaim != nil {
		cs.claims = append(cs.claims, extendedResourceClaim)
		cs.initialExtendedResourceClaimUID = extendedResourceClaim.UID
	}
	return cs
}

// empty 在没有任何 ResourceClaim 时返回 true。
func (cs *claimStore) empty() bool {
	return len(cs.claims) == 0
}

// len 返回 Claim 总数，包括用户持有和调度器持有的。
func (cs *claimStore) len() int {
	return len(cs.claims)
}

// all 返回遍历所有 Claim（用户或调度器持有）的迭代器。
// 扩展资源的特殊 Claim 若存在，总是排在最后。
func (cs *claimStore) all() iter.Seq2[int, *resourceapi.ResourceClaim] {
	return func(yield func(int, *resourceapi.ResourceClaim) bool) {
		for i, claim := range cs.claims {
			if !yield(i, claim) {
				return
			}
		}
	}
}

// allUserClaims 返回仅遍历用户持有 Claim 的迭代器，排除扩展资源的特殊 Claim。
func (cs *claimStore) allUserClaims() iter.Seq2[int, *resourceapi.ResourceClaim] {
	return func(yield func(int, *resourceapi.ResourceClaim) bool) {
		for i, claim := range cs.claims[0:cs.numUserOwned] {
			if !yield(i, claim) {
				return
			}
		}
	}
}

// toAllocate 返回遍历所有尚未分配（Status.Allocation 为空）Claim 的迭代器。
// 产出的索引是 Claim 在底层存储中的原始下标，可能不连续。
func (cs *claimStore) toAllocate() iter.Seq2[int, *resourceapi.ResourceClaim] {
	return func(yield func(int, *resourceapi.ResourceClaim) bool) {
		for i, claim := range cs.claims {
			if claim.Status.Allocation != nil {
				continue
			}
			if !yield(i, claim) {
				return
			}
		}
	}
}

// extendedResourceClaim 返回扩展资源的特殊 Claim，不存在时返回 nil。
// 返回的 Claim 是只读的，修改前必须先克隆。
func (cs *claimStore) extendedResourceClaim() *resourceapi.ResourceClaim {
	if cs.numUserOwned == len(cs.claims) {
		return nil
	}
	return cs.claims[cs.numUserOwned]
}

// noUserClaim 在没有用户持有的 Claim 时返回 true。
func (cs *claimStore) noUserClaim() bool {
	return cs.numUserOwned == 0
}

// get 返回指定下标处的 Claim。
func (cs claimStore) get(i int) *resourceapi.ResourceClaim {
	return cs.claims[i]
}

// set 用给定 Claim 替换内部切片中指定下标处的元素。
func (cs claimStore) set(i int, c *resourceapi.ResourceClaim) {
	cs.claims[i] = c
}

// getBinding 返回指定下标的 Claim 应绑定到的对象引用（Pod 或其 PodGroup）。
// 扩展资源 Claim 始终绑定到 Pod。
func (cs claimStore) getBinding(i int, pod *v1.Pod) resourceapi.ResourceClaimConsumerReference {
	if i >= len(cs.bindTo) {
		return resourceapi.ResourceClaimConsumerReference{
			APIGroup: v1.GroupName,
			Resource: "pods",
			Name:     pod.Name,
			UID:      pod.UID,
		}
	}
	return cs.bindTo[i]
}

// updateExtendedResourceClaim 用给定 Claim 替换内部的扩展资源 Claim。
// 若内部存储中不存在扩展资源 Claim，则返回错误。
func (cs *claimStore) updateExtendedResourceClaim(c *resourceapi.ResourceClaim) error {
	if cs.numUserOwned == len(cs.claims) {
		return fmt.Errorf("no extended resource claim")
	}
	cs.claims[cs.numUserOwned] = c
	return nil
}

// getInitialExtendedResourceClaimUID 返回 PreBind 在 API 服务器创建真实 Claim 之前
// 所使用的临时 Claim UID。仅当扩展资源 Claim 存在时才能调用。
func (cs claimStore) getInitialExtendedResourceClaimUID() types.UID {
	return cs.initialExtendedResourceClaimUID
}
