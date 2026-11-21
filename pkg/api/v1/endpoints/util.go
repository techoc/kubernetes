/*
Copyright 2015 The Kubernetes Authors.

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

package endpoints

import (
	"bytes"
	"encoding/hex"
	"hash"
	"hash/fnv"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
)

// RepackSubsets 把 EndpointSubset 切片展开为完整表示后再按规范布局重新打包，
// 使依赖对象比较的代码可以假定统一的形态。返回新分配的切片。
func RepackSubsets(subsets []v1.EndpointSubset) []v1.EndpointSubset {
	// 第一遍：把每个唯一的端口定义映射到提供它的地址集合。
	// 同一个地址可能出现在多个 subset 中，用 addressKey 去重。
	allAddrs := map[addressKey]*v1.EndpointAddress{}
	portToAddrReadyMap := map[v1.EndpointPort]addressSet{}
	for i := range subsets {
		if len(subsets[i].Ports) == 0 {
			// 没有端口的 endpoint 也要保留，用 Port=-1 作为哨兵键。
			mapAddressesByPort(&subsets[i], v1.EndpointPort{Port: -1}, allAddrs, portToAddrReadyMap)
		} else {
			for _, port := range subsets[i].Ports {
				mapAddressesByPort(&subsets[i], port, allAddrs, portToAddrReadyMap)
			}
		}
	}

	// 第二遍：反过来把地址集合映射到它们提供的端口集合。
	// Go 不允许用 map/slice 作为 map 的 key，
	// 所以先对地址集合做哈希，用字符串作为人工 key，
	// 形成“地址集合 ↔ 端口集合”的双向关联。
	type keyString string
	keyToAddrReadyMap := map[keyString]addressSet{}
	addrReadyMapKeyToPorts := map[keyString][]v1.EndpointPort{}
	for port, addrs := range portToAddrReadyMap {
		key := keyString(hashAddresses(addrs))
		keyToAddrReadyMap[key] = addrs
		if port.Port > 0 { // avoid sentinels
			addrReadyMapKeyToPorts[key] = append(addrReadyMapKeyToPorts[key], port)
		} else {
			if _, found := addrReadyMapKeyToPorts[key]; !found {
				// 即使没有真实端口，也要在 map 中占位，保留无端口的 endpoint。
				addrReadyMapKeyToPorts[key] = nil
			}
		}
	}

	// 第三遍：按 API 期望的形态构建 N 对 M 的 association。
	final := []v1.EndpointSubset{}
	for key, ports := range addrReadyMapKeyToPorts {
		var readyAddrs, notReadyAddrs []v1.EndpointAddress
		for addr, ready := range keyToAddrReadyMap[key] {
			if ready {
				readyAddrs = append(readyAddrs, *addr)
			} else {
				notReadyAddrs = append(notReadyAddrs, *addr)
			}
		}
		final = append(final, v1.EndpointSubset{Addresses: readyAddrs, NotReadyAddresses: notReadyAddrs, Ports: ports})
	}

	// 最后排序，保证输出形态确定。
	return SortSubsets(final)
}

// 地址集合以 IP+UID 为键去重：同一 IP 可能属于不同的 Pod（如 Mesos 场景）。
type addressKey struct {
	ip  string
	uid types.UID
}

// mapAddressesByPort 把某个 subset 中所有 ready/not-ready 地址按单个端口登记到映射中。
func mapAddressesByPort(subset *v1.EndpointSubset, port v1.EndpointPort, allAddrs map[addressKey]*v1.EndpointAddress, portToAddrReadyMap map[v1.EndpointPort]addressSet) {
	for k := range subset.Addresses {
		mapAddressByPort(&subset.Addresses[k], port, true, allAddrs, portToAddrReadyMap)
	}
	for k := range subset.NotReadyAddresses {
		mapAddressByPort(&subset.NotReadyAddresses[k], port, false, allAddrs, portToAddrReadyMap)
	}
}

// mapAddressByPort 登记单个地址与端口的关联，保留已记录的 ready 状态。
// not-ready 优先于 ready：一旦某地址对该端口曾是不就绪，后续不能改回就绪。
func mapAddressByPort(addr *v1.EndpointAddress, port v1.EndpointPort, ready bool, allAddrs map[addressKey]*v1.EndpointAddress, portToAddrReadyMap map[v1.EndpointPort]addressSet) *v1.EndpointAddress {
	// 用 addressKey 区分 IP 相同但来源不同的 endpoint（例如使用节点 IP 的 Pod），
	// 以便归属到不同的宿主。例如 Mesos 会给 Pod 分配节点 IP，
	// 但 Pod 彼此是不同的对象。
	key := addressKey{ip: addr.IP}
	if addr.TargetRef != nil {
		key.uid = addr.TargetRef.UID
	}

	// 保留完整的 EndpointAddress 结构，重建 subset 时 TargetRef 等信息才不会丢失。
	// 同一个地址只在首次出现时复制入 allAddrs。
	existingAddress := allAddrs[key]
	if existingAddress == nil {
		// Make a copy so we don't write to the
		// input args of this function.
		existingAddress = &v1.EndpointAddress{}
		*existingAddress = *addr
		allAddrs[key] = existingAddress
	}

	// 记录该端口到该地址的映射。
	if _, found := portToAddrReadyMap[port]; !found {
		portToAddrReadyMap[port] = addressSet{}
	}
	// 如果该地址此前没有记录这个端口，或之前是 ready 状态，
	// 就写入当前的 ready 状态。not-ready 优先：一旦不就绪就不能被后续的 ready 覆盖。
	//
	if wasReady, found := portToAddrReadyMap[port][existingAddress]; !found || wasReady {
		portToAddrReadyMap[port][existingAddress] = ready
	}
	return existingAddress
}

type addressSet map[*v1.EndpointAddress]bool

type addrReady struct {
	addr  *v1.EndpointAddress
	ready bool
}

// hashAddresses 把地址集合压成字符串以便作为 map key。
// DeepHashObject 基于 spew，不能很好地处理非基本类型的 map key，
// 所以先转成切片、排序，再计算哈希。
func hashAddresses(addrs addressSet) string {
	// 把地址列表展开成可哈希的字符串。
	// DeepHashObject 基于 spew，不能很好地处理非基本类型的 map key，
	// 所以先把集合转成切片、排序，再计算哈希。
	// 排序保证哈希结果与遍历顺序无关。
	slice := make([]addrReady, 0, len(addrs))
	for k, ready := range addrs {
		slice = append(slice, addrReady{k, ready})
	}
	sort.Sort(addrsReady(slice))
	hasher := fnv.New128a()
	hashutil.DeepHashObject(hasher, slice)
	return hex.EncodeToString(hasher.Sum(nil)[0:])
}

// lessAddrReady 比较两个带就绪状态的地址。
// ready 状态不参与哈希：同一地址不会重复出现。
func lessAddrReady(a, b addrReady) bool {
	// ready 不参与比较：同一地址不会重复出现，哈希只按地址本身。
	return LessEndpointAddress(a.addr, b.addr)
}

type addrsReady []addrReady

func (sl addrsReady) Len() int      { return len(sl) }
func (sl addrsReady) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl addrsReady) Less(i, j int) bool {
	return lessAddrReady(sl[i], sl[j])
}

// LessEndpointAddress 按 IP 的字节序比较地址，IP 相同再按 TargetRef.UID 比较。
func LessEndpointAddress(a, b *v1.EndpointAddress) bool {
	ipComparison := bytes.Compare([]byte(a.IP), []byte(b.IP))
	if ipComparison != 0 {
		return ipComparison < 0
	}
	if b.TargetRef == nil {
		return false
	}
	if a.TargetRef == nil {
		return true
	}
	return a.TargetRef.UID < b.TargetRef.UID
}

// SortSubsets 就地排序 EndpointSubset 切片（地址、端口各自排序后再排 subset 本身），
// 为方便使用同时返回入参切片。
func SortSubsets(subsets []v1.EndpointSubset) []v1.EndpointSubset {
	for i := range subsets {
		ss := &subsets[i]
		sort.Sort(addrsByIPAndUID(ss.Addresses))
		sort.Sort(addrsByIPAndUID(ss.NotReadyAddresses))
		sort.Sort(portsByHash(ss.Ports))
	}
	sort.Sort(subsetsByHash(subsets))
	return subsets
}

// hashObject 计算对象的深度哈希并返回字节切片。
func hashObject(hasher hash.Hash, obj interface{}) []byte {
	hashutil.DeepHashObject(hasher, obj)
	return hasher.Sum(nil)
}

type subsetsByHash []v1.EndpointSubset

func (sl subsetsByHash) Len() int      { return len(sl) }
func (sl subsetsByHash) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl subsetsByHash) Less(i, j int) bool {
	hasher := fnv.New128a()
	h1 := hashObject(hasher, sl[i])
	h2 := hashObject(hasher, sl[j])
	return bytes.Compare(h1, h2) < 0
}

type addrsByIPAndUID []v1.EndpointAddress

func (sl addrsByIPAndUID) Len() int      { return len(sl) }
func (sl addrsByIPAndUID) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl addrsByIPAndUID) Less(i, j int) bool {
	return LessEndpointAddress(&sl[i], &sl[j])
}

type portsByHash []v1.EndpointPort

func (sl portsByHash) Len() int      { return len(sl) }
func (sl portsByHash) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl portsByHash) Less(i, j int) bool {
	hasher := fnv.New128a()
	h1 := hashObject(hasher, sl[i])
	h2 := hashObject(hasher, sl[j])
	return bytes.Compare(h1, h2) < 0
}
