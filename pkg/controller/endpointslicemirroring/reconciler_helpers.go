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

package endpointslicemirroring

import (
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	endpointsliceutil "k8s.io/endpointslice/util"
)

// slicesByAction 按操作类型分组保存待创建、待更新和待删除的 EndpointSlice 列表。
type slicesByAction struct {
	toCreate, toUpdate, toDelete []*discovery.EndpointSlice
}

// append 将另一个 slicesByAction 中的各类切片合并到当前结构中。
func (s *slicesByAction) append(slices slicesByAction) {
	s.toCreate = append(s.toCreate, slices.toCreate...)
	s.toUpdate = append(s.toUpdate, slices.toUpdate...)
	s.toDelete = append(s.toDelete, slices.toDelete...)
}

// totalsByAction 统计新增、更新和删除的数量。
type totalsByAction struct {
	added, updated, removed int
}

// add 将另一个 totalsByAction 中的计数累加到当前结构中。
func (t *totalsByAction) add(totals totalsByAction) {
	t.added += totals.added
	t.updated += totals.updated
	t.removed += totals.removed
}

// newDesiredCalc 初始化并返回一个新的 desiredCalc。
func newDesiredCalc() *desiredCalc {
	return &desiredCalc{
		portsByKey:          map[addrTypePortMapKey][]discovery.EndpointPort{},
		endpointsByKey:      map[addrTypePortMapKey]endpointsliceutil.EndpointSet{},
		numDesiredEndpoints: 0,
	}
}

// desiredCalc 辅助计算期望的 Endpoint 和端口，按地址类型与端口组合分别归组。
type desiredCalc struct {
	portsByKey          map[addrTypePortMapKey][]discovery.EndpointPort
	endpointsByKey      map[addrTypePortMapKey]endpointsliceutil.EndpointSet
	numDesiredEndpoints int
}

// multiAddrTypePortMapKey 为不同地址类型分别保存对应的 addrTypePortMapKey。
type multiAddrTypePortMapKey map[discovery.AddressType]addrTypePortMapKey

// initPorts 为某个子集和地址类型初始化端口，并返回对应的 multiAddrTypePortMapKey。
// 同一组端口会同时为 IPv4 和 IPv6 建立映射，便于后续按地址类型查找。
func (d *desiredCalc) initPorts(subsetPorts []v1.EndpointPort) multiAddrTypePortMapKey {
	endpointPorts := epPortsToEpsPorts(subsetPorts)
	addrTypes := []discovery.AddressType{discovery.AddressTypeIPv4, discovery.AddressTypeIPv6}
	multiKey := multiAddrTypePortMapKey{}

	for _, addrType := range addrTypes {
		multiKey[addrType] = newAddrTypePortMapKey(endpointPorts, addrType)
		if _, ok := d.endpointsByKey[multiKey[addrType]]; !ok {
			d.endpointsByKey[multiKey[addrType]] = endpointsliceutil.EndpointSet{}
		}
		d.portsByKey[multiKey[addrType]] = endpointPorts
	}

	return multiKey
}

// addAddress 在地址有效时将其加入期望状态，地址无效时返回 false。
func (d *desiredCalc) addAddress(address v1.EndpointAddress, multiKey multiAddrTypePortMapKey, ready bool) bool {
	endpoint, addrType := addressToEndpoint(address, ready)
	if addrType == nil {
		return false
	}

	d.endpointsByKey[multiKey[*addrType]].Insert(endpoint)
	d.numDesiredEndpoints++
	return true
}

// slicesByAddrType 按地址类型分组保存 EndpointSlice 列表。
type slicesByAddrType map[discovery.AddressType][]*discovery.EndpointSlice

// recycleSlices 回收待删除的切片，用待创建切片替换它们，从而把删除改为更新。
// 相同地址类型下配对复用已有切片名称，避免删除后再创建带来的额外开销与抖动。
func recycleSlices(slices *slicesByAction) {
	toCreateByAddrType := toSlicesByAddrType(slices.toCreate)
	toDeleteByAddrType := toSlicesByAddrType(slices.toDelete)

	for addrType, slicesToCreate := range toCreateByAddrType {
		slicesToDelete := toDeleteByAddrType[addrType]
		for i := 0; len(slicesToCreate) > i && len(slicesToDelete) > i; i++ {
			slices.toCreate = removeSlice(slices.toCreate, slicesToCreate[i])
			slices.toDelete = removeSlice(slices.toDelete, slicesToDelete[i])
			slice := slicesToCreate[i]
			slice.Name = slicesToDelete[i].Name
			slices.toUpdate = append(slices.toUpdate, slice)
		}
	}
}

// removeSlice 从 EndpointSlice 列表中移除名称匹配的切片。
func removeSlice(slices []*discovery.EndpointSlice, sliceToRemove *discovery.EndpointSlice) []*discovery.EndpointSlice {
	for i, slice := range slices {
		if slice.Name == sliceToRemove.Name {
			return append(slices[:i], slices[i+1:]...)
		}
	}
	return slices
}

// toSlicesByAddrType 按地址类型对 EndpointSlice 列表进行分组。
func toSlicesByAddrType(slices []*discovery.EndpointSlice) slicesByAddrType {
	byAddrType := slicesByAddrType{}
	for _, slice := range slices {
		byAddrType[slice.AddressType] = append(byAddrType[slice.AddressType], slice)
	}
	return byAddrType
}
