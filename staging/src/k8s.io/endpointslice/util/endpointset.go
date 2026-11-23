/*
Copyright 2019 The Kubernetes Authors.

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

package util

import (
	"sort"

	discovery "k8s.io/api/discovery/v1"
)

// endpointHash is used to uniquely identify endpoints. Only including addresses
// and hostnames as unique identifiers allows us to do more in place updates
// should attributes such as topology or conditions change.
// endpointHash 是由地址、hostname、目标引用组成的对象哈希，用作 EndpointSet 的键。
// 紧随其后的 endpointHashObj 定义参与身份计算的字段：Addresses 为待排序的地址列表，
// Hostname 为可选主机名，Namespace 与 Name 来自 TargetRef，用于区分同名 Pod。
type endpointHash string
type endpointHashObj struct {
	Addresses []string
	Hostname  string
	Namespace string
	Name      string
}

// hashEndpoint 计算单个 endpoint 的身份哈希，地址会先排序以保证结果稳定。
func hashEndpoint(endpoint *discovery.Endpoint) endpointHash {
	// 对地址排序，避免地址顺序不同导致哈希不同。
	sort.Strings(endpoint.Addresses)
	hashObj := endpointHashObj{Addresses: endpoint.Addresses}
	if endpoint.Hostname != nil {
		// hostname 存在时纳入哈希。
		hashObj.Hostname = *endpoint.Hostname
	}
	if endpoint.TargetRef != nil {
		// TargetRef 存在时把命名空间与名称纳入哈希。
		hashObj.Namespace = endpoint.TargetRef.Namespace
		hashObj.Name = endpoint.TargetRef.Name
	}

	// 对规范化后的对象做深度哈希得到最终键。
	return endpointHash(deepHashObjectToString(hashObj))
}

// EndpointSet provides simple methods for comparing sets of Endpoints.
// EndpointSet 是以 endpointHash 为键的 Endpoint 集合，提供增删查等基本操作。
type EndpointSet map[endpointHash]*discovery.Endpoint

// Insert adds items to the set.
// Insert 将一个或多个 endpoint 加入集合并返回集合本身。
func (s EndpointSet) Insert(items ...*discovery.Endpoint) EndpointSet {
	for _, item := range items {
		// 以 endpoint 的身份哈希作为键写入。
		s[hashEndpoint(item)] = item
	}
	return s
}

// Delete removes all items from the set.
// Delete 从集合中移除一个或多个 endpoint 并返回集合本身。
func (s EndpointSet) Delete(items ...*discovery.Endpoint) EndpointSet {
	for _, item := range items {
		// 按身份哈希删除对应条目。
		delete(s, hashEndpoint(item))
	}
	return s
}

// Has returns true if and only if item is contained in the set.
// Has 判断给定 endpoint 是否已存在于集合中。
func (s EndpointSet) Has(item *discovery.Endpoint) bool {
	_, contained := s[hashEndpoint(item)]
	return contained
}

// Returns an endpoint matching the hash if contained in the set.
// Get 返回集合中与给定 endpoint 哈希匹配的条目，不存在时返回 nil。
func (s EndpointSet) Get(item *discovery.Endpoint) *discovery.Endpoint {
	return s[hashEndpoint(item)]
}

// UnsortedList returns the slice with contents in random order.
// UnsortedList 以任意顺序返回集合中的全部 endpoint。
func (s EndpointSet) UnsortedList() []*discovery.Endpoint {
	endpoints := make([]*discovery.Endpoint, 0, len(s))
	for _, endpoint := range s {
		// 遍历 map 收集所有 endpoint，顺序不保证。
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

// Returns a single element from the set.
// PopAny 随机取出并删除集合中的一个 endpoint，集合为空时返回 false。
func (s EndpointSet) PopAny() (*discovery.Endpoint, bool) {
	for _, endpoint := range s {
		// map 遍历顺序随机，取到第一个后删除并返回。
		s.Delete(endpoint)
		return endpoint, true
	}
	return nil, false
}

// Len returns the size of the set.
// Len 返回集合中的 endpoint 数量。
func (s EndpointSet) Len() int {
	return len(s)
}
