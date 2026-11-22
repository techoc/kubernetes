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

package csi

import (
	"sync"

	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// Driver 描述一个 CSI 驱动：由其端点与所支持的最高 CSI 版本确定。
type Driver struct {
	endpoint                string
	highestSupportedVersion *utilversion.Version
}

// DriversStore 保存 CSI 驱动列表，并自带读写锁保证并发安全。
type DriversStore struct {
	store
	sync.RWMutex
}

// store 以驱动名为键保存 CSI 驱动。
type store map[string]Driver

// Get 按名称查询 CSI 驱动。
// 该方法由互斥锁保护。
func (s *DriversStore) Get(driverName string) (Driver, bool) {
	s.RLock()
	defer s.RUnlock()

	driver, ok := s.store[driverName]
	return driver, ok
}

// Set 以指定名称把 CSI 驱动存入列表；首次写入时才初始化底层 map。
// 该方法由互斥锁保护。
func (s *DriversStore) Set(driverName string, driver Driver) {
	s.Lock()
	defer s.Unlock()

	if s.store == nil {
		s.store = store{}
	}

	s.store[driverName] = driver
}

// Delete 按名称删除 CSI 驱动。
// 该方法由互斥锁保护。
func (s *DriversStore) Delete(driverName string) {
	s.Lock()
	defer s.Unlock()

	delete(s.store, driverName)
}

// Clear 清空存储中的所有条目。
// 该方法由互斥锁保护。
func (s *DriversStore) Clear() {
	s.Lock()
	defer s.Unlock()

	s.store = store{}
}

// List 返回所有已注册 CSI 驱动的名称。
func (s *DriversStore) List() []string {
	s.RLock()
	defer s.RUnlock()

	names := make([]string, 0, len(s.store))
	for name := range s.store {
		names = append(names, name)
	}
	return names
}
