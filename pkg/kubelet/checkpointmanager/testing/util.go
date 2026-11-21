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

package testing

import (
	"fmt"
	"sync"
)

// MemStore 是 CheckpointStore 接口的内存实现。
type MemStore struct {
	mem map[string][]byte
	sync.Mutex
}

// NewMemStore 返回 MemStore 实例。
func NewMemStore() *MemStore {
	return &MemStore{mem: make(map[string][]byte)}
}

// Write 把数据写入内存 store。
func (mstore *MemStore) Write(key string, data []byte) error {
	mstore.Lock()
	defer mstore.Unlock()
	mstore.mem[key] = data
	return nil
}

// Read 从内存 store 读取数据，不存在时返回错误。
func (mstore *MemStore) Read(key string) ([]byte, error) {
	mstore.Lock()
	defer mstore.Unlock()
	data, ok := mstore.mem[key]
	if !ok {
		return nil, fmt.Errorf("checkpoint is not found")
	}
	return data, nil
}

// Delete 从内存 store 删除数据。
func (mstore *MemStore) Delete(key string) error {
	mstore.Lock()
	defer mstore.Unlock()
	delete(mstore.mem, key)
	return nil
}

// List 返回 store 中的所有 key。
func (mstore *MemStore) List() ([]string, error) {
	mstore.Lock()
	defer mstore.Unlock()
	keys := make([]string, 0)
	for key := range mstore.mem {
		keys = append(keys, key)
	}
	return keys, nil
}
