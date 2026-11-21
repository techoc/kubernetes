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

package pullmanager

import (
	"hash/fnv"
	"sync"
)

const (
	// minStripedLockSetSize 是分段锁集合的最小规模。
	minStripedLockSetSize int32 = 1
	// maxStripedLockSetSize 是分段锁集合的最大规模。
	maxStripedLockSetSize int32 = 31
)

// StripedLockSet 按字符串 key 映射到固定大小的锁数组，实现分段加锁。
type StripedLockSet struct {
	locks []sync.Mutex
	size  int32
}

// NewStripedLockSet 创建包含 size 个锁的 StripedLockSet，用于按字符串 key 分段加锁。
// size 会被归一化到 <1, 31> 区间。
func NewStripedLockSet(size int32) *StripedLockSet {
	size = min(maxStripedLockSetSize, max(minStripedLockSetSize, size))

	return &StripedLockSet{
		locks: make([]sync.Mutex, size),
		size:  size,
	}
}

// Lock 获取 key 对应的分段锁。
func (s *StripedLockSet) Lock(key string) {
	s.locks[keyToID(key, s.size)].Lock()
}

// Unlock 释放 key 对应的分段锁。
func (s *StripedLockSet) Unlock(key string) {
	s.locks[keyToID(key, s.size)].Unlock()
}

// GlobalLock 依次获取所有分段锁，用于需要全局互斥的场景。
func (s *StripedLockSet) GlobalLock() {
	for i := range s.locks {
		s.locks[i].Lock()
	}
}

// GlobalUnlock 依次释放所有分段锁。
func (s *StripedLockSet) GlobalUnlock() {
	for i := range s.locks {
		s.locks[i].Unlock()
	}
}

// keyToID 用 FNV-1a 哈希把字符串 key 映射到 [0, sliceSize) 的锁下标。
func keyToID(key string, sliceSize int32) uint32 {
	h := fnv.New32()
	h.Write([]byte(key))
	return h.Sum32() % uint32(sliceSize)
}
