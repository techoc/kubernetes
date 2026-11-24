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

package allocator

// Interface 定义线程安全的整数资源池：支持指定值/自动分配、释放、遍历、查询和剩余容量。
// Interface 定义资源范围分配契约；所有实现都必须支持并发安全调用。
type Interface interface {
	Allocate(int) (bool, error)
	AllocateNext() (int, bool, error)
	Release(int) error
	ForEach(func(int))
	Has(int) bool
	Free() int

	// Destroy shuts down all internal structures.
	// Destroy needs to be implemented in thread-safe way and be prepared for being
	// called more than once.
	Destroy()
}

// Snapshottable 在普通分配接口上增加快照导出与恢复能力，使内存位图可持久化并在重启后重建。
// Snapshottable 扩展 Interface，支持导出快照和从快照恢复；实现同样必须保证线程安全。
type Snapshottable interface {
	Interface
	Snapshot() (string, []byte)
	Restore(string, []byte) error
}

type AllocatorFactory func(max int, rangeSpec string) (Interface, error)

type AllocatorWithOffsetFactory func(max int, rangeSpec string, offset int) (Interface, error)
