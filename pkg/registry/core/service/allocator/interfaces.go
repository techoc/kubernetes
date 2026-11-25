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

// Interface 定义线程安全的整数资源范围分配契约，支持指定值分配、自动分配、释放、遍历、查询和剩余容量查询。
type Interface interface {
	Allocate(int) (bool, error)
	AllocateNext() (int, bool, error)
	Release(int) error
	ForEach(func(int))
	Has(int) bool
	Free() int

	// Destroy 关闭分配器内部结构；实现必须线程安全，并允许被重复调用。
	Destroy()
}

// Snapshottable 扩展 Interface，支持导出快照和从快照恢复，使内存位图可持久化并在重启后重建；实现必须保证线程安全。
type Snapshottable interface {
	Interface
	Snapshot() (string, []byte)
	Restore(string, []byte) error
}

type AllocatorFactory func(max int, rangeSpec string) (Interface, error)

type AllocatorWithOffsetFactory func(max int, rangeSpec string, offset int) (Interface, error)
