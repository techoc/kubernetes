/*
Copyright 2024 The Kubernetes Authors.

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

package nfacct

// Counter 表示一个 nfacct 计数对象。
type Counter struct {
	Name    string
	Packets uint64
	Bytes   uint64
}

// Interface 是可注入的 nfacct 命令操作接口。
type Interface interface {
	// Ensure 检查指定名称的 nfacct 计数器是否存在，不存在则创建。
	Ensure(name string) error
	// Add 创建指定名称的 nfacct 计数器，若已存在则返回错误。
	Add(name string) error
	// Get 获取指定名称的 nfacct 计数器，不存在则返回错误。
	Get(name string) (*Counter, error)
	// List 获取 nfacct 计数器列表；可能被 unix.EINTR 中断，此时只返回部分计数器。
	List() ([]*Counter, error)
}
