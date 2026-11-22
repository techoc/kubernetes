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

// Package memory 是 discovery/cached/memory 的历史入口别名包。
//
// 早期调用方从该路径导入内存缓存发现客户端；实现已迁移到
// discovery/cached/memory，此处仅保留转发以免破坏既有导入。
package memory

import (
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
)

// NewMemCacheClient 是 DEPRECATED：请直接使用 memory.NewMemCacheClient。
//
// 保留该函数只为兼容从旧导入路径编译的代码，行为与目标函数完全一致。
func NewMemCacheClient(delegate discovery.DiscoveryInterface) discovery.CachedDiscoveryInterface {
	// 直接委托给新包，避免在同一进程内产生两套缓存实现。
	return memory.NewMemCacheClient(delegate)
}

// ErrCacheNotFound 是 DEPRECATED：请直接使用 memory.ErrCacheNotFound。
//
// 通过变量别名共享同一个错误值，调用方仍可用 errors.Is 与内存缓存包比较。
var ErrCacheNotFound = memory.ErrCacheNotFound
