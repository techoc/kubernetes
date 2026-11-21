//go:build windows

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

package eviction

// DefaultEvictionHard 是 Windows 上硬驱逐的默认阈值配置。
// 注意：Linux 上 memory.available 的默认硬驱逐阈值是 100Mi，但 Windows 通常需要为系统进程
// 预留更多内存，因此这里将默认阈值设为 500Mi。
var DefaultEvictionHard = map[string]string{
	"memory.available":  "500Mi",
	"nodefs.available":  "10%",
	"imagefs.available": "15%",
}
