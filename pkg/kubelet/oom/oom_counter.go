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

package oom

import "sync"

// oomEventCounts 按 cgroup 名称累计每个容器观察到的 OOM kill 次数。
// OOM watcher 写入，kubelet metrics server 读取，用于生成 container_oom_events_total 指标。
//
// 说明：这里使用进程级全局变量，因为 OOM watcher 是单例。若将来存在多个 watcher，
// 应改为向 NewWatcher 和 metrics server 显式传入计数器。
//
// 说明：条目从不删除，因此每个曾发生 OOM 的 cgroup 都会让 map 增长一项。
// OOM kill 很少见且 cgroup 名按容器唯一，所以这是缓慢泄漏，而不是过期指标
// （collector 只输出存活容器的数据）。如有需要，可对照存活容器集合进行清理。
var oomEventCounts = struct {
	sync.Mutex
	byContainer map[string]uint64
}{byContainer: make(map[string]uint64)}

// recordOOMKill 递增指定 cgroup 名称的 OOM kill 计数。
func recordOOMKill(containerName string) {
	oomEventCounts.Lock()
	defer oomEventCounts.Unlock()
	oomEventCounts.byContainer[containerName]++
}

// OOMEventsForContainer 返回指定 cgroup 名称观察到的 OOM kill 次数。
// 未知容器或 OOM watcher 未运行的平台返回 0。
func OOMEventsForContainer(containerName string) uint64 {
	oomEventCounts.Lock()
	defer oomEventCounts.Unlock()
	return oomEventCounts.byContainer[containerName]
}
