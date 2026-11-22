/*
Copyright 2015 The Kubernetes Authors.

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

package procfs

// FakeProcFS 是 ProcFS 的测试替身，所有查询均返回空结果。
type FakeProcFS struct{}

// GetFullContainerName 根据容器的根进程 ID 返回容器名称。
// 例如容器的 devices cgroup 位于 /sys/fs/cgroup/devices/docker/nginx 时，
// 返回 docker/nginx。假定进程只属于一个 cgroup 层级。
func (fakePfs *FakeProcFS) GetFullContainerName(pid int) (string, error) {
	return "", nil
}
