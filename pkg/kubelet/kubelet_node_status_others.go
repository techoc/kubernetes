//go:build !windows && !linux

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

package kubelet

// getOSSpecificLabels 返回操作系统相关的节点标签；非 Linux/Windows 平台没有此类标签。
func getOSSpecificLabels() (map[string]string, error) {
	return nil, nil
}

// runningInUserNS 在 kubelet 运行于用户命名空间时返回指向 true 的指针；其他平台无法判断，返回 nil。
func (kl *Kubelet) runningInUserNS() *bool {
	return nil
}
