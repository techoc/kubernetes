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

// 本文件覆盖 labels.go 里的 label 取值函数：这些 key 来自容器运行时上报的标签，
// 拼错或大小写不符都会让容器/Pod 元数据丢失。

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGetContainerName 校验按约定 key 取容器名，防止标签常量漂移。
func TestGetContainerName(t *testing.T) {
	var cases = []struct {
		labels        map[string]string
		containerName string
	}{
		{
			labels: map[string]string{
				"io.kubernetes.container.name": "c1",
			},
			containerName: "c1",
		},
		{
			labels: map[string]string{
				"io.kubernetes.container.name": "c2",
			},
			containerName: "c2",
		},
	}
	for _, data := range cases {
		containerName := GetContainerName(data.labels)
		assert.Equal(t, data.containerName, containerName)
	}
}

// TestGetPodName 确保 Pod 名能从运行时标签中原样取回。
func TestGetPodName(t *testing.T) {
	var cases = []struct {
		labels  map[string]string
		podName string
	}{
		{
			labels: map[string]string{
				"io.kubernetes.pod.name": "p1",
			},
			podName: "p1",
		},
		{
			labels: map[string]string{
				"io.kubernetes.pod.name": "p2",
			},
			podName: "p2",
		},
	}
	for _, data := range cases {
		podName := GetPodName(data.labels)
		assert.Equal(t, data.podName, podName)
	}
}

// TestGetPodUID 校验 UID 标签的 key，UID 是跨重建区分 Pod 的唯一依据。
func TestGetPodUID(t *testing.T) {
	var cases = []struct {
		labels map[string]string
		podUID string
	}{
		{
			labels: map[string]string{
				"io.kubernetes.pod.uid": "uid1",
			},
			podUID: "uid1",
		},
		{
			labels: map[string]string{
				"io.kubernetes.pod.uid": "uid2",
			},
			podUID: "uid2",
		},
	}
	for _, data := range cases {
		podUID := GetPodUID(data.labels)
		assert.Equal(t, data.podUID, podUID)
	}
}

// TestGetPodNamespace 校验命名空间标签的 key，缺了它会让同名 Pod 混淆。
func TestGetPodNamespace(t *testing.T) {
	var cases = []struct {
		labels       map[string]string
		podNamespace string
	}{
		{
			labels: map[string]string{
				"io.kubernetes.pod.namespace": "ns1",
			},
			podNamespace: "ns1",
		},
		{
			labels: map[string]string{
				"io.kubernetes.pod.namespace": "ns2",
			},
			podNamespace: "ns2",
		},
	}
	for _, data := range cases {
		podNamespace := GetPodNamespace(data.labels)
		assert.Equal(t, data.podNamespace, podNamespace)
	}
}
