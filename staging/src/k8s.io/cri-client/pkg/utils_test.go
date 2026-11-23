/*
Copyright 2021 The Kubernetes Authors.

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

package cri

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// makePodSandboxMetadata 用给定的 name/namespace/uid 构造一个 PodSandboxMetadata，
// 便于测试中快速生成缺少某个字段的元数据。
func makePodSandboxMetadata(name, namespace, uid string) *runtimeapi.PodSandboxMetadata {
	return &runtimeapi.PodSandboxMetadata{
		Name:      name,
		Namespace: namespace,
		Uid:       uid,
	}
}

// TestVerifySandboxStatus 验证 verifySandboxStatus 能按顺序校验沙箱状态的各必需字段。
// 用例覆盖 Id、Metadata、Metadata 三个子字段、CreatedAt 缺失以及全部字段齐全的情形。
func TestVerifySandboxStatus(t *testing.T) {
	ct := int64(1)
	// 预先构造三种残缺元数据，分别缺少 Name、Namespace、Uid。
	metaWithoutName := makePodSandboxMetadata("", "bar", "1")
	metaWithoutNamespace := makePodSandboxMetadata("foo", "", "1")
	metaWithoutUid := makePodSandboxMetadata("foo", "bar", "")

	// 表驱动：input 为待校验状态，expected 为期望得到的错误（nil 表示应校验通过）。
	statuses := []struct {
		input    *runtimeapi.PodSandboxStatus
		expected error
	}{
		{
			input: &runtimeapi.PodSandboxStatus{
				CreatedAt: ct,
				Metadata:  makePodSandboxMetadata("foo", "bar", "1"),
			},
			expected: fmt.Errorf("status.Id is not set"),
		},
		{
			input: &runtimeapi.PodSandboxStatus{
				Id:        "1",
				CreatedAt: ct,
			},
			expected: fmt.Errorf("status.Metadata is not set"),
		},
		{
			input: &runtimeapi.PodSandboxStatus{
				Id:        "2",
				CreatedAt: ct,
				Metadata:  metaWithoutName,
			},
			expected: fmt.Errorf("metadata.Name, metadata.Namespace or metadata.Uid is not in metadata %q", metaWithoutName),
		},
		{
			input: &runtimeapi.PodSandboxStatus{
				Id:        "3",
				CreatedAt: ct,
				Metadata:  metaWithoutNamespace,
			},
			expected: fmt.Errorf("metadata.Name, metadata.Namespace or metadata.Uid is not in metadata %q", metaWithoutNamespace),
		},
		{
			input: &runtimeapi.PodSandboxStatus{
				Id:        "4",
				CreatedAt: ct,
				Metadata:  metaWithoutUid,
			},
			expected: fmt.Errorf("metadata.Name, metadata.Namespace or metadata.Uid is not in metadata %q", metaWithoutUid),
		},
		{
			input: &runtimeapi.PodSandboxStatus{
				Id:       "5",
				Metadata: makePodSandboxMetadata("foo", "bar", "1"),
			},
			expected: fmt.Errorf("status.CreatedAt is not set"),
		},
		{
			input: &runtimeapi.PodSandboxStatus{
				Id:        "6",
				CreatedAt: ct,
				Metadata:  makePodSandboxMetadata("foo", "bar", "1"),
			},
			expected: nil,
		},
	}

	// 逐个用例校验：有错误时比较错误文本，无错误时期望本身也应为 nil。
	for _, status := range statuses {
		actual := verifySandboxStatus(status.input)
		if actual != nil {
			assert.EqualError(t, actual, status.expected.Error())
		} else {
			assert.NoError(t, status.expected)
		}
	}
}

// TestVerifyContainerStatus 验证 verifyContainerStatus 对容器状态各必需字段的校验。
// 除 Id/Metadata/CreatedAt 外，还覆盖 Image 与 ImageRef 缺失的场景。
func TestVerifyContainerStatus(t *testing.T) {
	// 完整的元数据与镜像规格，以及对应的残缺版本。
	meta := &runtimeapi.ContainerMetadata{Name: "cname", Attempt: 3}
	metaWithoutName := &runtimeapi.ContainerMetadata{Attempt: 3}
	imageSpec := &runtimeapi.ImageSpec{Image: "fimage"}
	imageSpecWithoutImage := &runtimeapi.ImageSpec{}

	// 表驱动：input 为待校验状态，expected 为期望错误（nil 表示应通过）。
	statuses := []struct {
		input    *runtimeapi.ContainerStatus
		expected error
	}{
		{
			input:    &runtimeapi.ContainerStatus{},
			expected: fmt.Errorf("status.Id is not set"),
		},
		{
			input: &runtimeapi.ContainerStatus{
				Id: "1",
			},
			expected: fmt.Errorf("status.Metadata is not set"),
		},
		{
			input: &runtimeapi.ContainerStatus{
				Id:       "2",
				Metadata: metaWithoutName,
			},
			expected: fmt.Errorf("metadata.Name is not in metadata %q", metaWithoutName),
		},
		{
			input: &runtimeapi.ContainerStatus{
				Id:       "3",
				Metadata: meta,
			},
			expected: fmt.Errorf("status.CreatedAt is not set"),
		},
		{
			input: &runtimeapi.ContainerStatus{
				Id:        "4",
				Metadata:  meta,
				CreatedAt: 1,
				Image:     imageSpecWithoutImage,
			},
			expected: fmt.Errorf("status.Image is not set"),
		},
		{
			input: &runtimeapi.ContainerStatus{
				Id:        "5",
				Metadata:  meta,
				Image:     imageSpec,
				CreatedAt: 1,
			},
			expected: fmt.Errorf("status.ImageRef is not set"),
		},
		{
			input: &runtimeapi.ContainerStatus{
				Id:        "5",
				Metadata:  meta,
				Image:     imageSpec,
				CreatedAt: 1,
				ImageRef:  "Ref-1",
			},
			expected: nil,
		},
	}
	// 逐个用例校验，逻辑与 TestVerifySandboxStatus 相同。
	for _, status := range statuses {
		actual := verifyContainerStatus(status.input)
		if actual != nil {
			assert.EqualError(t, actual, status.expected.Error())
		} else {
			assert.NoError(t, status.expected)
		}
	}
}
