/*
Copyright 2023 The Kubernetes Authors.

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

package job

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

var (
	validSelector = &metav1.LabelSelector{
		MatchLabels: map[string]string{"a": "b"},
	}

	validPodTemplate = core.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: validSelector.MatchLabels,
		},
		Spec: core.PodSpec{
			RestartPolicy: core.RestartPolicyOnFailure,
			DNSPolicy:     core.DNSClusterFirst,
			Containers:    []core.Container{{Name: "abc", Image: "image", ImagePullPolicy: "IfNotPresent", TerminationMessagePolicy: core.TerminationMessageReadFile}},
		},
	}
)

// TestWarningsForJobSpec 验证 WarningsForJobSpec 只在真正需要时给出警告：
// 普通 NonIndexed Job 即使并发和完成数很大也不告警；Indexed Job 仅在完成数与
// 并发数同时超过软上限时告警；PodTemplate 本身的问题则复用 pod 层的检查。
func TestWarningsForJobSpec(t *testing.T) {
	cases := map[string]struct {
		spec              *batch.JobSpec
		wantWarningsCount int
	}{
		// 合法的 NonIndexed 配置：不应产生任何警告。
		"valid NonIndexed": {
			spec: &batch.JobSpec{
				CompletionMode: ptr.To(batch.NonIndexedCompletion),
				Template:       validPodTemplate,
			},
		},
		// NonIndexed 模式下大数值不触发 Indexed 特有的软上限告警。
		"NondIndexed with high completions and parallelism": {
			spec: &batch.JobSpec{
				CompletionMode: ptr.To(batch.NonIndexedCompletion),
				Template:       validPodTemplate,
				Parallelism:    ptr.To[int32](1_000_000_000),
				Completions:    ptr.To[int32](1_000_000_000),
			},
		},
		// PodTemplate 中引用了空名字的 imagePullSecret，pod 层应报一条警告。
		"invalid PodTemplate": {
			spec: &batch.JobSpec{
				CompletionMode: ptr.To(batch.NonIndexedCompletion),
				Template: core.PodTemplateSpec{
					Spec: core.PodSpec{ImagePullSecrets: []core.LocalObjectReference{{Name: ""}}},
				},
			},
			wantWarningsCount: 1,
		},
		// Indexed 模式下完成数和并发数都低于软上限，不告警。
		"valid Indexed low completions low parallelism": {
			spec: &batch.JobSpec{
				CompletionMode: ptr.To(batch.IndexedCompletion),
				Completions:    ptr.To[int32](10_000),
				Parallelism:    ptr.To[int32](10_000),
				Template:       validPodTemplate,
			},
		},
		// 完成数很高但并发很低：只有两个指标同时超标才告警，此处不告警。
		"valid Indexed high completions low parallelism": {
			spec: &batch.JobSpec{
				CompletionMode: ptr.To(batch.IndexedCompletion),
				Completions:    ptr.To[int32](1000_000_000),
				Parallelism:    ptr.To[int32](10_000),
				Template:       validPodTemplate,
			},
		},
		// 两个指标都处于中等水平（未超过软上限），不告警。
		"valid Indexed medium completions medium parallelism": {
			spec: &batch.JobSpec{
				CompletionMode: ptr.To(batch.IndexedCompletion),
				Completions:    ptr.To[int32](100_000),
				Parallelism:    ptr.To[int32](100_000),
				Template:       validPodTemplate,
			},
		},
		// 完成数刚超过 10^5、并发刚超过 10^4：同时越界，应产生一条告警。
		"invalid Indexed high completions high parallelism": {
			spec: &batch.JobSpec{
				CompletionMode: ptr.To(batch.IndexedCompletion),
				Completions:    ptr.To[int32](100_001),
				Parallelism:    ptr.To[int32](10_001),
				Template:       validPodTemplate,
			},
			wantWarningsCount: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			warnings := WarningsForJobSpec(ctx, nil, tc.spec, nil)
			// 只检查警告数量：具体文案由生成方保证，这里关注触发条件是否正确。
			if len(warnings) != tc.wantWarningsCount {
				t.Errorf("Got %d warnings, want %d.\nWarnings: %v", len(warnings), tc.wantWarningsCount, warnings)
			}
		})
	}
}
