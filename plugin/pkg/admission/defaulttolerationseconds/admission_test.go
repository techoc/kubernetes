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

package defaulttolerationseconds

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	admissiontesting "k8s.io/apiserver/pkg/admission/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
)

// TestForgivenessAdmission 验证插件会为缺失的 not-ready/unreachable NoExecute 容忍补上默认秒数，
// 且保留 Pod 已有的容忍及其顺序。
func TestForgivenessAdmission(t *testing.T) {
	// 插件的默认容忍时长，测试用例中作为期望值复用。
	var defaultTolerationSeconds int64 = 300

	// 构造指向独立副本的指针，避免多个用例共享同一地址后相互污染。
	genTolerationSeconds := func(s int64) *int64 {
		return &s
	}

	plugin, err := newHandlerForTest()
	if err != nil {
		t.Errorf("unexpected error initializing handler: %v", err)
	}
	handler := admissiontesting.WithReinvocationTesting(t, plugin)
	// NOTE: 修改本测试时需注意容忍项的顺序：断言按切片逐项比较，顺序即语义。
	tests := []struct {
		description  string
		requestedPod api.Pod
		expectedPod  api.Pod
	}{
		{
			description: "pod has no tolerations, expect add tolerations for `not-ready:NoExecute` and `unreachable:NoExecute`",
			requestedPod: api.Pod{
				Spec: api.PodSpec{},
			},
			expectedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: &defaultTolerationSeconds,
						},
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: &defaultTolerationSeconds,
						},
					},
				},
			},
		},
		{
			description: "pod has tolerations, but none is for taint `not-ready:NoExecute` or `unreachable:NoExecute`, expect add tolerations for `not-ready:NoExecute` and `unreachable:NoExecute`",
			requestedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               "foo",
							Operator:          api.TolerationOpEqual,
							Value:             "bar",
							Effect:            api.TaintEffectNoSchedule,
							TolerationSeconds: genTolerationSeconds(700),
						},
					},
				},
			},
			expectedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               "foo",
							Operator:          api.TolerationOpEqual,
							Value:             "bar",
							Effect:            api.TaintEffectNoSchedule,
							TolerationSeconds: genTolerationSeconds(700),
						},
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: &defaultTolerationSeconds,
						},
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: &defaultTolerationSeconds,
						},
					},
				},
			},
		},
		{
			description: "pod specified a toleration for taint `not-ready:NoExecute`, expect add toleration for `unreachable:NoExecute`",
			requestedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
					},
				},
			},
			expectedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: &defaultTolerationSeconds,
						},
					},
				},
			},
		},
		{
			description: "pod specified a toleration for taint `unreachable:NoExecute`, expect add toleration for `not-ready:NoExecute`",
			requestedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
					},
				},
			},
			expectedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: &defaultTolerationSeconds,
						},
					},
				},
			},
		},
		{
			description: "pod specified tolerations for both `not-ready:NoExecute` and `unreachable:NoExecute`, expect no change",
			requestedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
					},
				},
			},
			expectedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(700),
						},
					},
				},
			},
		},
		{
			description: "pod specified toleration for taint `unreachable`, expect add toleration for `not-ready:NoExecute`",
			requestedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							TolerationSeconds: genTolerationSeconds(700),
						},
					},
				},
			},
			expectedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Key:               v1.TaintNodeUnreachable,
							Operator:          api.TolerationOpExists,
							TolerationSeconds: genTolerationSeconds(700),
						},
						{
							Key:               v1.TaintNodeNotReady,
							Operator:          api.TolerationOpExists,
							Effect:            api.TaintEffectNoExecute,
							TolerationSeconds: genTolerationSeconds(300),
						},
					},
				},
			},
		},
		{
			description: "pod has wildcard toleration for all kind of taints, expect no change",
			requestedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{Operator: api.TolerationOpExists, TolerationSeconds: genTolerationSeconds(700)},
					},
				},
			},
			expectedPod: api.Pod{
				Spec: api.PodSpec{
					Tolerations: []api.Toleration{
						{
							Operator:          api.TolerationOpExists,
							TolerationSeconds: genTolerationSeconds(700),
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		// 经带重入检查的 handler 准入，确保重复调用不会再次追加容忍项。
		err := handler.Admit(context.TODO(), admission.NewAttributesRecord(&test.requestedPod, nil, api.Kind("Pod").WithVersion("version"), "foo", "name", api.Resource("pods").WithVersion("version"), "", "ignored", nil, false, nil), nil)
		if err != nil {
			t.Errorf("[%s]: unexpected error %v for pod %+v", test.description, err, test.requestedPod)
		}

		// 用语义相等而非反射相等：容忍项内指针字段的值相同即可。
		if !helper.Semantic.DeepEqual(test.expectedPod.Spec.Tolerations, test.requestedPod.Spec.Tolerations) {
			t.Errorf("[%s]: expected %#v got %#v", test.description, test.expectedPod.Spec.Tolerations, test.requestedPod.Spec.Tolerations)
		}
	}
}

// TestHandles 验证插件只处理 Pod 的创建和更新，不处理删除与连接操作。
func TestHandles(t *testing.T) {
	handler := NewDefaultTolerationSeconds()
	tests := map[admission.Operation]bool{
		admission.Update:  true,
		admission.Create:  true,
		admission.Delete:  false,
		admission.Connect: false,
	}
	for op, expected := range tests {
		result := handler.Handles(op)
		if result != expected {
			t.Errorf("Unexpected result for operation %s: %v\n", op, result)
		}
	}
}

// newHandlerForTest 返回完成初始化、可直接用于测试的 handler。
func newHandlerForTest() (*Plugin, error) {
	handler := NewDefaultTolerationSeconds()
	// 先用空依赖完成初始化，再校验插件的初始化前置条件是否满足。
	pluginInitializer := initializer.New(nil, nil, nil, nil, nil, nil, nil, nil)
	pluginInitializer.Initialize(handler)
	return handler, admission.ValidateInitialization(handler)
}
