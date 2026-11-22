/*
Copyright 2016 The Kubernetes Authors.

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

package apparmor

import (
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
)

// TestValidateBadHost 测试当主机不支持 AppArmor 时，验证器的行为。
// 验证两种情况：未指定 profile 的 Pod 应通过验证（不需要 AppArmor），
// 指定了 profile 的 Pod 应返回主机错误。
func TestValidateBadHost(t *testing.T) {
	hostErr := errors.New("expected host error")
	v := &validator{
		validateHostErr: hostErr,
	}

	tests := []struct {
		profile     string
		expectValid bool
	}{
		{"", true},
		{v1.DeprecatedAppArmorBetaProfileRuntimeDefault, false},
		{v1.DeprecatedAppArmorBetaProfileNamePrefix + "docker-default", false},
	}

	for _, test := range tests {
		err := v.Validate(getPodWithProfile(test.profile))
		if test.expectValid {
			assert.NoErrorf(t, err, "Pod with profile %q should be valid", test.profile)
		} else {
			assert.Equalf(t, hostErr, err, "Pod with profile %q should trigger a host validation error", test.profile)
		}
	}
}

// TestValidateValidHost 测试当主机支持 AppArmor 时，各种 profile 的验证结果。
// 覆盖有效 profile（runtime-default、localhost 类型）、无效 profile（空名称、纯空白）
// 以及多容器 Pod（包含 InitContainer 和普通 Container）的验证。
func TestValidateValidHost(t *testing.T) {
	v := &validator{}

	tests := []struct {
		profile     string
		expectValid bool
	}{
		{"", true},
		{v1.DeprecatedAppArmorBetaProfileRuntimeDefault, true},
		{v1.DeprecatedAppArmorBetaProfileNamePrefix + "docker-default", true},
		{v1.DeprecatedAppArmorBetaProfileNamePrefix + "foo-container", true},
		{v1.DeprecatedAppArmorBetaProfileNamePrefix + "/usr/sbin/ntpd", true},
		{v1.DeprecatedAppArmorBetaProfileNamePrefix + "", false}, // 空 profile 名称明确禁止。
		{v1.DeprecatedAppArmorBetaProfileNamePrefix + " ", false},
	}

	for _, test := range tests {
		err := v.Validate(getPodWithProfile(test.profile))
		if test.expectValid {
			assert.NoErrorf(t, err, "Pod with profile %q should be valid", test.profile)
		} else {
			assert.Errorf(t, err, "Pod with profile %q should trigger a validation error", test.profile)
		}
	}

	// 测试多容器 Pod：包含 InitContainer 和多个普通 Container，
	// 每个容器有不同的 AppArmor profile，验证应全部通过。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix + "init":  v1.DeprecatedAppArmorBetaProfileNamePrefix + "foo-container",
				v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix + "test1": v1.DeprecatedAppArmorBetaProfileRuntimeDefault,
				v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix + "test2": v1.DeprecatedAppArmorBetaProfileNamePrefix + "docker-default",
			},
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{Name: "init"},
			},
			Containers: []v1.Container{
				{Name: "test1"},
				{Name: "test2"},
				{Name: "no-profile"},
			},
		},
	}
	assert.NoError(t, v.Validate(pod), "Multi-container pod should validate")
}

// getPodWithProfile 创建一个带有指定 AppArmor profile annotation 的测试 Pod。
// 如果 profile 为空，使用无关的 annotation 代替，以测试未指定 profile 的场景。
func getPodWithProfile(profile string) *v1.Pod {
	annotations := map[string]string{
		v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix + "test": profile,
	}
	if profile == "" {
		annotations = map[string]string{
			"foo": "bar",
		}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: annotations,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "test",
				},
			},
		},
	}
}
