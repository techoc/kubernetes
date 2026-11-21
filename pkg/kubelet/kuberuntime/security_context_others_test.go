//go:build !windows

/*
Copyright 2020 The Kubernetes Authors.

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

package kuberuntime

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestVerifyRunAsNonRoot 验证非 Windows 平台下 RunAsNonRoot 的校验：
// 容器或镜像用户为 root、用户名未知、UID 非法（超范围或负数）时应拒绝。
func TestVerifyRunAsNonRoot(t *testing.T) {
	tCtx := ktesting.Init(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "foo",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					Command:         []string{"testCommand"},
					WorkingDir:      "testWorkingDir",
				},
			},
		},
	}

	// 构造各种用户 ID 与 RunAsNonRoot 组合。
	rootUser := int64(0)
	anyUser := int64(1000)
	invalidUser := int64(2147483648)
	negativeUser := int64(-1000)
	maxInt32User := int64(math.MaxInt32)
	runAsNonRootTrue := true
	runAsNonRootFalse := false
	// 用例覆盖未设置安全上下文、显式/隐式 root、非法 UID 等场景。
	for _, test := range []struct {
		desc     string
		sc       *v1.SecurityContext
		uid      *int64
		username string
		fail     bool
	}{
		{
			desc: "Pass if SecurityContext is not set",
			sc:   nil,
			uid:  &rootUser,
			fail: false,
		},
		{
			desc: "Pass if RunAsUser is non-root and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
				RunAsUser:    &anyUser,
			},
			fail: false,
		},
		{
			desc: "Pass if RunAsNonRoot is not set",
			sc: &v1.SecurityContext{
				RunAsUser: &rootUser,
			},
			uid:  &rootUser,
			fail: false,
		},
		{
			desc: "Pass if RunAsNonRoot is false (image user is root)",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootFalse,
			},
			uid:  &rootUser,
			fail: false,
		},
		{
			desc: "Pass if RunAsNonRoot is false (RunAsUser is root)",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootFalse,
				RunAsUser:    &rootUser,
			},
			uid:  &rootUser,
			fail: false,
		},
		{
			desc: "Fail if container's RunAsUser is root and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
				RunAsUser:    &rootUser,
			},
			uid:  &rootUser,
			fail: true,
		},
		{
			desc: "Fail if image's user is root and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
			},
			uid:  &rootUser,
			fail: true,
		},
		{
			desc: "Fail if image's username is set and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
			},
			username: "test",
			fail:     true,
		},
		{
			desc: "Pass if image's user is non-root and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
			},
			uid:  &anyUser,
			fail: false,
		},
		{
			desc: "Pass if container's user and image's user aren't set and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
			},
			fail: false,
		},
		{
			desc: "Fail if image's user is invalid and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
			},
			uid:  &invalidUser,
			fail: true,
		},
		{
			desc: "Fail if image's user is negative and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
			},
			uid:  &negativeUser,
			fail: true,
		},
		{
			desc: "Pass if image's user is math.MaxInt32 and RunAsNonRoot is true",
			sc: &v1.SecurityContext{
				RunAsNonRoot: &runAsNonRootTrue,
			},
			uid:  &maxInt32User,
			fail: false,
		},
	} {
		// 逐用例设置安全上下文并校验错误是否符合预期。
		pod.Spec.Containers[0].SecurityContext = test.sc
		err := verifyRunAsNonRoot(tCtx, pod, &pod.Spec.Containers[0], test.uid, test.username)
		if test.fail {
			assert.Error(t, err, test.desc)
		} else {
			assert.NoError(t, err, test.desc)
		}
	}
}
