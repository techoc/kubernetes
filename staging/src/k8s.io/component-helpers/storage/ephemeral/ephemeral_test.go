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

package ephemeral

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestVolumeIsForPod 验证 VolumeIsForPod 用 UID 而非名字来判定所有权。
// 用递增的 UID 生成器而不是硬编码字符串，是为了让每个对象身份唯一，
// 避免测试因为名字相同而意外通过；同时显式构造“同为 controller 但 UID 不同”的情况，
// 确保不会把同名 Pod 误认成所有者。
func TestVolumeIsForPod(t *testing.T) {
	uid := 0
	newUID := func() types.UID {
		uid++
		return types.UID(fmt.Sprintf("%d", uid))
	}
	isController := true

	podNotOwner := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podNotOwner",
			UID:       newUID(),
		},
	}
	podOwner := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "podOwner",
			UID:       newUID(),
		},
	}
	pvcNoOwner := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "pvcNoOwner",
			UID:       newUID(),
		},
	}
	pvcWithOwner := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "pvcNoOwner",
			UID:       newUID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					UID:        podOwner.UID,
					Controller: &isController,
				},
			},
		},
	}
	userPVCWithOwner := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "user-namespace",
			Name:      "userPVCWithOwner",
			UID:       newUID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					UID:        podOwner.UID,
					Controller: &isController,
				},
			},
		},
	}

	testcases := map[string]struct {
		pod           *v1.Pod
		pvc           *v1.PersistentVolumeClaim
		expectedError string
	}{
		"owned": {
			pod: podOwner,
			pvc: pvcWithOwner,
		},
		"other-pod": {
			pod:           podNotOwner,
			pvc:           pvcWithOwner,
			expectedError: `PVC kube-system/pvcNoOwner was not created for pod kube-system/podNotOwner (pod is not owner)`,
		},
		"no-owner": {
			pod:           podOwner,
			pvc:           pvcNoOwner,
			expectedError: `PVC kube-system/pvcNoOwner was not created for pod kube-system/podOwner (pod is not owner)`,
		},
		// 名字和所有者都对，但命名空间不同仍然不算属于该 Pod：
		// 归属判断必须同时匹配 UID 与 namespace，防止跨命名空间的误用。
		"different-namespace": {
			pod:           podOwner,
			pvc:           userPVCWithOwner,
			expectedError: `PVC user-namespace/userPVCWithOwner was not created for pod kube-system/podOwner (pod is not owner)`,
		},
	}

	// 每个用例既校验“该不该报错”，也校验报错文本本身：
	// 错误信息会直接返回给用户，必须在没有额外上下文时就能说明问题出在哪对 pod/pvc。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			err := VolumeIsForPod(tc.pod, tc.pvc)
			if tc.expectedError == "" {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error %q, got nil", tc.expectedError)
				} else if tc.expectedError != err.Error() {
					t.Errorf("expected error %q, got %v", tc.expectedError, err)
				}
			}
		})
	}
}
