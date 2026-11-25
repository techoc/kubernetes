/*
Copyright 2024 The Kubernetes Authors.

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

package pv

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	"k8s.io/utils/ptr"
)

// WaitForPersistentVolumeClaimModified 在给定超时时间内等待指定 PVC 绑定到期望的卷属性类。
// 若先超时则返回错误。
func WaitForPersistentVolumeClaimModified(ctx context.Context, c clientset.Interface, claim *v1.PersistentVolumeClaim, timeout time.Duration) error {
	desiredClass := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")

	var match = func(claim *v1.PersistentVolumeClaim) bool {
		for _, condition := range claim.Status.Conditions {
			// 表明 PVC 正在被修改或修改卷时出错的条件
			if condition.Type == v1.PersistentVolumeClaimVolumeModifyVolumeError ||
				condition.Type == v1.PersistentVolumeClaimVolumeModifyingVolume {
				return false
			}
		}

		// 检查 PVC 是否已绑定到期望的卷属性类
		currentClass := ptr.Deref(claim.Status.CurrentVolumeAttributesClassName, "")
		return claim.Status.Phase == v1.ClaimBound &&
			desiredClass == currentClass && claim.Status.ModifyVolumeStatus == nil
	}

	if match(claim) {
		return nil
	}

	return framework.Gomega().
		Eventually(ctx, framework.GetObject(c.CoreV1().PersistentVolumeClaims(claim.Namespace).Get, claim.Name, metav1.GetOptions{})).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(func(claim *v1.PersistentVolumeClaim) (func() string, error) {
			if match(claim) {
				return nil, nil
			}

			return func() string {
				return fmt.Sprintf("expected claim's status to be modified with the given VolumeAttirbutesClass %s, got instead:\n%s", desiredClass, format.Object(claim, 1))
			}, nil
		}))
}

// WaitForPersistentVolumeClaimModificationFailure 在给定超时时间内等待指定 PVC 绑定到无效卷属性类失败。
// 若先超时则返回错误。
func WaitForPersistentVolumeClaimModificationFailure(ctx context.Context, c clientset.Interface, claim *v1.PersistentVolumeClaim, timeout time.Duration) error {
	desiredClass := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")

	var match = func(claim *v1.PersistentVolumeClaim) bool {
		foundErrorCondition := false
		for _, condition := range claim.Status.Conditions {
			if condition.Type == v1.PersistentVolumeClaimVolumeModifyVolumeError {
				foundErrorCondition = true
			}
		}
		// 若没有找到错误，那本身就是错误
		if !foundErrorCondition {
			return false
		}

		// 检查 PVC 当前的卷属性类是否不是期望的那个，且带有相应的 ModifyVolumeStatus
		currentClass := ptr.Deref(claim.Status.CurrentVolumeAttributesClassName, "")
		return claim.Status.Phase == v1.ClaimBound &&
			desiredClass != currentClass && claim.Status.ModifyVolumeStatus != nil &&
			(claim.Status.ModifyVolumeStatus.Status == v1.PersistentVolumeClaimModifyVolumeInProgress ||
				claim.Status.ModifyVolumeStatus.Status == v1.PersistentVolumeClaimModifyVolumeInfeasible)
	}

	if match(claim) {
		return nil
	}

	return framework.Gomega().
		Eventually(ctx, framework.GetObject(c.CoreV1().PersistentVolumeClaims(claim.Namespace).Get, claim.Name, metav1.GetOptions{})).
		WithTimeout(timeout).
		Should(framework.MakeMatcher(func(claim *v1.PersistentVolumeClaim) (func() string, error) {
			if match(claim) {
				return nil, nil
			}

			return func() string {
				return fmt.Sprintf("expected claim's status to NOT be modified with the given VolumeAttirbutesClass %s, got instead:\n%s", desiredClass, format.Object(claim, 1))
			}, nil
		}))
}
