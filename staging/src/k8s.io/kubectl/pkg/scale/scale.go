/*
Copyright 2014 The Kubernetes Authors.

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

package scale

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	scaleclient "k8s.io/client-go/scale"
)

// Scaler 为可伸缩的资源提供接口。
type Scaler interface {
	// Scale 在检查前置条件后对指定资源执行伸缩。当资源版本不匹配时，
	// 可选择重试（retry 不为 nil 时）；也可选择等待，直到资源状态与 newSize 一致
	// （wait 不为 nil 时）。
	// TODO: 待 #31345 修复后，将该实现改为基于 watch 的方式（#56075）。
	Scale(namespace, name string, newSize uint, actualSize *int32, preconditions *ScalePrecondition, retry, wait *RetryParams, gvr schema.GroupVersionResource, dryRun bool) error
	// ScaleSimple 执行一次简单的单次伸缩尝试——它本身并无太大用途，
	// 但却是 Scale 的必要组成部分
	ScaleSimple(namespace, name string, preconditions *ScalePrecondition, newSize uint, gvr schema.GroupVersionResource, dryRun bool) (updatedResourceVersion string, actualSize int32, err error)
}

// NewScaler 获取指定资源的 scaler
func NewScaler(scalesGetter scaleclient.ScalesGetter) Scaler {
	return &genericScaler{scalesGetter}
}

// ScalePrecondition 描述执行伸缩前必须成立的条件。
// 若 CurrentSize == -1，则忽略该条件。
// 若 CurrentResourceVersion 为空字符串，则忽略该条件。
// 否则，它们必须与资源中的值相等才视为有效。
type ScalePrecondition struct {
	Size            int
	ResourceVersion string
}

// 当资源未能匹配传给 kubectl 的
// 伸缩前置条件时，返回 PreconditionError。
type PreconditionError struct {
	Precondition  string
	ExpectedValue string
	ActualValue   string
}

func (pe PreconditionError) Error() string {
	return fmt.Sprintf("Expected %s to be %s, was %s", pe.Precondition, pe.ExpectedValue, pe.ActualValue)
}

// RetryParams 封装 kubectl scaler 使用的重试参数。
type RetryParams struct {
	Interval, Timeout time.Duration
}

func NewRetryParams(interval, timeout time.Duration) *RetryParams {
	return &RetryParams{interval, timeout}
}

// ScaleCondition 是围绕 Scale 的闭包，便于通过 util.wait 进行重试
func ScaleCondition(r Scaler, precondition *ScalePrecondition, namespace, name string, count uint, updatedResourceVersion *string, actualSize *int32, gvr schema.GroupVersionResource, dryRun bool) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		rv, size, err := r.ScaleSimple(namespace, name, precondition, count, gvr, dryRun)
		if updatedResourceVersion != nil {
			*updatedResourceVersion = rv
		}
		if actualSize != nil {
			*actualSize = size
		}
		// 仅在更新冲突时重试。
		if apierrors.IsConflict(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
}

// validateGeneric 确保前置条件匹配。条件有效时返回 nil，否则返回错误
func (precondition *ScalePrecondition) validate(scale *autoscalingv1.Scale) error {
	if precondition.Size != -1 && int(scale.Spec.Replicas) != precondition.Size {
		return PreconditionError{"replicas", strconv.Itoa(precondition.Size), strconv.Itoa(int(scale.Spec.Replicas))}
	}
	if len(precondition.ResourceVersion) > 0 && scale.ResourceVersion != precondition.ResourceVersion {
		return PreconditionError{"resource version", precondition.ResourceVersion, scale.ResourceVersion}
	}
	return nil
}

// genericScaler 可更新特定命名空间中资源的 scale
type genericScaler struct {
	scaleNamespacer scaleclient.ScalesGetter
}

var _ Scaler = &genericScaler{}

// ScaleSimple 更新指定资源的 scale。更新成功时返回 scale 的 resourceVersion。
func (s *genericScaler) ScaleSimple(namespace, name string, preconditions *ScalePrecondition, newSize uint, gvr schema.GroupVersionResource, dryRun bool) (updatedResourceVersion string, actualSize int32, err error) {
	if preconditions != nil {
		scale, err := s.scaleNamespacer.Scales(namespace).Get(context.TODO(), gvr.GroupResource(), name, metav1.GetOptions{})
		if err != nil {
			return "", 0, err
		}
		if err = preconditions.validate(scale); err != nil {
			return "", 0, err
		}
		scale.Spec.Replicas = int32(newSize)
		updateOptions := metav1.UpdateOptions{}
		if dryRun {
			updateOptions.DryRun = []string{metav1.DryRunAll}
		}
		updatedScale, err := s.scaleNamespacer.Scales(namespace).Update(context.TODO(), gvr.GroupResource(), scale, updateOptions)
		if err != nil {
			return "", 0, err
		}
		return updatedScale.ResourceVersion, updatedScale.Spec.Replicas, nil
	}

	// objectForReplicas 用于编码 scale patch
	type objectForReplicas struct {
		Replicas uint `json:"replicas"`
	}
	// objectForSpec 用于编码 scale patch
	type objectForSpec struct {
		Spec objectForReplicas `json:"spec"`
	}
	spec := objectForSpec{
		Spec: objectForReplicas{Replicas: newSize},
	}
	patch, err := json.Marshal(&spec)
	if err != nil {
		return "", 0, err
	}
	patchOptions := metav1.PatchOptions{}
	if dryRun {
		patchOptions.DryRun = []string{metav1.DryRunAll}
	}
	updatedScale, err := s.scaleNamespacer.Scales(namespace).Patch(context.TODO(), gvr, name, types.MergePatchType, patch, patchOptions)
	if err != nil {
		return "", 0, err
	}
	return updatedScale.ResourceVersion, updatedScale.Spec.Replicas, nil
}

// Scale 将指定资源的 scale 更新为新大小，可选择进行前置条件检查（preconditions 不为 nil 时）、
// 重试（retry 不为 nil 时），然后可选择等待状态达到期望的副本数。
func (s *genericScaler) Scale(namespace, resourceName string, newSize uint, actualSize *int32, preconditions *ScalePrecondition, retry, waitForReplicas *RetryParams, gvr schema.GroupVersionResource, dryRun bool) error {
	if retry == nil {
		// 使其立即只尝试一次
		retry = &RetryParams{Interval: time.Millisecond, Timeout: time.Millisecond}
	}
	cond := ScaleCondition(s, preconditions, namespace, resourceName, newSize, nil, actualSize, gvr, dryRun)
	if err := wait.PollUntilContextTimeout(context.Background(), retry.Interval, retry.Timeout, true, cond); err != nil {
		return err
	}
	if waitForReplicas != nil {
		return WaitForScaleHasDesiredReplicas(s.scaleNamespacer, gvr.GroupResource(), resourceName, namespace, newSize, waitForReplicas)
	}
	return nil
}

// scaleHasDesiredReplicas 返回一个条件函数，当且仅当 scale 的期望副本数（Spec）
// 等于其已更新的副本数（Status）时，该条件才为 true
func scaleHasDesiredReplicas(sClient scaleclient.ScalesGetter, gr schema.GroupResource, resourceName string, namespace string, desiredReplicas int32) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		actualScale, err := sClient.Scales(namespace).Get(ctx, gr, resourceName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		// 这说明期望的 scale 目标已被其他因素重置
		if actualScale.Spec.Replicas != desiredReplicas {
			return true, nil
		}
		return actualScale.Spec.Replicas == actualScale.Status.Replicas &&
			desiredReplicas == actualScale.Status.Replicas, nil
	}
}

// WaitForScaleHasDesiredReplicas 等待 scaleHasDesiredReplicas 条件满足，
// 超时则返回错误
func WaitForScaleHasDesiredReplicas(sClient scaleclient.ScalesGetter, gr schema.GroupResource, resourceName string, namespace string, newSize uint, waitForReplicas *RetryParams) error {
	if waitForReplicas == nil {
		return fmt.Errorf("waitForReplicas parameter cannot be nil")
	}
	err := wait.PollUntilContextTimeout(context.Background(), waitForReplicas.Interval, waitForReplicas.Timeout, true, scaleHasDesiredReplicas(sClient, gr, resourceName, namespace, int32(newSize)))

	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("timed out waiting for %q to be synced", resourceName)
	}
	return err
}
