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

// Package storageversion 维护每个资源的 StorageVersion 对象：记录各
// apiserver 实例使用的编码版本，并计算公共编码版本，供迁移与降级判断。
package storageversion

import (
	"context"
	"fmt"
	"time"

	"k8s.io/api/apiserverinternal/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// Client 只暴露更新 storage version 所需的最小方法集，便于测试替换。
type Client interface {
	Create(context.Context, *v1alpha1.StorageVersion, metav1.CreateOptions) (*v1alpha1.StorageVersion, error)
	UpdateStatus(context.Context, *v1alpha1.StorageVersion, metav1.UpdateOptions) (*v1alpha1.StorageVersion, error)
	Get(context.Context, string, metav1.GetOptions) (*v1alpha1.StorageVersion, error)
}

// SetCommonEncodingVersion 依据 StorageVersions 更新 CommonEncodingVersion
// 以及 AllEncodingVersionsEqual 条件。
//
// 公共编码版本取所有实例编码版本的“交集”——只有全部实例一致时才设置；
// 任一实例不同就清空，迫使调用方继续使用可解码的版本。
func SetCommonEncodingVersion(sv *v1alpha1.StorageVersion) {
	var oldCommonEncodingVersion *string
	if sv.Status.CommonEncodingVersion != nil {
		version := *sv.Status.CommonEncodingVersion
		oldCommonEncodingVersion = &version
	}
	sv.Status.CommonEncodingVersion = nil
	if len(sv.Status.StorageVersions) != 0 {
		firstVersion := sv.Status.StorageVersions[0].EncodingVersion
		agreed := true
		for _, ssv := range sv.Status.StorageVersions {
			if ssv.EncodingVersion != firstVersion {
				agreed = false
				break
			}
		}
		if agreed {
			sv.Status.CommonEncodingVersion = &firstVersion
		}
	}

	condition := v1alpha1.StorageVersionCondition{
		Type:               v1alpha1.AllEncodingVersionsEqual,
		Status:             v1alpha1.ConditionFalse,
		ObservedGeneration: sv.Generation,
		LastTransitionTime: metav1.NewTime(time.Now()),
		Reason:             "CommonEncodingVersionUnset",
		Message:            "Common encoding version unset",
	}
	if sv.Status.CommonEncodingVersion != nil {
		condition.Status = v1alpha1.ConditionTrue
		condition.Reason = "CommonEncodingVersionSet"
		condition.Message = "Common encoding version set"
	}
	forceTransition := false
	if oldCommonEncodingVersion != nil && sv.Status.CommonEncodingVersion != nil &&
		*oldCommonEncodingVersion != *sv.Status.CommonEncodingVersion {
		forceTransition = true
	}
	setStatusCondition(&sv.Status.Conditions, condition, forceTransition)
}

// findStatusCondition 返回第一个匹配类型的条件指针；找不到返回 nil。
func findStatusCondition(conditions []v1alpha1.StorageVersionCondition,
	conditionType v1alpha1.StorageVersionConditionType) *v1alpha1.StorageVersionCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

// setStatusCondition 把 conditions 中对应类型的条件设置为 newCondition，conditions 不可为 nil。
//  1. 指定类型的条件已存在：用 newCondition 覆盖其全部字段；若新旧状态不同，
//     把 LastTransitionTime 设为当前时间
//  2. 指定类型的条件不存在：若 LastTransitionTime 未设置则置为当前时间，并追加 newCondition
//
// 注意：forceTransition 允许在状态未变化时仍然覆盖 LastTransitionTime。
func setStatusCondition(conditions *[]v1alpha1.StorageVersionCondition, newCondition v1alpha1.StorageVersionCondition,
	forceTransition bool) {
	if conditions == nil {
		return
	}

	if newCondition.LastTransitionTime.IsZero() {
		newCondition.LastTransitionTime = metav1.NewTime(time.Now())
	}
	existingCondition := findStatusCondition(*conditions, newCondition.Type)
	if existingCondition == nil {
		*conditions = append(*conditions, newCondition)
		return
	}

	statusChanged := existingCondition.Status != newCondition.Status
	if statusChanged || forceTransition {
		existingCondition.LastTransitionTime = newCondition.LastTransitionTime
	}
	existingCondition.Status = newCondition.Status
	existingCondition.Reason = newCondition.Reason
	existingCondition.Message = newCondition.Message
	existingCondition.ObservedGeneration = newCondition.ObservedGeneration
}

// updateStorageVersionFor 更新该资源的 storage version 对象。
//
// 有限重试（3 次）：StorageVersion 是集群级共享对象，多 apiserver 并发
// 更新时冲突很常见，但不应无限重试拖慢启动。
func updateStorageVersionFor(c Client, apiserverID string, gr schema.GroupResource, encodingVersion string, decodableVersions []string, servedVersions []string) error {
	retries := 3
	var retry int
	var err error
	for retry < retries {
		err = singleUpdate(c, apiserverID, gr, encodingVersion, decodableVersions, servedVersions)
		if err == nil {
			return nil
		}
		if apierrors.IsAlreadyExists(err) || apierrors.IsConflict(err) {
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			klog.Errorf("retry %d, failed to update storage version for %v: %v", retry, gr, err)
			retry++
			time.Sleep(1 * time.Second)
		}
	}
	return err
}

func singleUpdate(c Client, apiserverID string, gr schema.GroupResource, encodingVersion string, decodableVersions []string, servedVersions []string) error {
	shouldCreate := false
	// 对象名固定为 "<group>.<resource>"，core 组则形如 ".pods"；
	// 这是 StorageVersion 的命名约定，不能随意更改。
	name := fmt.Sprintf("%s.%s", gr.Group, gr.Resource)
	sv, err := c.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if apierrors.IsNotFound(err) {
		shouldCreate = true
		sv = &v1alpha1.StorageVersion{}
		sv.ObjectMeta.Name = name
	}
	updatedSV := localUpdateStorageVersion(sv, apiserverID, encodingVersion, decodableVersions, servedVersions)
	if shouldCreate {
		// 先 Create 再单独 UpdateStatus：status 是子资源，创建请求中
		// 携带的 status 会被忽略。
		createdSV, err := c.Create(context.TODO(), updatedSV, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		// 把计算出的 status 赋给刚创建的对象，然后更新 status
		createdSV.Status = updatedSV.Status
		_, err = c.UpdateStatus(context.TODO(), createdSV, metav1.UpdateOptions{})
		return err
	}
	_, err = c.UpdateStatus(context.TODO(), updatedSV, metav1.UpdateOptions{})
	return err
}

// localUpdateStorageVersion 用给定 server 的 storageversion 信息更新入参 storageversion，
// 该函数就地修改入参对象。
//
// 以 apiserverID 为键做 upsert：同一实例重启后只更新自己的条目，
// 不会把其它实例的信息覆盖掉。
func localUpdateStorageVersion(sv *v1alpha1.StorageVersion, apiserverID, encodingVersion string, decodableVersions []string, servedVersions []string) *v1alpha1.StorageVersion {
	newSSV := v1alpha1.ServerStorageVersion{
		APIServerID:       apiserverID,
		EncodingVersion:   encodingVersion,
		DecodableVersions: decodableVersions,
		ServedVersions:    servedVersions,
	}
	foundSSV := false
	for i, ssv := range sv.Status.StorageVersions {
		if ssv.APIServerID == apiserverID {
			sv.Status.StorageVersions[i] = newSSV
			foundSSV = true
			break
		}
	}
	if !foundSSV {
		sv.Status.StorageVersions = append(sv.Status.StorageVersions, newSSV)
	}
	SetCommonEncodingVersion(sv)
	return sv
}
