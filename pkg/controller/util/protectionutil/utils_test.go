/*
Copyright 2019 The Kubernetes Authors.

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

package protectionutil

import (
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/volume/util"
)

// 测试用的固定命名空间、PVC 与 PV 名称。
const (
	defaultNS      = "default"
	defaultPVCName = "pvc1"
	defaultPVName  = "pv1"
)

// TestCase 描述一组 IsDeletionCandidate / NeedToAddFinalizer 的测试输入与期望结果。
type TestCase struct {
	name      string
	obj       metav1.Object
	finalizer string
	result    bool
}

// pvc 构造一个没有删除时间戳、没有 finalizer 的 PVC。
func pvc() *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultPVCName,
			Namespace: defaultNS,
		},
	}
}

// pv 构造一个没有删除时间戳、没有 finalizer 的 PV。
func pv() *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultPVName,
		},
	}
}

// TestIsDeletionCandidateLackDeleteTimeAndFinalizer 验证对象既无删除时间戳也无 finalizer 时不是删除候选。
func TestIsDeletionCandidateLackDeleteTimeAndFinalizer(t *testing.T) {

	tests := []TestCase{
		{
			name:      "pv lacks delete time and finalizer",
			obj:       pv(),
			finalizer: util.PVProtectionFinalizer,
			result:    false,
		},
		{
			name:      "pvc lacks delete time and finalizer",
			obj:       pvc(),
			finalizer: util.PVCProtectionFinalizer,

			result: false,
		},
	}
	for _, test := range tests {
		if test.result != IsDeletionCandidate(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}

// TestIsDeletionCandidateLackDeleteTime 验证仅有 finalizer、无删除时间戳时不是删除候选。
func TestIsDeletionCandidateLackDeleteTime(t *testing.T) {
	pv := pv()
	pv.SetFinalizers([]string{util.PVProtectionFinalizer})
	pvc := pvc()
	pvc.SetFinalizers([]string{util.PVCProtectionFinalizer})
	tests := []TestCase{
		{
			name:      "pv lacks delete time",
			obj:       pv,
			finalizer: util.PVProtectionFinalizer,
			result:    false,
		},
		{
			name:      "pvc lacks delete time",
			obj:       pvc,
			finalizer: util.PVCProtectionFinalizer,

			result: false,
		},
	}
	for _, test := range tests {
		if test.result != IsDeletionCandidate(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}

// TestIsDeletionCandidateLackFinalizer 验证仅有删除时间戳、无 finalizer 时不是删除候选。
func TestIsDeletionCandidateLackFinalizer(t *testing.T) {

	pv := pv()
	pv.SetDeletionTimestamp(&metav1.Time{})
	pvc := pvc()
	pvc.SetDeletionTimestamp(&metav1.Time{})
	tests := []TestCase{
		{
			name:      "pv lacks finalizer",
			obj:       pv,
			finalizer: util.PVProtectionFinalizer,
			result:    false,
		},
		{
			name:      "pvc lacks finalizer",
			obj:       pvc,
			finalizer: util.PVCProtectionFinalizer,
			result:    false,
		},
	}
	for _, test := range tests {
		if test.result != IsDeletionCandidate(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}

// TestIsDeletionCandidateSuccess 验证同时具备删除时间戳与 finalizer 的对象是删除候选。
func TestIsDeletionCandidateSuccess(t *testing.T) {

	pv := pv()
	pv.SetDeletionTimestamp(&metav1.Time{})
	pv.SetFinalizers([]string{util.PVProtectionFinalizer})
	pvc := pvc()
	pvc.SetDeletionTimestamp(&metav1.Time{})
	pvc.SetFinalizers([]string{util.PVCProtectionFinalizer})

	tests := []TestCase{
		{
			name:      "pv is to delete",
			obj:       pv,
			finalizer: util.PVProtectionFinalizer,
			result:    true,
		},
		{
			name:      "pvc is to delete",
			obj:       pvc,
			finalizer: util.PVCProtectionFinalizer,
			result:    true,
		},
	}
	for _, test := range tests {
		if test.result != IsDeletionCandidate(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}

// TestNeedToAddFinalizerHasDeleteTimeAndFinalizer 验证已有删除时间戳和 finalizer 的对象无需再添加 finalizer。
func TestNeedToAddFinalizerHasDeleteTimeAndFinalizer(t *testing.T) {
	pv := pv()
	pv.SetDeletionTimestamp(&metav1.Time{})
	pv.SetFinalizers([]string{util.PVProtectionFinalizer})
	pvc := pvc()
	pvc.SetDeletionTimestamp(&metav1.Time{})
	pvc.SetFinalizers([]string{util.PVCProtectionFinalizer})

	tests := []TestCase{
		{
			name:      "pv has delete time and finalizer",
			obj:       pv,
			finalizer: util.PVProtectionFinalizer,
			result:    false,
		},
		{
			name:      "pvc has delete time and finalizer",
			obj:       pvc,
			finalizer: util.PVCProtectionFinalizer,
			result:    false,
		},
	}
	for _, test := range tests {
		if test.result != NeedToAddFinalizer(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}

// TestNeedToAddFinalizerHasDeleteTime 验证已进入删除流程的对象不应再添加 finalizer。
func TestNeedToAddFinalizerHasDeleteTime(t *testing.T) {
	pv := pv()
	pv.SetDeletionTimestamp(&metav1.Time{})
	pvc := pvc()
	pvc.SetDeletionTimestamp(&metav1.Time{})
	tests := []TestCase{
		{
			name:      "pv has delete",
			obj:       pv,
			finalizer: util.PVProtectionFinalizer,
			result:    false,
		},
		{
			name:      "pvc has delete",
			obj:       pvc,
			finalizer: util.PVCProtectionFinalizer,
			result:    false,
		},
	}
	for _, test := range tests {
		if test.result != NeedToAddFinalizer(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}

// TestNeedToAddFinalizerHasFinalizer 验证已有 finalizer 的对象无需重复添加。
func TestNeedToAddFinalizerHasFinalizer(t *testing.T) {
	pv := pv()
	pv.SetFinalizers([]string{util.PVProtectionFinalizer})
	pvc := pvc()
	pvc.SetFinalizers([]string{util.PVCProtectionFinalizer})

	tests := []TestCase{
		{
			name:      "pv has finalizer",
			obj:       pv,
			finalizer: util.PVProtectionFinalizer,
			result:    false,
		},
		{
			name:      "pvc has finalizer",
			obj:       pvc,
			finalizer: util.PVCProtectionFinalizer,
			result:    false,
		},
	}
	for _, test := range tests {
		if test.result != NeedToAddFinalizer(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}

// TestNeedToAddFinalizerSuccess 验证既无删除时间戳也无 finalizer 的活跃对象需要添加 finalizer。
func TestNeedToAddFinalizerSuccess(t *testing.T) {
	tests := []TestCase{
		{
			name:      "pv needs add finalizer",
			obj:       pv(),
			finalizer: util.PVProtectionFinalizer,
			result:    true,
		},
		{
			name:      "pvc needs add finalizer",
			obj:       pvc(),
			finalizer: util.PVCProtectionFinalizer,
			result:    true,
		},
	}
	for _, test := range tests {
		if test.result != NeedToAddFinalizer(test.obj, test.finalizer) {
			t.Error(test.name)
		}
	}
}
