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

package statefulset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/history"
	"k8s.io/utils/ptr"
)

// noopRecorder 是空实现的 EventRecorder。FakeRecorder 的缓冲区大小固定，事件超出缓冲区时测试会挂起。
type noopRecorder struct{}

// Event 丢弃普通事件。
func (r *noopRecorder) Event(object runtime.Object, eventtype, reason, message string) {}

// Eventf 丢弃格式化事件。
func (r *noopRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
}

// AnnotatedEventf 丢弃带注解的事件。
func (r *noopRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
}

// getClaimPodName 从 PVC 名称中提取关联的 Pod 名称；名称不匹配时返回空字符串。
func getClaimPodName(set *apps.StatefulSet, claim *v1.PersistentVolumeClaim) string {
	podName := ""

	// PVC 名称由卷名加 "-<set 名>-<序号>" 构成，只需匹配末尾的 Pod 名部分。
	statefulClaimRegex := regexp.MustCompile(fmt.Sprintf(".*-(%s-[0-9]+)$", set.Name))
	matches := statefulClaimRegex.FindStringSubmatch(claim.Name)
	if len(matches) != 2 {
		return podName
	}
	return matches[1]
}

// ownerRefsChanged 比较两组 ownerReference 是否不同（忽略顺序）。
func ownerRefsChanged(originalRefs, newRefs []metav1.OwnerReference) bool {
	if len(originalRefs) != len(newRefs) {
		return true
	}
	// 用 APIVersion/Kind/Name 三元组作为身份标识；UID 不匹配属于过期引用，由其他测试覆盖。
	key := func(ref *metav1.OwnerReference) string {
		return fmt.Sprintf("%s-%s-%s", ref.APIVersion, ref.Kind, ref.Name)
	}
	refs := map[string]bool{}
	for i := range originalRefs {
		refs[key(&originalRefs[i])] = true
	}
	for i := range newRefs {
		k := key(&newRefs[i])
		// 布尔标记同时防止重复引用被当成同一条旧引用匹配两次。
		if val, found := refs[k]; !found || !val {
			return true
		}
		refs[k] = false
	}
	return false
}

// TestOwnerRefsChanged 验证 ownerReference 比较能识别增删与改名，忽略顺序但不忽略重复。
func TestOwnerRefsChanged(t *testing.T) {
	toRefs := func(strs []string) []metav1.OwnerReference {
		refs := []metav1.OwnerReference{}
		for _, s := range strs {
			pieces := strings.Split(s, "/")
			refs = append(refs, metav1.OwnerReference{
				APIVersion: pieces[0],
				Kind:       pieces[1],
				Name:       pieces[2],
			})
		}
		return refs
	}
	testCases := []struct {
		orig, new []string
		changed   bool
	}{
		{
			orig:    []string{"v1/pod/foo"},
			new:     []string{},
			changed: true,
		},
		{
			orig:    []string{"v1/pod/foo"},
			new:     []string{"v1/pod/foo"},
			changed: false,
		},
		{
			orig:    []string{"v1/pod/foo"},
			new:     []string{"v1/pod/bar"},
			changed: true,
		},
		{
			orig:    []string{"v1/pod/foo", "v1/set/bob"},
			new:     []string{"v1/pod/foo", "v1/set/alice"},
			changed: true,
		},
		{
			orig:    []string{"v1/pod/foo", "v1/set/bob"},
			new:     []string{"v1/pod/foo", "v1/set/bob"},
			changed: false,
		},
		{
			orig:    []string{"v1/pod/foo", "v1/set/bob"},
			new:     []string{"v1/pod/foo", "v1/set/bob", "v1/set/bob"},
			changed: true,
		},
		{
			orig:    []string{"v1/pod/foo", "v1/set/bob"},
			new:     []string{"v1/set/bob", "v1/pod/foo"},
			changed: false,
		},
	}
	for _, tc := range testCases {
		if ownerRefsChanged(toRefs(tc.orig), toRefs(tc.new)) != tc.changed {
			t.Errorf("Expected change=%t but got %t for %v vs %v", tc.changed, !tc.changed, tc.orig, tc.new)
		}
	}
}

// TestGetParentNameAndOrdinal 验证从 Pod 名称中提取父对象名与序号，以及非法名称的返回值。
func TestGetParentNameAndOrdinal(t *testing.T) {
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 1)
	if parent, ordinal := getParentNameAndOrdinal(pod); parent != set.Name {
		t.Errorf("Extracted the wrong parent name expected %s found %s", set.Name, parent)
	} else if ordinal != 1 {
		t.Errorf("Extracted the wrong ordinal expected %d found %d", 1, ordinal)
	}
	pod.Name = "1-bar"
	if parent, ordinal := getParentNameAndOrdinal(pod); parent != "" {
		t.Error("Expected empty string for non-member Pod parent")
	} else if ordinal != -1 {
		t.Error("Expected -1 for non member Pod ordinal")
	}
}

// TestGetClaimPodName 验证 PVC 名称到 Pod 名称的提取，包括多段前缀和不匹配的情形。
func TestGetClaimPodName(t *testing.T) {
	set := apps.StatefulSet{}
	set.Name = "my-set"
	claim := v1.PersistentVolumeClaim{}
	claim.Name = "volume-my-set-2"
	if pod := getClaimPodName(&set, &claim); pod != "my-set-2" {
		t.Errorf("Expected my-set-2 found %s", pod)
	}
	claim.Name = "long-volume-my-set-20"
	if pod := getClaimPodName(&set, &claim); pod != "my-set-20" {
		t.Errorf("Expected my-set-20 found %s", pod)
	}
	claim.Name = "volume-2-my-set"
	if pod := getClaimPodName(&set, &claim); pod != "" {
		t.Errorf("Expected empty string found %s", pod)
	}
	claim.Name = "volume-pod-2"
	if pod := getClaimPodName(&set, &claim); pod != "" {
		t.Errorf("Expected empty string found %s", pod)
	}
}

// TestIsMemberOf 验证成员判定不会把同名序号的 Pod 归到另一个 StatefulSet。
func TestIsMemberOf(t *testing.T) {
	set := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	pod := newStatefulSetPod(set, 1)
	if !isMemberOf(set, pod) {
		t.Error("isMemberOf returned false negative")
	}
	if isMemberOf(set2, pod) {
		t.Error("isMemberOf returned false positive")
	}
}

// TestIdentityMatches 验证身份校验覆盖名称、命名空间和 statefulSetPodName 标签。
func TestIdentityMatches(t *testing.T) {
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 1)
	if !identityMatches(set, pod) {
		t.Error("Newly created Pod has a bad identity")
	}
	pod.Name = "foo"
	if identityMatches(set, pod) {
		t.Error("identity matches for a Pod with the wrong name")
	}
	pod = newStatefulSetPod(set, 1)
	pod.Namespace = ""
	if identityMatches(set, pod) {
		t.Error("identity matches for a Pod with the wrong namespace")
	}
	pod = newStatefulSetPod(set, 1)
	delete(pod.Labels, apps.StatefulSetPodNameLabel)
	if identityMatches(set, pod) {
		t.Error("identity matches for a Pod with the wrong statefulSetPodNameLabel")
	}
}

// TestStorageMatches 验证存储校验覆盖卷、PVC 声明名和序号对应的名称。
func TestStorageMatches(t *testing.T) {
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 1)
	if !storageMatches(set, pod) {
		t.Error("Newly created Pod has a invalid storage")
	}
	pod.Spec.Volumes = nil
	if storageMatches(set, pod) {
		t.Error("Pod with invalid Volumes has valid storage")
	}
	pod = newStatefulSetPod(set, 1)
	for i := range pod.Spec.Volumes {
		pod.Spec.Volumes[i].PersistentVolumeClaim = nil
	}
	if storageMatches(set, pod) {
		t.Error("Pod with invalid Volumes claim valid storage")
	}
	pod = newStatefulSetPod(set, 1)
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].PersistentVolumeClaim != nil {
			pod.Spec.Volumes[i].PersistentVolumeClaim.ClaimName = "foo"
		}
	}
	if storageMatches(set, pod) {
		t.Error("Pod with invalid Volumes claim valid storage")
	}
	pod = newStatefulSetPod(set, 1)
	pod.Name = "bar"
	if storageMatches(set, pod) {
		t.Error("Pod with invalid ordinal has valid storage")
	}
}

// TestUpdateIdentity 验证 updateIdentity 能修复被破坏的命名空间和 Pod 名称标签。
func TestUpdateIdentity(t *testing.T) {
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 1)
	if !identityMatches(set, pod) {
		t.Error("Newly created Pod has a bad identity")
	}
	pod.Namespace = ""
	if identityMatches(set, pod) {
		t.Error("identity matches for a Pod with the wrong namespace")
	}
	updateIdentity(set, pod)
	if !identityMatches(set, pod) {
		t.Error("updateIdentity failed to update the Pods namespace")
	}
	delete(pod.Labels, apps.StatefulSetPodNameLabel)
	updateIdentity(set, pod)
	if !identityMatches(set, pod) {
		t.Error("updateIdentity failed to restore the statefulSetPodName label")
	}
}

// TestUpdateStorage 验证 updateStorage 能重建缺失的卷、PVC 声明及错误的声明名。
func TestUpdateStorage(t *testing.T) {
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 1)
	if !storageMatches(set, pod) {
		t.Error("Newly created Pod has a invalid storage")
	}
	pod.Spec.Volumes = nil
	if storageMatches(set, pod) {
		t.Error("Pod with invalid Volumes has valid storage")
	}
	updateStorage(set, pod)
	if !storageMatches(set, pod) {
		t.Error("updateStorage failed to recreate volumes")
	}
	pod = newStatefulSetPod(set, 1)
	for i := range pod.Spec.Volumes {
		pod.Spec.Volumes[i].PersistentVolumeClaim = nil
	}
	if storageMatches(set, pod) {
		t.Error("Pod with invalid Volumes claim valid storage")
	}
	updateStorage(set, pod)
	if !storageMatches(set, pod) {
		t.Error("updateStorage failed to recreate volume claims")
	}
	pod = newStatefulSetPod(set, 1)
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].PersistentVolumeClaim != nil {
			pod.Spec.Volumes[i].PersistentVolumeClaim.ClaimName = "foo"
		}
	}
	if storageMatches(set, pod) {
		t.Error("Pod with invalid Volumes claim valid storage")
	}
	updateStorage(set, pod)
	if !storageMatches(set, pod) {
		t.Error("updateStorage failed to recreate volume claim names")
	}
}

// TestGetPersistentVolumeClaimRetentionPolicy 验证保留策略读取，以及未配置时的默认行为。
func TestGetPersistentVolumeClaimRetentionPolicy(t *testing.T) {
	retainPolicy := apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	scaledownPolicy := apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
	}

	set := apps.StatefulSet{}
	set.Spec.PersistentVolumeClaimRetentionPolicy = &retainPolicy
	got := getPersistentVolumeClaimRetentionPolicy(&set)
	if got.WhenScaled != apps.RetainPersistentVolumeClaimRetentionPolicyType || got.WhenDeleted != apps.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Errorf("Expected retain policy")
	}
	set.Spec.PersistentVolumeClaimRetentionPolicy = &scaledownPolicy
	got = getPersistentVolumeClaimRetentionPolicy(&set)
	if got.WhenScaled != apps.DeletePersistentVolumeClaimRetentionPolicyType || got.WhenDeleted != apps.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Errorf("Expected scaledown policy")
	}
}

// TestMatchesRef 验证引用匹配要求 API 版本、Kind 和名称一致，UID 不参与匹配。
func TestMatchesRef(t *testing.T) {
	testCases := []struct {
		name        string
		ref         metav1.OwnerReference
		obj         metav1.ObjectMeta
		schema      schema.GroupVersionKind
		shouldMatch bool
	}{
		{
			name: "full match",
			ref: metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       "fred",
				UID:        "abc",
			},
			obj: metav1.ObjectMeta{
				Name: "fred",
				UID:  "abc",
			},
			schema:      podKind,
			shouldMatch: true,
		},
		{
			name: "match without UID",
			ref: metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       "fred",
				UID:        "abc",
			},
			obj: metav1.ObjectMeta{
				Name: "fred",
				UID:  "not-matching",
			},
			schema:      podKind,
			shouldMatch: true,
		},
		{
			name: "mismatch name",
			ref: metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       "fred",
				UID:        "abc",
			},
			obj: metav1.ObjectMeta{
				Name: "joan",
				UID:  "abc",
			},
			schema:      podKind,
			shouldMatch: false,
		},
		{
			name: "wrong schema",
			ref: metav1.OwnerReference{
				APIVersion: "beta2",
				Kind:       "Pod",
				Name:       "fred",
				UID:        "abc",
			},
			obj: metav1.ObjectMeta{
				Name: "fred",
				UID:  "abc",
			},
			schema:      podKind,
			shouldMatch: false,
		},
	}
	for _, tc := range testCases {
		got := matchesRef(&tc.ref, &tc.obj, tc.schema)
		if got != tc.shouldMatch {
			t.Errorf("Failed %s: got %t, expected %t", tc.name, got, tc.shouldMatch)
		}
	}
}

// TestIsClaimOwnerUpToDate 验证 PVC ownerReference 判定与保留策略一致，且不依赖引用的排列顺序。
func TestIsClaimOwnerUpToDate(t *testing.T) {
	testCases := []struct {
		name            string
		scaleDownPolicy apps.PersistentVolumeClaimRetentionPolicyType
		setDeletePolicy apps.PersistentVolumeClaimRetentionPolicyType
		needsPodRef     bool
		needsSetRef     bool
		replicas        int32
		ordinal         int
	}{
		{
			name:            "retain",
			scaleDownPolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			needsPodRef:     false,
			needsSetRef:     false,
		},
		{
			name:            "on SS delete",
			scaleDownPolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			needsPodRef:     false,
			needsSetRef:     true,
		},
		{
			name:            "on scaledown only, condemned",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			needsPodRef:     true,
			needsSetRef:     false,
			replicas:        2,
			ordinal:         2,
		},
		{
			name:            "on scaledown only, remains",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			needsPodRef:     false,
			needsSetRef:     false,
			replicas:        2,
			ordinal:         1,
		},
		{
			name:            "on both, condemned",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			needsPodRef:     true,
			needsSetRef:     false,
			replicas:        2,
			ordinal:         2,
		},
		{
			name:            "on both, remains",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			needsPodRef:     false,
			needsSetRef:     true,
			replicas:        2,
			ordinal:         1,
		},
	}

	for _, tc := range testCases {
		for _, useOtherRefs := range []bool{false, true} {
			for _, setPodRef := range []bool{false, true} {
				for _, setSetRef := range []bool{false, true} {
					_, ctx := ktesting.NewTestContext(t)
					logger := klog.FromContext(ctx)
					claim := v1.PersistentVolumeClaim{}
					claim.Name = "target-claim"
					pod := v1.Pod{}
					pod.Name = fmt.Sprintf("pod-%d", tc.ordinal)
					pod.GetObjectMeta().SetUID("pod-123")
					set := apps.StatefulSet{}
					set.Name = "stateful-set"
					set.GetObjectMeta().SetUID("ss-456")
					set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenScaled:  tc.scaleDownPolicy,
						WhenDeleted: tc.setDeletePolicy,
					}
					set.Spec.Replicas = &tc.replicas
					claimRefs := claim.GetOwnerReferences()
					// 遍历 Pod/Set 引用有无及其组合，确保判定只看策略要求的引用是否存在。
					if setPodRef {
						claimRefs = addControllerRef(claimRefs, &pod, podKind)
					}
					if setSetRef {
						claimRefs = addControllerRef(claimRefs, &set, controllerKind)
					}
					if useOtherRefs {
						// 无关的非控制器引用不应干扰策略判定。
						claimRefs = append(
							claimRefs,
							metav1.OwnerReference{
								Name:       "rand1",
								APIVersion: "v1",
								Kind:       "Pod",
								UID:        "rand1-uid",
							},
							metav1.OwnerReference{
								Name:       "rand2",
								APIVersion: "v1",
								Kind:       "Pod",
								UID:        "rand2-uid",
							})
					}
					claim.SetOwnerReferences(claimRefs)
					shouldMatch := setPodRef == tc.needsPodRef && setSetRef == tc.needsSetRef
					if isClaimOwnerUpToDate(logger, &claim, &set, &pod) != shouldMatch {
						t.Errorf("Bad match for %s with pod=%v,set=%v,others=%v", tc.name, setPodRef, setSetRef, useOtherRefs)
					}
				}
			}
		}
	}
}

// TestClaimOwnerUpToDateEdgeCases 验证非控制器标记、过期 UID 和额外控制器对 owner 判定的影响。
func TestClaimOwnerUpToDateEdgeCases(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	testCases := []struct {
		name        string
		ownerRefs   []metav1.OwnerReference
		policy      apps.StatefulSetPersistentVolumeClaimRetentionPolicy
		shouldMatch bool
	}{
		{
			name: "normal controller, pod",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "pod-1",
					APIVersion: "v1",
					Kind:       "Pod",
					UID:        "pod-123",
					Controller: ptr.To(true),
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: true,
		},
		{
			name: "non-controller causes policy mismatch, pod",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "pod-1",
					APIVersion: "v1",
					Kind:       "Pod",
					UID:        "pod-123",
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: false,
		},
		{
			name: "stale controller does not affect policy, pod",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "pod-1",
					APIVersion: "v1",
					Kind:       "Pod",
					UID:        "pod-stale",
					Controller: ptr.To(true),
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: true,
		},
		{
			name: "unexpected controller causes policy mismatch, pod",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "pod-1",
					APIVersion: "v1",
					Kind:       "Pod",
					UID:        "pod-123",
					Controller: ptr.To(true),
				},
				{
					Name:       "Random",
					APIVersion: "v1",
					Kind:       "Pod",
					UID:        "random",
					Controller: ptr.To(true),
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: false,
		},
		{
			name: "normal controller, set",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "stateful-set",
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					UID:        "ss-456",
					Controller: ptr.To(true),
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: true,
		},
		{
			name: "non-controller causes policy mismatch, set",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "stateful-set",
					APIVersion: "appsv1",
					Kind:       "StatefulSet",
					UID:        "ss-456",
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: false,
		},
		{
			name: "stale controller ignored, set",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "stateful-set",
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					UID:        "set-stale",
					Controller: ptr.To(true),
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: true,
		},
		{
			name: "unexpected controller causes policy mismatch, set",
			ownerRefs: []metav1.OwnerReference{
				{
					Name:       "stateful-set",
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					UID:        "ss-456",
					Controller: ptr.To(true),
				},
				{
					Name:       "Random",
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					UID:        "random",
					Controller: ptr.To(true),
				},
			},
			policy: apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			shouldMatch: false,
		},
	}

	for _, tc := range testCases {
		claim := v1.PersistentVolumeClaim{}
		claim.Name = "target-claim"
		pod := v1.Pod{}
		pod.Name = "pod-1"
		pod.GetObjectMeta().SetUID("pod-123")
		set := apps.StatefulSet{}
		set.Name = "stateful-set"
		set.GetObjectMeta().SetUID("ss-456")
		set.Spec.PersistentVolumeClaimRetentionPolicy = &tc.policy
		set.Spec.Replicas = ptr.To(int32(1))
		claim.SetOwnerReferences(tc.ownerRefs)
		got := isClaimOwnerUpToDate(logger, &claim, &set, &pod)
		if got != tc.shouldMatch {
			t.Errorf("Unexpected match for %s, got %t expected %t", tc.name, got, tc.shouldMatch)
		}
	}
}

// TestUpdateClaimOwnerRefForSetAndPod 验证更新后只保留策略要求的控制器引用，并清除多余控制器。
func TestUpdateClaimOwnerRefForSetAndPod(t *testing.T) {
	testCases := []struct {
		name                 string
		scaleDownPolicy      apps.PersistentVolumeClaimRetentionPolicyType
		setDeletePolicy      apps.PersistentVolumeClaimRetentionPolicyType
		condemned            bool
		needsPodRef          bool
		needsSetRef          bool
		unexpectedController bool
	}{
		{
			name:            "retain",
			scaleDownPolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			condemned:       false,
			needsPodRef:     false,
			needsSetRef:     false,
		},
		{
			name:            "delete with set",
			scaleDownPolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			condemned:       false,
			needsPodRef:     false,
			needsSetRef:     true,
		},
		{
			name:            "delete with scaledown, not condemned",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			condemned:       false,
			needsPodRef:     false,
			needsSetRef:     false,
		},
		{
			name:            "delete on scaledown, condemned",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			condemned:       true,
			needsPodRef:     true,
			needsSetRef:     false,
		},
		{
			name:            "delete on both, not condemned",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			condemned:       false,
			needsPodRef:     false,
			needsSetRef:     true,
		},
		{
			name:            "delete on both, condemned",
			scaleDownPolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy: apps.DeletePersistentVolumeClaimRetentionPolicyType,
			condemned:       true,
			needsPodRef:     true,
			needsSetRef:     false,
		},
		{
			name:                 "unexpected controller",
			scaleDownPolicy:      apps.DeletePersistentVolumeClaimRetentionPolicyType,
			setDeletePolicy:      apps.DeletePersistentVolumeClaimRetentionPolicyType,
			condemned:            true,
			needsPodRef:          false,
			needsSetRef:          false,
			unexpectedController: true,
		},
	}
	for _, tc := range testCases {
		for variations := 0; variations < 8; variations++ {
			hasPodRef := (variations & 1) != 0
			hasSetRef := (variations & 2) != 0
			extraOwner := (variations & 3) != 0
			_, ctx := ktesting.NewTestContext(t)
			logger := klog.FromContext(ctx)
			set := apps.StatefulSet{}
			set.Name = "ss"
			numReplicas := int32(5)
			set.Spec.Replicas = &numReplicas
			set.SetUID("ss-123")
			set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  tc.scaleDownPolicy,
				WhenDeleted: tc.setDeletePolicy,
			}
			pod := v1.Pod{}
			if tc.condemned {
				pod.Name = "pod-8"
			} else {
				pod.Name = "pod-1"
			}
			pod.SetUID("pod-456")
			claim := v1.PersistentVolumeClaim{}
			claimRefs := claim.GetOwnerReferences()
			if hasPodRef {
				claimRefs = addControllerRef(claimRefs, &pod, podKind)
			}
			if hasSetRef {
				claimRefs = addControllerRef(claimRefs, &set, controllerKind)
			}
			if extraOwner {
				// 非控制器 owner 不属于本控制器的管理范围，应原样保留。
				claimRefs = append(claimRefs, metav1.OwnerReference{
					APIVersion: "custom/v1",
					Kind:       "random",
					Name:       "random",
					UID:        "abc",
				})
			}
			if tc.unexpectedController {
				claimRefs = append(claimRefs, metav1.OwnerReference{
					APIVersion: "custom/v1",
					Kind:       "Unknown",
					Name:       "unknown",
					UID:        "xyz",
					Controller: ptr.To(true),
				})
			}
			claim.SetOwnerReferences(claimRefs)
			updateClaimOwnerRefForSetAndPod(logger, &claim, &set, &pod)
			// 更新后应只有策略指定的 owner 带 controller 标记；其他控制器会被清除。
			check := func(target, owner metav1.Object) bool {
				for _, ref := range target.GetOwnerReferences() {
					if ref.UID == owner.GetUID() {
						return ref.Controller != nil && *ref.Controller
					}
				}
				return false
			}
			if check(&claim, &pod) != tc.needsPodRef {
				t.Errorf("Bad pod ref for %s hasPodRef=%v hasSetRef=%v", tc.name, hasPodRef, hasSetRef)
			}
			if check(&claim, &set) != tc.needsSetRef {
				t.Errorf("Bad set ref for %s hasPodRef=%v hasSetRef=%v", tc.name, hasPodRef, hasSetRef)
			}
		}
	}
}

// TestUpdateClaimControllerRef 验证控制器引用缺失或过期时的增删行为，保留非控制器引用。
func TestUpdateClaimControllerRef(t *testing.T) {
	testCases := []struct {
		name         string
		originalRefs []metav1.OwnerReference
		expectedRefs []metav1.OwnerReference
	}{
		{
			name: "set correctly",
			originalRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
					UID:        "123",
					Controller: ptr.To(true),
				},
				{
					APIVersion: "someone",
					Kind:       "Else",
					Name:       "foo",
				},
			},
			expectedRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
					UID:        "123",
					Controller: ptr.To(true),
				},
				{
					APIVersion: "someone",
					Kind:       "Else",
					Name:       "foo",
				},
			},
		},
		{
			name: "missing controller",
			originalRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
					UID:        "123",
				},
			},
			expectedRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
					UID:        "123",
					Controller: ptr.To(true),
				},
			},
		},
		{
			name: "matching name but missing",
			originalRefs: []metav1.OwnerReference{
				{
					APIVersion: "someone",
					Kind:       "else",
					Name:       "sts",
					UID:        "456",
				},
			},
			expectedRefs: []metav1.OwnerReference{
				{
					APIVersion: "someone",
					Kind:       "else",
					Name:       "sts",
					UID:        "456",
				},
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
					UID:        "123",
					Controller: ptr.To(true),
				},
			},
		},
		{
			name:         "not present",
			originalRefs: []metav1.OwnerReference{},
			expectedRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
					UID:        "123",
					Controller: ptr.To(true),
				},
			},
		},
		{
			name: "controller, but no UID",
			originalRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
					Controller: ptr.To(true),
				},
			},
			// UID 缺失的引用视为过期的意外控制器，会被清理。
			expectedRefs: []metav1.OwnerReference{},
		},
		{
			name: "neither controller nor UID",
			originalRefs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "sts",
				},
			},
			// UID 缺失的引用视为过期的意外控制器，会被清理。
			expectedRefs: []metav1.OwnerReference{},
		},
	}
	for _, tc := range testCases {
		_, ctx := ktesting.NewTestContext(t)
		logger := klog.FromContext(ctx)
		set := apps.StatefulSet{}
		set.Name = "sts"
		set.Spec.Replicas = ptr.To(int32(1))
		set.SetUID("123")
		set.Spec.PersistentVolumeClaimRetentionPolicy = &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		}
		pod := v1.Pod{}
		pod.Name = "pod-0"
		pod.SetUID("456")
		claim := v1.PersistentVolumeClaim{}
		claim.SetOwnerReferences(tc.originalRefs)
		updateClaimOwnerRefForSetAndPod(logger, &claim, &set, &pod)
		if ownerRefsChanged(tc.expectedRefs, claim.GetOwnerReferences()) {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expectedRefs, claim.GetOwnerReferences())
		}
	}
}

// TestHasOwnerRef 验证只要存在对应 UID 的引用即算命中，不要求它是控制器。
func TestHasOwnerRef(t *testing.T) {
	target := v1.Pod{}
	target.SetOwnerReferences([]metav1.OwnerReference{
		{UID: "123", Controller: ptr.To(true)},
		{UID: "456", Controller: ptr.To(false)},
		{UID: "789"},
	})
	testCases := []struct {
		uid    types.UID
		hasRef bool
	}{
		{
			uid:    "123",
			hasRef: true,
		},
		{
			uid:    "456",
			hasRef: true,
		},
		{
			uid:    "789",
			hasRef: true,
		},
		{
			uid:    "012",
			hasRef: false,
		},
	}
	for _, tc := range testCases {
		owner := v1.Pod{}
		owner.GetObjectMeta().SetUID(tc.uid)
		got := hasOwnerRef(&target, &owner)
		if got != tc.hasRef {
			t.Errorf("Expected %t for %s, got %t", tc.hasRef, tc.uid, got)
		}
	}
}

// TestHasUnexpectedController 验证第三方控制器及过期 UID 的识别，并确认 Retain 策略下不报异常。
func TestHasUnexpectedController(t *testing.T) {
	// 所有用例共用名为 "set" 和 "pod" 的对象；UID 由下方分别设置。
	testCases := []struct {
		name                             string
		refs                             []metav1.OwnerReference
		shouldReportUnexpectedController bool
	}{
		{
			name: "custom controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "chipmunks/v1",
					Kind:       "CustomController",
					Name:       "simon",
					UID:        "other-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: true,
		},
		{
			name: "custom non-controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "chipmunks/v1",
					Kind:       "CustomController",
					Name:       "simon",
					UID:        "other-uid",
					Controller: ptr.To(false),
				},
			},
			shouldReportUnexpectedController: false,
		},
		{
			name: "custom unspecified controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "chipmunks/v1",
					Kind:       "CustomController",
					Name:       "simon",
					UID:        "other-uid",
				},
			},
			shouldReportUnexpectedController: false,
		},
		{
			name: "other pod controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "simon",
					UID:        "other-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: true,
		},
		{
			name: "other set controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Set",
					Name:       "simon",
					UID:        "other-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: true,
		},
		{
			name: "own set controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "set",
					UID:        "set-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: false,
		},
		{
			name: "own set controller, stale uid",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "set",
					UID:        "stale-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: true,
		},
		{
			name: "own pod controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "pod-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: false,
		},
		{
			name: "own pod controller, stale uid",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "stale-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: true,
		},
		{
			// API 校验本应阻止两个控制器同时存在，这里仍覆盖该场景以保证逻辑完备。
			name: "own controller and another",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "pod-uid",
					Controller: ptr.To(true),
				},
				{
					APIVersion: "chipmunks/v1",
					Kind:       "CustomController",
					Name:       "simon",
					UID:        "other-uid",
					Controller: ptr.To(true),
				},
			},
			shouldReportUnexpectedController: true,
		},
		{
			name: "own controller and a non-controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "pod-uid",
					Controller: ptr.To(true),
				},
				{
					APIVersion: "chipmunks/v1",
					Kind:       "CustomController",
					Name:       "simon",
					UID:        "other-uid",
					Controller: ptr.To(false),
				},
			},
			shouldReportUnexpectedController: false,
		},
	}
	for _, tc := range testCases {
		target := &v1.PersistentVolumeClaim{}
		target.SetOwnerReferences(tc.refs)
		set := &apps.StatefulSet{}
		set.SetName("set")
		set.SetUID("set-uid")
		pod := &v1.Pod{}
		pod.SetName("pod")
		pod.SetUID("pod-uid")
		set.Spec.PersistentVolumeClaimRetentionPolicy = nil
		if hasUnexpectedController(target, set, pod) {
			t.Errorf("Any controller should be allowed when no retention policy (retain behavior) is specified. Incorrectly identified unexpected controller at %s", tc.name)
		}
		// 任意删除策略组合都应遵循同一套控制器判定规则。
		for _, policy := range []apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
			{WhenDeleted: "Retain", WhenScaled: "Delete"},
			{WhenDeleted: "Delete", WhenScaled: "Retain"},
			{WhenDeleted: "Delete", WhenScaled: "Delete"},
		} {
			set.Spec.PersistentVolumeClaimRetentionPolicy = &policy
			got := hasUnexpectedController(target, set, pod)
			if got != tc.shouldReportUnexpectedController {
				t.Errorf("Unexpected controller mismatch at %s (policy %v)", tc.name, policy)
			}
		}
	}
}

// TestNonController 验证 set 或 Pod 作为 owner 但非 controller 时的识别，以及无关引用不触发。
func TestNonController(t *testing.T) {
	testCases := []struct {
		name string
		refs []metav1.OwnerReference
		// set 和 Pod 对象分别以 "set" 和 "pod" 命名；只有 UID 参与匹配。
		setUID        types.UID
		podUID        types.UID
		nonController bool
	}{
		{
			// API 校验本应阻止两个控制器同时存在，这里仍覆盖该场景以保证语义完备。
			name: "set and pod controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "pod",
					Controller: ptr.To(true),
				},
				{
					APIVersion: "apps/v1",
					Kind:       "Set",
					Name:       "set",
					UID:        "set",
					Controller: ptr.To(true),
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: false,
		},
		{
			name: "set controller, pod noncontroller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "pod",
				},
				{
					APIVersion: "apps/v1",
					Kind:       "Set",
					Name:       "set",
					UID:        "set",
					Controller: ptr.To(true),
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: true,
		},
		{
			name: "set noncontroller, pod controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "pod",
					Controller: ptr.To(true),
				},
				{
					APIVersion: "apps/v1",
					Kind:       "Set",
					Name:       "set",
					UID:        "set",
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: true,
		},
		{
			name: "set controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Set",
					Name:       "set",
					UID:        "set",
					Controller: ptr.To(true),
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: false,
		},
		{
			name: "pod controller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       "pod",
					UID:        "pod",
					Controller: ptr.To(true),
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: false,
		},
		{
			name:          "nothing",
			refs:          []metav1.OwnerReference{},
			setUID:        "set",
			podUID:        "pod",
			nonController: false,
		},
		{
			name: "set noncontroller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Set",
					Name:       "set",
					UID:        "set",
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: true,
		},
		{
			name: "set noncontroller with ptr",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Set",
					Name:       "set",
					UID:        "set",
					Controller: ptr.To(false),
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: true,
		},
		{
			name: "pod noncontroller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "pod",
					Name:       "pod",
					UID:        "pod",
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: true,
		},
		{
			name: "other noncontroller",
			refs: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "pod",
					Name:       "pod",
					UID:        "not-matching",
				},
			},
			setUID:        "set",
			podUID:        "pod",
			nonController: false,
		},
	}

	for _, tc := range testCases {
		claim := v1.PersistentVolumeClaim{}
		claim.SetOwnerReferences(tc.refs)
		pod := v1.Pod{}
		pod.SetUID(tc.podUID)
		pod.SetName("pod")
		set := apps.StatefulSet{}
		set.SetUID(tc.setUID)
		set.SetName("set")
		got := hasNonControllerOwner(&claim, &set, &pod)
		if got != tc.nonController {
			t.Errorf("Failed %s: got %t, expected %t", tc.name, got, tc.nonController)
		}
	}
}

// TestHasStaleOwnerRef 验证 UID 变化视为过期引用，未关联的对象不算过期。
func TestHasStaleOwnerRef(t *testing.T) {
	target := v1.PersistentVolumeClaim{}
	target.SetOwnerReferences([]metav1.OwnerReference{
		{Name: "bob", UID: "123", APIVersion: "v1", Kind: "Pod"},
		{Name: "shirley", UID: "456", APIVersion: "v1", Kind: "Pod"},
	})
	ownerA := v1.Pod{}
	ownerA.SetUID("123")
	ownerA.Name = "bob"
	ownerB := v1.Pod{}
	ownerB.Name = "shirley"
	ownerB.SetUID("789")
	ownerC := v1.Pod{}
	ownerC.Name = "yvonne"
	ownerC.SetUID("345")
	if hasStaleOwnerRef(&target, &ownerA, podKind) {
		t.Error("ownerA should not be stale")
	}
	if !hasStaleOwnerRef(&target, &ownerB, podKind) {
		t.Error("ownerB should be stale")
	}
	if hasStaleOwnerRef(&target, &ownerC, podKind) {
		t.Error("ownerC should not be stale")
	}
}

// TestIsRunningAndReady 验证 Pod 必须同时处于 Running 阶段且 Ready 条件为真。
func TestIsRunningAndReady(t *testing.T) {
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 1)
	if isRunningAndReady(pod) {
		t.Error("isRunningAndReady does not respect Pod phase")
	}
	pod.Status.Phase = v1.PodRunning
	if isRunningAndReady(pod) {
		t.Error("isRunningAndReady does not respect Pod condition")
	}
	condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue}
	podutil.UpdatePodCondition(&pod.Status, &condition)
	if !isRunningAndReady(pod) {
		t.Error("Pod should be running and ready")
	}
}

// TestAscendingOrdinal 验证排序器能把乱序 Pod 按序号排列。
func TestAscendingOrdinal(t *testing.T) {
	set := newStatefulSet(10)
	pods := make([]*v1.Pod, 10)
	perm := rand.Perm(10)
	for i, v := range perm {
		pods[i] = newStatefulSetPod(set, v)
	}
	sort.Sort(ascendingOrdinal(pods))
	if !sort.IsSorted(ascendingOrdinal(pods)) {
		t.Error("ascendingOrdinal fails to sort Pods")
	}
}

// TestOverlappingStatefulSets 验证重叠 StatefulSet 按创建时间、再按名称排序以保证顺序稳定。
func TestOverlappingStatefulSets(t *testing.T) {
	sets := make([]*apps.StatefulSet, 10)
	perm := rand.Perm(10)
	for i, v := range perm {
		sets[i] = newStatefulSet(10)
		sets[i].CreationTimestamp = metav1.NewTime(sets[i].CreationTimestamp.Add(time.Duration(v) * time.Second))
	}
	sort.Sort(overlappingStatefulSets(sets))
	if !sort.IsSorted(overlappingStatefulSets(sets)) {
		t.Error("ascendingOrdinal fails to sort Pods")
	}
	for i, v := range perm {
		sets[i] = newStatefulSet(10)
		sets[i].Name = strconv.FormatInt(int64(v), 10)
	}
	sort.Sort(overlappingStatefulSets(sets))
	if !sort.IsSorted(overlappingStatefulSets(sets)) {
		t.Error("ascendingOrdinal fails to sort Pods")
	}
}

// TestNewPodControllerRef 验证新建 Pod 上的 controller 引用指向所属 StatefulSet。
func TestNewPodControllerRef(t *testing.T) {
	set := newStatefulSet(1)
	pod := newStatefulSetPod(set, 0)
	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		t.Fatalf("No ControllerRef found on new pod")
	}
	if got, want := controllerRef.APIVersion, apps.SchemeGroupVersion.String(); got != want {
		t.Errorf("controllerRef.APIVersion = %q, want %q", got, want)
	}
	if got, want := controllerRef.Kind, "StatefulSet"; got != want {
		t.Errorf("controllerRef.Kind = %q, want %q", got, want)
	}
	if got, want := controllerRef.Name, set.Name; got != want {
		t.Errorf("controllerRef.Name = %q, want %q", got, want)
	}
	if got, want := controllerRef.UID, set.UID; got != want {
		t.Errorf("controllerRef.UID = %q, want %q", got, want)
	}
	if got, want := *controllerRef.Controller, true; got != want {
		t.Errorf("controllerRef.Controller = %v, want %v", got, want)
	}
}

// TestApplyRevision 验证修订版本只恢复 spec，忽略元数据、状态和类型信息。
func TestApplyRevision(t *testing.T) {
	set := newStatefulSet(1)

	testCases := []struct {
		name       string
		sts        *apps.StatefulSet
		revisionFn func() *apps.ControllerRevision
		expectSts  *apps.StatefulSet
		expectErr  bool
	}{
		{
			name: "clean revision",
			sts:  set,
			revisionFn: func() *apps.ControllerRevision {
				revision, err := newRevision(set, 1, new(int32))
				if err != nil {
					panic(err)
				}
				return revision
			},
			expectSts: set,
		},
		{
			name: "empty revision",
			sts:  set,
			revisionFn: func() *apps.ControllerRevision {
				return &apps.ControllerRevision{}
			},
			expectSts: set,
		},
		{
			name: "malformed revision data",
			sts:  set,
			revisionFn: func() *apps.ControllerRevision {
				// 用数组代替 StatefulSet 对象，解码应失败。
				return &apps.ControllerRevision{
					Data: runtime.RawExtension{Raw: []byte(`[]`)},
				}
			},
			expectErr: true,
		},
		{
			name: "spec change is applied",
			sts:  set,
			revisionFn: func() *apps.ControllerRevision {
				changed := newStatefulSet(1)
				changed.Spec.Template.Spec.Containers[0].Image = "nginx:latest"
				changed.Spec.Template.Spec.Containers[0].Env = []v1.EnvVar{{Name: "foo", Value: "bar"}}
				revision, err := newRevision(changed, 1, new(int32))
				if err != nil {
					panic(err)
				}
				return revision
			},
			expectSts: func() *apps.StatefulSet {
				changed := newStatefulSet(1)
				changed.Spec.Template.Spec.Containers[0].Image = "nginx:latest"
				changed.Spec.Template.Spec.Containers[0].Env = []v1.EnvVar{{Name: "foo", Value: "bar"}}
				return changed
			}(),
		},
		{
			name: "metadata changes are ignored",
			sts:  set,
			revisionFn: func() *apps.ControllerRevision {
				// 元数据不属于 spec，恢复后应保留原对象的值。
				copy := set.DeepCopy()
				copy.ObjectMeta.Namespace = "non-default-namespace"
				copy.ObjectMeta.UID = "non-default-uid"
				copy.Annotations = map[string]string{"injected": "true"}
				buf := new(bytes.Buffer)
				enc := json.NewEncoder(buf)
				if err := enc.Encode(copy); err != nil {
					panic(err)
				}
				return &apps.ControllerRevision{
					Data: runtime.RawExtension{Raw: buf.Bytes()},
				}
			},
			expectSts: set,
		},
		{
			name: "status changes are ignored",
			sts: func() *apps.StatefulSet {
				s := newStatefulSet(1)
				cc := int32(1)
				s.Status.CollisionCount = &cc
				s.Status.ObservedGeneration = 5
				return s
			}(),
			revisionFn: func() *apps.ControllerRevision {
				copy := set.DeepCopy()
				cc := int32(99)
				copy.Status.CollisionCount = &cc
				copy.Status.ObservedGeneration = 100
				buf := new(bytes.Buffer)
				enc := json.NewEncoder(buf)
				if err := enc.Encode(copy); err != nil {
					panic(err)
				}
				return &apps.ControllerRevision{
					Data: runtime.RawExtension{Raw: buf.Bytes()},
				}
			},
			expectSts: func() *apps.StatefulSet {
				s := newStatefulSet(1)
				cc := int32(1)
				s.Status.CollisionCount = &cc
				s.Status.ObservedGeneration = 5
				return s
			}(),
		},
		{
			name: "typemeta changes are ignored",
			sts:  set,
			revisionFn: func() *apps.ControllerRevision {
				copy := set.DeepCopy()
				copy.TypeMeta.Kind = "NotAStatefulSet"
				copy.TypeMeta.APIVersion = "v0fake"
				buf := new(bytes.Buffer)
				enc := json.NewEncoder(buf)
				if err := enc.Encode(copy); err != nil {
					panic(err)
				}
				return &apps.ControllerRevision{
					Data: runtime.RawExtension{Raw: buf.Bytes()},
				}
			},
			expectSts: set,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			revision := tc.revisionFn()
			restoredSet, err := ApplyRevision(tc.sts, revision)
			if tc.expectErr && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tc.expectErr && err != nil {
				t.Errorf("unexpected error %s", err)
			}
			if tc.expectErr {
				return
			}
			if !apiequality.Semantic.DeepEqual(tc.expectSts, restoredSet) {
				t.Errorf("unexpected diff between expected and restored: %s", cmp.Diff(tc.expectSts, restoredSet))
			}
		})
	}
}

// TestCreateApplyRevision 验证修订版本经过恢复再创建后保持一致，且注解能够保留。
func TestCreateApplyRevision(t *testing.T) {
	set := newStatefulSet(1)
	set.Status.CollisionCount = new(int32)
	revision, err := newRevision(set, 1, set.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	set.Spec.Template.Spec.Containers[0].Name = "foo"
	if set.Annotations == nil {
		set.Annotations = make(map[string]string)
	}
	key := "foo"
	expectedValue := "bar"
	set.Annotations[key] = expectedValue
	restoredSet, err := ApplyRevision(set, revision)
	if err != nil {
		t.Fatal(err)
	}
	restoredRevision, err := newRevision(restoredSet, 2, restoredSet.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	if !history.EqualRevision(revision, restoredRevision) {
		t.Errorf("wanted %v got %v", string(revision.Data.Raw), string(restoredRevision.Data.Raw))
	}
	value, ok := restoredRevision.Annotations[key]
	if !ok {
		t.Errorf("missing annotation %s", key)
	}
	if value != expectedValue {
		t.Errorf("for annotation %s wanted %s got %s", key, expectedValue, value)
	}
}

// TestGetPersistentVolumeClaims 验证 PVC 从模板生成，且标签按 StatefulSet 优先的规则合并。
func TestGetPersistentVolumeClaims(t *testing.T) {

	// 模板未设标签时继承 StatefulSet 标签（此处 selector 也为 nil）。
	pod := newPod()
	statefulSet := newStatefulSet(1)
	statefulSet.Spec.Selector.MatchLabels = nil
	claims := getPersistentVolumeClaims(statefulSet, pod)
	pvc := newPVC("datadir-foo-0")
	resultClaims := map[string]v1.PersistentVolumeClaim{"datadir": pvc}

	if !reflect.DeepEqual(claims, resultClaims) {
		t.Fatalf("Unexpected pvc:\n %+v\n, desired pvc:\n %+v", claims, resultClaims)
	}

	// 模板未设标签时继承 StatefulSet selector 的标签。
	statefulSet.Spec.Selector.MatchLabels = map[string]string{"test": "test"}
	claims = getPersistentVolumeClaims(statefulSet, pod)
	pvc.SetLabels(map[string]string{"test": "test"})
	resultClaims = map[string]v1.PersistentVolumeClaim{"datadir": pvc}
	if !reflect.DeepEqual(claims, resultClaims) {
		t.Fatalf("Unexpected pvc:\n %+v\n, desired pvc:\n %+v", claims, resultClaims)
	}

	// 标签不冲突时，模板标签与 selector 标签合并。
	statefulSet.Spec.Selector.MatchLabels = map[string]string{"name": "foo"}
	statefulSet.Spec.VolumeClaimTemplates[0].ObjectMeta.Labels = map[string]string{"test": "test"}
	claims = getPersistentVolumeClaims(statefulSet, pod)
	pvc.SetLabels(map[string]string{"test": "test", "name": "foo"})
	resultClaims = map[string]v1.PersistentVolumeClaim{"datadir": pvc}
	if !reflect.DeepEqual(claims, resultClaims) {
		t.Fatalf("Unexpected pvc:\n %+v\n, desired pvc:\n %+v", claims, resultClaims)
	}

	// 标签冲突时以 StatefulSet selector 的值为准。
	statefulSet.Spec.Selector.MatchLabels = map[string]string{"test": "foo"}
	statefulSet.Spec.VolumeClaimTemplates[0].ObjectMeta.Labels = map[string]string{"test": "test"}
	claims = getPersistentVolumeClaims(statefulSet, pod)
	pvc.SetLabels(map[string]string{"test": "foo"})
	resultClaims = map[string]v1.PersistentVolumeClaim{"datadir": pvc}
	if !reflect.DeepEqual(claims, resultClaims) {
		t.Fatalf("Unexpected pvc:\n %+v\n, desired pvc:\n %+v", claims, resultClaims)
	}
}

// newPod 构造一个 name 为 foo-0 的最小测试 Pod。
func newPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-0",
			Namespace: v1.NamespaceDefault,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
				},
			},
		},
	}
}

// newPVC 构造一个请求 1 单位存储的测试 PVC。
func newPVC(name string) v1.PersistentVolumeClaim {
	return v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: v1.NamespaceDefault,
			Name:      name,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
				},
			},
		},
	}
}

// newStatefulSetWithVolumes 构造带 PVC 卷和普通卷挂载的测试 StatefulSet。
func newStatefulSetWithVolumes(replicas int32, name string, petMounts []v1.VolumeMount, podMounts []v1.VolumeMount) *apps.StatefulSet {
	// petMounts 对应 PVC 模板，podMounts 对应 HostPath 普通卷。
	mounts := append(petMounts, podMounts...)
	claims := []v1.PersistentVolumeClaim{}
	for _, m := range petMounts {
		claims = append(claims, newPVC(m.Name))
	}

	vols := []v1.Volume{}
	for _, m := range podMounts {
		vols = append(vols, v1.Volume{
			Name: m.Name,
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: fmt.Sprintf("/tmp/%v", m.Name),
				},
			},
		})
	}

	template := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:         "nginx",
					Image:        "nginx",
					VolumeMounts: mounts,
				},
			},
			Volumes: vols,
		},
	}

	template.Labels = map[string]string{"foo": "bar"}

	return &apps.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: v1.NamespaceDefault,
			UID:       types.UID("test"),
		},
		Spec: apps.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"foo": "bar"},
			},
			Replicas:             ptr.To(replicas),
			Template:             template,
			VolumeClaimTemplates: claims,
			ServiceName:          "governingsvc",
			UpdateStrategy:       apps.StatefulSetUpdateStrategy{Type: apps.RollingUpdateStatefulSetStrategyType},
			PersistentVolumeClaimRetentionPolicy: &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			RevisionHistoryLimit: func() *int32 {
				limit := int32(2)
				return &limit
			}(),
		},
	}
}

// newStatefulSet 构造名为 foo、带 datadir PVC 和 home 普通卷的测试 StatefulSet。
func newStatefulSet(replicas int32) *apps.StatefulSet {
	petMounts := []v1.VolumeMount{
		{Name: "datadir", MountPath: "/tmp/zookeeper"},
	}
	podMounts := []v1.VolumeMount{
		{Name: "home", MountPath: "/home"},
	}
	return newStatefulSetWithVolumes(replicas, "foo", petMounts, podMounts)
}

// newStatefulSetWithLabels 构造使用集合型 selector 的测试 StatefulSet。
func newStatefulSetWithLabels(replicas int32, name string, uid types.UID, labels map[string]string) *apps.StatefulSet {
	// 把 map 形式的选择条件转换为 set-based 表达式。
	var testMatchExpressions []metav1.LabelSelectorRequirement
	for key, value := range labels {
		sel := metav1.LabelSelectorRequirement{
			Key:      key,
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{value},
		}
		testMatchExpressions = append(testMatchExpressions, sel)
	}
	return &apps.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: v1.NamespaceDefault,
			UID:       uid,
		},
		Spec: apps.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				// 故意不设 MatchLabels：若调用链中任一环节忽略 MatchExpressions，测试就会失败。
				MatchLabels:      nil,
				MatchExpressions: testMatchExpressions,
			},
			Replicas: ptr.To(replicas),
			PersistentVolumeClaimRetentionPolicy: &apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  apps.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "nginx",
							Image: "nginx",
							VolumeMounts: []v1.VolumeMount{
								{Name: "datadir", MountPath: "/tmp/"},
								{Name: "home", MountPath: "/home"},
							},
						},
					},
					Volumes: []v1.Volume{{
						Name: "home",
						VolumeSource: v1.VolumeSource{
							HostPath: &v1.HostPathVolumeSource{
								Path: fmt.Sprintf("/tmp/%v", "home"),
							},
						}}},
				},
			},
			VolumeClaimTemplates: []v1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "datadir"},
					Spec: v1.PersistentVolumeClaimSpec{
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
							},
						},
					},
				},
			},
			ServiceName: "governingsvc",
		},
	}
}

// TestGetStatefulSetMaxUnavailable 验证整数和百分比形式的最大不可用副本数计算及下限。
func TestGetStatefulSetMaxUnavailable(t *testing.T) {
	testCases := []struct {
		maxUnavailable         *intstr.IntOrString
		replicaCount           int
		expectedMaxUnavailable int
	}{
		// 0 和 0% 虽被 API 校验禁止，仍覆盖以确认下限行为。
		{maxUnavailable: nil, replicaCount: 10, expectedMaxUnavailable: 1},
		{maxUnavailable: ptr.To(intstr.FromInt32(3)), replicaCount: 10, expectedMaxUnavailable: 3},
		{maxUnavailable: ptr.To(intstr.FromInt32(3)), replicaCount: 0, expectedMaxUnavailable: 3},
		{maxUnavailable: ptr.To(intstr.FromInt32(0)), replicaCount: 0, expectedMaxUnavailable: 1},
		{maxUnavailable: ptr.To(intstr.FromString("10%")), replicaCount: 25, expectedMaxUnavailable: 2},
		{maxUnavailable: ptr.To(intstr.FromString("100%")), replicaCount: 5, expectedMaxUnavailable: 5},
		{maxUnavailable: ptr.To(intstr.FromString("50%")), replicaCount: 5, expectedMaxUnavailable: 2},
		{maxUnavailable: ptr.To(intstr.FromString("10%")), replicaCount: 5, expectedMaxUnavailable: 1},
		{maxUnavailable: ptr.To(intstr.FromString("1%")), replicaCount: 0, expectedMaxUnavailable: 1},
		{maxUnavailable: ptr.To(intstr.FromString("0%")), replicaCount: 0, expectedMaxUnavailable: 1},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			gotMaxUnavailable, err := getStatefulSetMaxUnavailable(tc.maxUnavailable, tc.replicaCount)
			if err != nil {
				t.Fatal(err)
			}
			if gotMaxUnavailable != tc.expectedMaxUnavailable {
				t.Errorf("Expected maxUnavailable %v, got pods %v", tc.expectedMaxUnavailable, gotMaxUnavailable)
			}
		})
	}
}

// TestCompleteUpdate 验证全部副本更新且就绪时才推进 currentRevision。
func TestCompleteUpdate(t *testing.T) {
	tests := []struct {
		name            string
		strategyType    apps.StatefulSetUpdateStrategyType
		replicas        int32
		readyReplicas   int32
		updatedReplicas int32
		totalReplicas   int32
		currentRevision string
		updateRevision  string
		expectPromoted  bool
	}{
		{
			name:            "RollingUpdate: all replicas updated and ready",
			strategyType:    apps.RollingUpdateStatefulSetStrategyType,
			replicas:        3,
			readyReplicas:   3,
			updatedReplicas: 3,
			totalReplicas:   3,
			currentRevision: "rev-1",
			updateRevision:  "rev-2",
			expectPromoted:  true,
		},
		{
			name:            "OnDelete: all replicas updated and ready",
			strategyType:    apps.OnDeleteStatefulSetStrategyType,
			replicas:        3,
			readyReplicas:   3,
			updatedReplicas: 3,
			totalReplicas:   3,
			currentRevision: "rev-1",
			updateRevision:  "rev-2",
			expectPromoted:  true,
		},
		{
			name:            "OnDelete: partial update",
			strategyType:    apps.OnDeleteStatefulSetStrategyType,
			replicas:        3,
			readyReplicas:   3,
			updatedReplicas: 1,
			totalReplicas:   3,
			currentRevision: "rev-1",
			updateRevision:  "rev-2",
			expectPromoted:  false,
		},
		{
			name:            "RollingUpdate: not all ready",
			strategyType:    apps.RollingUpdateStatefulSetStrategyType,
			replicas:        3,
			readyReplicas:   2,
			updatedReplicas: 3,
			totalReplicas:   3,
			currentRevision: "rev-1",
			updateRevision:  "rev-2",
			expectPromoted:  false,
		},
		{
			name:            "not all replicas created",
			strategyType:    apps.RollingUpdateStatefulSetStrategyType,
			replicas:        3,
			readyReplicas:   2,
			updatedReplicas: 2,
			totalReplicas:   2,
			currentRevision: "rev-1",
			updateRevision:  "rev-2",
			expectPromoted:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			set := newStatefulSet(tc.replicas)
			set.Spec.UpdateStrategy.Type = tc.strategyType

			status := apps.StatefulSetStatus{
				Replicas:        tc.totalReplicas,
				ReadyReplicas:   tc.readyReplicas,
				UpdatedReplicas: tc.updatedReplicas,
				CurrentRevision: tc.currentRevision,
				UpdateRevision:  tc.updateRevision,
			}
			completeUpdate(set, &status)

			if tc.expectPromoted {
				if status.CurrentRevision != tc.updateRevision {
					t.Errorf("expected CurrentRevision to be promoted to %q, got %q", tc.updateRevision, status.CurrentRevision)
				}
				if status.CurrentReplicas != tc.updatedReplicas {
					t.Errorf("expected CurrentReplicas to be %d, got %d", tc.updatedReplicas, status.CurrentReplicas)
				}
			} else if status.CurrentRevision != tc.currentRevision {
				t.Errorf("expected CurrentRevision to remain %q, got %q", tc.currentRevision, status.CurrentRevision)
			}
		})
	}
}
