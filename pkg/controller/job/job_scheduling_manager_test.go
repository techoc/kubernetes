/*
Copyright The Kubernetes Authors.

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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	apischeduling "k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/features"
)

// newGangSchedulingJob 构造一个通过 spec.scheduling 选择 gang 调度的 Job，
// 其 minCount 与 parallelism 保持一致，便于验证默认策略。
func newGangSchedulingJob(name string, parallelism int32) *batch.Job {
	indexed := batch.IndexedCompletion
	return &batch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name + "-uid"),
		},
		Spec: batch.JobSpec{
			Parallelism:    new(parallelism),
			Completions:    new(parallelism),
			CompletionMode: &indexed,
			Scheduling: &batch.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
					Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: new(parallelism)},
				},
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"job-name": name},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"job-name": name},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{Image: "test"}},
				},
			},
		},
	}
}

// newWorkloadJobControllerRef 构造指向 Job 的控制器引用，供 Workload.Spec.ControllerRef 使用，
// 以便控制器识别该 Workload 归属哪个 Job。
func newWorkloadJobControllerRef(jobName string) *schedulingv1beta1.TypedLocalObjectReference {
	return &schedulingv1beta1.TypedLocalObjectReference{
		APIGroup: batch.SchemeGroupVersion.Group,
		Kind:     "Job",
		Name:     jobName,
	}
}

// newControllerWithSchedulingInformers 创建接入 Workload 与 PodGroup informer 的 Job 控制器，
// 并开启相关特性门控与仿真版本，便于测试调度管理逻辑。
// 将各存储同步函数置为 alwaysReady，避免测试中等待 informer 同步。
func newControllerWithSchedulingInformers(ctx context.Context, t *testing.T, kubeClient *fake.Clientset) (*Controller, informers.SharedInformerFactory) {
	t.Helper()
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.36"))
	featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
		features.WorkloadWithJob: true,
	})
	sharedInformers := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
	jm, err := newControllerWithClock(ctx,
		kubeClient,
		realClock,
		sharedInformers.Core().V1().Pods(),
		sharedInformers.Batch().V1().Jobs(),
		sharedInformers.Scheduling().V1beta1().Workloads(),
		sharedInformers.Scheduling().V1beta1().PodGroups(),
	)
	if err != nil {
		t.Fatalf("Error creating Job controller: %v", err)
	}
	jm.podControl = &controller.FakePodControl{}
	jm.podStoreSynced = alwaysReady
	jm.jobStoreSynced = alwaysReady
	jm.workloadStoreSynced = alwaysReady
	jm.podGroupStoreSynced = alwaysReady
	return jm, sharedInformers
}

// TestShouldManageWorkloadForJob 验证不同特性门控与 Job 形态下的管理模式：
// 独立 Job 同时纳管 Workload/PodGroup，被父级 Workload 拥有或自带调度组的 Job 则不纳管或仅纳管 PodGroup。
func TestShouldManageWorkloadForJob(t *testing.T) {
	// 为父级 Workload 与 CronJob 各准备一组 ownerRef，用于覆盖委托与父级拥有的场景；
	// 只有控制者 owner 才参与归属判断。
	parentWorkloadOwner := []metav1.OwnerReference{{
		APIVersion: "jobset.x-k8s.io/v1alpha2",
		Kind:       "JobSet",
		Name:       "js",
		Controller: new(true),
	}}
	cronJobOwner := []metav1.OwnerReference{{
		APIVersion: "batch/v1",
		Kind:       "CronJob",
		Name:       "cj",
		Controller: new(true),
	}}

	tests := map[string]struct {
		workloadWithJob bool
		job             *batch.Job
		wantMode        managementMode
	}{
		"gate enabled, standalone Job -> managed (Universal Representation)": {
			workloadWithJob: true,
			job:             &batch.Job{Spec: batch.JobSpec{Parallelism: new(int32(4))}},
			wantMode:        manageBoth,
		},
		"gate enabled, basic single-pod Job -> still managed": {
			workloadWithJob: true,
			job:             &batch.Job{Spec: batch.JobSpec{Parallelism: new(int32(1))}},
			wantMode:        manageBoth,
		},
		"feature gate disabled -> not managed": {
			workloadWithJob: false,
			job:             &batch.Job{Spec: batch.JobSpec{Parallelism: new(int32(4))}},
			wantMode:        manageNone,
		},
		"BYO PodGroup via pod template schedulingGroup -> not managed": {
			workloadWithJob: true,
			job: &batch.Job{
				Spec: batch.JobSpec{
					Parallelism: new(int32(4)),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							SchedulingGroup: &v1.PodSchedulingGroup{PodGroupName: new("existing-pg")},
						},
					},
				},
			},
			wantMode: manageNone,
		},
		"BYO schedulingGroup takes precedence over delegation annotation -> not managed": {
			workloadWithJob: true,
			job: &batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: parentWorkloadOwner,
					Annotations:     map[string]string{apischeduling.GroupTemplateNameAnnotation: "workers"},
				},
				Spec: batch.JobSpec{
					Parallelism: new(int32(4)),
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							SchedulingGroup: &v1.PodSchedulingGroup{PodGroupName: new("existing-pg")},
						},
					},
				},
			},
			wantMode: manageNone,
		},
		"parent-owned Workload without delegation annotation -> not managed": {
			workloadWithJob: true,
			job: &batch.Job{
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: parentWorkloadOwner},
				Spec:       batch.JobSpec{Parallelism: new(int32(4))},
			},
			wantMode: manageNone,
		},
		"parent-owned Workload with delegation annotation -> managed (delegated PodGroup)": {
			workloadWithJob: true,
			job: &batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: parentWorkloadOwner,
					Annotations:     map[string]string{apischeduling.GroupTemplateNameAnnotation: "workers"},
				},
				Spec: batch.JobSpec{Parallelism: new(int32(4))},
			},
			wantMode: managePodGroupOnly,
		},
		"CronJob-owned Job is treated as parent-owned (not managed)": {
			workloadWithJob: true,
			job: &batch.Job{
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: cronJobOwner},
				Spec:       batch.JobSpec{Parallelism: new(int32(4))},
			},
			wantMode: manageNone,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// 管理模式依赖特性门控，每个子用例都单独设置，避免相互干扰。
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, utilversion.MustParse("1.36"))
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: tc.workloadWithJob,
				features.WorkloadWithJob: tc.workloadWithJob,
			})
			if got := getManagementMode(tc.job); got != tc.wantMode {
				t.Errorf("getManagementMode() = %v, want %v", got, tc.wantMode)
			}
			want := tc.wantMode != manageNone
			if got := shouldManageWorkloadForJob(tc.job); got != want {
				t.Errorf("shouldManageWorkloadForJob() = %v, want %v", got, want)
			}
		})
	}
}

// TestGenerateWorkload 验证根据 Job 的调度配置生成 Workload 的正确性：
// 覆盖默认 Basic 策略、Gang 策略及其 minCount 默认值、约束透传以及优先级类名复制。
func TestGenerateWorkload(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientSet := fake.NewClientset()
	jm, _ := newControllerWithSchedulingInformers(ctx, t, clientSet)

	// 集中构造不同调度配置的 Job，避免在用例表中重复样板代码。
	newJob := func(name string, parallelism int32, scheduling *batch.JobSchedulingConfiguration) *batch.Job {
		return &batch.Job{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metav1.NamespaceDefault, UID: types.UID(name + "-uid")},
			Spec:       batch.JobSpec{Parallelism: new(parallelism), Scheduling: scheduling},
		}
	}

	// 期望字段只描述本用例关心的部分；例如 wantGangMinCount 为 0 表示
	// 该用例期望 Basic 策略，而 wantErr 用于需要生成失败的场景。
	testCases := map[string]struct {
		job                   *batch.Job
		wantErr               bool
		wantBasic             bool
		wantGangMinCount      int32
		wantTopologyKey       string
		wantAllDisrupt        bool
		wantClaimNames        []string
		wantPriorityClassName string
	}{
		"nil scheduling defaults to Basic": {
			job:       newJob("basic-default", 4, nil),
			wantBasic: true,
		},
		"scheduling set but policy nil defaults to Basic": {
			job:       newJob("basic-nil-policy", 4, &batch.JobSchedulingConfiguration{}),
			wantBasic: true,
		},
		"explicit basic policy": {
			job: newJob("basic-explicit", 4, &batch.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Basic: &schedulingv1alpha3.WorkloadPodGroupBasicSchedulingPolicy{}},
			}),
			wantBasic: true,
		},
		"gang with explicit minCount": {
			job: newJob("gang", 4, &batch.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: new(int32(3))}},
			}),
			wantGangMinCount: 3,
		},
		"gang without minCount defaults to parallelism": {
			job: newJob("gang-zero", 4, &batch.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{}},
			}),
			wantGangMinCount: 4,
		},
		"gang without minCount and zero parallelism clamps to 1": {
			job: newJob("gang-zero-parallelism", 0, &batch.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{}},
			}),
			wantGangMinCount: 1,
		},
		"gang, topology, disruption, and claims passthrough": {
			job: newJob("full", 4, &batch.JobSchedulingConfiguration{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: new(int32(4))}},
				SchedulingConstraints: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
					Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}},
				},
				DisruptionMode: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{}},
				ResourceClaims: []schedulingv1alpha3.WorkloadPodGroupResourceClaim{{Name: "gpu", ResourceClaimName: new("my-claim")}},
			}),
			wantGangMinCount: 4,
			wantTopologyKey:  "topology.kubernetes.io/zone",
			wantAllDisrupt:   true,
			wantClaimNames:   []string{"gpu"},
		},
		"priorityClassName copied from pod template": {
			job: func() *batch.Job {
				j := newJob("prio", 4, nil)
				j.Spec.Template.Spec.PriorityClassName = "high-priority"
				return j
			}(),
			wantBasic:             true,
			wantPriorityClassName: "high-priority",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			wl, err := jm.generateWorkload(tc.job)
			// 先处理期望失败的场景，避免后续对空 Workload 解引用。
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(wl.Spec.PodGroupTemplates) != 1 {
				t.Fatalf("expected 1 template, got %d", len(wl.Spec.PodGroupTemplates))
			}
			// 模板名必须可复算，否则后续无法通过名字找到匹配的 PodGroup。
			tpl := wl.Spec.PodGroupTemplates[0]
			if tpl.Name != podGroupTemplateName(tc.job) {
				t.Errorf("template name = %q, want %q", tpl.Name, podGroupTemplateName(tc.job))
			}
			if tc.wantBasic {
				if tpl.SchedulingPolicy.Basic == nil {
					t.Errorf("expected Basic policy, got %+v", tpl.SchedulingPolicy)
				}
			} else {
				if tpl.SchedulingPolicy.Gang == nil {
					t.Fatalf("expected Gang policy, got %+v", tpl.SchedulingPolicy)
				}
				if tpl.SchedulingPolicy.Gang.MinCount != tc.wantGangMinCount {
					t.Errorf("gang minCount = %d, want %d", tpl.SchedulingPolicy.Gang.MinCount, tc.wantGangMinCount)
				}
			}
			if tc.wantTopologyKey != "" {
				if tpl.SchedulingConstraints == nil || len(tpl.SchedulingConstraints.Topology) != 1 ||
					tpl.SchedulingConstraints.Topology[0].Key != tc.wantTopologyKey {
					t.Errorf("topology = %+v, want key %q", tpl.SchedulingConstraints, tc.wantTopologyKey)
				}
			}
			if tc.wantAllDisrupt && (tpl.DisruptionMode == nil || tpl.DisruptionMode.All == nil) {
				t.Errorf("expected All disruption mode, got %+v", tpl.DisruptionMode)
			}
			if len(tc.wantClaimNames) > 0 {
				if len(tpl.ResourceClaims) != len(tc.wantClaimNames) {
					t.Fatalf("resourceClaims = %d, want %d", len(tpl.ResourceClaims), len(tc.wantClaimNames))
				}
				for i, want := range tc.wantClaimNames {
					if tpl.ResourceClaims[i].Name != want {
						t.Errorf("claim[%d].Name = %q, want %q", i, tpl.ResourceClaims[i].Name, want)
					}
				}
			}
			if tpl.PriorityClassName != tc.wantPriorityClassName {
				t.Errorf("priorityClassName = %q, want %q", tpl.PriorityClassName, tc.wantPriorityClassName)
			}
			if wl.Spec.ControllerRef == nil || wl.Spec.ControllerRef.Name != tc.job.Name {
				t.Errorf("unexpected controllerRef: %+v", wl.Spec.ControllerRef)
			}
		})
	}
}

// TestComputeWorkloadName 验证 Workload 名称满足 DNS 子域长度限制、带哈希后缀、
// 结果确定且随 UID 变化，避免同名 Job 复用后发生名称冲突。
func TestComputeWorkloadName(t *testing.T) {
	tests := map[string]struct {
		jobName string
		jobUID  types.UID
	}{
		"short name": {
			jobName: "my-job",
			jobUID:  "uid-123",
		},
		"very long name exceeding DNS limit": {
			jobName: strings.Repeat("a", 300),
			jobUID:  "uid-long",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			job := &batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: tc.jobName,
					UID:  tc.jobUID,
				},
			}
			result := computeWorkloadName(job)

			// 名称必须是合法的 DNS 子域，否则 API Server 会拒绝创建。
			if len(result) > validation.DNS1123SubdomainMaxLength {
				t.Errorf("name length %d exceeds DNS1123SubdomainMaxLength %d", len(result), validation.DNS1123SubdomainMaxLength)
			}

			// 名称须含哈希后缀，长名字截断后才能保持唯一。
			parts := strings.Split(result, "-")
			if len(parts) < 2 {
				t.Errorf("expected at least 2 parts (prefix-hash), got %q", result)
			}

			// 名称必须确定，重复计算才能定位到同一个 Workload。
			result2 := computeWorkloadName(job)
			if result != result2 {
				t.Errorf("non-deterministic: %q != %q", result, result2)
			}

			// 同名重建的 Job 必须靠 UID 区分，否则会误用旧 Workload。
			job2 := job.DeepCopy()
			job2.UID = "different-uid"
			result3 := computeWorkloadName(job2)
			if result == result3 {
				t.Error("different UIDs should produce different names")
			}
		})
	}
}

// TestComputePodGroupName 验证 PodGroup 名称的长度限制、命名结构、确定性，
// 以及截断时同时保留 Workload 名与模板名，避免两者同名串扰。
func TestComputePodGroupName(t *testing.T) {
	tests := map[string]struct {
		workloadName string
		templateName string
	}{
		"short names": {
			workloadName: "my-workload",
			templateName: "my-template",
		},
		"long workload name": {
			workloadName: strings.Repeat("w", 250),
			templateName: "tpl",
		},
		"long template name": {
			workloadName: "wl",
			templateName: strings.Repeat("t", 250),
		},
		"both long names": {
			workloadName: strings.Repeat("w", 200),
			templateName: strings.Repeat("t", 200),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := computePodGroupName(tc.workloadName, tc.templateName)

			if len(result) > validation.DNS1123SubdomainMaxLength {
				t.Errorf("name length %d exceeds DNS1123SubdomainMaxLength %d", len(result), validation.DNS1123SubdomainMaxLength)
			}

			parts := strings.Split(result, "-")
			if len(parts) < 3 {
				t.Errorf("expected at least 3 parts (wl-tpl-hash), got %q", result)
			}

			result2 := computePodGroupName(tc.workloadName, tc.templateName)
			if result != result2 {
				t.Errorf("non-deterministic: %q != %q", result, result2)
			}

			result3 := computePodGroupName(tc.workloadName, "other-template")
			if result == result3 {
				t.Error("different template names should produce different PodGroup names")
			}
		})
	}

	// 超长名字截断时必须同时保留两段来源信息，便于从名称追溯归属。
	t.Run("truncation preserves both parts", func(t *testing.T) {
		result := computePodGroupName(strings.Repeat("w", 200), strings.Repeat("t", 200))
		lastDash := strings.LastIndex(result, "-")
		if lastDash < 0 {
			t.Fatalf("expected at least one dash in %q", result)
		}
		prefix := result[:lastDash]
		if !strings.Contains(prefix, "www") {
			t.Errorf("expected truncated workload name in prefix %q", prefix)
		}
		if !strings.Contains(prefix, "ttt") {
			t.Errorf("expected truncated template name in prefix %q", prefix)
		}
	})

	// 短名字本身不携带歧义，截断时应优先牺牲另一段长名字。
	t.Run("short workload name is not truncated", func(t *testing.T) {
		result := computePodGroupName("wl", strings.Repeat("t", 250))
		if !strings.HasPrefix(result, "wl-") {
			t.Errorf("short workload name should not be truncated, got %q", result)
		}
	})
}

// TestEnsureWorkloadAndPodGroup 验证 ensureWorkloadAndPodGroup 的行为：
// 仅在 Job 尚未启动且无 Pod 时创建 Workload/PodGroup，能识别已有对象，
// 对多义或非本控制器创建的对象回退重建，并正确处理委托场景与创建错误。
func TestEnsureWorkloadAndPodGroup(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	baseJob := newGangSchedulingJob("test-job", 4)
	workloadName := computeWorkloadName(baseJob)
	templateName := fmt.Sprintf("%s-pgt-%d", baseJob.Name, 0)
	podGroupName := computePodGroupName(workloadName, templateName)

	// 构造由本控制器持有的 Workload，用于模拟"已存在的目标对象"，
	// 因为发现逻辑依赖 ownerRef 归属。
	makeWorkload := func(name, jobName string) *schedulingv1beta1.Workload {
		return &schedulingv1beta1.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       metav1.NamespaceDefault,
				UID:             types.UID(name + "-uid"),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(baseJob, controllerKind)},
			},
			Spec: schedulingv1beta1.WorkloadSpec{
				ControllerRef: newWorkloadJobControllerRef(jobName),
				PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{
					{
						Name: templateName,
						SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
							Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 4},
						},
					},
				},
			},
		}
	}

	// PodGroup 通过 WorkloadRef 关联模板，构造时需同时指向 Workload 与模板名。
	makePodGroup := func(name, wlName string) *schedulingv1beta1.PodGroup {
		return &schedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       metav1.NamespaceDefault,
				UID:             types.UID(name + "-uid"),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(baseJob, controllerKind)},
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				WorkloadRef: &schedulingv1beta1.WorkloadReference{
					WorkloadName: wlName,
					TemplateName: templateName,
				},
			},
		}
	}

	// 委托场景：Job 由父级 Workload 拥有，并通过注解指定要继承的模板名。
	delegatedJob := baseJob.DeepCopy()
	delegatedJob.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "jobset.x-k8s.io/v1alpha2",
		Kind:       "JobSet",
		Name:       "parent-js",
		UID:        types.UID("parent-js-uid"),
		Controller: new(true),
	}}
	delegatedJob.Annotations = map[string]string{apischeduling.GroupTemplateNameAnnotation: "workers"}

	// 父级 Workload 不属于本控制器，其模板作为委托 PodGroup 的配置来源。
	makeParentWorkload := func(minCount int32) *schedulingv1beta1.Workload {
		return &schedulingv1beta1.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "parent-workload",
				Namespace: metav1.NamespaceDefault,
				UID:       types.UID("parent-wl-uid"),
			},
			Spec: schedulingv1beta1.WorkloadSpec{
				ControllerRef: &schedulingv1beta1.TypedLocalObjectReference{
					APIGroup: "jobset.x-k8s.io",
					Kind:     "JobSet",
					Name:     "parent-js",
				},
				PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{{
					Name: "workers",
					SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
						Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: minCount},
					},
				}},
			},
		}
	}
	delegatedPodGroupName := computePodGroupName("parent-workload", "workers")
	// 委托 PodGroup 名称由父 Workload 与模板名推导，与 Job 自身名称无关。
	makeDelegatedPodGroup := func(minCount int32) *schedulingv1beta1.PodGroup {
		return &schedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            delegatedPodGroupName,
				Namespace:       metav1.NamespaceDefault,
				UID:             types.UID("delegated-pg-uid"),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(delegatedJob, controllerKind)},
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				WorkloadRef: &schedulingv1beta1.WorkloadReference{
					WorkloadName: "parent-workload",
					TemplateName: "workers",
				},
				SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
					Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: minCount},
				},
			},
		}
	}

	testCases := map[string]struct {
		job               *batch.Job
		pods              []*v1.Pod
		existingWorkloads []*schedulingv1beta1.Workload
		existingPodGroups []*schedulingv1beta1.PodGroup

		// 为 true 时向 Workload 创建注入错误，用于验证错误向上传播。
		simulateCreateError bool
		// 为 true 时向 PodGroup 创建注入错误，验证独立于 Workload 的错误路径。
		simulatePGCreateError bool

		wantPodGroup             bool
		wantPodGroupName         string
		wantPodGroupGangMinCount *int32
		wantErr                  bool
	}{
		"creates both Workload and PodGroup when no pods exist": {
			job:              baseJob,
			wantPodGroup:     true,
			wantPodGroupName: podGroupName,
		},
		"skips creation when Job has pods but no Workload": {
			job:  baseJob,
			pods: []*v1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: metav1.NamespaceDefault}}},
		},
		"skips creation when Job has StartTime set": {
			job: func() *batch.Job {
				j := baseJob.DeepCopy()
				now := metav1.Now()
				j.Status.StartTime = &now
				return j
			}(),
		},
		"skips creation when Job has succeeded pods": {
			job: func() *batch.Job {
				j := baseJob.DeepCopy()
				j.Status.Succeeded = 1
				return j
			}(),
		},
		"skips creation when Job has failed pods": {
			job: func() *batch.Job {
				j := baseJob.DeepCopy()
				j.Status.Failed = 1
				return j
			}(),
		},
		"skips creating Workload/PodGroup when Job was previously suspended": {
			job: func() *batch.Job {
				j := baseJob.DeepCopy()
				j.Status.Conditions = []batch.JobCondition{
					{
						Type:   batch.JobSuspended,
						Status: v1.ConditionTrue,
					},
				}
				return j
			}(),
		},
		"discovers existing Workload and PodGroup": {
			job:               baseJob,
			existingWorkloads: []*schedulingv1beta1.Workload{makeWorkload(workloadName, baseJob.Name)},
			existingPodGroups: []*schedulingv1beta1.PodGroup{makePodGroup(podGroupName, workloadName)},
			wantPodGroup:      true,
			wantPodGroupName:  podGroupName,
		},
		"discovers existing objects even when pods exist": {
			job:               baseJob,
			pods:              []*v1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: metav1.NamespaceDefault}}},
			existingWorkloads: []*schedulingv1beta1.Workload{makeWorkload(workloadName, baseJob.Name)},
			existingPodGroups: []*schedulingv1beta1.PodGroup{makePodGroup(podGroupName, workloadName)},
			wantPodGroup:      true,
			wantPodGroupName:  podGroupName,
		},
		"workload found but no PodGroup creates one": {
			job:               baseJob,
			existingWorkloads: []*schedulingv1beta1.Workload{makeWorkload("user-workload", baseJob.Name)},
			wantPodGroup:      true,
		},

		"BYO Workload (no controller ownerRef) is ignored; controller creates its own": {
			job: baseJob,
			existingWorkloads: []*schedulingv1beta1.Workload{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "user-created-workload",
						Namespace: metav1.NamespaceDefault,
						UID:       types.UID("user-wl-uid"),
					},
					Spec: schedulingv1beta1.WorkloadSpec{
						ControllerRef: newWorkloadJobControllerRef(baseJob.Name),
						PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{
							{
								Name: templateName,
								SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
									Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 4},
								},
							},
						},
					},
				},
			},
			wantPodGroup:     true,
			wantPodGroupName: podGroupName,
		},
		"ambiguous Workloads fall back": {
			job: baseJob,
			existingWorkloads: []*schedulingv1beta1.Workload{
				makeWorkload("workload-1", baseJob.Name),
				makeWorkload("workload-2", baseJob.Name),
			},
		},
		"BYO PodGroup (no controller ownerRef) is ignored; controller creates its own": {
			job:               baseJob,
			existingWorkloads: []*schedulingv1beta1.Workload{makeWorkload(workloadName, baseJob.Name)},
			existingPodGroups: []*schedulingv1beta1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "external-pg",
						Namespace: metav1.NamespaceDefault,
						UID:       types.UID("ext-pg-uid"),
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						WorkloadRef: &schedulingv1beta1.WorkloadReference{
							WorkloadName: workloadName,
						},
					},
				},
			},
			wantPodGroup:     true,
			wantPodGroupName: podGroupName,
		},
		"ambiguous PodGroups fall back": {
			job:               baseJob,
			existingWorkloads: []*schedulingv1beta1.Workload{makeWorkload(workloadName, baseJob.Name)},
			existingPodGroups: []*schedulingv1beta1.PodGroup{
				makePodGroup("pg-1", workloadName),
				makePodGroup("pg-2", workloadName),
			},
		},
		"unsupported PodGroupTemplate count returns an error": {
			job: baseJob,
			existingWorkloads: []*schedulingv1beta1.Workload{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:            workloadName,
						Namespace:       metav1.NamespaceDefault,
						UID:             types.UID(workloadName + "-uid"),
						OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(baseJob, controllerKind)},
					},
					Spec: schedulingv1beta1.WorkloadSpec{
						ControllerRef: newWorkloadJobControllerRef(baseJob.Name),
						PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{
							{Name: "group-a"},
							{Name: "group-b"},
						},
					},
				},
			},
			wantErr: true,
		},

		"Workload create error propagates": {
			job:                 baseJob,
			simulateCreateError: true,
			wantErr:             true,
		},

		"PodGroup create error propagates": {
			job:                   baseJob,
			existingWorkloads:     []*schedulingv1beta1.Workload{makeWorkload(workloadName, baseJob.Name)},
			simulatePGCreateError: true,
			wantErr:               true,
		},
		"delegated Job creates the PodGroup from the parent's template": {
			job:                      delegatedJob,
			existingWorkloads:        []*schedulingv1beta1.Workload{makeParentWorkload(4)},
			wantPodGroup:             true,
			wantPodGroupName:         delegatedPodGroupName,
			wantPodGroupGangMinCount: new(int32(4)),
		},
		"delegated PodGroup follows a parent Workload minCount change": {
			job:                      delegatedJob,
			existingWorkloads:        []*schedulingv1beta1.Workload{makeParentWorkload(7)},
			existingPodGroups:        []*schedulingv1beta1.PodGroup{makeDelegatedPodGroup(4)},
			wantPodGroup:             true,
			wantPodGroupName:         delegatedPodGroupName,
			wantPodGroupGangMinCount: new(int32(7)),
		},
		"delegated PodGroup already matching the parent template is left unchanged": {
			job:                      delegatedJob,
			existingWorkloads:        []*schedulingv1beta1.Workload{makeParentWorkload(4)},
			existingPodGroups:        []*schedulingv1beta1.PodGroup{makeDelegatedPodGroup(4)},
			wantPodGroup:             true,
			wantPodGroupName:         delegatedPodGroupName,
			wantPodGroupGangMinCount: new(int32(4)),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var objs []runtime.Object
			for _, wl := range tc.existingWorkloads {
				objs = append(objs, wl)
			}
			for _, pg := range tc.existingPodGroups {
				objs = append(objs, pg)
			}
			clientSet := fake.NewClientset(objs...)
			jm, sharedInformers := newControllerWithSchedulingInformers(ctx, t, clientSet)

			// 用前置 reactor 拦截创建请求，模拟 API Server 返回错误。
			if tc.simulateCreateError {
				clientSet.PrependReactor("create", "workloads", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("synthetic API error")
				})
			}

			if tc.simulatePGCreateError {
				clientSet.PrependReactor("create", "podgroups", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("synthetic PG API error")
				})
			}

			// 发现逻辑走 informer 缓存，先启动并等待同步，避免读到空缓存误判为不存在。
			sharedInformers.Start(ctx.Done())
			sharedInformers.WaitForCacheSync(ctx.Done())

			_, pg, err := jm.ensureWorkloadAndPodGroup(ctx, tc.job, tc.pods)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantPodGroup {
				if pg == nil {
					t.Fatal("expected non-nil PodGroup")
				}
				if tc.wantPodGroupName != "" && pg.Name != tc.wantPodGroupName {
					t.Errorf("PodGroup name = %q, want %q", pg.Name, tc.wantPodGroupName)
				}
			} else if pg != nil {
				t.Errorf("expected nil PodGroup, got %v", pg)
			}
			// minCount 的期望值需要从 API 读取，确认实际写入的是更新后的策略而非返回值。
			if tc.wantPodGroupGangMinCount != nil {
				gotPG, err := clientSet.SchedulingV1beta1().PodGroups(metav1.NamespaceDefault).Get(ctx, tc.wantPodGroupName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("get podGroup %q: %v", tc.wantPodGroupName, err)
				}
				if gotPG.Spec.SchedulingPolicy.Gang == nil {
					t.Fatalf("podGroup %q has no gang policy", tc.wantPodGroupName)
				}
				if got := gotPG.Spec.SchedulingPolicy.Gang.MinCount; got != *tc.wantPodGroupGangMinCount {
					t.Errorf("podGroup gang minCount = %d, want %d", got, *tc.wantPodGroupGangMinCount)
				}
			}
		})
	}
}

// TestCreateWorkloadForJob 验证创建出的 Workload 名称、命名空间、控制器引用、
// 模板策略及 ownerRef 均正确，保证调度器能识别其归属与 gang 规模。
func TestCreateWorkloadForJob(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientSet := fake.NewClientset()
	jm, _ := newControllerWithSchedulingInformers(ctx, t, clientSet)
	job := newGangSchedulingJob("my-job", 8)

	wl, err := jm.createWorkloadForJob(ctx, job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedName := computeWorkloadName(job)
	if wl.Name != expectedName {
		t.Errorf("workload name = %q, want %q", wl.Name, expectedName)
	}
	if wl.Namespace != job.Namespace {
		t.Errorf("workload namespace = %q, want %q", wl.Namespace, job.Namespace)
	}

	if wl.Spec.ControllerRef == nil {
		t.Fatal("expected controllerRef")
	}
	// 用 cmp.Diff 对比整个引用结构，避免遗漏 APIGroup 或 Kind 字段。
	if diff := cmp.Diff(wl.Spec.ControllerRef, newWorkloadJobControllerRef(job.Name)); diff != "" {
		t.Errorf("unexpected controllerRef (-got +want):\n%s", diff)
	}

	if len(wl.Spec.PodGroupTemplates) != 1 {
		t.Fatalf("expected 1 template, got %d", len(wl.Spec.PodGroupTemplates))
	}
	tpl := wl.Spec.PodGroupTemplates[0]
	if tpl.SchedulingPolicy.Gang == nil || tpl.SchedulingPolicy.Gang.MinCount != 8 {
		t.Errorf("expected gang minCount=8, got %+v", tpl.SchedulingPolicy)
	}

	if len(wl.OwnerReferences) != 1 || wl.OwnerReferences[0].Name != job.Name {
		t.Errorf("unexpected ownerReferences: %v", wl.OwnerReferences)
	}
}

// TestCreatePodGroup 验证 PodGroup 的名称、ownerRef 数量及从 Workload 模板复制的
// 调度策略与优先级；根 Job 会额外把 Workload 作为非控制器 owner，委托场景则不会。
func TestCreatePodGroup(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientSet := fake.NewClientset()
	jm, _ := newControllerWithSchedulingInformers(ctx, t, clientSet)
	job := newGangSchedulingJob("my-job", 4)
	templateName := fmt.Sprintf("%s-pgt-%d", job.Name, 0)

	testCases := map[string]struct {
		workloadName      string
		linkWorkloadOwner bool
		wantOwnerRefs     int
	}{
		"root Job links the Workload as a non-controller owner": {
			workloadName:      "root-workload",
			linkWorkloadOwner: true,
			wantOwnerRefs:     2,
		},
		"delegated Job does not link the parent-owned Workload": {
			workloadName:      "delegated-workload",
			linkWorkloadOwner: false,
			wantOwnerRefs:     1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			workload := &schedulingv1beta1.Workload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.workloadName,
					Namespace: job.Namespace,
					UID:       "wl-uid",
				},
				Spec: schedulingv1beta1.WorkloadSpec{
					PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{
						{
							Name: templateName,
							SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
								Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 4},
							},
							PriorityClassName: "high-priority",
							Priority:          new(int32(1000)),
						},
					},
				},
			}

			// 从已有 Workload 构造 builder，确保 PodGroup 继承模板中的全部策略字段。
			builder := newBuilderFromExistingWorkload(job, workload)
			pg, err := jm.createPodGroup(ctx, job, workload, "", tc.linkWorkloadOwner, builder)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expectedName := computePodGroupName(workload.Name, templateName)
			if pg.Name != expectedName {
				t.Errorf("PodGroup name = %q, want %q", pg.Name, expectedName)
			}
			if len(pg.OwnerReferences) != tc.wantOwnerRefs {
				t.Fatalf("expected %d ownerReferences, got %d", tc.wantOwnerRefs, len(pg.OwnerReferences))
			}

			wantSpec := schedulingv1beta1.PodGroupSpec{
				WorkloadRef: &schedulingv1beta1.WorkloadReference{
					WorkloadName: tc.workloadName,
					TemplateName: templateName,
				},
				SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{
					Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 4},
				},
				PriorityClassName: "high-priority",
				Priority:          new(int32(1000)),
			}
			// 整段比较 Spec，防止模板字段在复制过程中被漏掉或被改写。
			if diff := cmp.Diff(wantSpec, pg.Spec); diff != "" {
				t.Errorf("unexpected PodGroup spec (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSyncGangMinCount 验证控制器拥有的 Workload/PodGroup 会被调整到 Job 的
// gang minCount，而已经一致时不再变更。
func TestSyncGangMinCount(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	// Job 声明 minCount=5，因此已存在的对象都应被同步为 5。
	job := newGangSchedulingJob("scale-job", 5)
	wlName := computeWorkloadName(job)
	tmplName := podGroupTemplateName(job)
	pgName := computePodGroupName(wlName, tmplName)

	makeWL := func(minCount int32) *schedulingv1beta1.Workload {
		return &schedulingv1beta1.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name:            wlName,
				Namespace:       job.Namespace,
				UID:             "wl-uid",
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, controllerKind)},
			},
			Spec: schedulingv1beta1.WorkloadSpec{
				ControllerRef: newWorkloadJobControllerRef(job.Name),
				PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{{
					Name:             tmplName,
					SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: minCount}},
				}},
			},
		}
	}
	makePG := func(minCount int32) *schedulingv1beta1.PodGroup {
		return &schedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            pgName,
				Namespace:       job.Namespace,
				UID:             "pg-uid",
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, controllerKind)},
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				WorkloadRef:      &schedulingv1beta1.WorkloadReference{WorkloadName: wlName, TemplateName: tmplName},
				SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: minCount}},
			},
		}
	}

	testCases := map[string]struct {
		workload     *schedulingv1beta1.Workload
		podGroup     *schedulingv1beta1.PodGroup
		wantMinCount int32
	}{
		"controller-owned objects are resized to the Job's minCount": {
			workload: makeWL(2), podGroup: makePG(2), wantMinCount: 5,
		},
		"already at desired minCount is left unchanged": {
			workload: makeWL(5), podGroup: makePG(5), wantMinCount: 5,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			clientSet := fake.NewClientset(tc.workload, tc.podGroup)
			jm, _ := newControllerWithSchedulingInformers(ctx, t, clientSet)

			if err := jm.syncGangMinCount(ctx, job, tc.workload, tc.podGroup); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			gotWL, err := clientSet.SchedulingV1beta1().Workloads(job.Namespace).Get(ctx, wlName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get workload: %v", err)
			}
			if got := gotWL.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount; got != tc.wantMinCount {
				t.Errorf("workload minCount = %d, want %d", got, tc.wantMinCount)
			}
			gotPG, err := clientSet.SchedulingV1beta1().PodGroups(job.Namespace).Get(ctx, pgName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get podGroup: %v", err)
			}
			if got := gotPG.Spec.SchedulingPolicy.Gang.MinCount; got != tc.wantMinCount {
				t.Errorf("podGroup minCount = %d, want %d", got, tc.wantMinCount)
			}
		})
	}
}
