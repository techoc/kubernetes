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
	"testing"

	"github.com/google/go-cmp/cmp"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-helpers/scheduling/schedulingv1/workloadbuilder"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// TestDropDisabledFieldsScheduling 验证特性门控关闭时，DropDisabledFields 会清除
// Job 的 scheduling 字段；但如果旧对象已经在用该字段（更新场景），则必须保留，
// 否则升级/回滚过程中会丢失用户已有的配置。
func TestDropDisabledFieldsScheduling(t *testing.T) {
	schedulingConfig := func() *batch.JobSchedulingConfiguration {
		return &batch.JobSchedulingConfiguration{SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Basic: &schedulingv1alpha3.WorkloadPodGroupBasicSchedulingPolicy{}}}
	}

	cases := map[string]struct {
		enableWorkloadWithJob bool
		jobSpec               *batch.JobSchedulingConfiguration
		oldJobSpec            *batch.JobSchedulingConfiguration
		wantDropped           bool
		// hasOldSpec 区分创建（old spec 为 nil）与更新两种场景：
		// 只有更新时才需要考虑旧对象是否已经在用该字段。
		hasOldSpec bool
	}{
		// 特性开启：创建时正常保留 scheduling。
		"feature enabled, create keeps scheduling": {
			enableWorkloadWithJob: true,
			jobSpec:               schedulingConfig(),
		},
		// 特性关闭：新对象不应带上该字段，应被清除。
		"feature disabled, create drops scheduling": {
			enableWorkloadWithJob: false,
			jobSpec:               schedulingConfig(),
			wantDropped:           true,
		},
		// 原本就没有 scheduling，清除后仍为 nil，不应引入额外变化。
		"feature disabled, create without scheduling stays nil": {
			enableWorkloadWithJob: false,
			jobSpec:               nil,
		},
		// 特性开启：更新时新旧都带该字段，正常保留。
		"feature enabled, update keeps scheduling": {
			enableWorkloadWithJob: true,
			jobSpec:               schedulingConfig(),
			oldJobSpec:            schedulingConfig(),
			hasOldSpec:            true,
		},
		// 特性关闭但旧对象已在用该字段：必须保留，避免破坏已有配置。
		"feature disabled, update preserves scheduling already in use": {
			enableWorkloadWithJob: false,
			jobSpec:               schedulingConfig(),
			oldJobSpec:            schedulingConfig(),
			hasOldSpec:            true,
		},
		// 特性关闭且旧对象没有该字段（本次新增）：应清除，防止绕过门控。
		"feature disabled, update drops newly added scheduling": {
			enableWorkloadWithJob: false,
			jobSpec:               schedulingConfig(),
			oldJobSpec:            nil,
			hasOldSpec:            true,
			wantDropped:           true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// WorkloadWithJob 依赖 GenericWorkload，测试中要同步开关这两个门控。
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: tc.enableWorkloadWithJob,
				features.WorkloadWithJob: tc.enableWorkloadWithJob,
			})

			newSpec := &batch.JobSpec{Scheduling: tc.jobSpec}
			var oldSpec *batch.JobSpec
			// hasOldSpec 为 false 时保持 oldSpec 为 nil，模拟创建请求。
			if tc.hasOldSpec {
				oldSpec = &batch.JobSpec{Scheduling: tc.oldJobSpec}
			}

			DropDisabledFields(newSpec, oldSpec)

			// wantDropped 为真时期望字段被清成 nil，否则应保持原值不变。
			var want *batch.JobSchedulingConfiguration
			if !tc.wantDropped {
				want = tc.jobSpec
			}
			if diff := cmp.Diff(want, newSpec.Scheduling); diff != "" {
				t.Errorf("unexpected scheduling after DropDisabledFields (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestWorkloadInput 验证 WorkloadInput 能把四块 scheduling 配置映射到
// workloadbuilder 的输入结构，并且即使某块配置为 nil，也始终携带对应的字段路径，
// 这样校验错误才能准确定位到原始字段。
func TestWorkloadInput(t *testing.T) {
	policy := &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
		Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](3)},
	}
	constraints := &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
		Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/rack"}},
	}
	disruption := &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
		All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{},
	}
	claims := []schedulingv1alpha3.WorkloadPodGroupResourceClaim{
		{Name: "c1", ResourceClaimName: new("rc1")},
	}

	cases := map[string]struct {
		policy         *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy
		constraints    *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints
		disruptionMode *schedulingv1alpha3.WorkloadPodGroupDisruptionMode
		resourceClaims []schedulingv1alpha3.WorkloadPodGroupResourceClaim
		want           workloadbuilder.WorkloadInput
	}{
		// 全为 nil 时数据为空，但路径元素仍应保留，便于错误定位。
		"nil blocks still carry path elements": {
			want: workloadbuilder.WorkloadInput{
				Policy:         workloadbuilder.PolicyInput{PathElements: []string{"schedulingPolicy"}},
				Constraints:    workloadbuilder.ConstraintsInput{PathElements: []string{"schedulingConstraints"}},
				DisruptionMode: workloadbuilder.DisruptionModeInput{PathElements: []string{"disruptionMode"}},
				ResourceClaims: workloadbuilder.ResourceClaimsInput{PathElements: []string{"resourceClaims"}},
			},
		},
		// 有数据时原样透传，同时附带各自的路径元素。
		"populated blocks pass through with path elements": {
			policy:         policy,
			constraints:    constraints,
			disruptionMode: disruption,
			resourceClaims: claims,
			want: workloadbuilder.WorkloadInput{
				Policy:         workloadbuilder.PolicyInput{PodGroupData: policy, PathElements: []string{"schedulingPolicy"}},
				Constraints:    workloadbuilder.ConstraintsInput{PodGroupData: constraints, PathElements: []string{"schedulingConstraints"}},
				DisruptionMode: workloadbuilder.DisruptionModeInput{PodGroupData: disruption, PathElements: []string{"disruptionMode"}},
				ResourceClaims: workloadbuilder.ResourceClaimsInput{PodGroupData: claims, PathElements: []string{"resourceClaims"}},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := WorkloadInput(tc.policy, tc.constraints, tc.disruptionMode, tc.resourceClaims)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("unexpected WorkloadInput (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestWorkloadItemForJob 验证 WorkloadItemForJob 构造出的 WorkloadItem：
// 名称、字段路径、输入数据要原样带入，默认配置使用 basic 策略，
// 并且恰好注册一个回调，供后续对 gang 配置做默认值填充。
func TestWorkloadItemForJob(t *testing.T) {
	input := WorkloadInput(nil, nil, nil, nil)

	cases := map[string]struct {
		itemName          string
		priorityClassName string
		parallelism       *int32
		input             workloadbuilder.WorkloadInput
		path              *field.Path
	}{
		// 名称、优先级类和路径都要原样带到 item 上。
		"name and priority class carried onto the item": {
			itemName:          "job-pgt-0",
			priorityClassName: "high",
			parallelism:       ptr.To[int32](4),
			input:             input,
			path:              field.NewPath("spec", "scheduling"),
		},
		// 优先级类为空、并发为 nil 是合法输入，不应触发 panic 或多余配置。
		"empty priority class and nil parallelism": {
			itemName: "job",
			input:    input,
		},
		// path 为 nil 也要原样透传，由调用方决定是否需要定位路径。
		"nil path is carried through": {
			itemName: "job",
			input:    input,
			path:     nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := WorkloadItemForJob(tc.itemName, tc.priorityClassName, tc.parallelism, tc.input, tc.path)

			if got.Name != tc.itemName {
				t.Errorf("Name = %q, want %q", got.Name, tc.itemName)
			}
			if got.Path.String() != tc.path.String() {
				t.Errorf("Path = %q, want %q", got.Path.String(), tc.path.String())
			}
			// 默认配置固定为 basic 策略，并带上传入的优先级类。
			wantDefault := &workloadbuilder.SchedulingConfig{
				Policy:            &workloadbuilder.SchedulingPolicy{Basic: &workloadbuilder.BasicSchedulingPolicy{}},
				PriorityClassName: tc.priorityClassName,
			}
			if diff := cmp.Diff(wantDefault, got.DefaultConfig); diff != "" {
				t.Errorf("unexpected DefaultConfig (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.input, got.Input); diff != "" {
				t.Errorf("unexpected Input (-want,+got):\n%s", diff)
			}
			// 恰好一个回调：后续测试依赖 Callbacks[0] 做默认值填充。
			if len(got.Callbacks) != 1 {
				t.Fatalf("Callbacks length = %d, want 1", len(got.Callbacks))
			}
		})
	}
}

// TestWorkloadItemForJobMinCountDefaulting 验证注册到 WorkloadItem 上的回调如何
// 给 gang 策略填充 minCount：未设置时取 parallelism，且至少为 1；已设置的不覆盖；
// basic 策略不受影响，传入 nil 配置也不会 panic。
func TestWorkloadItemForJobMinCountDefaulting(t *testing.T) {
	gangConfig := func(minCount *int32) *workloadbuilder.SchedulingConfig {
		return &workloadbuilder.SchedulingConfig{
			Policy: &workloadbuilder.SchedulingPolicy{Gang: &workloadbuilder.GangSchedulingPolicy{MinCount: minCount}},
		}
	}

	cases := map[string]struct {
		parallelism *int32
		cfg         *workloadbuilder.SchedulingConfig
		wantMin     *int32
	}{
		// minCount 未设置：沿用 Job 的 parallelism。
		"gang unset minCount defaults to parallelism": {
			parallelism: ptr.To[int32](5),
			cfg:         gangConfig(nil),
			wantMin:     ptr.To[int32](5),
		},
		// parallelism 为 nil 时没有可用参考值，下限保护为 1。
		"gang unset minCount with nil parallelism clamps to 1": {
			parallelism: nil,
			cfg:         gangConfig(nil),
			wantMin:     ptr.To[int32](1),
		},
		// parallelism 为 0 同样非法，钳制为 1。
		"gang unset minCount with zero parallelism clamps to 1": {
			parallelism: ptr.To[int32](0),
			cfg:         gangConfig(nil),
			wantMin:     ptr.To[int32](1),
		},
		// 用户显式设置的 minCount 优先，不能被默认值覆盖。
		"gang minCount already set is preserved": {
			parallelism: ptr.To[int32](5),
			cfg:         gangConfig(ptr.To[int32](2)),
			wantMin:     ptr.To[int32](2),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			item := WorkloadItemForJob("job", "", tc.parallelism, WorkloadInput(nil, nil, nil, nil), nil)
			// 回调会原地修改 cfg，因此直接检查同一个对象。
			item.Callbacks[0](tc.cfg)
			if diff := cmp.Diff(tc.wantMin, tc.cfg.Policy.Gang.MinCount); diff != "" {
				t.Errorf("unexpected resolved MinCount (-want,+got):\n%s", diff)
			}
		})
	}

	t.Run("basic policy is left untouched", func(t *testing.T) {
		item := WorkloadItemForJob("job", "", ptr.To[int32](5), WorkloadInput(nil, nil, nil, nil), nil)
		cfg := &workloadbuilder.SchedulingConfig{
			Policy: &workloadbuilder.SchedulingPolicy{Basic: &workloadbuilder.BasicSchedulingPolicy{}},
		}
		item.Callbacks[0](cfg)
		// 回调只应处理 gang 策略，basic 策略上不应凭空生成 Gang 配置。
		if cfg.Policy.Gang != nil {
			t.Errorf("Gang = %+v, want nil for a basic policy", cfg.Policy.Gang)
		}
	})

	t.Run("nil config does not panic", func(t *testing.T) {
		item := WorkloadItemForJob("job", "", ptr.To[int32](5), WorkloadInput(nil, nil, nil, nil), nil)
		// 回调内部需要对 nil 配置做防御，调用不 panic 即通过。
		item.Callbacks[0](nil)
	})
}
