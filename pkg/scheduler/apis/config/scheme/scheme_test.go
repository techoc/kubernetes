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

package scheme

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	v1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/testing/defaults"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// TestCodecsDecodePluginConfig 验证嵌套的插件参数会被解码为正确的内部类型，并应用默认值。
func TestCodecsDecodePluginConfig(t *testing.T) {
	// 每条用例要么给出期望的配置文件，要么给出解码失败的精确错误文本。
	testCases := []struct {
		name         string
		data         []byte
		wantErr      string
		wantProfiles []config.KubeSchedulerProfile
	}{
		// v1 用例
		{
			// 各类内嵌插件参数都应解码为内部类型，并补齐缺失/为零的默认值。
			// YAML 中注释的含义：cpu/scalar1 未指定 weight 时取默认值 1；
			// memory/scalar2 的 weight 为 0 时会被替换为 1。
			name: "v1 all plugin args in default profile",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: DefaultPreemption
    args:
      minCandidateNodesPercentage: 50
      minCandidateNodesAbsolute: 500
  - name: DynamicResources
    args:
      filterTimeout: 10s
      bindingTimeout: 30s
  - name: InterPodAffinity
    args:
      hardPodAffinityWeight: 5
  - name: NodeResourcesFit
    args:
      ignoredResources: ["foo"]
  - name: PodTopologySpread
    args:
      defaultConstraints:
      - maxSkew: 1
        topologyKey: zone
        whenUnsatisfiable: ScheduleAnyway
  - name: VolumeBinding
    args:
      bindTimeoutSeconds: 300
  - name: NodeAffinity
    args:
      addedAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: foo
              operator: In
              values: ["bar"]
  - name: NodeResourcesBalancedAllocation
    args:
      resources:
        - name: cpu       # default weight(1) will be set.
        - name: memory    # weight 0 will be replaced by 1.
          weight: 0
        - name: scalar0
          weight: 1
        - name: scalar1   # default weight(1) will be set for scalar1
        - name: scalar2   # weight 0 will be replaced by 1.
          weight: 0
        - name: scalar3
          weight: 2
`),
			wantProfiles: []config.KubeSchedulerProfile{
				{
					SchedulerName:            "default-scheduler",
					PercentageOfNodesToScore: nil,
					Plugins:                  defaults.PluginsV1,
					PluginConfig: []config.PluginConfig{
						{
							Name: "DefaultPreemption",
							Args: &config.DefaultPreemptionArgs{MinCandidateNodesPercentage: 50, MinCandidateNodesAbsolute: 500},
						},
						{
							Name: "DynamicResources",
							Args: &config.DynamicResourcesArgs{
								FilterTimeout:  &metav1.Duration{Duration: 10 * time.Second},
								BindingTimeout: &metav1.Duration{Duration: 30 * time.Second},
							},
						},
						{
							Name: "InterPodAffinity",
							Args: &config.InterPodAffinityArgs{HardPodAffinityWeight: 5},
						},
						{
							Name: "NodeResourcesFit",
							Args: &config.NodeResourcesFitArgs{
								IgnoredResources: []string{"foo"},
								ScoringStrategy: &config.ScoringStrategy{
									Type: config.LeastAllocated,
									Resources: []config.ResourceSpec{
										{Name: "cpu", Weight: 1},
										{Name: "memory", Weight: 1},
									},
								},
							},
						},
						{
							Name: "PodTopologySpread",
							Args: &config.PodTopologySpreadArgs{
								DefaultConstraints: []corev1.TopologySpreadConstraint{
									{MaxSkew: 1, TopologyKey: "zone", WhenUnsatisfiable: corev1.ScheduleAnyway},
								},
								DefaultingType: config.SystemDefaulting,
							},
						},
						{
							Name: "VolumeBinding",
							Args: &config.VolumeBindingArgs{
								BindTimeoutSeconds: 300,
								Shape: []config.UtilizationShapePoint{
									{Utilization: 0, Score: 10},
									{Utilization: 100, Score: 0},
								},
							},
						},
						{
							Name: "NodeAffinity",
							Args: &config.NodeAffinityArgs{
								AddedAffinity: &corev1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
										NodeSelectorTerms: []corev1.NodeSelectorTerm{
											{
												MatchExpressions: []corev1.NodeSelectorRequirement{
													{
														Key:      "foo",
														Operator: corev1.NodeSelectorOpIn,
														Values:   []string{"bar"},
													},
												},
											},
										},
									},
								},
							},
						},
						{
							Name: "NodeResourcesBalancedAllocation",
							Args: &config.NodeResourcesBalancedAllocationArgs{
								Resources: []config.ResourceSpec{
									{Name: "cpu", Weight: 1},
									{Name: "memory", Weight: 1},
									{Name: "scalar0", Weight: 1},
									{Name: "scalar1", Weight: 1},
									{Name: "scalar2", Weight: 1},
									{Name: "scalar3", Weight: 2}},
							},
						},
					},
				},
			},
		},
		{
			// 全局值只留在顶层，不注入 profile；这里期望 profile 内该字段仍为 nil。
			name: "v1 with non-default global percentageOfNodesToScore",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
percentageOfNodesToScore: 10
`),
			wantProfiles: []config.KubeSchedulerProfile{
				{
					SchedulerName:            "default-scheduler",
					PercentageOfNodesToScore: nil,
					Plugins:                  defaults.PluginsV1,
					PluginConfig:             defaults.PluginConfigsV1,
				},
			},
		},
		{
			// profile 级设置应优先于全局值，解码后只保留 profile 级的值。
			name: "v1 with non-default global and profile percentageOfNodesToScore",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
percentageOfNodesToScore: 10
profiles:
- percentageOfNodesToScore: 20
`),
			wantProfiles: []config.KubeSchedulerProfile{
				{
					SchedulerName:            "default-scheduler",
					PercentageOfNodesToScore: ptr.To[int32](20),
					Plugins:                  defaults.PluginsV1,
					PluginConfig:             defaults.PluginConfigsV1,
				},
			},
		},
		{
			// args 中显式携带的 apiVersion/kind 应被识别，不影响解码与默认值补全。
			name: "v1 plugins can include version and kind",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: DefaultPreemption
    args:
      apiVersion: kubescheduler.config.k8s.io/v1
      kind: DefaultPreemptionArgs
      minCandidateNodesPercentage: 50
`),
			wantProfiles: []config.KubeSchedulerProfile{
				{
					SchedulerName: "default-scheduler",
					Plugins:       defaults.PluginsV1,
					PluginConfig: []config.PluginConfig{
						{
							Name: "DefaultPreemption",
							Args: &config.DefaultPreemptionArgs{MinCandidateNodesPercentage: 50, MinCandidateNodesAbsolute: 100},
						},
						{
							Name: "DynamicResources",
							Args: &config.DynamicResourcesArgs{
								FilterTimeout:  &metav1.Duration{Duration: 10 * time.Second},
								BindingTimeout: &metav1.Duration{Duration: 10 * time.Minute},
							},
						},
						{
							Name: "InterPodAffinity",
							Args: &config.InterPodAffinityArgs{
								HardPodAffinityWeight: 1,
							},
						},
						{
							Name: "NodeAffinity",
							Args: &config.NodeAffinityArgs{},
						},
						{
							Name: "NodeResourcesBalancedAllocation",
							Args: &config.NodeResourcesBalancedAllocationArgs{
								Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
							},
						},
						{
							Name: "NodeResourcesFit",
							Args: &config.NodeResourcesFitArgs{
								ScoringStrategy: &config.ScoringStrategy{
									Type: config.LeastAllocated,
									Resources: []config.ResourceSpec{
										{Name: "cpu", Weight: 1},
										{Name: "memory", Weight: 1},
									},
								},
							},
						},
						{
							Name: "PodTopologySpread",
							Args: &config.PodTopologySpreadArgs{
								DefaultingType: config.SystemDefaulting,
							},
						},
						{
							Name: "VolumeBinding",
							Args: &config.VolumeBindingArgs{
								BindTimeoutSeconds: 600,
								Shape: []config.UtilizationShapePoint{
									{Utilization: 0, Score: 10},
									{Utilization: 100, Score: 0},
								},
							},
						},
					},
				},
			},
		},
		{
			// args 的 kind 与插件期望的类型不一致时必须报错。
			name: "plugin group and kind should match the type",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: DefaultPreemption
    args:
      apiVersion: kubescheduler.config.k8s.io/v1
      kind: InterPodAffinityArgs
`),
			wantErr: `decoding .profiles[0].pluginConfig[0]: args for plugin DefaultPreemption were not of type DefaultPreemptionArgs.kubescheduler.config.k8s.io, got InterPodAffinityArgs.kubescheduler.config.k8s.io`,
		},
		{
			// 严格解码下，shape 中大写开头的字段名不被接受。
			name: "v1 NodResourcesFitArgs shape encoding is strict",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        requestedToCapacityRatio:
          shape:
          - Score: 2
            Utilization: 1
`),
			wantErr: `strict decoding error: decoding .profiles[0].pluginConfig[0]: strict decoding error: decoding args for plugin NodeResourcesFit: strict decoding error: unknown field "scoringStrategy.requestedToCapacityRatio.shape[0].Score", unknown field "scoringStrategy.requestedToCapacityRatio.shape[0].Utilization"`,
		},
		{
			// 严格解码下，resources 中大写开头的字段名不被接受。
			name: "v1 NodeResourcesFitArgs resources encoding is strict",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        resources:
        - Name: cpu
          Weight: 1
`),
			wantErr: `strict decoding error: decoding .profiles[0].pluginConfig[0]: strict decoding error: decoding args for plugin NodeResourcesFit: strict decoding error: unknown field "scoringStrategy.resources[0].Name", unknown field "scoringStrategy.resources[0].Weight"`,
		},
		{
			// 树外插件参数无法解析为已知类型，应原样保留为 runtime.Unknown。
			name: "out-of-tree plugin args",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: OutOfTreePlugin
    args:
      foo: bar
`),
			wantProfiles: []config.KubeSchedulerProfile{
				{
					SchedulerName: "default-scheduler",
					Plugins:       defaults.PluginsV1,
					PluginConfig: append([]config.PluginConfig{
						{
							Name: "OutOfTreePlugin",
							Args: &runtime.Unknown{
								ContentType: "application/json",
								Raw:         []byte(`{"foo":"bar"}`),
							},
						},
					}, defaults.PluginConfigsV1...),
				},
			},
		},
		{
			// args 为空或缺失时应为插件补齐默认参数；树外插件没有默认参数则保持 nil。
			name: "empty and no plugin args",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: DefaultPreemption
    args:
  - name: DynamicResources
    args:
  - name: InterPodAffinity
    args:
  - name: NodeResourcesFit
  - name: OutOfTreePlugin
    args:
  - name: VolumeBinding
    args:
  - name: PodTopologySpread
  - name: NodeAffinity
  - name: NodeResourcesBalancedAllocation
`),
			wantProfiles: []config.KubeSchedulerProfile{
				{
					SchedulerName: "default-scheduler",
					Plugins:       defaults.PluginsV1,
					PluginConfig: []config.PluginConfig{
						{
							Name: "DefaultPreemption",
							Args: &config.DefaultPreemptionArgs{MinCandidateNodesPercentage: 10, MinCandidateNodesAbsolute: 100},
						},
						{
							Name: "DynamicResources",
							Args: &config.DynamicResourcesArgs{
								FilterTimeout:  &metav1.Duration{Duration: 10 * time.Second},
								BindingTimeout: &metav1.Duration{Duration: 10 * time.Minute},
							},
						},
						{
							Name: "InterPodAffinity",
							Args: &config.InterPodAffinityArgs{
								HardPodAffinityWeight: 1,
							},
						},
						{
							Name: "NodeResourcesFit",
							Args: &config.NodeResourcesFitArgs{
								ScoringStrategy: &config.ScoringStrategy{
									Type: config.LeastAllocated,
									Resources: []config.ResourceSpec{
										{Name: "cpu", Weight: 1},
										{Name: "memory", Weight: 1},
									},
								},
							},
						},
						{Name: "OutOfTreePlugin"},
						{
							Name: "VolumeBinding",
							Args: &config.VolumeBindingArgs{
								BindTimeoutSeconds: 600,
								Shape: []config.UtilizationShapePoint{
									{Utilization: 0, Score: 10},
									{Utilization: 100, Score: 0},
								},
							},
						},
						{
							Name: "PodTopologySpread",
							Args: &config.PodTopologySpreadArgs{
								DefaultingType: config.SystemDefaulting,
							},
						},
						{
							Name: "NodeAffinity",
							Args: &config.NodeAffinityArgs{},
						},
						{
							Name: "NodeResourcesBalancedAllocation",
							Args: &config.NodeResourcesBalancedAllocationArgs{
								Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
							},
						},
					},
				},
			},
		},
		{
			// 布尔型参数显式设为 true 时不应被默认值覆盖。
			name: "ignorePreferredTermsOfExistingPods is enabled",
			data: []byte(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- pluginConfig:
  - name: InterPodAffinity
    args:
      ignorePreferredTermsOfExistingPods: true
`),
			wantProfiles: []config.KubeSchedulerProfile{
				{
					SchedulerName: "default-scheduler",
					Plugins:       defaults.PluginsV1,
					PluginConfig: []config.PluginConfig{
						{
							Name: "InterPodAffinity",
							Args: &config.InterPodAffinityArgs{
								HardPodAffinityWeight:              1,
								IgnorePreferredTermsOfExistingPods: true,
							},
						},
						{
							Name: "DefaultPreemption",
							Args: &config.DefaultPreemptionArgs{MinCandidateNodesPercentage: 10, MinCandidateNodesAbsolute: 100},
						},
						{
							Name: "DynamicResources",
							Args: &config.DynamicResourcesArgs{
								FilterTimeout:  &metav1.Duration{Duration: 10 * time.Second},
								BindingTimeout: &metav1.Duration{Duration: 10 * time.Minute},
							},
						},
						{
							Name: "NodeAffinity",
							Args: &config.NodeAffinityArgs{},
						},
						{
							Name: "NodeResourcesBalancedAllocation",
							Args: &config.NodeResourcesBalancedAllocationArgs{
								Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
							},
						},
						{
							Name: "NodeResourcesFit",
							Args: &config.NodeResourcesFitArgs{
								ScoringStrategy: &config.ScoringStrategy{
									Type: config.LeastAllocated,
									Resources: []config.ResourceSpec{
										{Name: "cpu", Weight: 1},
										{Name: "memory", Weight: 1},
									},
								},
							},
						},
						{
							Name: "PodTopologySpread",
							Args: &config.PodTopologySpreadArgs{
								DefaultingType: config.SystemDefaulting,
							},
						},
						{
							Name: "VolumeBinding",
							Args: &config.VolumeBindingArgs{
								BindTimeoutSeconds: 600,
								Shape: []config.UtilizationShapePoint{
									{Utilization: 0, Score: 10},
									{Utilization: 100, Score: 0},
								},
							},
						},
					},
				},
			},
		},
	}
	// 使用通用解码器，由请求中的 apiVersion 决定具体解码路径。
	decoder := Codecs.UniversalDecoder()
	// 逐一验证：要么得到预期的精确错误，要么得到预期的内部配置。
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			// 期望失败的场景：错误文本必须逐字匹配，防止错误信息退化。
			obj, gvk, err := decoder.Decode(tt.data, nil, nil)
			if err != nil {
				if tt.wantErr != err.Error() {
					t.Fatalf("\ngot err:\n\t%v\nwant:\n\t%s", err, tt.wantErr)
				}
				return
			}
			// 期望成功的场景却报错，直接失败。
			if len(tt.wantErr) != 0 {
				t.Fatalf("no error produced, wanted %v", tt.wantErr)
			}
			// 校验解码结果确实是内部类型，而非某个外部版本类型。
			got, ok := obj.(*config.KubeSchedulerConfiguration)
			if !ok {
				t.Fatalf("decoded into %s, want %s", gvk, config.SchemeGroupVersion.WithKind("KubeSchedulerConfiguration"))
			}
			// 只比较 Profiles：其余字段不参与本测试关注的参数解码行为。
			if diff := cmp.Diff(tt.wantProfiles, got.Profiles); diff != "" {
				t.Errorf("unexpected configuration (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestCodecsEncodePluginConfig 验证内部及版本化的插件参数均可编码为 v1，且 YAML 与 JSON 两条路径输出一致。
func TestCodecsEncodePluginConfig(t *testing.T) {
	testCases := []struct {
		name    string
		obj     runtime.Object
		version schema.GroupVersion
		want    string
	}{
		// v1 用例
		{
			// 版本化对象：树内参数带类型信息，树外参数保留原始 JSON。
			name:    "v1 in-tree and out-of-tree plugins",
			version: v1.SchemeGroupVersion,
			obj: &v1.KubeSchedulerConfiguration{
				Profiles: []v1.KubeSchedulerProfile{
					{
						PluginConfig: []v1.PluginConfig{
							{
								Name: "InterPodAffinity",
								Args: runtime.RawExtension{
									Object: &v1.InterPodAffinityArgs{
										HardPodAffinityWeight: ptr.To[int32](5),
									},
								},
							},
							{
								Name: "VolumeBinding",
								Args: runtime.RawExtension{
									Object: &v1.VolumeBindingArgs{
										BindTimeoutSeconds: ptr.To[int64](300),
										Shape: []v1.UtilizationShapePoint{
											{
												Utilization: 0,
												Score:       0,
											},
											{
												Utilization: 100,
												Score:       10,
											},
										},
									},
								},
							},
							{
								Name: "NodeResourcesFit",
								Args: runtime.RawExtension{
									Object: &v1.NodeResourcesFitArgs{
										ScoringStrategy: &v1.ScoringStrategy{
											Type:      v1.RequestedToCapacityRatio,
											Resources: []v1.ResourceSpec{{Name: "cpu", Weight: 1}},
											RequestedToCapacityRatio: &v1.RequestedToCapacityRatioParam{
												Shape: []v1.UtilizationShapePoint{
													{Utilization: 1, Score: 2},
												},
											},
										},
									},
								},
							},
							{
								Name: "PodTopologySpread",
								Args: runtime.RawExtension{
									Object: &v1.PodTopologySpreadArgs{
										DefaultConstraints: []corev1.TopologySpreadConstraint{},
									},
								},
							},
							{
								Name: "OutOfTreePlugin",
								Args: runtime.RawExtension{
									Raw: []byte(`{"foo":"bar"}`),
								},
							},
						},
					},
				},
			},
			want: `apiVersion: kubescheduler.config.k8s.io/v1
clientConnection:
  acceptContentTypes: ""
  burst: 0
  contentType: ""
  kubeconfig: ""
  qps: 0
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: null
  leaseDuration: 0s
  renewDeadline: 0s
  resourceLock: ""
  resourceName: ""
  resourceNamespace: ""
  retryPeriod: 0s
profiles:
- pluginConfig:
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      hardPodAffinityWeight: 5
      ignorePreferredTermsOfExistingPods: false
      kind: InterPodAffinityArgs
    name: InterPodAffinity
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      bindTimeoutSeconds: 300
      kind: VolumeBindingArgs
      shape:
      - score: 0
        utilization: 0
      - score: 10
        utilization: 100
    name: VolumeBinding
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      kind: NodeResourcesFitArgs
      scoringStrategy:
        requestedToCapacityRatio:
          shape:
          - score: 2
            utilization: 1
        resources:
        - name: cpu
          weight: 1
        type: RequestedToCapacityRatio
    name: NodeResourcesFit
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      kind: PodTopologySpreadArgs
    name: PodTopologySpread
  - args:
      foo: bar
    name: OutOfTreePlugin
`,
		},
		{
			// 内部对象走同一编码器：转换后应输出相同的 v1 结构。
			name:    "v1 in-tree and out-of-tree plugins from internal",
			version: v1.SchemeGroupVersion,
			obj: &config.KubeSchedulerConfiguration{
				Parallelism:           8,
				DelayCacheUntilActive: true,
				Profiles: []config.KubeSchedulerProfile{
					{
						PluginConfig: []config.PluginConfig{
							{
								Name: "InterPodAffinity",
								Args: &config.InterPodAffinityArgs{
									HardPodAffinityWeight: 5,
								},
							},
							{
								Name: "NodeResourcesFit",
								Args: &config.NodeResourcesFitArgs{
									ScoringStrategy: &config.ScoringStrategy{
										Type:      config.LeastAllocated,
										Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}},
									},
								},
							},
							{
								Name: "VolumeBinding",
								Args: &config.VolumeBindingArgs{
									BindTimeoutSeconds: 300,
								},
							},
							{
								Name: "PodTopologySpread",
								Args: &config.PodTopologySpreadArgs{},
							},
							{
								Name: "OutOfTreePlugin",
								Args: &runtime.Unknown{
									Raw: []byte(`{"foo":"bar"}`),
								},
							},
						},
					},
				},
			},
			want: `apiVersion: kubescheduler.config.k8s.io/v1
clientConnection:
  acceptContentTypes: ""
  burst: 0
  contentType: ""
  kubeconfig: ""
  qps: 0
delayCacheUntilActive: true
enableContentionProfiling: false
enableProfiling: false
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: false
  leaseDuration: 0s
  renewDeadline: 0s
  resourceLock: ""
  resourceName: ""
  resourceNamespace: ""
  retryPeriod: 0s
parallelism: 8
podInitialBackoffSeconds: 0
podMaxBackoffSeconds: 0
profiles:
- pluginConfig:
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      hardPodAffinityWeight: 5
      ignorePreferredTermsOfExistingPods: false
      kind: InterPodAffinityArgs
    name: InterPodAffinity
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      kind: NodeResourcesFitArgs
      scoringStrategy:
        resources:
        - name: cpu
          weight: 1
        type: LeastAllocated
    name: NodeResourcesFit
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      bindTimeoutSeconds: 300
      kind: VolumeBindingArgs
    name: VolumeBinding
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      kind: PodTopologySpreadArgs
    name: PodTopologySpread
  - args:
      foo: bar
    name: OutOfTreePlugin
  schedulerName: ""
`,
		},
		{
			// true 值必须被序列化出来，不能被默认值 false 覆盖。
			name:    "v1 ignorePreferredTermsOfExistingPods is enabled",
			version: v1.SchemeGroupVersion,
			obj: &config.KubeSchedulerConfiguration{
				Parallelism:           8,
				DelayCacheUntilActive: true,
				Profiles: []config.KubeSchedulerProfile{
					{
						PluginConfig: []config.PluginConfig{
							{
								Name: "InterPodAffinity",
								Args: &config.InterPodAffinityArgs{
									HardPodAffinityWeight:              5,
									IgnorePreferredTermsOfExistingPods: true,
								},
							},
						},
					},
				},
			},
			want: `apiVersion: kubescheduler.config.k8s.io/v1
clientConnection:
  acceptContentTypes: ""
  burst: 0
  contentType: ""
  kubeconfig: ""
  qps: 0
delayCacheUntilActive: true
enableContentionProfiling: false
enableProfiling: false
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: false
  leaseDuration: 0s
  renewDeadline: 0s
  resourceLock: ""
  resourceName: ""
  resourceNamespace: ""
  retryPeriod: 0s
parallelism: 8
podInitialBackoffSeconds: 0
podMaxBackoffSeconds: 0
profiles:
- pluginConfig:
  - args:
      apiVersion: kubescheduler.config.k8s.io/v1
      hardPodAffinityWeight: 5
      ignorePreferredTermsOfExistingPods: true
      kind: InterPodAffinityArgs
    name: InterPodAffinity
  schedulerName: ""
`,
		},
	}
	// YAML 期望结果是手写常量，先确认 YAML 编码器确实可用。
	yamlInfo, ok := runtime.SerializerInfoForMediaType(Codecs.SupportedMediaTypes(), runtime.ContentTypeYAML)
	if !ok {
		t.Fatalf("unable to locate encoder -- %q is not a supported media type", runtime.ContentTypeYAML)
	}
	// JSON 路径可复用同一期望值（JSON 是 YAML 的子集），同样先确认编码器可用。
	jsonInfo, ok := runtime.SerializerInfoForMediaType(Codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
	if !ok {
		t.Fatalf("unable to locate encoder -- %q is not a supported media type", runtime.ContentTypeJSON)
	}
	// 每条用例都经 YAML 和 JSON 两种序列化路径，结果必须与期望逐字一致。
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			encoder := Codecs.EncoderForVersion(yamlInfo.Serializer, tt.version)
			var buf bytes.Buffer
			if err := encoder.Encode(tt.obj, &buf); err != nil {
				t.Fatal(err)
			}
			// YAML 编码器输出直接比对。
			if diff := cmp.Diff(tt.want, buf.String()); diff != "" {
				t.Errorf("unexpected encoded configuration:\n%s", diff)
			}
			// 复用 buf 前清空，避免两次编码结果拼接。
			encoder = Codecs.EncoderForVersion(jsonInfo.Serializer, tt.version)
			buf = bytes.Buffer{}
			if err := encoder.Encode(tt.obj, &buf); err != nil {
				t.Fatal(err)
			}
			// JSON 先转成 YAML，才能与同一份期望值比较，顺带验证两条路径等价。
			out, err := yaml.JSONToYAML(buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, string(out)); diff != "" {
				t.Errorf("unexpected encoded configuration:\n%s", diff)
			}
		})
	}
}
