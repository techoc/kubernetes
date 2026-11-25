/*
Copyright 2022 The Kubernetes Authors.

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

package v1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/util/feature"
	componentbaseconfigv1alpha1 "k8s.io/component-base/config/v1alpha1"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/utils/ptr"
)

// defaultResourceSpec 是评分插件默认的资源及权重（CPU、内存，权重均为 1）。
var defaultResourceSpec = []configv1.ResourceSpec{
	{Name: string(v1.ResourceCPU), Weight: 1},
	{Name: string(v1.ResourceMemory), Weight: 1},
}

// addDefaultingFuncs 向 scheme 注册 v1 配置的默认值生成函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// pluginsNames 汇总所有扩展点上启用的插件名，去重后按字典序返回。
func pluginsNames(p *configv1.Plugins) []string {
	if p == nil {
		return nil
	}
	extensions := []configv1.PluginSet{
		p.MultiPoint,
		p.PreFilter,
		p.Filter,
		p.PostFilter,
		p.Reserve,
		p.PreScore,
		p.Score,
		p.PreBind,
		p.Bind,
		p.PostBind,
		p.Permit,
		p.PreEnqueue,
		p.QueueSort,
		p.PlacementGenerate,
		p.PlacementScore,
	}
	n := sets.New[string]()
	for _, e := range extensions {
		for _, pg := range e.Enabled {
			n.Insert(pg.Name)
		}
	}
	return sets.List(n)
}

func setDefaults_KubeSchedulerProfile(logger klog.Logger, prof *configv1.KubeSchedulerProfile) {
	// 合并默认插件与用户配置，得到该 profile 最终启用的插件集合。
	prof.Plugins = mergePlugins(logger, getDefaultPlugins(), prof.Plugins)
	// 为显式配置的插件参数补默认值；跳过 Unknown 以免误改外部插件的原始参数。
	scheme := GetPluginArgConversionScheme()
	existingConfigs := sets.New[string]()
	for j := range prof.PluginConfig {
		existingConfigs.Insert(prof.PluginConfig[j].Name)
		args := prof.PluginConfig[j].Args.Object
		if _, isUnknown := args.(*runtime.Unknown); isUnknown {
			continue
		}
		scheme.Default(args)
	}

	// 为未显式配置的插件追加默认参数；外部插件或无需配置的插件在 scheme.New 时会被跳过。
	for _, name := range pluginsNames(prof.Plugins) {
		if existingConfigs.Has(name) {
			continue
		}
		gvk := configv1.SchemeGroupVersion.WithKind(name + "Args")
		args, err := scheme.New(gvk)
		if err != nil {
			// 外部插件或无需配置的插件在 scheme 中无对应 Args 类型，跳过即可。
			continue
		}
		scheme.Default(args)
		args.GetObjectKind().SetGroupVersionKind(gvk)
		prof.PluginConfig = append(prof.PluginConfig, configv1.PluginConfig{
			Name: name,
			Args: runtime.RawExtension{Object: args},
		})
	}
}

// SetDefaults_KubeSchedulerConfiguration 为调度器配置补充默认值：并发度、profile、选主、客户端连接及退避等。
func SetDefaults_KubeSchedulerConfiguration(obj *configv1.KubeSchedulerConfiguration) {
	logger := klog.TODO() // 由未传入 logger 的生成代码调用，参见 #115724。
	if obj.Parallelism == nil {
		obj.Parallelism = ptr.To[int32](16)
	}

	if len(obj.Profiles) == 0 {
		obj.Profiles = append(obj.Profiles, configv1.KubeSchedulerProfile{})
	}
	// 仅当只有一个 profile 时才自动填默认调度器名；多 profile 时名字必须显式给出，
	// 校验阶段会保证每个 profile 的名字非空且唯一。
	if len(obj.Profiles) == 1 && obj.Profiles[0].SchedulerName == nil {
		obj.Profiles[0].SchedulerName = ptr.To(v1.DefaultSchedulerName)
	}

	// 逐个 profile 应用默认插件集与默认插件参数。
	for i := range obj.Profiles {
		prof := &obj.Profiles[i]
		setDefaults_KubeSchedulerProfile(logger, prof)
	}

	if obj.PercentageOfNodesToScore == nil {
		obj.PercentageOfNodesToScore = ptr.To[int32](config.DefaultPercentageOfNodesToScore)
	}

	if len(obj.LeaderElection.ResourceLock) == 0 {
		// 采用 Lease 锁降低对 apiserver 的压力：1.17 起从 Endpoints 迁移到 EndpointsLease，
		// 1.20 起进一步迁移到原生 Lease。
		obj.LeaderElection.ResourceLock = "leases"
	}
	if len(obj.LeaderElection.ResourceNamespace) == 0 {
		obj.LeaderElection.ResourceNamespace = configv1.SchedulerDefaultLockObjectNamespace
	}
	if len(obj.LeaderElection.ResourceName) == 0 {
		obj.LeaderElection.ResourceName = configv1.SchedulerDefaultLockObjectName
	}

	if len(obj.ClientConnection.ContentType) == 0 {
		obj.ClientConnection.ContentType = "application/vnd.kubernetes.protobuf"
	}
	// 调度器对 QPS/Burst 有专门默认值，不沿用通用的客户端默认值。
	if obj.ClientConnection.QPS == 0.0 {
		obj.ClientConnection.QPS = 50.0
	}
	if obj.ClientConnection.Burst == 0 {
		obj.ClientConnection.Burst = 100
	}

	// 用 component-base 推荐的选主配置补齐时长等字段。
	componentbaseconfigv1alpha1.RecommendedDefaultLeaderElectionConfiguration(&obj.LeaderElection)

	if obj.PodInitialBackoffSeconds == nil {
		obj.PodInitialBackoffSeconds = ptr.To[int64](1)
	}

	if obj.PodMaxBackoffSeconds == nil {
		obj.PodMaxBackoffSeconds = ptr.To[int64](10)
	}

	// 调度器默认开启 profiling，便于线上排查性能问题。
	if obj.EnableProfiling == nil {
		obj.EnableProfiling = ptr.To(true)
	}

	// 锁竞争采样依赖 profiling，故仅在 profiling 开启时默认启用。
	if *obj.EnableProfiling && obj.EnableContentionProfiling == nil {
		obj.EnableContentionProfiling = ptr.To(true)
	}
}

// SetDefaults_DefaultPreemptionArgs 为默认抢占插件设置候选节点比例与绝对数量下限。
func SetDefaults_DefaultPreemptionArgs(obj *configv1.DefaultPreemptionArgs) {
	if obj.MinCandidateNodesPercentage == nil {
		obj.MinCandidateNodesPercentage = ptr.To[int32](10)
	}
	if obj.MinCandidateNodesAbsolute == nil {
		obj.MinCandidateNodesAbsolute = ptr.To[int32](100)
	}
}

// SetDefaults_InterPodAffinityArgs 为 Pod 间亲和性插件设置硬约束默认权重。
func SetDefaults_InterPodAffinityArgs(obj *configv1.InterPodAffinityArgs) {
	if obj.HardPodAffinityWeight == nil {
		obj.HardPodAffinityWeight = ptr.To[int32](1)
	}
}

// SetDefaults_VolumeBindingArgs 为卷绑定插件设置超时，并在启用容量评分特性时补充默认评分曲线。
func SetDefaults_VolumeBindingArgs(obj *configv1.VolumeBindingArgs) {
	if obj.BindTimeoutSeconds == nil {
		obj.BindTimeoutSeconds = ptr.To[int64](600)
	}
	if len(obj.Shape) == 0 && feature.DefaultFeatureGate.Enabled(features.StorageCapacityScoring) {
		obj.Shape = []configv1.UtilizationShapePoint{
			{
				Utilization: 0,
				Score:       int32(config.MaxCustomPriorityScore),
			},
			{
				Utilization: 100,
				Score:       0,
			},
		}
	}
}

// SetDefaults_NodeResourcesBalancedAllocationArgs 为均衡分配插件设置默认资源及权重；权重为 0 时按 1 处理。
func SetDefaults_NodeResourcesBalancedAllocationArgs(obj *configv1.NodeResourcesBalancedAllocationArgs) {
	if len(obj.Resources) == 0 {
		obj.Resources = defaultResourceSpec
		return
	}
	// 权重未设置或显式为 0 都视为使用默认值 1，避免用户漏配导致资源不参与评分。
	for i := range obj.Resources {
		if obj.Resources[i].Weight == 0 {
			obj.Resources[i].Weight = 1
		}
	}
}

// SetDefaults_PodTopologySpreadArgs 为拓扑分布插件设置默认的默认约束补全策略。
func SetDefaults_PodTopologySpreadArgs(obj *configv1.PodTopologySpreadArgs) {
	if obj.DefaultingType == "" {
		obj.DefaultingType = configv1.SystemDefaulting
	}
}

// SetDefaults_NodeResourcesFitArgs 为节点资源适配插件设置默认评分策略（最少分配）及资源列表。
func SetDefaults_NodeResourcesFitArgs(obj *configv1.NodeResourcesFitArgs) {
	if obj.ScoringStrategy == nil {
		obj.ScoringStrategy = &configv1.ScoringStrategy{
			Type:      configv1.ScoringStrategyType(config.LeastAllocated),
			Resources: defaultResourceSpec,
		}
	}
	if len(obj.ScoringStrategy.Resources) == 0 {
		// 未指定资源时使用默认集合（CPU、内存）。
		obj.ScoringStrategy.Resources = append(obj.ScoringStrategy.Resources, defaultResourceSpec...)
	}
	for i := range obj.ScoringStrategy.Resources {
		if obj.ScoringStrategy.Resources[i].Weight == 0 {
			obj.ScoringStrategy.Resources[i].Weight = 1
		}
	}
}

// SetDefaults_DynamicResourcesArgs 为动态资源插件设置过滤/绑定超时，仅在对应特性门控开启时生效。
func SetDefaults_DynamicResourcesArgs(obj *configv1.DynamicResourcesArgs) {
	if obj.FilterTimeout == nil && feature.DefaultFeatureGate.Enabled(features.DRASchedulerFilterTimeout) {
		obj.FilterTimeout = &metav1.Duration{Duration: configv1.DynamicResourcesFilterTimeoutDefault}
	}

	if obj.BindingTimeout == nil &&
		feature.DefaultFeatureGate.Enabled(features.DRADeviceBindingConditions) &&
		feature.DefaultFeatureGate.Enabled(features.DRAResourceClaimDeviceStatus) {
		obj.BindingTimeout = &metav1.Duration{Duration: configv1.DynamicResourcesBindingTimeoutDefault}
	}
}
