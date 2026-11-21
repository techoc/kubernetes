/*
Copyright 2017 The Kubernetes Authors.

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

package config

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestKubeletConfigurationPathFields 校验路径字段集合与非路径字段集合互斥，
// 且两者并集覆盖 KubeletConfiguration 的所有原始字段。
func TestKubeletConfigurationPathFields(t *testing.T) {
	// 两个集合不能有交集：一个字段要么是路径，要么不是。
	if i := kubeletConfigurationPathFieldPaths.Intersection(kubeletConfigurationNonPathFieldPaths); len(i) > 0 {
		t.Fatalf("expect the intersection of kubeletConfigurationPathFieldPaths and "+
			"KubeletConfigurationNonPathFields to be empty, got:\n%s",
			strings.Join(sets.List(i), "\n"))
	}

	// 两个集合的并集必须等于 KubeletConfiguration 的全部原始字段路径。
	expect := sets.New[string]().Union(kubeletConfigurationPathFieldPaths).Union(kubeletConfigurationNonPathFieldPaths)
	result := allPrimitiveFieldPaths(t, expect, reflect.TypeOf(&KubeletConfiguration{}), nil)
	if !expect.Equal(result) {
		// 期望存在但结果中缺失的字段。
		missing := expect.Difference(result)
		// 结果中出现但未在期望集合中声明的字段。
		unexpected := result.Difference(expect)
		if len(missing) > 0 {
			t.Errorf("the following fields were expected, but missing from the result. "+
				"If the field has been removed, please remove it from the kubeletConfigurationPathFieldPaths set "+
				"and the KubeletConfigurationPathRefs function, "+
				"or remove it from the kubeletConfigurationNonPathFieldPaths set, as appropriate:\n%s",
				strings.Join(sets.List(missing), "\n"))
		}
		if len(unexpected) > 0 {
			t.Errorf("the following fields were in the result, but unexpected. "+
				"If the field is new, please add it to the kubeletConfigurationPathFieldPaths set "+
				"and the KubeletConfigurationPathRefs function, "+
				"or add it to the kubeletConfigurationNonPathFieldPaths set, as appropriate:\n%s",
				strings.Join(sets.List(unexpected), "\n"))
		}
	}
}

// allPrimitiveFieldPaths 返回类型 tp 中以 path 为根的所有原始字段路径。
// 递归下降结构体定义，遇到原始类型叶子或 skipRecurseList 中的路径时停止。
func allPrimitiveFieldPaths(t *testing.T, skipRecurseList sets.Set[string], tp reflect.Type, path *field.Path) sets.Set[string] {
	// 当前路径在“不递归”列表中时直接返回该路径，不再收集子字段。
	if pathStr := path.String(); len(pathStr) > 0 && skipRecurseList.Has(pathStr) {
		return sets.New[string](pathStr)
	}

	paths := sets.New[string]()
	// 按类型递归：指针/结构体/映射/切片分别展开；interface 不支持。
	switch tp.Kind() {
	case reflect.Pointer:
		paths.Insert(sets.List(allPrimitiveFieldPaths(t, skipRecurseList, tp.Elem(), path))...)
	case reflect.Struct:
		for i := 0; i < tp.NumField(); i++ {
			field := tp.Field(i)
			paths.Insert(sets.List(allPrimitiveFieldPaths(t, skipRecurseList, field.Type, path.Child(field.Name)))...)
		}
	case reflect.Map, reflect.Slice:
		paths.Insert(sets.List(allPrimitiveFieldPaths(t, skipRecurseList, tp.Elem(), path.Key("*")))...)
	case reflect.Interface:
		t.Fatalf("unexpected interface{} field %s", path.String())
	default:
		// 原始类型即叶子，记录其路径。
		paths.Insert(path.String())
	}
	return paths
}

//lint:file-ignore U1000 Ignore dummy types, used by tests.

// 用于测试反射遍历的辅助类型。
type foo struct {
	foo int
}
type bar struct {
	str    string
	strptr *string

	ints      []int
	stringMap map[string]string

	foo    foo
	fooptr *foo

	bars   []foo
	barMap map[string]foo

	skipRecurseStruct  foo
	skipRecursePointer *foo
	skipRecurseList1   []foo
	skipRecurseList2   []foo
	skipRecurseMap1    map[string]foo
	skipRecurseMap2    map[string]foo
}

// TestAllPrimitiveFieldPaths 验证反射遍历能正确处理指针、切片、映射，
// 并在 skipRecurseList 指定的位置停止递归。
func TestAllPrimitiveFieldPaths(t *testing.T) {
	expect := sets.New[string](
		"str",
		"strptr",
		"ints[*]",
		"stringMap[*]",
		"foo.foo",
		"fooptr.foo",
		"bars[*].foo",
		"barMap[*].foo",
		"skipRecurseStruct",   // 不递归结构体
		"skipRecursePointer",  // 不递归结构体指针
		"skipRecurseList1",    // 不递归列表
		"skipRecurseList2[*]", // 不递归列表元素
		"skipRecurseMap1",     // 不递归映射
		"skipRecurseMap2[*]",  // 不递归映射元素
	)
	result := allPrimitiveFieldPaths(t, expect, reflect.TypeOf(&bar{}), nil)
	if !expect.Equal(result) {
		// 期望存在但结果中缺失的字段。
		missing := expect.Difference(result)

		// 结果中出现但未在期望集合中声明的字段。
		unexpected := result.Difference(expect)

		if len(missing) > 0 {
			t.Errorf("the following fields were expected, but missing from the result:\n%s", strings.Join(sets.List(missing), "\n"))
		}
		if len(unexpected) > 0 {
			t.Errorf("the following fields were in the result, but unexpected:\n%s", strings.Join(sets.List(unexpected), "\n"))
		}
	}
}

var (
	// 包含文件路径的 KubeletConfiguration 字段；修改时需同步更新 KubeletConfigurationPathRefs。
	kubeletConfigurationPathFieldPaths = sets.New[string](
		"StaticPodPath",
		"Authentication.X509.ClientCAFile",
		"TLSCertFile",
		"TLSPrivateKeyFile",
		"ResolverConfig",
		"PodLogsDir",
	)

	// 不包含文件路径的 KubeletConfiguration 字段。
	kubeletConfigurationNonPathFieldPaths = sets.New[string](
		"Address",
		"AllowedUnsafeSysctls[*]",
		"DefaultPodSysctls[*]",
		"Authentication.Anonymous.Enabled",
		"Authentication.Webhook.CacheTTL.Duration",
		"Authentication.Webhook.Enabled",
		"Authorization.Mode",
		"Authorization.Webhook.CacheAuthorizedTTL.Duration",
		"Authorization.Webhook.CacheUnauthorizedTTL.Duration",
		"CPUCFSQuota",
		"CPUCFSQuotaPeriod.Duration",
		"CPUManagerPolicy",
		"CPUManagerPolicyOptions[*]",
		"CPUManagerReconcilePeriod.Duration",
		"TopologyManagerPolicy",
		"TopologyManagerScope",
		"TopologyManagerPolicyOptions[*]",
		"QOSReserved[*]",
		"CgroupDriver",
		"CgroupRoot",
		"CgroupsPerQOS",
		"ClusterDNS[*]",
		"ClusterDomain",
		"ConfigMapAndSecretChangeDetectionStrategy",
		"ContainerLogMaxFiles",
		"ContainerLogMaxSize",
		"ContainerLogMaxWorkers",
		"ContainerLogMonitorInterval",
		"ContentType",
		"EnableContentionProfiling",
		"EnableControllerAttachDetach",
		"EnableDebugFlagsHandler",
		"EnableDebuggingHandlers",
		"EnableSystemLogQuery",
		"EnableProfilingHandler",
		"EnableServer",
		"EnableSystemLogHandler",
		"EnforceNodeAllocatable[*]",
		"EventBurst",
		"EventRecordQPS",
		"EvictionHard[*]",
		"EvictionMaxPodGracePeriod",
		"EvictionMinimumReclaim[*]",
		"EvictionPressureTransitionPeriod.Duration",
		"EvictionSoft[*]",
		"EvictionSoftGracePeriod[*]",
		"FailSwapOn",
		"FeatureGates[*]",
		"FileCheckFrequency.Duration",
		"HTTPCheckFrequency.Duration",
		"HairpinMode",
		"HealthzBindAddress",
		"HealthzPort",
		"Logging.FlushFrequency",
		"Logging.Format",
		"Logging.Options.JSON.OutputRoutingOptions.InfoBufferSize.Quantity.Format",
		"Logging.Options.JSON.OutputRoutingOptions.InfoBufferSize.Quantity.d.Dec.scale",
		"Logging.Options.JSON.OutputRoutingOptions.InfoBufferSize.Quantity.d.Dec.unscaled.abs[*]",
		"Logging.Options.JSON.OutputRoutingOptions.InfoBufferSize.Quantity.d.Dec.unscaled.neg",
		"Logging.Options.JSON.OutputRoutingOptions.InfoBufferSize.Quantity.i.scale",
		"Logging.Options.JSON.OutputRoutingOptions.InfoBufferSize.Quantity.i.value",
		"Logging.Options.JSON.OutputRoutingOptions.InfoBufferSize.Quantity.s",
		"Logging.Options.JSON.OutputRoutingOptions.SplitStream",
		"Logging.Options.Text.OutputRoutingOptions.InfoBufferSize.Quantity.Format",
		"Logging.Options.Text.OutputRoutingOptions.InfoBufferSize.Quantity.d.Dec.scale",
		"Logging.Options.Text.OutputRoutingOptions.InfoBufferSize.Quantity.d.Dec.unscaled.abs[*]",
		"Logging.Options.Text.OutputRoutingOptions.InfoBufferSize.Quantity.d.Dec.unscaled.neg",
		"Logging.Options.Text.OutputRoutingOptions.InfoBufferSize.Quantity.i.scale",
		"Logging.Options.Text.OutputRoutingOptions.InfoBufferSize.Quantity.i.value",
		"Logging.Options.Text.OutputRoutingOptions.InfoBufferSize.Quantity.s",
		"Logging.Options.Text.OutputRoutingOptions.SplitStream",
		"Logging.VModule[*].FilePattern",
		"Logging.VModule[*].Verbosity",
		"SingleProcessOOMKill",
		"Logging.Verbosity",
		"TLSCipherSuites[*]",
		"TLSCurvePreferences[*]",
		"TLSMinVersion",
		"IPTablesDropBit",
		"IPTablesMasqueradeBit",
		"ImageGCHighThresholdPercent",
		"ImageGCLowThresholdPercent",
		"ImageMinimumGCAge.Duration",
		"ImageMaximumGCAge.Duration",
		"ImagePullCredentialsVerificationPolicy",
		"KernelMemcgNotification",
		"KubeAPIBurst",
		"KubeAPIQPS",
		"KubeReservedCgroup",
		"KubeReserved[*]",
		"KubeletCgroups",
		"MakeIPTablesUtilChains",
		"RotateCertificates",
		"ServerTLSBootstrap",
		"StaticPodURL",
		"StaticPodURLHeader[*][*]",
		"MaxOpenFiles",
		"MaxPods",
		"MemoryManagerPolicy",
		"MemorySwap.SwapBehavior",
		"MergeDefaultEvictionSettings",
		"NodeLeaseDurationSeconds",
		"NodeStatusMaxImages",
		"NodeStatusUpdateFrequency.Duration",
		"NodeStatusReportFrequency.Duration",
		"OOMScoreAdj",
		"PodCIDR",
		"PodPidsLimit",
		"PodsPerCore",
		"Port",
		"PreloadedImagesVerificationAllowlist[*]",
		"ProtectKernelDefaults",
		"ProviderID",
		"ReadOnlyPort",
		"RegisterNode",
		"RegistryBurst",
		"RegistryPullQPS",
		"ReservedMemory",
		"ReservedSystemCPUs",
		"RegisterWithTaints",
		"RuntimeRequestTimeout.Duration",
		"RunOnce",
		"SeccompDefault",
		"SerializeImagePulls",
		"MaxParallelImagePulls",
		"ShowHiddenMetricsForVersion",
		"ShutdownGracePeriodByPodPriority[*].Priority",
		"ShutdownGracePeriodByPodPriority[*].ShutdownGracePeriodSeconds",
		"StreamingConnectionIdleTimeout.Duration",
		"SyncFrequency.Duration",
		"SystemCgroups",
		"SystemReservedCgroup",
		"SystemReserved[*]",
		"TypeMeta.APIVersion",
		"TypeMeta.Kind",
		"VolumeStatsAggPeriod.Duration",
		"VolumePluginDir",
		"ShutdownGracePeriod.Duration",
		"ShutdownGracePeriodCriticalPods.Duration",
		"MemoryThrottlingFactor",
		"MemoryReservationPolicy",
		"ContainerRuntimeEndpoint",
		"ImageServiceEndpoint",
		"Tracing.Endpoint",
		"Tracing.SamplingRatePerMillion",
		"LocalStorageCapacityIsolation",
		"FailCgroupV1",
		"CrashLoopBackOff.MaxContainerRestartPeriod",
		"UserNamespaces.IDsPerPod",
	)
)
