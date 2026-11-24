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

package features

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
)

// 本文件定义 apiextensions-apiserver 的功能门控（Feature Gates）。
// 每个功能门控控制一个 CRD 相关特性的开关状态（Alpha/Beta/GA）。
//
// 每个功能门控都应按照以下模板在此处登记：
//
// // owner: @username
// MyFeature() bool
//
// 功能门控应按区分大小写的字母顺序排列（大写字母排在小写字母之前）。
// 这样可以让不同功能门控的修改分散在文件各处，降低多人并行修改时发生冲突的概率。
const (
	// owner: @michaelasp
	// kep: https://kep.k8s.io/4192
	//
	// 在 CRD 的 status 和 conditions 中跟踪 observedGeneration。
	// 使控制器能够判断当前 status 是否反映了最新的 spec。
	CRDObservedGenerationTracking featuregate.Feature = "CRDObservedGenerationTracking"

	// owner: @alexzielenski
	//
	// 在 UPDATE/PATCH 请求中，忽略未修改字段的验证错误。
	// 当 CRD 的验证规则升级后，旧资源中未变更的字段即使不满足新规则也不会报错。
	// 这避免了批量更新所有资源的需求。
	CRDValidationRatcheting featuregate.Feature = "CRDValidationRatcheting"

	// owner: @jpbetz
	//
	// 允许 CRD 通过 SelectableFields 声明哪些字段可用于字段选择器（field selector）。
	// 使 CR 支持类似 "status.phase=Running" 的查询过滤。
	CustomResourceFieldSelectors featuregate.Feature = "CustomResourceFieldSelectors"
)

// init 将所有功能门控注册到全局 DefaultMutableFeatureGate。
// 这样 apiextensions-apiserver 和 kube-apiserver 都可以使用这些功能门控。
func init() {
	runtime.Must(utilfeature.DefaultMutableFeatureGate.AddVersioned(defaultVersionedKubernetesFeatureGates))
}

// defaultVersionedKubernetesFeatureGates 定义了所有 CRD 功能门控的版本化配置。
// 每个功能门控记录了从 Alpha → Beta → GA 的演进历程，包括默认值和锁定状态。
// 为了支持 n-3 兼容性，功能门控在 GA 后至少保留 3 个版本才能移除。
//
// 条目按字母顺序排列。
var defaultVersionedKubernetesFeatureGates = map[featuregate.Feature]featuregate.VersionedSpecs{
	CRDObservedGenerationTracking: {
		{Version: version.MustParse("1.35"), PreRelease: featuregate.Beta, Default: false},
		{Version: version.MustParse("1.36"), PreRelease: featuregate.Beta, Default: true},
	},
	CRDValidationRatcheting: {
		{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, LockToDefault: true, PreRelease: featuregate.GA},
	},
	CustomResourceFieldSelectors: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.32"), Default: true, LockToDefault: true, PreRelease: featuregate.GA},
	},
}
