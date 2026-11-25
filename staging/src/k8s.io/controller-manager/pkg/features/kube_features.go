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

package features

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
)

// 本文件集中声明 controller-manager（主要是 CCM/KCM）专有的 feature gate。
// 新增 gate 时：先在此处定义常量，再在下面的版本表中登记其版本与默认状态。
// 每个 feature gate 都应在此按如下模板登记条目：
//
// // owner: @username
// MyFeature featuregate.Feature = "MyFeature"
//
// feature gate 应按区分大小写的字母序（大写在前）排列，
// 这可减少多人同时修改同一区域造成的代码冲突，因为改动会更分散地落在文件各处。
const (
	// owner: @lukasmetzner
	// kep:  http://kep.k8s.io/5237
	// 使用 watch 驱动路由控制器对账，替代频繁的定时对账（KEP-5237，Alpha）。
	CloudControllerManagerWatchBasedRoutesReconciliation featuregate.Feature = "CloudControllerManagerWatchBasedRoutesReconciliation"

	// owner: @nckturner
	// kep:  http://kep.k8s.io/2699
	// 在 cloud-controller-manager 中启用 webhook（KEP-2699，Alpha）。
	CloudControllerManagerWebhook featuregate.Feature = "CloudControllerManagerWebhook"

	// owner: @jefftree, @tchap
	//
	// 让 kube-controller-manager 退出时主动释放领导者选举锁，缩短故障转移等待时间（Alpha）。
	ControllerManagerReleaseLeaderElectionLockOnExit featuregate.Feature = "ControllerManagerReleaseLeaderElectionLockOnExit"
)

// 把本包定义的版本化 feature gate 注册到给定的可变 feature gate 集合中。
func SetupCurrentKubernetesSpecificFeatureGates(featuregates featuregate.MutableVersionedFeatureGate) error {
	return featuregates.AddVersioned(versionedCloudPublicFeatureGates)
}

// versionedCloudPublicFeatureGates 收录版本化的云专有 feature key：
// 记录每个 gate 首次出现的版本、默认值与发布阶段（Alpha/Beta/GA）。
// 新增特性时先在上面定义 key，再添加到这里。
var versionedCloudPublicFeatureGates = map[featuregate.Feature]featuregate.VersionedSpecs{
	CloudControllerManagerWatchBasedRoutesReconciliation: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
	},
	CloudControllerManagerWebhook: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
	},
	ControllerManagerReleaseLeaderElectionLockOnExit: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},
}
