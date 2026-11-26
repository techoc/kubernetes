/*
Copyright 2014 The Kubernetes Authors.

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

package skipper

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/discovery"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/featuregate"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

func skipInternalf(caller int, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	ginkgo.Skip(msg, caller+1)
	panic("unreachable")
}

// Skipf 跳过测试并记录跳过原因。
// 直接调用方会记录在 callstack 中。
func Skipf(format string, args ...interface{}) {
	skipInternalf(1, format, args...)
	panic("unreachable")
}

// Skip 是 ginkgo.Skip 的别名。
var Skip = ginkgo.Skip

// 当 value 小于 minValue 时 SkipUnlessAtLeast 跳过测试。
func SkipUnlessAtLeast(value int, minValue int, message string) {
	if value < minValue {
		skipInternalf(1, "%s", message)
	}
}

var featureGate featuregate.FeatureGate

// 带有 --feature-gates 参数的测试套件必须调用 InitFeatureGates。
// 未调用时，SkipUnlessFeatureGateEnabled 会记录测试失败。
func InitFeatureGates(defaults featuregate.FeatureGate, overrides map[string]bool) error {
	clone := defaults.DeepCopy()
	if err := clone.SetFromMap(overrides); err != nil {
		return err
	}
	featureGate = clone
	return nil
}

// e2e 测试期间可用 IsFeatureGateEnabled 判断某个 feature gate 是否启用。
// 该函数底层依赖 InitFeatureGates，因此测试必须以
// --feature-gates 参数调用。
func IsFeatureGateEnabled(feature featuregate.Feature) bool {
	if featureGate == nil {
		framework.Failf("feature gate interface is not initialized")
	}
	return featureGate.Enabled(feature)
}

// 特性被禁用时 SkipUnlessFeatureGateEnabled 跳过测试。
//
// 注意，这只适用于带 --feature-gate 参数的测试套件
// 且其中调用了 InitFeatureGates。在 test/e2e 中必须改用 `Feature: XYZ` 标签，
// 并且调用时要确保
// 只运行与给定测试集群兼容的测试。
func SkipUnlessFeatureGateEnabled(gate featuregate.Feature) {
	if featureGate == nil {
		framework.Failf("Feature gate checking is not enabled, don't use SkipUnlessFeatureGateEnabled(%v). Instead use the Feature tag.", gate)
	}
	if !featureGate.Enabled(gate) {
		skipInternalf(1, "Only supported when %v feature is enabled", gate)
	}
}

// 节点数少于 minNodeCount 时 SkipUnlessNodeCountIsAtLeast 跳过测试。
func SkipUnlessNodeCountIsAtLeast(minNodeCount int) {
	if framework.TestContext.CloudConfig.NumNodes < minNodeCount {
		skipInternalf(1, "Requires at least %d nodes (not %d)", minNodeCount, framework.TestContext.CloudConfig.NumNodes)
	}
}

// 节点数大于 maxNodeCount 时 SkipUnlessNodeCountIsAtMost 跳过测试。
func SkipUnlessNodeCountIsAtMost(maxNodeCount int) {
	if framework.TestContext.CloudConfig.NumNodes > maxNodeCount {
		skipInternalf(1, "Requires at most %d nodes (not %d)", maxNodeCount, framework.TestContext.CloudConfig.NumNodes)
	}
}

// 集群不是 multizone 时 SkipUnlessMultizone 跳过测试。
func SkipUnlessMultizone(ctx context.Context, c clientset.Interface) {
	zones, err := e2enode.GetClusterZones(ctx, c)
	if err != nil {
		skipInternalf(1, "Error listing cluster zones")
	}
	if zones.Len() <= 1 {
		skipInternalf(1, "Requires more than one zone")
	}
}

// 集群没有 n 个 multizone 时 SkipUnlessAtLeastNZones 跳过测试。
func SkipUnlessAtLeastNZones(ctx context.Context, c clientset.Interface, n int) {
	zones, err := e2enode.GetClusterZones(ctx, c)
	if err != nil {
		skipInternalf(1, "Error listing cluster zones")
	}
	if zones.Len() < n {
		skipInternalf(1, "Requires >= %d zones", n)
	}
}

// 集群是 multizone 时 SkipIfMultizone 跳过测试。
func SkipIfMultizone(ctx context.Context, c clientset.Interface) {
	zones, err := e2enode.GetClusterZones(ctx, c)
	if err != nil {
		skipInternalf(1, "Error listing cluster zones")
	}
	if zones.Len() > 1 {
		skipInternalf(1, "Requires at most one zone")
	}
}

// master OS 发行版不在 supportedMasterOsDistros 中时 SkipUnlessMasterOSDistroIs 跳过测试。
func SkipUnlessMasterOSDistroIs(supportedMasterOsDistros ...string) {
	if !framework.MasterOSDistroIs(supportedMasterOsDistros...) {
		skipInternalf(1, "Only supported for master OS distro %v (not %s)", supportedMasterOsDistros, framework.TestContext.MasterOSDistro)
	}
}

// node OS 发行版不在 supportedNodeOsDistros 中时 SkipUnlessNodeOSDistroIs 跳过测试。
func SkipUnlessNodeOSDistroIs(supportedNodeOsDistros ...string) {
	if !framework.NodeOSDistroIs(supportedNodeOsDistros...) {
		skipInternalf(1, "Only supported for node OS distro %v (not %s)", supportedNodeOsDistros, framework.TestContext.NodeOSDistro)
	}
}

// node OS 发行版不在 supportedNodeOsArchs 中时 SkipUnlessNodeOSArchIs 跳过测试。
func SkipUnlessNodeOSArchIs(supportedNodeOsArchs ...string) {
	if !framework.NodeOSArchIs(supportedNodeOsArchs...) {
		skipInternalf(1, "Only supported for node OS arch %v (not %s)", supportedNodeOsArchs, framework.TestContext.NodeOSArch)
	}
}

// node OS 发行版在 unsupportedNodeOsDistros 中时 SkipIfNodeOSDistroIs 跳过测试。
func SkipIfNodeOSDistroIs(unsupportedNodeOsDistros ...string) {
	if framework.NodeOSDistroIs(unsupportedNodeOsDistros...) {
		skipInternalf(1, "Not supported for node OS distro %v (is %s)", unsupportedNodeOsDistros, framework.TestContext.NodeOSDistro)
	}
}

// server 版本低于 v 时 SkipUnlessServerVersionGTE 跳过测试。
func SkipUnlessServerVersionGTE(v *utilversion.Version, c discovery.ServerVersionInterface) {
	gte, err := serverVersionGTE(v, c)
	if err != nil {
		framework.Failf("Failed to get server version: %v", err)
	}
	if !gte {
		skipInternalf(1, "Not supported for server versions before %q", v)
	}
}

// 找不到 SSH key 时 SkipUnlessSSHKeyPresent 跳过测试。
func SkipUnlessSSHKeyPresent() {
	if _, err := e2essh.GetSigner(framework.TestContext.Provider); err != nil {
		skipInternalf(1, "No SSH Key for provider %s: '%v'", framework.TestContext.Provider, err)
	}
}

// serverVersionGTE 在 v 大于或等于 server 版本时返回 true。
func serverVersionGTE(v *utilversion.Version, c discovery.ServerVersionInterface) (bool, error) {
	serverVersion, err := c.ServerVersion()
	if err != nil {
		return false, fmt.Errorf("Unable to get server version: %w", err)
	}
	sv, err := utilversion.ParseSemantic(serverVersion.GitVersion)
	if err != nil {
		return false, fmt.Errorf("Unable to parse server version %q: %w", serverVersion.GitVersion, err)
	}
	return sv.AtLeast(v), nil
}

// AppArmorDistros 是支持 AppArmor 的发行版
var AppArmorDistros = []string{"gci", "ubuntu"}

// node OS 发行版不支持 AppArmor 时 SkipIfAppArmorNotSupported 跳过测试。
func SkipIfAppArmorNotSupported() {
	SkipUnlessNodeOSDistroIs(AppArmorDistros...)
}

// 组件以 Pod 运行且 client 能删除它们时，SkipUnlessComponentRunsAsPodsAndClientCanDeleteThem 运行
func SkipUnlessComponentRunsAsPodsAndClientCanDeleteThem(ctx context.Context, componentName string, c clientset.Interface, ns string, labelSet labels.Set) {
	// 检查组件是否以 Pod 形式运行
	label := labels.SelectorFromSet(labelSet)
	listOpts := metav1.ListOptions{LabelSelector: label.String()}
	pods, err := c.CoreV1().Pods(ns).List(ctx, listOpts)
	framework.Logf("SkipUnlessComponentRunsAsPodsAndClientCanDeleteThem: %v, %v", pods, err)
	if err != nil {
		skipInternalf(1, "Skipped because client failed to get component:%s pod err:%v", componentName, err)
	}

	if len(pods.Items) == 0 {
		skipInternalf(1, "Skipped because component:%s is not running as pod.", componentName)
	}

	// 检查 client 是否能删除 Pod
	pod := pods.Items[0]
	if err := c.CoreV1().Pods(ns).Delete(ctx, pod.Name, metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		skipInternalf(1, "Skipped because client failed to delete component:%s pod, err:%v", componentName, err)
	}
}

// 集群 IP family 为 IPv6 且 provider 在 unsupportedProviders 中时 SkipIfIPv6 跳过测试。
func SkipIfIPv6(unsupportedProviders ...string) {
	if framework.TestContext.ClusterIsIPv6() && framework.ProviderIs(unsupportedProviders...) {
		skipInternalf(1, "Not supported for IPv6 clusters and providers %v (found %s)", unsupportedProviders, framework.TestContext.Provider)
	}
}
