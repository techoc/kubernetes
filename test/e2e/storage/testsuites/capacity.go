/*
Copyright 2021 The Kubernetes Authors.

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

package testsuites

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/types"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

type capacityTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitCustomCapacityTestSuite 返回使用自定义测试 pattern、实现了 TestSuite 接口的 capacityTestSuite。
func InitCustomCapacityTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &capacityTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "capacity",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
		},
	}
}

// InitCapacityTestSuite 返回使用测试套件默认 pattern、实现了 TestSuite 接口的 capacityTestSuite。
func InitCapacityTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DefaultFsDynamicPV,
	}
	return InitCustomCapacityTestSuite(patterns)
}

func (p *capacityTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return p.tsInfo
}

func (p *capacityTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	// 检查前置条件。
	if pattern.VolType != storageframework.DynamicPV {
		return fmt.Sprintf("Suite %q does not support %v", p.tsInfo.Name, pattern.VolType)
	}
	dInfo := driver.GetDriverInfo()
	if !dInfo.Capabilities[storageframework.CapCapacity] {
		return fmt.Sprintf("Driver %s doesn't publish storage capacity", dInfo.Name)
	}
	return ""
}

func (p *capacityTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	var (
		dInfo   = driver.GetDriverInfo()
		dDriver storageframework.DynamicPVTestDriver
		sc      *storagev1.StorageClass
	)

	// 注意，它还会注册一个 AfterEach，使 f 变得不可用。
	// 任何使用 f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("capacity", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		dDriver, _ = driver.(storageframework.DynamicPVTestDriver)
		// 在这里执行开销较大的测试初始化。
		config := driver.PrepareTest(ctx, f)
		sc = dDriver.GetDynamicProvisionStorageClass(ctx, config, pattern.FsType)
		if sc == nil {
			e2eskipper.Skipf("Driver %q does not define Dynamic Provision StorageClass - skipping", dInfo.Name)
		}
	}

	ginkgo.It("provides storage capacity information", func(ctx context.Context) {
		init(ctx)

		timeout := time.Minute
		pollInterval := time.Second
		matchSC := HaveCapacitiesForClass(sc.Name)
		listAll := gomega.Eventually(ctx, func() (*storagev1.CSIStorageCapacityList, error) {
			return f.ClientSet.StorageV1().CSIStorageCapacities("").List(ctx, metav1.ListOptions{})
		}, timeout, pollInterval)

		// 如果掌握了 driver 应提供哪些 capacity 信息的更多信息，
		// 就可以做更精确的检查。基线要求是它为 storage class
		// 提供任意容量信息。
		matcher := matchSC
		if len(dInfo.TopologyKeys) == 1 {
			// 收集这个唯一 key 的所有取值即可构造 topology segment，
			// 然后期望该 storage class 的每个取值对应一个 CSIStorageCapacity 对象。
			//
			// 这种检查能覆盖 node 上的本地存储。对有多个 key 的 driver
			// 也可以采用更复杂的方式，但目前尚未实现。
			matcher = HaveCapacitiesForClassAndNodes(ctx, f.ClientSet, sc.Provisioner, sc.Name, dInfo.TopologyKeys[0])
		}

		// 创建 storage class 并等待 capacity 信息。
		sc := SetupStorageClass(ctx, f.ClientSet, sc)
		listAll.Should(MatchCapacities(matcher), "after creating storage class")

		// 再次删除 storage class，并等待 capacity 信息被移除。
		err := f.ClientSet.StorageV1().StorageClasses().Delete(ctx, sc.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "delete storage class")
		listAll.ShouldNot(MatchCapacities(matchSC), "after deleting storage class")
	})
}

func formatCapacities(capacities []storagev1.CSIStorageCapacity) []string {
	lines := []string{}
	for _, capacity := range capacities {
		lines = append(lines, fmt.Sprintf("   %+v", capacity))
	}
	return lines
}

// MatchCapacities 对 *storagev1.CSIStorageCapacityList 执行某种检查。
// 失败时，会把所有实际对象附加到失败消息中。
func MatchCapacities(match types.GomegaMatcher) types.GomegaMatcher {
	return matchCSIStorageCapacities{match: match}
}

type matchCSIStorageCapacities struct {
	match types.GomegaMatcher
}

var _ types.GomegaMatcher = matchCSIStorageCapacities{}

func (m matchCSIStorageCapacities) Match(actual interface{}) (success bool, err error) {
	return m.match.Match(actual)
}

func (m matchCSIStorageCapacities) FailureMessage(actual interface{}) (message string) {
	return m.match.FailureMessage(actual) + m.dump(actual)
}

func (m matchCSIStorageCapacities) NegatedFailureMessage(actual interface{}) (message string) {
	return m.match.NegatedFailureMessage(actual) + m.dump(actual)
}

func (m matchCSIStorageCapacities) dump(actual interface{}) string {
	capacities, ok := actual.(*storagev1.CSIStorageCapacityList)
	if !ok || capacities == nil {
		return ""
	}
	lines := []string{"\n\nall CSIStorageCapacity objects:"}
	for _, capacity := range capacities.Items {
		lines = append(lines, fmt.Sprintf("%+v", capacity))
	}
	return strings.Join(lines, "\n")
}

// CapacityMatcher 用于组合不同的 matcher，其中一个 matcher 可以对
// 另一个已检查过的 CSIStorageCapacity 对象追加检查。
type CapacityMatcher interface {
	types.GomegaMatcher
	// MatchedCapacities 返回上一次 Match 调用中找到的所有 CSICapacityObject。
	MatchedCapacities() []storagev1.CSIStorageCapacity
}

// HaveCapacitiesForClass 按 storage class 过滤 *storagev1.CSIStorageCapacityList 中的
// 所有 storage capacity 对象，至少找到一个即匹配成功。
func HaveCapacitiesForClass(scName string) CapacityMatcher {
	return &haveCSIStorageCapacities{scName: scName}
}

type haveCSIStorageCapacities struct {
	scName             string
	matchingCapacities []storagev1.CSIStorageCapacity
}

var _ CapacityMatcher = &haveCSIStorageCapacities{}

func (h *haveCSIStorageCapacities) Match(actual interface{}) (success bool, err error) {
	capacities, ok := actual.(*storagev1.CSIStorageCapacityList)
	if !ok {
		return false, fmt.Errorf("expected *storagev1.CSIStorageCapacityList, got: %T", actual)
	}
	h.matchingCapacities = nil
	for _, capacity := range capacities.Items {
		if capacity.StorageClassName == h.scName {
			h.matchingCapacities = append(h.matchingCapacities, capacity)
		}
	}
	return len(h.matchingCapacities) > 0, nil
}

func (h *haveCSIStorageCapacities) MatchedCapacities() []storagev1.CSIStorageCapacity {
	return h.matchingCapacities
}

func (h *haveCSIStorageCapacities) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("no CSIStorageCapacity objects for storage class %q", h.scName)
}

func (h *haveCSIStorageCapacities) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("CSIStorageCapacity objects for storage class %q:\n%s",
		h.scName,
		strings.Join(formatCapacities(h.matchingCapacities), "\n"),
	)
}

// HaveCapacitiesForClassAndNodes 按 storage class 名称匹配对象。
// 它会找出 driver 运行的所有 node，并期望每个 node 对应一个对象。
func HaveCapacitiesForClassAndNodes(ctx context.Context, client kubernetes.Interface, driverName, scName, topologyKey string) CapacityMatcher {
	return &haveLocalStorageCapacities{
		ctx:         ctx,
		client:      client,
		driverName:  driverName,
		match:       HaveCapacitiesForClass(scName),
		topologyKey: topologyKey,
	}
}

type haveLocalStorageCapacities struct {
	ctx         context.Context
	client      kubernetes.Interface
	driverName  string
	match       CapacityMatcher
	topologyKey string

	matchSuccess          bool
	expectedCapacities    []storagev1.CSIStorageCapacity
	unexpectedCapacities  []storagev1.CSIStorageCapacity
	missingTopologyValues []string
}

var _ CapacityMatcher = &haveLocalStorageCapacities{}

func (h *haveLocalStorageCapacities) Match(actual interface{}) (success bool, err error) {
	ctx := h.ctx
	h.expectedCapacities = nil
	h.unexpectedCapacities = nil
	h.missingTopologyValues = nil

	// 先用底层 matcher 检查。
	success, err = h.match.Match(actual)
	h.matchSuccess = success
	if !success || err != nil {
		return
	}

	// 找出 driver 运行的所有 node。
	csiNodes, err := h.client.StorageV1().CSINodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	topologyValues := map[string]bool{}
	for _, csiNode := range csiNodes.Items {
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name != h.driverName {
				continue
			}
			node, err := h.client.CoreV1().Nodes().Get(ctx, csiNode.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			value, ok := node.Labels[h.topologyKey]
			if !ok || value == "" {
				return false, fmt.Errorf("driver %q should run on node %q, but its topology label %q was not set",
					h.driverName,
					node.Name,
					h.topologyKey)
			}
			topologyValues[value] = true
			break
		}
	}
	if len(topologyValues) == 0 {
		return false, fmt.Errorf("driver %q not running on any node", h.driverName)
	}

	// 然后检查每个 topology 取值是否恰好对应一个 CSIStorageCapacity 对象。
	remainingTopologyValues := map[string]bool{}
	for value := range topologyValues {
		remainingTopologyValues[value] = true
	}
	capacities := h.match.MatchedCapacities()
	for _, capacity := range capacities {
		if capacity.NodeTopology == nil ||
			len(capacity.NodeTopology.MatchExpressions) > 0 ||
			len(capacity.NodeTopology.MatchLabels) != 1 ||
			!remainingTopologyValues[capacity.NodeTopology.MatchLabels[h.topologyKey]] {
			h.unexpectedCapacities = append(h.unexpectedCapacities, capacity)
			continue
		}
		remainingTopologyValues[capacity.NodeTopology.MatchLabels[h.topologyKey]] = false
		h.expectedCapacities = append(h.expectedCapacities, capacity)
	}

	// 没有多余的 capacity，且期望的 capacity 都齐全时才算成功。
	for value, remaining := range remainingTopologyValues {
		if remaining {
			h.missingTopologyValues = append(h.missingTopologyValues, value)
		}
	}
	return len(h.unexpectedCapacities) == 0 && len(h.missingTopologyValues) == 0, nil
}

func (h *haveLocalStorageCapacities) MatchedCapacities() []storagev1.CSIStorageCapacity {
	return h.match.MatchedCapacities()
}

func (h *haveLocalStorageCapacities) FailureMessage(actual interface{}) (message string) {
	if !h.matchSuccess {
		return h.match.FailureMessage(actual)
	}
	var lines []string
	if len(h.unexpectedCapacities) != 0 {
		lines = append(lines, "unexpected CSIStorageCapacity objects:")
		lines = append(lines, formatCapacities(h.unexpectedCapacities)...)
	}
	if len(h.missingTopologyValues) != 0 {
		lines = append(lines, fmt.Sprintf("no CSIStorageCapacity objects with topology key %q and values %v",
			h.topologyKey, h.missingTopologyValues,
		))
	}
	return strings.Join(lines, "\n")
}

func (h *haveLocalStorageCapacities) NegatedFailureMessage(actual interface{}) (message string) {
	if h.matchSuccess {
		return h.match.NegatedFailureMessage(actual)
	}
	// 取反这个检查是否有用尚不明确，这里直接输出所有已知信息。
	var lines []string
	if len(h.expectedCapacities) != 0 {
		lines = append(lines, "expected CSIStorageCapacity objects:")
		lines = append(lines, formatCapacities(h.expectedCapacities)...)
	}
	if len(h.unexpectedCapacities) != 0 {
		lines = append(lines, "unexpected CSIStorageCapacity objects:")
		lines = append(lines, formatCapacities(h.unexpectedCapacities)...)
	}
	if len(h.missingTopologyValues) != 0 {
		lines = append(lines, fmt.Sprintf("no CSIStorageCapacity objects with topology key %q and values %v",
			h.topologyKey, h.missingTopologyValues,
		))
	}
	return strings.Join(lines, "\n")
}
