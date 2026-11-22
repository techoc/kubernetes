/*
Copyright 2024 The Kubernetes Authors.

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

// Package testing 提供在单元测试中临时覆盖 client-go 特性门控的辅助函数。
//
// 覆盖只对当前测试（及其子测试）生效，测试结束时由 tb.Cleanup 自动恢复原值，
// 因此测试之间不会互相污染。
package testing

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	clientfeatures "k8s.io/client-go/features"
)

// overriddenFeaturesLock 保护 overriddenFeatures；测试可能并行运行，登记与检查必须串行化。
// overriddenFeatures 记录每个特性当前被哪个测试覆盖，用于检测两个并行测试同时改写
// 同一门控造成的不可预期行为。
var (
	overriddenFeaturesLock sync.Mutex
	overriddenFeatures     map[clientfeatures.Feature]string
)

func init() {
	overriddenFeatures = map[clientfeatures.Feature]string{}
}

// featureGatesSetter 是测试侧需要的最小门控接口：能读、能改。
type featureGatesSetter interface {
	clientfeatures.Gates

	Set(clientfeatures.Feature, bool) error
}

// featureGatesSetterForTesting 针对生产门控实现额外提供 SetForTesting，
// 以便在非测试环境下拒绝改写（生产门控不允许运行时变更）。
type featureGatesSetterForTesting interface {
	featureGatesSetter

	SetForTesting(clientfeatures.Feature, bool) error
}

// SetFeatureDuringTest sets the specified feature to the specified value for the duration of the test.
//
// Example use:
//
//	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
//
// 若当前门控实现不支持被测试覆盖，直接 Fatal 终止测试，避免出现“看似生效、实则未改”的假象。
func SetFeatureDuringTest(tb testing.TB, feature clientfeatures.Feature, featureValue bool) {
	if err := setFeatureDuringTestInternal(tb, feature, featureValue); err != nil {
		tb.Fatal(err)
	}
}

// setFeatureDuringTestInternal 完成覆盖登记、值修改以及 Cleanup 中的恢复。
func setFeatureDuringTestInternal(tb testing.TB, feature clientfeatures.Feature, featureValue bool) error {
	// 持有锁直到登记完成，保证“检查是否已被他人覆盖”与“写入覆盖记录”是原子的。
	overriddenFeaturesLock.Lock()
	defer overriddenFeaturesLock.Unlock()

	currentFeatureGates := clientfeatures.FeatureGates()
	featureGates, ok := currentFeatureGates.(featureGatesSetter)
	if !ok {
		panic(fmt.Errorf("clientfeatures.FeatureGates(): %T does not implement featureGatesSetter interface", currentFeatureGates))
	}

	originalFeatureValue := featureGates.Enabled(feature)
	// 同一门控只允许一个测试（或它的子测试）覆盖；否则并行测试会互相看到对方的值。
	if overridingTestName, ok := overriddenFeatures[feature]; ok {
		if !sameTestOrSubtest(tb, overridingTestName) {
			return fmt.Errorf("client-go feature %q is currently overridden by %q test and cannot be also modified by %q", feature, overridingTestName, tb.Name())
		}
	}

	// 优先走 SetForTesting：生产实现据此区分测试与生产调用。
	setFeature := func(feature clientfeatures.Feature, featureValue bool) error {
		if testSetter, ok := featureGates.(featureGatesSetterForTesting); ok {
			return testSetter.SetForTesting(feature, featureValue)
		}
		return featureGates.Set(feature, featureValue)
	}

	if err := setFeature(feature, featureValue); err != nil {
		return err
	}

	overriddenFeatures[feature] = tb.Name()

	// 恢复动作必须放在 Cleanup 中，才能覆盖 t.FailNow / panic 等提前退出路径。
	tb.Cleanup(func() {
		overriddenFeaturesLock.Lock()
		defer overriddenFeaturesLock.Unlock()
		delete(overriddenFeatures, feature)
		// if default is not set
		if err := setFeature(feature, originalFeatureValue); err != nil {
			tb.Errorf("failed restoring client-go feature: %v to its original value: %v, err: %v", feature, originalFeatureValue, err)
		}
	})
	return nil
}

// copied from component-base/featuregate/testing
// sameTestOrSubtest 判断 testName 是否为 tb 本身或其父级子测试名，
// 从而允许同一测试树的父子测试共享覆盖。
func sameTestOrSubtest(tb testing.TB, testName string) bool {
	return tb.Name() == testName || strings.HasPrefix(tb.Name(), testName+"/")
}
