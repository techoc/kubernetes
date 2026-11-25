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

package features

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/apimachinery/pkg/util/version"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/component-base/featuregate"
)

// TestClientAdapterEnabled 验证 Enabled 正确转发查询：已注册项返回 true，未知项按底层门控语义 panic。
// 同时固定查询错误的行为，防止适配器把未知功能误判为未启用。
func TestClientAdapterEnabled(t *testing.T) {
	// 用独立的门控实例，避免测试间相互干扰。
	fg := featuregate.NewFeatureGate()
	if err := fg.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		"Foo": {Default: true},
	}); err != nil {
		t.Fatal(err)
	}

	// 适配器直接复用同一个门控实例。
	a := &clientAdapter{fg}
	if !a.Enabled("Foo") {
		t.Error("expected Enabled(\"Foo\") to return true")
	}
	// 未知功能会触发底层门控 panic，这里捕获并断言。
	var r interface{}
	func() {
		defer func() {
			r = recover()
		}()
		a.Enabled("Bar")
	}()
	if r == nil {
		t.Error("expected Enabled(\"Bar\") to panic due to unknown feature name")
	}
}

// TestClientAdapterAdd 验证 Add 把 client-go 的 FeatureSpec（含四种稳定性等级）正确转换并注册。
// 默认值与锁定标记必须原样保留，否则会改变生产环境的功能开关语义。
func TestClientAdapterAdd(t *testing.T) {
	fg := featuregate.NewFeatureGate()
	a := &clientAdapter{fg}
	// 记录注册前的默认项，比对时要排除它们，避免环境预置项干扰。
	defaults := fg.GetAll()
	if err := a.Add(map[clientfeatures.Feature]clientfeatures.FeatureSpec{
		"FeatureAlpha":      {PreRelease: clientfeatures.Alpha, Default: true},
		"FeatureBeta":       {PreRelease: clientfeatures.Beta, Default: false},
		"FeatureGA":         {PreRelease: clientfeatures.GA, Default: true, LockToDefault: true},
		"FeatureDeprecated": {PreRelease: clientfeatures.Deprecated, Default: false, LockToDefault: true},
	}); err != nil {
		t.Fatal(err)
	}
	// 期望值使用 component-base 的常量，与输入一一对应。
	all := fg.GetAll()
	allexpected := map[featuregate.Feature]featuregate.FeatureSpec{
		"FeatureAlpha":      {PreRelease: featuregate.Alpha, Default: true},
		"FeatureBeta":       {PreRelease: featuregate.Beta, Default: false},
		"FeatureGA":         {PreRelease: featuregate.GA, Default: true, LockToDefault: true},
		"FeatureDeprecated": {PreRelease: featuregate.Deprecated, Default: false, LockToDefault: true},
	}
	// 把预置默认项并入期望集合，保证集合完整。
	for name, spec := range defaults {
		allexpected[name] = spec
	}
	if len(all) != len(allexpected) {
		t.Errorf("expected %d registered features, got %d", len(allexpected), len(all))
	}
	for name, expected := range allexpected {
		actual, ok := all[name]
		if !ok {
			t.Errorf("expected feature %q not found", name)
			continue
		}
		// 适配器不设置版本字段，比对时忽略它。
		if diff := cmp.Diff(actual, expected, cmpopts.IgnoreFields(featuregate.FeatureSpec{}, "Version", "MinCompatibilityVersion")); diff != "" {
			t.Errorf("expected feature %q spec %#v, got spec %#v", name, expected, actual)
		}
	}

	// 未知 prerelease 必须在映射阶段 panic，而不是静默注册。
	var r interface{}
	func() {
		defer func() {
			r = recover()
		}()
		_ = a.Add(map[clientfeatures.Feature]clientfeatures.FeatureSpec{
			"FeatureAlpha": {PreRelease: "foobar"},
		})
	}()
	if r == nil {
		t.Error("expected panic when adding feature with unknown prerelease")
	}
}

// TestClientAdapterAddVersioned 验证 AddVersioned 的转换、版本字段保留及未知 prerelease 的 panic 行为。
// 版本边界还决定默认值切换，因此同时检查跨版本解析，避免仅转换字段却选错版本。
func TestClientAdapterAddVersioned(t *testing.T) {
	// 用指定版本的门控，确保版本化注册可用。
	fg := featuregate.NewVersionedFeatureGate(version.MustParse("1.29"))
	a := &clientAdapter{fg}
	// 记录预置项，后续比对时剔除干扰。
	defaults := fg.GetAllVersioned()

	// FeatureB 跨越两个版本：1.28 为 Alpha，1.30 升为 Beta。
	clientFeatures := map[clientfeatures.Feature]clientfeatures.VersionedSpecs{
		"FeatureA": {
			{Version: version.MustParse("1.28"), PreRelease: clientfeatures.Beta, Default: true},
		},
		"FeatureB": {
			{Version: version.MustParse("1.28"), PreRelease: clientfeatures.Alpha, Default: false},
			{Version: version.MustParse("1.30"), PreRelease: clientfeatures.Beta, Default: true},
		},
	}

	if err := a.AddVersioned(clientFeatures); err != nil {
		t.Fatal(err)
	}
	all := fg.GetAllVersioned()
	allexpected := map[featuregate.Feature]featuregate.VersionedSpecs{
		"FeatureA": {
			{Version: version.MustParse("1.28"), PreRelease: featuregate.Beta, Default: true},
		},
		"FeatureB": {
			{Version: version.MustParse("1.28"), PreRelease: featuregate.Alpha, Default: false},
			{Version: version.MustParse("1.30"), PreRelease: featuregate.Beta, Default: true},
		},
	}
	// 合并预置项，构造完整期望集合。
	for name, spec := range defaults {
		allexpected[name] = spec
	}
	if len(all) != len(allexpected) {
		t.Errorf("expected %d registered features, got %d", len(allexpected), len(all))
	}
	for name, expected := range allexpected {
		actual, ok := all[name]
		if !ok {
			t.Errorf("expected feature %q not found", name)
			continue
		}
		// 忽略 Version 字段：适配器原样透传，此处只关心稳定性等级和默认值。
		if diff := cmp.Diff(actual, expected, cmpopts.IgnoreFields(featuregate.FeatureSpec{}, "Version")); diff != "" {
			t.Errorf("feature %q spec mismatch (-want +got):\n%s", name, diff)
		}
	}

	// 同 Add：未知 prerelease 应当直接 panic。
	var r interface{}
	func() {
		defer func() {
			r = recover()
		}()
		_ = a.AddVersioned(map[clientfeatures.Feature]clientfeatures.VersionedSpecs{
			"FeaturePanic": {{PreRelease: "foobar"}},
		})
	}()
	if r == nil {
		t.Error("expected panic when adding feature with unknown prerelease")
	}
	// 注册后按当前版本（1.29）解析：FeatureA 为 Beta 默认 true。
	if !a.Enabled("FeatureA") {
		t.Error("expected Enabled(\"FeatureA\") to return true")
	}
	// FeatureB 在 1.29 仍取 1.28 的 Alpha 默认 false，1.30 才升为 Beta。
	if a.Enabled("FeatureB") {
		t.Error("expected Enabled(\"FeatureB\") to return false")
	}
}
