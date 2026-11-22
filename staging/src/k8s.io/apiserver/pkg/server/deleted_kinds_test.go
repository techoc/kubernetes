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

package server

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	apimachineryversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/server/resourceconfig"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/utils/dump"

	"github.com/stretchr/testify/require"
)

// Test_newResourceExpirationEvaluator 验证从版本字符串识别 alpha/beta 通道：
// 只关心 major.minor；alpha.0 与其他 alpha 的严格移除语义不同，需区分。
func Test_newResourceExpirationEvaluator(t *testing.T) {
	tests := []struct {
		name           string
		currentVersion string
		expected       resourceExpirationEvaluator
		expectedErr    string
	}{
		{
			name:           "beta",
			currentVersion: "v1.20.0-beta.0.62+a5d22854a2ac21",
			expected:       resourceExpirationEvaluator{currentVersion: apimachineryversion.MajorMinor(1, 20)},
		},
		{
			name:           "alpha .0",
			currentVersion: "v1.20.0-alpha.0.62+a5d22854a2ac21",
			expected:       resourceExpirationEvaluator{currentVersion: apimachineryversion.MajorMinor(1, 20), isAlpha: true, isAlphaZero: true},
		},
		{
			name:           "alpha not .0",
			currentVersion: "v1.20.0-alpha.1.62+a5d22854a2ac21",
			expected:       resourceExpirationEvaluator{currentVersion: apimachineryversion.MajorMinor(1, 20), isAlpha: true, isAlphaZero: false},
		},

		{
			name:           "maintenance",
			currentVersion: "v1.20.1",
			expected:       resourceExpirationEvaluator{currentVersion: apimachineryversion.MajorMinor(1, 20)},
		},
		{
			name:           "no v prefix",
			currentVersion: "1.20.1",
			expected:       resourceExpirationEvaluator{currentVersion: apimachineryversion.MajorMinor(1, 20)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, actualErr := NewResourceExpirationEvaluator(apimachineryversion.MustParse(tt.currentVersion))

			checkErr(t, actualErr, tt.expectedErr)
			if actualErr != nil {
				return
			}

			actual.(*resourceExpirationEvaluator).strictRemovedHandlingInAlpha = false
			if !reflect.DeepEqual(tt.expected, *actual.(*resourceExpirationEvaluator)) {
				t.Fatal(actual)
			}
		})
	}
}

// defaultObj 没有生命周期元数据；测试中不应调用其 runtime.Object 方法，
// 故直接 panic 以暴露误用。
type defaultObj struct {
}

func (r *defaultObj) GetObjectKind() schema.ObjectKind {
	panic("don't do this")
}
func (r *defaultObj) DeepCopyObject() runtime.Object {
	panic("don't do this either")
}

// removedInObj 只声明移除版本，用于测试“已移除”判定。
type removedInObj struct {
	major, minor int
}

func (r *removedInObj) GetObjectKind() schema.ObjectKind {
	panic("don't do this")
}
func (r *removedInObj) DeepCopyObject() runtime.Object {
	panic("don't do this either")
}
func (r *removedInObj) APILifecycleRemoved() (major, minor int) {
	return r.major, r.minor
}

// IntroducedInObj 只声明引入版本，用于测试“尚未引入”判定。
type IntroducedInObj struct {
	major, minor int
}

func (r *IntroducedInObj) GetObjectKind() schema.ObjectKind {
	panic("don't do this")
}
func (r *IntroducedInObj) DeepCopyObject() runtime.Object {
	panic("don't do this either")
}
func (r *IntroducedInObj) APILifecycleIntroduced() (major, minor int) {
	return r.major, r.minor
}

// introducedAndRemovedInObj 同时声明引入与移除版本，用于验证区间判定。
type introducedAndRemovedInObj struct {
	majorIntroduced, minorIntroduced int
	majorRemoved, minorRemoved       int
}

func (r *introducedAndRemovedInObj) GetObjectKind() schema.ObjectKind {
	panic("don't do this")
}
func (r *introducedAndRemovedInObj) DeepCopyObject() runtime.Object {
	panic("don't do this either")
}
func (r *introducedAndRemovedInObj) APILifecycleIntroduced() (major, minor int) {
	return r.majorIntroduced, r.minorIntroduced
}
func (r *introducedAndRemovedInObj) APILifecycleRemoved() (major, minor int) {
	return r.majorRemoved, r.minorRemoved
}

// 以下构造器简化表驱动用例中的 storage 声明，分别表达“已移除”“从未移除”
// “某版本引入”“引入且移除”四种生命周期。
func storageRemovedIn(major, minor int) *introducedAndRemovedInStorage {
	return &introducedAndRemovedInStorage{majorRemoved: major, minorRemoved: minor}
}

func storageNeverRemoved() *introducedAndRemovedInStorage {
	return &introducedAndRemovedInStorage{}
}

func storageIntroducedIn(major, minor int) *introducedAndRemovedInStorage {
	return &introducedAndRemovedInStorage{majorIntroduced: major, minorIntroduced: minor}
}

func storageIntroducedAndRemovedIn(majorIntroduced, minorIntroduced, majorRemoved, minorRemoved int) *introducedAndRemovedInStorage {
	return &introducedAndRemovedInStorage{majorIntroduced: majorIntroduced, minorIntroduced: minorIntroduced, majorRemoved: majorRemoved, minorRemoved: minorRemoved}
}

// introducedAndRemovedInStorage 是 rest.Storage 测试替身，New() 根据设置的
// 版本字段返回不同生命周期对象，从而复用同一套 storage map。
type introducedAndRemovedInStorage struct {
	majorIntroduced, minorIntroduced int
	majorRemoved, minorRemoved       int
}

// New 按字段组合返回对应的生命周期测试对象；全部为零表示无生命周期信息。
func (r *introducedAndRemovedInStorage) New() runtime.Object {
	if r.majorIntroduced == 0 && r.minorIntroduced == 0 && r.majorRemoved == 0 && r.minorRemoved == 0 {
		return &defaultObj{}
	}
	if r.majorIntroduced == 0 && r.minorIntroduced == 0 {
		return &removedInObj{major: r.majorRemoved, minor: r.minorRemoved}
	}
	if r.majorRemoved == 0 && r.minorRemoved == 0 {
		return &IntroducedInObj{major: r.majorIntroduced, minor: r.minorIntroduced}
	}
	return &introducedAndRemovedInObj{majorIntroduced: r.majorIntroduced, minorIntroduced: r.minorIntroduced, majorRemoved: r.majorRemoved, minorRemoved: r.minorRemoved}
}

func (r *introducedAndRemovedInStorage) Destroy() {}

// Test_resourceExpirationEvaluator_isNotRemoved 验证“是否仍应提供”判定：
// 当前版本已移除即 false，但延迟一个版本、alpha 通道（非严格）等兼容开关
// 会让它继续存在；未来才移除或从未移除则为 true。
func Test_resourceExpirationEvaluator_isNotRemoved(t *testing.T) {
	tests := []struct {
		name                        string
		resourceExpirationEvaluator resourceExpirationEvaluator
		restStorage                 rest.Storage
		expected                    bool
	}{
		{
			name: "removed-in-curr",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			restStorage: storageRemovedIn(1, 20),
			expected:    false,
		},
		{
			name: "removed-in-curr-but-deferred",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                 apimachineryversion.MajorMinor(1, 20),
				serveRemovedAPIsOneMoreRelease: true,
			},
			restStorage: storageRemovedIn(1, 20),
			expected:    true,
		},
		{
			name: "removed-in-curr-but-alpha",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
				isAlpha:        true,
			},
			restStorage: storageRemovedIn(1, 20),
			expected:    true,
		},
		{
			name: "removed-in-curr-but-alpha-but-strict",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:               apimachineryversion.MajorMinor(1, 20),
				isAlpha:                      true,
				strictRemovedHandlingInAlpha: true,
			},
			restStorage: storageRemovedIn(1, 20),
			expected:    false,
		},
		{
			name: "removed-in-curr-but-alpha-but-strict-and-alpha-zero",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:               apimachineryversion.MajorMinor(1, 20),
				isAlpha:                      true,
				isAlphaZero:                  true,
				strictRemovedHandlingInAlpha: true,
			},
			restStorage: storageRemovedIn(1, 20),
			expected:    true,
		},
		{
			name: "removed-in-prev-deferral-does-not-help",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                 apimachineryversion.MajorMinor(1, 21),
				serveRemovedAPIsOneMoreRelease: true,
			},
			restStorage: storageRemovedIn(1, 20),
			expected:    false,
		},
		{
			name: "removed-in-prev-major",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                 apimachineryversion.MajorMinor(2, 20),
				serveRemovedAPIsOneMoreRelease: true,
			},
			restStorage: storageRemovedIn(1, 20),
			expected:    false,
		},
		{
			name: "removed-in-future",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			restStorage: storageRemovedIn(1, 21),
			expected:    true,
		},
		{
			name: "never-removed",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			restStorage: storageNeverRemoved(),
			expected:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gv := schema.GroupVersion{Group: "mygroup", Version: "myversion"}
			convertor := &dummyConvertor{prioritizedVersions: []schema.GroupVersion{gv}}
			if actual := tt.resourceExpirationEvaluator.isNotRemoved(gv, convertor, tt.restStorage); actual != tt.expected {
				t.Errorf("isRemoved() = %v, want %v", actual, tt.expected)
			}
			if !reflect.DeepEqual(convertor.called, gv) {
				t.Errorf("expected converter to be called with %#v, got %#v", gv, convertor.called)
			}
		})
	}
}

// dummyConvertor 记录最近一次 ConvertToVersion 的 GroupVersioner，并返回
// 固定版本列表，用于验证转换器被以正确版本调用。
type dummyConvertor struct {
	called              runtime.GroupVersioner
	prioritizedVersions []schema.GroupVersion
}

func (d *dummyConvertor) ConvertToVersion(in runtime.Object, gv runtime.GroupVersioner) (runtime.Object, error) {
	d.called = gv
	return in, nil
}

func (d *dummyConvertor) PrioritizedVersionsForGroup(group string) []schema.GroupVersion {
	return d.prioritizedVersions
}

func (d *dummyConvertor) IsGroupRegistered(group string) bool {
	return true
}

func (d *dummyConvertor) IsVersionRegistered(v schema.GroupVersion) bool {
	return true
}

func (d *dummyConvertor) PrioritizedVersionsAllGroups() []schema.GroupVersion {
	return d.prioritizedVersions
}

// checkErr 断言错误包含（或不包含）期望文本，简化表驱动测试中的错误检查。
func checkErr(t *testing.T, actual error, expected string) {
	t.Helper()
	switch {
	case len(expected) == 0 && actual == nil:
	case len(expected) == 0 && actual != nil:
		t.Fatal(actual)
	case len(expected) != 0 && actual == nil:
		t.Fatalf("missing %q, <nil>", expected)
	case len(expected) != 0 && actual != nil && !strings.Contains(actual.Error(), expected):
		t.Fatalf("missing %q, %v", expected, actual)
	}
}

// Test_removeDeletedKinds 验证删除过期资源：同一版本内按资源名删除，
// 其子资源（prefix/...）一并删除；整版资源都被移除时该版本整体消失。
func Test_removeDeletedKinds(t *testing.T) {
	groupName := "group.name"
	tests := []struct {
		name                         string
		resourceExpirationEvaluator  resourceExpirationEvaluator
		versionedResourcesStorageMap map[string]map[string]rest.Storage
		expectedStorage              map[string]map[string]rest.Storage
	}{
		{
			name: "remove-one-of-two",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					"twenty":    storageRemovedIn(1, 20),
					"twentyone": storageRemovedIn(1, 21),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					"twentyone": storageRemovedIn(1, 21),
				},
			},
		},
		{
			name: "remove-nested-not-expired",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					"twenty":       storageRemovedIn(1, 20),
					"twenty/scale": storageRemovedIn(1, 21),
					"twentyone":    storageRemovedIn(1, 21),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					"twentyone": storageRemovedIn(1, 21),
				},
			},
		},
		{
			name: "remove-all-of-version",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					"twenty": storageRemovedIn(1, 20),
				},
				"v2": {
					"twenty":    storageRemovedIn(1, 20),
					"twentyone": storageRemovedIn(1, 21),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v2": {
					"twentyone": storageRemovedIn(1, 21),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			convertor := &dummyConvertor{prioritizedVersions: []schema.GroupVersion{
				{Group: groupName, Version: "v2"}, {Group: groupName, Version: "v1"}}}
			tt.resourceExpirationEvaluator.removeDeletedKinds(groupName, convertor, tt.versionedResourcesStorageMap)
			if !reflect.DeepEqual(tt.expectedStorage, tt.versionedResourcesStorageMap) {
				t.Fatal(dump.Pretty(tt.versionedResourcesStorageMap))
			}
		})
	}
}

// Test_removeUnIntroducedKinds 验证尚未引入的资源/版本被剔除：未到引入
// 版本的 API 不能提前暴露；emulation/runtime-config forward-compatible
// 开关的优先级与报错规则也在本测试中固定下来。
func Test_removeUnIntroducedKinds(t *testing.T) {
	groupName := "group.name"
	resource1 := "resource1"
	resource2 := "resource2"
	tests := []struct {
		name                         string
		resourceExpirationEvaluator  resourceExpirationEvaluator
		runtimeConfig                map[string]string
		expectErr                    bool
		versionedResourcesStorageMap map[string]map[string]rest.Storage
		expectedStorage              map[string]map[string]rest.Storage
	}{
		{
			name: "remove-future-version",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 21),
			},
			runtimeConfig: map[string]string{
				"api/beta":             "true",
				groupName + "/v2beta1": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
			},
		},
		{
			name: "missing-introduced-version",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageRemovedIn(1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageRemovedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-ga-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 19),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-alpha-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-beta1-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 21),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-new-resource",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 22),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "runtime-config-enable-future-version-err",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			expectErr: true,
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "runtime-config-enable-future-resource-err",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2/resource2": "true",
			},
			expectErr: true,
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "runtime-config-emulation-forward-compatible-beta2-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
			},
		},
		{
			name: "runtime-config-emulation-forward-compatible-beta2-resource",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2/resource2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta2": {
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-runtime-config-beta2-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectErr: true,
		},
		{
			name: "both-runtime-config-and-emulation-forward-compatible-runtime-config-beta2-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible:              true,
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "both-runtime-config-and-emulation-forward-compatible-runtime-config-alpha-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 19),
				runtimeConfigEmulationForwardCompatible: true,
				emulationForwardCompatible:              true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2alpha1": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 19),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
					resource2: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 19),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
					resource2: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "both-runtime-config-and-emulation-forward-compatible-runtime-config-alpha-resource",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 19),
				runtimeConfigEmulationForwardCompatible: true,
				emulationForwardCompatible:              true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2alpha1/resource2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 19),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
					resource2: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 19),
				},
				"v2alpha1": {
					resource2: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-beta1-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 21),
				emulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta1": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-beta1-resource",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 21),
				emulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta1/resource2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "both-runtime-config-and-emulation-forward-compatible-runtime-config-beta1-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible:              true,
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta1": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "both-runtime-config-and-emulation-forward-compatible-runtime-config-beta1-resource",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible:              true,
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta1/resource2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource2: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resourceConfig := serverstorage.NewResourceConfig()
			convertor := &dummyConvertor{prioritizedVersions: []schema.GroupVersion{
				{Group: groupName, Version: "v2"}, {Group: groupName, Version: "v1"},
				{Group: groupName, Version: "v2beta1"},
				{Group: groupName, Version: "v2beta2"},
				{Group: groupName, Version: "v2alpha1"}}}
			// 先启用所有版本，再合并 --runtime-config，确保测试覆盖“显式启用
			// 未来版本”的错误分支。
			resourceConfig.EnableVersions(convertor.PrioritizedVersionsForGroup(groupName)...)
			resourceConfig, err := resourceconfig.MergeAPIResourceConfigs(resourceConfig, tt.runtimeConfig, convertor)
			require.NoError(t, err)
			err = tt.resourceExpirationEvaluator.removeUnintroducedKinds(groupName, convertor, tt.versionedResourcesStorageMap, resourceConfig)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if !reflect.DeepEqual(tt.expectedStorage, tt.versionedResourcesStorageMap) {
				t.Fatal(dump.Pretty(tt.versionedResourcesStorageMap))
			}
		})
	}
}

// Test_RemoveUnavailableKinds 是 removeUnintroducedKinds 的对外入口版本，
// 同时处理“已移除”与“未引入”两类不可用资源；用例与内部函数保持一致，
// 用于防止两条路径行为漂移。
func Test_RemoveUnavailableKinds(t *testing.T) {
	groupName := "group.name"
	resource1 := "resource1"
	resource2 := "resource2"
	tests := []struct {
		name                         string
		resourceExpirationEvaluator  resourceExpirationEvaluator
		runtimeConfig                map[string]string
		expectErr                    bool
		versionedResourcesStorageMap map[string]map[string]rest.Storage
		expectedStorage              map[string]map[string]rest.Storage
	}{
		{
			name: "remove-future-version",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 21),
			},
			runtimeConfig: map[string]string{
				"api/beta":             "true",
				groupName + "/v2beta1": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
			},
		},
		{
			name: "missing-introduced-version",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageRemovedIn(1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageRemovedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-ga-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 19),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-alpha-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 18),
				},
				"v2alpha1": {
					resource1: storageIntroducedAndRemovedIn(1, 20, 1, 21),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-beta1-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 21),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-new-resource",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 22),
				emulationForwardCompatible: true,
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "runtime-config-enable-future-version-err",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			expectErr: true,
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "runtime-config-enable-future-resource-err",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion: apimachineryversion.MajorMinor(1, 20),
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2/resource2": "true",
			},
			expectErr: true,
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "runtime-config-emulation-forward-compatible-beta2-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
			},
		},
		{
			name: "runtime-config-emulation-forward-compatible-beta2-resource",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2/resource2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta2": {
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-runtime-config-beta2-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectErr: true,
		},
		{
			name: "both-runtime-config-and-emulation-forward-compatible-runtime-config-beta2-api",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:                          apimachineryversion.MajorMinor(1, 20),
				emulationForwardCompatible:              true,
				runtimeConfigEmulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
		},
		{
			name: "emulation-forward-compatible-runtime-config-beta2-api-resource1-ok",
			resourceExpirationEvaluator: resourceExpirationEvaluator{
				currentVersion:             apimachineryversion.MajorMinor(1, 21),
				emulationForwardCompatible: true,
			},
			runtimeConfig: map[string]string{
				groupName + "/v2beta2": "true",
			},
			versionedResourcesStorageMap: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
					resource2: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
					resource2: storageIntroducedIn(1, 23),
				},
			},
			expectedStorage: map[string]map[string]rest.Storage{
				"v1": {
					resource1: storageIntroducedIn(1, 20),
				},
				"v2beta1": {
					resource1: storageIntroducedAndRemovedIn(1, 21, 1, 22),
				},
				"v2beta2": {
					resource1: storageIntroducedAndRemovedIn(1, 22, 1, 23),
				},
				"v2": {
					resource1: storageIntroducedIn(1, 23),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resourceConfig := serverstorage.NewResourceConfig()
			convertor := &dummyConvertor{prioritizedVersions: []schema.GroupVersion{
				{Group: groupName, Version: "v2"}, {Group: groupName, Version: "v1"},
				{Group: groupName, Version: "v2beta2"}, {Group: groupName, Version: "v2beta1"},
				{Group: groupName, Version: "v2alpha1"}}}
			resourceConfig.EnableVersions(convertor.PrioritizedVersionsForGroup(groupName)...)
			resourceConfig, err := resourceconfig.MergeAPIResourceConfigs(resourceConfig, tt.runtimeConfig, convertor)
			require.NoError(t, err)
			err = tt.resourceExpirationEvaluator.RemoveUnavailableKinds(groupName, convertor, tt.versionedResourcesStorageMap, resourceConfig)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if !reflect.DeepEqual(tt.expectedStorage, tt.versionedResourcesStorageMap) {
				t.Fatal(dump.Pretty(tt.versionedResourcesStorageMap))
			}
		})
	}
}

// Test_shouldRemoveResource 验证子资源随父资源删除：匹配规则按
// “精确或前缀+斜杠”判断，避免误删名字仅共享前缀的兄弟资源。
func Test_shouldRemoveResource(t *testing.T) {
	tests := []struct {
		name              string
		resourcesToRemove sets.String
		resourceName      string
		want              bool
	}{
		{
			name:              "prefix-matches",
			resourcesToRemove: sets.NewString("foo"),
			resourceName:      "foo/scale",
			want:              true,
		},
		{
			name:              "exact-matches",
			resourcesToRemove: sets.NewString("foo"),
			resourceName:      "foo",
			want:              true,
		},
		{
			name:              "no-match",
			resourcesToRemove: sets.NewString("foo"),
			resourceName:      "bar",
			want:              false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := shouldRemoveResourceAndSubresources(tt.resourcesToRemove, tt.resourceName); actual != tt.want {
				t.Errorf("shouldRemoveResourceAndSubresources() = %v, want %v", actual, tt.want)
			}
		})
	}
}
