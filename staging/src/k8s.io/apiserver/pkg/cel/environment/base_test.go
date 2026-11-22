/*
Copyright 2023 The Kubernetes Authors.

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

package environment

import (
	"sort"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/library"
)

// BenchmarkLoadBaseEnv is expected to be very fast, because a
// a cached environment is loaded for each MustBaseEnvSet call.
//
// BenchmarkLoadBaseEnv 验证同一兼容版本的 BaseEnv 走缓存路径，单次获取
// 应只有很小的固定开销。
func BenchmarkLoadBaseEnv(b *testing.B) {
	ver := DefaultCompatibilityVersion()
	MustBaseEnvSet(ver)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MustBaseEnvSet(ver)
	}
}

// BenchmarkLoadBaseEnvDifferentVersions is expected to be relatively slow, because a
// a new environment must be created for each MustBaseEnvSet call.
//
// BenchmarkLoadBaseEnvDifferentVersions 每次使用不同版本，强制走完整构建
// 路径；与缓存路径对比可发现意外的缓存失效。
func BenchmarkLoadBaseEnvDifferentVersions(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MustBaseEnvSet(version.MajorMinor(1, uint(i)))
	}
}

// TestLibraryCoverage lints the management of libraries in baseOpts by
// checking for:
//
//   - No gaps and overlap in library inclusion, including when libraries are version bumped
//   - RemovedVersion is always greater than IntroducedVersion
//   - Libraries are not removed once added (although they can be replaced with new versions)
//
// 中文说明：该测试对 baseOpts 做静态校验，按 IntroducedVersion 排序后
// 逐版本跟踪每个库的存在区间，确保同一版本的库不会重复引入或出现重叠，
// 新版本库必须在旧版本移除的同一 Kubernetes 版本引入（否则会留下空档），
// 且任何库都不能在未被替代的情况下被删除——这是保持 CEL 表达式向后
// 兼容的关键约束。
func TestLibraryCoverage(t *testing.T) {
	vops := make([]VersionedOptions, len(baseOpts))
	copy(vops, baseOpts)
	sort.SliceStable(vops, func(i, j int) bool {
		return vops[i].IntroducedVersion.LessThan(vops[j].IntroducedVersion)
	})

	tracked := map[string]*versionTracker{}

	for _, vop := range vops {
		// 有移除版本时必须同时声明引入版本，且引入早于移除。
		if vop.RemovedVersion != nil {
			if vop.IntroducedVersion == nil {
				t.Errorf("VersionedOptions with RemovedVersion %v is missing required IntroducedVersion", vop.RemovedVersion)
			}
			if !vop.IntroducedVersion.LessThan(vop.RemovedVersion) {
				t.Errorf("VersionedOptions with IntroducedVersion %s must be less than RemovedVersion %v", vop.IntroducedVersion, vop.RemovedVersion)
			}
		}

		for _, name := range librariesInVersions(t, vop) {
			versionTracking, ok := tracked[name]
			if !ok {
				versionTracking = &versionTracker{}
				tracked[name] = versionTracking
			}
			// 同一个库在同一版本区间内只能引入一次。
			if versionTracking.added != nil {
				t.Errorf("Did not expect %s library to be added again at version %v. It was already added at version %v", name, vop.IntroducedVersion, versionTracking.added)
			} else {
				versionTracking.added = vop.IntroducedVersion
				// 若此前记录过移除版本，检查新旧版本衔接是否无缝。
				if versionTracking.removed != nil {
					if versionTracking.removed.LessThan(vop.IntroducedVersion) {
						t.Errorf("Did not expect gap in presence of %s library. It was "+
							"removed in %v and not added again until %v. When versioning "+
							"libraries, introduce a new version of the library as the same "+
							"kubernetes version that the old version of the library is removed.", name, versionTracking.removed, vop.IntroducedVersion)
					} else if vop.IntroducedVersion.LessThan(versionTracking.removed) {
						t.Errorf("Did not expect overlap in presence of %s library. It was "+
							"added again at version %v while scheduled to be removed at %v. When versioning "+
							"libraries, introduce a new version of the library as the same "+
							"kubernetes version that the old version of the library is removed.", name, vop.IntroducedVersion, versionTracking.removed)
					}
				}
				versionTracking.removed = nil
			}
			// 记录本次移除；重复移除同一库属于配置错误。
			if vop.RemovedVersion != nil {
				if versionTracking.removed != nil {
					t.Errorf("Unexpected RemovedVersion of %v for library %s already removed at version %v", vop.RemovedVersion, name, versionTracking.removed)
				}
				versionTracking.added = nil
				versionTracking.removed = vop.RemovedVersion
			}
		}
	}
	// 任一库若以"移除"状态收尾且没有新版替代，说明破坏了兼容性。
	for name, lib := range tracked {
		if lib.removed != nil {
			t.Errorf("Unexpected RemovedVersion of %v for library %s without replacement. "+
				"For backward compatibility, libraries should not be removed without being replaced by a new version.", lib.removed, name)
		}
	}
}

// TestKnownLibraries ensures that all libraries used in the base environment are also registered with
// KnownLibraries.  Other tests rely on KnownLibraries to provide an up-to-date list of CEL libraries.
//
// TestKnownLibraries 校验 base 环境实际使用的库都登记在 KnownLibraries 中。
// 其他测试依赖该清单保持最新，否则新增库可能被静默遗漏。
func TestKnownLibraries(t *testing.T) {
	known := sets.New[string]()
	used := sets.New[string]()

	for _, lib := range library.KnownLibraries() {
		known.Insert(lib.LibraryName())
	}
	for _, libName := range MustBaseEnvSet(version.MajorMinor(1, 0)).storedExpressions.Libraries() {
		// cel.lib 开头的是 CEL 运行时内置库，不属于 Kubernetes 登记范围。
		if strings.HasPrefix(libName, "cel.lib") { // ignore core libs
			continue
		}
		used.Insert(libName)
	}

	unexpected := used.Difference(known)

	if len(unexpected) != 0 {
		t.Errorf("Expected all libraries in the base environment to be included in k8s.io/apiserver/pkg/cel/library's KnownLibraries, but found missing libraries: %v", unexpected)
	}
}

// librariesInVersions 依次叠加给定 VersionedOptions 的环境选项，返回最终
// 环境中存在的库列表，用于推断某版本区间会引入哪些库。
func librariesInVersions(t *testing.T, vops ...VersionedOptions) []string {
	env, err := cel.NewCustomEnv()
	if err != nil {
		t.Fatalf("Error creating env: %v", err)
	}
	for _, vop := range vops {
		env, err = env.Extend(vop.EnvOptions...)
		if err != nil {
			t.Fatalf("Error updating env: %v", err)
		}
	}
	libs := env.Libraries()
	return libs
}

// versionTracker 记录某个库最近一次被引入与移除的版本，nil 表示当前处于
// 相反状态。
type versionTracker struct {
	added   *version.Version
	removed *version.Version
}
