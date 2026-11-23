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

package test

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/component-base/featuregate"
	"k8s.io/pod-security-admission/api"
	"k8s.io/pod-security-admission/policy"
	"k8s.io/utils/ptr"
)

// minimalValidPods holds minimal valid OS neutral pods per-level per-version.
// To get a valid pod for a particular level/version, use getMinimalValidPod().
// 各版本只保存“刚好达标”的最小 Pod；生成器在此基础上叠加变体，
// 避免每个检查各自维护一份完整 Pod 导致夹具之间悄悄漂移。
var minimalValidPods = map[api.Level]map[api.Version]*corev1.Pod{}

// minimalValidLinuxPods holds minimal valid linux pods per-level per-version.
// To get a valid pod for a particular level/version, use getMinimalValidPod().
// 按操作系统拆分是为了让 Linux 专属要求（如 seccomp）不误伤 Windows 夹具。
var minimalValidLinuxPods = map[api.Level]map[api.Version]*corev1.Pod{}

// minimalValidWindowsPods holds minimal valid Windows pods per-level per-version.
// To get a valid pod for a particular level/version, use getMinimalValidPod().
// 1.25 起受限策略按 OS 分化，许多字段只对 Linux 有意义，Windows 夹具需单独基线。
var minimalValidWindowsPods = map[api.Level]map[api.Version]*corev1.Pod{}

// addLinux 返回显式声明 OS 为 Linux 的副本，不修改入参。
// 显式 OS 能让依赖于 OS 判定的检查稳定命中预期分支，不受默认平台影响。
func addLinux(pod *corev1.Pod) *corev1.Pod {
	copyPod := pod.DeepCopy()
	copyPod.Spec.OS = &corev1.PodOS{Name: corev1.Linux}
	return copyPod
}

// addWindows 返回显式声明 OS 为 Windows 的副本；DeepCopy 避免多个夹具共享同一对象后被相互污染。
func addWindows(pod *corev1.Pod) *corev1.Pod {
	copyPod := pod.DeepCopy()
	copyPod.Spec.OS = &corev1.PodOS{Name: corev1.Windows}
	return copyPod
}

func init() {
	// These are the OS neutral pods
	minimalValidPods[api.LevelBaseline] = map[api.Version]*corev1.Pod{}
	minimalValidPods[api.LevelRestricted] = map[api.Version]*corev1.Pod{}

	minimalValidLinuxPods[api.LevelRestricted] = map[api.Version]*corev1.Pod{}
	minimalValidWindowsPods[api.LevelRestricted] = map[api.Version]*corev1.Pod{}
	// Define minimal valid baseline pod.
	// This must remain valid for all versions.
	// 只放最低限度的 initContainer/container；任何更严格的新要求都由各版本受限夹具增量添加。
	baseline_1_0 := &corev1.Pod{Spec: corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "initcontainer1", Image: "registry.k8s.io/pause"}},
		Containers:     []corev1.Container{{Name: "container1", Image: "registry.k8s.io/pause"}}}}
	minimalValidPods[api.LevelBaseline][api.MajorMinorVersion(1, 0)] = baseline_1_0

	//
	// Define minimal valid restricted pods.
	//

	// 1.0+: baseline + runAsNonRoot=true
	restricted_1_0 := tweak(baseline_1_0, func(p *corev1.Pod) {
		p.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true)}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 0)] = restricted_1_0
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 0)] = addLinux(restricted_1_0)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 0)] = addWindows(restricted_1_0)

	// 1.8+: allowPrivilegeEscalation=false
	restricted_1_8 := tweak(restricted_1_0, func(p *corev1.Pod) {
		p.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false)}
		p.Spec.InitContainers[0].SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false)}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 8)] = restricted_1_8
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 8)] = addLinux(restricted_1_8)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 8)] = addWindows(restricted_1_8)

	// 1.19+: seccompProfile.type=RuntimeDefault
	restricted_1_19 := tweak(restricted_1_8, func(p *corev1.Pod) {
		p.Annotations = nil
		p.Spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 19)] = restricted_1_19
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 19)] = addLinux(restricted_1_19)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 19)] = addWindows(restricted_1_19)

	// 1.22+: capabilities.drop=["ALL"]
	restricted_1_22 := tweak(restricted_1_19, func(p *corev1.Pod) {
		p.Spec.Containers[0].SecurityContext.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}
		p.Spec.InitContainers[0].SecurityContext.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 22)] = restricted_1_22
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 22)] = addLinux(restricted_1_22)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 22)] = addWindows(restricted_1_22)

	// 1.25+: OS specific changes
	// 从此版本起按 OS 分化：OS 中立夹具停留在 1.22 不变，Linux/Windows 各自走专属夹具，
	// 这样新增的 OS 专属限制不会让另一平台的测试无谓失败。
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 25)] = restricted_1_22
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 25)] = addLinux(restricted_1_22)
	// none of the restricted requirements added between 1.0 and 1.25 apply to the pods that are explicitly Windows
	restricted_1_25_windows := addWindows(restricted_1_0)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 25)] = restricted_1_25_windows

}

// GetMinimalValidPod returns a minimal valid OS neutral pod for the specified level and version.
// 指定版本没有夹具时向更低版本回退：策略是累加的，旧版本的最小合法 Pod 对新版本未必合法，
// 但新版本夹具缺失时回退能避免测试因版本矩阵扩容而集体失效。
func GetMinimalValidPod(level api.Level, version api.Version) (*corev1.Pod, error) {
	originalVersion := version
	for {
		pod, exists := minimalValidPods[level][version]
		if exists {
			return pod.DeepCopy(), nil
		}
		if version.Minor() <= 0 {
			return nil, fmt.Errorf("no valid pod fixture found in specified or older versions for %s/%s", level, originalVersion.String())
		}
		version = api.MajorMinorVersion(version.Major(), version.Minor()-1)
	}
}

// GetMinimalValidLinuxPod returns a minimal valid linux pod for the specified level and version.
// 与 OS 中立版本一样按版本回退，差别仅在于查询的是 Linux 专属夹具表。
func GetMinimalValidLinuxPod(level api.Level, version api.Version) (*corev1.Pod, error) {
	originalVersion := version
	for {
		pod, exists := minimalValidLinuxPods[level][version]
		if exists {
			return pod.DeepCopy(), nil
		}
		if version.Minor() <= 0 {
			return nil, fmt.Errorf("no valid pod fixture found in specified or older versions for %s/%s", level, originalVersion.String())
		}
		version = api.MajorMinorVersion(version.Major(), version.Minor()-1)
	}
}

// GetMinimalValidWindowsPod returns a minimal valid windows pod for the specified level and version.
// Windows 的受限基线在 1.25 之前与 Linux 同源，回退时需命中对应的 Windows 副本而非 OS 中立夹具。
func GetMinimalValidWindowsPod(level api.Level, version api.Version) (*corev1.Pod, error) {
	originalVersion := version
	for {
		pod, exists := minimalValidWindowsPods[level][version]
		if exists {
			return pod.DeepCopy(), nil
		}
		if version.Minor() <= 0 {
			return nil, fmt.Errorf("no valid pod fixture found in specified or older versions for %s/%s", level, originalVersion.String())
		}
		version = api.MajorMinorVersion(version.Major(), version.Minor()-1)
	}
}

// fixtureGenerators holds fixture generators per-level per-version.
// To add generators, use registerFixtureGenerator().
// To get fixtures for a particular level/version, use getFixtures().
// 集中注册让每个检查的通过与失败样例共用同一份最小合法 Pod，改动一次即可覆盖所有版本矩阵。
var fixtureGenerators = map[fixtureKey]fixtureGenerator{}

// fixtureKey is a tuple of version/level/check name
// 三元组定位唯一一个检查在某个版本/级别下的夹具，缺一不可；
// 同名检查在不同版本可有不同规则，因此版本必须参与键。
type fixtureKey struct {
	version api.Version
	level   api.Level
	check   policy.CheckID
}

// fixtureGenerator holds generators for valid and invalid fixtures.
// 生成器按版本注册一次即可向后沿用，避免每个版本重复定义相同的通过/失败变体。
type fixtureGenerator struct {
	// expectErrorSubstring is a substring to expect in the error message for failed pods.
	// if empty, the check ID is used.
	expectErrorSubstring string

	// failRequiresFeatures lists feature gates that must all be enabled for failure cases to fail properly.
	// This allows failure cases depending on rejecting data populated in alpha or beta fields to be skipped when those features are not enabled.
	// If empty, failure test cases are always run.
	// Pass cases are not allowed to be feature-gated (pass cases must only depend on data existing in GA fields).
	failRequiresFeatures []featuregate.Feature

	// failRequiresError indicates the fixtures in the failure cases cannot be created with warnings, but result in API errors.
	// This happens when the combination of fields required to fail the PSA check also fails validation.
	// When failRequiresError=true, the controller test scenarios expect failure cases to be rejected rather than accepted with warnings.
	failRequiresError bool

	// generatePass transforms a minimum valid pod into one or more valid pods that pass the given policy level.
	// pods do not need to populate metadata.name.
	// 接收 level 是为了让同一个生成器在 baseline 与 restricted 下产出不同夹具
	// （例如某些字段只在更严格级别下才算违规）。
	generatePass func(*corev1.Pod, api.Level) []*corev1.Pod
	// generateFail transforms a minimum valid pod into one or more invalid pods.
	// pods do not need to populate metadata.name.
	// 失败夹具必须至少返回一个，否则 getFixtures 无法验证该检查确实会拒绝违规 Pod。
	generateFail func(*corev1.Pod) []*corev1.Pod
}

// fixtureData holds valid and invalid pod fixtures.
// 由生成器即时物化，调用方拿到的是独立副本，可随意修改而不污染注册表。
type fixtureData struct {
	expectErrorSubstring string

	// failRequiresFeatures lists feature gates that must all be enabled for failure cases to fail properly.
	// This allows failure cases depending on rejecting data populated in alpha or beta fields to be skipped when those features are not enabled.
	// If empty, failure test cases are always run.
	// Pass cases are not allowed to be feature-gated (pass cases must only depend on data existing in GA fields).
	failRequiresFeatures []featuregate.Feature

	// failRequiresError indicates the fixtures in the failure cases cannot be created with warnings, but result in API errors.
	// This happens when the combination of fields required to fail the PSA check also fails validation.
	// When failRequiresError=true, the controller test scenarios expect failure cases to be rejected rather than accepted with warnings.
	failRequiresError bool

	pass []*corev1.Pod
	fail []*corev1.Pod
}

// registerFixtureGenerator adds a generator for the given level/version/check.
// A generator registered for v1.x is used for v1.x+ if no generator is registered for the higher version.
// 注册时立即校验键与生成器完整性，把错误暴露在 init 阶段，避免测试运行时才出现空指针。
// baseline 的生成器会同时挂到 restricted 上：同名的基线限制在两个级别都应生效。
func registerFixtureGenerator(key fixtureKey, generator fixtureGenerator) {
	if err := checkKey(key); err != nil {
		panic(err)
	}
	if _, exists := fixtureGenerators[key]; exists {
		panic(fmt.Errorf("fixture generator already registered for key %#v", key))
	}
	if generator.generatePass == nil || generator.generateFail == nil {
		panic(fmt.Errorf("adding %#v: must specify generatePass/generateFail", key))
	}
	fixtureGenerators[key] = generator

	if key.level == api.LevelBaseline {
		// also register to restricted
		// 基线规则在受限级别同样禁止，共享同一生成器可避免两处夹具出现分歧。
		restrictedKey := key
		restrictedKey.level = api.LevelRestricted
		if _, exists := fixtureGenerators[restrictedKey]; exists {
			panic(fmt.Errorf("fixture generator already registered for restricted version of key %#v", key))
		}
		fixtureGenerators[restrictedKey] = generator
	}
}

// getFixtures returns the fixture data for the specified level/version/check.
// Fixtures are generated by applying the registered generator to the minimal valid pod for that level/version.
// If no fixture generator exists for the given version, previous generators are checked back to 1.0.
// 每次调用都重新生成，防止某个用例对 Pod 的修改泄漏到下一个用例。
// 未注册 check ID 时回退到 check ID 本身作为错误子串，多数检查的拒绝原因恰好与 ID 同名。
func getFixtures(key fixtureKey) (fixtureData, error) {
	if err := checkKey(key); err != nil {
		return fixtureData{}, err
	}

	validPodForLevel, err := GetMinimalValidPod(key.level, key.version)
	if err != nil {
		return fixtureData{}, err
	}

	for {
		if generator, exists := fixtureGenerators[key]; exists {
			data := fixtureData{
				expectErrorSubstring: generator.expectErrorSubstring,
				failRequiresFeatures: generator.failRequiresFeatures,
				failRequiresError:    generator.failRequiresError,

				pass: generator.generatePass(validPodForLevel.DeepCopy(), key.level),
				fail: generator.generateFail(validPodForLevel.DeepCopy()),
			}
			if len(data.expectErrorSubstring) == 0 {
				data.expectErrorSubstring = string(key.check)
			}
			if len(data.fail) == 0 {
				return fixtureData{}, fmt.Errorf("generateFail for %#v must return at least one pod", key)
			}
			return data, nil
		}
		if key.version.Minor() == 0 {
			return fixtureData{}, fmt.Errorf("no fixture generator found in specified or older versions for %#v", key)
		}
		// check the next older version
		key.version = api.MajorMinorVersion(key.version.Major(), key.version.Minor()-1)
	}
}

// checkKey ensures the fixture key has a valid level, version, and check.
// 提前拦住拼错的 check ID 与越界版本，避免注册了永远不会被查找的夹具而测试静默漏跑。
func checkKey(key fixtureKey) error {
	if key.level != api.LevelBaseline && key.level != api.LevelRestricted {
		return fmt.Errorf("invalid key, level must be baseline or restricted: %#v", key)
	}
	if key.version.Latest() || key.version.Major() != 1 || key.version.Minor() < 0 {
		return fmt.Errorf("invalid key, version must be 1.0+: %#v", key)
	}
	if key.check == "" {
		return fmt.Errorf("invalid key, check must not be empty")
	}
	found := false
	for _, check := range policy.DefaultChecks() {
		if check.ID == key.check {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("invalid key %#v, check does not exist at version", key)
	}
	return nil
}
