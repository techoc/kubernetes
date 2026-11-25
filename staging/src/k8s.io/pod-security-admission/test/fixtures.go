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

// minimalValidPods 按级别和版本保存 OS 中立的最小合法 Pod，访问时使用 GetMinimalValidPod。
// 各检查在同一基线上叠加变体，避免分别维护完整 Pod 导致夹具漂移。
var minimalValidPods = map[api.Level]map[api.Version]*corev1.Pod{}

// minimalValidLinuxPods 按级别和版本保存 Linux 专属的最小合法 Pod，访问时使用 GetMinimalValidLinuxPod。
// 单独保存可避免 seccomp 等 Linux 要求误伤 Windows 夹具。
var minimalValidLinuxPods = map[api.Level]map[api.Version]*corev1.Pod{}

// minimalValidWindowsPods 按级别和版本保存 Windows 专属的最小合法 Pod，访问时使用 GetMinimalValidWindowsPod。
// 1.25 起受限策略按 OS 分化，Windows 需要独立基线。
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
	// 以下为 OS 中立的最小合法 Pod。
	minimalValidPods[api.LevelBaseline] = map[api.Version]*corev1.Pod{}
	minimalValidPods[api.LevelRestricted] = map[api.Version]*corev1.Pod{}

	minimalValidLinuxPods[api.LevelRestricted] = map[api.Version]*corev1.Pod{}
	minimalValidWindowsPods[api.LevelRestricted] = map[api.Version]*corev1.Pod{}
	// 基线只需最低限度的 init 容器和普通容器；后续收紧要求由受限夹具增量添加。
	baseline_1_0 := &corev1.Pod{Spec: corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "initcontainer1", Image: "registry.k8s.io/pause"}},
		Containers:     []corev1.Container{{Name: "container1", Image: "registry.k8s.io/pause"}}}}
	minimalValidPods[api.LevelBaseline][api.MajorMinorVersion(1, 0)] = baseline_1_0

	// 定义各版本的最小合法受限 Pod。

	// 1.0 起：基线 + runAsNonRoot=true
	restricted_1_0 := tweak(baseline_1_0, func(p *corev1.Pod) {
		p.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true)}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 0)] = restricted_1_0
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 0)] = addLinux(restricted_1_0)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 0)] = addWindows(restricted_1_0)

	// 1.8 起：allowPrivilegeEscalation=false
	restricted_1_8 := tweak(restricted_1_0, func(p *corev1.Pod) {
		p.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false)}
		p.Spec.InitContainers[0].SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false)}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 8)] = restricted_1_8
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 8)] = addLinux(restricted_1_8)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 8)] = addWindows(restricted_1_8)

	// 1.19 起：seccompProfile.type=RuntimeDefault
	restricted_1_19 := tweak(restricted_1_8, func(p *corev1.Pod) {
		p.Annotations = nil
		p.Spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 19)] = restricted_1_19
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 19)] = addLinux(restricted_1_19)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 19)] = addWindows(restricted_1_19)

	// 1.22 起：capabilities.drop=["ALL"]
	restricted_1_22 := tweak(restricted_1_19, func(p *corev1.Pod) {
		p.Spec.Containers[0].SecurityContext.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}
		p.Spec.InitContainers[0].SecurityContext.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}
	})
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 22)] = restricted_1_22
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 22)] = addLinux(restricted_1_22)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 22)] = addWindows(restricted_1_22)

	// 1.25 起按 OS 分化：OS 中立夹具沿用 1.22，Linux/Windows 使用各自基线，
	// 避免新增的 OS 专属限制让另一平台无谓失败。
	minimalValidPods[api.LevelRestricted][api.MajorMinorVersion(1, 25)] = restricted_1_22
	minimalValidLinuxPods[api.LevelRestricted][api.MajorMinorVersion(1, 25)] = addLinux(restricted_1_22)
	// 1.0 之后、1.25 之前新增的受限要求均不适用于显式声明为 Windows 的 Pod。
	restricted_1_25_windows := addWindows(restricted_1_0)
	minimalValidWindowsPods[api.LevelRestricted][api.MajorMinorVersion(1, 25)] = restricted_1_25_windows

}

// GetMinimalValidPod 返回指定级别和版本的 OS 中立最小合法 Pod 深拷贝。
// 缺少夹具时逐版本回退；规则或基线变化后必须注册新版本，回退不能替代新版本夹具。
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

// GetMinimalValidLinuxPod 返回指定级别和版本的 Linux 最小合法 Pod 深拷贝，并按版本回退查找。
// 规则变化后需注册新版本，不能依赖回退沿用旧基线。
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

// GetMinimalValidWindowsPod 返回指定级别和版本的 Windows 最小合法 Pod 深拷贝，并按版本回退查找。
// 规则变化后需注册新版本，不能依赖回退沿用旧基线。
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

// fixtureGenerators 按级别、版本和检查保存生成器，分别使用 registerFixtureGenerator 注册、getFixtures 获取。
// 集中注册可让同一检查的通过和失败夹具共用最小合法 Pod，并覆盖整个版本矩阵。
var fixtureGenerators = map[fixtureKey]fixtureGenerator{}

// fixtureKey 由版本、级别和检查名组成，唯一标识一组夹具。
// 同一检查在不同版本可有不同规则，版本必须参与键。
type fixtureKey struct {
	version api.Version
	level   api.Level
	check   policy.CheckID
}

// fixtureGenerator 保存通过和失败夹具的生成函数及失败断言约束。
// 生成器按版本注册，未单独注册更高版本时可继续沿用。
type fixtureGenerator struct {
	// expectErrorSubstring 是失败 Pod 错误信息中预期的子串；为空时使用检查 ID。
	expectErrorSubstring string

	// failRequiresFeatures 列出失败夹具正常触发拒绝所需的全部 feature gate。
	// 依赖 alpha/beta 字段的失败样例在门控未启用时跳过；为空则始终运行。
	// 通过夹具不得依赖门控，只能使用已 GA 的字段。
	failRequiresFeatures []featuregate.Feature

	// failRequiresError 表示失败夹具会触发 API 错误，而不是带警告被接纳。
	// 这通常是因为触发 PodSecurity 拒绝的字段组合同时违反 API 校验；
	// 控制器测试据此预期硬拒绝。
	failRequiresError bool

	// generatePass 将最小合法 Pod 变换为一个或多个通过指定级别的 Pod，无需设置 metadata.name。
	// 接收 level 是为了让同一生成器在不同级别产出不同夹具。
	generatePass func(*corev1.Pod, api.Level) []*corev1.Pod
	// generateFail 将最小合法 Pod 变换为一个或多个失败 Pod，无需设置 metadata.name。
	// 必须至少返回一个 Pod，getFixtures 会校验失败夹具非空。
	generateFail func(*corev1.Pod) []*corev1.Pod
}

// fixtureData 保存物化后的通过和失败 Pod，以及对应的失败断言约束。
// 每次获取都会重新生成并深拷贝，调用方修改不会污染注册表。
type fixtureData struct {
	expectErrorSubstring string

	// failRequiresFeatures 列出运行失败夹具所需的全部 feature gate；为空表示始终运行。
	// 通过夹具不得依赖门控，只能使用已 GA 的字段。
	failRequiresFeatures []featuregate.Feature

	// failRequiresError 表示失败夹具会触发 API 硬错误，控制器测试应预期拒绝而非警告。
	failRequiresError bool

	pass []*corev1.Pod // 通过夹具。
	fail []*corev1.Pod // 失败夹具。
}

// registerFixtureGenerator 为指定级别、版本和检查注册生成器，并校验键与生成函数完整。
// v1.x 的生成器会用于 v1.x+，直到注册更高版本；基线生成器同时注册到受限级别。
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
		// 基线规则在受限级别同样生效，复用生成器避免两处夹具分歧。
		restrictedKey := key
		restrictedKey.level = api.LevelRestricted
		if _, exists := fixtureGenerators[restrictedKey]; exists {
			panic(fmt.Errorf("fixture generator already registered for restricted version of key %#v", key))
		}
		fixtureGenerators[restrictedKey] = generator
	}
}

// getFixtures 返回指定级别、版本和检查的夹具数据。
// 生成器作用于该级别/版本的最小合法 Pod；缺少当前版本生成器时回退查找至 1.0。
// 每次调用都重新生成并深拷贝，防止用例修改泄漏到后续测试。
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
				// 未指定时以检查 ID 作为预期错误子串。
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
		// 继续查找下一个更低版本。
		key.version = api.MajorMinorVersion(key.version.Major(), key.version.Minor()-1)
	}
}

// checkKey 校验级别、版本和检查 ID，防止注册错误键后夹具永远不会被查找。
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
