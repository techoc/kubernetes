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
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	apimachineryversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/registry/rest"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/klog/v2"
)

// alphaPattern 匹配形如 v1alpha1 的版本名，用于区分 alpha API。
var alphaPattern = regexp.MustCompile(`^v\d+alpha\d+$`)

// resourceExpirationEvaluator 持有判断某个 rest.Storage 是否需要从 API 中排除所需的信息；
// 判断依据来自生成代码中的 APILifecycleRemoved/APILifecycleIntroduced，
// 以及若干用于发布窗口内“宽限”的环境变量。
type resourceExpirationEvaluator struct {
	currentVersion                          *apimachineryversion.Version
	emulationForwardCompatible              bool
	runtimeConfigEmulationForwardCompatible bool
	isAlpha                                 bool
	// 专门识别 alpha.0：该版本下合入 master 的改动会自动回传到 release-1.n 分支。
	isAlphaZero bool
	// 测试通过它调整待移除 API 的行为，避免 CI 立刻失败。
	// 设置 KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA 可预演打 beta 标签时会移除哪些 API。
	// 该开关只在 alpha（不含 alpha.0）阶段生效。
	strictRemovedHandlingInAlpha bool
	// 由集群管理员在出事后用作临时缓冲；合并前应改为正式的 flag。
	// 设置 KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE 可把 API 移除再推迟一个版本。
	serveRemovedAPIsOneMoreRelease bool
}

// ResourceExpirationEvaluator 判断某个资源是否应继续对外提供服务。
type ResourceExpirationEvaluator interface {
	// 原地修改 storage map，移除已删除或晚于当前版本才引入的 kind。
	// versionedResourcesStorageMap 与 APIGroupInfo 的同名字段一致：版本 -> 资源 -> storage。
	RemoveUnavailableKinds(groupName string, versioner runtime.ObjectVersioner, versionedResourcesStorageMap map[string]map[string]rest.Storage, apiResourceConfigSource serverstorage.APIResourceConfigSource) error
	// 给定的移除版本晚于当前版本时返回 true。
	ShouldServeForVersion(majorRemoved, minorRemoved int) bool
}

// ResourceExpirationEvaluatorOptions 收集构造 evaluator 所需的全部输入，
// 便于调用方（尤其是测试）逐项覆盖。
type ResourceExpirationEvaluatorOptions struct {
	// apiserver 的当前版本。
	CurrentVersion *apimachineryversion.Version
	// 版本中的预发布字段，用于判断当前二进制是否为 alpha。
	Prerelease string
	// 当同名资源的低优先级版本已在服务时，是否也对外提供晚于当前版本引入的资源。
	// 对 alpha API 不适用。
	EmulationForwardCompatible bool
	// 当未来版本资源被 runtime-config 显式开启时，是否也对外提供服务。
	RuntimeConfigEmulationForwardCompatible bool
}

// NewResourceExpirationEvaluator 以完整版本号构造 evaluator；
// 内部只保留 major.minor，pre-release 仅用于 alpha 判断。
func NewResourceExpirationEvaluator(currentVersion *apimachineryversion.Version) (ResourceExpirationEvaluator, error) {
	opts := ResourceExpirationEvaluatorOptions{
		CurrentVersion: apimachineryversion.MajorMinor(currentVersion.Major(), currentVersion.Minor()),
		Prerelease:     currentVersion.PreRelease(),
	}
	return NewResourceExpirationEvaluatorFromOptions(opts)
}

// NewResourceExpirationEvaluatorFromOptions 按选项构造 evaluator，并读取发布窗口相关的环境变量。
func NewResourceExpirationEvaluatorFromOptions(opts ResourceExpirationEvaluatorOptions) (ResourceExpirationEvaluator, error) {
	currentVersion := opts.CurrentVersion
	if currentVersion == nil {
		return nil, fmt.Errorf("empty NewResourceExpirationEvaluator currentVersion")
	}
	klog.V(1).Infof("NewResourceExpirationEvaluator with currentVersion: %s.", currentVersion)
	ret := &resourceExpirationEvaluator{
		strictRemovedHandlingInAlpha:            false,
		emulationForwardCompatible:              opts.EmulationForwardCompatible,
		runtimeConfigEmulationForwardCompatible: opts.RuntimeConfigEmulationForwardCompatible,
	}
	// 只保留输入版本的 major/minor，忽略补丁号。
	ret.currentVersion = apimachineryversion.MajorMinor(currentVersion.Major(), currentVersion.Minor())
	ret.isAlpha = strings.Contains(opts.Prerelease, "alpha")
	ret.isAlphaZero = strings.Contains(opts.Prerelease, "alpha.0")

	// 环境变量仅在显式设置时生效；解析失败直接报错，避免静默使用错误的发布策略。
	if envString, ok := os.LookupEnv("KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA"); !ok {
		// 未设置该环境变量，保持默认
	} else if envBool, err := strconv.ParseBool(envString); err != nil {
		return nil, err
	} else {
		ret.strictRemovedHandlingInAlpha = envBool
	}

	if envString, ok := os.LookupEnv("KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE"); !ok {
		// 未设置该环境变量，保持默认
	} else if envBool, err := strconv.ParseBool(envString); err != nil {
		return nil, err
	} else {
		ret.serveRemovedAPIsOneMoreRelease = envBool
	}

	return ret, nil
}

// isNotRemoved 根据 APILifecycleRemoved 信息检查资源是否已被移除；
// 需要先转换到目标版本再断言 removedInterface，因为删除注解按外部版本生成。
func (e *resourceExpirationEvaluator) isNotRemoved(gv schema.GroupVersion, versioner runtime.ObjectVersioner, resourceServingInfo rest.Storage) bool {
	internalPtr := resourceServingInfo.New()

	target := gv
	// 尊重 storage 自行指定的组版本（如 scale 子资源），以此判断引入版本。
	if versionProvider, ok := resourceServingInfo.(rest.GroupVersionKindProvider); ok {
		target = versionProvider.GroupVersionKind(target).GroupVersion()
	}

	versionedPtr, err := versioner.ConvertToVersion(internalPtr, target)
	if err != nil {
		utilruntime.HandleError(err)
		return false
	}

	removed, ok := versionedPtr.(removedInterface)
	if !ok {
		// 没有删除注解即视为尚未移除。
		return true
	}
	majorRemoved, minorRemoved := removed.APILifecycleRemoved()
	return e.ShouldServeForVersion(majorRemoved, minorRemoved)
}

// ShouldServeForVersion 根据 API 的移除版本与当前版本/ prerelease 阶段决定是否继续服务。
func (e *resourceExpirationEvaluator) ShouldServeForVersion(majorRemoved, minorRemoved int) bool {
	removedVer := apimachineryversion.MajorMinor(uint(majorRemoved), uint(minorRemoved))
	if removedVer.GreaterThan(e.currentVersion) {
		return true
	}
	if removedVer.LessThan(e.currentVersion) {
		return false
	}
	// 走到这里 major 与 minor 相同，说明该 API 应在当前发布 GA 时移除。
	// 若是 alpha 标签，默认不移除，但提供选项强制移除；
	// 若集群管理员要求再服务一个发布周期，则允许继续服务。
	// 恰好处于移除版本时，默认只在 GA 后移除；alpha 阶段留出清理测试的时间。
	if e.isAlpha && !e.isAlphaZero && e.strictRemovedHandlingInAlpha { // don't serve in alpha.1+ if we want strict handling
		return false
	}
	if e.isAlpha { // alphas are allowed to continue serving expired betas while we clean up the test
		return true
	}
	if e.serveRemovedAPIsOneMoreRelease { // cluster-admins are allowed to kick the can one release down the road
		return true
	}
	return false
}

// removedInterface 由 prerelease-lifecycle-gen 生成，声明 API 的移除版本。
type removedInterface interface {
	APILifecycleRemoved() (major, minor int)
}

// 由 types.go 中 "k8s:prerelease-lifecycle-gen:introduced" 标记生成的对象接口。
type introducedInterface interface {
	APILifecycleIntroduced() (major, minor int)
}

// removeDeletedKinds 原地修改 storage map，移除已删除 kind 的 storage。
// versionedResourcesStorageMap 与 APIGroupInfo 的同名字段一致：版本 -> 资源 -> storage。
func (e *resourceExpirationEvaluator) removeDeletedKinds(groupName string, versioner runtime.ObjectVersioner, versionedResourcesStorageMap map[string]map[string]rest.Storage) {
	versionsToRemove := sets.NewString()
	for apiVersion := range sets.StringKeySet(versionedResourcesStorageMap) {
		versionToResource := versionedResourcesStorageMap[apiVersion]
		resourcesToRemove := sets.NewString()
		for resourceName, resourceServingInfo := range versionToResource {
			if !e.isNotRemoved(schema.GroupVersion{Group: groupName, Version: apiVersion}, versioner, resourceServingInfo) {
				resourcesToRemove.Insert(resourceName)
			}
		}

		for resourceName := range versionedResourcesStorageMap[apiVersion] {
			if !shouldRemoveResourceAndSubresources(resourcesToRemove, resourceName) {
				continue
			}

			// 必须调用 Destroy 释放后台协程/连接，否则移除资源会泄漏。
			klog.V(1).Infof("Removing resource %v.%v.%v because it is time to stop serving it per APILifecycle.", resourceName, apiVersion, groupName)
			storage := versionToResource[resourceName]
			storage.Destroy()
			delete(versionToResource, resourceName)
		}
		versionedResourcesStorageMap[apiVersion] = versionToResource

		// 版本下已无资源，整个版本也应从发现文档中消失。
		if len(versionedResourcesStorageMap[apiVersion]) == 0 {
			versionsToRemove.Insert(apiVersion)
		}
	}

	for _, apiVersion := range versionsToRemove.List() {
		klog.V(1).Infof("Removing version %v.%v because it is time to stop serving it because it has no resources per APILifecycle.", apiVersion, groupName)
		delete(versionedResourcesStorageMap, apiVersion)
	}
}

// RemoveUnavailableKinds 先剔除已声明移除的 API，再剔除当前版本尚未引入的 API。
func (e *resourceExpirationEvaluator) RemoveUnavailableKinds(groupName string, versioner runtime.ObjectVersioner, versionedResourcesStorageMap map[string]map[string]rest.Storage, apiResourceConfigSource serverstorage.APIResourceConfigSource) error {
	e.removeDeletedKinds(groupName, versioner, versionedResourcesStorageMap)
	return e.removeUnintroducedKinds(groupName, versioner, versionedResourcesStorageMap, apiResourceConfigSource)
}

// removeUnintroducedKinds 原地修改 storage map，移除晚于当前版本引入的 kind。
// versionedResourcesStorageMap 与 APIGroupInfo 的同名字段一致：版本 -> 资源 -> storage。
func (e *resourceExpirationEvaluator) removeUnintroducedKinds(groupName string, versioner runtime.ObjectVersioner, versionedResourcesStorageMap map[string]map[string]rest.Storage, apiResourceConfigSource serverstorage.APIResourceConfigSource) error {
	versionsToRemove := sets.NewString()
	prioritizedVersions := versioner.PrioritizedVersionsForGroup(groupName)
	// 从高到低排序，随后倒序遍历即可先处理低优先级版本。
	sort.Slice(prioritizedVersions, func(i, j int) bool {
		return version.CompareKubeAwareVersionStrings(prioritizedVersions[i].Version, prioritizedVersions[j].Version) > 0
	})
	enabledResources := sets.NewString()

	// 倒序遍历，先处理低优先级版本。
	for i := len(prioritizedVersions) - 1; i >= 0; i-- {
		apiVersion := prioritizedVersions[i].Version
		versionToResource := versionedResourcesStorageMap[apiVersion]
		if len(versionToResource) == 0 {
			continue
		}
		resourcesToRemove := sets.NewString()
		for resourceName, resourceServingInfo := range versionToResource {
			// 按优先级从低到高推进：若此前已启用同名资源的其它版本，说明存在低优先级版本被启用，
			// 当前版本的向前兼容模拟据此判断。
			lowerPriorityEnabled := enabledResources.Has(resourceName)
			shouldKeep, err := e.shouldServeBasedOnVersionIntroduced(schema.GroupVersionResource{Group: groupName, Version: apiVersion, Resource: resourceName},
				versioner, resourceServingInfo, apiResourceConfigSource, lowerPriorityEnabled)
			if err != nil {
				return err
			}
			if !shouldKeep {
				resourcesToRemove.Insert(resourceName)
			} else if !alphaPattern.MatchString(apiVersion) {
				// 把启用信息带到下一轮，用于判断更高优先级资源的模拟兼容；
				// alpha API 不参与其中，因为它不适用向前兼容模拟。
				enabledResources.Insert(resourceName)
			}
		}

		for resourceName := range versionedResourcesStorageMap[apiVersion] {
			if !shouldRemoveResourceAndSubresources(resourcesToRemove, resourceName) {
				continue
			}

			klog.V(1).Infof("Removing resource %v.%v.%v because it is introduced after the current version %s per APILifecycle.", resourceName, apiVersion, groupName, e.currentVersion.String())
			storage := versionToResource[resourceName]
			storage.Destroy()
			delete(versionToResource, resourceName)
		}
		versionedResourcesStorageMap[apiVersion] = versionToResource

		if len(versionedResourcesStorageMap[apiVersion]) == 0 {
			versionsToRemove.Insert(apiVersion)
		}
	}

	for _, apiVersion := range versionsToRemove.List() {
		gv := schema.GroupVersion{Group: groupName, Version: apiVersion}
		// 用户显式启用了一个尚未引入的版本，说明配置与二进制不匹配，应尽早报错而非静默忽略。
		if apiResourceConfigSource != nil && apiResourceConfigSource.VersionExplicitlyEnabled(gv) {
			return fmt.Errorf(
				"cannot enable version %s in runtime-config because all the resources have been introduced after the current version %s. Consider setting --runtime-config-emulation-forward-compatible=true",
				gv, e.currentVersion)
		}
		klog.V(1).Infof("Removing version %v.%v because it is introduced after the current version %s and because it has no resources per APILifecycle.", apiVersion, groupName, e.currentVersion.String())
		delete(versionedResourcesStorageMap, apiVersion)
	}
	return nil
}

// shouldServeBasedOnVersionIntroduced 判断“当前版本尚未引入”的资源是否应因兼容/模拟模式而继续服务。
func (e *resourceExpirationEvaluator) shouldServeBasedOnVersionIntroduced(gvr schema.GroupVersionResource, versioner runtime.ObjectVersioner, resourceServingInfo rest.Storage,
	apiResourceConfigSource serverstorage.APIResourceConfigSource, lowerPriorityEnabled bool) (bool, error) {
	// 未声明引入版本时按 0.0 处理，即始终视为已引入。
	verIntroduced := apimachineryversion.MajorMinor(0, 0)
	internalPtr := resourceServingInfo.New()

	target := gvr.GroupVersion()
	// 尊重 storage 自行指定的组版本（如 scale 子资源），以此判断引入版本。
	if versionProvider, ok := resourceServingInfo.(rest.GroupVersionKindProvider); ok {
		target = versionProvider.GroupVersionKind(target).GroupVersion()
	}

	versionedPtr, err := versioner.ConvertToVersion(internalPtr, target)
	if err != nil {
		utilruntime.HandleError(err)
		return false, err
	}

	introduced, ok := versionedPtr.(introducedInterface)
	if ok {
		majorIntroduced, minorIntroduced := introduced.APILifecycleIntroduced()
		verIntroduced = apimachineryversion.MajorMinor(uint(majorIntroduced), uint(minorIntroduced))
	}
	// 在当前版本或更早引入的资源，正常提供服务。
	if e.currentVersion.AtLeast(verIntroduced) {
		return true, nil
	}
	// 后续逻辑判断晚于当前版本引入的资源是否应提供服务（仅模拟兼容模式下适用）。

	// 已启用低优先级版本时，若开启模拟兼容，高优先级的同名资源也应启用。
	if e.emulationForwardCompatible && lowerPriorityEnabled {
		return true, nil
	}
	if apiResourceConfigSource == nil {
		return false, nil
	}
	// runtime-config 的向前兼容模式允许显式开启未来版本资源。
	if e.runtimeConfigEmulationForwardCompatible && (apiResourceConfigSource.ResourceExplicitlyEnabled(gvr) || apiResourceConfigSource.VersionExplicitlyEnabled(gvr.GroupVersion())) {
		return true, nil
	}
	// 未来版本资源被显式开启但未启用 runtime-config 向前兼容时，直接报错。
	if apiResourceConfigSource.ResourceExplicitlyEnabled(gvr) {
		return false, fmt.Errorf("cannot enable resource %s in runtime-config because it is introduced at %s after the current version %s. Consider setting --runtime-config-emulation-forward-compatible=true",
			gvr, verIntroduced, e.currentVersion)
	}
	return false, nil
}

// shouldRemoveResourceAndSubresources 判断资源本身或其父资源是否在删除集合中。
func shouldRemoveResourceAndSubresources(resourcesToRemove sets.String, resourceName string) bool {
	for _, resourceToRemove := range resourcesToRemove.List() {
		if resourceName == resourceToRemove {
			return true
		}
		// API 资源可以嵌套，如 deployments、deployments/status、deployments/scale。
		// 子资源未必服务父类型，但父类型被删除时其所有子资源也应一并移除。
		if strings.HasPrefix(resourceName, resourceToRemove+"/") {
			return true
		}
	}
	return false
}
