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

package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	schedulingapi "k8s.io/kubernetes/pkg/apis/scheduling"

	// TODO: 若 api.Registry.GroupOrDie(v1.GroupName).GroupVersion.String()
	// 改为 "v1"，则移除此导入。
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	// 确保已安装核心 API
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/util/hash"

	"k8s.io/klog/v2"
)

const (
	// maxConfigLength 限制单个静态 Pod 配置文件的最大长度（10MB）；
	// nodeCriticalPriority 是静态 Pod 的节点关键优先级；
	// maxStaticPodWarningMessages 是告警去重缓存容量。
	maxConfigLength             = 10 * 1 << 20 // 10MB
	nodeCriticalPriority        = schedulingapi.SystemCriticalPriority + 1000
	maxStaticPodWarningMessages = 1000
)

var (
	// staticPodWarningCache 用于对静态 Pod 优先级告警去重。
	staticPodWarningCache = newWarningCache(maxStaticPodWarningMessages)
)

// 在名称后附加小写的节点名，使 Pod 名在节点间唯一。
func generatePodName(name string, nodeName types.NodeName) string {
	return fmt.Sprintf("%s-%s", name, strings.ToLower(string(nodeName)))
}

// applyDefaults 为静态 Pod 补充 UID、名称、命名空间、节点名、注解与默认状态。
func applyDefaults(logger klog.Logger, pod *api.Pod, source string, isFile bool, nodeName types.NodeName) error {
	// UID 为空时基于 Pod 内容与来源计算哈希，保证同一配置在多次读取间保持稳定。
	if len(pod.UID) == 0 {
		hasher := fnv.New128a()
		hash.DeepHashObject(hasher, pod)
		// DeepHashObject 会重置哈希状态，因此必须在它之后再写入 Pod 来源信息。
		if isFile {
			fmt.Fprintf(hasher, "host:%s", nodeName)
			fmt.Fprintf(hasher, "file:%s", source)
		} else {
			fmt.Fprintf(hasher, "url:%s", source)
		}
		pod.UID = types.UID(hex.EncodeToString(hasher.Sum(nil)[0:]))
		logger.V(5).Info("Generated UID", "pod", klog.KObj(pod), "podUID", pod.UID, "source", source)
	}

	// 名称附加节点名，避免同一静态 Pod 在不同节点上重名。
	pod.Name = generatePodName(pod.Name, nodeName)
	logger.V(5).Info("Generated pod name", "pod", klog.KObj(pod), "podUID", pod.UID, "source", source)

	// 未指定命名空间时使用 default。
	if pod.Namespace == "" {
		pod.Namespace = metav1.NamespaceDefault
	}
	logger.V(5).Info("Set namespace for pod", "pod", klog.KObj(pod), "source", source)

	// 设置 NodeName 字段，表示该 Pod 已调度到当前节点。
	pod.Spec.NodeName = string(nodeName)

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	// 生成的 UID 实际是配置内容的哈希，用于检测配置变化。
	pod.Annotations[kubetypes.ConfigHashAnnotationKey] = string(pod.UID)

	if isFile {
		// 为静态 Pod 应用默认的污点容忍，使其在节点出现问题时不会被驱逐。
		helper.AddOrUpdateTolerationInPod(pod, &api.Toleration{
			Operator: "Exists",
			Effect:   api.TaintEffectNoExecute,
		})
	}

	// 默认状态设为 Pending，等待容器运行时启动。
	pod.Status.Phase = api.PodPending
	return nil
}

// defaultFunc 是为 Pod 应用默认值的回调类型。
type defaultFunc func(logger klog.Logger, pod *api.Pod) error

// ErrStaticPodTriedToUseClusterTrustBundle 表示静态 Pod 试图使用 ClusterTrustBundle 投影卷。
var ErrStaticPodTriedToUseClusterTrustBundle = errors.New("static pods may not use ClusterTrustBundle projected volume sources")

// ErrStaticPodTriedToUseResourceClaims 表示静态 Pod 试图使用 ResourceClaim。
var ErrStaticPodTriedToUseResourceClaims = errors.New("static pods may not use ResourceClaims")

// tryDecodeSinglePod 从数据中解析并校验单个 Pod 配置。
func tryDecodeSinglePod(logger klog.Logger, data []byte, defaultFn defaultFunc) (parsed bool, pod *v1.Pod, err error) {
	// JSON 是合法的 YAML，因此该转换对所有输入都适用。
	json, err := utilyaml.ToJSON(data)
	if err != nil {
		return false, nil, err
	}
	obj, err := runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), json)
	if err != nil {
		return false, pod, err
	}

	newPod, ok := obj.(*api.Pod)
	// 检查解码结果是否为单个 Pod。
	if !ok {
		return false, pod, fmt.Errorf("invalid pod: %#v", obj)
	}

	if newPod.Name == "" {
		return true, pod, fmt.Errorf("invalid pod: name is needed for the pod")
	}

	// 应用默认值并校验 Pod。
	if err = defaultFn(logger, newPod); err != nil {
		return true, pod, err
	}
	opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&newPod.Spec, nil, &newPod.ObjectMeta, nil)
	if errs := validation.ValidatePodCreate(newPod, opts); len(errs) > 0 {
		return true, pod, fmt.Errorf("invalid pod: %v", errs)
	}
	v1Pod := &v1.Pod{}
	if err := k8s_api_v1.Convert_core_Pod_To_v1_Pod(newPod, v1Pod, nil); err != nil {
		logger.Error(err, "Pod failed to convert to v1", "pod", klog.KObj(newPod))
		return true, nil, err
	}

	// 静态 Pod 不允许引用其他 API 对象（如 ResourceClaim）。
	_, resource, err := podutil.HasAPIObjectReference(newPod)
	if err != nil {
		return true, nil, err
	}
	if resource != "" {
		return true, nil, fmt.Errorf("static pods may not reference %s", resource)
	}

	// 优先级配置异常只产生告警，并通过缓存去重避免刷屏。
	warning := getStaticPodPriorityWarning(newPod)
	if warning != "" && staticPodWarningCache.addIfAbsent(warning) {
		logger.Info(warning, "pod", klog.KObj(newPod))
	}

	return true, v1Pod, nil
}

// tryDecodePodList 从数据中解析 Pod 列表，并逐个应用默认值与校验。
func tryDecodePodList(logger klog.Logger, data []byte, defaultFn defaultFunc) (parsed bool, pods v1.PodList, err error) {
	obj, err := runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), data)
	if err != nil {
		return false, pods, err
	}

	newPods, ok := obj.(*api.PodList)
	// 检查解码结果是否为 Pod 列表。
	if !ok {
		err = fmt.Errorf("invalid pods list: %#v", obj)
		return false, pods, err
	}

	// 逐个应用默认值并校验 Pod。
	for i := range newPods.Items {
		newPod := &newPods.Items[i]
		if newPod.Name == "" {
			return true, pods, fmt.Errorf("invalid pod: name is needed for the pod")
		}
		if err = defaultFn(logger, newPod); err != nil {
			return true, pods, err
		}
		opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&newPod.Spec, nil, &newPod.ObjectMeta, nil)
		if errs := validation.ValidatePodCreate(newPod, opts); len(errs) > 0 {
			err = fmt.Errorf("invalid pod: %v", errs)
			return true, pods, err
		}
		// 静态 Pod 不允许引用其他 API 对象。
		_, resource, err := podutil.HasAPIObjectReference(newPod)
		if err != nil {
			return true, pods, err
		}
		if resource != "" {
			return true, pods, fmt.Errorf("static pods may not reference %s", resource)
		}
	}
	v1Pods := &v1.PodList{}
	if err := k8s_api_v1.Convert_core_PodList_To_v1_PodList(newPods, v1Pods, nil); err != nil {
		return true, pods, err
	}
	return true, *v1Pods, err
}

// getStaticPodPriorityWarning 检查静态 Pod 的优先级配置，返回需要提示给用户的告警信息。
func getStaticPodPriorityWarning(pod *api.Pod) string {
	podSpec := pod.Spec

	// 只处理三种可能导致镜像 Pod 创建失败或被无效驱逐的配置。
	switch {
	// 有 priorityClassName 但没有 priority 时，kubelet 不会使用该优先级类。
	case podSpec.Priority == nil && len(podSpec.PriorityClassName) > 0:
		return "Static pod has priorityClassName and nil priority. Kubelet will not use priorityClassName, and mirror pod creation may fail."

	// 有 priority 但没有 priorityClassName 时，默认优先级类可能不匹配。
	case podSpec.Priority != nil && len(podSpec.PriorityClassName) == 0:
		return "Static pod has priority without priorityClassName. Mirror pod creation may fail if the default priority class doesn't match the given priority."

	// 优先级或优先级类与节点关键优先级不一致时，镜像 Pod 可能被无效驱逐。
	case podSpec.Priority != nil && (*podSpec.Priority != nodeCriticalPriority || podSpec.PriorityClassName != schedulingapi.SystemNodeCritical):
		return fmt.Sprintf("Static pod has a priority other than %d or a priorityClassName other than %s. Mirror pod may be attempted to be evicted from the node ineffectively.", nodeCriticalPriority, schedulingapi.SystemNodeCritical)
	}

	return ""
}
