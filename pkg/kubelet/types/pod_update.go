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

package types

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/scheduling"
)

// 本包使用的注解键。
const (
	ConfigSourceAnnotationKey    = "kubernetes.io/config.source"
	ConfigMirrorAnnotationKey    = v1.MirrorPodAnnotationKey
	ConfigFirstSeenAnnotationKey = "kubernetes.io/config.seen"
	ConfigHashAnnotationKey      = "kubernetes.io/config.hash"
)

// PodOperation 表示将对 Pod 配置执行的操作类型。
type PodOperation int

// 以下常量标识可对 Pod 配置执行的操作。
const (
	// ADD 表示该来源中新增的 Pod。
	ADD PodOperation = iota
	// DELETE 表示从该来源优雅删除的 Pod。
	DELETE
	// REMOVE 表示已从该来源移除的 Pod。
	REMOVE
	// UPDATE 表示该来源中更新过的 Pod。
	UPDATE
	// RECONCILE 表示该来源中状态异常的 Pod，kubelet 应与其对齐状态。
	RECONCILE
)

// 以下常量标识 Pod 的来源。
const (
	// FileSource 表示来自配置文件的更新。
	FileSource = "file"
	// HTTPSource 表示来自网页轮询的更新。
	HTTPSource = "http"
	// ApiserverSource 表示来自 Kubernetes API Server 的更新。
	ApiserverSource = "api"
	// AllSource 表示来自所有来源的更新。
	AllSource = "*"
)

// NamespaceDefault 是默认命名空间的字符串表示。
const NamespaceDefault = metav1.NamespaceDefault

// PodUpdate 定义在 channel 上传递的操作：可通过大小为 1 的数组增删单个 Pod；
// 也可用 SET 语义把该来源的系统状态整体重置为 Pods 指定的集合，
// 清空全部 Pod 则传入空集合。
//
// Pods 不应为 nil，而应指向空切片：两者功能相近，但空切片便于单元测试
// 校验生成的 PodUpdate 是否正确。
type PodUpdate struct {
	Pods   []*v1.Pod
	Op     PodOperation
	Source string
}

// GetValidatedSources 校验并规范化传入的 Pod 来源列表。
// 通配来源 "*" 展开为 file/http/api 三种；空字符串被忽略；
// 未知来源返回错误，避免配置错误被静默吞掉。
func GetValidatedSources(sources []string) ([]string, error) {
	validated := make([]string, 0, len(sources))
	for _, source := range sources {
		switch source {
		case AllSource:
			return []string{FileSource, HTTPSource, ApiserverSource}, nil
		case FileSource, HTTPSource, ApiserverSource:
			validated = append(validated, source)
		// 空来源视为未配置，跳过而不是报错。
		case "":
			// 空来源跳过即可。
		default:
			return []string{}, fmt.Errorf("unknown pod source %q", source)
		}
	}
	return validated, nil
}

// GetPodSource 从 Pod 注解中读取来源；缺少注解时返回错误。
func GetPodSource(pod *v1.Pod) (string, error) {
	if pod.Annotations != nil {
		if source, ok := pod.Annotations[ConfigSourceAnnotationKey]; ok {
			return source, nil
		}
	}
	return "", fmt.Errorf("cannot get source of pod %q", pod.UID)
}

// SyncPodType 对 Pod 更新进行分类，例如创建、更新。
type SyncPodType int

const (
	// SyncPodSync 表示同步 Pod 以确保其达到期望状态。
	SyncPodSync SyncPodType = iota
	// SyncPodUpdate 表示 Pod 从来源侧被更新。
	SyncPodUpdate
	// SyncPodCreate 表示 Pod 从来源侧被创建。
	SyncPodCreate
	// SyncPodKill 表示 Pod 不应有运行中的容器；
	// 如此停止的 Pod 可能因配置变更在未来被重新启动。
	SyncPodKill
)

// String 返回 SyncPodType 的可读名称，用于日志与事件输出。
func (sp SyncPodType) String() string {
	switch sp {
	case SyncPodCreate:
		return "create"
	case SyncPodUpdate:
		return "update"
	case SyncPodSync:
		return "sync"
	case SyncPodKill:
		return "kill"
	default:
		return "unknown"
	}
}

// IsMirrorPod 判断 Pod 是否为镜像 Pod，即静态 Pod 在 API Server 中的投影。
func IsMirrorPod(pod *v1.Pod) bool {
	if pod.Annotations == nil {
		return false
	}
	_, ok := pod.Annotations[ConfigMirrorAnnotationKey]
	return ok
}

// IsStaticPod 判断 Pod 是否来自非 API Server 的来源（文件或 HTTP），即静态 Pod。
func IsStaticPod(pod *v1.Pod) bool {
	// 只有能明确识别为 file/http 来源的 Pod 才算静态 Pod。
	source, err := GetPodSource(pod)
	return err == nil && source != ApiserverSource
}

// IsCriticalPod 判断 Pod 是否为关键 Pod：静态 Pod、镜像 Pod，
// 或优先级达到系统关键级别；关键 Pod 在资源紧张时最后被驱逐或抢占。
func IsCriticalPod(pod *v1.Pod) bool {
	// 静态 Pod 与镜像 Pod 承载节点核心组件，天然视为关键 Pod。
	if IsStaticPod(pod) {
		return true
	}
	if IsMirrorPod(pod) {
		return true
	}
	if pod.Spec.Priority != nil && IsCriticalPodBasedOnPriority(*pod.Spec.Priority) {
		return true
	}
	return false
}

// Preemptable 判断 preemptor 能否抢占 preemptee：
// 关键 Pod 可抢占非关键 Pod，否则比较两者优先级。
func Preemptable(preemptor, preemptee *v1.Pod) bool {
	if IsCriticalPod(preemptor) && !IsCriticalPod(preemptee) {
		return true
	}
	if (preemptor != nil && preemptor.Spec.Priority != nil) &&
		(preemptee != nil && preemptee.Spec.Priority != nil) {
		// 双方都有显式优先级时，按数值大小决定。
		return *(preemptor.Spec.Priority) > *(preemptee.Spec.Priority)
	}

	return false
}

// IsCriticalPodBasedOnPriority 根据优先级数值判断是否达到系统关键优先级。
func IsCriticalPodBasedOnPriority(priority int32) bool {
	return priority >= scheduling.SystemCriticalPriority
}

// IsNodeCriticalPod 判断 Pod 是否为 system-node-critical 级别的节点关键 Pod。
func IsNodeCriticalPod(pod *v1.Pod) bool {
	return IsCriticalPod(pod) && (pod.Spec.PriorityClassName == scheduling.SystemNodeCritical)
}

// HasRestartableInitContainer 判断 Pod 是否包含可重启的边车式 init 容器。
func HasRestartableInitContainer(pod *v1.Pod) bool {
	for _, container := range pod.Spec.InitContainers {
		if podutil.IsRestartableInitContainer(&container) {
			return true
		}
	}
	return false
}
