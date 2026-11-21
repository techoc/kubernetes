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

package job

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
)

const (
	// Indexed Job 的 completions 软上限：超过它且并发也很大时给出警告。
	completionsSoftLimit = 100_000
	// 触发上述警告所需的 parallelism 软上限。
	parallelismSoftLimitForUnlimitedCompletions = 10_000
)

// WarningsForJobSpec 为 JobSpec 中的字段生成警告信息。
// 警告不同于校验错误：不会拒绝请求，只是提醒用户可能遇到的运行期问题。
func WarningsForJobSpec(ctx context.Context, path *field.Path, spec, oldSpec *batch.JobSpec) []string {
	var warnings []string
	// 只对 Indexed 完成模式的 Job 检查：该模式下每个 index 的完成状态都要跟踪。
	if spec.CompletionMode != nil && *spec.CompletionMode == batch.IndexedCompletion {
		completions := ptr.Deref(spec.Completions, 0)
		parallelism := ptr.Deref(spec.Parallelism, 0)
		// completions 和 parallelism 同时超过软上限时，大量 index 失败可能导致
		// completedIndexes 无法被可靠跟踪。
		if completions > completionsSoftLimit && parallelism > parallelismSoftLimitForUnlimitedCompletions {
			msg := "In Indexed Jobs with a number of completions higher than 10^5 and a parallelism higher than 10^4, Kubernetes might not be able to track completedIndexes when a big number of indexes fail"
			warnings = append(warnings, fmt.Sprintf("%s: %s", path, msg))
		}
	}
	// 更新场景下取出旧 PodTemplate，以便 pod 层的警告逻辑对比新旧字段，
	// 避免对“原本就在用”的弃用字段重复告警。
	var oldPodTemplate *core.PodTemplateSpec
	if oldSpec != nil {
		oldPodTemplate = &oldSpec.Template
	}
	// Job 内嵌了 PodTemplateSpec，pod 层的弃用/不规范检查同样适用。
	warnings = append(warnings, pod.GetWarningsForPodTemplate(ctx, path.Child("template"), &spec.Template, oldPodTemplate)...)
	return warnings
}
