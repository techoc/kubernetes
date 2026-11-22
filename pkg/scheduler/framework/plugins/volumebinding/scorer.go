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

package volumebinding

import (
	"math"

	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
)

// classResourceMap 把存储类名映射到对应的存储资源信息。
type classResourceMap map[string]*StorageResource

// volumeCapacityScorer 根据各存储类的资源信息计算节点得分。
type volumeCapacityScorer func(classResourceMap) int64

// buildScorerFunction 根据给定的函数形状构造 volumeCapacityScorer。
func buildScorerFunction(scoringFunctionShape helper.FunctionShape) volumeCapacityScorer {
	rawScoringFunction := helper.BuildBrokenLinearFunction(scoringFunctionShape)
	f := func(requested, capacity int64) int64 {
		// 容量为 0 或请求超过容量都按最大利用率计分：此时应给最低分。
		if capacity == 0 || requested > capacity {
			return rawScoringFunction(maxUtilization)
		}

		return rawScoringFunction(requested * maxUtilization / capacity)
	}
	return func(classResources classResourceMap) int64 {
		var nodeScore int64
		// alpha 阶段所有存储类权重相同，直接用类的数量作为权重和。
		weightSum := len(classResources)
		if weightSum == 0 {
			return 0
		}
		for _, resource := range classResources {
			classScore := f(resource.Requested, resource.Capacity)
			nodeScore += classScore
		}
		return int64(math.Round(float64(nodeScore) / float64(weightSum)))
	}
}
