/*
Copyright 2019 The Kubernetes Authors.

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

package helper

import (
	fwk "k8s.io/kube-scheduler/framework"
)

// DefaultNormalizeScore 生成一个 Normalize Score 函数，把分数从 [0, max(scores)]
// 归一化到 [0, maxPriority]。reverse 为 true 时，用 maxPriority 减去分数以反转排名。
// 注意：输入分数一律假定为非负整数。
func DefaultNormalizeScore(maxPriority int64, reverse bool, scores fwk.NodeScoreList) *fwk.Status {
	var maxCount int64
	for i := range scores {
		if scores[i].Score > maxCount {
			maxCount = scores[i].Score
		}
	}

	// 全零分数无法做分母；反转时统一给满分，否则保持零分。
	if maxCount == 0 {
		if reverse {
			for i := range scores {
				scores[i].Score = maxPriority
			}
		}
		return nil
	}

	for i := range scores {
		score := scores[i].Score

		score = maxPriority * score / maxCount
		if reverse {
			score = maxPriority - score
		}

		scores[i].Score = score
	}
	return nil
}
