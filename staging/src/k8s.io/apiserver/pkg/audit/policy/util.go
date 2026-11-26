/*
Copyright 2018 The Kubernetes Authors.

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

package policy

import (
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/apis/audit"
)

// AllStages 返回所有可能的阶段
//
// 用于把“省略阶段”取反：策略里写 OmitStages 时，其余阶段才是需要记录的。
func AllStages() sets.String {
	return sets.NewString(
		string(audit.StageRequestReceived),
		string(audit.StageResponseStarted),
		string(audit.StageResponseComplete),
		string(audit.StagePanic),
	)
}

// AllLevels 返回所有可能的级别
func AllLevels() sets.String {
	return sets.NewString(
		string(audit.LevelNone),
		string(audit.LevelMetadata),
		string(audit.LevelRequest),
		string(audit.LevelRequestResponse),
	)
}

// InvertStages 从所有阶段中减去给定的阶段数组
//
// 返回值保持集合的排序顺序，保证同一输入得到稳定输出。
func InvertStages(stages []audit.Stage) []audit.Stage {
	s := ConvertStagesToStrings(stages)
	a := AllStages()
	a.Delete(s...)
	return ConvertStringSetToStages(a)
}

// ConvertStagesToStrings 将阶段数组转换为字符串数组
func ConvertStagesToStrings(stages []audit.Stage) []string {
	s := make([]string, len(stages))
	for i, stage := range stages {
		s[i] = string(stage)
	}
	return s
}

// ConvertStringSetToStages 将字符串集合转换为阶段数组
func ConvertStringSetToStages(set sets.String) []audit.Stage {
	stages := make([]audit.Stage, len(set))
	for i, stage := range set.List() {
		stages[i] = audit.Stage(stage)
	}
	return stages
}
