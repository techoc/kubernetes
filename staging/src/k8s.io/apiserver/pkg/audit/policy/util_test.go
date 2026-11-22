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
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apiserver/pkg/apis/audit"
)

// TestInvertStages 覆盖 InvertStages 的补集语义：返回所有未被传入的审计阶段。
// 关键边界是空输入（返回全部阶段）与全量输入（返回空集），顺序不重要，
// 因此用 ElementsMatch 而非 Equal 做断言。
func TestInvertStages(t *testing.T) {
	for _, test := range []struct {
		desc           string
		stages         []audit.Stage
		expectedStages []audit.Stage
	}{
		{
			desc: "should remove one",
			stages: []audit.Stage{
				audit.StageResponseStarted,
			},
			expectedStages: []audit.Stage{
				audit.StageRequestReceived,
				audit.StageResponseComplete,
				audit.StagePanic,
			},
		},
		{
			desc: "should remove both",
			stages: []audit.Stage{
				audit.StageResponseStarted,
				audit.StageRequestReceived,
			},
			expectedStages: []audit.Stage{
				audit.StageResponseComplete,
				audit.StagePanic,
			},
		},
		{
			desc:   "should remove none",
			stages: []audit.Stage{},
			expectedStages: []audit.Stage{
				audit.StageResponseComplete,
				audit.StageResponseStarted,
				audit.StageRequestReceived,
				audit.StagePanic,
			},
		},
		{
			desc: "should remove all",
			stages: []audit.Stage{
				audit.StageResponseComplete,
				audit.StageResponseStarted,
				audit.StageRequestReceived,
				audit.StagePanic,
			},
			expectedStages: []audit.Stage{},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			e := InvertStages(test.stages)
			require.ElementsMatch(t, e, test.expectedStages)
		})
	}
}
