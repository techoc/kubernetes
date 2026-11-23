/*
Copyright 2022 The Kubernetes Authors.

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

	"github.com/stretchr/testify/assert"
)

// TestValidChecks ensures that all registered checks are valid.
// 除了结构合法性，还要确认每个 override 都指向真实存在的检查——
// 写错 ID 的 override 不会报错，只会在运行时悄悄失效。
func TestValidChecks(t *testing.T) {
	allChecks := append(DefaultChecks(), ExperimentalChecks()...)

	assert.NoError(t, validateChecks(allChecks))

	// Ensure that all overrides map to existing checks.
	// 先把 ID 收成集合，再核对每个 override，避免 O(n²) 的逐个查找。
	allIDs := map[CheckID]bool{}
	for _, check := range allChecks {
		allIDs[check.ID] = true
	}
	for _, check := range allChecks {
		for _, c := range check.Versions {
			for _, override := range c.OverrideCheckIDs {
				assert.Contains(t, allIDs, override, "check %s overrides non-existent check %s", check.ID, override)
			}
		}
	}
}
