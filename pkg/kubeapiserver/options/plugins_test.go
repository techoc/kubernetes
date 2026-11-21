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

package options

import (
	"strings"
	"testing"
)

// TestAdmissionPluginOrder 校验准入插件的顺序以变更类（策略、webhook）、
// 校验类（策略、webhook）、配额、拒绝结尾。
func TestAdmissionPluginOrder(t *testing.T) {
	// 插件顺序决定了执行语义：必须先变更再校验，最后做配额与拒绝检查。
	allplugins := strings.Join(AllOrderedPlugins, ",")
	expectSuffix := ",MutatingAdmissionPolicy,MutatingAdmissionWebhook,ValidatingAdmissionPolicy,ValidatingAdmissionWebhook,ResourceQuota,AlwaysDeny"
	if !strings.HasSuffix(allplugins, expectSuffix) {
		t.Fatalf("AllOrderedPlugins must end with ...%s", expectSuffix)
	}
}
