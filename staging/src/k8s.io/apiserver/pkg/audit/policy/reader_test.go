/*
Copyright 2017 The Kubernetes Authors.

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
	"os"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/apis/audit"

	// import to call webhook's init() function to register audit.Policy to schema
	_ "k8s.io/apiserver/plugin/pkg/audit/webhook"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// policyDefPattern 是正常策略的模板，{version} 由各用例替换为具体 API 版本。
const policyDefPattern = `
apiVersion: audit.k8s.io/{version}
kind: Policy
rules:
  - level: None
    nonResourceURLs:
      - /healthz*
      - /version
  - level: RequestResponse
    users: ["tim"]
    userGroups: ["testers", "developers"]
    verbs: ["patch", "delete", "create"]
    resources:
      - group: ""
      - group: "rbac.authorization.k8s.io"
        resources: ["clusterroles", "clusterrolebindings"]
    namespaces: ["default", "kube-system"]
  - level: Metadata
`

// policyWithNoVersionOrKind 用于验证缺少 apiVersion/kind 时加载失败。
const policyWithNoVersionOrKind = `
rules:
  - level: None
    nonResourceURLs:
      - /healthz*
      - /version
  - level: RequestResponse
    users: ["tim"]
    userGroups: ["testers", "developers"]
    verbs: ["patch", "delete", "create"]
    resources:
      - group: ""
      - group: "rbac.authorization.k8s.io"
        resources: ["clusterroles", "clusterrolebindings"]
    namespaces: ["default", "kube-system"]
  - level: Metadata
`

// policyWithUnknownField 中的 verbs 缩进错误，严格解码会失败，用于验证宽松回退。
const policyWithUnknownField = `
apiVersion: audit.k8s.io/v1
kind: Policy
rules:
- level: None
  resources:
  - group: coordination.k8s.io
    resources:
    - "leases"
    verbs: ["watch", "get", "list"] # invalid indentation on verbs
`

// expectedPolicy 是 policyDefPattern 解析后的内部版本期望值。
var expectedPolicy = &audit.Policy{
	Rules: []audit.PolicyRule{{
		Level:           audit.LevelNone,
		NonResourceURLs: []string{"/healthz*", "/version"},
	}, {
		Level:      audit.LevelRequestResponse,
		Users:      []string{"tim"},
		UserGroups: []string{"testers", "developers"},
		Verbs:      []string{"patch", "delete", "create"},
		Resources: []audit.GroupResources{{}, {
			Group:     "rbac.authorization.k8s.io",
			Resources: []string{"clusterroles", "clusterrolebindings"},
		}},
		Namespaces: []string{"default", "kube-system"},
	}, {
		Level: audit.LevelMetadata,
	}},
}

// TestParser 验证完整策略文件的解析结果与预期对象完全一致。
func TestParser(t *testing.T) {
	policyDef := strings.Replace(policyDefPattern, "{version}", "v1", 1)
	f, err := writePolicy(t, policyDef)
	require.NoError(t, err)
	defer os.Remove(f)

	policy, err := LoadPolicyFromFile(f)
	require.NoError(t, err)

	assert.Len(t, policy.Rules, 3) // Sanity check.
	if !reflect.DeepEqual(policy, expectedPolicy) {
		t.Errorf("Unexpected policy! Diff:\n%s", cmp.Diff(policy, expectedPolicy))
	}
}

// TestParsePolicyWithNoVersionOrKind 验证缺少 apiVersion/kind 的策略被拒绝，
// 避免因无法确定版本而误用默认类型。
func TestParsePolicyWithNoVersionOrKind(t *testing.T) {
	f, err := writePolicy(t, policyWithNoVersionOrKind)
	require.NoError(t, err)
	defer os.Remove(f)

	_, err = LoadPolicyFromFile(f)
	assert.ErrorContains(t, err, "unknown group version field")
}

// TestParsePolicyWithUnknownField 验证严格解码失败（未知字段/缩进错误）后会
// 退回宽松解码并成功加载，这是向前兼容行为。
func TestParsePolicyWithUnknownField(t *testing.T) {
	f, err := writePolicy(t, policyWithUnknownField)
	require.NoError(t, err)
	defer os.Remove(f)

	_, err = LoadPolicyFromFile(f)
	require.NoError(t, err)
}

// TestPolicyCntCheck 验证空规则集的策略（包括空文件）会被拒绝。
func TestPolicyCntCheck(t *testing.T) {
	var testCases = []struct {
		caseName, policy string
	}{
		{
			"policyWithNoRule",
			`apiVersion: audit.k8s.io/v1
kind: Policy`,
		},
		{"emptyPolicyFile", ""},
	}

	for _, tc := range testCases {
		f, err := writePolicy(t, tc.policy)
		require.NoError(t, err)
		defer os.Remove(f)

		_, err = LoadPolicyFromFile(f)
		assert.Errorf(t, err, "loaded illegal policy with 0 rules from testCase %s", tc.caseName)
	}
}

// writePolicy 把策略内容写入临时文件并返回路径，测试结束时由调用方删除。
func writePolicy(t *testing.T, policy string) (string, error) {
	f, err := os.CreateTemp("", "policy.yaml")
	require.NoError(t, err)

	_, err = f.WriteString(policy)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return f.Name(), nil
}
