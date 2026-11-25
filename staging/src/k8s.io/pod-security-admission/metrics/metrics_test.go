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

package metrics

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/pod-security-admission/api"

	"github.com/stretchr/testify/assert"
)

var (
	decisions  = []Decision{DecisionAllow, DecisionDeny}
	modes      = []Mode{ModeEnforce, ModeAudit, ModeWarn}
	operations = []admissionv1.Operation{admissionv1.Create, admissionv1.Update}
	levels     = []api.Level{api.LevelPrivileged, api.LevelBaseline, api.LevelRestricted}

	// 资源类型到预期指标标签值的映射。
	// 指标刻意把各种控制器收敛成同一个 resource 标签，避免标签基数随工作负载类型暴涨。
	resourceExpectations = map[schema.GroupVersionResource]string{
		corev1.SchemeGroupVersion.WithResource("pods"):        "pod",
		appsv1.SchemeGroupVersion.WithResource("deployments"): "controller",
		batchv1.SchemeGroupVersion.WithResource("cronjobs"):   "controller",
	}

	// 策略版本到预期标签值的映射，版本基准为 testVersion。
	// 只有 latest/当前/过去三种取值；比测试版本新的统一归到 future，同样是为了压标签基数。
	versionExpectations = map[string]string{
		"latest": "latest",
		"v1.22":  "v1.22",
		"v1.23":  "v1.23",
		"v1.24":  "future",
	}
	testVersion = api.MajorMinorVersion(1, 23)
)

// 遍历决策/模式/操作/等级/版本/资源的笛卡尔积，确保每个标签组合都能产出预期序列。
func TestRecordEvaluation(t *testing.T) {
	recorder := NewPrometheusRecorder(testVersion)
	registry := testutil.NewFakeKubeRegistry("1.23.0")
	recorder.MustRegister(registry.MustRegister)

	for _, decision := range decisions {
		for _, mode := range modes {
			for _, op := range operations {
				for _, level := range levels {
					for version, expectedVersion := range versionExpectations {
						for resource, expectedResource := range resourceExpectations {
							recorder.RecordEvaluation(decision, levelVersion(level, version), mode, &api.AttributesRecord{
								Resource:  resource,
								Operation: op,
							})

							// privileged 不做任何检查，版本标签统一记成 latest；
							// 否则同一个含义会散成一堆不同的版本序列。
							if level == api.LevelPrivileged {
								expectedVersion = "latest"
							}
							expected := fmt.Sprintf(`
							# HELP pod_security_evaluations_total [ALPHA] Number of policy evaluations that occurred, not counting ignored or exempt requests.
        	            	# TYPE pod_security_evaluations_total counter
							pod_security_evaluations_total{decision="%s",mode="%s",policy_level="%s",policy_version="%s",request_operation="%s",resource="%s",subresource=""} 1
							`, decision, mode, level, expectedVersion, strings.ToLower(string(op)), expectedResource)
							// 预置的零值序列也要算进期望，否则 GatherAndCompare 会漏比。
							expected = expectCachedMetrics("pod_security_evaluations_total", expected)

							assert.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(expected), "pod_security_evaluations_total"))

							recorder.Reset()
						}
					}
				}
			}
		}
	}
}

// 豁免也要计数，才能区分「策略放行了」和「请求根本没进策略引擎」。
func TestRecordExemption(t *testing.T) {
	recorder := NewPrometheusRecorder(testVersion)
	registry := testutil.NewFakeKubeRegistry("1.23.0")
	recorder.MustRegister(registry.MustRegister)

	for _, op := range operations {
		for resource, expectedResource := range resourceExpectations {
			for _, subresource := range []string{"", "ephemeralcontainers"} {
				recorder.RecordExemption(&api.AttributesRecord{
					Resource:    resource,
					Operation:   op,
					Subresource: subresource,
				})

				expected := fmt.Sprintf(`
				# HELP pod_security_exemptions_total [ALPHA] Number of exempt requests, not counting ignored or out of scope requests.
				# TYPE pod_security_exemptions_total counter
				pod_security_exemptions_total{request_operation="%s",resource="%s",subresource="%s"} 1
				`, strings.ToLower(string(op)), expectedResource, subresource)
				expected = expectCachedMetrics("pod_security_exemptions_total", expected)

				assert.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(expected), "pod_security_exemptions_total"))

				recorder.Reset()
			}
		}
	}
}

// 致命和非致命错误用 fatal 标签分开：后者会退回 latest restricted 继续评估，
// 运维需要能把这类「降级但仍在工作」的情况和彻底失败区分开。
func TestRecordError(t *testing.T) {
	recorder := NewPrometheusRecorder(testVersion)
	registry := testutil.NewFakeKubeRegistry("1.23.0")
	recorder.MustRegister(registry.MustRegister)

	for _, fatal := range []bool{true, false} {
		for _, op := range operations {
			for resource, expectedResource := range resourceExpectations {
				recorder.RecordError(fatal, &api.AttributesRecord{
					Resource:  resource,
					Operation: op,
				})

				expected := bytes.NewBufferString(fmt.Sprintf(`
				# HELP pod_security_errors_total [ALPHA] Number of errors preventing normal evaluation. Non-fatal errors may result in the latest restricted profile being used for evaluation.
				# TYPE pod_security_errors_total counter
				pod_security_errors_total{fatal="%t",request_operation="%s",resource="%s",subresource=""} 1
				`, fatal, strings.ToLower(string(op)), expectedResource))

				assert.NoError(t, testutil.GatherAndCompare(registry, expected, "pod_security_errors_total"))

				recorder.Reset()
			}
		}
	}
}

// 测试用例里的版本串都是常量，解析不了就是测试自己写错了，直接 panic 比静默跳过好。
func levelVersion(level api.Level, version string) api.LevelVersion {
	lv := api.LevelVersion{Level: level}
	var err error
	if lv.Version, err = api.ParseVersion(version); err != nil {
		panic(err)
	}
	return lv
}

// 缓存的指标序列应始终存在，未被计数时值为 0。
// 这些序列是 recorder 注册时预热的，断言里必须显式带上 0 值，才能确认没被意外漏掉。
var expectedCachedMetrics = map[string][]string{
	"pod_security_evaluations_total": {
		`pod_security_evaluations_total{decision="allow",mode="enforce",policy_level="privileged",policy_version="latest",request_operation="create",resource="pod",subresource=""}`,
		`pod_security_evaluations_total{decision="allow",mode="enforce",policy_level="privileged",policy_version="latest",request_operation="update",resource="pod",subresource=""}`,
	},
	"pod_security_exemptions_total": {
		`pod_security_exemptions_total{request_operation="create",resource="controller",subresource=""}`,
		`pod_security_exemptions_total{request_operation="create",resource="pod",subresource=""}`,
		`pod_security_exemptions_total{request_operation="update",resource="controller",subresource=""}`,
		`pod_security_exemptions_total{request_operation="update",resource="pod",subresource=""}`,
	},
}

// 把预置的零值序列补进期望文本，并去掉缩进差异，保证排序比较不受格式影响。
func expectCachedMetrics(metricName, expected string) string {
	expectations := strings.Split(strings.TrimSpace(expected), "\n")
	for i, expectation := range expectations {
		expectations[i] = strings.TrimSpace(expectation) // 空白会影响排序。
	}
	for _, cached := range expectedCachedMetrics[metricName] {
		expectations = addZeroExpectation(expectations, cached)
	}
	sort.Strings(expectations[:len(expectations)-1])
	return "\n" + strings.Join(expectations, "\n") + "\n"
}

// addZeroExpectation 在期望中尚未包含对应标签组合时补入值为 0 的样本。
func addZeroExpectation(currentExpectations []string, mixin string) []string {
	for _, current := range currentExpectations {
		if strings.HasPrefix(current, mixin) {
			return currentExpectations // 该组合已有非零计数期望，无需再补 0。
		}
	}
	return append(currentExpectations, fmt.Sprintf("%s 0", mixin))
}
