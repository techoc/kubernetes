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

package exec

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/utils/ptr"
)

// mockExpiryGauge 记录最近一次 Set 的时间点，用于断言过期时间指标。
type mockExpiryGauge struct {
	v *time.Time
}

func (m *mockExpiryGauge) Set(t *time.Time) {
	m.v = t
}

// TestCertificateExpirationTracker 验证证书过期时间指标取所有认证器中最小的那个到期时间。
// 子测试之间有状态依赖（共享 tracker），因此必须串行执行。
func TestCertificateExpirationTracker(t *testing.T) {
	now := time.Now()
	mockMetric := &mockExpiryGauge{}

	tracker := &certificateExpirationTracker{
		m:         map[*Authenticator]time.Time{},
		metricSet: mockMetric.Set,
	}

	firstAuthenticator := &Authenticator{}
	secondAuthenticator := &Authenticator{}
	for _, tc := range []struct {
		desc string
		auth *Authenticator
		time time.Time
		want *time.Time
	}{
		{
			desc: "ttl for one authenticator",
			auth: firstAuthenticator,
			time: now.Add(time.Minute * 10),
			want: ptr.To(now.Add(time.Minute * 10)),
		},
		{
			desc: "second authenticator shorter ttl",
			auth: secondAuthenticator,
			time: now.Add(time.Minute * 5),
			want: ptr.To(now.Add(time.Minute * 5)),
		},
		{
			desc: "update shorter to be longer",
			auth: secondAuthenticator,
			time: now.Add(time.Minute * 15),
			want: ptr.To(now.Add(time.Minute * 10)),
		},
		{
			desc: "update shorter to be zero time",
			auth: firstAuthenticator,
			time: time.Time{},
			want: ptr.To(now.Add(time.Minute * 15)),
		},
		{
			desc: "update last to be zero time records nil",
			auth: secondAuthenticator,
			time: time.Time{},
			want: nil,
		},
	} {
		// Must run in series as the tests build off each other.
		t.Run(tc.desc, func(t *testing.T) {
			tracker.set(tc.auth, tc.time)
			if mockMetric.v != nil && tc.want != nil {
				if !mockMetric.v.Equal(*tc.want) {
					t.Errorf("got: %s; want: %s", mockMetric.v, tc.want)
				}
			} else if mockMetric.v != tc.want {
				t.Errorf("got: %s; want: %s", mockMetric.v, tc.want)
			}
		})
	}
}

// mockCallsMetric 记录一次插件调用的退出码与错误类型。
type mockCallsMetric struct {
	exitCode  int
	errorType string
}

// mockCallsMetricCounter 按调用顺序累积插件调用指标，便于整体比对。
type mockCallsMetricCounter struct {
	calls []mockCallsMetric
}

func (f *mockCallsMetricCounter) Increment(exitCode int, errorType string) {
	f.calls = append(f.calls, mockCallsMetric{exitCode: exitCode, errorType: errorType})
}

// TestCallsMetric 验证执行插件时上报的退出码与错误分类，包括插件根本不存在的特殊情况。
func TestCallsMetric(t *testing.T) {
	const (
		goodOutput = `{
			"kind": "ExecCredential",
			"apiVersion": "client.authentication.k8s.io/v1beta1",
			"status": {
				"token": "foo-bar"
			}
		}`
	)

	callsMetricCounter := &mockCallsMetricCounter{}
	originalExecPluginCalls := metrics.ExecPluginCalls
	t.Cleanup(func() { metrics.ExecPluginCalls = originalExecPluginCalls })
	metrics.ExecPluginCalls = callsMetricCounter

	exitCodes := []int{0, 1, 2, 0}
	var wantCallsMetrics []mockCallsMetric
	for _, exitCode := range exitCodes {
		c := api.ExecConfig{
			Command:    "./testdata/test-plugin.sh",
			APIVersion: "client.authentication.k8s.io/v1beta1",
			Env: []api.ExecEnvVar{
				{Name: "TEST_EXIT_CODE", Value: fmt.Sprintf("%d", exitCode)},
				{Name: "TEST_OUTPUT", Value: goodOutput},
			},
			InteractiveMode: api.IfAvailableExecInteractiveMode,
		}

		a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &c, nil)
		if err != nil {
			t.Fatal(err)
		}
		a.stderr = io.Discard

		// Run refresh creds twice so that our test validates that the metrics are set correctly twice
		// in a row with the same authenticator.
		refreshCreds := func() {
			if err := a.refreshCredsLocked(); (err == nil) != (exitCode == 0) {
				if err != nil {
					t.Fatalf("wanted no error, but got %q", err.Error())
				} else {
					t.Fatal("wanted error, but got nil")
				}
			}
			mockCallsMetric := mockCallsMetric{exitCode: exitCode, errorType: "no_error"}
			if exitCode != 0 {
				mockCallsMetric.errorType = "plugin_execution_error"
			}
			wantCallsMetrics = append(wantCallsMetrics, mockCallsMetric)
		}
		refreshCreds()
		refreshCreds()
	}

	// Run some iterations of the authenticator where the exec plugin fails to run to test special
	// metric values.
	// 覆盖插件路径不存在的三种写法（无斜杠、相对路径、绝对路径），它们都应归类为 plugin_not_found_error。
	refreshCreds := func(command string) {
		c := api.ExecConfig{
			Command:         command,
			APIVersion:      "client.authentication.k8s.io/v1beta1",
			InteractiveMode: api.IfAvailableExecInteractiveMode,
		}
		a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &c, nil)
		if err != nil {
			t.Fatal(err)
		}
		a.stderr = io.Discard
		if err := a.refreshCredsLocked(); err == nil {
			t.Fatal("expected the authenticator to fail because the plugin does not exist")
		}
		wantCallsMetrics = append(wantCallsMetrics, mockCallsMetric{exitCode: 1, errorType: "plugin_not_found_error"})
	}
	refreshCreds("does not exist without path slashes")
	refreshCreds("./does/not/exist/with/relative/path")
	refreshCreds("/does/not/exist/with/absolute/path")

	callsMetricComparer := cmp.Comparer(func(a, b mockCallsMetric) bool {
		return a.exitCode == b.exitCode && a.errorType == b.errorType
	})
	actualCallsMetrics := callsMetricCounter.calls
	if diff := cmp.Diff(wantCallsMetrics, actualCallsMetrics, callsMetricComparer); diff != "" {
		t.Fatalf("got unexpected metrics calls; -want, +got:\n%s", diff)
	}
}

// mockPolicyCallsMetric 记录一次策略判断的结果（allowed/denied）。
type mockPolicyCallsMetric struct {
	status string
}

// mockPolicyCallsMetricCounter 按顺序累积策略调用指标。
type mockPolicyCallsMetricCounter struct {
	policyCalls []mockPolicyCallsMetric
}

func (f *mockPolicyCallsMetricCounter) Increment(status string) {
	f.policyCalls = append(f.policyCalls, mockPolicyCallsMetric{status: status})
}

// TestPolicyCallsMetric 验证 AllowAll/DenyAll/Allowlist 等插件策略的判定结果与指标上报一致。
func TestPolicyCallsMetric(t *testing.T) {
	const (
		goodOutput = `{
			"kind": "ExecCredential",
			"apiVersion": "client.authentication.k8s.io/v1beta1",
			"status": {
				"token": "foo-bar"
			}
		}`
	)

	policyCallsMetricCounter := &mockPolicyCallsMetricCounter{}
	originalExecPluginPolicyCalls := metrics.ExecPluginPolicyCalls
	t.Cleanup(func() { metrics.ExecPluginPolicyCalls = originalExecPluginPolicyCalls })
	metrics.ExecPluginPolicyCalls = policyCallsMetricCounter

	tests := []struct {
		wantDenied bool
		policy     api.PluginPolicy
	}{
		{
			wantDenied: false,
			policy:     api.PluginPolicy{PolicyType: api.PluginPolicyAllowAll},
		},
		{
			wantDenied: true,
			policy:     api.PluginPolicy{PolicyType: api.PluginPolicyDenyAll},
		},
		{
			wantDenied: false,
			policy: api.PluginPolicy{
				PolicyType: api.PluginPolicyAllowlist,
				Allowlist: []api.AllowlistEntry{
					{
						Command: "foobar",
					},
					{
						Command: "testdata/test-plugin.sh",
					},
				},
			},
		},
		{
			wantDenied: true,
			policy: api.PluginPolicy{
				PolicyType: api.PluginPolicyAllowlist,
				Allowlist: []api.AllowlistEntry{
					{Command: "foobar"},
					{Command: "baz"},
				},
			},
		},
	}

	var wantPolicyCallsMetrics []mockPolicyCallsMetric
	for _, test := range tests {
		c := api.ExecConfig{
			Command:    "./testdata/test-plugin.sh",
			APIVersion: "client.authentication.k8s.io/v1beta1",
			Env: []api.ExecEnvVar{
				{Name: "TEST_EXIT_CODE", Value: fmt.Sprintf("%d", 0)},
				{Name: "TEST_OUTPUT", Value: goodOutput},
			},
			InteractiveMode: api.IfAvailableExecInteractiveMode,
			PluginPolicy:    test.policy,
		}

		a, err := newAuthenticator(newCache(), func(_ int) bool { return false }, &c, nil)
		if err != nil {
			t.Fatal(err)
		}
		a.stderr = io.Discard

		err = a.refreshCredsLocked()
		if err != nil && !test.wantDenied {
			t.Fatalf("wanted no error, but got %q", err.Error())
		}
		if err == nil && test.wantDenied {
			t.Fatal("wanted error, but got nil")
		}

		mockPolicyCallsMetric := mockPolicyCallsMetric{status: "allowed"}
		if test.wantDenied {
			mockPolicyCallsMetric.status = "denied"
		}

		wantPolicyCallsMetrics = append(wantPolicyCallsMetrics, mockPolicyCallsMetric)
	}

	policyCallsMetricComparer := cmp.Comparer(func(a, b mockPolicyCallsMetric) bool {
		return a.status == b.status
	})

	actualPolicyCallsMetrics := policyCallsMetricCounter.policyCalls
	if diff := cmp.Diff(wantPolicyCallsMetrics, actualPolicyCallsMetrics, policyCallsMetricComparer); diff != "" {
		t.Fatalf("got unexpected metrics calls; -want, +got:\n%s", diff)
	}
}
