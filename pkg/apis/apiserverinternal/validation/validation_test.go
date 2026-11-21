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

package validation

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/apiserverinternal"
	"k8s.io/utils/ptr"
)

// TestValidateServerStorageVersion 验证单个 ServerStorageVersion 的字段校验。
// 覆盖 APIServerID 格式、版本格式、encodingVersion 可解码性、servedVersions 可解码性等约束。
func TestValidateServerStorageVersion(t *testing.T) {
	cases := []struct {
		ssv         apiserverinternal.ServerStorageVersion
		expectedErr string
	}{{
		// APIServerID 以 "-" 开头，不符合 DNS 子域规则。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "-fea",
			EncodingVersion:   "v1alpha1",
			DecodableVersions: []string{"v1alpha1", "v1"},
			ServedVersions:    []string{"v1alpha1", "v1"},
		},
		expectedErr: "apiServerID: Invalid value",
	}, {
		// decodableVersions 缺少 encodingVersion，属于逻辑约束错误。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1alpha1",
			DecodableVersions: []string{"v1beta1", "v1"},
			ServedVersions:    []string{"v1beta1", "v1"},
		},
		expectedErr: "decodableVersions must include encodingVersion",
	}, {
		// decodableVersions 中出现非法版本 "-fea"。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1alpha1",
			DecodableVersions: []string{"v1alpha1", "v1", "-fea"},
			ServedVersions:    []string{"v1alpha1", "v1", "-fea"},
		},
		expectedErr: "decodableVersions[2]: Invalid value",
	}, {
		// 全字段合法的基准用例。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1alpha1",
			DecodableVersions: []string{"v1alpha1", "v1"},
			ServedVersions:    []string{"v1alpha1", "v1"},
		},
		expectedErr: "",
	}, {
		// 与上一用例重复，重复本身不报错。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1alpha1",
			DecodableVersions: []string{"v1alpha1", "v1"},
			ServedVersions:    []string{"v1alpha1", "v1"},
		},
		expectedErr: "",
	}, {
		// 带 group 前缀的合法版本 mygroup.com/v2。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "mygroup.com/v2",
			DecodableVersions: []string{"v1alpha1", "v1", "mygroup.com/v2"},
			ServedVersions:    []string{"v1alpha1", "v1", "mygroup.com/v2"},
		},
		expectedErr: "",
	}, {
		// servedVersions 中 "/v3" 的 group 部分为空。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1alpha1",
			DecodableVersions: []string{"v1alpha1", "v1"},
			ServedVersions:    []string{"/v3"},
		},
		expectedErr: `[].servedVersions[0]: Invalid value: "/v3": group part: must be non-empty`,
	}, {
		// decodableVersions 中 "/v3" 的 group 部分为空。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "mygroup.com/v2",
			DecodableVersions: []string{"mygroup.com/v2", "/v3"},
			ServedVersions:    []string{"mygroup.com/v2", "/v3"},
		},
		expectedErr: `[].decodableVersions[1]: Invalid value: "/v3": group part: must be non-empty`,
	}, {
		// servedVersions 中 "mygroup.com/" 的 version 部分为空。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "mygroup.com/v2",
			DecodableVersions: []string{"mygroup.com/v2", "/v3"},
			ServedVersions:    []string{"mygroup.com/"},
		},
		expectedErr: `[].servedVersions[0]: Invalid value: "mygroup.com/": version part: must be non-empty`,
	}, {
		// decodableVersions 中 "mygroup.com/" 的 version 部分为空。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "mygroup.com/v2",
			DecodableVersions: []string{"mygroup.com/v2", "mygroup.com/"},
			ServedVersions:    []string{"mygroup.com/v2", "mygroup.com/"},
		},
		expectedErr: `[].decodableVersions[1]: Invalid value: "mygroup.com/": version part: must be non-empty`,
	}, {
		// encodingVersion 本身 "/v3" 的 group 部分为空。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "/v3",
			DecodableVersions: []string{"mygroup.com/v2", "/v3"},
			ServedVersions:    []string{"mygroup.com/v2", "/v3"},
		},
		expectedErr: `[].encodingVersion: Invalid value: "/v3": group part: must be non-empty`,
	}, {
		// group 段含下划线，不符合 RFC 1123 子域规则。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1", "mygroup_com/v2"},
			ServedVersions:    []string{"v1", "mygroup_com/v2"},
		},
		expectedErr: `[].decodableVersions[1]: Invalid value: "mygroup_com/v2": group part: a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`,
	}, {
		// servedVersions 的 group 段同样不允许下划线。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1", "mygroup.com/v2"},
			ServedVersions:    []string{"v1", "mygroup_com/v2"},
		},
		expectedErr: `[].servedVersions[1]: Invalid value: "mygroup_com/v2": group part: a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`,
	}, {
		// version 段以下划线结尾，不符合 DNS-1035 标签规则。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1", "mygroup.com/v2_"},
			ServedVersions:    []string{"v1", "mygroup.com/v2_"},
		},
		expectedErr: `[].decodableVersions[1]: Invalid value: "mygroup.com/v2_": version part: a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')`,
	}, {
		// servedVersions 的 version 段同样非法。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1", "mygroup.com/v2"},
			ServedVersions:    []string{"v1", "mygroup.com/v2_"},
		},
		expectedErr: `[].servedVersions[1]: Invalid value: "mygroup.com/v2_": version part: a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')`,
	}, {
		// apiVersion 含两个 "/"，超出 group/version 结构。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1", "mygroup.com/v2/myresource"},
			ServedVersions:    []string{"v1", "mygroup.com/v2/myresource"},
		},
		expectedErr: `[].decodableVersions[1]: Invalid value: "mygroup.com/v2/myresource": an apiVersion is a DNS-1035 label, which must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?') with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyVersion')`,
	}, {
		// servedVersions 中同样不允许多余的 "/"。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1", "mygroup.com/v2"},
			ServedVersions:    []string{"v1", "mygroup.com/v2/myresource"},
		},
		expectedErr: `[].servedVersions[1]: Invalid value: "mygroup.com/v2/myresource": an apiVersion is a DNS-1035 label, which must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?') with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyVersion')`,
	}, {
		// served 版本未出现在 decodableVersions 中，无法保证可解码。
		ssv: apiserverinternal.ServerStorageVersion{
			APIServerID:       "fea",
			EncodingVersion:   "v1alpha1",
			DecodableVersions: []string{"v1alpha1", "v1"},
			ServedVersions:    []string{"v2"},
		},
		expectedErr: `[].servedVersions[0]: Invalid value: "v2": individual served version : v2 must be included in decodableVersions : [v1alpha1 v1]`,
	}}

	for _, tc := range cases {
		err := validateServerStorageVersion(tc.ssv, field.NewPath("")).ToAggregate()
		if err == nil && len(tc.expectedErr) == 0 {
			continue
		}
		if err != nil && len(tc.expectedErr) == 0 {
			t.Errorf("unexpected error %v", err)
			continue
		}
		if err == nil && len(tc.expectedErr) != 0 {
			t.Errorf("unexpected empty error")
			continue
		}
		if !strings.Contains(err.Error(), tc.expectedErr) {
			t.Errorf("expected error to contain %s, got %s", tc.expectedErr, err)
		}
	}
}

// TestValidateStorageVersionStatus 验证 StorageVersionStatus 整体校验，重点是 APIServerID 的唯一性。
// 不同 ID 合法；重复 ID 在 storageVersions[1].apiServerID 处报错。
func TestValidateStorageVersionStatus(t *testing.T) {
	cases := []struct {
		svs         apiserverinternal.StorageVersionStatus
		expectedErr string
	}{{
		// 两个不同 APIServerID：合法。
		svs: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{{
				APIServerID:       "1",
				EncodingVersion:   "v1alpha1",
				DecodableVersions: []string{"v1alpha1", "v1"},
			}, {
				APIServerID:       "2",
				EncodingVersion:   "v1alpha1",
				DecodableVersions: []string{"v1alpha1", "v1"},
			}},
			CommonEncodingVersion: ptr.To("v1alpha1"),
		},
		expectedErr: "",
	}, {
		// 两个相同 APIServerID：应报 Duplicate。
		svs: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{{
				APIServerID:       "1",
				EncodingVersion:   "v1alpha1",
				DecodableVersions: []string{"v1alpha1", "v1"},
			}, {
				APIServerID:       "1",
				EncodingVersion:   "v1beta1",
				DecodableVersions: []string{"v1alpha1", "v1"},
			}},
			CommonEncodingVersion: ptr.To("v1alpha1"),
		},
		expectedErr: "storageVersions[1].apiServerID: Duplicate value: \"1\"",
	}}

	for _, tc := range cases {
		err := validateStorageVersionStatus(tc.svs, field.NewPath("")).ToAggregate()
		if err == nil && len(tc.expectedErr) == 0 {
			continue
		}
		if err != nil && len(tc.expectedErr) == 0 {
			t.Errorf("unexpected error %v", err)
			continue
		}
		if err == nil && len(tc.expectedErr) != 0 {
			t.Errorf("unexpected empty error")
			continue
		}
		if !strings.Contains(err.Error(), tc.expectedErr) {
			t.Errorf("expected error to contain %s, got %s", tc.expectedErr, err)
		}
	}
}

// TestValidateCommonVersion 验证 commonEncodingVersion 与实际公共编码版本的一致性。
// 各服务器不一致或没有服务器时必须为 nil；一致时必须非空且等于实际值。
func TestValidateCommonVersion(t *testing.T) {
	cases := []struct {
		status      apiserverinternal.StorageVersionStatus
		expectedErr string
	}{{
		// 没有服务器上报：CommonEncodingVersion 必须为 nil。
		status: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{},
			// Go 无法取字符串字面量的地址，用立即执行函数构造 *string。
			CommonEncodingVersion: func() *string { a := "v1alpha1"; return &a }(),
		},
		expectedErr: "should be nil if servers do not agree on the same encoding version, or if there is no server reporting the supported versions yet",
	}, {
		// 服务器编码版本不一致：公共版本不存在，必须为 nil。
		status: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{{
				APIServerID:     "1",
				EncodingVersion: "v1alpha1",
			}, {
				APIServerID:     "2",
				EncodingVersion: "v1",
			}},
			CommonEncodingVersion: func() *string { a := "v1alpha1"; return &a }(),
		},
		expectedErr: "should be nil if servers do not agree on the same encoding version, or if there is no server reporting the supported versions yet",
	}, {
		// 服务器一致但 CommonEncodingVersion 为 nil：应报出实际公共版本。
		status: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{{
				APIServerID:     "1",
				EncodingVersion: "v1alpha1",
			}, {
				APIServerID:     "2",
				EncodingVersion: "v1alpha1",
			}},
			CommonEncodingVersion: nil,
		},
		expectedErr: "Invalid value: null: the common encoding version is v1alpha1",
	}, {
		// 上报值与实际公共版本不符：应报出期望值 v1alpha1。
		status: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{{
				APIServerID:     "1",
				EncodingVersion: "v1alpha1",
			}, {
				APIServerID:     "2",
				EncodingVersion: "v1alpha1",
			}},
			CommonEncodingVersion: func() *string { a := "v1"; return &a }(),
		},
		expectedErr: "Invalid value: \"v1\": the actual common encoding version is v1alpha1",
	}, {
		// 上报值与实际一致：合法。
		status: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{{
				APIServerID:     "1",
				EncodingVersion: "v1alpha1",
			}, {
				APIServerID:     "2",
				EncodingVersion: "v1alpha1",
			}},
			CommonEncodingVersion: func() *string { a := "v1alpha1"; return &a }(),
		},
		expectedErr: "",
	}, {
		// 单台服务器时其版本即公共版本：合法。
		status: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{{
				APIServerID:     "1",
				EncodingVersion: "v1alpha1",
			}},
			CommonEncodingVersion: func() *string { a := "v1alpha1"; return &a }(),
		},
		expectedErr: "",
	}}
	for _, tc := range cases {
		err := validateCommonVersion(tc.status, field.NewPath(""))
		if err == nil && len(tc.expectedErr) == 0 {
			continue
		}
		if err != nil && len(tc.expectedErr) == 0 {
			t.Errorf("unexpected error %v", err)
			continue
		}
		if err == nil && len(tc.expectedErr) != 0 {
			t.Errorf("unexpected empty error")
			continue
		}
		if !strings.Contains(err.Error(), tc.expectedErr) {
			t.Errorf("expected error to contain %s, got %s", tc.expectedErr, err)
		}
	}
}

// TestValidateStorageVersionCondition 验证 condition 的字段合法性与 type 唯一性。
// type/status 必须是合法限定名，reason/message 必填，type 不可重复。
func TestValidateStorageVersionCondition(t *testing.T) {
	cases := []struct {
		conditions  []apiserverinternal.StorageVersionCondition
		expectedErr string
	}{{
		// type 以 "-" 开头，不是合法限定名。
		conditions: []apiserverinternal.StorageVersionCondition{{
			Type:    "-fea",
			Status:  "True",
			Reason:  "unknown",
			Message: "unknown",
		}},
		expectedErr: "type: Invalid value",
	}, {
		// status 以 "-" 开头，不是合法限定名。
		conditions: []apiserverinternal.StorageVersionCondition{{
			Type:    "fea",
			Status:  "-True",
			Reason:  "unknown",
			Message: "unknown",
		}},
		expectedErr: "status: Invalid value",
	}, {
		// 缺少 reason：必填校验应报错。
		conditions: []apiserverinternal.StorageVersionCondition{{
			Type:    "fea",
			Status:  "True",
			Message: "unknown",
		}},
		expectedErr: "Required value",
	}, {
		// 缺少 message：必填校验应报错。
		conditions: []apiserverinternal.StorageVersionCondition{{
			Type:   "fea",
			Status: "True",
			Reason: "unknown",
		}},
		expectedErr: "Required value",
	}, {
		// 两个 condition 的 type 相同：应报重复。
		conditions: []apiserverinternal.StorageVersionCondition{{
			Type:    "fea",
			Status:  "True",
			Reason:  "unknown",
			Message: "unknown",
		}, {
			Type:    "fea",
			Status:  "True",
			Reason:  "unknown",
			Message: "unknown",
		}},
		expectedErr: `"fea": the type of the condition is not unique, it also appears in conditions[0]`,
	}, {
		// 字段齐全且 type 唯一：合法。
		conditions: []apiserverinternal.StorageVersionCondition{{
			Type:    "fea",
			Status:  "True",
			Reason:  "unknown",
			Message: "unknown",
		}},
		expectedErr: "",
	}}
	for _, tc := range cases {
		err := validateStorageVersionCondition(tc.conditions, field.NewPath("")).ToAggregate()
		if err == nil && len(tc.expectedErr) == 0 {
			continue
		}
		if err != nil && len(tc.expectedErr) == 0 {
			t.Errorf("unexpected error %v", err)
			continue
		}
		if err == nil && len(tc.expectedErr) != 0 {
			t.Errorf("unexpected empty error")
			continue
		}
		if !strings.Contains(err.Error(), tc.expectedErr) {
			t.Errorf("expected error to contain %s, got %s", tc.expectedErr, err)
		}
	}
}

// TestValidateStorageVersionName 验证名称必须为 <group>.<resource> 形式。
// group 段按 DNS-1123 子域校验（上限 253），resource 段按 DNS-1035 标签校验（上限 63）。
func TestValidateStorageVersionName(t *testing.T) {
	cases := []struct {
		name        string
		expectedErr string
	}{{
		// 空名：没有 "."，不满足形式要求。
		name:        "",
		expectedErr: `name must be in the form of <group>.<resource>`,
	}, {
		// 无 "."：不满足 <group>.<resource> 形式。
		name:        "pods",
		expectedErr: `name must be in the form of <group>.<resource>`,
	}, {
		// 最短的合法形式。
		name:        "core.pods",
		expectedErr: "",
	}, {
		// 多段 group + resource：合法。
		name:        "authentication.k8s.io.tokenreviews",
		expectedErr: "",
	}, {
		// group 段恰好 253 字符，仍在边界内。
		name:        strings.Repeat("x", 253) + ".tokenreviews",
		expectedErr: "",
	}, {
		// group 段 254 字符，超出上限。
		name:        strings.Repeat("x", 254) + ".tokenreviews",
		expectedErr: `the group segment must be no more than 253 characters`,
	}, {
		// resource 段恰好 63 字符，仍在边界内。
		name:        "authentication.k8s.io." + strings.Repeat("x", 63),
		expectedErr: "",
	}, {
		// resource 段 64 字符，超出上限。
		name:        "authentication.k8s.io." + strings.Repeat("x", 64),
		expectedErr: `the resource segment must be no more than 63 characters`,
	}}
	for _, tc := range cases {
		errs := ValidateStorageVersionName(tc.name, false)
		if errs == nil && len(tc.expectedErr) == 0 {
			continue
		}
		if errs != nil && len(tc.expectedErr) == 0 {
			t.Errorf("unexpected error %v", errs)
			continue
		}
		if errs == nil && len(tc.expectedErr) != 0 {
			t.Errorf("unexpected empty error")
			continue
		}
		// 校验返回错误列表，逐条精确匹配即可，无需子串包含判断。
		found := false
		for _, msg := range errs {
			if msg == tc.expectedErr {
				found = true
			}
		}
		if !found {
			t.Errorf("expected error to contain %s, got %v", tc.expectedErr, errs)
		}
	}
}
