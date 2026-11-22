/*
Copyright The Kubernetes Authors.

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

// Package testing 为 flagz 端点提供测试辅助：校验结构化响应的
// TypeMeta、对象名、标志集合以及弃用 Warning 头。
package testing

import (
	"encoding/json"
	"strings"
	"testing"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/server/flagz/api/v1alpha1"
	"k8s.io/apiserver/pkg/server/flagz/api/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VerifyStructuredResponse 按 Accept 头选择解码器解析响应体，并与期望对象
// 逐项比较；wantDeprecationHeader 控制是否要求出现 deprecated 的 Warning。
func VerifyStructuredResponse(t *testing.T, acceptHeader string, body []byte, warnings []string, want interface{}, wantDeprecationHeader bool) {
	t.Helper()

	unmarshal := unmarshalFunc(t, acceptHeader)
	wantTypeMeta, wantName, wantFlags := wantFields(t, want)
	gotTypeMeta, gotName, gotFlags := gotFields(t, unmarshal, body, wantTypeMeta.APIVersion)

	if gotName != wantName {
		t.Errorf("name mismatch: got %q, want %q", gotName, wantName)
	}
	if gotTypeMeta != wantTypeMeta {
		t.Errorf("type meta mismatch: got %v, want %v", gotTypeMeta, wantTypeMeta)
	}
	// 只检查期望中的标志，避免测试因实现新增标志而脆弱。
	for k, v := range wantFlags {
		gotV, ok := gotFlags[k]
		if !ok {
			t.Errorf("missing flag %q", k)
			continue
		}
		if gotV != v {
			t.Errorf("flag %q match: got %q, want %q", k, gotV, v)
		}
	}

	foundWarning := false
	for _, w := range warnings {
		if strings.Contains(w, "deprecated") {
			foundWarning = true
			break
		}
	}
	if foundWarning != wantDeprecationHeader {
		t.Errorf("deprecation header mismatch: got %v, want %v", foundWarning, wantDeprecationHeader)
	}
}

// unmarshalFunc 根据 Accept 头返回对应的解码函数，保证测试走的是客户端
// 实际使用的序列化路径。
func unmarshalFunc(t *testing.T, acceptHeader string) func([]byte, interface{}) error {
	switch {
	case strings.Contains(acceptHeader, "application/json"):
		return json.Unmarshal
	case strings.Contains(acceptHeader, "application/yaml"):
		return yaml.Unmarshal
	case strings.Contains(acceptHeader, "application/cbor"):
		return cbor.Unmarshal
	default:
		t.Fatalf("unexpected Accept header: %q", acceptHeader)
	}
	return nil
}

// wantFields 从期望对象中提取需要比较的字段，兼容两个 API 版本。
func wantFields(t *testing.T, want interface{}) (metav1.TypeMeta, string, map[string]string) {
	t.Helper()
	switch w := want.(type) {
	case *v1alpha1.Flagz:
		return w.TypeMeta, w.Name, w.Flags
	case *v1beta1.Flagz:
		return w.TypeMeta, w.Name, w.Flags
	default:
		t.Fatalf("unexpected type for want: %T", want)
		return metav1.TypeMeta{}, "", nil
	}
}

// gotFields 依据 apiVersion 选择具体类型解码响应体，再取出对应字段。
func gotFields(t *testing.T, unmarshal func([]byte, interface{}) error, body []byte, apiVersion string) (metav1.TypeMeta, string, map[string]string) {
	var gotName string
	var gotTypeMeta metav1.TypeMeta
	var gotFlags map[string]string
	switch apiVersion {
	case "config.k8s.io/v1alpha1":
		var got v1alpha1.Flagz
		if err := unmarshal(body, &got); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		gotName = got.Name
		gotTypeMeta = got.TypeMeta
		gotFlags = got.Flags
	case "config.k8s.io/v1beta1":
		var got v1beta1.Flagz
		if err := unmarshal(body, &got); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		gotName = got.Name
		gotTypeMeta = got.TypeMeta
		gotFlags = got.Flags
	default:
		t.Fatalf("unexpected API version: %q", apiVersion)
	}
	return gotTypeMeta, gotName, gotFlags
}
