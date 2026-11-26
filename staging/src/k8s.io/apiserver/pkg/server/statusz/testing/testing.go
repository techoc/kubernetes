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

// Package testing 提供 statusz 端点的响应断言辅助函数，供各版本 statusz 的
// handler 测试复用（校验结构字段、编码协商与弃用告警）。
package testing

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/server/statusz/api/v1alpha1"
	"k8s.io/apiserver/pkg/server/statusz/api/v1beta1"
)

// VerifyStructuredResponse 校验响应体匹配预期的静态字段，
// 并检查 warning 是否存在。
//
// 按 Accept 头选择对应的反序列化器，因此同一组期望值可以同时验证 JSON/YAML/CBOR 三种编码。
func VerifyStructuredResponse(t *testing.T, acceptHeader string, body []byte, warnings []string, want interface{}, wantDeprecationHeader bool) {
	t.Helper()

	unmarshal := unmarshalFunc(t, acceptHeader)
	wantTypeMeta, wantName, wantPaths := wantFields(t, want)
	gotTypeMeta, gotName, gotPaths := gotFields(t, unmarshal, body, wantTypeMeta.APIVersion)

	if gotName != wantName {
		t.Errorf("ObjectMeta.Name mismatch: want %q, got %q", wantName, gotName)
	}
	if gotTypeMeta != wantTypeMeta {
		t.Errorf("TypeMeta mismatch: want %+v, got %+v", wantTypeMeta, gotTypeMeta)
	}
	if diff := cmp.Diff(wantPaths, gotPaths); diff != "" {
		t.Errorf("Paths mismatch (-want,+got):\n%s", diff)
	}

	foundWarning := false
	for _, w := range warnings {
		if strings.Contains(w, "deprecated") {
			foundWarning = true
			break
		}
	}

	// 已弃用 API 版本必须通过 Warning 头提示客户端，未弃用版本则不得出现。
	if wantDeprecationHeader {
		if !foundWarning {
			t.Errorf("want warning 'deprecated', got none or different: %v", warnings)
		}
	} else {
		if foundWarning {
			t.Errorf("want no warning 'deprecated', got one: %v", warnings)
		}
	}
}

// unmarshalFunc 根据 Accept 头返回对应的解码函数；无法识别的编码直接失败。
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

// wantFields 从期望对象中提取参与比较的字段。
func wantFields(t *testing.T, want interface{}) (metav1.TypeMeta, string, []string) {
	t.Helper()
	switch w := want.(type) {
	case *v1alpha1.Statusz:
		return w.TypeMeta, w.Name, w.Paths
	case *v1beta1.Statusz:
		return w.TypeMeta, w.Name, w.Paths
	default:
		t.Fatalf("unexpected type: %T", want)
	}
	return metav1.TypeMeta{}, "", nil
}

// gotFields 按 apiVersion 选择具体类型反序列化响应体，避免跨版本字段混淆。
func gotFields(t *testing.T, unmarshal func([]byte, interface{}) error, body []byte, apiVersion string) (metav1.TypeMeta, string, []string) {
	t.Helper()
	switch apiVersion {
	case "config.k8s.io/v1alpha1":
		var got v1alpha1.Statusz
		if err := unmarshal(body, &got); err != nil {
			t.Fatalf("error unmarshalling %s: %v", apiVersion, err)
		}
		return got.TypeMeta, got.Name, got.Paths
	case "config.k8s.io/v1beta1":
		var got v1beta1.Statusz
		if err := unmarshal(body, &got); err != nil {
			t.Fatalf("error unmarshalling %s: %v", apiVersion, err)
		}
		return got.TypeMeta, got.Name, got.Paths
	default:
		t.Fatalf("unexpected APIVersion: %q", apiVersion)
	}
	return metav1.TypeMeta{}, "", nil
}
