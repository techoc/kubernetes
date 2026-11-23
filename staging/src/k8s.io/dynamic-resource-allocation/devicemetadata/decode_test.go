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

// Package devicemetadata 本文件包含 devicemetadata 包的单元测试，
// 覆盖 JSON 流解码、解码选项以及元数据文件与目录的读取合并逻辑。
package devicemetadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/dynamic-resource-allocation/api/metadata"
	"k8s.io/dynamic-resource-allocation/api/metadata/v1alpha1"
	"k8s.io/dynamic-resource-allocation/api/metadata/v1beta1"
	"k8s.io/utils/ptr"
)

// 以下是各测试共享的合法元数据夹具：validInternal 为内部类型期望值，
// validV1Alpha1 / validV1Beta1 为对应版本化对象，validV1Alpha1JSON / validV1Beta1JSON
// 是它们序列化后带换行符的 JSON 字节流，用作解码输入。
var (
	validInternal = &metadata.DeviceMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-claim",
			Namespace:  "default",
			UID:        "uid-1234",
			Generation: 1,
		},
		Requests: []metadata.DeviceMetadataRequest{{
			Name: "gpu",
			Devices: []metadata.Device{{
				Driver: "gpu.example.com",
				Pool:   "worker-0",
				Name:   "gpu-0",
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
				},
			}},
		}},
	}

	validV1Alpha1 = &v1alpha1.DeviceMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "DeviceMetadata",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-claim",
			Namespace:  "default",
			UID:        "uid-1234",
			Generation: 1,
		},
		Requests: []v1alpha1.DeviceMetadataRequest{{
			Name: "gpu",
			Devices: []v1alpha1.Device{{
				Driver: "gpu.example.com",
				Pool:   "worker-0",
				Name:   "gpu-0",
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"model": {StringValue: new("LATEST-GPU-MODEL")},
				},
			}},
		}},
	}
	validV1Alpha1JSON = mustMarshal(validV1Alpha1)

	validV1Beta1 = &v1beta1.DeviceMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       "DeviceMetadata",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-claim",
			Namespace:  "default",
			UID:        "uid-1234",
			Generation: 1,
		},
		Requests: []v1beta1.DeviceMetadataRequest{{
			Name: "gpu",
			Devices: []v1beta1.Device{{
				Driver: "gpu.example.com",
				Pool:   "worker-0",
				Name:   "gpu-0",
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
				},
			}},
		}},
	}
	validV1Beta1JSON = mustMarshal(validV1Beta1)
)

// mustMarshal 把对象序列化为 JSON，失败时直接 panic；末尾追加换行符以模拟按行分隔的元数据流。
func mustMarshal(obj interface{}) []byte {
	data, err := json.Marshal(obj)
	if err != nil {
		// 测试夹具必须可序列化，否则视为测试编写错误。
		panic(err)
	}
	// 追加换行符，便于多个对象拼接成 JSON 流。
	return append(data, '\n')
}

// TestDecodeMetadataFromStream 测试流解码的各种场景：
// 不同版本之间的相互转换、多版本流、未知版本跳过、以及各类畸形输入产生的错误。
// 每个用例在开启和关闭校验两种模式下各运行一次，并按需断言解码结果、校验错误与 GVK。
func TestDecodeMetadataFromStream(t *testing.T) {
	// 未知版本（v99/v100）：应被静默跳过；缺少 kind 的未知版本则属于错误。
	unknownVersionJSON := `{"apiVersion":"metadata.k8s.io/v99","kind":"DeviceMetadata","metadata":{"name":"test"}}` + "\n"
	unknownV2JSON := `{"apiVersion":"metadata.k8s.io/v100","kind":"DeviceMetadata","metadata":{"name":"test"}}` + "\n"
	unknownVersionMissingKindJSON := `{"apiVersion":"metadata.k8s.io/v99","metadata":{"name":"test"}}` + "\n"
	// 已注册版本但缺少 kind / apiVersion，以及字段类型错误的输入：均应致命报错。
	missingKindJSON := `{"apiVersion":"metadata.resource.k8s.io/v1alpha1","metadata":{"name":"test"}}` + "\n"
	missingApiversionJSON := `{"kind":"DeviceMetadata","metadata":{"name":"test"}}` + "\n"
	malformedV1Alpha1JSON := `{"apiVersion":"metadata.resource.k8s.io/v1alpha1","kind":"DeviceMetadata","metadata":{"name":"test"},"requests":"this-should-be-an-array"}` + "\n"

	// podClaimName 非法的输入，用于验证声明式校验能捕获错误。
	invalidV1Alpha1JSON := `{"apiVersion":"metadata.resource.k8s.io/v1alpha1","kind":"DeviceMetadata","metadata":{"name":"test"},"podClaimName":"%!$#"}`
	invalidV1Beta1JSON := `{"apiVersion":"metadata.resource.k8s.io/v1beta1","kind":"DeviceMetadata","metadata":{"name":"test"},"podClaimName":"%!$#"}`
	// 非法输入对应的内部类型期望值：数据仍被解码，但校验结果应报错。
	invalidInternal := &metadata.DeviceMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
		PodClaimName: new("%!$#"),
	}

	// 表驱动用例：isInvalid 标记对象能否通过校验，streamInput 为输入 JSON 流，
	// dest 为目标类型，expected 为期望解码结果，expectError / expectGVK 为期望错误与 GVK。
	testcases := map[string]struct {
		isInvalid   bool
		streamInput []byte
		dest        runtime.Object
		expected    runtime.Object
		expectError string
		expectGVK   *schema.GroupVersionKind
	}{
		"decode-v1alpha1-to-v1alpha1": {
			streamInput: validV1Alpha1JSON,
			dest:        &v1alpha1.DeviceMetadata{},
			expected:    validV1Alpha1,
			expectGVK:   new(v1alpha1.SchemeGroupVersion.WithKind("DeviceMetadata")),
		},
		"decode-v1alpha1-to-v1beta1": {
			streamInput: validV1Alpha1JSON,
			dest:        &v1beta1.DeviceMetadata{},
			expected:    validV1Beta1,
		},
		"decode-v1alpha1-to-internal": {
			streamInput: validV1Alpha1JSON,
			dest:        &metadata.DeviceMetadata{},
			expected:    validInternal,
			expectGVK:   new(v1alpha1.SchemeGroupVersion.WithKind("DeviceMetadata")),
		},
		"decode-v1beta1-to-v1alpha1": {
			streamInput: validV1Beta1JSON,
			dest:        &v1alpha1.DeviceMetadata{},
			expected:    validV1Alpha1,
		},
		"decode-v1beta1-to-v1beta1": {
			streamInput: validV1Beta1JSON,
			dest:        &v1beta1.DeviceMetadata{},
			expected:    validV1Beta1,
			expectGVK:   new(v1beta1.SchemeGroupVersion.WithKind("DeviceMetadata")),
		},
		"decode-v1beta1-to-internal": {
			streamInput: validV1Beta1JSON,
			dest:        &metadata.DeviceMetadata{},
			expected:    validInternal,
			expectGVK:   new(v1beta1.SchemeGroupVersion.WithKind("DeviceMetadata")),
		},
		"decode-all-to-v1alpha1": {
			streamInput: []byte(string(validV1Beta1JSON) + string(validV1Alpha1JSON)),
			dest:        &v1alpha1.DeviceMetadata{},
			expected:    validV1Alpha1,
		},
		"decode-all-to-v1beta1": {
			streamInput: []byte(string(validV1Beta1JSON) + string(validV1Alpha1JSON)),
			dest:        &v1beta1.DeviceMetadata{},
			expected:    validV1Beta1,
		},
		"decode-all-to-internal": {
			streamInput: []byte(string(validV1Beta1JSON) + string(validV1Alpha1JSON)),
			dest:        &metadata.DeviceMetadata{},
			expected:    validInternal,
			expectGVK:   new(v1beta1.SchemeGroupVersion.WithKind("DeviceMetadata")),
		},
		"decode-all-reversed-to-internal": {
			streamInput: []byte(string(validV1Alpha1JSON) + string(validV1Beta1JSON)),
			dest:        &metadata.DeviceMetadata{},
			expected:    validInternal,
			expectGVK:   new(v1alpha1.SchemeGroupVersion.WithKind("DeviceMetadata")),
		},
		"decode-all-to-wrong-type": {
			streamInput: []byte(string(validV1Alpha1JSON) + string(validV1Beta1JSON)),
			dest:        &resourceapi.ResourceClaim{},
			expectError: "determine target type",
		},
		"invalid-v1alpha1": {
			isInvalid:   true,
			streamInput: []byte(invalidV1Alpha1JSON),
			dest:        &metadata.DeviceMetadata{},
			expected:    invalidInternal,
		},
		"invalid-v1beta1": {
			isInvalid:   true,
			streamInput: []byte(invalidV1Beta1JSON),
			dest:        &metadata.DeviceMetadata{},
			expected:    invalidInternal,
		},
		"empty-stream": {
			streamInput: nil,
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "no metadata objects found in stream",
		},
		"invalid-json": {
			streamInput: []byte("{not-json"),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "read metadata object from stream",
		},
		"truncated-json": {
			streamInput: []byte(`{"apiVersion":"metadata.resource.k8s.io/v1alpha1","kind":"DeviceMeta`),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "read metadata object from stream",
		},
		"missing-kind": {
			streamInput: []byte(missingKindJSON),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata.resource.k8s.io/v1alpha1",
		},
		"missing-apiversion": {
			streamInput: []byte(missingApiversionJSON),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata object",
		},
		"malformed-registered-version": {
			streamInput: []byte(malformedV1Alpha1JSON),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata.resource.k8s.io/v1alpha1",
		},
		"only-unknown-versions": {
			streamInput: []byte(unknownVersionJSON),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "no compatible metadata version found in stream (unknown versions: metadata.k8s.io/v99)",
		},
		"unknown-version-missing-kind": {
			streamInput: []byte(unknownVersionMissingKindJSON),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata.k8s.io/v99",
		},
		"multiple-unknown-versions": {
			streamInput: append([]byte(unknownVersionJSON), []byte(unknownV2JSON)...),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "no compatible metadata version found in stream (unknown versions: metadata.k8s.io/v99, metadata.k8s.io/v100)",
		},
		"known-version-then-broken": {
			streamInput: append(validV1Alpha1JSON, []byte("{broken")...),
			dest:        &v1alpha1.DeviceMetadata{},
			expected:    validV1Alpha1,
		},
		"skips-unknown-version": {
			streamInput: append([]byte(unknownVersionJSON), validV1Alpha1JSON...),
			dest:        &v1alpha1.DeviceMetadata{},
			expected:    validV1Alpha1,
		},
		"malformed-registered-then-valid-is-fatal": {
			streamInput: append([]byte(malformedV1Alpha1JSON), validV1Alpha1JSON...),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata.resource.k8s.io/v1alpha1",
		},
		"missing-kind-then-valid-is-fatal": {
			streamInput: append([]byte(missingKindJSON), validV1Alpha1JSON...),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata.resource.k8s.io/v1alpha1",
		},
		"missing-apiversion-then-valid-is-fatal": {
			streamInput: append([]byte(missingApiversionJSON), validV1Alpha1JSON...),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata object",
		},
		"unknown-version-then-malformed-is-fatal": {
			streamInput: append([]byte(unknownVersionJSON), []byte(missingKindJSON)...),
			dest:        &v1alpha1.DeviceMetadata{},
			expectError: "decode metadata.resource.k8s.io/v1alpha1",
		},
	}

	// 遍历所有用例，每个用例都同时测试开启与关闭校验两条路径。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, withValidation := range []bool{true, false} {
				t.Run(fmt.Sprintf("with-validation-%v", withValidation), func(t *testing.T) {
					// 按需注册校验结果选项，用独立副本作为解码目标以免影响其他子测试。
					var opts []DecodeMetadataOption
					var validationResult error
					if withValidation {
						opts = append(opts, DecodeMetadataWithValidationResult(&validationResult))
					}
					dest := tc.dest.DeepCopyObject()
					err := DecodeMetadataFromStream(json.NewDecoder(bytes.NewReader(tc.streamInput)), dest, opts...)
					// 期望报错时，断言错误非空且包含预期子串。
					if tc.expectError != "" {
						if err == nil {
							t.Fatalf("expected error containing %q, got nil", tc.expectError)
						}
						if !strings.Contains(err.Error(), tc.expectError) {
							t.Fatalf("expected error containing %q, got: %v", tc.expectError, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("DecodeMetadataFromStream: %v", err)
					}

					// 断言解码出的对象与期望值完全一致。
					if diff := cmp.Diff(tc.expected, dest); diff != "" {
						t.Errorf("metadata mismatch (-want +got):\n%s", diff)
					}

					// 校验结果断言：仅当开启校验且输入非法时应当报错。
					if withValidation && tc.isInvalid && validationResult == nil {
						t.Errorf("validation did not fail as expected")
					}
					if validationResult != nil && (!withValidation || !tc.isInvalid) {
						t.Errorf("unexpected validation error: %v", validationResult)
					}

					// 需要校验 GVK 时，再解码一次并通过选项捕获实际 GVK 进行比对。
					if tc.expectGVK != nil {
						var gvk schema.GroupVersionKind
						dest := tc.dest.DeepCopyObject()
						err := DecodeMetadataFromStream(json.NewDecoder(bytes.NewReader(tc.streamInput)), dest, append(opts, DecodeMetadataStoreGVKResult(&gvk))...)
						if err != nil {
							t.Fatalf("DecodeMetadataFromStream with GVK: %v", err)
						}
						if gvk != *tc.expectGVK {
							t.Errorf("expected %s, got %s", tc.expectGVK, gvk)
						}
					}
				})
			}
		})
	}
}

// TestReadMetadata 测试单文件读取：合法文件能解码为内部类型，
// 文件缺失与空对象（无 apiVersion）分别产生对应错误，且错误信息中包含文件路径。
func TestReadMetadata(t *testing.T) {
	dir := t.TempDir()
	// 写入一个合法的元数据文件。
	validFile := filepath.Join(dir, "valid", "metadata.json")
	if err := os.MkdirAll(filepath.Dir(validFile), 0755); err != nil {
		t.Fatalf("create test dir: %v", err)
	}
	if err := os.WriteFile(validFile, validV1Alpha1JSON, 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// 写入一个空 JSON 对象：不含 apiVersion，应导致解码失败。
	emptyObjectFile := filepath.Join(dir, "empty-object", "metadata.json")
	if err := os.MkdirAll(filepath.Dir(emptyObjectFile), 0755); err != nil {
		t.Fatalf("create test dir: %v", err)
	}
	if err := os.WriteFile(emptyObjectFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	// 合法文件解码后的期望内部类型结果。
	expectedInternal := &metadata.DeviceMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-claim",
			Namespace:  "default",
			UID:        "uid-1234",
			Generation: 1,
		},
		Requests: []metadata.DeviceMetadataRequest{{
			Name: "gpu",
			Devices: []metadata.Device{{
				Driver: "gpu.example.com",
				Pool:   "worker-0",
				Name:   "gpu-0",
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
				},
			}},
		}},
	}

	// 表驱动用例：path 为待读取文件，expected 为期望结果，expectError 为期望错误信息子串。
	testcases := map[string]struct {
		path        string
		expected    *metadata.DeviceMetadata
		expectError string
	}{
		"valid-file": {
			path:     validFile,
			expected: expectedInternal,
		},
		"missing-file": {
			path:        "/nonexistent/path/metadata.json",
			expectError: "open metadata file",
		},
		"error-includes-path": {
			path:        emptyObjectFile,
			expectError: emptyObjectFile,
		},
	}

	// 逐一读取文件并核对错误或解码结果。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			dm, err := readMetadata(tc.path)

			// 期望错误时：断言错误非空且包含预期子串。
			if tc.expectError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectError)
				}
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Fatalf("expected error containing %q, got: %v", tc.expectError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("readMetadata: %v", err)
			}

			// 断言解码结果符合预期。
			if diff := cmp.Diff(tc.expected, dm); diff != "" {
				t.Errorf("metadata mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestReadRequestDir 测试目录合并逻辑：单个/多个驱动的元数据、同名请求跨驱动合并、
// 忽略非匹配文件、保留 PodClaimName，以及空目录或不存在的目录报错。
func TestReadRequestDir(t *testing.T) {
	dir := t.TempDir()

	// 期望的内部类型请求：gpu 与 nic，以及另一个驱动提供的同名 gpu 请求。
	gpuInternal := metadata.DeviceMetadataRequest{
		Name: "gpu",
		Devices: []metadata.Device{{
			Driver: "gpu.example.com",
			Pool:   "worker-0",
			Name:   "gpu-0",
			Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
			},
		}},
	}
	// 另一个驱动提供的同名 gpu 请求，用于验证跨驱动合并设备列表。
	gpuAltInternal := metadata.DeviceMetadataRequest{
		Name: "gpu",
		Devices: []metadata.Device{{
			Driver: "mlx.example.com",
			Pool:   "worker-1",
			Name:   "gpu-1",
		}},
	}
	// 不同名的 nic 请求，用于验证多请求共存。
	nicInternal := metadata.DeviceMetadataRequest{
		Name: "nic",
		Devices: []metadata.Device{{
			Driver: "nic.example.com",
			Pool:   "worker-0",
			Name:   "nic-0",
		}},
	}

	// 各驱动写入的元数据文件 JSON 内容（v1alpha1 序列化结果）。
	gpuJSON := mustMarshal(&v1alpha1.DeviceMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.SchemeGroupVersion.String(), Kind: "DeviceMetadata"},
		ObjectMeta: metav1.ObjectMeta{Name: "my-claim", Namespace: "default", UID: "uid-1234", Generation: 1},
		Requests: []v1alpha1.DeviceMetadataRequest{{
			Name: "gpu",
			Devices: []v1alpha1.Device{{
				Driver: "gpu.example.com",
				Pool:   "worker-0",
				Name:   "gpu-0",
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
				},
			}},
		}},
	})
	// nic 请求对应的元数据文件。
	nicJSON := mustMarshal(&v1alpha1.DeviceMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.SchemeGroupVersion.String(), Kind: "DeviceMetadata"},
		ObjectMeta: metav1.ObjectMeta{Name: "my-claim", Namespace: "default", UID: "uid-1234", Generation: 1},
		Requests: []v1alpha1.DeviceMetadataRequest{{
			Name:    "nic",
			Devices: []v1alpha1.Device{{Driver: "nic.example.com", Pool: "worker-0", Name: "nic-0"}},
		}},
	})
	// 主驱动写入三个请求，其中 gpu-0 与 gpu-1 会与次驱动的同名请求合并。
	primaryDriverJSON := mustMarshal(&v1alpha1.DeviceMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.SchemeGroupVersion.String(), Kind: "DeviceMetadata"},
		ObjectMeta: metav1.ObjectMeta{Name: "my-claim", Namespace: "default", UID: "uid-1234", Generation: 1},
		Requests: []v1alpha1.DeviceMetadataRequest{
			{
				Name: "gpu-0",
				Devices: []v1alpha1.Device{{
					Driver: "gpu.example.com",
					Pool:   "worker-0",
					Name:   "gpu-0",
					Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
					},
				}},
			},
			{
				Name: "single-gpu",
				Devices: []v1alpha1.Device{{
					Driver: "gpu.example.com",
					Pool:   "worker-0",
					Name:   "gpu-0",
					Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
					},
				}},
			},
			{
				Name: "gpu-1",
				Devices: []v1alpha1.Device{{
					Driver: "gpu.example.com",
					Pool:   "worker-0",
					Name:   "gpu-0",
					Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"model": {StringValue: ptr.To("LATEST-GPU-MODEL")}, //nolint:modernize
					},
				}},
			},
		},
	})
	// 次驱动写入两个与主驱动同名的请求，用于验证设备列表追加。
	secondaryDriverJSON := mustMarshal(&v1alpha1.DeviceMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.SchemeGroupVersion.String(), Kind: "DeviceMetadata"},
		ObjectMeta: metav1.ObjectMeta{Name: "my-claim", Namespace: "default", UID: "uid-1234", Generation: 1},
		Requests: []v1alpha1.DeviceMetadataRequest{
			{
				Name:    "gpu-0",
				Devices: []v1alpha1.Device{{Driver: "mlx.example.com", Pool: "worker-1", Name: "gpu-1"}},
			},
			{
				Name:    "gpu-1",
				Devices: []v1alpha1.Device{{Driver: "mlx.example.com", Pool: "worker-1", Name: "gpu-1"}},
			},
		},
	})
	// 带 PodClaimName 的元数据文件，用于验证合并时保留该字段。
	podClaimJSON := mustMarshal(&v1alpha1.DeviceMetadata{
		TypeMeta:     metav1.TypeMeta{APIVersion: v1alpha1.SchemeGroupVersion.String(), Kind: "DeviceMetadata"},
		ObjectMeta:   metav1.ObjectMeta{Name: "my-claim", Namespace: "default", UID: "uid-1234", Generation: 1},
		PodClaimName: ptr.To("my-gpu"), //nolint:modernize
		Requests: []v1alpha1.DeviceMetadataRequest{{
			Name:    "gpu",
			Devices: []v1alpha1.Device{{Driver: "gpu.example.com", Pool: "worker-0", Name: "gpu-0"}},
		}},
	})

	// writeFile 在子目录中写入一个文件并返回该子目录路径，便于构造不同测试场景。
	writeFile := func(subdir, name string, data []byte) string {
		p := filepath.Join(dir, subdir)
		if err := os.MkdirAll(p, 0755); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
		if err := os.WriteFile(filepath.Join(p, name), data, 0644); err != nil {
			t.Fatalf("write %s/%s: %v", p, name, err)
		}
		return p
	}

	// 单驱动目录：仅一个元数据文件。
	singleDir := writeFile("single", "gpu.example.com"+metadata.MetadataFileSuffix, gpuJSON)
	// 多驱动目录：两个驱动各写一个文件。
	multiDir := writeFile("multi", "gpu.example.com"+metadata.MetadataFileSuffix, gpuJSON)
	writeFile("multi", "nic.example.com"+metadata.MetadataFileSuffix, nicJSON)
	// 同名请求目录：两个驱动写入部分同名的请求，验证设备合并。
	sameRequestNameDir := writeFile("same-request-name", "gpu.example.com"+metadata.MetadataFileSuffix, primaryDriverJSON)
	writeFile("same-request-name", "mlx.example.com"+metadata.MetadataFileSuffix, secondaryDriverJSON)
	// 空目录：只有一个不相关的 txt 文件，不应被匹配为元数据。
	emptyDir := writeFile("empty", "unrelated.txt", []byte("not metadata"))
	// 混合目录：一个合法元数据文件加一个不相关的 json 文件，后者应被忽略。
	mixedDir := writeFile("mixed", "gpu.example.com"+metadata.MetadataFileSuffix, gpuJSON)
	writeFile("mixed", "unrelated.json", []byte(`{"foo":"bar"}`))
	// PodClaimName 目录：验证合并结果保留 pod 本地声明名。
	podClaimDir := writeFile("podclaim", "gpu.example.com"+metadata.MetadataFileSuffix, podClaimJSON)

	// 表驱动用例：dir 为待读取目录，expected 为期望合并结果，expectError 为期望错误子串。
	testcases := map[string]struct {
		dir         string
		expected    *metadata.DeviceMetadata
		expectError string
	}{
		"single-driver": {
			dir: singleDir,
			expected: &metadata.DeviceMetadata{
				Requests: []metadata.DeviceMetadataRequest{gpuInternal},
			},
		},
		"multiple-drivers": {
			dir: multiDir,
			expected: &metadata.DeviceMetadata{
				Requests: []metadata.DeviceMetadataRequest{gpuInternal, nicInternal},
			},
		},
		"merge-same-request-name-across-drivers": {
			dir: sameRequestNameDir,
			expected: &metadata.DeviceMetadata{
				Requests: []metadata.DeviceMetadataRequest{
					{
						Name:    "gpu-0",
						Devices: append(gpuInternal.Devices, gpuAltInternal.Devices...),
					},
					{
						Name:    "single-gpu",
						Devices: gpuInternal.Devices,
					},
					{
						Name:    "gpu-1",
						Devices: append(gpuInternal.Devices, gpuAltInternal.Devices...),
					},
				},
			},
		},
		"empty-directory": {
			dir:         emptyDir,
			expectError: "no metadata files found",
		},
		"nonexistent-directory": {
			dir:         "/nonexistent/path",
			expectError: "no metadata files found",
		},
		"ignores-non-matching-files": {
			dir: mixedDir,
			expected: &metadata.DeviceMetadata{
				Requests: []metadata.DeviceMetadataRequest{gpuInternal},
			},
		},
		"preserves-pod-claim-name": {
			dir: podClaimDir,
			expected: &metadata.DeviceMetadata{
				PodClaimName: ptr.To("my-gpu"), //nolint:modernize
				Requests: []metadata.DeviceMetadataRequest{{
					Name:    "gpu",
					Devices: []metadata.Device{{Driver: "gpu.example.com", Pool: "worker-0", Name: "gpu-0"}},
				}},
			},
		},
	}

	// 遍历目录用例，核对报错或合并后的元数据结果。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			dm, err := readRequestDir(tc.dir)

			// 期望错误时：断言错误非空且包含预期子串。
			if tc.expectError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectError)
				}
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Fatalf("expected error containing %q, got: %v", tc.expectError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("readRequestDir: %v", err)
			}

			// 断言合并结果符合预期。
			if diff := cmp.Diff(tc.expected, dm); diff != "" {
				t.Errorf("metadata mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
