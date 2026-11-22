/*
Copyright 2019 The Kubernetes Authors.

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

package testing

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// TestCase defines a testcase for roundtrip and defaulting tests
// name 用作子测试名；in/out 为相对测试数据目录的 YAML 文件路径。
type TestCase struct {
	name, in, out string
	codec         runtime.Codec
}

// RunTestsOnYAMLData decodes the yaml file from specified path, encodes the object and matches
// with expected yaml in specified path
func RunTestsOnYAMLData(t *testing.T, tests []TestCase) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			roundTrip(t, tc)
		})
	}
}

// decodeYAML 读取并解码 YAML 文件；解码失败视为测试数据错误，直接终止。
func decodeYAML(t *testing.T, path string, codec runtime.Codec) runtime.Object {
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// decode to internal type
	object, err := runtime.Decode(codec, content)
	if err != nil {
		t.Fatal(err)
	}

	return object
}

// getCodecForGV 为指定 GroupVersion 选择 YAML 序列化器；
// 传入 nil 作为目标版本表示仅做解码，不强制转换。
func getCodecForGV(codecs serializer.CodecFactory, gv schema.GroupVersion) (runtime.Codec, error) {
	mediaType := runtime.ContentTypeYAML
	serializerInfo, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return nil, fmt.Errorf("unable to locate encoder -- %q is not a supported media type", mediaType)
	}
	codec := codecs.CodecForVersions(serializerInfo.Serializer, codecs.UniversalDeserializer(), gv, nil)
	return codec, nil
}

// matchOutputFile 比较输出与期望文件；可用 UPDATE_COMPONENTCONFIG_FIXTURE_DATA=true
// 重新生成兼容性测试数据。文件缺失同样视为需要更新。
func matchOutputFile(t *testing.T, actual []byte, expectedFilePath string) {
	expected, err := os.ReadFile(expectedFilePath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("couldn't read test data: %v", err)
	}

	needsUpdate := false
	const updateEnvVar = "UPDATE_COMPONENTCONFIG_FIXTURE_DATA"

	if os.IsNotExist(err) {
		needsUpdate = true
		if os.Getenv(updateEnvVar) != "true" {
			t.Error("couldn't find test data")
		}
	} else {
		if !bytes.Equal(expected, actual) {
			t.Errorf("Output does not match expected, diff (- want, + got):\n%s\n",
				cmp.Diff(string(expected), string(actual)))
			needsUpdate = true
		}
	}
	if needsUpdate {
		if os.Getenv(updateEnvVar) == "true" {
			// 递归创建目录，允许新增 API 组的 fixture 首次生成。
			if err := os.MkdirAll(filepath.Dir(expectedFilePath), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(expectedFilePath, actual, 0644); err != nil {
				t.Fatal(err)
			}
			t.Error("wrote expected test data... verify, commit, and rerun tests")
		} else {
			t.Errorf("if the diff is expected because of a new type or a new field, "+
				"re-run with %s=true to update the compatibility data or generate missing files", updateEnvVar)
		}
	}
}
