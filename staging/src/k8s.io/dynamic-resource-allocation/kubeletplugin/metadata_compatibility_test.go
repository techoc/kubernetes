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

package kubeletplugin

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/dynamic-resource-allocation/api/metadata"
	"k8s.io/dynamic-resource-allocation/devicemetadata"
)

// updateCompatibilityFixtureData 是用于触发更新兼容性测试数据的环境变量名。
const updateCompatibilityFixtureData = "UPDATE_COMPATIBILITY_FIXTURE_DATA"

// TestDeviceMetadataCompatibility 验证元数据格式的前后兼容性：当前版本编码结果必须与 HEAD 固定数据一致，
// 历史版本的固定数据必须能解码并以相同版本原样重新编码。
// 输入：内部 DeviceMetadata 与已注册版本列表；
// 断言：固定数据匹配、解码结果一致且往返编码字节不变。
func TestDeviceMetadataCompatibility(t *testing.T) {
	// 生成当前版本的测试数据对象。
	expected := compatibilityTestMetadata(t)
	// 获取所有已注册的外部版本。
	versions := registeredMetadataVersions()
	// 没有注册版本则测试无意义。
	if len(versions) == 0 {
		t.Fatal("no external metadata API versions are registered")
	}

	// 编出期望的流数据。
	expectedData := encodeCompatibilityMetadata(t, expected, versions)
	// 固定数据目录。
	testDataDir := filepath.Join("..", "api", "metadata", "testdata")
	// HEAD 版本的固定文件。
	headFixture := filepath.Join(testDataDir, "HEAD", "metadata.json")
	// 与固定数据比较；不匹配则提前返回，避免后续断言掩盖问题。
	if !matchCompatibilityFixture(t, headFixture, expectedData) {
		return
	}

	// HEAD 固定数据需可解码且往返一致。
	t.Run("HEAD", func(t *testing.T) {
		// 目录中只能有一个文件。
		requireSingleMetadataFixture(t, filepath.Dir(headFixture))
		// 校验内容并验证往返。
		testMetadataFixture(t, headFixture, expected)
	})

	// 枚举历史版本目录。
	previousVersionDirs, err := filepath.Glob(filepath.Join(testDataDir, "v*"))
	if err != nil {
		t.Fatal(err)
	}
	// 没有历史数据则测试无意义。
	if len(previousVersionDirs) == 0 {
		t.Fatal("no previous-version compatibility fixtures found")
	}
	// 逐历史版本校验。
	for _, dir := range previousVersionDirs {
		// 以目录名作为子测试名。
		t.Run(filepath.Base(dir), func(t *testing.T) {
			// 目录中只能有一个文件。
			requireSingleMetadataFixture(t, dir)
			// 历史数据不比较具体对象，只校验可解码且往返一致。
			testMetadataFixture(t, filepath.Join(dir, "metadata.json"), nil)
		})
	}
}

// requireSingleMetadataFixture 断言目录中只含一个 metadata.json 文件。
func requireSingleMetadataFixture(t *testing.T, dir string) {
	// 标记为测试辅助函数。
	t.Helper()

	// 读取目录项。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 断言唯一文件且文件名正确。
	if len(entries) != 1 || entries[0].Name() != "metadata.json" {
		t.Fatalf("%s must contain only metadata.json", dir)
	}
}

// compatibilityTestMetadata 用 roundtrip 工具生成一个覆盖各字段的 DeviceMetadata 对象，
// 对时间等字段做确定性填充，再把关键字段改成可读的测试值。
func compatibilityTestMetadata(t *testing.T) *metadata.DeviceMetadata {
	// 标记为测试辅助函数。
	t.Helper()

	// 自定义填充函数，保证数据确定性且便于阅读。
	fillFuncs := map[reflect.Type]roundtrip.FillFunc{
		// TypeMeta 填空，由编码器按目标版本生成。
		reflect.TypeFor[*metav1.TypeMeta](): func(_ string, _ int, obj interface{}) {
			*obj.(*metav1.TypeMeta) = metav1.TypeMeta{}
		},
		// FieldsV1 使用空对象 "{}"。
		reflect.TypeFor[*metav1.FieldsV1](): func(_ string, _ int, obj interface{}) {
			obj.(*metav1.FieldsV1).SetRawString("{}")
		},
		// Time 使用确定性的年份时间。
		reflect.TypeFor[*metav1.Time](): func(_ string, i int, obj interface{}) {
			obj.(*metav1.Time).Time = time.Date(2000+i, 1, 1, 1, 1, 1, 0, time.UTC)
		},
		// MicroTime 额外带上微秒。
		reflect.TypeFor[*metav1.MicroTime](): func(_ string, i int, obj interface{}) {
			obj.(*metav1.MicroTime).Time = time.Date(2000+i, 1, 1, 1, 1, 1, i*int(time.Microsecond), time.UTC)
		},
	}
	// 生成测试对象。
	obj, err := roundtrip.CompatibilityTestObject(
		metadataScheme,
		metadata.SchemeGroupVersion.WithKind("DeviceMetadata"),
		fillFuncs,
	)
	// 生成失败直接终止。
	if err != nil {
		t.Fatal(err)
	}
	// 以下赋值不是必须，但能让生成的数据更易读、语义合理。
	// 类型断言为内部元数据对象。
	dm := obj.(*metadata.DeviceMetadata)
	// 清空 TypeMeta，由编码器填充。
	dm.TypeMeta = metav1.TypeMeta{}
	// 设置可读的 claim、request 与设备字段。
	dm.Name = "claim"
	dm.Namespace = "default"
	dm.UID = "claim-uid"
	dm.Requests[0].Name = "request"
	dm.Requests[0].Devices[0].Driver = "gpu.example.com"
	dm.Requests[0].Devices[0].Pool = "worker-0"
	dm.Requests[0].Devices[0].Name = "gpu-0"
	// 把属性收敛为一个可读的 model 属性。
	for _, attribute := range dm.Requests[0].Devices[0].Attributes {
		// 属性值。
		model := "LATEST-GPU-MODEL"
		attribute.StringValue = &model
		// 只保留一个属性。
		dm.Requests[0].Devices[0].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"model": attribute,
		}
		// 只需处理第一个属性。
		break
	}
	return dm
}

// registeredMetadataVersions 从 metadataScheme 中收集所有已注册的外部 DeviceMetadata 版本，
// 并按 Kubernetes 版本比较规则降序排列。
func registeredMetadataVersions() []schema.GroupVersion {
	// 用集合去重。
	versionSet := map[schema.GroupVersion]struct{}{}
	// 遍历所有已注册类型。
	for gvk := range metadataScheme.AllKnownTypes() {
		// 只保留目标组、外部版本、DeviceMetadata 类型。
		if gvk.Group != metadata.GroupName || gvk.Version == runtime.APIVersionInternal || gvk.Kind != "DeviceMetadata" {
			continue
		}
		// 记录版本。
		versionSet[gvk.GroupVersion()] = struct{}{}
	}

	// 把集合转为切片。
	versions := make([]schema.GroupVersion, 0, len(versionSet))
	// 逐个加入切片。
	for gv := range versionSet {
		versions = append(versions, gv)
	}
	// 按 Kubernetes 版本规则降序排序。
	sort.Slice(versions, func(i, j int) bool {
		return version.CompareKubeAwareVersionStrings(versions[i].Version, versions[j].Version) > 0
	})
	return versions
}

// encodeCompatibilityMetadata 用 newMetadataWriter 把对象按给定版本编码为 JSON 流，供固定数据比较。
func encodeCompatibilityMetadata(t *testing.T, dm *metadata.DeviceMetadata, versions []schema.GroupVersion) []byte {
	// 标记为测试辅助函数。
	t.Helper()

	// 创建写入器（目录参数为空即可，只用到编码方法）。
	writer, err := newMetadataWriter(
		"gpu.example.com",
		"",
		"",
		versions,
		defaultMetadataFileOperations(MetadataFileOperations{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	// 编码为流。
	data, err := writer.encodeMetadataStream(dm)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// matchCompatibilityFixture 比较文件内容与期望字节；不匹配时报错。
// 若设置了 UPDATE_COMPATIBILITY_FIXTURE_DATA=true，则写入期望数据并提示复核提交。
// 返回内容是否完全匹配。
func matchCompatibilityFixture(t *testing.T, path string, expected []byte) bool {
	// 标记为测试辅助函数。
	t.Helper()

	// 读取现有固定数据。
	actual, err := os.ReadFile(path)
	// 完全一致直接返回成功。
	if err == nil && bytes.Equal(actual, expected) {
		return true
	}
	// 非“不存在”的读取错误直接终止。
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	// 文件存在但内容不同：报告差异。
	if err == nil {
		t.Errorf("%s differs (-want +got):\n%s", path, cmp.Diff(string(actual), string(expected)))
	} else {
		// 文件不存在。
		t.Errorf("%s does not exist", path)
	}
	// 按需更新固定数据。
	if os.Getenv(updateCompatibilityFixtureData) == "true" {
		// 创建目录。
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		// 写入新数据。
		if err := os.WriteFile(path, expected, 0644); err != nil {
			t.Fatal(err)
		}
		// 提示复核并提交。
		t.Logf("wrote %s; verify, commit, and rerun tests", path)
	} else {
		// 否则提示如何更新固定数据。
		t.Logf("rerun with %s=true to update compatibility data", updateCompatibilityFixtureData)
	}
	// 本次不匹配。
	return false
}

// testMetadataFixture 校验固定数据文件：解码后（若提供期望对象则）进行比较，
// 再用文件中出现的版本重新编码并逐字节比较，确保往返无变化。
func testMetadataFixture(t *testing.T, path string, expected *metadata.DeviceMetadata) {
	// 标记为测试辅助函数。
	t.Helper()

	// 读取文件。
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// 不使用校验解码，因为固定数据中 managedFields[0].operation 是无效值 "operationValue"。
	// 准备解码目标。
	var decoded metadata.DeviceMetadata
	// 流式解码。
	if err := devicemetadata.DecodeMetadataFromStream(json.NewDecoder(bytes.NewReader(data)), &decoded); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	// 与期望对象做语义比较。
	if expected != nil && !apiequality.Semantic.DeepEqual(expected, &decoded) {
		t.Errorf("decoded metadata differs (-want +got):\n%s", cmp.Diff(expected, &decoded))
	}

	// 读取流中包含的版本。
	versions := metadataVersionsInStream(t, data)
	// 按相同版本重新编码。
	roundTripped := encodeCompatibilityMetadata(t, &decoded, versions)
	// 断言往返后字节完全一致。
	if !bytes.Equal(data, roundTripped) {
		t.Errorf("metadata changed after round trip (-want +got):\n%s", cmp.Diff(string(data), string(roundTripped)))
	}
}

// metadataVersionsInStream 逐段解码 JSON 流的 TypeMeta，提取其中包含的所有 GroupVersion。
func metadataVersionsInStream(t *testing.T, data []byte) []schema.GroupVersion {
	// 标记为测试辅助函数。
	t.Helper()

	// 结果列表。
	var versions []schema.GroupVersion
	// 用流式解码器逐段读取。
	decoder := json.NewDecoder(bytes.NewReader(data))
	// 流中还有对象。
	for decoder.More() {
		// 只解析 TypeMeta。
		var typeMeta metav1.TypeMeta
		// 解析失败直接终止。
		if err := decoder.Decode(&typeMeta); err != nil {
			t.Fatal(err)
		}
		// 断言对象类型。
		if typeMeta.Kind != "DeviceMetadata" {
			t.Fatalf("unexpected metadata kind %q", typeMeta.Kind)
		}
		// 解析 apiVersion。
		gv, err := schema.ParseGroupVersion(typeMeta.APIVersion)
		if err != nil {
			t.Fatal(err)
		}
		// 记录版本。
		versions = append(versions, gv)
	}
	// 空流无效。
	if len(versions) == 0 {
		t.Fatal("metadata stream is empty")
	}
	return versions
}
