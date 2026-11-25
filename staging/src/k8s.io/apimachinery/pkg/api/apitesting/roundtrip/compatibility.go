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

package roundtrip

import (
	"bytes"
	gojson "encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/util/sets"
)

// 兼容性测试用于保证“新旧版本的序列化数据可以被新代码解码、且重新编码后
// 字节不变”。选项结构既承载输入（scheme、测试数据目录、fuzz 函数），也在
// Complete 后承载补全结果（填充好的对象、序列化器）。
type CompatibilityTestOptions struct {
	// Scheme 用于创建待填充、待解码的对象以及构造序列化器，必填。
	Scheme *runtime.Scheme

	// TestDataDir 指向兼容性测试数据目录；未设置时 Complete() 填入 "testdata"。
	TestDataDir string

	// TestDataDirCurrentVersion 指向当前版本的兼容性测试数据目录；未设置时
	// Complete() 填入 "<TestDataDir>/HEAD"。目录中必须存在
	// `<group>.<version>.<kind>.[json|yaml|pb]` 文件，并验证其：
	// * 与 FilledObjects[GVK] 的序列化结果一致；
	// * 可无错误解码；
	// * 重新编码后逐字节不变；
	// * 解码到内存后语义等价。
	TestDataDirCurrentVersion string

	// TestDataDirsPreviousVersions 列出历史版本的兼容性数据目录；为 nil 时
	// Complete() 填入 "<TestDataDir>/v*" 目录。其中
	// `<group>.<version>.<kind>.[json|yaml|pb]` 文件可选；存在时验证其：
	// * 可无错误解码；
	// * 重新编码后逐字节不变，或在存在
	//   `<group>.<version>.<kind>.[json|yaml|pb].after_roundtrip.[json|yaml|pb]` 时与之匹配；
	// * 解码到内存后语义等价。
	TestDataDirsPreviousVersions []string

	// Kinds 是待测试的完整 Kind 列表；未设置时 Complete() 填入 Scheme.AllKnownTypes()。
	Kinds []schema.GroupVersionKind

	// FilledObjects 是校验 HEAD fixture 时可选用的预填充对象集合。
	// Complete() 为缺失 Kind 填入 CompatibilityTestObject(Kinds[*], Scheme, FillFuncs) 的结果。
	// 对象必须确定性地填充所有字段，且每次调用结果一致。
	FilledObjects map[schema.GroupVersionKind]runtime.Object

	// FillFuncs 是可选的类型自定义填充函数映射。
	FillFuncs map[reflect.Type]FillFunc

	// 以下三个序列化器在 Complete() 中按需初始化；外部注入可覆盖默认实现，
	// 以便测试不同的编码选项。
	JSON  runtime.Serializer
	YAML  runtime.Serializer
	Proto runtime.Serializer
}

// FillFunc 负责把 obj 的所有可序列化字段填充为可识别的内容；s/i 提供上下文
// 信息（如 json key 或 protobuf tag），让填充结果能反映字段位置，便于定位差异。
type FillFunc func(s string, i int, obj interface{})

// NewCompatibilityTestOptions 只保存 scheme；其余字段由 Complete 补全。
func NewCompatibilityTestOptions(scheme *runtime.Scheme) *CompatibilityTestOptions {
	return &CompatibilityTestOptions{Scheme: scheme}
}

// 这些是各 group 共用的通用选项/事件类型，只在 core group 测一次即可，
// 避免在所有 group 中重复生成与校验同一份数据。
var coreKinds = sets.NewString(
	"CreateOptions", "UpdateOptions", "PatchOptions", "DeleteOptions",
	"GetOptions", "ListOptions", "ExportOptions",
	"WatchEvent",
)

// Complete 校验并补全选项：填默认目录、枚举待测 kind、按需构造填充对象与
// 序列化器。必须在使用 Run 之前调用；所有失败都直接 t.Fatal，因为配置缺失
// 后继续跑测试只会产生误导性的结果。
func (c *CompatibilityTestOptions) Complete(t *testing.T) *CompatibilityTestOptions {
	t.Helper()

	// 校验 scheme。
	if c.Scheme == nil {
		t.Fatal("scheme is required")
	}

	// 补全测试数据目录。
	if c.TestDataDir == "" {
		c.TestDataDir = "testdata"
	}
	// 当前版本 fixture 目录固定为 <TestDataDir>/HEAD，复用 http.MethodHead 常量。
	if c.TestDataDirCurrentVersion == "" {
		c.TestDataDirCurrentVersion = filepath.Join(c.TestDataDir, http.MethodHead)
	}
	if c.TestDataDirsPreviousVersions == nil {
		// 历史版本目录按 v* 通配；排序保证测试顺序稳定。
		dirs, err := filepath.Glob(filepath.Join(c.TestDataDir, "v*"))
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(dirs)
		c.TestDataDirsPreviousVersions = dirs
	}

	// 补全待测 Kind。
	if len(c.Kinds) == 0 {
		gvks := []schema.GroupVersionKind{}
		for gvk := range c.Scheme.AllKnownTypes() {
			if gvk.Version == "" || gvk.Version == runtime.APIVersionInternal {
				// 仅测试外部类型；内部版本不对外序列化。
				continue
			}
			if strings.HasSuffix(gvk.Kind, "List") {
				// 跳过 List 类型；item 类型已间接覆盖，且可避免 fixture 数量激增。
				continue
			}
			if gvk.Group != "" && coreKinds.Has(gvk.Kind) {
				// 选项类型仅在核心 API 组测试。
				continue
			}
			gvks = append(gvks, gvk)
		}
		c.Kinds = gvks
	}

	// 按 group/version/kind 字典序排序，确保 fixture 生成与测试输出可复现。
	sort.Slice(c.Kinds, func(i, j int) bool {
		if c.Kinds[i].Group != c.Kinds[j].Group {
			return c.Kinds[i].Group < c.Kinds[j].Group
		}
		if c.Kinds[i].Version != c.Kinds[j].Version {
			return c.Kinds[i].Version < c.Kinds[j].Version
		}
		if c.Kinds[i].Kind != c.Kinds[j].Kind {
			return c.Kinds[i].Kind < c.Kinds[j].Kind
		}
		return false
	})

	// 填充缺失对象。
	if c.FilledObjects == nil {
		c.FilledObjects = map[schema.GroupVersionKind]runtime.Object{}
	}
	// 用户提供的 FillFunc 覆盖默认实现，缺省值仍保留以便填充其他字段。
	fillFuncs := defaultFillFuncs()
	for k, v := range c.FillFuncs {
		fillFuncs[k] = v
	}
	for _, gvk := range c.Kinds {
		if _, ok := c.FilledObjects[gvk]; ok {
			continue
		}
		obj, err := CompatibilityTestObject(c.Scheme, gvk, fillFuncs)
		if err != nil {
			t.Fatal(err)
		}
		c.FilledObjects[gvk] = obj
	}

	// 默认序列化器：JSON 带缩进、YAML 由 JSON 序列化器加 yaml 选项产生、
	// protobuf 用标准 serializer。Pretty 保证 fixture 人类可读、diff 友好。
	if c.JSON == nil {
		c.JSON = json.NewSerializerWithOptions(json.DefaultMetaFactory, c.Scheme, c.Scheme, json.SerializerOptions{Pretty: true})
	}
	if c.YAML == nil {
		c.YAML = json.NewSerializerWithOptions(json.DefaultMetaFactory, c.Scheme, c.Scheme, json.SerializerOptions{Yaml: true})
	}
	if c.Proto == nil {
		c.Proto = protobuf.NewSerializer(c.Scheme, c.Scheme)
	}

	return c
}

// Run 对每个 kind 跑当前版本与所有历史版本的兼容性检查，最后检查是否有
// 未被任何测试使用的 HEAD fixture（通常意味着类型被删除或重命名，需要清理）。
func (c *CompatibilityTestOptions) Run(t *testing.T) {
	usedHEADFixtures := sets.NewString()

	for _, gvk := range c.Kinds {
		t.Run(makeName(gvk), func(t *testing.T) {

			t.Run(http.MethodHead, func(t *testing.T) {
				c.runCurrentVersionTest(t, gvk, usedHEADFixtures)
			})

			for _, previousVersionDir := range c.TestDataDirsPreviousVersions {
				t.Run(filepath.Base(previousVersionDir), func(t *testing.T) {
					c.runPreviousVersionTest(t, gvk, previousVersionDir, nil)
				})
			}

		})
	}

	// HEAD fixture 必须全部使用，否则会积累过期数据。
	t.Run("unused_fixtures", func(t *testing.T) {
		files, err := os.ReadDir(c.TestDataDirCurrentVersion)
		if err != nil {
			t.Fatal(err)
		}
		allFixtures := sets.NewString()
		for _, file := range files {
			allFixtures.Insert(file.Name())
		}

		if unused := allFixtures.Difference(usedHEADFixtures); len(unused) > 0 {
			t.Fatalf("remove unused fixtures from %s:\n%s", c.TestDataDirCurrentVersion, strings.Join(unused.List(), "\n"))
		}
	})
}

// runCurrentVersionTest 校验 HEAD fixture 与当前代码序列化结果逐字节一致，
// 并验证反序列化后的语义等价。若不一致且设置了环境变量，则回写 fixture。
func (c *CompatibilityTestOptions) runCurrentVersionTest(t *testing.T, gvk schema.GroupVersionKind, usedFiles sets.String) {
	expectedObject := c.FilledObjects[gvk]
	expectedJSON, expectedYAML, expectedProto := c.encode(t, expectedObject)

	actualJSON, actualYAML, actualProto, err := read(c.TestDataDirCurrentVersion, gvk, "", usedFiles)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	needsUpdate := false
	if os.IsNotExist(err) {
		// 缺少 fixture 视为失败，但允许通过更新模式生成。
		t.Errorf("current version compatibility files did not exist: %v", err)
		needsUpdate = true
	} else {
		// 逐字节比较：任何编码变化（字段顺序、格式）都应显式暴露。
		if !bytes.Equal(expectedJSON, actualJSON) {
			t.Errorf("json differs")
			t.Log(cmp.Diff(string(actualJSON), string(expectedJSON)))
			needsUpdate = true
		}

		if !bytes.Equal(expectedYAML, actualYAML) {
			t.Errorf("yaml differs")
			t.Log(cmp.Diff(string(actualYAML), string(expectedYAML)))
			needsUpdate = true
		}

		if !bytes.Equal(expectedProto, actualProto) {
			t.Errorf("proto differs")
			needsUpdate = true
			// protobuf 前 4 字节是 runtime 前缀，跳过后再解码为可读文本。
			t.Log(cmp.Diff(dumpProto(t, actualProto[4:]), dumpProto(t, expectedProto[4:])))
			// t.Logf("json (for locating the offending field based on surrounding data): %s", string(expectedJSON))
		}
	}

	if needsUpdate {
		const updateEnvVar = "UPDATE_COMPATIBILITY_FIXTURE_DATA"
		if os.Getenv(updateEnvVar) == "true" {
			// 仅在显式要求时写盘，避免测试运行产生副作用。
			writeFile(t, c.TestDataDirCurrentVersion, gvk, "", "json", expectedJSON)
			writeFile(t, c.TestDataDirCurrentVersion, gvk, "", "yaml", expectedYAML)
			writeFile(t, c.TestDataDirCurrentVersion, gvk, "", "pb", expectedProto)
			t.Logf("wrote expected compatibility data... verify, commit, and rerun tests")
		} else {
			t.Logf("if the diff is expected because of a new type or a new field, re-run with %s=true to update the compatibility data", updateEnvVar)
		}
		return
	}

	emptyObj, err := c.Scheme.New(gvk)
	if err != nil {
		t.Fatal(err)
	}
	{
		// 内嵌 RawExtension 会保留缩进，先压缩以与 json.Unmarshal 行为一致。
		compacted := &bytes.Buffer{}
		if err := gojson.Compact(compacted, actualJSON); err != nil {
			t.Error(err)
		}

		jsonDecoded := emptyObj.DeepCopyObject()
		jsonDecoded, _, err = c.JSON.Decode(compacted.Bytes(), &gvk, jsonDecoded)
		if err != nil {
			t.Error(err)
		} else if !apiequality.Semantic.DeepEqual(expectedObject, jsonDecoded) {
			// 字节一致之外还要保证语义一致（如 nil 与空集合的规范化）。
			t.Errorf("expected and decoded json objects differed:\n%s", cmp.Diff(expectedObject, jsonDecoded))
		}
	}
	{
		yamlDecoded := emptyObj.DeepCopyObject()
		yamlDecoded, _, err = c.YAML.Decode(actualYAML, &gvk, yamlDecoded)
		if err != nil {
			t.Error(err)
		} else if !apiequality.Semantic.DeepEqual(expectedObject, yamlDecoded) {
			t.Errorf("expected and decoded yaml objects differed:\n%s", cmp.Diff(expectedObject, yamlDecoded))
		}
	}
	{
		protoDecoded := emptyObj.DeepCopyObject()
		protoDecoded, _, err = c.Proto.Decode(actualProto, &gvk, protoDecoded)
		if err != nil {
			t.Error(err)
		} else if !apiequality.Semantic.DeepEqual(expectedObject, protoDecoded) {
			t.Errorf("expected and decoded proto objects differed:\n%s", cmp.Diff(expectedObject, protoDecoded))
		}
	}
}

// encode 用三种序列化器把对象编码为字节，任一失败都直接终止测试。
func (c *CompatibilityTestOptions) encode(t *testing.T, obj runtime.Object) (json, yaml, proto []byte) {
	jsonBytes := bytes.NewBuffer(nil)
	if err := c.JSON.Encode(obj, jsonBytes); err != nil {
		t.Fatalf("error encoding json: %v", err)
	}
	yamlBytes := bytes.NewBuffer(nil)
	if err := c.YAML.Encode(obj, yamlBytes); err != nil {
		t.Fatalf("error encoding yaml: %v", err)
	}
	protoBytes := bytes.NewBuffer(nil)
	if err := c.Proto.Encode(obj, protoBytes); err != nil {
		t.Fatalf("error encoding proto: %v", err)
	}
	return jsonBytes.Bytes(), yamlBytes.Bytes(), protoBytes.Bytes()
}

// read 一次性读取某 kind 的 json/yaml/pb 三种 fixture。usedFiles 非 nil 时
// 记录被访问的文件名，用于事后检测未使用的 HEAD fixture。返回第一个读取错误，
// 调用方据此区分“文件不存在”（可跳过）与其他 IO 错误。
func read(dir string, gvk schema.GroupVersionKind, suffix string, usedFiles sets.String) (json, yaml, proto []byte, err error) {
	jsonFilename := makeName(gvk) + suffix + ".json"
	actualJSON, jsonErr := os.ReadFile(filepath.Join(dir, jsonFilename))
	yamlFilename := makeName(gvk) + suffix + ".yaml"
	actualYAML, yamlErr := os.ReadFile(filepath.Join(dir, yamlFilename))
	protoFilename := makeName(gvk) + suffix + ".pb"
	actualProto, protoErr := os.ReadFile(filepath.Join(dir, protoFilename))
	// 无论读取是否成功都登记文件名：只要被尝试消费过，就不算“未使用”。
	if usedFiles != nil {
		usedFiles.Insert(jsonFilename)
		usedFiles.Insert(yamlFilename)
		usedFiles.Insert(protoFilename)
	}
	if jsonErr != nil {
		return actualJSON, actualYAML, actualProto, jsonErr
	}
	if yamlErr != nil {
		return actualJSON, actualYAML, actualProto, yamlErr
	}
	if protoErr != nil {
		return actualJSON, actualYAML, actualProto, protoErr
	}
	return actualJSON, actualYAML, actualProto, nil
}

// writeFile 写入单个 fixture，自动创建目录；权限 0644/0755 与常规仓库文件一致。
func writeFile(t *testing.T, dir string, gvk schema.GroupVersionKind, suffix, extension string, data []byte) {
	if err := os.MkdirAll(dir, os.FileMode(0755)); err != nil {
		t.Fatal("error making directory", err)
	}
	if err := os.WriteFile(filepath.Join(dir, makeName(gvk)+suffix+"."+extension), data, os.FileMode(0644)); err != nil {
		t.Fatalf("error writing %s: %v", extension, err)
	}
}

// deleteFile 删除不再需要的 after_roundtrip fixture（内容与原始数据相同的情况）。
func deleteFile(t *testing.T, dir string, gvk schema.GroupVersionKind, suffix, extension string) {
	if err := os.Remove(filepath.Join(dir, makeName(gvk)+suffix+"."+extension)); err != nil {
		t.Fatalf("error removing %s: %v", extension, err)
	}
}

// runPreviousVersionTest 验证历史版本 fixture 可被新代码解码，并检查重新编码
// 后是否与预期一致。若存在 `<name>.after_roundtrip.*` 文件，则以它作为重编码
// 后的期望值（用于记录“新代码会规范化旧数据”的已知差异）；否则要求逐字节不变。
// 三者（json/yaml/pb）解码后必须语义一致，防止不同编码产生分歧。
func (c *CompatibilityTestOptions) runPreviousVersionTest(t *testing.T, gvk schema.GroupVersionKind, previousVersionDir string, usedFiles sets.String) {
	jsonBeforeRoundTrip, yamlBeforeRoundTrip, protoBeforeRoundTrip, err := read(previousVersionDir, gvk, "", usedFiles)
	if os.IsNotExist(err) || (len(jsonBeforeRoundTrip) == 0 && len(yamlBeforeRoundTrip) == 0 && len(protoBeforeRoundTrip) == 0) {
		// 历史 fixture 是可选的：不存在即跳过，不影响测试通过。
		t.SkipNow()
		return
	}
	if err != nil {
		t.Fatal(err)
	}

	emptyObj, err := c.Scheme.New(gvk)
	if err != nil {
		t.Fatal(err)
	}

	// 内嵌 RawExtension 保留缩进，解码前先压缩。
	compacted := &bytes.Buffer{}
	if err := gojson.Compact(compacted, jsonBeforeRoundTrip); err != nil {
		t.Fatal(err)
	}

	jsonDecoded := emptyObj.DeepCopyObject()
	jsonDecoded, _, err = c.JSON.Decode(compacted.Bytes(), &gvk, jsonDecoded)
	if err != nil {
		t.Fatal(err)
	}
	jsonBytes := bytes.NewBuffer(nil)
	if err := c.JSON.Encode(jsonDecoded, jsonBytes); err != nil {
		t.Fatalf("error encoding json: %v", err)
	}
	jsonAfterRoundTrip := jsonBytes.Bytes()

	yamlDecoded := emptyObj.DeepCopyObject()
	yamlDecoded, _, err = c.YAML.Decode(yamlBeforeRoundTrip, &gvk, yamlDecoded)
	if err != nil {
		t.Fatal(err)
	} else if !apiequality.Semantic.DeepEqual(jsonDecoded, yamlDecoded) {
		// 不同编码的解码结果必须一致，否则说明某个序列化器存在语义偏差。
		t.Errorf("decoded json and yaml objects differ:\n%s", cmp.Diff(jsonDecoded, yamlDecoded))
	}
	yamlBytes := bytes.NewBuffer(nil)
	if err := c.YAML.Encode(yamlDecoded, yamlBytes); err != nil {
		t.Fatalf("error encoding yaml: %v", err)
	}
	yamlAfterRoundTrip := yamlBytes.Bytes()

	protoDecoded := emptyObj.DeepCopyObject()
	protoDecoded, _, err = c.Proto.Decode(protoBeforeRoundTrip, &gvk, protoDecoded)
	if err != nil {
		t.Fatal(err)
	} else if !apiequality.Semantic.DeepEqual(jsonDecoded, protoDecoded) {
		t.Errorf("decoded json and proto objects differ:\n%s", cmp.Diff(jsonDecoded, protoDecoded))
	}
	protoBytes := bytes.NewBuffer(nil)
	if err := c.Proto.Encode(protoDecoded, protoBytes); err != nil {
		t.Fatalf("error encoding proto: %v", err)
	}
	protoAfterRoundTrip := protoBytes.Bytes()

	jsonNeedsRemove := false
	yamlNeedsRemove := false
	protoNeedsRemove := false

	// 读取可选的 after_roundtrip 覆盖文件；若它与原始 fixture 完全相同，则说明
	// 该文件已无意义，应删除以保持数据目录精简。
	expectedJSONAfterRoundTrip, expectedYAMLAfterRoundTrip, expectedProtoAfterRoundTrip, _ := read(previousVersionDir, gvk, ".after_roundtrip", usedFiles)
	if len(expectedJSONAfterRoundTrip) == 0 {
		expectedJSONAfterRoundTrip = jsonBeforeRoundTrip
	} else if bytes.Equal(jsonBeforeRoundTrip, expectedJSONAfterRoundTrip) {
		t.Errorf("JSON after_roundtrip file is identical and should be removed")
		jsonNeedsRemove = true
	}
	if len(expectedYAMLAfterRoundTrip) == 0 {
		expectedYAMLAfterRoundTrip = yamlBeforeRoundTrip
	} else if bytes.Equal(yamlBeforeRoundTrip, expectedYAMLAfterRoundTrip) {
		t.Errorf("YAML after_roundtrip file is identical and should be removed")
		yamlNeedsRemove = true
	}
	if len(expectedProtoAfterRoundTrip) == 0 {
		expectedProtoAfterRoundTrip = protoBeforeRoundTrip
	} else if bytes.Equal(protoBeforeRoundTrip, expectedProtoAfterRoundTrip) {
		t.Errorf("Proto after_roundtrip file is identical and should be removed")
		protoNeedsRemove = true
	}

	jsonNeedsUpdate := false
	yamlNeedsUpdate := false
	protoNeedsUpdate := false

	if !bytes.Equal(expectedJSONAfterRoundTrip, jsonAfterRoundTrip) {
		t.Errorf("json differs")
		t.Log(cmp.Diff(string(expectedJSONAfterRoundTrip), string(jsonAfterRoundTrip)))
		jsonNeedsUpdate = true
	}

	if !bytes.Equal(expectedYAMLAfterRoundTrip, yamlAfterRoundTrip) {
		t.Errorf("yaml differs")
		t.Log(cmp.Diff(string(expectedYAMLAfterRoundTrip), string(yamlAfterRoundTrip)))
		yamlNeedsUpdate = true
	}

	if !bytes.Equal(expectedProtoAfterRoundTrip, protoAfterRoundTrip) {
		t.Errorf("proto differs")
		protoNeedsUpdate = true
		// 跳过 4 字节前缀后解码为可读文本，便于定位差异。
		t.Log(cmp.Diff(dumpProto(t, expectedProtoAfterRoundTrip[4:]), dumpProto(t, protoAfterRoundTrip[4:])))
		// t.Logf("json (for locating the offending field based on surrounding data): %s", string(expectedJSON))
	}

	if jsonNeedsUpdate || yamlNeedsUpdate || protoNeedsUpdate || jsonNeedsRemove || yamlNeedsRemove || protoNeedsRemove {
		const updateEnvVar = "UPDATE_COMPATIBILITY_FIXTURE_DATA"
		if os.Getenv(updateEnvVar) == "true" {
			// 需要更新则写新覆盖文件，否则删除冗余文件。
			if jsonNeedsUpdate {
				writeFile(t, previousVersionDir, gvk, ".after_roundtrip", "json", jsonAfterRoundTrip)
			} else if jsonNeedsRemove {
				deleteFile(t, previousVersionDir, gvk, ".after_roundtrip", "json")
			}

			if yamlNeedsUpdate {
				writeFile(t, previousVersionDir, gvk, ".after_roundtrip", "yaml", yamlAfterRoundTrip)
			} else if yamlNeedsRemove {
				deleteFile(t, previousVersionDir, gvk, ".after_roundtrip", "yaml")
			}

			if protoNeedsUpdate {
				writeFile(t, previousVersionDir, gvk, ".after_roundtrip", "pb", protoAfterRoundTrip)
			} else if protoNeedsRemove {
				deleteFile(t, previousVersionDir, gvk, ".after_roundtrip", "pb")
			}
			t.Logf("wrote expected compatibility data... verify, commit, and rerun tests")
		} else {
			t.Logf("if the diff is expected because of a new type or a new field, re-run with %s=true to update the compatibility data", updateEnvVar)
		}
		return
	}
}

// makeName 生成 fixture 文件名前缀 `<group>.<version>.<kind>`；core group 用
// "core" 占位，避免出现前导点而成为隐藏文件。
func makeName(gvk schema.GroupVersionKind) string {
	g := gvk.Group
	if g == "" {
		g = "core"
	}
	return g + "." + gvk.Version + "." + gvk.Kind
}

// dumpProto 借助 protoc --decode_raw 把二进制 protobuf 转成可读文本用于 diff。
// protoc 不可用或解码失败时只记录日志并返回空串——诊断输出不应让测试失败。
func dumpProto(t *testing.T, data []byte) string {
	t.Helper()
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Log(err)
		return ""
	}
	cmd := exec.Command(protoc, "--decode_raw")
	cmd.Stdin = bytes.NewBuffer(data)
	d, err := cmd.CombinedOutput()
	if err != nil {
		t.Log(err)
		return ""
	}
	return string(d)
}
