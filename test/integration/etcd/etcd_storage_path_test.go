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

package etcd

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	clientv3 "go.etcd.io/etcd/client/v3"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	componentbaseversion "k8s.io/component-base/version"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
)

// 只有具备 get/create 动词、但并不真正按该 kind 存储数据的虚拟资源才加入此列表；下游曾用它做映射。
var kindAllowList = sets.NewString()

// 所有测试共用的 namespace，请勿修改
const testNamespace = "etcdstoragepathtestnamespace"

// allowMissingTestdataFixtures 列出 API testdata 目录中允许缺少序列化 fixture 的 kind。
// 这里只能放自定义资源，以及已有 issue 跟踪补充 fixture 的内置类型；新增内置类型请改到 k8s.io/api/roundtrip_test.go。
var allowMissingTestdataFixtures = map[schema.GroupVersionKind]bool{
	// TODO(https://github.com/kubernetes/kubernetes/issues/79027)
	gvk("apiregistration.k8s.io", "v1", "APIService"):     true,
	gvk("apiregistration.k8s.io", "v1beta", "APIService"): true,

	// TODO(https://github.com/kubernetes/kubernetes/issues/79026)
	gvk("apiextensions.k8s.io", "v1beta1", "CustomResourceDefinition"): true,
	gvk("apiextensions.k8s.io", "v1", "CustomResourceDefinition"):      true,

	// 自定义资源无需在 k8s.io/api 中提供序列化 fixture
	gvk("awesome.bears.com", "v1", "Panda"):    true,
	gvk("cr.bar.com", "v1", "Foo"):             true,
	gvk("random.numbers.com", "v1", "Integer"): true,
	gvk("custom.fancy.com", "v2", "Pant"):      true,
}

// TestEtcdStoragePath 确保所有对象都存放在 etcd 中的预期位置。新增类型时测试会失败，强制你把新类型加入本测试；类型存储位置变动时也会失败，此时务必谨慎，因为这意味着除非为旧数据提供迁移路径，否则会破坏旧集群的升级。
func TestEtcdStoragePath(t *testing.T) {
	supportedVersions := GetSupportedEmulatedVersions()
	for _, v := range supportedVersions {
		t.Run(v, func(t *testing.T) {
			testEtcdStoragePathWithVersion(t, v)
		})
	}
}

func testEtcdStoragePathWithVersion(t *testing.T, v string) {
	if v == componentbaseversion.DefaultKubeBinaryVersion {
		featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			"AllAlpha": true,
			"AllBeta":  true,
		})
	} else {
		// 使用模拟版本时只测试 beta 和 GA API。
		featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, version.MustParse(v))
		featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, "AllBeta", true)
	}

	apiServer := StartRealAPIServerOrDie(t, func(opts *options.ServerRunOptions) {
		// 模拟旧版本时禁用 alpha 特性。
		if v != componentbaseversion.DefaultKubeBinaryVersion {
			opts.Options.GenericServerRunOptions.EmulationForwardCompatible = true
			opts.Options.APIEnablement.RuntimeConfig["api/alpha"] = "false"
		}
	})

	defer apiServer.Cleanup()
	defer dumpEtcdKVOnFailure(t, apiServer.KV)

	client := &allClient{dynamicClient: apiServer.Dynamic}

	if _, err := apiServer.Client.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	var etcdStorageData map[schema.GroupVersionResource]StorageData
	if v == componentbaseversion.DefaultKubeBinaryVersion {
		etcdStorageData = GetEtcdStorageDataForNamespaceServedAt("etcdstoragepathtestnamespace", v, false)
	} else {
		// 模拟旧版本时从 etcd 数据 fixture 中剔除 alpha 类型，因为模拟版本不支持 alpha。
		original := logExpectedVersionChange
		logExpectedVersionChange = t.Logf
		etcdStorageData = GetEtcdStorageDataForNamespaceServedAt("etcdstoragepathtestnamespace", v, true)
		logExpectedVersionChange = original
	}

	kindSeen := sets.NewString()
	pathSeen := map[string][]schema.GroupVersionResource{}
	etcdSeen := map[schema.GroupVersionResource]empty{}
	cohabitatingResources := map[string]map[schema.GroupVersionKind]empty{}

	for _, resourceToPersist := range apiServer.Resources {
		t.Run(resourceToPersist.Mapping.Resource.String(), func(t *testing.T) {
			mapping := resourceToPersist.Mapping
			gvk := resourceToPersist.Mapping.GroupVersionKind
			gvResource := resourceToPersist.Mapping.Resource
			kind := gvk.Kind

			if kindAllowList.Has(kind) {
				kindSeen.Insert(kind)
				t.Skip("allowlisted")
			}

			etcdSeen[gvResource] = empty{}
			testData, hasTest := etcdStorageData[gvResource]

			if !hasTest {
				t.Fatalf("no test data for %s.  Please add a test for your new type to GetEtcdStorageData().", gvResource)
			}

			if len(testData.ExpectedEtcdPath) == 0 {
				t.Fatalf("empty test data for %s", gvResource)
			}

			shouldCreate := len(testData.Stub) != 0 // 有 stub 时才尝试创建

			var (
				input *metaObject
				err   error
			)
			if shouldCreate {
				input = new(metaObject)
				if err = utiljson.Unmarshal([]byte(testData.Stub), input); err != nil || input.isEmpty() {
					t.Fatalf("invalid test data for %s: %v", gvResource, err)
				}
				// 清空 type meta 字段：只有 CRD 测试数据会设置它们，若设置了会让带 expectedGVK 覆盖的 CRD 测试在 DeepDerivative 比对时失败
				input.Kind = ""
				input.APIVersion = ""
			}

			all := &[]cleanupData{}
			defer func() {
				if !t.Failed() { // 测试失败时不清理，etcd dump 中可能需要保留现场
					if err := client.cleanup(all); err != nil {
						t.Fatalf("failed to clean up etcd: %#v", err)
					}
				}
			}()

			if err := client.createPrerequisites(apiServer.Mapper, testNamespace, testData.Prerequisites, all); err != nil {
				t.Fatalf("failed to create prerequisites for %s: %#v", gvResource, err)
			}

			if shouldCreate { // 没有 stub 时不尝试创建对象
				if err := client.create(testData.Stub, testNamespace, mapping, all); err != nil {
					t.Fatalf("failed to create stub for %s: %#v", gvResource, err)
				}
			}

			// 构建一个能解码存储中 JSON 和 CBOR 数据的 decoder。
			scheme := runtime.NewScheme()
			if testData.ExpectedGVK != nil {
				scheme.AddKnownTypeWithName(*testData.ExpectedGVK, &metaObject{})
			} else {
				scheme.AddKnownTypeWithName(gvk, &metaObject{})

			}
			decoder := recognizer.NewDecoder(
				cbor.NewSerializer(scheme, scheme),
				json.NewSerializerWithOptions(json.DefaultMetaFactory, scheme, scheme, json.SerializerOptions{}),
			)

			output, err := getFromEtcd(decoder, apiServer.KV, testData.ExpectedEtcdPath)
			if err != nil {
				t.Fatalf("failed to get from etcd for %s: %#v", gvResource, err)
			}

			expectedGVK := gvk
			if testData.ExpectedGVK != nil {
				expectedGVK = *testData.ExpectedGVK
			}

			// 如果旧版本曾提供该 group/kind 的非 alpha 版本，需确保当前存储版本能被旧版本识别
			fixtureFilenameGroup := expectedGVK.Group
			if fixtureFilenameGroup == "" {
				fixtureFilenameGroup = "core"
			}
			// 从各版本的序列化 fixture testdata 中找出该 group/kind 的所有版本
			releaseGroupKindFiles, err := filepath.Glob("../../../staging/src/k8s.io/api/testdata/*/" + fixtureFilenameGroup + ".*." + expectedGVK.Kind + ".yaml")
			if err != nil {
				t.Error(err)
			}
			if len(releaseGroupKindFiles) == 0 && !allowMissingTestdataFixtures[expectedGVK] {
				// 至少应能找到 HEAD 的 fixture
				t.Errorf("No testdata serialization files found for %#v, cannot determine if previous releases could read this group/kind. Add this group-version to k8s.io/api/roundtrip_test.go", expectedGVK)
			}

			// 找出当前及历史版本都能识别的非 alpha 版本
			currentNonAlphaVersions := sets.NewString()
			previousNonAlphaVersions := sets.NewString()
			for _, previousReleaseGroupKindFile := range releaseGroupKindFiles {
				parts := strings.Split(filepath.Base(previousReleaseGroupKindFile), ".")
				version := parts[len(parts)-3]
				if !strings.Contains(version, "alpha") {
					if serverVersion := filepath.Base(filepath.Dir(previousReleaseGroupKindFile)); serverVersion == "HEAD" {
						currentNonAlphaVersions.Insert(version)
					} else {
						previousNonAlphaVersions.Insert(version)
					}
				}
			}
			if len(currentNonAlphaVersions) > 0 && strings.Contains(expectedGVK.Version, "alpha") {
				t.Logf("Non-alpha versions %q exist, but the expected storage version is %q. Prefer beta or GA storage versions over alpha.",
					currentNonAlphaVersions.List(),
					expectedGVK.Version,
				)
			}
			if !strings.Contains(expectedGVK.Version, "alpha") && len(previousNonAlphaVersions) > 0 && !previousNonAlphaVersions.Has(expectedGVK.Version) {
				t.Errorf("Previous releases understand non-alpha versions %q, but do not understand the expected current non-alpha storage version %q. "+
					"This means a current server will store data in etcd that is not understood by a previous version.",
					previousNonAlphaVersions.List(),
					expectedGVK.Version,
				)
			}

			actualGVK := output.GroupVersionKind()
			if actualGVK != expectedGVK {
				t.Errorf("GVK for %s does not match, expected %s got %s", kind, expectedGVK, actualGVK)
			}

			if !apiequality.Semantic.DeepDerivative(input, output) {
				t.Errorf("Test stub for %s does not match: %s", kind, cmp.Diff(input, output))
			}

			addGVKToEtcdBucket(cohabitatingResources, actualGVK, getEtcdBucket(testData.ExpectedEtcdPath))
			pathSeen[testData.ExpectedEtcdPath] = append(pathSeen[testData.ExpectedEtcdPath], mapping.Resource)
		})
	}

	if inEtcdData, inEtcdSeen := diffMaps(etcdStorageData, etcdSeen); len(inEtcdData) != 0 || len(inEtcdSeen) != 0 {
		t.Errorf("etcd data does not match the types we saw:\nin etcd data but not seen:\n%s\nseen but not in etcd data:\n%s", inEtcdData, inEtcdSeen)
	}
	if inKindData, inKindSeen := diffMaps(kindAllowList, kindSeen); len(inKindData) != 0 || len(inKindSeen) != 0 {
		t.Errorf("kind allowlist data does not match the types we saw:\nin kind allowlist but not seen:\n%s\nseen but not in kind allowlist:\n%s", inKindData, inKindSeen)
	}

	for bucket, gvks := range cohabitatingResources {
		if len(gvks) != 1 {
			gvkStrings := []string{}
			for key := range gvks {
				gvkStrings = append(gvkStrings, keyStringer(key))
			}
			t.Errorf("cohabitating resources in etcd bucket %s have inconsistent GVKs\nyou may need to use DefaultStorageFactory.AddCohabitatingResources to sync the GVK of these resources:\n%s", bucket, gvkStrings)
		}
	}

	for path, gvrs := range pathSeen {
		if len(gvrs) != 1 {
			gvrStrings := []string{}
			for _, key := range gvrs {
				gvrStrings = append(gvrStrings, keyStringer(key))
			}
			t.Errorf("invalid test data, please ensure all expectedEtcdPath are unique, path %s has duplicate GVRs:\n%s", path, gvrStrings)
		}
	}
}

var debug = false

func dumpEtcdKVOnFailure(t *testing.T, kvClient clientv3.KV) {
	if t.Failed() && debug {
		response, err := kvClient.Get(context.Background(), "/", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}

		for _, kv := range response.Kvs {
			t.Error(string(kv.Key), "->", string(kv.Value))
		}
	}
}

func addGVKToEtcdBucket(cohabitatingResources map[string]map[schema.GroupVersionKind]empty, gvk schema.GroupVersionKind, bucket string) {
	if cohabitatingResources[bucket] == nil {
		cohabitatingResources[bucket] = map[schema.GroupVersionKind]empty{}
	}
	cohabitatingResources[bucket][gvk] = empty{}
}

// getEtcdBucket 假定 etcd 路径的最后一段是对象名，去掉该段即可得到对象的存储 "bucket"。同一 bucket 中的对象（共置资源）应使用相同 GVK 存储。
func getEtcdBucket(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		panic("path with no slashes " + path)
	}
	bucket := path[:idx]
	if len(bucket) == 0 {
		panic("invalid bucket for path " + path)
	}
	return bucket
}

// 选取少量稳定字段，便于人工核对输出。
type metaObject struct {
	metav1.TypeMeta `json:""`

	// ObjectMeta 的部分字段
	Metadata struct {
		Name      string `json:"name,omitempty"`
		Namespace string `json:"namespace,omitempty"`
	} `json:"metadata,omitempty"`
}

var _ runtime.Object = &metaObject{}

func (*metaObject) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

func (obj *metaObject) isEmpty() bool {
	return obj == nil || *obj == metaObject{} // 字段均为字符串，直接与零值比较即可
}

type empty struct{}

type cleanupData struct {
	obj      *unstructured.Unstructured
	resource schema.GroupVersionResource
}

func keyStringer(i interface{}) string {
	base := "\n\t"
	switch key := i.(type) {
	case string:
		return base + key
	case schema.GroupVersionResource:
		return base + key.String()
	case schema.GroupVersionKind:
		return base + key.String()
	default:
		panic("unexpected type")
	}
}

type allClient struct {
	dynamicClient dynamic.Interface
}

func (c *allClient) create(stub, ns string, mapping *meta.RESTMapping, all *[]cleanupData) error {
	resourceClient, obj, err := JSONToUnstructured(stub, ns, mapping, c.dynamicClient)
	if err != nil {
		return err
	}

	actual, err := resourceClient.Create(context.TODO(), obj, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	*all = append(*all, cleanupData{obj: actual, resource: mapping.Resource})

	return nil
}

func (c *allClient) cleanup(all *[]cleanupData) error {
	for i := len(*all) - 1; i >= 0; i-- { // 逆序删除，避免创建顺序有影响
		obj := (*all)[i].obj
		gvr := (*all)[i].resource

		if err := c.dynamicClient.Resource(gvr).Namespace(obj.GetNamespace()).Delete(context.TODO(), obj.GetName(), metav1.DeleteOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *allClient) createPrerequisites(mapper meta.RESTMapper, ns string, prerequisites []Prerequisite, all *[]cleanupData) error {
	for _, prerequisite := range prerequisites {
		gvk, err := mapper.KindFor(prerequisite.GvrData)
		if err != nil {
			return err
		}
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return err
		}
		if err := c.create(prerequisite.Stub, ns, mapping, all); err != nil {
			return err
		}
	}
	return nil
}

func getFromEtcd(decoder runtime.Decoder, keys clientv3.KV, path string) (*metaObject, error) {
	response, err := keys.Get(context.Background(), path)
	if err != nil {
		return nil, err
	}
	if response.More || response.Count != 1 || len(response.Kvs) != 1 {
		return nil, fmt.Errorf("Invalid etcd response (not found == %v): %#v", response.Count == 0, response)
	}
	var obj metaObject
	if err := runtime.DecodeInto(decoder, response.Kvs[0].Value, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

func diffMaps(a, b interface{}) ([]string, []string) {
	inA := diffMapKeys(a, b, keyStringer)
	inB := diffMapKeys(b, a, keyStringer)
	return inA, inB
}

func diffMapKeys(a, b interface{}, stringer func(interface{}) string) []string {
	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)
	ret := []string{}

	for _, ka := range av.MapKeys() {
		kat := ka.Interface()
		found := false
		for _, kb := range bv.MapKeys() {
			kbt := kb.Interface()
			if kat == kbt {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, stringer(kat))
		}
	}

	return ret
}
