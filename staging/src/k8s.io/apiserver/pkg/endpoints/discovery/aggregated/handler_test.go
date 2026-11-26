/*
Copyright 2022 The Kubernetes Authors.

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

package aggregated_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/randfill"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apidiscoveryv2beta1 "k8s.io/api/apidiscovery/v2beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/version"
	apidiscoveryv2conversion "k8s.io/apiserver/pkg/apis/apidiscovery/v2"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

var scheme = runtime.NewScheme()
var codecs = runtimeserializer.NewCodecFactory(scheme)

const discoveryPath = "/apis"

func init() {
	utilruntime.Must(apidiscoveryv2.AddToScheme(scheme))
	utilruntime.Must(apidiscoveryv2beta1.AddToScheme(scheme))
	// 注册 apidiscovery 的转换
	utilruntime.Must(apidiscoveryv2conversion.RegisterConversions(scheme))
	codecs = runtimeserializer.NewCodecFactory(scheme)
}

// fuzzAPIGroups 随机生成一组 APIGroupDiscovery 用于测试。
//
// 额外约束：每个组至少有一个版本（否则 ResourceManager 会跳过该组），版本按
// Kubernetes 版本序降序排列，并清空 TypeMeta 只保留 Name，保证输出可预期。
func fuzzAPIGroups(atLeastNumGroups, maxNumGroups int, seed int64) apidiscoveryv2.APIGroupDiscoveryList {
	fuzzer := randfill.NewWithSeed(seed)
	fuzzer.NumElements(atLeastNumGroups, maxNumGroups)
	fuzzer.NilChance(0)
	fuzzer.Funcs(func(o *apidiscoveryv2.APIGroupDiscovery, c randfill.Continue) {
		c.FillNoCustom(o)

		// 若 ResourceManager 中某组的 versions 列表为空，该组将不再被服务
		atLeastOne := apidiscoveryv2.APIVersionDiscovery{}
		c.Fill(&atLeastOne)
		o.Versions = append(o.Versions, atLeastOne)
		sort.Slice(o.Versions[:], func(i, j int) bool {
			return version.CompareKubeAwareVersionStrings(o.Versions[i].Version, o.Versions[j].Version) > 0
		})

		o.TypeMeta = metav1.TypeMeta{}
		var name string
		c.Fill(&name)
		o.ObjectMeta = metav1.ObjectMeta{
			Name: name,
		}
	})

	var apis []apidiscoveryv2.APIGroupDiscovery
	fuzzer.Fill(&apis)
	sort.Slice(apis[:], func(i, j int) bool {
		return apis[i].Name < apis[j].Name
	})

	return apidiscoveryv2.APIGroupDiscoveryList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "APIGroupDiscoveryList",
			APIVersion: "apidiscovery.k8s.io/v2",
		},
		Items: apis,
	}
}

// fetchPathV2Beta1 以 v2beta1 格式请求并解码响应，用于验证版本偏斜兼容。
func fetchPathV2Beta1(handler http.Handler, acceptPrefix string, path string, etag string) (*http.Response, []byte, *apidiscoveryv2beta1.APIGroupDiscoveryList) {
	acceptSuffix := ";g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList"
	r, bytes := fetchPathHelper(handler, acceptPrefix+acceptSuffix, path, etag)
	var decoded *apidiscoveryv2beta1.APIGroupDiscoveryList
	if len(bytes) > 0 {
		decoded = &apidiscoveryv2beta1.APIGroupDiscoveryList{}
		err := runtime.DecodeInto(codecs.UniversalDecoder(), bytes, decoded)
		if err != nil {
			panic(fmt.Sprintf("failed to decode response: %v", err))
		}

	}
	return r, bytes, decoded
}

// fetchPath 同时接受 v2 与 v2beta1 的 Accept 头，解码为 v2 结构。
func fetchPath(handler http.Handler, acceptPrefix string, path string, etag string) (*http.Response, []byte, *apidiscoveryv2.APIGroupDiscoveryList) {
	acceptSuffix := ";g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,"
	acceptSuffixV2Beta1 := ";g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList,"
	r, bytes := fetchPathHelper(handler, acceptPrefix+acceptSuffix+","+acceptPrefix+acceptSuffixV2Beta1, path, etag)
	var decoded *apidiscoveryv2.APIGroupDiscoveryList
	if len(bytes) > 0 {
		decoded = &apidiscoveryv2.APIGroupDiscoveryList{}
		err := runtime.DecodeInto(codecs.UniversalDecoder(), bytes, decoded)
		if err != nil {
			panic(fmt.Sprintf("failed to decode response: %v", err))
		}
	}
	return r, bytes, decoded
}

// fetchPathHelper 构造请求并执行，按需设置 If-None-Match（自动补引号）。
func fetchPathHelper(handler http.Handler, accept string, path string, etag string) (*http.Response, []byte) {
	// 期望返回 JSON 格式的 apis 组列表
	w := httptest.NewRecorder()
	req := httptest.NewRequest(request.MethodGet, path, nil)

	// 请求 JSON 响应
	req.Header.Set("Accept", accept)

	if etag != "" {
		// 若提供的 etag 未加引号则补上引号
		quoted := etag
		if !strings.HasPrefix(etag, "\"") {
			quoted = strconv.Quote(etag)
		}
		req.Header.Set("If-None-Match", quoted)
	}

	handler.ServeHTTP(w, req)

	bytes := w.Body.Bytes()
	return w.Result(), bytes
}

// 把所有内建 APIService 加入 manager 并检查输出
func TestBasicResponse(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")

	apis := fuzzAPIGroups(1, 3, 10)
	manager.SetGroups(apis.Items)

	response, body, decoded := fetchPath(manager, "application/json", discoveryPath, "")

	jsonFormatted, err := json.Marshal(&apis)
	require.NoError(t, err, "json marshal should always succeed")

	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")
	assert.Equal(t, "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList", response.Header.Get("Content-Type"), "Content-Type response header should be as requested in Accept header if supported")
	assert.NotEmpty(t, response.Header.Get("ETag"), "E-Tag should be set")

	assert.NoError(t, err, "decode should always succeed")
	assert.EqualValues(t, &apis, decoded, "decoded value should equal input")
	assert.Equal(t, string(jsonFormatted)+"\n", string(body), "response should be the api group list")
}

// 测试 protobuf 输出是否正确
func TestBasicResponseProtobuf(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")

	apis := fuzzAPIGroups(1, 3, 10)
	manager.SetGroups(apis.Items)

	response, _, decoded := fetchPath(manager, "application/vnd.kubernetes.protobuf", discoveryPath, "")
	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")
	assert.Equal(t, "application/vnd.kubernetes.protobuf;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList", response.Header.Get("Content-Type"), "Content-Type response header should be as requested in Accept header if supported")
	assert.NotEmpty(t, response.Header.Get("ETag"), "E-Tag should be set")
	assert.EqualValues(t, &apis, decoded, "decoded value should equal input")
}

// v2beta1 仍应被提供服务
func TestV2Beta1SkewSupport(t *testing.T) {
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, utilversion.MustParse("1.34"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.AggregatedDiscoveryRemoveBetaType, false)
	manager := discoveryendpoint.NewResourceManager("apis")

	apis := fuzzAPIGroups(1, 3, 10)
	manager.SetGroups(apis.Items)

	converted, err := scheme.ConvertToVersion(&apis, &schema.GroupVersion{Group: "apidiscovery.k8s.io", Version: "v2beta1"})
	if err != nil {
		t.Fatal(err)
	}

	v2beta1apis := converted.(*apidiscoveryv2beta1.APIGroupDiscoveryList)

	response, body, decoded := fetchPathV2Beta1(manager, "application/json", discoveryPath, "")

	jsonFormatted, err := json.Marshal(v2beta1apis)
	require.NoError(t, err, "json marshal should always succeed")

	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")
	assert.Equal(t, "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList", response.Header.Get("Content-Type"), "Content-Type response header should be as requested in Accept header if supported")
	assert.NotEmpty(t, response.Header.Get("ETag"), "E-Tag should be set")

	assert.NoError(t, err, "decode should always succeed")
	assert.EqualValues(t, v2beta1apis, decoded, "decoded value should equal input")
	assert.Equal(t, string(jsonFormatted)+"\n", string(body), "response should be the api group list")
}

// 测试与服务关联的 etag 只取决于 apiresources，
// 例如内容相同的多个 service 应具有相同的 etag。
func TestEtagConsistent(t *testing.T) {
	// 创建 2 个 manager，各添加一批 service
	manager1 := discoveryendpoint.NewResourceManager("apis")
	manager2 := discoveryendpoint.NewResourceManager("apis")

	apis := fuzzAPIGroups(1, 3, 11)
	manager1.SetGroups(apis.Items)
	manager2.SetGroups(apis.Items)

	// 确认两者的 etag 相同
	res1_initial, _, _ := fetchPath(manager1, "application/json", discoveryPath, "")
	res2_initial, _, _ := fetchPath(manager2, "application/json", discoveryPath, "")

	assert.NotEmpty(t, res1_initial.Header.Get("ETag"), "Etag should be populated")
	assert.NotEmpty(t, res2_initial.Header.Get("ETag"), "Etag should be populated")
	assert.Equal(t, res1_initial.Header.Get("ETag"), res2_initial.Header.Get("ETag"), "etag should be deterministic")

	// 然后只向其中一个添加一个 service。
	// 确认它的 etag 变化而另一个不变
	apis = fuzzAPIGroups(1, 1, 11)
	for _, group := range apis.Items {
		for _, version := range group.Versions {
			manager1.AddGroupVersion(group.Name, version)
		}
	}

	res1_addedToOne, _, _ := fetchPath(manager1, "application/json", discoveryPath, "")
	res2_addedToOne, _, _ := fetchPath(manager2, "application/json", discoveryPath, "")

	assert.NotEmpty(t, res1_addedToOne.Header.Get("ETag"), "Etag should be populated")
	assert.NotEmpty(t, res2_addedToOne.Header.Get("ETag"), "Etag should be populated")
	assert.NotEqual(t, res1_initial.Header.Get("ETag"), res1_addedToOne.Header.Get("ETag"), "ETag should be changed since version was added")
	assert.Equal(t, res2_initial.Header.Get("ETag"), res2_addedToOne.Header.Get("ETag"), "ETag should be unchanged since data was unchanged")

	// 再把该 service 添加到另一个
	// 确认 etag 相同
	for _, group := range apis.Items {
		for _, version := range group.Versions {
			manager2.AddGroupVersion(group.Name, version)
		}
	}

	res1_addedToBoth, _, _ := fetchPath(manager1, "application/json", discoveryPath, "")
	res2_addedToBoth, _, _ := fetchPath(manager2, "application/json", discoveryPath, "")

	assert.NotEmpty(t, res1_addedToOne.Header.Get("ETag"), "Etag should be populated")
	assert.NotEmpty(t, res2_addedToOne.Header.Get("ETag"), "Etag should be populated")
	assert.Equal(t, res1_addedToBoth.Header.Get("ETag"), res2_addedToBoth.Header.Get("ETag"), "ETags should be equal since content is equal")
	assert.NotEqual(t, res2_initial.Header.Get("ETag"), res2_addedToBoth.Header.Get("ETag"), "ETag should be changed since data was changed")

	// 从两者移除该组版本，初始 E-Tag 应恢复
	for _, group := range apis.Items {
		for _, version := range group.Versions {
			manager1.RemoveGroupVersion(metav1.GroupVersion{
				Group:   group.Name,
				Version: version.Version,
			})
			manager2.RemoveGroupVersion(metav1.GroupVersion{
				Group:   group.Name,
				Version: version.Version,
			})
		}
	}

	res1_removeFromBoth, _, _ := fetchPath(manager1, "application/json", discoveryPath, "")
	res2_removeFromBoth, _, _ := fetchPath(manager2, "application/json", discoveryPath, "")

	assert.NotEmpty(t, res1_addedToOne.Header.Get("ETag"), "Etag should be populated")
	assert.NotEmpty(t, res2_addedToOne.Header.Get("ETag"), "Etag should be populated")
	assert.Equal(t, res1_removeFromBoth.Header.Get("ETag"), res2_removeFromBoth.Header.Get("ETag"), "ETags should be equal since content is equal")
	assert.Equal(t, res1_initial.Header.Get("ETag"), res1_removeFromBoth.Header.Get("ETag"), "ETag should be equal to initial value since added content was removed")
}

// 测试请求携带不匹配 E-Tag 的 If-None-Match 头时，返回最新内容。
func TestEtagNonMatching(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")
	apis := fuzzAPIGroups(1, 3, 12)
	manager.SetGroups(apis.Items)

	// 先获取一次文档
	initial, _, _ := fetchPath(manager, "application/json", discoveryPath, "")
	assert.NotEmpty(t, initial.Header.Get("ETag"), "ETag should be populated")

	// 用错误的 e-tag 再发一次请求，应返回同样的响应
	second, _, _ := fetchPath(manager, "application/json", discoveryPath, "wrongetag")

	assert.Equal(t, http.StatusOK, initial.StatusCode, "response should be 200 OK")
	assert.Equal(t, http.StatusOK, second.StatusCode, "response should be 200 OK")
	assert.Equal(t, initial.Header.Get("ETag"), second.Header.Get("ETag"), "ETag of both requests should be equal")
}

// 测试请求携带匹配 E-Tag 的 If-None-Match 头时，返回 304 Not Modified
func TestEtagMatching(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")
	apis := fuzzAPIGroups(1, 3, 12)
	manager.SetGroups(apis.Items)

	// 先获取一次文档
	initial, initialBody, _ := fetchPath(manager, "application/json", discoveryPath, "")
	assert.NotEmpty(t, initial.Header.Get("ETag"), "ETag should be populated")
	assert.NotEmpty(t, initialBody, "body should not be empty")

	// 用错误的 e-tag 再发一次请求，应返回同样的响应
	second, secondBody, _ := fetchPath(manager, "application/json", discoveryPath, initial.Header.Get("ETag"))

	assert.Equal(t, http.StatusOK, initial.StatusCode, "initial response should be 200 OK")
	assert.Equal(t, http.StatusNotModified, second.StatusCode, "second response should be 304 Not Modified")
	assert.Equal(t, initial.Header.Get("ETag"), second.Header.Get("ETag"), "ETag of both requests should be equal")
	assert.Empty(t, secondBody, "body should be empty when returning 304 Not Modified")
}

// 测试请求携带过期 E-Tag 的 If-None-Match 头时，返回最新内容
func TestEtagOutdated(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")
	apis := fuzzAPIGroups(1, 3, 15)
	manager.SetGroups(apis.Items)

	// 先获取一次文档
	initial, initialBody, _ := fetchPath(manager, "application/json", discoveryPath, "")
	assert.NotEmpty(t, initial.Header.Get("ETag"), "ETag should be populated")
	assert.NotEmpty(t, initialBody, "body should not be empty")

	// 然后添加一些 service 使 etag 变化
	apis = fuzzAPIGroups(1, 3, 14)
	for _, group := range apis.Items {
		for _, version := range group.Versions {
			manager.AddGroupVersion(group.Name, version)
		}
	}

	// 用旧的 e-tag 再发一次请求，响应不应是 304 Not Modified
	second, secondBody, _ := fetchPath(manager, "application/json", discoveryPath, initial.Header.Get("ETag"))

	assert.Equal(t, http.StatusOK, initial.StatusCode, "initial response should be 200 OK")
	assert.Equal(t, http.StatusOK, second.StatusCode, "second response should be 304 Not Modified")
	assert.NotEqual(t, initial.Header.Get("ETag"), second.Header.Get("ETag"), "ETag of both requests should be unequal since contents differ")
	assert.NotEmpty(t, secondBody, "body should be not empty when returning 304 Not Modified")
}

// 测试可以添加或移除 api service
func TestAddRemove(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")
	apis := fuzzAPIGroups(1, 3, 15)
	for _, group := range apis.Items {
		for _, version := range group.Versions {
			manager.AddGroupVersion(group.Name, version)
		}
	}

	_, _, initialDocument := fetchPath(manager, "application/json", discoveryPath, "")

	for _, group := range apis.Items {
		for _, version := range group.Versions {
			manager.RemoveGroupVersion(metav1.GroupVersion{
				Group:   group.Name,
				Version: version.Version,
			})
		}
	}

	_, _, secondDocument := fetchPath(manager, "application/json", discoveryPath, "")

	require.NotNil(t, initialDocument, "initial document should parse")
	require.NotNil(t, secondDocument, "second document should parse")
	assert.Len(t, initialDocument.Items, len(apis.Items), "initial document should have set number of groups")
	assert.Empty(t, secondDocument.Items, "second document should have no groups")
}

// 说明更新已有 service 时是替换条目而不是新增条目
func TestUpdateService(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")
	apis := fuzzAPIGroups(1, 3, 15)
	for _, group := range apis.Items {
		for _, version := range group.Versions {
			manager.AddGroupVersion(group.Name, version)
		}
	}

	_, _, initialDocument := fetchPath(manager, "application/json", discoveryPath, "")

	assert.Equal(t, initialDocument, &apis, "should have returned expected document")

	b, err := json.Marshal(apis)
	if err != nil {
		t.Error(err)
	}
	var newapis apidiscoveryv2.APIGroupDiscoveryList
	err = json.Unmarshal(b, &newapis)
	if err != nil {
		t.Error(err)
	}

	newapis.Items[0].Versions[0].Resources[0].Resource = "changed a resource name!"
	for _, group := range newapis.Items {
		for _, version := range group.Versions {
			manager.AddGroupVersion(group.Name, version)
		}
	}

	_, _, secondDocument := fetchPath(manager, "application/json", discoveryPath, "")
	assert.Equal(t, secondDocument, &newapis, "should have returned expected document")
	assert.NotEqual(t, secondDocument, initialDocument, "should have returned expected document")
}

// TestMultipleSources 验证多个 source 的组版本按优先级合并后统一输出：
// 通过 SetGroupVersionPriority 指定顺序，避免依赖版本名的字母序。
func TestMultipleSources(t *testing.T) {
	type pair struct {
		manager discoveryendpoint.ResourceManager
		apis    apidiscoveryv2.APIGroupDiscoveryList
	}

	pairs := []pair{}

	defaultManager := discoveryendpoint.NewResourceManager("apis")
	for i := 0; i < 10; i++ {
		name := discoveryendpoint.Source(100 * i)
		manager := defaultManager.WithSource(name)
		apis := fuzzAPIGroups(1, 3, int64(15+i))

		// 给各组确定性的名称
		for i := range apis.Items {
			apis.Items[i].Name = fmt.Sprintf("%v.%v.com", i, name)
		}

		pairs = append(pairs, pair{manager, apis})
	}

	expectedResult := []apidiscoveryv2.APIGroupDiscovery{}

	groupCounter := 0
	for _, p := range pairs {
		for gi, g := range p.apis.Items {
			for vi, v := range g.Versions {
				p.manager.AddGroupVersion(g.Name, v)

				// 用下标作为优先级，这样无需任何排序
				// 用负下标，因为排序是降序
				p.manager.SetGroupVersionPriority(metav1.GroupVersion{Group: g.Name, Version: v.Version}, -gi-groupCounter, -vi)
			}

			expectedResult = append(expectedResult, g)
		}

		groupCounter += len(p.apis.Items)
	}

	// 说明发现文档与预期一致
	_, _, initialDocument := fetchPath(defaultManager, "application/json", discoveryPath, "")

	require.Len(t, initialDocument.Items, len(expectedResult))
	require.Equal(t, expectedResult, initialDocument.Items)
}

// 说明当多个数据源（含 Default 源）使用相同组名时，
// 实际采用由 "Default" 源添加的组
func TestSourcePrecedence(t *testing.T) {
	defaultManager := discoveryendpoint.NewResourceManager("apis")
	otherManager := defaultManager.WithSource(500)
	apis := fuzzAPIGroups(1, 3, int64(15))
	for _, g := range apis.Items {
		for i, v := range g.Versions {
			v.Freshness = apidiscoveryv2.DiscoveryFreshnessCurrent
			g.Versions[i] = v
			otherManager.AddGroupVersion(g.Name, v)
		}
	}

	_, _, initialDocument := fetchPath(defaultManager, "application/json", discoveryPath, "")
	require.Equal(t, apis.Items, initialDocument.Items)

	// 把第一个 groupversion 加到 default 下。
	// 发现文档中除它以外不应出现其他版本
	overrideVersion := initialDocument.Items[0].Versions[0]
	overrideVersion.Freshness = apidiscoveryv2.DiscoveryFreshnessStale
	defaultManager.AddGroupVersion(initialDocument.Items[0].Name, overrideVersion)

	_, _, maskedDocument := fetchPath(defaultManager, "application/json", discoveryPath, "")
	masked := initialDocument.DeepCopy()
	masked.Items[0].Versions[0].Freshness = apidiscoveryv2.DiscoveryFreshnessStale

	require.Equal(t, masked.Items, maskedDocument.Items)

	// 清空 default 组。由于该组不再被 default 源覆盖，
	// 其他源中同组的其他版本现在应当出现
	defaultManager.RemoveGroup(apis.Items[0].Name)

	_, _, resetDocument := fetchPath(defaultManager, "application/json", discoveryPath, "")
	require.Equal(t, resetDocument.Items, initialDocument.Items)
}

// 说明 discovery manager 能在数据不变的情况下
// 同时服务多个用户的请求
func TestConcurrentRequests(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")
	apis := fuzzAPIGroups(1, 3, 15)
	manager.SetGroups(apis.Items)

	waitGroup := sync.WaitGroup{}

	numReaders := 100
	numRequestsPerReader := 100

	// 启动一批读取者，持续向 server 发送请求
	for i := 0; i < numReaders; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			etag := ""
			for j := 0; j < numRequestsPerReader; j++ {
				usedEtag := etag
				if j%2 == 0 {
					// 每隔一个请求禁用一次 etag
					usedEtag = ""
				}
				response, body, document := fetchPath(manager, "application/json", discoveryPath, usedEtag)

				if usedEtag != "" {
					assert.Equal(t, http.StatusNotModified, response.StatusCode, "response should be Not Modified if etag was used")
					assert.Empty(t, body, "body should be empty if etag used")
				} else {
					assert.Equal(t, http.StatusOK, response.StatusCode, "response should be OK if etag was unused")
					assert.Equal(t, &apis, document, "document should be equal")
				}

				etag = response.Header.Get("ETag")
			}
		}()
	}
	waitGroup.Wait()
}

// 说明该 handler 能同时服务大量并发读者与写者而不出问题。
// 由于这里没有多少"正确性"断言，适合配合 go '-race' 检测器运行
func TestAbuse(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")

	numReaders := 100
	numRequestsPerReader := 1000

	numWriters := 10
	numWritesPerWriter := 1000

	waitGroup := sync.WaitGroup{}

	// 启动一批写者，随机添加组、移除组并重置组列表
	for i := 0; i < numWriters; i++ {
		source := rand.NewSource(int64(i))

		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			// 记录已添加的组，以便随机移除
			var addedGroups []metav1.GroupVersion

			for j := 0; j < numWritesPerWriter; j++ {
				switch source.Int63() % 3 {
				case 0:
					// 添加一个经过 fuzz 的 group。
					apis := fuzzAPIGroups(1, 2, 15)
					for _, group := range apis.Items {
						for _, version := range group.Versions {
							manager.AddGroupVersion(group.Name, version)
							addedGroups = append(addedGroups, metav1.GroupVersion{
								Group:   group.Name,
								Version: version.Version,
							})
						}
					}
				case 1:
					// 移除一个我们已添加的组
					if len(addedGroups) > 0 {
						manager.RemoveGroupVersion(addedGroups[0])
						addedGroups = addedGroups[1:]
					} else {
						// 发送一次请求，并尝试移除别人可能刚添加的组
						_, _, document := fetchPath(manager, "application/json", discoveryPath, "")
						assert.NotNil(t, document, "manager should always succeed in returning a document")

						if len(document.Items) > 0 {
							manager.RemoveGroupVersion(metav1.GroupVersion{
								Group:   document.Items[0].Name,
								Version: document.Items[0].Versions[0].Version,
							})
						}

					}
				case 2:
					manager.SetGroups(nil)
					addedGroups = nil
				default:
					panic("unreachable")
				}
			}
		}()
	}

	// 启动一批读取者持续向 server 发送请求，
	// 并确认响应合理
	for i := 0; i < numReaders; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			etag := ""
			for j := 0; j < numRequestsPerReader; j++ {
				response, body, document := fetchPath(manager, "application/json", discoveryPath, etag)

				if response.StatusCode == http.StatusNotModified {
					assert.Equal(t, etag, response.Header.Get("ETag"))
					assert.Empty(t, body, "body should be empty if etag used")
					assert.Nil(t, document)
				} else {
					assert.Equal(t, http.StatusOK, response.StatusCode, "response should be OK if etag was unused")
					assert.NotNil(t, document)
				}

				etag = response.Header.Get("ETag")
			}
		}()
	}

	waitGroup.Wait()
}

// TestVersionSortingNoPriority 验证未设置优先级时按 Kubernetes 版本序排序
// （v2 > v1 > v2beta1 > v1beta1 > v1alpha1）。
func TestVersionSortingNoPriority(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")

	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1alpha1",
	})
	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v2beta1",
	})
	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1",
	})
	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1beta1",
	})
	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v2",
	})

	response, _, decoded := fetchPath(manager, "application/json", discoveryPath, "")
	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")

	versions := decoded.Items[0].Versions

	// 确保 v1 排在 v1alpha1 之前
	assert.Equal(t, "v2", versions[0].Version)
	assert.Equal(t, "v1", versions[1].Version)
	assert.Equal(t, "v2beta1", versions[2].Version)
	assert.Equal(t, "v1beta1", versions[3].Version)
	assert.Equal(t, "v1alpha1", versions[4].Version)
}

// TestVersionSortingWithPriority 验证显式设置的版本优先级可以覆盖默认排序。
func TestVersionSortingWithPriority(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")

	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: "default", Version: "v1"}, 1000, 100)
	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1alpha1",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: "default", Version: "v1alpha1"}, 1000, 200)

	response, _, decoded := fetchPath(manager, "application/json", discoveryPath, "")
	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")

	versions := decoded.Items[0].Versions

	// 确保通过设置组版本优先级可以覆盖逆字母序排序。
	assert.Equal(t, "v1alpha1", versions[0].Version)
	assert.Equal(t, "v1", versions[1].Version)
}

// 若两个 apiservice 声明的组优先级冲突，取较高的那个。
func TestGroupVersionSortingConflictingPriority(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")

	manager.AddGroupVersion("default", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: "default", Version: "v1"}, 1000, 100)
	manager.AddGroupVersion("test", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1alpha1",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: "test", Version: "v1alpha1"}, 500, 100)
	manager.AddGroupVersion("test", apidiscoveryv2.APIVersionDiscovery{
		Version: "v1alpha2",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: "test", Version: "v1alpha1"}, 2000, 100)

	response, _, decoded := fetchPath(manager, "application/json", discoveryPath, "")
	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")

	groups := decoded.Items

	// 确保通过设置组版本优先级可以覆盖逆字母序排序。
	assert.Equal(t, "test", groups[0].Name)
	assert.Equal(t, "default", groups[1].Name)
}

// 说明若先添加较低组版本再移除较高组版本，
// GroupPriorityMinimum 不会残留
func TestStatelessGroupPriorityMinimum(t *testing.T) {
	manager := discoveryendpoint.NewResourceManager("apis")

	stableGroup := "stable.example.com"
	experimentalGroup := "experimental.example.com"

	manager.AddGroupVersion(stableGroup, apidiscoveryv2.APIVersionDiscovery{
		Version: "v1",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: stableGroup, Version: "v1"}, 1000, 100)

	manager.AddGroupVersion(experimentalGroup, apidiscoveryv2.APIVersionDiscovery{
		Version: "v1",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: experimentalGroup, Version: "v1"}, 100, 100)

	manager.AddGroupVersion(experimentalGroup, apidiscoveryv2.APIVersionDiscovery{
		Version: "v1alpha1",
	})
	manager.SetGroupVersionPriority(metav1.GroupVersion{Group: experimentalGroup, Version: "v1alpha1"}, 10000, 100)

	// 预期采用 v1alpha1 的组优先级，并将其排在列表首位
	response, _, decoded := fetchPath(manager, "application/json", discoveryPath, "")
	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")
	assert.Equal(t, "experimental.example.com", decoded.Items[0].Name)
	assert.Equal(t, "stable.example.com", decoded.Items[1].Name)

	// 移除 v1alpha1 后，预期新的较低优先级生效
	manager.RemoveGroupVersion(metav1.GroupVersion{Group: experimentalGroup, Version: "v1alpha1"})

	response, _, decoded = fetchPath(manager, "application/json", discoveryPath, "")
	assert.Equal(t, http.StatusOK, response.StatusCode, "response should be 200 OK")

	assert.Equal(t, "stable.example.com", decoded.Items[0].Name)
	assert.Equal(t, "experimental.example.com", decoded.Items[1].Name)
}
