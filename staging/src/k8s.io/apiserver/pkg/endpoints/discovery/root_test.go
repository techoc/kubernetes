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

package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
)

var (
	scheme = runtime.NewScheme()
	codecs = serializer.NewCodecFactory(scheme)
)

func init() {
	// 将 Unversioned 类型注册到它们专属的特殊组下
	scheme.AddUnversionedTypes(schema.GroupVersion{Group: "", Version: "v1"},
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

// decodeResponse 读取并反序列化响应体；t.Log 输出原始 JSON 便于失败时定位。
func decodeResponse(t *testing.T, resp *http.Response, obj interface{}) error {
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	t.Log(string(data))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, obj); err != nil {
		return err
	}
	return nil
}

// getGroupList 请求 discovery 根路径并解析为 APIGroupList。
func getGroupList(t *testing.T, server *httptest.Server) (*metav1.APIGroupList, error) {
	ctx := t.Context()
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, server.URL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected server response, expected %d, actual: %d", http.StatusOK, resp.StatusCode)
	}

	groupList := metav1.APIGroupList{}
	err = decodeResponse(t, resp, &groupList)
	return &groupList, err
}

// contextHandler 在请求上下文中注入 RequestInfo，模拟真实 apiserver 的过滤链，
// 因为发现处理器依赖它区分 /api 与 /apis 前缀。
func contextHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		resolver := &request.RequestInfoFactory{
			APIPrefixes:          sets.NewString("api", "apis"),
			GrouplessAPIPrefixes: sets.NewString("api"),
		}
		info, err := resolver.NewRequestInfo(req)
		if err == nil {
			ctx = request.WithRequestInfo(ctx, info)
		}
		req = req.WithContext(ctx)
		handler.ServeHTTP(w, req)
	})
}

// TestDiscoveryAtAPIS 验证 /apis 根发现端点的增删行为与服务地址回填。
// 空列表、加入组、移除组三步构成完整生命周期；
// ServerAddressByClientCIDRs 必须由 handler 的 addresses 计算得出，
// 因为客户端会根据来源 IP 选择可达的 apiserver 地址。
func TestDiscoveryAtAPIS(t *testing.T) {
	handler := NewRootAPIsHandler(DefaultAddresses{DefaultAddress: "192.168.1.1"}, codecs)

	server := httptest.NewServer(contextHandler(handler))

	groupList, err := getGroupList(t, server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Empty(t, groupList.Groups)

	// 添加一个 Group。
	extensionsGroupName := "extensions"
	extensionsVersions := []metav1.GroupVersionForDiscovery{
		{
			GroupVersion: extensionsGroupName + "/v1",
			Version:      "v1",
		},
	}
	extensionsPreferredVersion := metav1.GroupVersionForDiscovery{
		GroupVersion: extensionsGroupName + "/preferred",
		Version:      "preferred",
	}
	handler.AddGroup(metav1.APIGroup{
		Name:             extensionsGroupName,
		Versions:         extensionsVersions,
		PreferredVersion: extensionsPreferredVersion,
	})

	groupList, err = getGroupList(t, server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assert.Len(t, groupList.Groups, 1)
	groupListGroup := groupList.Groups[0]
	assert.Equal(t, extensionsGroupName, groupListGroup.Name)
	assert.Equal(t, extensionsVersions, groupListGroup.Versions)
	assert.Equal(t, extensionsPreferredVersion, groupListGroup.PreferredVersion)
	assert.Equal(t, handler.addresses.ServerAddressByClientCIDRs(utilnet.GetClientIP(&http.Request{})), groupListGroup.ServerAddressByClientCIDRs)

	// 移除该组。
	handler.RemoveGroup(extensionsGroupName)
	groupList, err = getGroupList(t, server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assert.Empty(t, groupList.Groups)
}

// TestDiscoveryOrdering 验证组列表的顺序语义。
// 发现文档的顺序对客户端有实际影响——它决定了 kubectl 等工具
// 遍历组的先后（例如同名资源的优先解析）。规则是「按注册顺序、不排序」；
// 重复注册不改变位置，移除后重新加入则排到末尾。
// 该用例同时断言内部 Groups() 与 HTTP 响应一致，防止两条路径实现分叉。
func TestDiscoveryOrdering(t *testing.T) {
	handler := NewRootAPIsHandler(DefaultAddresses{DefaultAddress: "192.168.1.1"}, codecs)

	server := httptest.NewServer(handler)
	groupList, err := getGroupList(t, server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Empty(t, groupList.Groups)

	// Register three groups
	handler.AddGroup(metav1.APIGroup{Name: "x"})
	handler.AddGroup(metav1.APIGroup{Name: "y"})
	handler.AddGroup(metav1.APIGroup{Name: "z"})
	// 再注册三个按字母序排在更前面的组
	handler.AddGroup(metav1.APIGroup{Name: "a"})
	handler.AddGroup(metav1.APIGroup{Name: "b"})
	handler.AddGroup(metav1.APIGroup{Name: "c"})
	// 确认重新添加不会重复注册，也不会让组丢失原有位置
	handler.AddGroup(metav1.APIGroup{Name: "x"})

	groupList, err = getGroupList(t, server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 检查内部 groups lister 返回的是否为同一个组。
	groups, err := handler.Groups(context.TODO(), &http.Request{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Len(t, groups, 6)

	assert.Len(t, groupList.Groups, 6)
	assert.Equal(t, "x", groupList.Groups[0].Name)
	assert.Equal(t, "y", groupList.Groups[1].Name)
	assert.Equal(t, "z", groupList.Groups[2].Name)
	assert.Equal(t, "a", groupList.Groups[3].Name)
	assert.Equal(t, "b", groupList.Groups[4].Name)
	assert.Equal(t, "c", groupList.Groups[5].Name)

	// 移除一个 group。
	handler.RemoveGroup("a")
	groupList, err = getGroupList(t, server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Len(t, groupList.Groups, 5)

	// 重新添加应把它移到末尾。
	handler.AddGroup(metav1.APIGroup{Name: "a"})
	groupList, err = getGroupList(t, server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Len(t, groupList.Groups, 6)
	assert.Equal(t, "x", groupList.Groups[0].Name)
	assert.Equal(t, "y", groupList.Groups[1].Name)
	assert.Equal(t, "z", groupList.Groups[2].Name)
	assert.Equal(t, "b", groupList.Groups[3].Name)
	assert.Equal(t, "c", groupList.Groups[4].Name)
	assert.Equal(t, "a", groupList.Groups[5].Name)
}
