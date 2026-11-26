/*
Copyright 2016 The Kubernetes Authors.

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

package fake

import (
	"context"
	"fmt"
	"net/http"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	kubeversion "k8s.io/client-go/pkg/version"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/testing"
)

// FakeDiscovery 实现 discovery.DiscoveryInterface 与 discovery.DiscoveryInterfaceWithContext。
//
// 它有时会用 action 调用 testing.Fake.Invoke，但不使用其返回值；
// 需要固定返回值（如 ServerVersion）时，直接设置结构体上的 Faked... 字段。
type FakeDiscovery struct {
	*testing.Fake
	FakedServerVersion *version.Info
}

var (
	//nolint:staticcheck // Intentionally using the old interface here.
	_ discovery.DiscoveryInterface            = &FakeDiscovery{}
	_ discovery.DiscoveryInterfaceWithContext = &FakeDiscovery{}
)

// ServerResourcesForGroupVersion 返回指定 group/version 支持的资源列表。
//
// ServerResourcesForGroupVersionWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerResourcesForGroupVersionWithContext。
//
// ServerResourcesForGroupVersion 返回指定 group/version 支持的资源列表；
// 新代码应使用带 context 的版本。
func (c *FakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return c.ServerResourcesForGroupVersionWithContext(context.Background(), groupVersion)
}

// ServerResourcesForGroupVersionWithContext 返回指定 group/version 支持的资源列表。
//
// 在 FakeDiscovery.Resources 中按 GroupVersion 查找；找不到时返回 NotFound 的 StatusError，
// 模拟真实服务端的行为。
func (c *FakeDiscovery) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	action := testing.ActionImpl{
		Verb:     "get",
		Resource: schema.GroupVersionResource{Resource: "resource"},
	}
	// 记录一次 action，便于测试断言调用序列。
	if _, err := c.Invokes(action, nil); err != nil {
		return nil, err
	}
	for _, resourceList := range c.Resources {
		if resourceList.GroupVersion == groupVersion {
			return resourceList, nil
		}
	}
	return nil, &errors.StatusError{
		ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusNotFound,
			Reason:  metav1.StatusReasonNotFound,
			Message: fmt.Sprintf("the server could not find the requested resource, GroupVersion %q not found", groupVersion),
		}}
}

// ServerGroupsAndResources 返回所有 group/version 的组信息与资源列表。
//
// ServerGroupsAndResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsAndResourcesWithContext。
func (c *FakeDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return c.ServerGroupsAndResourcesWithContext(context.Background())
}

// ServerGroupsAndResourcesWithContext 返回所有 group/version 的组信息与资源列表。
func (c *FakeDiscovery) ServerGroupsAndResourcesWithContext(ctx context.Context) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	sgs, err := c.ServerGroupsWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	// APIGroupList 中是值类型，这里取地址以避免复制整组数据。
	resultGroups := []*metav1.APIGroup{}
	for i := range sgs.Groups {
		resultGroups = append(resultGroups, &sgs.Groups[i])
	}

	action := testing.ActionImpl{
		Verb:     "get",
		Resource: schema.GroupVersionResource{Resource: "resource"},
	}
	if _, err = c.Invokes(action, nil); err != nil {
		return resultGroups, c.Resources, err
	}
	return resultGroups, c.Resources, nil
}

// ServerPreferredResources 返回服务端首选版本下的资源列表。
//
// ServerPreferredResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredResourcesWithContext。
func (c *FakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return c.ServerPreferredResourcesWithContext(context.Background())
}

// ServerPreferredResourcesWithContext 返回服务端首选版本下的资源列表。
//
// fake 实现始终返回空结果。
func (c *FakeDiscovery) ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return nil, nil
}

// ServerPreferredNamespacedResources 返回服务端首选版本下的命名空间级资源。
//
// ServerPreferredNamespacedResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredNamespacedResourcesWithContext。
func (c *FakeDiscovery) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return c.ServerPreferredNamespacedResourcesWithContext(context.Background())
}

// ServerPreferredNamespacedResourcesWithContext 返回服务端首选版本下的命名空间级资源。
//
// fake 实现始终返回空结果。
func (c *FakeDiscovery) ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return nil, nil
}

// ServerGroups 返回支持的 API 组，包括版本列表与首选版本。
//
// ServerGroupsWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsWithContext。
func (c *FakeDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	return c.ServerGroupsWithContext(context.Background())
}

// ServerGroupsWithContext 返回支持的 API 组，包括版本列表与首选版本。
//
// 从 Resources 推导出所有 API 组；每个组第一次出现时被记录为 preferred version。
func (c *FakeDiscovery) ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error) {
	action := testing.ActionImpl{
		Verb:     "get",
		Resource: schema.GroupVersionResource{Resource: "group"},
	}
	if _, err := c.Invokes(action, nil); err != nil {
		return nil, err
	}

	groups := map[string]*metav1.APIGroup{}

	for _, res := range c.Resources {
		gv, err := schema.ParseGroupVersion(res.GroupVersion)
		if err != nil {
			return nil, err
		}
		group := groups[gv.Group]
		if group == nil {
			// 首次见到该组：把当前版本作为 preferred，并登记后续版本。
			group = &metav1.APIGroup{
				Name: gv.Group,
				PreferredVersion: metav1.GroupVersionForDiscovery{
					GroupVersion: res.GroupVersion,
					Version:      gv.Version,
				},
			}
			groups[gv.Group] = group
		}

		group.Versions = append(group.Versions, metav1.GroupVersionForDiscovery{
			GroupVersion: res.GroupVersion,
			Version:      gv.Version,
		})
	}

	list := &metav1.APIGroupList{}
	for _, apiGroup := range groups {
		list.Groups = append(list.Groups, *apiGroup)
	}

	return list, nil

}

// ServerVersion 获取并解析服务端版本。
//
// ServerVersionWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerVersionWithContext。
func (c *FakeDiscovery) ServerVersion() (*version.Info, error) {
	return c.ServerVersionWithContext(context.Background())
}

// ServerVersionWithContext 获取并解析服务端版本。
//
// 优先返回 FakedServerVersion；未设置时返回编译期的客户端版本信息，保证测试中非空。
func (c *FakeDiscovery) ServerVersionWithContext(ctx context.Context) (*version.Info, error) {
	action := testing.ActionImpl{}
	action.Verb = "get"
	action.Resource = schema.GroupVersionResource{Resource: "version"}
	_, err := c.Invokes(action, nil)
	if err != nil {
		return nil, err
	}

	if c.FakedServerVersion != nil {
		return c.FakedServerVersion, nil
	}

	versionInfo := kubeversion.Get()
	return &versionInfo, nil
}

// OpenAPISchema 获取并解析服务端支持的 swagger API schema。
//
// OpenAPISchemaWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 OpenAPISchemaWithContext。
func (c *FakeDiscovery) OpenAPISchema() (*openapi_v2.Document, error) {
	return c.OpenAPISchemaWithContext(context.Background())
}

// OpenAPISchemaWithContext 获取并解析服务端支持的 swagger API schema。
//
// fake 实现返回空文档，调用方无需处理 nil。
func (c *FakeDiscovery) OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error) {
	return &openapi_v2.Document{}, nil
}

// OpenAPIV3WithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 OpenAPIV3WithContext。
//
// OpenAPIV3 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) OpenAPIV3() openapi.Client {
	panic("unimplemented")
}

// OpenAPIV3WithContext 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext {
	panic("unimplemented")
}

// RESTClient 返回该客户端用于与 API server 通信的 RESTClient。
//
// fake 没有真实 REST 客户端，返回 nil。
func (c *FakeDiscovery) RESTClient() restclient.Interface {
	return nil
}

// WithLegacyWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 WithLegacyWithContext。
//
// WithLegacy 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) WithLegacy() discovery.DiscoveryInterface {
	panic("unimplemented")
}

// WithLegacyWithContext 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) WithLegacyWithContext(ctx context.Context) discovery.DiscoveryInterfaceWithContext {
	panic("unimplemented")
}
