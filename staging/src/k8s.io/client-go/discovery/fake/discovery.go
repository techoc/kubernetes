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

// ServerResourcesForGroupVersion returns the supported resources for a group
// and version.
//
// ServerResourcesForGroupVersionWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerResourcesForGroupVersionWithContext instead.
//
// ServerResourcesForGroupVersion 返回指定 group/version 支持的资源列表；
// 新代码应使用带 context 的版本。
func (c *FakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return c.ServerResourcesForGroupVersionWithContext(context.Background(), groupVersion)
}

// ServerResourcesForGroupVersionWithContext returns the supported resources for a group
// and version.
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

// ServerGroupsAndResources returns the supported groups and resources for all groups and versions.
//
// ServerGroupsAndResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerGroupsAndResourcesWithContext instead.
func (c *FakeDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return c.ServerGroupsAndResourcesWithContext(context.Background())
}

// ServerGroupsAndResourcesWithContext returns the supported groups and resources for all groups and versions.
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

// ServerPreferredResources returns the supported resources with the version
// preferred by the server.
//
// ServerPreferredResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerPreferredResourcesWithContext instead.
func (c *FakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return c.ServerPreferredResourcesWithContext(context.Background())
}

// ServerPreferredResourcesWithContext returns the supported resources with the version
// preferred by the server.
//
// fake 实现始终返回空结果。
func (c *FakeDiscovery) ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return nil, nil
}

// ServerPreferredNamespacedResources returns the supported namespaced resources
// with the version preferred by the server.
//
// ServerPreferredNamespacedResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerPreferredNamespacedResourcesWithContext instead.
func (c *FakeDiscovery) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return c.ServerPreferredNamespacedResourcesWithContext(context.Background())
}

// ServerPreferredNamespacedResourcesWithContext returns the supported namespaced resources
// with the version preferred by the server.
//
// fake 实现始终返回空结果。
func (c *FakeDiscovery) ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return nil, nil
}

// ServerGroups returns the supported groups, with information like supported
// versions and the preferred version.
//
// ServerGroupsWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerGroupsWithContext instead.
func (c *FakeDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	return c.ServerGroupsWithContext(context.Background())
}

// ServerGroupsWithContext returns the supported groups, with information like supported
// versions and the preferred version.
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

// ServerVersion retrieves and parses the server's version.
//
// ServerVersionWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerVersionWithContext instead.
func (c *FakeDiscovery) ServerVersion() (*version.Info, error) {
	return c.ServerVersionWithContext(context.Background())
}

// ServerVersionWithContext retrieves and parses the server's version.
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

// OpenAPISchema retrieves and parses the swagger API schema the server supports.
//
// OpenAPISchemaWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use OpenAPISchemaWithContext instead.
func (c *FakeDiscovery) OpenAPISchema() (*openapi_v2.Document, error) {
	return c.OpenAPISchemaWithContext(context.Background())
}

// OpenAPISchemaWithContext retrieves and parses the swagger API schema the server supports.
//
// fake 实现返回空文档，调用方无需处理 nil。
func (c *FakeDiscovery) OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error) {
	return &openapi_v2.Document{}, nil
}

// OpenAPIV3WithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use OpenAPIV3WithContext instead.
//
// OpenAPIV3 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) OpenAPIV3() openapi.Client {
	panic("unimplemented")
}

// OpenAPIV3WithContext 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext {
	panic("unimplemented")
}

// RESTClient returns a RESTClient that is used to communicate with API server
// by this client implementation.
//
// fake 没有真实 REST 客户端，返回 nil。
func (c *FakeDiscovery) RESTClient() restclient.Interface {
	return nil
}

// WithLegacyWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use WithLegacyWithContext instead.
//
// WithLegacy 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) WithLegacy() discovery.DiscoveryInterface {
	panic("unimplemented")
}

// WithLegacyWithContext 在 fake 中未实现，调用会 panic。
func (c *FakeDiscovery) WithLegacyWithContext(ctx context.Context) discovery.DiscoveryInterfaceWithContext {
	panic("unimplemented")
}
