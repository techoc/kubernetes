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

package resolver

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// ResourceResolver 将 ParamKind 的 GroupVersionKind 转换为实际 API GroupVersionResource，供参数资源授权与解析使用。
type ResourceResolver interface {
	Resolve(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error)
}

type discoveryResourceResolver struct {
	client discovery.DiscoveryInterface
}

// Resolve 查询对应 GroupVersion 的 Discovery 文档，按 Kind 查找资源并跳过子资源；找不到时返回 NoKindMatch。
func (d *discoveryResourceResolver) Resolve(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	gv := gvk.GroupVersion()
	// TODO：重构为高效的 GVK→GVR 解析器，缓存已命中的结果，并在未命中时重新解析 Group/Version 信息。
	resources, err := d.client.ServerResourcesForGroupVersion(gv.String())
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	for _, resource := range resources.APIResources {
		if resource.Kind != gvk.Kind {
			// 跳过 Kind 不匹配的 API 资源。
			continue
		}
		if strings.Contains(resource.Name, "/") {
			// 跳过子资源，只解析顶层资源。
			continue
		}
		if resource.Group != "" && resource.Group != gvk.Group {
			// API Group 与目标 GVK 不匹配，继续检查下一项。
			continue
		}
		if resource.Version != "" && resource.Version != gvk.Version {
			// API Version 与目标 GVK 不匹配，继续检查下一项。
			continue
		}
		return gv.WithResource(resource.Name), nil
	}
	return schema.GroupVersionResource{}, &meta.NoKindMatchError{GroupKind: gvk.GroupKind(), SearchedVersions: []string{gvk.Version}}
}

// NewDiscoveryResourceResolver 将 Discovery Client 包装成 GVK→GVR 解析器。
func NewDiscoveryResourceResolver(client discovery.DiscoveryInterface) (ResourceResolver, error) {
	return &discoveryResourceResolver{client: client}, nil
}

// ResourceResolverFunc 将普通函数适配为 ResourceResolver 接口，便于测试注入自定义解析逻辑。
type ResourceResolverFunc func(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error)

// Resolve 调用底层函数完成 GVK 到 GVR 的解析。
func (f ResourceResolverFunc) Resolve(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	return f(gvk)
}
