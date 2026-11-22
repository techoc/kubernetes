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

package scale

import (
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	scalescheme "k8s.io/client-go/scale/scheme"
	scaleappsint "k8s.io/client-go/scale/scheme/appsint"
	scaleappsv1beta1 "k8s.io/client-go/scale/scheme/appsv1beta1"
	scaleappsv1beta2 "k8s.io/client-go/scale/scheme/appsv1beta2"
	scaleautoscaling "k8s.io/client-go/scale/scheme/autoscalingv1"
	scaleextint "k8s.io/client-go/scale/scheme/extensionsint"
	scaleext "k8s.io/client-go/scale/scheme/extensionsv1beta1"
)

// PreferredResourceMapper determines the preferred version of a resource to scale
// 通常由 meta.RESTMapper 实现：给定可能缺少版本的资源，返回服务端首选版本的 GVR。
type PreferredResourceMapper interface {
	// ResourceFor takes a partial resource and returns the preferred resource.
	ResourceFor(resource schema.GroupVersionResource) (preferredResource schema.GroupVersionResource, err error)
}

// Ensure a RESTMapper satisfies the PreferredResourceMapper interface
var _ PreferredResourceMapper = meta.RESTMapper(nil)

// ScaleKindResolver knows about the relationship between
// resources and the GroupVersionKind of their scale subresources.
type ScaleKindResolver interface {
	// ScaleForResource returns the GroupVersionKind of the
	// scale subresource for the given GroupVersionResource.
	// 注意返回的是 scale 子资源自身的 GVK，而非父资源的 GVK。
	ScaleForResource(resource schema.GroupVersionResource) (scaleVersion schema.GroupVersionKind, err error)
}

// discoveryScaleResolver is a ScaleKindResolver that uses
// a DiscoveryInterface to associate resources with their
// scale-kinds
type discoveryScaleResolver struct {
	discoveryClient discovery.ServerResourcesInterface
}

func (r *discoveryScaleResolver) ScaleForResource(inputRes schema.GroupVersionResource) (scaleVersion schema.GroupVersionKind, err error) {
	groupVerResources, err := r.discoveryClient.ServerResourcesForGroupVersion(inputRes.GroupVersion().String())
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("unable to fetch discovery information for %s: %v", inputRes.String(), err)
	}

	// 子资源在 discovery 中以 "<resource>/scale" 命名，按名匹配即可。
	for _, resource := range groupVerResources.APIResources {
		resourceParts := strings.SplitN(resource.Name, "/", 2)
		if len(resourceParts) != 2 || resourceParts[0] != inputRes.Resource || resourceParts[1] != "scale" {
			// skip non-scale resources, or scales for resources that we're not looking for
			continue
		}

		// 若 discovery 显式给出了 group/version（子资源可指向另一个版本），则以它为准；
		// 否则沿用父资源的版本。
		scaleGV := inputRes.GroupVersion()
		if resource.Group != "" && resource.Version != "" {
			scaleGV = schema.GroupVersion{
				Group:   resource.Group,
				Version: resource.Version,
			}
		}

		return scaleGV.WithKind(resource.Kind), nil
	}

	return schema.GroupVersionKind{}, fmt.Errorf("could not find scale subresource for %s in discovery information", inputRes.String())
}

// cachedScaleKindResolver is a ScaleKindResolver that caches results
// from another ScaleKindResolver, re-fetching on cache misses.
type cachedScaleKindResolver struct {
	base ScaleKindResolver

	cache map[schema.GroupVersionResource]schema.GroupVersionKind
	mu    sync.RWMutex
}

func (r *cachedScaleKindResolver) ScaleForResource(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	// 读路径只拿 RLock，避免 discovery 缓存命中时被慢请求阻塞。
	r.mu.RLock()
	gvk, isCached := r.cache[resource]
	r.mu.RUnlock()
	if isCached {
		return gvk, nil
	}

	// we could have multiple fetches of the same resources, but that's probably
	// better than limiting to only one reader at once (mu.Mutex),
	// or blocking checks for other resources while we fetch
	// (mu.Lock before fetch).
	// 即有少量重复的 discovery 请求，也优于把所有资源查询串行化。
	gvk, err := r.base.ScaleForResource(resource)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	// 只在写回缓存时短暂持有写锁，网络请求不持锁。
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[resource] = gvk

	return gvk, nil
}

// NewDiscoveryScaleKindResolver creates a new ScaleKindResolver which uses information from the given
// disovery client to resolve the correct Scale GroupVersionKind for different resources.
func NewDiscoveryScaleKindResolver(client discovery.ServerResourcesInterface) ScaleKindResolver {
	base := &discoveryScaleResolver{
		discoveryClient: client,
	}

	// 默认带缓存：discovery 结果在一个进程生命周期内基本稳定，可显著减少请求量。
	return &cachedScaleKindResolver{
		base:  base,
		cache: make(map[schema.GroupVersionResource]schema.GroupVersionKind),
	}
}

// ScaleConverter knows how to convert between external scale versions.
// 内部通过 internalVersioner 把任意外部版本先收敛到内部类型，再由 scheme 转出目标版本。
type ScaleConverter struct {
	scheme            *runtime.Scheme
	codecs            serializer.CodecFactory
	internalVersioner runtime.GroupVersioner
}

// NewScaleConverter creates a new ScaleConverter for converting between
// Scales in autoscaling/v1 and extensions/v1beta1.
func NewScaleConverter() *ScaleConverter {
	scheme := runtime.NewScheme()
	// 注册所有历史与当前版本的 Scale 类型；这些版本都是需要兼容的 API 表示。
	utilruntime.Must(scaleautoscaling.AddToScheme(scheme))
	utilruntime.Must(scalescheme.AddToScheme(scheme))
	utilruntime.Must(scaleext.AddToScheme(scheme))
	utilruntime.Must(scaleextint.AddToScheme(scheme))
	utilruntime.Must(scaleappsint.AddToScheme(scheme))
	utilruntime.Must(scaleappsv1beta1.AddToScheme(scheme))
	utilruntime.Must(scaleappsv1beta2.AddToScheme(scheme))

	return &ScaleConverter{
		scheme: scheme,
		codecs: serializer.NewCodecFactory(scheme),
		// 各 group 中同名 Kind 的 Scale 需要一个统一的目标内部版本，MultiGroupVersioner
		// 按 GroupKind 一一映射，避免跨 group 误转换。
		internalVersioner: runtime.NewMultiGroupVersioner(
			scalescheme.SchemeGroupVersion,
			schema.GroupKind{Group: scaleext.GroupName, Kind: "Scale"},
			schema.GroupKind{Group: scaleautoscaling.GroupName, Kind: "Scale"},
			schema.GroupKind{Group: scaleappsv1beta1.GroupName, Kind: "Scale"},
			schema.GroupKind{Group: scaleappsv1beta2.GroupName, Kind: "Scale"},
		),
	}
}

// Scheme returns the scheme used by this scale converter.
func (c *ScaleConverter) Scheme() *runtime.Scheme {
	return c.scheme
}

func (c *ScaleConverter) Codecs() serializer.CodecFactory {
	return c.codecs
}

func (c *ScaleConverter) ScaleVersions() []schema.GroupVersion {
	// 解码响应时按此列表注册所有可能出现的 scale 版本。
	return []schema.GroupVersion{
		scaleautoscaling.SchemeGroupVersion,
		scalescheme.SchemeGroupVersion,
		scaleext.SchemeGroupVersion,
		scaleextint.SchemeGroupVersion,
		scaleappsint.SchemeGroupVersion,
		scaleappsv1beta1.SchemeGroupVersion,
		scaleappsv1beta2.SchemeGroupVersion,
	}
}

// ConvertToVersion converts the given *external* input object to the given output *external* output group-version.
func (c *ScaleConverter) ConvertToVersion(in runtime.Object, outVersion schema.GroupVersion) (runtime.Object, error) {
	// 两段式转换：外部 -> 内部 -> 目标外部，借助内部类型统一字段语义。
	scaleInt, err := c.scheme.ConvertToVersion(in, c.internalVersioner)
	if err != nil {
		return nil, err
	}

	return c.scheme.ConvertToVersion(scaleInt, outVersion)
}
