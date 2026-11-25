/*
Copyright 2014 The Kubernetes Authors.

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

package endpoints

import (
	"path"
	"time"

	restful "github.com/emicklei/go-restful/v3"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storageversion"
)

// ConvertabilityChecker 指明某个 GroupKind 在哪些版本上可用，
// 用于在解码时确定有哪些目标版本可用，避免把对象转换到未注册的版本。
type ConvertabilityChecker interface {
	// VersionsForGroupKind 返回能够转换指定 group kind 的版本集合，
	// 这决定了本 apiserver 具备哪些解码能力。
	VersionsForGroupKind(gk schema.GroupKind) []schema.GroupVersion
}

// APIGroupVersion 是一个辅助结构，通过 go-restful 把 rest.Storage 对象暴露为 http.Handler，
// 处理的 URL 形如：/${storage_key}[/${object_name}]，其中 storage_key 指向存放在 Storage 中的
// rest.Storage 对象。该结构应包含运行某个 API 版本所需的全部参数。
//
// 一个 APIGroupVersion 对应"一个 group/version 的全部资源"；不同版本共享同一份 Storage，
// 差异由 Serializer/Convertor 承担，因此这里的字段大多是编解码与授权所需的依赖注入点。
type APIGroupVersion struct {
	// Storage 以 URL 中的资源段为键，映射到具体实现；同一个对象可能在多个路径下暴露（如 status 子资源）。
	Storage map[string]rest.Storage

	Root string

	// GroupVersion 是对外暴露的 group version。
	GroupVersion schema.GroupVersion

	// AllServedVersionsByResource 以资源名为键，映射到该资源存在的所有版本列表。
	// 引入它是为了让各 GroupResource 元组的 StorageVersion 能够列出其全部对外服务版本。
	AllServedVersionsByResource map[string][]string

	// OptionsExternalVersion 指定 apiserver 内通用对象（如 api.Status、api.DeleteOptions、
	// metav1.ListOptions）所使用的 Kubernetes APIVersion。其他实现方可能定义了 "v1beta1" 版本，
	// 但仍想使用 Kubernetes 的 "v1" 内部对象。为空时回退到 GroupVersion。
	OptionsExternalVersion *schema.GroupVersion
	// MetaGroupVersion 默认为 "meta.k8s.io/v1"，用于解码 ListOptions 等通用 API 实现。
	// 后续改动会允许它按 group version 区分（以应对将来出现的 meta/v2）。
	MetaGroupVersion *schema.GroupVersion

	// Serializer 决定如何把 API 方法的响应转换成要在线上发送的字节。
	Serializer     runtime.NegotiatedSerializer
	ParameterCodec runtime.ParameterCodec

	// 下面这组是 scheme 能力的拆分注入：Typer/Creater 负责类型识别与实例化，
	// Convertor 负责版本转换，UnsafeConvertor 跳过校验（用于内部可信路径），
	// TypeConverter 服务于 server-side apply 的字段管理。
	Typer                 runtime.ObjectTyper
	Creater               runtime.ObjectCreater
	Convertor             runtime.ObjectConvertor
	ConvertabilityChecker ConvertabilityChecker
	Defaulter             runtime.ObjectDefaulter
	Namer                 runtime.Namer
	UnsafeConvertor       runtime.ObjectConvertor
	TypeConverter         managedfields.TypeConverter

	EquivalentResourceRegistry runtime.EquivalentResourceRegistry

	// Authorizer 判断用户是否被允许发起某个请求。Handler 会先基于请求 URI 做一次初步鉴权，
	// 但某些场景（例如 create-on-update）还需要额外的检查。
	Authorizer authorizer.UnconditionalAuthorizer

	Admit admission.Interface

	MinRequestTimeout time.Duration

	// 写请求中可接受并解码的请求体大小上限，0 表示不限制。
	// 限制请求体大小是防御超大对象耗尽内存的第一道闸门。
	MaxRequestBodyBytes int64
}

// InstallREST 把 REST handler（storage、watch、proxy、redirect）注册进 restful Container。
// 调用方需保证给定的路径根前缀承载全部操作，且 Root 不能以斜杠结尾。
//
// 返回聚合发现所需的资源描述、存储版本信息，以及注册过程中的错误集合。
// 错误以 Aggregate 返回而不是立即失败，是为了让调用方一次性看到所有冲突。
func (g *APIGroupVersion) InstallREST(container *restful.Container) ([]apidiscoveryv2.APIResourceDiscovery, []*storageversion.ResourceInfo, error) {
	prefix := path.Join(g.Root, g.GroupVersion.Group, g.GroupVersion.Version)
	installer := &APIInstaller{
		group:             g,
		prefix:            prefix,
		minRequestTimeout: g.MinRequestTimeout,
	}

	apiResources, resourceInfos, ws, registrationErrors := installer.Install()
	versionDiscoveryHandler := discovery.NewAPIVersionHandler(g.Serializer, g.GroupVersion, staticLister{apiResources})
	versionDiscoveryHandler.AddToWebService(ws)
	container.Add(ws)
	aggregatedDiscoveryResources, err := ConvertGroupVersionIntoToDiscovery(apiResources)
	if err != nil {
		registrationErrors = append(registrationErrors, err)
	}
	return aggregatedDiscoveryResources, removeNonPersistedResources(resourceInfos), utilerrors.NewAggregate(registrationErrors)
}

// removeNonPersistedResources 过滤掉无需向存储版本 API 登记的资源。
//
// EncodingVersion 为空说明该资源不落盘（如子资源、虚拟资源），让它们进入存储版本
// 屏障会造成永远无法完成的迁移等待。
func removeNonPersistedResources(infos []*storageversion.ResourceInfo) []*storageversion.ResourceInfo {
	var filtered []*storageversion.ResourceInfo
	for _, info := range infos {
		// 不落盘的资源无需通过 storage version API 注册，直接过滤掉。
		if info != nil && len(info.EncodingVersion) > 0 {
			filtered = append(filtered, info)
		}
	}
	return filtered
}

// staticLister 实现 APIResourceLister 接口。
type staticLister struct {
	list []metav1.APIResource
}

// ListAPIResources 直接返回构造时固定的列表：发现信息在安装时已确定，运行期不再变化。
func (s staticLister) ListAPIResources() []metav1.APIResource {
	return s.list
}

var _ discovery.APIResourceLister = &staticLister{}
