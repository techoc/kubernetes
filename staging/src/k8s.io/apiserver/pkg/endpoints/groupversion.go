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

// ConvertabilityChecker indicates what versions a GroupKind is available in.
//
// 用于在解码时确定有哪些目标版本可用，避免把对象转换到未注册的版本。
type ConvertabilityChecker interface {
	// VersionsForGroupKind indicates what versions are available to convert a group kind. This determines
	// what our decoding abilities are.
	VersionsForGroupKind(gk schema.GroupKind) []schema.GroupVersion
}

// APIGroupVersion is a helper for exposing rest.Storage objects as http.Handlers via go-restful
// It handles URLs of the form:
// /${storage_key}[/${object_name}]
// Where 'storage_key' points to a rest.Storage object stored in storage.
// This object should contain all parameterization necessary for running a particular API version
//
// 一个 APIGroupVersion 对应"一个 group/version 的全部资源"；不同版本共享同一份 Storage，
// 差异由 Serializer/Convertor 承担，因此这里的字段大多是编解码与授权所需的依赖注入点。
type APIGroupVersion struct {
	// Storage 以 URL 中的资源段为键，映射到具体实现；同一个对象可能在多个路径下暴露（如 status 子资源）。
	Storage map[string]rest.Storage

	Root string

	// GroupVersion is the external group version
	GroupVersion schema.GroupVersion

	// AllServedVersionsByResource is indexed by resource and maps to a list of versions that resource exists in.
	// This was created so that StorageVersion for APIs can include a list of all version that are served for each
	// GroupResource tuple.
	AllServedVersionsByResource map[string][]string

	// OptionsExternalVersion controls the Kubernetes APIVersion used for common objects in the apiserver
	// schema like api.Status, api.DeleteOptions, and metav1.ListOptions. Other implementors may
	// define a version "v1beta1" but want to use the Kubernetes "v1" internal objects. If
	// empty, defaults to GroupVersion.
	OptionsExternalVersion *schema.GroupVersion
	// MetaGroupVersion defaults to "meta.k8s.io/v1" and is the scheme group version used to decode
	// common API implementations like ListOptions. Future changes will allow this to vary by group
	// version (for when the inevitable meta/v2 group emerges).
	MetaGroupVersion *schema.GroupVersion

	// Serializer is used to determine how to convert responses from API methods into bytes to send over
	// the wire.
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

	// Authorizer determines whether a user is allowed to make a certain request. The Handler does a preliminary
	// authorization check using the request URI but it may be necessary to make additional checks, such as in
	// the create-on-update case
	Authorizer authorizer.UnconditionalAuthorizer

	Admit admission.Interface

	MinRequestTimeout time.Duration

	// The limit on the request body size that would be accepted and decoded in a write request.
	// 0 means no limit.
	// 限制请求体大小是防御超大对象耗尽内存的第一道闸门；0 表示不限制。
	MaxRequestBodyBytes int64
}

// InstallREST registers the REST handlers (storage, watch, proxy and redirect) into a restful Container.
// It is expected that the provided path root prefix will serve all operations. Root MUST NOT end
// in a slash.
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
		// if EncodingVersion is empty, then the apiserver does not
		// need to register this resource via the storage version API,
		// thus we can remove it.
		if info != nil && len(info.EncodingVersion) > 0 {
			filtered = append(filtered, info)
		}
	}
	return filtered
}

// staticLister implements the APIResourceLister interface
type staticLister struct {
	list []metav1.APIResource
}

// ListAPIResources 直接返回构造时固定的列表：发现信息在安装时已确定，运行期不再变化。
func (s staticLister) ListAPIResources() []metav1.APIResource {
	return s.list
}

var _ discovery.APIResourceLister = &staticLister{}
