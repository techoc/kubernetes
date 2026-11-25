/*
Copyright 2015 The Kubernetes Authors.

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
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	restful "github.com/emicklei/go-restful/v3"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/endpoints/deprecation"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	utilwarning "k8s.io/apiserver/pkg/endpoints/warning"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storageversion"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	versioninfo "k8s.io/component-base/version"
)

const (
	// 这三个 route metadata 键会被 requestinfo 等中间件读取，属于内部契约，
	// 改动会影响鉴权属性的推导。
	RouteMetaGVK              = "x-kubernetes-group-version-kind"
	RouteMetaSelectableFields = "x-kubernetes-selectable-fields"
	RouteMetaAction           = "x-kubernetes-action"
)

// APIInstaller 负责把一个 APIGroupVersion 的 storage 映射注册成 HTTP 路由。
// 它是“存储能力 → REST 端点”的转换器：storage 实现了哪些 rest 接口，
// 就注册哪些动词；缺少某接口则对应路由不暴露，而不是运行时报错。
type APIInstaller struct {
	group             *APIGroupVersion
	prefix            string // 要注册 API 资源的路径前缀。
	minRequestTimeout time.Duration
}

// action 汇总一个动作（MethodGet、MethodPost、MethodWatch、MethodProxy 等）的信息。
// 它是注册前的中间表示：先收集再统一生成路由，便于按 scope 复用同一套逻辑。
type action struct {
	Verb          string               // 标识该动作的 Verb（MethodGet、MethodPost、MethodWatch、MethodProxy 等）。
	Path          string               // 动作的路径。
	Params        []*restful.Parameter // 与该动作关联的参数列表。
	Namer         handlers.ScopeNamer
	AllNamespaces bool // 当动作属于命名空间资源、但作用于所有命名空间的聚合结果时为 true。
}

// ConvertGroupVersionIntoToDiscovery 把扁平 APIResource 列表转换为聚合发现结构。
// 先建父资源索引再挂子资源，因为子资源可能在列表中出现于父资源之前；
// 缺失父资源时补一个空壳父项，是为了兼容旧 client-go 对 nil ResponseKind 的 panic
// （见 kubernetes#118361），属于必须保留的兼容行为。
func ConvertGroupVersionIntoToDiscovery(list []metav1.APIResource) ([]apidiscoveryv2.APIResourceDiscovery, error) {
	var apiResourceList []apidiscoveryv2.APIResourceDiscovery
	parentResources := make(map[string]int)

	// 先遍历所有顶层资源。
	for _, r := range list {
		if strings.Contains(r.Name, "/") {
			// 此时先跳过子资源，以便得到资源列表。
			continue
		}

		var scope apidiscoveryv2.ResourceScope
		if r.Namespaced {
			scope = apidiscoveryv2.ScopeNamespace
		} else {
			scope = apidiscoveryv2.ScopeCluster
		}

		resource := apidiscoveryv2.APIResourceDiscovery{
			Resource: r.Name,
			Scope:    scope,
			ResponseKind: &metav1.GroupVersionKind{
				Group:   r.Group,
				Version: r.Version,
				Kind:    r.Kind,
			},
			Verbs:            r.Verbs,
			ShortNames:       r.ShortNames,
			Categories:       r.Categories,
			SingularResource: r.SingularName,
		}
		apiResourceList = append(apiResourceList, resource)
		parentResources[r.Name] = len(apiResourceList) - 1
	}

	// 再遍历所有子资源。
	for _, r := range list {
		// 拆出资源名与子资源名。
		split := strings.SplitN(r.Name, "/", 2)

		if len(split) != 2 {
			// 跳过父资源。
			continue
		}

		var scope apidiscoveryv2.ResourceScope
		if r.Namespaced {
			scope = apidiscoveryv2.ScopeNamespace
		} else {
			scope = apidiscoveryv2.ScopeCluster
		}

		parentidx, exists := parentResources[split[0]]
		if !exists {
			// 若子资源没有对应的父资源，则补一个父资源。
			apiResourceList = append(apiResourceList, apidiscoveryv2.APIResourceDiscovery{
				Resource: split[0],
				Scope:    scope,
				// 兼容 v0.26.0-v0.26.3 的 client-go 客户端的 nil panic，
				// 参见 https://github.com/kubernetes/kubernetes/issues/118361
				ResponseKind: &metav1.GroupVersionKind{},
			})
			parentidx = len(apiResourceList) - 1
			parentResources[split[0]] = parentidx
		}

		if apiResourceList[parentidx].Scope != scope {
			return nil, fmt.Errorf("Error: Parent %s (scope: %s) and subresource %s (scope: %s) scope do not match", split[0], apiResourceList[parentidx].Scope, split[1], scope)
			//
		}

		subresource := apidiscoveryv2.APISubresourceDiscovery{
			Subresource: split[1],
			Verbs:       r.Verbs,
			// 兼容 v0.26.0-v0.26.3 的 client-go 客户端的 nil panic，
			// 参见 https://github.com/kubernetes/kubernetes/issues/118361
			ResponseKind: &metav1.GroupVersionKind{},
		}
		if r.Kind != "" {
			subresource.ResponseKind = &metav1.GroupVersionKind{
				Group:   r.Group,
				Version: r.Version,
				Kind:    r.Kind,
			}
		}
		apiResourceList[parentidx].Subresources = append(apiResourceList[parentidx].Subresources, subresource)
	}

	return apiResourceList, nil
}

// MetricsVerbOverride 用于判断某个 storage 是否支持为监控覆盖其默认 verb。
// 少数资源（如 connect 类）对指标的 verb 语义有自己的定义，通过该接口覆盖。
type StorageMetricsOverride interface {
	// OverrideMetricsVerb 让 storage 对象有机会覆盖上报到指标端点的 verb。
	OverrideMetricsVerb(oldVerb string) (newVerb string)
}

// Documentation 用于判断对象是否通过方法提供 swagger 文档。
type documentable interface {
	SwaggerDoc() map[string]string
}

// toDiscoveryKubeVerb 把 action.Verb 映射为逻辑上的 kube verb，用于 discovery。
// 空字符串表示不在 discovery 中列出（connect 由具体方法名映射）；
// 一个 HTTP 动词可能对应多个 kube 动词（如 GET 既是 get 也是 list），因此映射是多对一。
var toDiscoveryKubeVerb = map[string]string{
	request.MethodConnect:          "", // 不在 discovery 中列出。
	request.MethodDelete:           "delete",
	request.MethodDeleteCollection: "deletecollection",
	request.MethodGet:              "get",
	request.MethodList:             "list",
	request.MethodPatch:            "patch",
	request.MethodPost:             "create",
	request.MethodProxy:            "proxy",
	request.MethodPut:              "update",
	request.MethodWatch:            "watch",
	request.MethodWatchList:        "watch",
}

// Install 安装 API 资源的 handler。
// 返回的 APIResource 列表用于 discovery，ResourceInfo 用于 storageversion
// API；单个资源注册失败不会中断整体安装，而是聚合错误交由调用方决定是否致命。
func (a *APIInstaller) Install() ([]metav1.APIResource, []*storageversion.ResourceInfo, *restful.WebService, []error) {
	var apiResources []metav1.APIResource
	var resourceInfos []*storageversion.ResourceInfo
	var errors []error
	ws := a.newWebService()

	// 以确定性的（排序后）顺序注册路径，从而得到确定性的 swagger spec。
	// map 遍历无序，排序是为了让 swagger/OpenAPI 输出稳定，避免生成物抖动。
	paths := make([]string, len(a.group.Storage))
	var i int = 0
	for path := range a.group.Storage {
		paths[i] = path
		i++
	}
	sort.Strings(paths)
	for _, path := range paths {
		apiResource, resourceInfo, err := a.registerResourceHandlers(path, a.group.Storage[path], ws)
		if err != nil {
			errors = append(errors, fmt.Errorf("error in registering resource: %s, %v", path, err))
		}
		if apiResource != nil {
			apiResources = append(apiResources, *apiResource)
		}
		if resourceInfo != nil {
			resourceInfos = append(resourceInfos, resourceInfo)
		}
	}
	return apiResources, resourceInfos, ws, errors
}

// newWebService 用 api installer 的前缀和版本创建一个新的 restful webservice。
func (a *APIInstaller) newWebService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path(a.prefix)
	// a.prefix 形如 "prefix/group/version"。
	ws.Doc("API at " + a.prefix)
	// 向后兼容：V1 允许 content-type 为空的请求体。
	// 如果将来不再使用 go-restful，可以按端点逐个把空 content-type 默认为 application/json。
	ws.Consumes("*/*")
	mediaTypes, streamMediaTypes := negotiation.MediaTypesForSerializer(a.group.Serializer)
	ws.Produces(append(mediaTypes, streamMediaTypes...)...)
	ws.ApiVersion(a.group.GroupVersion.String())

	return ws
}

// storageKind 计算落盘使用的 GVK，即对象被写入 etcd 之前所转换到的 GVK。
// 返回的是“落盘版本”，与对外暴露版本无关；storageversion API 依赖它来判断实际编码格式。
func getStorageVersionKind(storageVersioner runtime.GroupVersioner, storage rest.Storage, typer runtime.ObjectTyper) (schema.GroupVersionKind, error) {
	object := storage.New()
	fqKinds, _, err := typer.ObjectKinds(object)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	gvk, ok := storageVersioner.KindForGroupVersionKinds(fqKinds)
	if !ok {
		return schema.GroupVersionKind{}, fmt.Errorf("cannot find the storage version kind for %v", reflect.TypeOf(object))
	}
	return gvk, nil
}

// GetResourceKind 返回给定 storage 对象注册的外部 group version kind。
// 若该 storage 是子资源并提供了 override，则返回 override 中给定的 group version kind。
// 优先询问 storage 自身（GroupVersionKindProvider），因为子资源的 Kind
// 可能与父对象不同（如 Deployment/scale 返回 Scale）；否则按 Group 从注册的
// 全部 Kind 中筛选。
func GetResourceKind(groupVersion schema.GroupVersion, storage rest.Storage, typer runtime.ObjectTyper) (schema.GroupVersionKind, error) {
	// 直接让 storage 告诉我们它的确切 GVK。
	if gvkProvider, ok := storage.(rest.GroupVersionKindProvider); ok {
		return gvkProvider.GroupVersionKind(groupVersion), nil
	}

	object := storage.New()
	fqKinds, _, err := typer.ObjectKinds(object)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	// 同一个 Go 类型可能对应多个候选的完整 Kind，这里要找出属于待注册 group 的那个。
	fqKindToRegister := schema.GroupVersionKind{}
	for _, fqKind := range fqKinds {
		if fqKind.Group == groupVersion.Group {
			fqKindToRegister = groupVersion.WithKind(fqKind.Kind)
			break
		}
	}
	if fqKindToRegister.Empty() {
		return schema.GroupVersionKind{}, fmt.Errorf("unable to locate fully qualified kind for %v: found %v when registering for %v", reflect.TypeOf(object), fqKinds, groupVersion)
	}

	// 由于上面的检查已保证 group 匹配。
	return fqKindToRegister, nil
}

func (a *APIInstaller) registerResourceHandlers(path string, storage rest.Storage, ws *restful.WebService) (*metav1.APIResource, *storageversion.ResourceInfo, error) {
	// 本函数很长但顺序重要：先解析 Kind/scope，再按 storage 实现的接口
	// 收集 actions，最后为每个 action 生成路由。下面各段按这个顺序组织。
	admit := a.group.Admit

	optionsExternalVersion := a.group.GroupVersion
	if a.group.OptionsExternalVersion != nil {
		optionsExternalVersion = *a.group.OptionsExternalVersion
	}

	resource, subresource, err := splitSubresource(path)
	if err != nil {
		return nil, nil, err
	}

	group, version := a.group.GroupVersion.Group, a.group.GroupVersion.Version

	fqKindToRegister, err := GetResourceKind(a.group.GroupVersion, storage, a.group.Typer)
	if err != nil {
		return nil, nil, err
	}

	versionedPtr, err := a.group.Creater.New(fqKindToRegister)
	if err != nil {
		return nil, nil, err
	}
	defaultVersionedObject := indirectArbitraryPointer(versionedPtr)
	kind := fqKindToRegister.Kind
	isSubresource := len(subresource) > 0

	// 若存在子资源，命名空间作用域由父资源决定。
	// 子资源自身往往不实现 Scoper，scope 必须继承父资源，否则 URL 形态会错。
	var namespaceScoped bool
	if isSubresource {
		parentStorage, ok := a.group.Storage[resource]
		if !ok {
			return nil, nil, fmt.Errorf("missing parent storage: %q", resource)
		}
		scoper, ok := parentStorage.(rest.Scoper)
		if !ok {
			return nil, nil, fmt.Errorf("%q must implement scoper", resource)
		}
		namespaceScoped = scoper.NamespaceScoped()

	} else {
		scoper, ok := storage.(rest.Scoper)
		if !ok {
			return nil, nil, fmt.Errorf("%q must implement scoper", resource)
		}
		namespaceScoped = scoper.NamespaceScoped()
	}

	// 由 storage 支持的接口决定它支持哪些动词，进而决定每条路径支持哪些动词。
	// 这里用类型断言一次性探测能力，后续所有动作注册都基于这些布尔量。
	creater, isCreater := storage.(rest.Creater)
	namedCreater, isNamedCreater := storage.(rest.NamedCreater)
	lister, isLister := storage.(rest.Lister)
	getter, isGetter := storage.(rest.Getter)
	getterWithOptions, isGetterWithOptions := storage.(rest.GetterWithOptions)
	gracefulDeleter, isGracefulDeleter := storage.(rest.GracefulDeleter)
	collectionDeleter, isCollectionDeleter := storage.(rest.CollectionDeleter)
	updater, isUpdater := storage.(rest.Updater)
	patcher, isPatcher := storage.(rest.Patcher)
	watcher, isWatcher := storage.(rest.Watcher)
	connecter, isConnecter := storage.(rest.Connecter)
	storageMeta, isMetadata := storage.(rest.StorageMetadata)
	storageVersionProvider, isStorageVersionProvider := storage.(rest.StorageVersionProvider)
	gvAcceptor, _ := storage.(rest.GroupVersionAcceptor)
	if !isMetadata {
		storageMeta = defaultStorageMetadata{}
	}

	if isNamedCreater {
		isCreater = true
	}

	var versionedList interface{}
	if isLister {
		list := lister.NewList()
		listGVKs, _, err := a.group.Typer.ObjectKinds(list)
		if err != nil {
			return nil, nil, err
		}
		versionedListPtr, err := a.group.Creater.New(a.group.GroupVersion.WithKind(listGVKs[0].Kind))
		if err != nil {
			return nil, nil, err
		}
		versionedList = indirectArbitraryPointer(versionedListPtr)
	}

	versionedListOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("ListOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedCreateOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("CreateOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedPatchOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("PatchOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedUpdateOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("UpdateOptions"))
	if err != nil {
		return nil, nil, err
	}

	var versionedDeleteOptions runtime.Object
	var versionedDeleterObject interface{}
	deleteReturnsDeletedObject := false
	if isGracefulDeleter {
		// deleteOptions 与 body 类型分别构造：前者用于 query 参数，
		// 后者用于请求体解码；两者版本可能不同（optionsExternalVersion）。
		versionedDeleteOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind("DeleteOptions"))
		if err != nil {
			return nil, nil, err
		}
		versionedDeleterObject = indirectArbitraryPointer(versionedDeleteOptions)

		if mayReturnFullObjectDeleter, ok := storage.(rest.MayReturnFullObjectDeleter); ok {
			deleteReturnsDeletedObject = mayReturnFullObjectDeleter.DeleteReturnsDeletedObject()
		}
	}

	versionedStatusPtr, err := a.group.Creater.New(optionsExternalVersion.WithKind("Status"))
	if err != nil {
		return nil, nil, err
	}
	versionedStatus := indirectArbitraryPointer(versionedStatusPtr)
	var (
		getOptions             runtime.Object
		versionedGetOptions    runtime.Object
		getOptionsInternalKind schema.GroupVersionKind
		getSubpath             bool
	)
	if isGetterWithOptions {
		getOptions, getSubpath, _ = getterWithOptions.NewGetOptions()
		getOptionsInternalKinds, _, err := a.group.Typer.ObjectKinds(getOptions)
		if err != nil {
			return nil, nil, err
		}
		getOptionsInternalKind = getOptionsInternalKinds[0]
		versionedGetOptions, err = a.group.Creater.New(a.group.GroupVersion.WithKind(getOptionsInternalKind.Kind))
		if err != nil {
			versionedGetOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind(getOptionsInternalKind.Kind))
			if err != nil {
				return nil, nil, err
			}
		}
		isGetter = true
	}

	var versionedWatchEvent interface{}
	if isWatcher {
		versionedWatchEventPtr, err := a.group.Creater.New(a.group.GroupVersion.WithKind("WatchEvent"))
		if err != nil {
			return nil, nil, err
		}
		versionedWatchEvent = indirectArbitraryPointer(versionedWatchEventPtr)
	}

	var (
		connectOptions             runtime.Object
		versionedConnectOptions    runtime.Object
		connectOptionsInternalKind schema.GroupVersionKind
		connectSubpath             bool
	)
	if isConnecter {
		connectOptions, connectSubpath, _ = connecter.NewConnectOptions()
		if connectOptions != nil {
			connectOptionsInternalKinds, _, err := a.group.Typer.ObjectKinds(connectOptions)
			if err != nil {
				return nil, nil, err
			}

			connectOptionsInternalKind = connectOptionsInternalKinds[0]
			versionedConnectOptions, err = a.group.Creater.New(a.group.GroupVersion.WithKind(connectOptionsInternalKind.Kind))
			if err != nil {
				versionedConnectOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind(connectOptionsInternalKind.Kind))
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	allowWatchList := isWatcher && isLister // 只有同时支持 watch 与 list 的 Kind 才允许对列表做 watch。
	// 路径参数在所有动词间复用，避免重复分配。
	nameParam := ws.PathParameter("name", "name of the "+kind).DataType("string")
	pathParam := ws.PathParameter("path", "path to the resource").DataType("string")

	params := []*restful.Parameter{}
	actions := []action{}

	var resourceKind string
	kindProvider, ok := storage.(rest.KindProvider)
	if ok {
		resourceKind = kindProvider.Kind()
	} else {
		resourceKind = kind
	}

	tableProvider, isTableProvider := storage.(rest.TableConvertor)
	if isLister && !isTableProvider {
		// 所有 lister 都必须实现 TableProvider。
		return nil, nil, fmt.Errorf("%q must implement TableConvertor", resource)
	}

	var apiResource metav1.APIResource
	if utilfeature.DefaultFeatureGate.Enabled(features.StorageVersionHash) &&
		isStorageVersionProvider &&
		storageVersionProvider.StorageVersion() != nil {
		versioner := storageVersionProvider.StorageVersion()
		gvk, err := getStorageVersionKind(versioner, storage, a.group.Typer)
		if err != nil {
			return nil, nil, err
		}
		apiResource.StorageVersionHash = discovery.StorageVersionHash(gvk.Group, gvk.Version, gvk.Kind)
	}

	// 取得给定作用域下的动作列表。
	// 集群级与命名空间级两条分支逻辑几乎对称，只是路径前缀和 namer 不同；
	// 保持分开书写比参数化更易读，也便于各自演进。
	switch {
	case !namespaceScoped:
		// 处理 nodes 这类非命名空间作用域的资源。
		resourcePath := resource
		resourceParams := params
		itemPath := resourcePath + "/{name}"
		nameParams := append(params, nameParam)
		proxyParams := append(nameParams, pathParam)
		suffix := ""
		if isSubresource {
			suffix = "/" + subresource
			itemPath = itemPath + suffix
			resourcePath = itemPath
			resourceParams = nameParams
		}
		apiResource.Name = path
		apiResource.Namespaced = false
		apiResource.Kind = resourceKind
		namer := handlers.ContextBasedNaming{
			Namer:         a.group.Namer,
			ClusterScoped: true,
		}

		// 标准 REST 动词（GET、PUT、POST、DELETE）的 handler，
		// 在资源路径 /api/apiVersion/resource 上添加动作。
		actions = appendIf(actions, action{request.MethodList, resourcePath, resourceParams, namer, false}, isLister)
		actions = appendIf(actions, action{request.MethodPost, resourcePath, resourceParams, namer, false}, isCreater)
		actions = appendIf(actions, action{request.MethodDeleteCollection, resourcePath, resourceParams, namer, false}, isCollectionDeleter)
		// 已在 1.11 弃用
		actions = appendIf(actions, action{request.MethodWatchList, "watch/" + resourcePath, resourceParams, namer, false}, allowWatchList)

		// 在对象路径 /api/apiVersion/resource/{name} 上添加动作。
		actions = appendIf(actions, action{request.MethodGet, itemPath, nameParams, namer, false}, isGetter)
		if getSubpath {
			actions = appendIf(actions, action{request.MethodGet, itemPath + "/{path:*}", proxyParams, namer, false}, isGetter)
		}
		actions = appendIf(actions, action{request.MethodPut, itemPath, nameParams, namer, false}, isUpdater)
		actions = appendIf(actions, action{request.MethodPatch, itemPath, nameParams, namer, false}, isPatcher)
		actions = appendIf(actions, action{request.MethodDelete, itemPath, nameParams, namer, false}, isGracefulDeleter)
		// 已在 1.11 弃用
		actions = appendIf(actions, action{request.MethodWatch, "watch/" + itemPath, nameParams, namer, false}, isWatcher)
		actions = appendIf(actions, action{request.MethodConnect, itemPath, nameParams, namer, false}, isConnecter)
		actions = appendIf(actions, action{request.MethodConnect, itemPath + "/{path:*}", proxyParams, namer, false}, isConnecter && connectSubpath)
	default:
		namespaceParamName := "namespaces"
		// 标准 REST 动词（GET、PUT、POST、DELETE）的 handler。
		namespaceParam := ws.PathParameter("namespace", "object name and auth scope, such as for teams and projects").DataType("string")
		namespacedPath := namespaceParamName + "/{namespace}/" + resource
		namespaceParams := []*restful.Parameter{namespaceParam}

		resourcePath := namespacedPath
		resourceParams := namespaceParams
		itemPath := namespacedPath + "/{name}"
		nameParams := append(namespaceParams, nameParam)
		proxyParams := append(nameParams, pathParam)
		itemPathSuffix := ""
		if isSubresource {
			itemPathSuffix = "/" + subresource
			itemPath = itemPath + itemPathSuffix
			resourcePath = itemPath
			resourceParams = nameParams
		}
		apiResource.Name = path
		apiResource.Namespaced = true
		apiResource.Kind = resourceKind
		namer := handlers.ContextBasedNaming{
			Namer:         a.group.Namer,
			ClusterScoped: false,
		}

		actions = appendIf(actions, action{request.MethodList, resourcePath, resourceParams, namer, false}, isLister)
		actions = appendIf(actions, action{request.MethodPost, resourcePath, resourceParams, namer, false}, isCreater)
		actions = appendIf(actions, action{request.MethodDeleteCollection, resourcePath, resourceParams, namer, false}, isCollectionDeleter)
		// 已在 1.11 弃用
		actions = appendIf(actions, action{request.MethodWatchList, "watch/" + resourcePath, resourceParams, namer, false}, allowWatchList)

		actions = appendIf(actions, action{request.MethodGet, itemPath, nameParams, namer, false}, isGetter)
		if getSubpath {
			actions = appendIf(actions, action{request.MethodGet, itemPath + "/{path:*}", proxyParams, namer, false}, isGetter)
		}
		actions = appendIf(actions, action{request.MethodPut, itemPath, nameParams, namer, false}, isUpdater)
		actions = appendIf(actions, action{request.MethodPatch, itemPath, nameParams, namer, false}, isPatcher)
		actions = appendIf(actions, action{request.MethodDelete, itemPath, nameParams, namer, false}, isGracefulDeleter)
		// 已在 1.11 弃用
		actions = appendIf(actions, action{request.MethodWatch, "watch/" + itemPath, nameParams, namer, false}, isWatcher)
		actions = appendIf(actions, action{request.MethodConnect, itemPath, nameParams, namer, false}, isConnecter)
		actions = appendIf(actions, action{request.MethodConnect, itemPath + "/{path:*}", proxyParams, namer, false}, isConnecter && connectSubpath)

		// 跨命名空间的 list 或 post。
		// 例如：向 /api/apiVersion/pods 发 LIST 请求即可列出所有命名空间的 pod。
		// TODO: 用更强的类型表达某个资源是否允许这些“所有命名空间”上的操作（批量删除）。
		// 跨 namespace 的 list/watch 路径不带 namespace 段，因此单独注册；
		// 子资源不支持这种聚合形式。
		if !isSubresource {
			actions = appendIf(actions, action{request.MethodList, resource, params, namer, true}, isLister)
			// 已在 1.11 弃用
			actions = appendIf(actions, action{request.MethodWatchList, "watch/" + resource, params, namer, true}, allowWatchList)
		}
	}

	var resourceInfo *storageversion.ResourceInfo
	if utilfeature.DefaultFeatureGate.Enabled(features.StorageVersionAPI) &&
		utilfeature.DefaultFeatureGate.Enabled(features.APIServerIdentity) &&
		isStorageVersionProvider &&
		storageVersionProvider.StorageVersion() != nil {

		versioner := storageVersionProvider.StorageVersion()
		encodingGVK, err := getStorageVersionKind(versioner, storage, a.group.Typer)
		if err != nil {
			return nil, nil, err
		}
		decodableVersions := []schema.GroupVersion{}
		if a.group.ConvertabilityChecker != nil {
			decodableVersions = a.group.ConvertabilityChecker.VersionsForGroupKind(fqKindToRegister.GroupKind())
		}

		resourceInfo = &storageversion.ResourceInfo{
			GroupResource: schema.GroupResource{
				Group:    a.group.GroupVersion.Group,
				Resource: apiResource.Name,
			},
			EncodingVersion: encodingGVK.GroupVersion().String(),
			// 这里先记录 EquivalentResourceMapper，而不是立刻计算 DecodableVersions，
			// 因为必须先完成 API 安装，我们才能知道哪些 API 是等价的。
			EquivalentResourceMapper: a.group.EquivalentResourceRegistry,

			DirectlyDecodableVersions: decodableVersions,

			ServedVersions: a.group.AllServedVersionsByResource[path],
		}
	}

	// 为各个 action 创建 Route。
	// TODO: 用 Returns() 补充状态码文档。
	// 可能的错误（见 api/errors/errors.go 以及 go-restful router）：
	// http.StatusNotFound, http.StatusMethodNotAllowed,
	// http.StatusUnsupportedMediaType, http.StatusNotAcceptable,
	// http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
	// http.StatusRequestTimeout, http.StatusConflict, http.StatusPreconditionFailed,
	// http.StatusUnprocessableEntity, http.StatusInternalServerError,
	// http.StatusServiceUnavailable
	// 以及 api 错误码。
	// 注意：如果这里指定了带版本的 Status 对象，测试里可能也需要构造一个。
	// 成功的状态码：
	// http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent
	//
	// test/integration/auth_test.go 目前是对状态码覆盖最全面的测试。

	for _, s := range a.group.Serializer.SupportedMediaTypes() {
		if len(s.MediaTypeSubType) == 0 || len(s.MediaTypeType) == 0 {
			return nil, nil, fmt.Errorf("all serializers in the group Serializer must have MediaTypeType and MediaTypeSubType set: %s", s.MediaType)
		}
	}
	mediaTypes, streamMediaTypes := negotiation.MediaTypesForSerializer(a.group.Serializer)
	allMediaTypes := append(mediaTypes, streamMediaTypes...)
	ws.Produces(allMediaTypes...)

	kubeVerbs := map[string]struct{}{}
	reqScope := handlers.RequestScope{
		Serializer:      a.group.Serializer,
		ParameterCodec:  a.group.ParameterCodec,
		Creater:         a.group.Creater,
		Convertor:       a.group.Convertor,
		Defaulter:       a.group.Defaulter,
		Typer:           a.group.Typer,
		UnsafeConvertor: a.group.UnsafeConvertor,
		Authorizer:      a.group.Authorizer,

		EquivalentResourceMapper: a.group.EquivalentResourceRegistry,

		// TODO: 检查 storage 是否实现该接口。
		TableConvertor: tableProvider,

		// TODO: 对跨 group 的子资源来说这里似乎不对：它假设子资源与父资源在同一 group version。后续需要重新审视。
		Resource:    a.group.GroupVersion.WithResource(resource),
		Subresource: subresource,
		Kind:        fqKindToRegister,

		AcceptsGroupVersionDelegate: gvAcceptor,

		HubGroupVersion: schema.GroupVersion{Group: fqKindToRegister.Group, Version: runtime.APIVersionInternal},

		MetaGroupVersion: metav1.SchemeGroupVersion,

		MaxRequestBodyBytes: a.group.MaxRequestBodyBytes,
	}
	if a.group.MetaGroupVersion != nil {
		reqScope.MetaGroupVersion = *a.group.MetaGroupVersion
	}

	// strategy 可能通过重置字段值来忽略对某些字段的修改。
	//
	// 例如，spec 资源的 strategy 应重置 status，status 子资源的
	// strategy 应重置 spec。
	//
	// 会重置字段的 strategy 必须通过实现 ResetFieldsStrategy 或
	// ResetFieldsFilterStrategy 接口，告知字段管理器哪些字段被重置。
	//
	// 对于只允许写入特定嵌套字段的子资源，
	// fieldpath.NewPatternFilter 可以帮助构造一个过滤掉其余字段的 filter。
	var resetFieldsFilter map[fieldpath.APIVersion]fieldpath.Filter
	resetFieldsStrategy, isResetFieldsStrategy := storage.(rest.ResetFieldsStrategy)
	if isResetFieldsStrategy {
		resetFieldsFilter = fieldpath.NewExcludeFilterSetMap(resetFieldsStrategy.GetResetFields())
	}
	if resetFieldsStrategy, isResetFieldsFilterStrategy := storage.(rest.ResetFieldsFilterStrategy); isResetFieldsFilterStrategy {
		if isResetFieldsStrategy {
			return nil, nil, fmt.Errorf("may not implement both ResetFieldsStrategy and ResetFieldsFilterStrategy")
		}
		resetFieldsFilter = resetFieldsStrategy.GetResetFieldsFilter()
	}

	reqScope.FieldManager, err = managedfields.NewDefaultFieldManager(
		a.group.TypeConverter,
		a.group.UnsafeConvertor,
		a.group.Defaulter,
		a.group.Creater,
		fqKindToRegister,
		reqScope.HubGroupVersion,
		subresource,
		resetFieldsFilter,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create field manager: %v", err)
	}

	for _, action := range actions {
		// producedObject 决定 OpenAPI/路由的返回类型；storage 可按 verb 定制，
		// 未定制时回退到资源的默认外部对象。
		producedObject := storageMeta.ProducesObject(action.Verb)
		if producedObject == nil {
			producedObject = defaultVersionedObject
		}
		reqScope.Namer = action.Namer

		requestScope := "cluster"
		var namespaced string
		var operationSuffix string
		// requestScope/operationSuffix 只影响指标标签与 operationId 命名，
		// 不改变实际路由；两者共同保证不同形态的端点不会在监控里混淆。
		if apiResource.Namespaced {
			requestScope = "namespace"
			namespaced = "Namespaced"
		}
		if strings.HasSuffix(action.Path, "/{path:*}") {
			requestScope = "resource"
			operationSuffix = operationSuffix + "WithPath"
		}
		if strings.Contains(action.Path, "/{name}") || action.Verb == request.MethodPost {
			requestScope = "resource"
		}
		if action.AllNamespaces {
			requestScope = "cluster"
			operationSuffix = operationSuffix + "ForAllNamespaces"
			namespaced = ""
		}

		if kubeVerb, found := toDiscoveryKubeVerb[action.Verb]; found {
			if len(kubeVerb) != 0 {
				kubeVerbs[kubeVerb] = struct{}{}
			}
		} else {
			return nil, nil, fmt.Errorf("unknown action verb for discovery: %s", action.Verb)
		}

		routes := []*restful.RouteBuilder{}

		// 若存在子资源，kind 应取父资源的 kind。
		if isSubresource {
			parentStorage, ok := a.group.Storage[resource]
			if !ok {
				return nil, nil, fmt.Errorf("missing parent storage: %q", resource)
			}

			fqParentKind, err := GetResourceKind(a.group.GroupVersion, parentStorage, a.group.Typer)
			if err != nil {
				return nil, nil, err
			}
			kind = fqParentKind.Kind
		}

		verbOverrider, needOverride := storage.(StorageMetricsOverride)

		// 收集端点级别的警告。
		// 弃用警告按端点累积，随后包在 handler 外层，确保所有动词都能返回
		// Warning 头。
		var (
			warnings       []string
			deprecated     bool
			removedRelease string
		)

		{
			// 必须给副本设置 GVK 再判断弃用，否则 deprecation 包无法解析注解；
			// 直接修改 versionedPtr 会污染后续路由的默认对象。
			versionedPtrWithGVK := versionedPtr.DeepCopyObject()
			versionedPtrWithGVK.GetObjectKind().SetGroupVersionKind(fqKindToRegister)
			currentMajor, currentMinor, _ := deprecation.MajorMinor(versioninfo.Get())
			deprecated = deprecation.IsDeprecated(versionedPtrWithGVK, currentMajor, currentMinor)
			if deprecated {
				removedRelease = deprecation.RemovedRelease(versionedPtrWithGVK)
				warnings = append(warnings, deprecation.WarningMessage(versionedPtrWithGVK))
			}
		}

		switch action.Verb {
		case request.MethodGet: // 读取资源。
			var handler restful.RouteFunction
			if isGetterWithOptions {
				handler = restfulGetResourceWithOptions(getterWithOptions, reqScope, isSubresource)
			} else {
				handler = restfulGetResource(getter, reqScope)
			}

			if needOverride {
				// 需要改变上报的 verb。
				handler = metrics.InstrumentRouteFunc(verbOverrider.OverrideMetricsVerb(action.Verb), group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			} else {
				handler = metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			}
			handler = utilwarning.AddWarningsHandler(handler, warnings)

			doc := "read the specified " + kind
			if isSubresource {
				doc = "read " + subresource + " of the specified " + kind
			}
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("read"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				Writes(producedObject)
			if isGetterWithOptions {
				if err := AddObjectParams(ws, route, versionedGetOptions); err != nil {
					return nil, nil, err
				}
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodList: // 列出某种 Kind 的全部资源。
			doc := "list objects of kind " + kind
			if isSubresource {
				doc = "list " + subresource + " of objects of kind " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, false, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("list"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), allMediaTypes...)...).
				Returns(http.StatusOK, "OK", versionedList).
				Writes(versionedList)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			switch {
			case isLister && isWatcher:
				doc := "list or watch objects of kind " + kind
				if isSubresource {
					doc = "list or watch " + subresource + " of objects of kind " + kind
				}
				route.Doc(doc)
			case isWatcher:
				doc := "watch objects of kind " + kind
				if isSubresource {
					doc = "watch " + subresource + "of objects of kind " + kind
				}
				route.Doc(doc)
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodPut: // 更新资源。
			doc := "replace the specified " + kind
			if isSubresource {
				doc = "replace " + subresource + " of the specified " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulUpdateResource(updater, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.PUT(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("replace"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// TODO: 某些情况下 API 可能返回 v1.Status 而不是带版本的对象，
				// 但目前 go-restful 无法处理返回多种不同类型对象的情形。
				Returns(http.StatusCreated, "Created", producedObject).
				Reads(defaultVersionedObject).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedUpdateOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodPatch: // 部分更新资源。
			doc := "partially update the specified " + kind
			if isSubresource {
				doc = "partially update " + subresource + " of the specified " + kind
			}
			supportedTypes := []string{
				string(types.JSONPatchType),
				string(types.MergePatchType),
				string(types.StrategicMergePatchType),
				string(types.ApplyYAMLPatchType),
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
				supportedTypes = append(supportedTypes, string(types.ApplyCBORPatchType))
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulPatchResource(patcher, reqScope, admit, supportedTypes))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.PATCH(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Consumes(supportedTypes...).
				Operation("patch"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// 请求 server-side apply 时，PATCH 可能返回 201。
				Returns(http.StatusCreated, "Created", producedObject).
				Reads(metav1.Patch{}).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedPatchOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodPost: // 创建资源。
			var handler restful.RouteFunction
			if isNamedCreater {
				handler = restfulCreateNamedResource(namedCreater, reqScope, admit)
			} else {
				handler = restfulCreateResource(creater, reqScope, admit)
			}
			handler = metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			article := GetArticleForNoun(kind, " ")
			doc := "create" + article + kind
			if isSubresource {
				doc = "create " + subresource + " of" + article + kind
			}
			route := ws.POST(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("create"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// TODO: 某些情况下 API 可能返回 v1.Status 而不是带版本的对象，
				// 但目前 go-restful 无法处理返回多种不同类型对象的情形。
				Returns(http.StatusCreated, "Created", producedObject).
				Returns(http.StatusAccepted, "Accepted", producedObject).
				Reads(defaultVersionedObject).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedCreateOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodDelete: // 删除资源。
			article := GetArticleForNoun(kind, " ")
			doc := "delete" + article + kind
			if isSubresource {
				doc = "delete " + subresource + " of" + article + kind
			}
			deleteReturnType := versionedStatus
			if deleteReturnsDeletedObject {
				deleteReturnType = producedObject
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulDeleteResource(gracefulDeleter, isGracefulDeleter, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.DELETE(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("delete"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Writes(deleteReturnType).
				Returns(http.StatusOK, "OK", deleteReturnType).
				Returns(http.StatusAccepted, "Accepted", deleteReturnType)
			if isGracefulDeleter {
				route.Reads(versionedDeleterObject)
				route.ParameterNamed("body").Required(false)
				if err := AddObjectParams(ws, route, versionedDeleteOptions); err != nil {
					return nil, nil, err
				}
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodDeleteCollection:
			doc := "delete collection of " + kind
			if isSubresource {
				doc = "delete collection of " + subresource + " of a " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulDeleteCollection(collectionDeleter, isCollectionDeleter, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.DELETE(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("deletecollection"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Writes(versionedStatus).
				Returns(http.StatusOK, "OK", versionedStatus)
			if isCollectionDeleter {
				route.Reads(versionedDeleterObject)
				route.ParameterNamed("body").Required(false)
				if err := AddObjectParams(ws, route, versionedDeleteOptions); err != nil {
					return nil, nil, err
				}
			}
			if err := AddObjectParams(ws, route, versionedListOptions, "watch", "allowWatchBookmarks"); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
			// 1.11 起弃用
		case request.MethodWatch: // watch 某个资源。
			doc := "watch changes to an object of kind " + kind
			if isSubresource {
				doc = "watch changes to " + subresource + " of an object of kind " + kind
			}
			doc += ". deprecated: use the 'watch' parameter with a list operation instead, filtered to a single item with the 'fieldSelector' parameter."
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, true, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("watch"+namespaced+kind+cases.Title(language.English).String(subresource)+operationSuffix).
				Produces(allMediaTypes...).
				Returns(http.StatusOK, "OK", versionedWatchEvent).
				Writes(versionedWatchEvent)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
			// 1.11 起弃用
		case request.MethodWatchList: // watch 某种 Kind 的全部资源。
			doc := "watch individual changes to a list of " + kind
			if isSubresource {
				doc = "watch individual changes to a list of " + subresource + " of " + kind
			}
			doc += ". deprecated: use the 'watch' parameter with a list operation instead."
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, true, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("watch"+namespaced+kind+cases.Title(language.English).String(subresource)+"List"+operationSuffix).
				Produces(allMediaTypes...).
				Returns(http.StatusOK, "OK", versionedWatchEvent).
				Writes(versionedWatchEvent)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodConnect:
			for _, method := range connecter.ConnectMethods() {
				connectProducedObject := storageMeta.ProducesObject(method)
				if connectProducedObject == nil {
					connectProducedObject = "string"
				}
				doc := "connect " + method + " requests to " + kind
				if isSubresource {
					doc = "connect " + method + " requests to " + subresource + " of " + kind
				}
				handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulConnectResource(connecter, reqScope, admit, path, isSubresource))
				handler = utilwarning.AddWarningsHandler(handler, warnings)
				route := ws.Method(method).Path(action.Path).
					To(handler).
					Doc(doc).
					Operation("connect" + cases.Title(language.English).String(strings.ToLower(method)) + namespaced + kind + cases.Title(language.English).String(subresource) + operationSuffix).
					Produces("*/*").
					Consumes("*/*").
					Writes(connectProducedObject)
				if versionedConnectOptions != nil {
					if err := AddObjectParams(ws, route, versionedConnectOptions); err != nil {
						return nil, nil, err
					}
				}
				addParams(route, action.Params)
				routes = append(routes, route)

				// 把 ConnectMethods 转换成 kube verb。
				if kubeVerb, found := toDiscoveryKubeVerb[method]; found {
					if len(kubeVerb) != 0 {
						kubeVerbs[kubeVerb] = struct{}{}
					}
				}
			}
		default:
			return nil, nil, fmt.Errorf("unrecognized action verb: %s", action.Verb)
		}
		for _, route := range routes {
			// GVK 取自 reqScope.Kind 而非 fqKindToRegister，因为子资源的
			// Kind 已在上面被替换为父资源 Kind，requestinfo 依赖该值推导鉴权属性。
			route.Metadata(RouteMetaGVK, metav1.GroupVersionKind{
				Group:   reqScope.Kind.Group,
				Version: reqScope.Kind.Version,
				Kind:    reqScope.Kind.Kind,
			})
			route.Metadata(RouteMetaAction, strings.ToLower(action.Verb))
			ws.Route(route)
		}
		// 注意：新增自定义 handler 时要同步更新 GetAuthorizerAttributes()。
	}

	apiResource.Verbs = make([]string, 0, len(kubeVerbs))
	for kubeVerb := range kubeVerbs {
		apiResource.Verbs = append(apiResource.Verbs, kubeVerb)
	}
	// 排序保证 discovery 输出稳定，客户端缓存不会因顺序变化而失效。
	sort.Strings(apiResource.Verbs)

	if shortNamesProvider, ok := storage.(rest.ShortNamesProvider); ok {
		apiResource.ShortNames = shortNamesProvider.ShortNames()
	}
	if categoriesProvider, ok := storage.(rest.CategoriesProvider); ok {
		apiResource.Categories = categoriesProvider.Categories()
	}
	if !isSubresource {
		singularNameProvider, ok := storage.(rest.SingularNameProvider)
		if !ok {
			return nil, nil, fmt.Errorf("resource %s must implement SingularNameProvider", resource)
		}
		apiResource.SingularName = singularNameProvider.GetSingularName()
	}

	if gvkProvider, ok := storage.(rest.GroupVersionKindProvider); ok {
		gvk := gvkProvider.GroupVersionKind(a.group.GroupVersion)
		apiResource.Group = gvk.Group
		apiResource.Version = gvk.Version
		apiResource.Kind = gvk.Kind
	}

	// 记录该 GVR 及其对应 GVK 的存在。
	a.group.EquivalentResourceRegistry.RegisterKindFor(reqScope.Resource, reqScope.Subresource, fqKindToRegister)

	return &apiResource, resourceInfo, nil
}

// indirectArbitraryPointer 对任意指针返回 *ptrToObject。
// storage 构造的对象通常是 *T，而路由描述需要 T 值，这里做一次反射解引用。
func indirectArbitraryPointer(ptrToObject interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(ptrToObject)).Interface()
}

// appendIf 仅在条件成立时追加 action，使动词注册表保持声明式风格。
func appendIf(actions []action, a action, shouldAppend bool) []action {
	if shouldAppend {
		actions = append(actions, a)
	}
	return actions
}

func addParams(route *restful.RouteBuilder, params []*restful.Parameter) {
	for _, param := range params {
		route.Param(param)
	}
}

// AddObjectParams 把一个 runtime.Object 转换成路由上的一组 go-restful Param() 定义。
// 对象必须是指向结构体的指针；只使用结构体顶层、本身不是接口或结构体的字段；
// 只有 json tag 非空（即 Go JSON 不会省略该字段）的字段才会成为 query 参数，
// query 参数名即 JSON 字段名；若字段上设置了 description struct tag，
// 该描述会用于 query 参数。本质上它把标准 JSON 顶层对象转换成了 query 参数模式。
//
// 只处理顶层标量字段是刻意限制——嵌套结构无法用 query 参数表达。
// excludedNames 用于屏蔽由路径/框架处理的字段（如 watch、allowWatchBookmarks），
// excludedNames 用于屏蔽由路径/框架处理的字段（如 watch、allowWatchBookmarks），
// 避免同一参数出现两次。
func AddObjectParams(ws *restful.WebService, route *restful.RouteBuilder, obj interface{}, excludedNames ...string) error {
	sv, err := conversion.EnforcePtr(obj)
	if err != nil {
		return err
	}
	st := sv.Type()
	excludedNameSet := sets.NewString(excludedNames...)
	switch st.Kind() {
	case reflect.Struct:
		for i := 0; i < st.NumField(); i++ {
			name := st.Field(i).Name
			sf, ok := st.FieldByName(name)
			if !ok {
				continue
			}
			switch sf.Type.Kind() {
			case reflect.Interface, reflect.Struct:
			case reflect.Pointer:
				// TODO: 这是为了让 metav1.Time 通过而做的临时 hack，最终应以更通用的方式修复，见 bug #36191。
				if (sf.Type.Elem().Kind() == reflect.Interface || sf.Type.Elem().Kind() == reflect.Struct) && strings.TrimPrefix(sf.Type.String(), "*") != "metav1.Time" {
					continue
				}
				fallthrough
			default:
				jsonTag, _ := sf.Tag.Lookup("json")
				if len(jsonTag) == 0 {
					continue
				}
				jsonName := strings.SplitN(jsonTag, ",", 2)[0]
				if len(jsonName) == 0 {
					continue
				}
				if excludedNameSet.Has(jsonName) {
					continue
				}
				var desc string
				if docable, ok := obj.(documentable); ok {
					desc = docable.SwaggerDoc()[jsonName]
				}
				route.Param(ws.QueryParameter(jsonName, desc).DataType(typeToJSON(sf.Type.String())))
			}
		}
	}
	return nil
}

// TODO: 该实现不完整，按需扩展。
// typeToJSON 把 Go 类型名转换成 JSON 类型名。
// 仅为 OpenAPI 参数类型标注服务；未列出的类型原样返回，属于已知的
// 不完整实现（见上方待办注释）。
func typeToJSON(typeName string) string {
	switch typeName {
	case "bool", "*bool":
		return "boolean"
	case "uint8", "*uint8", "int", "*int", "int32", "*int32", "int64", "*int64", "uint32", "*uint32", "uint64", "*uint64":
		return "integer"
	case "float64", "*float64", "float32", "*float32":
		return "number"
	case "metav1.Time", "*metav1.Time":
		return "string"
	case "byte", "*byte":
		return "string"
	case "v1.DeletionPropagation", "*v1.DeletionPropagation":
		return "string"
	case "v1.ResourceVersionMatch", "*v1.ResourceVersionMatch":
		return "string"
	case "v1.IncludeObjectPolicy", "*v1.IncludeObjectPolicy":
		return "string"
	case "*string":
		return "string"

		// TODO: 待 go-restful 支持声明数组型 query 参数后再修正这些类型：
		// https://github.com/emicklei/go-restful/issues/225
	case "[]string", "[]*string":
		return "string"
	case "[]int32", "[]*int32":
		return "integer"

	default:
		return typeName
	}
}

// defaultStorageMetadata 为 rest.StorageMetadata 提供默认回答。
// 未实现 StorageMetadata 的 storage 使用此默认值，表示“不定制任何
// MIME 类型或返回对象”。
type defaultStorageMetadata struct{}

// defaultStorageMetadata 实现 rest.StorageMetadata。
var _ rest.StorageMetadata = defaultStorageMetadata{}

func (defaultStorageMetadata) ProducesMIMETypes(verb string) []string {
	return nil
}

func (defaultStorageMetadata) ProducesObject(verb string) interface{} {
	return nil
}

// splitSubresource 判断给定 storage 路径是否为子资源路径，
// 并返回资源与子资源两部分。
// 只允许一到两段路径；更深的层级会让 requestinfo 无法可靠解析，故显式拒绝。
func splitSubresource(path string) (string, string, error) {
	var resource, subresource string
	switch parts := strings.Split(path, "/"); len(parts) {
	case 2:
		resource, subresource = parts[0], parts[1]
	case 1:
		resource = parts[0]
	default:
		// TODO: 支持更深层的路径
		return "", "", fmt.Errorf("api_installer allows only one or two segment paths (resource or resource/subresource)")
	}
	return resource, subresource, nil
}

// GetArticleForNoun 返回给定名词前需要的冠词。
// 仅用于生成人类可读的文档描述；复数名词不加冠词，避免 "a pods"。
func GetArticleForNoun(noun string, padding string) string {
	if !strings.HasSuffix(noun, "ss") && strings.HasSuffix(noun, "s") {
		// 复数名词不加冠词。
		// 也不要误伤 class 这类词。
		return fmt.Sprintf("%v", padding)
	}

	article := "a"
	if isVowel(rune(noun[0])) {
		article = "an"
	}

	return fmt.Sprintf("%s%s%s", padding, article, padding)
}

// isVowel 判断该 rune 是否为元音字母（不区分大小写）。
func isVowel(c rune) bool {
	vowels := []rune{'a', 'e', 'i', 'o', 'u'}
	for _, value := range vowels {
		if value == unicode.ToLower(c) {
			return true
		}
	}
	return false
}

func restfulListResource(r rest.Lister, rw rest.Watcher, scope handlers.RequestScope, forceWatch bool, minRequestTimeout time.Duration) restful.RouteFunction {
	// 以下 restful* 函数只是把 net/http handler 适配成 go-restful 的
	// RouteFunction；scope 按值捕获，注册后不再修改，因此可安全共享。
	return func(req *restful.Request, res *restful.Response) {
		handlers.ListResource(r, rw, &scope, forceWatch, minRequestTimeout)(res.ResponseWriter, req.Request)
	}
}

func restfulCreateNamedResource(r rest.NamedCreater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.CreateNamedResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulCreateResource(r rest.Creater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.CreateResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulDeleteResource(r rest.GracefulDeleter, allowsOptions bool, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.DeleteResource(r, allowsOptions, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulDeleteCollection(r rest.CollectionDeleter, checkBody bool, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.DeleteCollection(r, checkBody, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulUpdateResource(r rest.Updater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.UpdateResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulPatchResource(r rest.Patcher, scope handlers.RequestScope, admit admission.Interface, supportedTypes []string) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.PatchResource(r, &scope, admit, supportedTypes)(res.ResponseWriter, req.Request)
	}
}

func restfulGetResource(r rest.Getter, scope handlers.RequestScope) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.GetResource(r, &scope)(res.ResponseWriter, req.Request)
	}
}

func restfulGetResourceWithOptions(r rest.GetterWithOptions, scope handlers.RequestScope, isSubresource bool) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.GetResourceWithOptions(r, &scope, isSubresource)(res.ResponseWriter, req.Request)
	}
}

func restfulConnectResource(connecter rest.Connecter, scope handlers.RequestScope, admit admission.Interface, restPath string, isSubresource bool) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.ConnectResource(connecter, &scope, admit, restPath, isSubresource)(res.ResponseWriter, req.Request)
	}
}
