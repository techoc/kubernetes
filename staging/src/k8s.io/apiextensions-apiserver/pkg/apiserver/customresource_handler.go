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

package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/conversion"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	schemaobjectmeta "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta"
	structuralpruning "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/controller/establish"
	"k8s.io/apiextensions-apiserver/pkg/controller/finalizer"
	"k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder"
	"k8s.io/apiextensions-apiserver/pkg/crdserverscheme"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresource"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresource/tableconvertor"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/runtime/serializer/versioning"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilwaitgroup "k8s.io/apimachinery/pkg/util/waitgroup"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/generic"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/scale/scheme/autoscalingv1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// crdHandler 是 /apis 端点的核心请求处理器。
// 作为 HTTP filter 注册，拦截所有 /apis 请求，动态路由到已注册的 CR 端点。
// 当 CRD 创建/更新/删除时，自动安装/更新/卸载对应的 CR HTTP 路由。
//
// 使用 atomic.Value 存储 customStorage，因为读取远多于写入，
// atomic.Value 的读取性能优于 sync.RWMutex。
type crdHandler struct {
	versionDiscoveryHandler *versionDiscoveryHandler
	groupDiscoveryHandler   *groupDiscoveryHandler

	customStorageLock sync.Mutex
	// customStorage 保存 crdStorageMap，
	// 选 atomic.Value 而非 sync.RWMutex 是因为读取极快，
	// 性能对比见 https://gist.github.com/dim/152e6bf80e1384ea72e17ac717a5000a。
	// 适合这里读多写少的场景。
	customStorage atomic.Value

	crdLister listers.CustomResourceDefinitionLister

	delegate          http.Handler
	restOptionsGetter generic.RESTOptionsGetter
	admission         admission.Interface

	establishingController *establish.EstablishingController

	// MasterCount 用于插入等待，缓解 HA 集群下
	// CRD 建立过程中的竞态。
	masterCount int

	converterFactory *conversion.CRConverterFactory

	// 这样可以在 update 路径里完成 create。
	authorizer authorizer.UnconditionalAuthorizer

	// 请求超时时间，据此决定存储销毁要延迟多久。
	requestTimeout time.Duration

	// minRequestTimeout 作用于自定义资源的 list/watch。
	minRequestTimeout time.Duration

	// staticOpenAPISpec 作为 CR schema 的基础，
	// CR handler 由此得知 TypeMeta 与 ObjectMeta 的结构，
	// 以便正确管理字段。
	staticOpenAPISpec map[string]*spec.Schema

	// 写请求中接受并解码的请求体大小上限，
	// 0 表示不限。
	maxRequestBodyBytes int64
}

// crdInfo 保存为自定义资源提供存储所需的全部信息。
type crdInfo struct {
	// spec 与 acceptedNames 用于判断 CRD 是否变化；只有这两者改变
	// 才需要更新存储。
	spec          *apiextensionsv1.CustomResourceDefinitionSpec
	acceptedNames *apiextensionsv1.CustomResourceDefinitionNames

	// 各版本的 deprecated 标记。
	deprecated map[string]bool

	// 各版本的 warnings。
	warnings map[string][]string

	// 各版本的存储。
	storages map[string]customresource.CustomResourceStorage

	// 各版本的请求 scope。
	requestScopes map[string]*handlers.RequestScope

	// 各版本的 scale 请求 scope。
	scaleRequestScopes map[string]*handlers.RequestScope

	// 各版本的 status 请求 scope。
	statusRequestScopes map[string]*handlers.RequestScope

	// storageVersion 是对象存入 etcd 时使用的 CRD 版本。
	storageVersion string

	waitGroup *utilwaitgroup.SafeWaitGroup
}

// crdStorageMap 把 CRD 映射到其存储：
// crdStorageMap 映射 CRD 的 UID 到其存储信息（REST Storage、schema 等）。
type crdStorageMap map[types.UID]*crdInfo

// NewCustomResourceDefinitionHandler 创建 CRD 请求处理器。
// 处理器监听 CRD 的增删改事件，动态安装/卸载 CR 的 HTTP 路由。
func NewCustomResourceDefinitionHandler(
	versionDiscoveryHandler *versionDiscoveryHandler,
	groupDiscoveryHandler *groupDiscoveryHandler,
	crdInformer informers.CustomResourceDefinitionInformer,
	delegate http.Handler,
	restOptionsGetter generic.RESTOptionsGetter,
	admission admission.Interface,
	establishingController *establish.EstablishingController,
	serviceResolver webhook.ServiceResolver,
	authResolverWrapper webhook.AuthenticationInfoResolverWrapper,
	masterCount int,
	authorizer authorizer.UnconditionalAuthorizer,
	requestTimeout time.Duration,
	minRequestTimeout time.Duration,
	staticOpenAPISpec map[string]*spec.Schema,
	maxRequestBodyBytes int64) (*crdHandler, error) {
	ret := &crdHandler{
		versionDiscoveryHandler: versionDiscoveryHandler,
		groupDiscoveryHandler:   groupDiscoveryHandler,
		customStorage:           atomic.Value{},
		crdLister:               crdInformer.Lister(),
		delegate:                delegate,
		restOptionsGetter:       restOptionsGetter,
		admission:               admission,
		establishingController:  establishingController,
		masterCount:             masterCount,
		authorizer:              authorizer,
		requestTimeout:          requestTimeout,
		minRequestTimeout:       minRequestTimeout,
		staticOpenAPISpec:       staticOpenAPISpec,
		maxRequestBodyBytes:     maxRequestBodyBytes,
	}
	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ret.createCustomResourceDefinition,
		UpdateFunc: ret.updateCustomResourceDefinition,
		DeleteFunc: func(obj interface{}) {
			ret.removeDeadStorage()
		},
	})
	crConverterFactory, err := conversion.NewCRConverterFactory(serviceResolver, authResolverWrapper)
	if err != nil {
		return nil, err
	}
	ret.converterFactory = crConverterFactory

	ret.customStorage.Store(crdStorageMap{})

	return ret, nil
}

// watch 应当优雅处理存储中断：
// 服务端主动断开连接，
// 客户端负责重新发起 watch。
var longRunningFilter = genericfilters.BasicLongRunningRequestCheck(sets.NewString("watch"), sets.NewString())

// possiblyAcrossAllNamespacesVerbs 列出那些对命名空间资源既可限定命名空间、
// 也可跨全部命名空间的 verb；这些情况下 requestInfo 的 namespace 为空是合法的。
var possiblyAcrossAllNamespacesVerbs = sets.NewString("list", "watch")

func (r *crdHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	requestInfo, ok := apirequest.RequestInfoFrom(ctx)
	if !ok {
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("no RequestInfo found in the context")),
			Codecs, schema.GroupVersion{}, w, req,
		)
		return
	}
	if !requestInfo.IsResourceRequest {
		pathParts := splitPath(requestInfo.Path)
		// 只匹配 /apis/<group>/<version>，
		// 因为路由只注册在 /apis 下。
		if len(pathParts) == 3 {
			r.versionDiscoveryHandler.ServeHTTP(w, req)
			return
		}
		// 只匹配 /apis/<group>。
		if len(pathParts) == 2 {
			r.groupDiscoveryHandler.ServeHTTP(w, req)
			return
		}

		r.delegate.ServeHTTP(w, req)
		return
	}

	crdName := requestInfo.Resource + "." + requestInfo.APIGroup
	crd, err := r.crdLister.Get(crdName)
	if apierrors.IsNotFound(err) {
		r.delegate.ServeHTTP(w, req)
		return
	}
	if err != nil {
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("error resolving resource")),
			Codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, req,
		)
		return
	}

	// 若 CRD 的作用域与请求的作用域不一致（命名空间资源中 possiblyAcrossAllNamespacesVerbs 所列 verb 除外），
	// 就把请求交给 delegate，预期返回 404。
	namespacedCRD, namespacedReq := crd.Spec.Scope == apiextensionsv1.NamespaceScoped, len(requestInfo.Namespace) > 0
	if !namespacedCRD && namespacedReq {
		r.delegate.ServeHTTP(w, req)
		return
	}
	if namespacedCRD && !namespacedReq && !possiblyAcrossAllNamespacesVerbs.Has(requestInfo.Verb) {
		r.delegate.ServeHTTP(w, req)
		return
	}

	if !apiextensionshelpers.HasServedCRDVersion(crd, requestInfo.APIVersion) {
		r.delegate.ServeHTTP(w, req)
		return
	}

	// CRD 可能因 NamesAccepted 为 true 而暂时对外服务，
	// 随后因名称更新冲突而变为 unserved；
	// EstablishingController 未能及时置为 Established 时会放大这个窗口。
	// 该窗口很短且能自愈，因此接受这一风险。
	if !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.NamesAccepted) &&
		!apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
		r.delegate.ServeHTTP(w, req)
		return
	}

	terminating := !crd.DeletionTimestamp.IsZero() || apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Terminating)

	crdInfo, err := r.getOrCreateServingInfoFor(crd.UID, crd.Name)
	if apierrors.IsNotFound(err) {
		r.delegate.ServeHTTP(w, req)
		return
	}
	if err != nil {
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("error resolving resource")),
			Codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, req,
		)
		return
	}
	if !hasServedCRDVersion(crdInfo.spec, requestInfo.APIVersion) {
		r.delegate.ServeHTTP(w, req)
		return
	}

	deprecated := crdInfo.deprecated[requestInfo.APIVersion]
	for _, w := range crdInfo.warnings[requestInfo.APIVersion] {
		warning.AddWarning(req.Context(), "", w)
	}

	verb := strings.ToUpper(requestInfo.Verb)
	resource := requestInfo.Resource
	subresource := requestInfo.Subresource
	scope := metrics.CleanScope(requestInfo)
	supportedTypes := []string{
		string(types.JSONPatchType),
		string(types.MergePatchType),
		string(types.ApplyYAMLPatchType),
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		supportedTypes = append(supportedTypes, string(types.ApplyCBORPatchType))
	}

	var handlerFunc http.HandlerFunc
	subresources, err := apiextensionshelpers.GetSubresourcesForVersion(crd, requestInfo.APIVersion)
	if err != nil {
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			apierrors.NewInternalError(fmt.Errorf("could not properly serve the subresource")),
			Codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, req,
		)
		return
	}
	switch {
	case subresource == "status" && subresources != nil && subresources.Status != nil:
		handlerFunc = r.serveStatus(w, req, requestInfo, crdInfo, terminating, supportedTypes)
	case subresource == "scale" && subresources != nil && subresources.Scale != nil:
		handlerFunc = r.serveScale(w, req, requestInfo, crdInfo, terminating, supportedTypes)
	case len(subresource) == 0:
		handlerFunc = r.serveResource(w, req, requestInfo, crdInfo, crd, terminating, supportedTypes)
	default:
		responsewriters.ErrorNegotiated(
			apierrors.NewNotFound(schema.GroupResource{Group: requestInfo.APIGroup, Resource: requestInfo.Resource}, requestInfo.Name),
			Codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, req,
		)
	}

	if handlerFunc != nil {
		handlerFunc = metrics.InstrumentHandlerFunc(verb, requestInfo.APIGroup, requestInfo.APIVersion, resource, subresource, scope, metrics.APIServerComponent, deprecated, "", handlerFunc)
		handler := genericfilters.WithWaitGroup(handlerFunc, longRunningFilter, crdInfo.waitGroup)
		handler.ServeHTTP(w, req)
		return
	}
}

func (r *crdHandler) serveResource(w http.ResponseWriter, req *http.Request, requestInfo *apirequest.RequestInfo, crdInfo *crdInfo, crd *apiextensionsv1.CustomResourceDefinition, terminating bool, supportedTypes []string) http.HandlerFunc {
	requestScope := crdInfo.requestScopes[requestInfo.APIVersion]
	storage := crdInfo.storages[requestInfo.APIVersion].CustomResource

	switch requestInfo.Verb {
	case "get":
		return handlers.GetResource(storage, requestScope)
	case "list":
		forceWatch := false
		return handlers.ListResource(storage, storage, requestScope, forceWatch, r.minRequestTimeout)
	case "watch":
		forceWatch := true
		return handlers.ListResource(storage, storage, requestScope, forceWatch, r.minRequestTimeout)
	case "create":
		// 记录近期创建的 CRD，避免 HA 环境下 A 服务器放行 create 后 B 服务器
		// 还没观察到 Established，使后续 get/update/delete 得到 404；实测延迟约 800ms。
		// 两秒足以覆盖该延迟，对频繁重试者也不算太久。
		justCreated := time.Since(apiextensionshelpers.FindCRDCondition(crd, apiextensionsv1.Established).LastTransitionTime.Time) < 2*time.Second
		if justCreated {
			time.Sleep(2 * time.Second)
		}

		a := r.admission
		if terminating {
			a = &forbidCreateAdmission{delegate: a}
		}
		return handlers.CreateResource(storage, requestScope, a)
	case "update":
		return handlers.UpdateResource(storage, requestScope, r.admission)
	case "patch":
		a := r.admission
		if terminating {
			a = &forbidCreateAdmission{delegate: a}
		}
		return handlers.PatchResource(storage, requestScope, a, supportedTypes)
	case "delete":
		allowsOptions := true
		return handlers.DeleteResource(storage, allowsOptions, requestScope, r.admission)
	case "deletecollection":
		checkBody := true
		return handlers.DeleteCollection(storage, checkBody, requestScope, r.admission)
	default:
		responsewriters.ErrorNegotiated(
			apierrors.NewMethodNotSupported(schema.GroupResource{Group: requestInfo.APIGroup, Resource: requestInfo.Resource}, requestInfo.Verb),
			Codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, req,
		)
		return nil
	}
}

func (r *crdHandler) serveStatus(w http.ResponseWriter, req *http.Request, requestInfo *apirequest.RequestInfo, crdInfo *crdInfo, terminating bool, supportedTypes []string) http.HandlerFunc {
	requestScope := crdInfo.statusRequestScopes[requestInfo.APIVersion]
	storage := crdInfo.storages[requestInfo.APIVersion].Status

	switch requestInfo.Verb {
	case "get":
		return handlers.GetResource(storage, requestScope)
	case "update":
		return handlers.UpdateResource(storage, requestScope, r.admission)
	case "patch":
		return handlers.PatchResource(storage, requestScope, r.admission, supportedTypes)
	default:
		responsewriters.ErrorNegotiated(
			apierrors.NewMethodNotSupported(schema.GroupResource{Group: requestInfo.APIGroup, Resource: requestInfo.Resource}, requestInfo.Verb),
			Codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, req,
		)
		return nil
	}
}

func (r *crdHandler) serveScale(w http.ResponseWriter, req *http.Request, requestInfo *apirequest.RequestInfo, crdInfo *crdInfo, terminating bool, supportedTypes []string) http.HandlerFunc {
	requestScope := crdInfo.scaleRequestScopes[requestInfo.APIVersion]
	storage := crdInfo.storages[requestInfo.APIVersion].Scale

	switch requestInfo.Verb {
	case "get":
		return handlers.GetResource(storage, requestScope)
	case "update":
		return handlers.UpdateResource(storage, requestScope, r.admission)
	case "patch":
		return handlers.PatchResource(storage, requestScope, r.admission, supportedTypes)
	default:
		responsewriters.ErrorNegotiated(
			apierrors.NewMethodNotSupported(schema.GroupResource{Group: requestInfo.APIGroup, Resource: requestInfo.Resource}, requestInfo.Verb),
			Codecs, schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}, w, req,
		)
		return nil
	}
}

// createCustomResourceDefinition 清理可能残留的旧存储，使其随后重建。
func (r *crdHandler) createCustomResourceDefinition(obj interface{}) {
	crd := obj.(*apiextensionsv1.CustomResourceDefinition)
	r.customStorageLock.Lock()
	defer r.customStorageLock.Unlock()
	// create 事件若与 update 事件合并，就可能出现这种情况。
	storageMap := r.customStorage.Load().(crdStorageMap)
	oldInfo, found := storageMap[crd.UID]
	if !found {
		return
	}
	if apiequality.Semantic.DeepEqual(&crd.Spec, oldInfo.spec) && apiequality.Semantic.DeepEqual(&crd.Status.AcceptedNames, oldInfo.acceptedNames) {
		klog.V(6).Infof("Ignoring customresourcedefinition %s create event because a storage with the same spec and accepted names exists",
			crd.Name)
		return
	}
	r.removeStorage_locked(crd.UID)
}

// updateCustomResourceDefinition 清理可能残留的旧存储，使其随后重建。
func (r *crdHandler) updateCustomResourceDefinition(oldObj, newObj interface{}) {
	oldCRD := oldObj.(*apiextensionsv1.CustomResourceDefinition)
	newCRD := newObj.(*apiextensionsv1.CustomResourceDefinition)

	r.customStorageLock.Lock()
	defer r.customStorageLock.Unlock()

	// 把 CRD 加入 establishing controller 队列。
	// HA 集群下改状态为 Established 存在竞态，
	// 因此要求 CRD 至少处于 Installing 状态 5 秒再推进。
	// TODO: 用真正 HA 安全的检查点机制替代这个拍脑袋的等待。
	if !apiextensionshelpers.IsCRDConditionTrue(newCRD, apiextensionsv1.Established) &&
		apiextensionshelpers.IsCRDConditionTrue(newCRD, apiextensionsv1.NamesAccepted) {
		if r.masterCount > 1 {
			r.establishingController.QueueCRD(newCRD.Name, 5*time.Second)
		} else {
			r.establishingController.QueueCRD(newCRD.Name, 0)
		}
	}

	if oldCRD.UID != newCRD.UID {
		r.removeStorage_locked(oldCRD.UID)
	}

	storageMap := r.customStorage.Load().(crdStorageMap)
	oldInfo, found := storageMap[newCRD.UID]
	if !found {
		return
	}
	if apiequality.Semantic.DeepEqual(&newCRD.Spec, oldInfo.spec) && apiequality.Semantic.DeepEqual(&newCRD.Status.AcceptedNames, oldInfo.acceptedNames) {
		klog.V(6).Infof("Ignoring customresourcedefinition %s update because neither spec, nor accepted names changed", oldCRD.Name)
		return
	}

	klog.V(4).Infof("Updating customresourcedefinition %s", newCRD.Name)
	r.removeStorage_locked(newCRD.UID)
}

// removeStorage_locked 按 UID 从缓存移除存储，用清理后的 storageMap
// 更新 r.customStorage，并销毁旧存储。
// 注意：调用方必须持有 r.customStorageLock，写 r.customStorage 才安全。
func (r *crdHandler) removeStorage_locked(uid types.UID) {
	storageMap := r.customStorage.Load().(crdStorageMap)
	if oldInfo, ok := storageMap[uid]; ok {
		// 先复制：storageMap 在无锁处被读取，
		// 直接写入会形成数据竞争。
		storageMap2 := storageMap.clone()

		// 从 CRD 信息表中删除并写回。
		delete(storageMap2, uid)
		r.customStorage.Store(storageMap2)

		// 销毁旧存储。
		go r.tearDown(oldInfo)
	}
}

// removeDeadStorage 清理不再使用的 REST 存储。
func (r *crdHandler) removeDeadStorage() {
	allCustomResourceDefinitions, err := r.crdLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	r.customStorageLock.Lock()
	defer r.customStorageLock.Unlock()

	storageMap := r.customStorage.Load().(crdStorageMap)
	// 先复制：storageMap 在无锁处被读取，
	storageMap2 := make(crdStorageMap)
	for _, crd := range allCustomResourceDefinitions {
		if _, ok := storageMap[crd.UID]; ok {
			storageMap2[crd.UID] = storageMap[crd.UID]
		}
	}
	r.customStorage.Store(storageMap2)

	for uid, crdInfo := range storageMap {
		if _, ok := storageMap2[uid]; !ok {
			klog.V(4).Infof("Removing dead CRD storage for %s/%s", crdInfo.spec.Group, crdInfo.spec.Names.Kind)
			go r.tearDown(crdInfo)
		}
	}
}

// 最多等待一分钟让请求排空，再销毁存储。
func (r *crdHandler) tearDown(oldInfo *crdInfo) {
	requestsDrained := make(chan struct{})
	go func() {
		defer close(requestsDrained)
		// 留一点时间让持有旧信息的在途请求完成登记。
		time.Sleep(time.Second)
		// 等待在途请求排空。
		oldInfo.waitGroup.Wait()
	}()

	select {
	case <-time.After(r.requestTimeout * 2):
		klog.Warningf("timeout waiting for requests to drain for %s/%s, tearing down storage", oldInfo.spec.Group, oldInfo.spec.Names.Kind)
	case <-requestsDrained:
	}

	for _, storage := range oldInfo.storages {
		// 只销毁主存储；子资源共享 cacher 与 etcd 客户端。
		storage.CustomResource.DestroyFunc()
	}
}

// Destroy 关闭所有已注册 CRD 的存储层。
// 应在关闭流程的最后一步调用。
func (r *crdHandler) destroy() {
	r.customStorageLock.Lock()
	defer r.customStorageLock.Unlock()

	storageMap := r.customStorage.Load().(crdStorageMap)
	for _, crdInfo := range storageMap {
		for _, storage := range crdInfo.storages {
			// DestroyFunc 必须幂等，
			// 这样才能安全承受与异步调用的 r.tearDown() 之间的竞争。
			// 与异步 tearDown 的竞争。
			storage.CustomResource.DestroyFunc()
		}
	}
}

// GetCustomResourceListerCollectionDeleter 返回给定 CRD 的 ListerCollectionDeleter。
func (r *crdHandler) GetCustomResourceListerCollectionDeleter(crd *apiextensionsv1.CustomResourceDefinition) (finalizer.ListerCollectionDeleter, error) {
	info, err := r.getOrCreateServingInfoFor(crd.UID, crd.Name)
	if err != nil {
		return nil, err
	}
	return info.storages[info.storageVersion].CustomResource, nil
}

// getOrCreateServingInfoFor 先按 UID 查缓存中的 serving info；
// 未命中则按名字取最新 CRD 并新建 serving info。
func (r *crdHandler) getOrCreateServingInfoFor(uid types.UID, name string) (*crdInfo, error) {
	storageMap := r.customStorage.Load().(crdStorageMap)
	if ret, ok := storageMap[uid]; ok {
		return ret, nil
	}

	r.customStorageLock.Lock()
	defer r.customStorageLock.Unlock()

	// 持锁期间读取最新 CRD，避免与 updateCustomResourceDefinition 竞争。
	// 若对方稍后处理更新，存储会被删除，
	// 届时按需重建即可；若它先处理完，
	// 这里保证读到的是同一份最新 CRD。
	crd, err := r.crdLister.Get(name)
	if err != nil {
		return nil, err
	}
	storageMap = r.customStorage.Load().(crdStorageMap)
	if ret, ok := storageMap[crd.UID]; ok {
		return ret, nil
	}

	storageVersion, err := apiextensionshelpers.GetCRDStorageVersion(crd)
	if err != nil {
		return nil, err
	}

	// 按版本组织的 request scope 与存储。
	requestScopes := map[string]*handlers.RequestScope{}
	storages := map[string]customresource.CustomResourceStorage{}
	statusScopes := map[string]*handlers.RequestScope{}
	scaleScopes := map[string]*handlers.RequestScope{}
	deprecated := map[string]bool{}
	warnings := map[string][]string{}

	equivalentResourceRegistry := runtime.NewEquivalentResourceRegistry()

	structuralSchemas := map[string]*structuralschema.Structural{}
	for _, v := range crd.Spec.Versions {
		val, err := apiextensionshelpers.GetSchemaForVersion(crd, v.Name)
		if err != nil {
			utilruntime.HandleError(err)
			return nil, fmt.Errorf("the server could not properly serve the CR schema")
		}
		if val == nil {
			continue
		}
		internalValidation := &apiextensionsinternal.CustomResourceValidation{}
		if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(val, internalValidation, nil); err != nil {
			return nil, fmt.Errorf("failed converting CRD validation to internal version: %v", err)
		}
		s, err := structuralschema.NewStructural(internalValidation.OpenAPIV3Schema)
		if !crd.Spec.PreserveUnknownFields && err != nil {
			// 理论上不会发生；一旦发生就是程序错误。
			utilruntime.HandleError(fmt.Errorf("failed to convert schema to structural: %v", err))
			return nil, fmt.Errorf("the server could not properly serve the CR schema") // validation should avoid this
		}

		if !crd.Spec.PreserveUnknownFields {
			// s 并非完全归我们所有（例如 defaults 未深拷贝），先复制更稳妥。
			s = s.DeepCopy()

			if err := structuraldefaulting.PruneDefaults(s); err != nil {
				// 理论上不会发生；一旦发生就是程序错误。
				utilruntime.HandleError(fmt.Errorf("failed to prune defaults: %v", err))
				return nil, fmt.Errorf("the server could not properly serve the CR schema") // validation should avoid this
			}
		}
		structuralSchemas[v.Name] = s
	}

	openAPIModels, err := buildOpenAPIModelsForApply(r.staticOpenAPISpec, crd)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error building openapi models for %s: %v", crd.Name, err))
		openAPIModels = nil
	}

	var typeConverter managedfields.TypeConverter = managedfields.NewDeducedTypeConverter()
	if len(openAPIModels) > 0 {
		typeConverter, err = managedfields.NewTypeConverter(openAPIModels, crd.Spec.PreserveUnknownFields)
		if err != nil {
			return nil, err
		}
	}

	safeConverter, unsafeConverter, err := r.converterFactory.NewConverter(crd)
	if err != nil {
		return nil, err
	}

	// 构造 replicasPathInCustomResource。
	replicasPathInCustomResource := managedfields.ResourcePathMappings{}
	for _, v := range crd.Spec.Versions {
		subresources, err := apiextensionshelpers.GetSubresourcesForVersion(crd, v.Name)
		if err != nil {
			utilruntime.HandleError(err)
			return nil, fmt.Errorf("the server could not properly serve the CR subresources")
		}
		if subresources == nil || subresources.Scale == nil {
			replicasPathInCustomResource[schema.GroupVersion{Group: crd.Spec.Group, Version: v.Name}.String()] = nil
			continue
		}
		path := fieldpath.Path{}
		splitReplicasPath := strings.Split(strings.TrimPrefix(subresources.Scale.SpecReplicasPath, "."), ".")
		for _, element := range splitReplicasPath {
			s := element
			path = append(path, fieldpath.PathElement{FieldName: &s})
		}
		replicasPathInCustomResource[schema.GroupVersion{Group: crd.Spec.Group, Version: v.Name}.String()] = path
	}

	// 确保 acceptedNames 合法。
	if len(crd.Status.AcceptedNames.Plural) == 0 {
		utilruntime.HandleError(fmt.Errorf("CustomResourceDefinition %s has unexpected empty status.acceptedNames.plural", crd.Name))
		return nil, fmt.Errorf("the server could not properly serve the resource")
	}
	if len(crd.Status.AcceptedNames.Singular) == 0 {
		utilruntime.HandleError(fmt.Errorf("CustomResourceDefinition %s has unexpected empty status.acceptedNames.singular", crd.Name))
		return nil, fmt.Errorf("the server could not properly serve the resource")
	}
	if len(crd.Status.AcceptedNames.Kind) == 0 {
		utilruntime.HandleError(fmt.Errorf("CustomResourceDefinition %s has unexpected empty status.acceptedNames.kind", crd.Name))
		return nil, fmt.Errorf("the server could not properly serve the kind")
	}
	if len(crd.Status.AcceptedNames.ListKind) == 0 {
		utilruntime.HandleError(fmt.Errorf("CustomResourceDefinition %s has unexpected empty status.acceptedNames.listKind", crd.Name))
		return nil, fmt.Errorf("the server could not properly serve the list kind")
	}

	var (
		versionToResource         = map[string]schema.GroupVersionResource{}
		versionToSingularResource = map[string]schema.GroupVersionResource{}
		versionToKind             = map[string]schema.GroupVersionKind{}
		versionToSubresources     = map[string]*apiextensionsv1.CustomResourceSubresources{}
	)
	for _, v := range crd.Spec.Versions {
		versionToResource[v.Name] = schema.GroupVersionResource{Group: crd.Spec.Group, Version: v.Name, Resource: crd.Status.AcceptedNames.Plural}
		versionToSingularResource[v.Name] = schema.GroupVersionResource{Group: crd.Spec.Group, Version: v.Name, Resource: crd.Status.AcceptedNames.Singular}
		versionToKind[v.Name] = schema.GroupVersionKind{Group: crd.Spec.Group, Version: v.Name, Kind: crd.Status.AcceptedNames.Kind}
		versionToSubresources[v.Name], err = apiextensionshelpers.GetSubresourcesForVersion(crd, v.Name)
		if err != nil {
			utilruntime.HandleError(err)
			return nil, fmt.Errorf("the server could not properly serve the CR subresources")
		}

		// 注册等价 kind。
		equivalentResourceRegistry.RegisterKindFor(versionToResource[v.Name], "", versionToKind[v.Name])
		if versionToSubresources[v.Name] != nil {
			if versionToSubresources[v.Name].Status != nil {
				equivalentResourceRegistry.RegisterKindFor(versionToResource[v.Name], "status", versionToKind[v.Name])
			}
			if versionToSubresources[v.Name].Scale != nil {
				equivalentResourceRegistry.RegisterKindFor(versionToResource[v.Name], "scale", autoscalingv1.SchemeGroupVersion.WithKind("Scale"))
			}
		}
	}

	for _, v := range crd.Spec.Versions {
		// 既不 served 也不 stored 的版本不构造存储。
		if !v.Storage && !v.Served {
			continue
		}

		// 除 Unstructured 对象（自定义资源）外，有时还需要
		// 有时还需解码无版本的 Options 对象，这类类型交给 parameterScheme。
		parameterScheme := runtime.NewScheme()
		parameterScheme.AddUnversionedTypes(schema.GroupVersion{Group: crd.Spec.Group, Version: v.Name},
			&metav1.ListOptions{},
			&metav1.GetOptions{},
			&metav1.DeleteOptions{},
		)
		parameterCodec := runtime.NewParameterCodec(parameterScheme)

		singularResource := versionToSingularResource[v.Name]
		resource := versionToResource[v.Name]
		kind := versionToKind[v.Name]

		typer := newUnstructuredObjectTyper(parameterScheme)
		creator := unstructuredCreator{}

		validationSchema, err := apiextensionshelpers.GetSchemaForVersion(crd, v.Name)
		if err != nil {
			utilruntime.HandleError(err)
			return nil, fmt.Errorf("the server could not properly serve the CR schema")
		}
		var internalSchemaProps *apiextensionsinternal.JSONSchemaProps
		var internalValidationSchema *apiextensionsinternal.CustomResourceValidation
		if validationSchema != nil {
			internalValidationSchema = &apiextensionsinternal.CustomResourceValidation{}
			if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(validationSchema, internalValidationSchema, nil); err != nil {
				return nil, fmt.Errorf("failed to convert CRD validation to internal version: %v", err)
			}
			internalSchemaProps = internalValidationSchema.OpenAPIV3Schema
		}
		validator, _, err := apiservervalidation.NewSchemaValidator(internalSchemaProps)
		if err != nil {
			return nil, err
		}

		var statusSpec *apiextensionsinternal.CustomResourceSubresourceStatus
		var statusValidator apiservervalidation.SchemaValidator
		subresources := versionToSubresources[v.Name]

		if subresources != nil && subresources.Status != nil {
			statusSpec = &apiextensionsinternal.CustomResourceSubresourceStatus{}
			if err := apiextensionsv1.Convert_v1_CustomResourceSubresourceStatus_To_apiextensions_CustomResourceSubresourceStatus(subresources.Status, statusSpec, nil); err != nil {
				return nil, fmt.Errorf("failed converting CRD status subresource to internal version: %v", err)
			}
			// status 子资源只按 status schema 校验。
			if internalValidationSchema != nil && internalValidationSchema.OpenAPIV3Schema != nil && internalValidationSchema.OpenAPIV3Schema.Properties != nil {
				if statusSchema, ok := internalValidationSchema.OpenAPIV3Schema.Properties["status"]; ok {
					statusValidator, _, err = apiservervalidation.NewSchemaValidator(&statusSchema)
					if err != nil {
						return nil, err
					}
				}
			}
		}

		var scaleSpec *apiextensionsinternal.CustomResourceSubresourceScale
		if subresources != nil && subresources.Scale != nil {
			scaleSpec = &apiextensionsinternal.CustomResourceSubresourceScale{}
			if err := apiextensionsv1.Convert_v1_CustomResourceSubresourceScale_To_apiextensions_CustomResourceSubresourceScale(subresources.Scale, scaleSpec, nil); err != nil {
				return nil, fmt.Errorf("failed converting CRD status subresource to internal version: %v", err)
			}
		}

		columns, err := getColumnsForVersion(crd, v.Name)
		if err != nil {
			utilruntime.HandleError(err)
			return nil, fmt.Errorf("the server could not properly serve the CR columns")
		}
		table, err := tableconvertor.New(columns)
		if err != nil {
			klog.V(2).Infof("The CRD for %v has an invalid printer specification, falling back to default printing: %v", kind, err)
		}

		listKind := schema.GroupVersionKind{Group: crd.Spec.Group, Version: v.Name, Kind: crd.Status.AcceptedNames.ListKind}

		storages[v.Name], err = customresource.NewStorage(
			resource.GroupResource(),
			singularResource.GroupResource(),
			kind,
			listKind,
			customresource.NewStrategy(
				typer,
				crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
				kind,
				validator,
				statusValidator,
				structuralSchemas[v.Name],
				statusSpec,
				scaleSpec,
				v.SelectableFields,
			),
			crdConversionRESTOptionsGetter{
				RESTOptionsGetter:     r.restOptionsGetter,
				converter:             safeConverter,
				decoderVersion:        schema.GroupVersion{Group: crd.Spec.Group, Version: v.Name},
				encoderVersion:        schema.GroupVersion{Group: crd.Spec.Group, Version: storageVersion},
				structuralSchemas:     structuralSchemas,
				structuralSchemaGK:    kind.GroupKind(),
				preserveUnknownFields: crd.Spec.PreserveUnknownFields,
			},
			crd.Status.AcceptedNames.Categories,
			table,
			replicasPathInCustomResource,
		)
		if err != nil {
			return nil, err
		}

		clusterScoped := crd.Spec.Scope == apiextensionsv1.ClusterScoped

		// CRD 明确不支持 protobuf，但 API Server 返回的某些对象支持，
		negotiatedSerializer := unstructuredNegotiatedSerializer{
			typer:                 typer,
			creator:               creator,
			converter:             safeConverter,
			structuralSchemas:     structuralSchemas,
			structuralSchemaGK:    kind.GroupKind(),
			preserveUnknownFields: crd.Spec.PreserveUnknownFields,
			supportedMediaTypes: []runtime.SerializerInfo{
				{
					MediaType:        "application/json",
					MediaTypeType:    "application",
					MediaTypeSubType: "json",
					EncodesAsText:    true,
					Serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, creator, typer, json.SerializerOptions{StreamingCollectionsEncoding: true}),
					PrettySerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, creator, typer, json.SerializerOptions{Pretty: true}),
					StrictSerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, creator, typer, json.SerializerOptions{
						Strict:                       true,
						StreamingCollectionsEncoding: true,
					}),
					StreamSerializer: &runtime.StreamSerializerInfo{
						EncodesAsText: true,
						Serializer:    json.NewSerializerWithOptions(json.DefaultMetaFactory, creator, typer, json.SerializerOptions{}),
						Framer:        json.Framer,
					},
				},
				{
					MediaType:        "application/yaml",
					MediaTypeType:    "application",
					MediaTypeSubType: "yaml",
					EncodesAsText:    true,
					Serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, creator, typer, json.SerializerOptions{Yaml: true}),
					StrictSerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, creator, typer, json.SerializerOptions{
						Yaml:   true,
						Strict: true,
					}),
				},
				{
					MediaType:        "application/vnd.kubernetes.protobuf",
					MediaTypeType:    "application",
					MediaTypeSubType: "vnd.kubernetes.protobuf",
					Serializer: protobuf.NewSerializerWithOptions(creator, typer, protobuf.SerializerOptions{
						StreamingCollectionsEncoding: true,
					}),
					StreamSerializer: &runtime.StreamSerializerInfo{
						Serializer: protobuf.NewRawSerializer(creator, typer),
						Framer:     protobuf.LengthDelimitedFramer,
					},
				},
			},
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
			negotiatedSerializer.supportedMediaTypes = append(negotiatedSerializer.supportedMediaTypes, cbor.NewSerializerInfo(creator, typer))
		}

		var standardSerializers []runtime.SerializerInfo
		for _, s := range negotiatedSerializer.SupportedMediaTypes() {
			if s.MediaType == runtime.ContentTypeProtobuf {
				continue
			}
			standardSerializers = append(standardSerializers, s)
		}

		reqScope := handlers.RequestScope{
			Namer: handlers.ContextBasedNaming{
				Namer:         meta.NewAccessor(),
				ClusterScoped: clusterScoped,
			},
			Serializer:          negotiatedSerializer,
			ParameterCodec:      parameterCodec,
			StandardSerializers: standardSerializers,

			Creater:         creator,
			Convertor:       safeConverter,
			Defaulter:       unstructuredDefaulter{parameterScheme, structuralSchemas, kind.GroupKind()},
			Typer:           typer,
			UnsafeConvertor: unsafeConverter,

			EquivalentResourceMapper: equivalentResourceRegistry,

			Resource: schema.GroupVersionResource{Group: crd.Spec.Group, Version: v.Name, Resource: crd.Status.AcceptedNames.Plural},
			Kind:     kind,

			// 因此针对特定 group-version 的 handler 以该版本作为内存表示。
			HubGroupVersion: kind.GroupVersion(),

			MetaGroupVersion: metav1.SchemeGroupVersion,

			TableConvertor: storages[v.Name].CustomResource,

			Authorizer: r.authorizer,

			MaxRequestBodyBytes: r.maxRequestBodyBytes,
		}

		resetFields := storages[v.Name].CustomResource.GetResetFields()
		reqScope, err = scopeWithFieldManager(
			typeConverter,
			reqScope,
			resetFields,
			"",
		)
		if err != nil {
			return nil, err
		}
		requestScopes[v.Name] = &reqScope

		scaleColumns, err := getScaleColumnsForVersion(crd, v.Name)
		if err != nil {
			return nil, fmt.Errorf("the server could not properly serve the CR scale subresource columns %w", err)
		}
		scaleTable, _ := tableconvertor.New(scaleColumns)

		// 覆盖 scale 子资源的值，
		// 这里做浅拷贝。
		scaleScope := *requestScopes[v.Name]
		scaleConverter := scale.NewScaleConverter()
		scaleScope.Subresource = "scale"
		opts := []serializer.CodecFactoryOptionsMutator{
			serializer.WithStreamingCollectionEncodingToJSON(),
			serializer.WithStreamingCollectionEncodingToProtobuf(),
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
			opts = append(opts, serializer.WithSerializer(cbor.NewSerializerInfo))
		}
		scaleScope.Serializer = serializer.NewCodecFactory(scaleConverter.Scheme(), opts...)
		scaleScope.Kind = autoscalingv1.SchemeGroupVersion.WithKind("Scale")
		scaleScope.Namer = handlers.ContextBasedNaming{
			Namer:         meta.NewAccessor(),
			ClusterScoped: clusterScoped,
		}
		scaleScope.TableConvertor = scaleTable

		if subresources != nil && subresources.Scale != nil {
			scaleScope, err = scopeWithFieldManager(
				typeConverter,
				scaleScope,
				nil,
				"scale",
			)
			if err != nil {
				return nil, err
			}
		}

		scaleScopes[v.Name] = &scaleScope

		if v.Deprecated {
			deprecated[v.Name] = true
			if v.DeprecationWarning != nil {
				warnings[v.Name] = append(warnings[v.Name], *v.DeprecationWarning)
			} else {
				warnings[v.Name] = append(warnings[v.Name], defaultDeprecationWarning(v.Name, crd.Spec))
			}
		}
	}

	statusResetFields := getStatusResetFields(crd, storages)
	statusScopes, err = setStatusScope(crd, requestScopes, typeConverter, statusResetFields, statusScopes)
	if err != nil {
		return nil, err
	}

	ret := &crdInfo{
		spec:                &crd.Spec,
		acceptedNames:       &crd.Status.AcceptedNames,
		storages:            storages,
		requestScopes:       requestScopes,
		scaleRequestScopes:  scaleScopes,
		statusRequestScopes: statusScopes,
		deprecated:          deprecated,
		warnings:            warnings,
		storageVersion:      storageVersion,
		waitGroup:           &utilwaitgroup.SafeWaitGroup{},
	}

	// 先复制：storageMap 在无锁处被读取，
	// 直接写入会形成数据竞争。
	storageMap2 := storageMap.clone()

	storageMap2[crd.UID] = ret
	r.customStorage.Store(storageMap2)

	return ret, nil
}

// getStatusResetFields 返回 CRD status 子资源在所有版本上的重置字段。
func getStatusResetFields(crd *apiextensionsv1.CustomResourceDefinition, storages map[string]customresource.CustomResourceStorage) map[fieldpath.APIVersion]*fieldpath.Set {
	var statusResetFields = make(map[fieldpath.APIVersion]*fieldpath.Set)
	for _, value := range crd.Spec.Versions {
		if storages[value.Name].Status == nil {
			continue
		}
		resetField := storages[value.Name].Status.GetResetFields()
		for apiVersion, set := range resetField {
			statusResetFields[apiVersion] = set
		}
	}
	return statusResetFields
}

// setStatusScope 为 CRD 的每个版本设置 status scope。
func setStatusScope(crd *apiextensionsv1.CustomResourceDefinition, requestScopes map[string]*handlers.RequestScope, typeConverter managedfields.TypeConverter, resetFields map[fieldpath.APIVersion]*fieldpath.Set, statusScopes map[string]*handlers.RequestScope) (map[string]*handlers.RequestScope, error) {
	for _, v := range crd.Spec.Versions {
		clusterScoped := crd.Spec.Scope == apiextensionsv1.ClusterScoped
		if requestScopes[v.Name] == nil {
			continue
		}
		statusScope := *requestScopes[v.Name]
		statusScope.Subresource = "status"
		statusScope.Namer = handlers.ContextBasedNaming{
			Namer:         meta.NewAccessor(),
			ClusterScoped: clusterScoped,
		}

		subresources, err := apiextensionshelpers.GetSubresourcesForVersion(crd, v.Name)
		if err != nil {
			utilruntime.HandleError(err)
			return nil, fmt.Errorf("the server could not properly serve the CR subresources")
		}
		if subresources != nil && subresources.Status != nil {
			statusScope, err = scopeWithFieldManager(
				typeConverter,
				statusScope,
				resetFields,
				"status",
			)
			if err != nil {
				return nil, err
			}
		}
		statusScopes[v.Name] = &statusScope
	}
	return statusScopes, nil

}

func scopeWithFieldManager(typeConverter managedfields.TypeConverter, reqScope handlers.RequestScope, resetFields map[fieldpath.APIVersion]*fieldpath.Set, subresource string) (handlers.RequestScope, error) {
	fieldManager, err := managedfields.NewDefaultCRDFieldManager(
		typeConverter,
		reqScope.Convertor,
		reqScope.Defaulter,
		reqScope.Creater,
		reqScope.Kind,
		reqScope.HubGroupVersion,
		subresource,
		fieldpath.NewExcludeFilterSetMap(resetFields),
	)
	if err != nil {
		return handlers.RequestScope{}, err
	}
	reqScope.FieldManager = fieldManager
	return reqScope, nil
}

func defaultDeprecationWarning(deprecatedVersion string, crd apiextensionsv1.CustomResourceDefinitionSpec) string {
	msg := fmt.Sprintf("%s/%s %s is deprecated", crd.Group, deprecatedVersion, crd.Names.Kind)

	var servedNonDeprecatedVersions []string
	for _, v := range crd.Versions {
		if v.Served && !v.Deprecated && version.CompareKubeAwareVersionStrings(deprecatedVersion, v.Name) < 0 {
			servedNonDeprecatedVersions = append(servedNonDeprecatedVersions, v.Name)
		}
	}
	if len(servedNonDeprecatedVersions) == 0 {
		return msg
	}
	sort.Slice(servedNonDeprecatedVersions, func(i, j int) bool {
		return version.CompareKubeAwareVersionStrings(servedNonDeprecatedVersions[i], servedNonDeprecatedVersions[j]) > 0
	})
	msg += fmt.Sprintf("; use %s/%s %s", crd.Group, servedNonDeprecatedVersions[0], crd.Names.Kind)
	return msg
}

type unstructuredNegotiatedSerializer struct {
	typer     runtime.ObjectTyper
	creator   runtime.ObjectCreater
	converter runtime.ObjectConvertor

	structuralSchemas     map[string]*structuralschema.Structural // by version
	structuralSchemaGK    schema.GroupKind
	preserveUnknownFields bool

	supportedMediaTypes []runtime.SerializerInfo
}

func (s unstructuredNegotiatedSerializer) SupportedMediaTypes() []runtime.SerializerInfo {
	return s.supportedMediaTypes
}

func (s unstructuredNegotiatedSerializer) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return versioning.NewCodec(encoder, nil, s.converter, Scheme, Scheme, Scheme, gv, nil, "crdNegotiatedSerializer")
}

func (s unstructuredNegotiatedSerializer) DecoderToVersion(decoder runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	returnUnknownFieldPaths := false
	if serializer, ok := decoder.(*json.Serializer); ok {
		returnUnknownFieldPaths = serializer.IsStrict()
	}
	d := schemaCoercingDecoder{delegate: decoder, validator: unstructuredSchemaCoercer{structuralSchemas: s.structuralSchemas, structuralSchemaGK: s.structuralSchemaGK, preserveUnknownFields: s.preserveUnknownFields, returnUnknownFieldPaths: returnUnknownFieldPaths}}
	return versioning.NewCodec(nil, d, runtime.UnsafeObjectConvertor(Scheme), Scheme, Scheme, unstructuredDefaulter{
		delegate:           Scheme,
		structuralSchemas:  s.structuralSchemas,
		structuralSchemaGK: s.structuralSchemaGK,
	}, nil, gv, "unstructuredNegotiatedSerializer")
}

type UnstructuredObjectTyper struct {
	Delegate          runtime.ObjectTyper
	UnstructuredTyper runtime.ObjectTyper
}

func newUnstructuredObjectTyper(Delegate runtime.ObjectTyper) UnstructuredObjectTyper {
	return UnstructuredObjectTyper{
		Delegate:          Delegate,
		UnstructuredTyper: crdserverscheme.NewUnstructuredObjectTyper(),
	}
}

func (t UnstructuredObjectTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	// 非 Unstructured 对象交给 delegate。
	if _, ok := obj.(runtime.Unstructured); !ok {
		return t.Delegate.ObjectKinds(obj)
	}
	return t.UnstructuredTyper.ObjectKinds(obj)
}

func (t UnstructuredObjectTyper) Recognizes(gvk schema.GroupVersionKind) bool {
	return t.Delegate.Recognizes(gvk) || t.UnstructuredTyper.Recognizes(gvk)
}

type unstructuredCreator struct{}

func (c unstructuredCreator) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	ret := &unstructured.Unstructured{}
	ret.SetGroupVersionKind(kind)
	return ret, nil
}

type unstructuredDefaulter struct {
	delegate           runtime.ObjectDefaulter
	structuralSchemas  map[string]*structuralschema.Structural // by version
	structuralSchemaGK schema.GroupKind
}

func (d unstructuredDefaulter) Default(in runtime.Object) {
	// 非 Unstructured 对象及其它 GK 交给 delegate。
	u, ok := in.(runtime.Unstructured)
	if !ok || u.GetObjectKind().GroupVersionKind().GroupKind() != d.structuralSchemaGK {
		d.delegate.Default(in)
		return
	}

	structuraldefaulting.Default(u.UnstructuredContent(), d.structuralSchemas[u.GetObjectKind().GroupVersionKind().Version])
}

// clone 复制给定的 crdStorageMap。
// 这里只做 map 的浅拷贝。
func (in crdStorageMap) clone() crdStorageMap {
	if in == nil {
		return nil
	}
	out := make(crdStorageMap, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// crdConversionRESTOptionsGetter 用指定的自定义转换器与编解码版本
// 构造新 codec，覆盖默认 codec。
type crdConversionRESTOptionsGetter struct {
	generic.RESTOptionsGetter
	converter             runtime.ObjectConvertor
	encoderVersion        schema.GroupVersion
	decoderVersion        schema.GroupVersion
	structuralSchemas     map[string]*structuralschema.Structural // by version
	structuralSchemaGK    schema.GroupKind
	preserveUnknownFields bool
}

func (t crdConversionRESTOptionsGetter) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	// 显式忽略 example：下面会覆盖 storageconfig。
	ret, err := t.RESTOptionsGetter.GetRESTOptions(resource, nil)
	if err == nil {
		d := schemaCoercingDecoder{delegate: ret.StorageConfig.Codec, validator: unstructuredSchemaCoercer{
			// 解码旧 CR 时丢弃非法字段（当时还没有 ObjectMeta 校验）。
			dropInvalidMetadata:   true,
			repairGeneration:      true,
			structuralSchemas:     t.structuralSchemas,
			structuralSchemaGK:    t.structuralSchemaGK,
			preserveUnknownFields: t.preserveUnknownFields,
		}}
		c := schemaCoercingConverter{delegate: t.converter, validator: unstructuredSchemaCoercer{
			structuralSchemas:     t.structuralSchemas,
			structuralSchemaGK:    t.structuralSchemaGK,
			preserveUnknownFields: t.preserveUnknownFields,
		}}
		ret.StorageConfig.Codec = versioning.NewCodec(
			ret.StorageConfig.Codec,
			d,
			c,
			&unstructuredCreator{},
			crdserverscheme.NewUnstructuredObjectTyper(),
			&unstructuredDefaulter{
				delegate:           Scheme,
				structuralSchemaGK: t.structuralSchemaGK,
				structuralSchemas:  t.structuralSchemas,
			},
			t.encoderVersion,
			t.decoderVersion,
			"crdRESTOptions",
		)
	}
	return ret, err
}

// schemaCoercingDecoder 先调用 delegate 解码器，再用 Unstructured schema 校验器
// 对结果做强转。
type schemaCoercingDecoder struct {
	delegate  runtime.Decoder
	validator unstructuredSchemaCoercer
}

var _ runtime.Decoder = schemaCoercingDecoder{}

func (d schemaCoercingDecoder) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	var decodingStrictErrs []error
	obj, gvk, err := d.delegate.Decode(data, defaults, into)
	if err != nil {
		decodeStrictErr, ok := runtime.AsStrictDecodingError(err)
		if !ok || obj == nil {
			return nil, gvk, err
		}
		decodingStrictErrs = decodeStrictErr.Errors()
	}
	var unknownFields []string
	if u, ok := obj.(*unstructured.Unstructured); ok {
		unknownFields, err = d.validator.apply(u)
		if err != nil {
			return nil, gvk, err
		}
	}
	if d.validator.returnUnknownFieldPaths && (len(decodingStrictErrs) > 0 || len(unknownFields) > 0) {
		for _, unknownField := range unknownFields {
			decodingStrictErrs = append(decodingStrictErrs, fmt.Errorf(`unknown field "%s"`, unknownField))
		}
		return obj, gvk, runtime.NewStrictDecodingError(decodingStrictErrs)
	}

	return obj, gvk, nil
}

// schemaCoercingConverter 先调用 delegate 转换器，再用 Unstructured 校验器强转 schema。
type schemaCoercingConverter struct {
	delegate  runtime.ObjectConvertor
	validator unstructuredSchemaCoercer
}

var _ runtime.ObjectConvertor = schemaCoercingConverter{}

func (v schemaCoercingConverter) Convert(in, out, context interface{}) error {
	if err := v.delegate.Convert(in, out, context); err != nil {
		return err
	}

	if u, ok := out.(*unstructured.Unstructured); ok {
		if _, err := v.validator.apply(u); err != nil {
			return err
		}
	}

	return nil
}

func (v schemaCoercingConverter) ConvertToVersion(in runtime.Object, gv runtime.GroupVersioner) (runtime.Object, error) {
	out, err := v.delegate.ConvertToVersion(in, gv)
	if err != nil {
		return nil, err
	}

	if u, ok := out.(*unstructured.Unstructured); ok {
		if _, err := v.validator.apply(u); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (v schemaCoercingConverter) ConvertFieldLabel(gvk schema.GroupVersionKind, label, value string) (string, string, error) {
	return v.delegate.ConvertFieldLabel(gvk, label, value)
}

// unstructuredSchemaCoercer 为 unstructured 反序列化补上 json.Unmarshal 对原生类型
// 解码到 Go 结构体时额外的处理：
//
// - 校验并裁剪 ObjectMeta
// - 按 structural schema 通用裁剪未知字段
// - 移除未设默认值且不可为 null 的 map 中的 null 值。
type unstructuredSchemaCoercer struct {
	dropInvalidMetadata bool
	repairGeneration    bool

	structuralSchemas       map[string]*structuralschema.Structural
	structuralSchemaGK      schema.GroupKind
	preserveUnknownFields   bool
	returnUnknownFieldPaths bool
}

func (v *unstructuredSchemaCoercer) apply(u *unstructured.Unstructured) (unknownFieldPaths []string, err error) {
	// 先保存那些无需写进校验 schema 的隐式元字段。
	kind, foundKind, err := unstructured.NestedString(u.UnstructuredContent(), "kind")
	if err != nil {
		return nil, err
	}
	apiVersion, foundApiVersion, err := unstructured.NestedString(u.UnstructuredContent(), "apiVersion")
	if err != nil {
		return nil, err
	}
	objectMeta, foundObjectMeta, metaUnknownFields, err := schemaobjectmeta.GetObjectMetaWithOptions(u.Object, schemaobjectmeta.ObjectMetaOptions{
		DropMalformedFields:     v.dropInvalidMetadata,
		ReturnUnknownFieldPaths: v.returnUnknownFieldPaths,
	})
	if err != nil {
		return nil, err
	}
	unknownFieldPaths = append(unknownFieldPaths, metaUnknownFields...)

	// 同时比较 group 与 kind：DeleteCollection options 等对象也会经过这里。
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, err
	}

	if gv.Group == v.structuralSchemaGK.Group && kind == v.structuralSchemaGK.Kind {
		if !v.preserveUnknownFields {
			// TODO: 根部的裁剪与强转也改用 schemaobjectmeta.Coerce。
			pruneOpts := structuralschema.UnknownFieldPathOptions{}
			if v.returnUnknownFieldPaths {
				pruneOpts.TrackUnknownFieldPaths = true
			}
			unknownFieldPaths = append(unknownFieldPaths, structuralpruning.PruneWithOptions(u.Object, v.structuralSchemas[gv.Version], true, pruneOpts)...)
			structuraldefaulting.PruneNonNullableNullsWithoutDefaults(u.Object, v.structuralSchemas[gv.Version])
		}

		err, paths := schemaobjectmeta.CoerceWithOptions(nil, u.Object, v.structuralSchemas[gv.Version], false, schemaobjectmeta.CoerceOptions{
			DropInvalidFields:       v.dropInvalidMetadata,
			ReturnUnknownFieldPaths: v.returnUnknownFieldPaths,
		})
		if err != nil {
			return nil, err
		}
		unknownFieldPaths = append(unknownFieldPaths, paths...)

		// 修补极旧 CR 中缺失的 generation。
		if v.repairGeneration && objectMeta.Generation == 0 {
			objectMeta.Generation = 1
		}
	}

	// 从干净状态开始恢复元字段。
	if foundKind {
		u.SetKind(kind)
	}
	if foundApiVersion {
		u.SetAPIVersion(apiVersion)
	}
	if foundObjectMeta {
		if err := schemaobjectmeta.SetObjectMeta(u.Object, objectMeta); err != nil {
			return nil, err
		}
	}

	return unknownFieldPaths, nil
}

// hasServedCRDVersion 判断给定版本是否在 CRD 版本列表中且 Served 为 true。
func hasServedCRDVersion(spec *apiextensionsv1.CustomResourceDefinitionSpec, version string) bool {
	for _, v := range spec.Versions {
		if v.Name == version {
			return v.Served
		}
	}
	return false
}

// buildOpenAPIModelsForApply 从自定义资源指定的校验 schema 构造 OpenAPI 模型，
// 并与静态 OpenAPI spec 中的模型合并。
// 静态 spec 为 nil 或出错时返回 nil 模型。
func buildOpenAPIModelsForApply(staticOpenAPISpec map[string]*spec.Schema, crd *apiextensionsv1.CustomResourceDefinition) (map[string]*spec.Schema, error) {
	if staticOpenAPISpec == nil {
		return nil, nil
	}

	// 先把静态 spec 转成 V3 格式，才能合并。
	staticSpecV3 := &spec3.OpenAPI{
		Version: "3.0.0",
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title:   "Kubernetes CRD Swagger",
				Version: "v0.1.0",
			},
		},
		Components: &spec3.Components{
			Schemas: staticOpenAPISpec,
		},
	}

	specs := []*spec3.OpenAPI{staticSpecV3}
	for _, v := range crd.Spec.Versions {
		// 默认值不在这里裁剪，而是在对外服务前处理。
		// 相关开关见 builder.go 中的 flag 说明。
		s, err := builder.BuildOpenAPIV3(crd, v.Name, builder.Options{})
		if err != nil {
			return nil, err
		}
		specs = append(specs, s)
	}

	mergedOpenAPI, err := builder.MergeSpecsV3(specs...)
	if err != nil {
		return nil, err
	}
	return mergedOpenAPI.Components.Schemas, nil
}

// forbidCreateAdmission 是 admission.Interface 的包装器，
// 在 CRD 处于 Terminating 时拒绝创建自定义资源。
type forbidCreateAdmission struct {
	delegate admission.Interface
}

func (f *forbidCreateAdmission) Handles(operation admission.Operation) bool {
	if operation == admission.Create {
		return true
	}
	return f.delegate.Handles(operation)
}

func (f *forbidCreateAdmission) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if a.GetOperation() == admission.Create {
		return apierrors.NewForbidden(a.GetResource().GroupResource(), a.GetName(), errors.New("create not allowed while custom resource definition is terminating"))
	}
	if delegate, ok := f.delegate.(admission.MutationInterface); ok {
		return delegate.Admit(ctx, a, o)
	}
	return nil
}

func (f *forbidCreateAdmission) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if a.GetOperation() == admission.Create {
		return apierrors.NewForbidden(a.GetResource().GroupResource(), a.GetName(), errors.New("create not allowed while custom resource definition is terminating"))
	}
	if delegate, ok := f.delegate.(admission.ValidationInterface); ok {
		return delegate.Validate(ctx, a, o)
	}
	return nil
}
