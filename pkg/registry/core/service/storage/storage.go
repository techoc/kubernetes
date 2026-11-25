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

package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/apiserver/pkg/util/proxy"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	svcreg "k8s.io/kubernetes/pkg/registry/core/service"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
	netutil "k8s.io/utils/net"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

type PodStorage interface {
	rest.Getter
}

type REST struct {
	*genericregistry.Store
	primaryIPFamily   api.IPFamily
	secondaryIPFamily api.IPFamily
	alloc             Allocators
	endpointSlices    proxy.EndpointSliceGetter
	pods              PodStorage
	proxyTransport    http.RoundTripper
}

var (
	_ rest.CategoriesProvider     = &REST{}
	_ rest.ShortNamesProvider     = &REST{}
	_ rest.StorageVersionProvider = &REST{}
	_ rest.ResetFieldsStrategy    = &REST{}
	_ rest.Redirector             = &REST{}
)

// NewREST 创建用于访问 Service 资源的 RESTStorage。
// NewREST 构建 Service 通用 Store，并安装 Decorator、BeginCreate、BeginUpdate、AfterDelete 钩子，把 API 持久化与网络资源分配连接起来。
func NewREST(
	optsGetter generic.RESTOptionsGetter,
	serviceIPFamily api.IPFamily,
	ipAllocs map[api.IPFamily]ipallocator.Interface,
	portAlloc portallocator.Interface,
	endpointSliceGetter proxy.EndpointSliceGetter,
	pods PodStorage,
	proxyTransport http.RoundTripper) (*REST, *StatusREST, *svcreg.ProxyREST, error) {

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.Service{} },
		NewListFunc:               func() runtime.Object { return &api.ServiceList{} },
		PredicateFunc:             svcreg.Matcher,
		DefaultQualifiedResource:  api.Resource("services"),
		SingularQualifiedResource: api.Resource("service"),
		ReturnDeletedObject:       true,

		CreateStrategy:      svcreg.Strategy,
		UpdateStrategy:      svcreg.Strategy,
		DeleteStrategy:      svcreg.Strategy,
		ResetFieldsStrategy: svcreg.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{
		RESTOptions: optsGetter,
		AttrFunc:    svcreg.GetAttrs,
	}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = svcreg.StatusStrategy
	statusStore.ResetFieldsStrategy = svcreg.StatusStrategy

	var primaryIPFamily api.IPFamily = serviceIPFamily
	var secondaryIPFamily api.IPFamily = "" // 哨兵值
	if len(ipAllocs) > 1 {
		secondaryIPFamily = otherFamily(serviceIPFamily)
	}
	genericStore := &REST{
		Store:             store,
		primaryIPFamily:   primaryIPFamily,
		secondaryIPFamily: secondaryIPFamily,
		alloc:             makeAlloc(serviceIPFamily, ipAllocs, portAlloc),
		endpointSlices:    endpointSliceGetter,
		pods:              pods,
		proxyTransport:    proxyTransport,
	}
	store.Decorator = genericStore.defaultOnRead
	store.AfterDelete = genericStore.afterDelete
	store.BeginCreate = genericStore.beginCreate
	store.BeginUpdate = genericStore.beginUpdate

	// 用户可以通过 Patch 修改 Status 并移除 Finalizer，因此 statusStore 也必须注册 AfterDelete 钩子，确保释放已分配的网络资源。
	statusStore.AfterDelete = genericStore.afterDelete

	return genericStore, &StatusREST{store: &statusStore}, &svcreg.ProxyREST{Redirector: genericStore, ProxyTransport: proxyTransport}, nil
}

// otherFamily 返回与输入 IPFamily 相反的地址族；调用方必须保证输入值有效。
func otherFamily(fam api.IPFamily) api.IPFamily {
	if fam == api.IPv4Protocol {
		return api.IPv6Protocol
	}
	return api.IPv4Protocol
}

var (
	_ rest.ShortNamesProvider = &REST{}
	_ rest.CategoriesProvider = &REST{}
)

// ShortNames 实现 ShortNamesProvider，返回 Service 资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"svc"}
}

// Categories 实现 CategoriesProvider，返回 Service 所属的资源类别。
func (r *REST) Categories() []string {
	return []string{"all"}
}

// Destroy 在 API Server 关闭时清理 Service Store 和 IP/端口分配器。
func (r *REST) Destroy() {
	r.Store.Destroy()
	r.alloc.Destroy()
}

// StatusREST 实现用于修改 Service.Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

func (r *StatusREST) New() runtime.Object {
	return &api.Service{}
}

// Destroy 在 API Server 关闭时清理该 REST 子资源拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与 Service 主资源 REST 共享，因此不能在此处重复销毁。
}

// Get 从底层存储读取 Service；状态子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 Service.Status，不允许通过该入口修改 Spec。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 显式将 forceAllowCreate 设为 false，因为子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Status 子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

// 本文件有许多函数同时接收更新前后的 Service。代码约定通常按 (new, old) 传参，但两者类型相同时容易传反。
// Before/After 包装类型用于显式标明参数角色：顺序有意义时使用 after/before，并在函数内部还原 oldSvc/newSvc；
// 顺序无关时则直接接收 lhs/rhs，避免误导调用方。

type Before struct {
	*api.Service
}
type After struct {
	*api.Service
}

// defaultOnRead 在读取时补齐历史对象中可能缺失的关联字段。
// 不能放在常规默认值处理阶段，因为常规默认值逻辑同时作用于 Get、Create 和 Update，而这里必须只处理读取路径。
// 此方法既可能接收 Service，也可能接收 ServiceList。
// defaultOnRead 仅在读取时补齐历史 Service 的关联字段，不把这类兼容默认值写回 etcd。
func (r *REST) defaultOnRead(obj runtime.Object) {
	switch s := obj.(type) {
	case *api.Service:
		r.defaultOnReadService(s)
	case *api.ServiceList:
		r.defaultOnReadServiceList(s)
	default:
		// 当前对象类型不支持此处的默认值补全；这不是错误，因为缓存层也可能将其他对象传递到这里。
	}
}

// defaultOnReadServiceList 为 ServiceList 中的每个 Service 补齐读取时的兼容字段。
func (r *REST) defaultOnReadServiceList(serviceList *api.ServiceList) {
	if serviceList == nil {
		return
	}

	for i := range serviceList.Items {
		r.defaultOnReadService(&serviceList.Items[i])
	}
}

// defaultOnReadService 为单个 Service 补齐读取时的兼容字段。
func (r *REST) defaultOnReadService(service *api.Service) {
	if service == nil {
		return
	}

	// 历史对象可能创建于 ClusterIP 字段扩展为 ClusterIPs 之前；读取时需要统一呈现新旧字段。
	normalizeClusterIPs(After{service}, Before{nil})

	// 必要时补齐 IPFamilies 和 IPFamilyPolicy。
	r.defaultOnReadIPFamilies(service)

	// 历史上曾错误地为 ExternalName 类型默认设置 InternalTrafficPolicy。虽然无法直接修改已持久化的数据，
	// 但可以在读取时清除该字段，避免向客户端暴露不适用的值。
	defaultOnReadInternalTrafficPolicy(service)
}

func defaultOnReadInternalTrafficPolicy(service *api.Service) {
	if service.Spec.Type == api.ServiceTypeExternalName {
		service.Spec.InternalTrafficPolicy = nil
	}
}

// defaultOnReadIPFamilies 为旧对象推导 IPFamilies/IPFamilyPolicy；Headless + selectorless 与普通 Headless Service 使用不同默认规则。
func (r *REST) defaultOnReadIPFamilies(service *api.Service) {
	// ExternalName 类型不需要 IP 地址族相关默认值。
	if !needsClusterIP(service) {
		return
	}

	// 如果 IPFamilies 已设置，则假定 IPFamilyPolicy 也已设置（正常情况下不应只设置其中一个），无需继续补默认值。
	// 同理，如果 IPFamilies 未设置，也假定 IPFamilyPolicy 不会单独设置。
	if len(service.Spec.IPFamilies) > 0 {
		return
	}

	singleStack := api.IPFamilyPolicySingleStack
	requireDualStack := api.IPFamilyPolicyRequireDualStack

	if service.Spec.ClusterIP == api.ClusterIPNone {
		// Headless Service。
		if len(service.Spec.Selector) == 0 {
			// 同时满足 Headless 和无 Selector 的 Service 属于特殊场景。
			//
			// 此时无法确定用户为无 Selector 的 Service 配置了哪种 Endpoint，尤其无法获知其 IP 地址族。
			// 因此假定其使用双栈，并在单栈或双栈集群中都默认采用 RequireDualStack。
			service.Spec.IPFamilyPolicy = &requireDualStack
			service.Spec.IPFamilies = []api.IPFamily{r.primaryIPFamily, otherFamily(r.primaryIPFamily)}
		} else {
			// Headless 且存在 Selector 时，默认使用单地址族。
			service.Spec.IPFamilyPolicy = &singleStack
			service.Spec.IPFamilies = []api.IPFamily{r.primaryIPFamily}
		}
	} else {
		// 非 Headless Service：根据 ClusterIPs 初始化 IPFamilies。
		service.Spec.IPFamilies = make([]api.IPFamily, len(service.Spec.ClusterIPs))
		for idx, ip := range service.Spec.ClusterIPs {
			if netutil.IsIPv6String(ip) {
				service.Spec.IPFamilies[idx] = api.IPv6Protocol
			} else {
				service.Spec.IPFamilies[idx] = api.IPv4Protocol
			}
		}
		if len(service.Spec.IPFamilies) == 1 {
			service.Spec.IPFamilyPolicy = &singleStack
		} else if len(service.Spec.IPFamilies) == 2 {
			// 正常情况下不应进入此分支，这里保留防御性处理。
			service.Spec.IPFamilyPolicy = &requireDualStack
		}
	}
}

// afterDelete 在非 DryRun 删除成功后释放该 Service 占用的 ClusterIP、NodePort 和健康检查端口。
func (r *REST) afterDelete(obj runtime.Object, options *metav1.DeleteOptions) {
	svc := obj.(*api.Service)

	// 通常这些默认值会自动完成，但 Decorator 在读取流程末尾才执行，因此这里需要确保拿到的是完整对象。
	r.defaultOnReadService(svc)

	// 只有非 dry-run 删除成功后才真正释放已分配的资源。
	if !dryrun.IsDryRun(options.DryRun) {
		r.alloc.releaseAllocatedResources(svc)
	}
}

// beginCreate 在写入 Service 前规范化 ClusterIP 字段并预留 IP/端口；Finish 根据存储成功与否提交或回滚副作用。
func (r *REST) beginCreate(ctx context.Context, obj runtime.Object, options *metav1.CreateOptions) (genericregistry.FinishFunc, error) {
	svc := obj.(*api.Service)

	// 尽早同步 ClusterIP 和 ClusterIPs，确保后续逻辑读取到一致的地址字段。
	normalizeClusterIPs(After{svc}, Before{nil})

	// 分配 IP 和端口。如果底层存储支持事务，这些操作本可纳入同一个事务；当前不支持，因此需要手动管理。
	// 分配必须在此钩子执行，而不能提前放在默认值阶段，因为这里才能读取 Feature Gate/启动配置并访问 API 存储。
	txn, err := r.alloc.allocateCreate(svc, dryrun.IsDryRun(options.DryRun))
	if err != nil {
		return nil, err
	}

	// 注册清理回调：后续持久化失败时回滚已预留的资源。
	finish := func(_ context.Context, success bool) {
		if success {
			txn.Commit()
		} else {
			txn.Revert()
		}
	}

	return finish, nil
}

// beginUpdate 先给旧对象补齐历史默认值，再把客户端省略的分配字段补回并规范化，最后为本次差异建立资源分配事务。
func (r *REST) beginUpdate(ctx context.Context, obj, oldObj runtime.Object, options *metav1.UpdateOptions) (genericregistry.FinishFunc, error) {
	newSvc := obj.(*api.Service)
	oldSvc := oldObj.(*api.Service)

	// 确保旧对象已经补齐当前版本预期存在的默认字段。
	// 如果对象早于这些字段引入时创建，则可能缺少它们；更新路径不会对 old 对象调用 Decorator。
	r.defaultOnReadService(oldSvc)

	// 补回客户端可能省略的系统分配值，使重复提交相同配置具备幂等性。
	patchAllocatedValues(After{newSvc}, Before{oldSvc})

	// 确保 ClusterIP 与 ClusterIPs 保持同步，这一步必须尽早完成，
	// 在任何代码读取它们之前就做好。
	normalizeClusterIPs(After{newSvc}, Before{oldSvc})

	// 根据新旧 Service 的差异分配或初始化网络字段。
	txn, err := r.alloc.allocateUpdate(After{newSvc}, Before{oldSvc}, dryrun.IsDryRun(options.DryRun))
	if err != nil {
		return nil, err
	}

	// 清理回调
	finish := func(_ context.Context, success bool) {
		if success {
			txn.Commit()
		} else {
			txn.Revert()
		}
	}

	return finish, nil
}

// ResourceLocation 返回指定 Service 的代理目标 URL 和 HTTP Transport。
// ResourceLocation 将 Service 名称/端口解析为 EndpointSlice 后端地址，并只选择能验证为 Pod IP 的 Endpoint。
func (r *REST) ResourceLocation(ctx context.Context, id string) (*url.URL, http.RoundTripper, error) {
	// 支持 svcname、svcname:port 或 scheme:svcname:port 三种请求标识格式。
	svcScheme, svcName, portStr, valid := utilnet.SplitSchemeNamePort(id)
	if !valid {
		return nil, nil, errors.NewBadRequest(fmt.Sprintf("invalid service request %q", id))
	}

	// 如果请求指定的是端口号，则查找对应的 Service Port 名称。
	if portNum, err := strconv.ParseInt(portStr, 10, 64); err == nil {
		obj, err := r.Get(ctx, svcName, &metav1.GetOptions{})
		if err != nil {
			return nil, nil, err
		}
		svc := obj.(*api.Service)
		found := false
		for _, svcPort := range svc.Spec.Ports {
			if int64(svcPort.Port) == portNum {
				// 使用 Service 中声明的端口名称。
				portStr = svcPort.Name
				found = true
				break
			}
		}
		if !found {
			return nil, nil, errors.NewServiceUnavailable(fmt.Sprintf("no service port %d found for service %q", portNum, svcName))
		}
	}

	if r.endpointSlices == nil {
		return nil, nil, errors.NewServiceUnavailable(fmt.Sprintf("no endpoints available for service %q", svcName))
	}
	namespace := genericapirequest.NamespaceValue(ctx)
	if len(namespace) == 0 {
		return nil, nil, errors.NewBadRequest("missing required namespace")
	}
	slices, err := r.endpointSlices.GetEndpointSlices(namespace, svcName)
	if err != nil {
		return nil, nil, errors.NewInternalError(err)
	}
	if len(slices) == 0 {
		return nil, nil, errors.NewServiceUnavailable(fmt.Sprintf("no endpoints available for service %q", svcName))
	}

	hostPort, err := proxy.RandomEndpoint(svcName, slices, portStr, v1.IPFamily(r.primaryIPFamily), func(ep *discovery.Endpoint) bool {
		// 只允许代理到确实对应 Pod 的 Endpoint 地址。
		if err := isValidAddress(ctx, ep, r.pods); err != nil {
			utilruntime.HandleError(fmt.Errorf("endpoint %v isn't valid (%w)", ep, err))
			return false
		}
		return true
	})
	if err != nil {
		return nil, nil, err
	}
	return &url.URL{Scheme: svcScheme, Host: hostPort}, r.proxyTransport, nil
}

// isValidAddress 校验 Endpoint 有 Pod TargetRef、namespace 与请求一致，且 endpoint 地址确实属于目标 Pod。
func isValidAddress(ctx context.Context, ep *discovery.Endpoint, pods rest.Getter) error {
	if ep.TargetRef == nil {
		return fmt.Errorf("endpoint has no target ref, skipping: %v", ep)
	}
	if genericapirequest.NamespaceValue(ctx) != ep.TargetRef.Namespace {
		return fmt.Errorf("endpoint namespace doesn't match context namespace")
	}
	obj, err := pods.Get(ctx, ep.TargetRef.Name, &metav1.GetOptions{})
	if err != nil {
		return err
	}
	pod, ok := obj.(*api.Pod)
	if !ok {
		return fmt.Errorf("failed to cast to pod: %v", obj)
	}
	if pod == nil {
		return fmt.Errorf("pod is missing, skipping (%s/%s)", ep.TargetRef.Namespace, ep.TargetRef.Name)
	}
	for _, podIP := range pod.Status.PodIPs {
		// Addresses 虽然是数组，但这里只使用第一个地址。
		if podIP.IP == ep.Addresses[0] {
			return nil
		}
	}
	return fmt.Errorf("pod ip(s) doesn't match endpoint ip, skipping: %v vs %s (%s/%s)", pod.Status.PodIPs, ep.Addresses[0], ep.TargetRef.Namespace, ep.TargetRef.Name)
}

// normalizeClusterIPs 仅根据 ClusterIP 调整 ClusterIPs，不应依赖或修改其他字段。
// normalizeClusterIPs 在校验前保持旧 ClusterIP 与新 ClusterIPs 字段一致，兼容只填写单数旧字段的客户端。
func normalizeClusterIPs(after After, before Before) {
	oldSvc, newSvc := before.Service, after.Service

	// 此处无需对输入做过度校验，后续很快会对新对象执行完整 Validation。
	// 当前逻辑只需根据关联字段推断用户意图；下面采用较为展开的写法以便理解。

	// **** 重要规则 *****
	// 客户端必须选择以下一种写法：旧客户端只使用单数 ClusterIP；新客户端同时使用单数和复数 ClusterIPs。

	if oldSvc == nil {
		// 当前是创建操作。客户端只提供单数 ClusterIP（例如旧客户端），因此由服务端补齐复数 ClusterIPs。
		if len(newSvc.Spec.ClusterIP) > 0 && len(newSvc.Spec.ClusterIPs) == 0 {
			newSvc.Spec.ClusterIPs = []string{newSvc.Spec.ClusterIP}
			return
		}

		// 不根据复数 ClusterIPs 反向补单数 ClusterIP，因为新客户端必须同时提交两个字段。

		// 两个字段要么都未指定（后续由分配器分配），要么都已指定（后续由校验器验证）。
		return
	}

	// 当前是更新操作。

	// 旧客户端尝试 Patch 其他字段时没有提交 ClusterIPs，导致该字段被清空。
	if len(oldSvc.Spec.ClusterIPs) > 0 && len(newSvc.Spec.ClusterIPs) == 0 {
		// 如果 ClusterIP 未变化，说明旧客户端很可能只是更新其他字段时省略了 ClusterIPs。
		if oldSvc.Spec.ClusterIP == newSvc.Spec.ClusterIP {
			newSvc.Spec.ClusterIPs = oldSvc.Spec.ClusterIPs
		}
	}

	// ClusterIP 发生了变化。
	if oldSvc.Spec.ClusterIP != newSvc.Spec.ClusterIP {
		// 客户端正在尝试清空 ClusterIP。
		if len(oldSvc.Spec.ClusterIP) > 0 && len(newSvc.Spec.ClusterIP) == 0 {
			// 如果 ClusterIPs 也未变化，则由服务端代替客户端同步清空。
			if sameClusterIPs(oldSvc, newSvc) {
				newSvc.Spec.ClusterIPs = nil
			}

			// 如果客户端提交 nil，则前面的 Patch 兼容分支已经处理；如果提交了其他变化，则交由校验器判断。
		} else {
			// ClusterIP 已修改但未清空，且 ClusterIPs 未变化时，根据新的 ClusterIP 同步设置 ClusterIPs。
			if sameClusterIPs(oldSvc, newSvc) {
				newSvc.Spec.ClusterIPs = []string{newSvc.Spec.ClusterIP}
			}
		}
	}
}

// patchAllocatedValues 在客户端省略系统分配字段时补回旧值，使客户端无需先 GET 再 PUT，也能幂等地重复提交配置。
// 例如，创建 Service 时未指定 ClusterIP，服务端自动分配了地址；客户端再次提交同一份 YAML 时仍应成功。
// patchAllocatedValues 为 PUT/PATCH 中被客户端省略的系统分配字段补回旧值，避免无关更新意外释放或重新申请地址/端口。
func patchAllocatedValues(after After, before Before) {
	oldSvc, newSvc := before.Service, after.Service

	if needsClusterIP(oldSvc) && needsClusterIP(newSvc) {
		if newSvc.Spec.ClusterIP == "" {
			newSvc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
		}
		if len(newSvc.Spec.ClusterIPs) == 0 && len(oldSvc.Spec.ClusterIPs) > 0 {
			newSvc.Spec.ClusterIPs = oldSvc.Spec.ClusterIPs
		}
	}

	if needsNodePort(oldSvc) && needsNodePort(newSvc) {
		nodePortsUsed := func(svc *api.Service) sets.Int32 {
			used := sets.NewInt32()
			for _, p := range svc.Spec.Ports {
				if p.NodePort != 0 {
					used.Insert(p.NodePort)
				}
			}
			return used
		}

		// 收集 oldSvc 和 newSvc 中都存在的端口；这些端口属于已有配置，不能在此处自动补丁修改。
		used := nodePortsUsed(oldSvc).Intersection(nodePortsUsed(newSvc))

		// 按端口名称建立 NodePort 映射。用户可能修改了端口的其他属性，但这些变化不在此处处理。
		np := map[string]int32{}
		for i := range oldSvc.Spec.Ports {
			p := &oldSvc.Spec.Ports[i]
			np[p.Name] = p.NodePort
		}

		// 如果 newSvc 缺少这些取值，就在我们已知取值、且它没有被其他端口占用时补回去，
		// 以保证重复提交相同配置是幂等的。

		for i := range newSvc.Spec.Ports {
			p := &newSvc.Spec.Ports[i]
			if p.NodePort == 0 {
				oldVal := np[p.Name]
				if !used.Has(oldVal) {
					p.NodePort = oldVal
				}
			}
		}
	}

	if needsHCNodePort(oldSvc) && needsHCNodePort(newSvc) {
		if newSvc.Spec.HealthCheckNodePort == 0 {
			newSvc.Spec.HealthCheckNodePort = oldSvc.Spec.HealthCheckNodePort
		}
	}
}

func needsClusterIP(svc *api.Service) bool {
	if svc.Spec.Type == api.ServiceTypeExternalName {
		return false
	}
	return true
}

func needsNodePort(svc *api.Service) bool {
	if svc.Spec.Type == api.ServiceTypeNodePort {
		return true
	}
	if svc.Spec.Type == api.ServiceTypeLoadBalancer &&
		(svc.Spec.AllocateLoadBalancerNodePorts == nil || *svc.Spec.AllocateLoadBalancerNodePorts) {
		return true
	}
	return false
}

func needsHCNodePort(svc *api.Service) bool {
	if svc.Spec.Type != api.ServiceTypeLoadBalancer {
		return false
	}
	if svc.Spec.ExternalTrafficPolicy != api.ServiceExternalTrafficPolicyLocal {
		return false
	}
	return true
}
