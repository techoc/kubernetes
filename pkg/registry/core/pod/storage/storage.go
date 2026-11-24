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

package storage

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/url"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	storeerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/util/dryrun"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	policyclient "k8s.io/client-go/kubernetes/typed/policy/v1"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	registrypod "k8s.io/kubernetes/pkg/registry/core/pod"
	podrest "k8s.io/kubernetes/pkg/registry/core/pod/rest"
)

// PodStorage 聚合 Pod 主资源及 binding、eviction、status、resize、logs、exec、attach、proxy 等子资源的 REST 接口。
type PodStorage struct {
	Pod                 *REST
	Binding             *BindingREST
	LegacyBinding       *LegacyBindingREST
	Eviction            *EvictionREST
	Status              *StatusREST
	EphemeralContainers *EphemeralContainersREST
	Resize              *ResizeREST
	Log                 *podrest.LogREST
	Proxy               *podrest.ProxyREST
	Exec                *podrest.ExecREST
	Attach              *podrest.AttachREST
	PortForward         *podrest.PortForwardREST
}

// REST implements a RESTStorage for pods
type REST struct {
	*genericregistry.Store
	proxyTransport http.RoundTripper
}

// NewStorage 创建共享底层存储，并为不同子资源派生各自的 UpdateStrategy，确保每个端点只能修改授权字段。
func NewStorage(optsGetter generic.RESTOptionsGetter, k client.ConnectionInfoGetter, proxyTransport http.RoundTripper, podDisruptionBudgetClient policyclient.PodDisruptionBudgetsGetter, authorizer authorizer.UnconditionalAuthorizer) (PodStorage, error) {

	// genericregistry.Store 负责通用 CRUD；Pod 专属的筛选、策略、资源名和 Table 转换在此注入。
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.Pod{} },
		NewListFunc:               func() runtime.Object { return &api.PodList{} },
		PredicateFunc:             registrypod.MatchPod,
		DefaultQualifiedResource:  api.Resource("pods"),
		SingularQualifiedResource: api.Resource("pod"),

		CreateStrategy:      registrypod.Strategy,
		UpdateStrategy:      registrypod.Strategy,
		DeleteStrategy:      registrypod.Strategy,
		ResetFieldsStrategy: registrypod.Strategy,
		ReturnDeletedObject: true,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	// 将持久化后端、属性提取函数、NodeName 触发索引与索引器交给 Store 完成初始化。
	options := &generic.StoreOptions{
		RESTOptions: optsGetter,
		AttrFunc:    registrypod.GetAttrs,
		TriggerFunc: map[string]storage.IndexerFunc{"spec.nodeName": registrypod.NodeNameTriggerFunc},
		Indexers:    registrypod.Indexers(),
	}
	if err := store.CompleteWithOptions(options); err != nil {
		return PodStorage{}, err
	}

	// 子资源复用同一底层 Storage，但复制 Store 并替换策略，避免 /status 等接口沿用主资源的可写字段集合。
	statusStore := *store
	statusStore.UpdateStrategy = registrypod.StatusStrategy
	statusStore.ResetFieldsStrategy = registrypod.StatusStrategy
	ephemeralContainersStore := *store
	ephemeralContainersStore.UpdateStrategy = registrypod.EphemeralContainersStrategy
	resizeStore := *store
	resizeStore.UpdateStrategy = registrypod.ResizeStrategy

	bindingREST := &BindingREST{store: store}
	// 返回所有 REST 端点：主资源共享基础 Store，status/ephemeralcontainers/resize 使用专属策略副本。
	return PodStorage{
		Pod:                 &REST{store, proxyTransport},
		Binding:             &BindingREST{store: store},
		LegacyBinding:       &LegacyBindingREST{bindingREST},
		Eviction:            newEvictionStorage(&statusStore, podDisruptionBudgetClient),
		Status:              &StatusREST{store: &statusStore},
		EphemeralContainers: &EphemeralContainersREST{store: &ephemeralContainersStore},
		Resize:              &ResizeREST{store: &resizeStore},
		Log:                 &podrest.LogREST{Store: store, KubeletConn: k},
		Proxy:               &podrest.ProxyREST{Store: store, ProxyTransport: proxyTransport},
		Exec:                &podrest.ExecREST{Store: store, KubeletConn: k, Authorizer: authorizer},
		Attach:              &podrest.AttachREST{Store: store, KubeletConn: k, Authorizer: authorizer},
		PortForward:         &podrest.PortForwardREST{Store: store, KubeletConn: k, Authorizer: authorizer},
	}, nil
}

// Implement Redirector.
var _ = rest.Redirector(&REST{})

// ResourceLocation returns a pods location from its HostIP
func (r *REST) ResourceLocation(ctx context.Context, name string) (*url.URL, http.RoundTripper, error) {
	return registrypod.ResourceLocation(ctx, r, r.proxyTransport, name)
}

// Implement ShortNamesProvider
var _ rest.ShortNamesProvider = &REST{}

// ShortNames implements the ShortNamesProvider interface. Returns a list of short names for a resource.
func (r *REST) ShortNames() []string {
	return []string{"po"}
}

// Implement CategoriesProvider
var _ rest.CategoriesProvider = &REST{}

// Categories implements the CategoriesProvider interface. Returns a list of categories a resource is part of.
func (r *REST) Categories() []string {
	return []string{"all"}
}

// BindingREST implements the REST endpoint for binding pods to nodes when etcd is in use.
type BindingREST struct {
	store *genericregistry.Store
}

// NamespaceScoped fulfill rest.Scoper
func (r *BindingREST) NamespaceScoped() bool {
	return r.store.NamespaceScoped()
}

// New creates a new binding resource
func (r *BindingREST) New() runtime.Object {
	return &api.Binding{}
}

// Destroy cleans up resources on shutdown.
func (r *BindingREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

var _ = rest.NamedCreater(&BindingREST{})
var _ = rest.SubresourceObjectMetaPreserver(&BindingREST{})

// Create ensures a pod is bound to a specific host.
// Create 验证 Binding 的类型、URL 名称和 API 字段，再以原子更新方式把 Pod 绑定到目标 Node。
func (r *BindingREST) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (out runtime.Object, err error) {
	binding, ok := obj.(*api.Binding)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("not a Binding object: %#v", obj))
	}

	// URL 中的 Pod 名必须与 Binding 对象 metadata.name 一致，避免对不同对象产生歧义。
	if name != binding.Name {
		return nil, errors.NewBadRequest("name in URL does not match name in Binding object")
	}

	// TODO: move me to a binding strategy
	if errs := validation.ValidatePodBinding(binding); len(errs) != 0 {
		return nil, errs.ToAggregate()
	}

	if createValidation != nil {
		if err := createValidation(ctx, binding.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	// dryRun 只执行校验和对象变换，不实际写入 etcd；正常请求通过 assignPod 更新 Pod。
	err = r.assignPod(ctx, binding.UID, binding.ResourceVersion, binding.Name, binding.Target.Name, binding.Annotations, binding.Labels, dryrun.IsDryRun(options.DryRun))
	out = &metav1.Status{Status: metav1.StatusSuccess}
	return
}

// PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate indicates to a
// handler that this endpoint requires the UID and ResourceVersion to use as
// preconditions. Other fields, such as timestamp, are ignored.
func (r *BindingREST) PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate() bool {
	return true
}

// setPodNodeAndMetadata sets the given pod's nodeName to 'machine' if and only if
// the pod is unassigned, and merges the provided annotations and labels with those of the pod.
// Returns the current state of the pod, or an error.
// setPodNodeAndMetadata 在 GuaranteedUpdate 回调内检查 Pod 前置条件并合并绑定数据，防止并发调度造成重复绑定。
func (r *BindingREST) setPodNodeAndMetadata(ctx context.Context, podUID types.UID, podResourceVersion, podID, machine string, annotations, labels map[string]string, dryRun bool) (finalPod *api.Pod, err error) {
	podKey, err := r.store.KeyFunc(ctx, podID)
	if err != nil {
		return nil, err
	}

	// Binding 可携带 UID/ResourceVersion 前置条件；存储层会在写入时再次核对，防止对象被删除重建或并发修改。
	var preconditions *storage.Preconditions
	if podUID != "" || podResourceVersion != "" {
		preconditions = &storage.Preconditions{}
		if podUID != "" {
			preconditions.UID = &podUID
		}
		if podResourceVersion != "" {
			preconditions.ResourceVersion = &podResourceVersion
		}
	}

	// GuaranteedUpdate 在读取到的最新版本上执行回调并处理资源版本冲突，保证“未绑定 → 已绑定”只成功一次。
	err = r.store.Storage.GuaranteedUpdate(ctx, podKey, &api.Pod{}, false, preconditions, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
		pod, ok := obj.(*api.Pod)
		if !ok {
			return nil, fmt.Errorf("unexpected object: %#v", obj)
		}
		// 正在删除的 Pod 不允许再被调度绑定。
		if pod.DeletionTimestamp != nil {
			return nil, fmt.Errorf("pod %s is being deleted, cannot be assigned to a host", pod.Name)
		}
		// 已有 NodeName 说明 Pod 已经被某个调度路径绑定，拒绝重复分配。
		if pod.Spec.NodeName != "" {
			return nil, fmt.Errorf("pod %v is already assigned to node %q", pod.Name, pod.Spec.NodeName)
		}
		// Reject binding to a scheduling un-ready Pod.
		// 调度门控尚未解除的 Pod 不能被绑定，即使调用方直接访问 binding 子资源也必须阻止。
		if len(pod.Spec.SchedulingGates) != 0 {
			return nil, fmt.Errorf("pod %v has non-empty .spec.schedulingGates", pod.Name)
		}
		// 通过 Binding 确认的目标 Node 写入 Pod.Spec.NodeName，后续 Kubelet watch 会据此接管 Pod。
		pod.Spec.NodeName = machine
		// Clear nomination hint to prevent stale information affecting external components.
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.ClearingNominatedNodeNameAfterBinding) {
			pod.Status.NominatedNodeName = ""
		}
		// Annotation 是合并语义：保留 Pod 原值，再应用 Binding 中显式提供的覆盖值。
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		var oldAnnotations map[string]string
		// 需要合并新 Annotation 时先保存旧值，后续用它校验哪些键被修改。
		if len(annotations) > 0 {
			oldAnnotations = maps.Clone(pod.Annotations)
		}
		for k, v := range annotations {
			pod.Annotations[k] = v
		}
		// Binding writes bypass the pod strategy (and thus DropDisabledPodFields),
		// so scrub the merged annotations here the same way.
		podutil.DropInitContainerAnnotations(pod.Annotations)
		// Copy all labels from the Binding over to the Pod object, overwriting
		// any existing labels set on the Pod.
		// 只有开启拓扑标签准入特性时才把 Binding 的 Labels 覆盖到 Pod，兼容旧版本行为。
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodTopologyLabelsAdmission) {
			copyLabelsWithOverwriting(pod, labels)
		}
		podutil.UpdatePodCondition(&pod.Status, &api.PodCondition{
			Type:   api.PodScheduled,
			Status: api.ConditionTrue,
		})

		// Binding 更新绕过普通 Pod Strategy，因此在合并后单独校验特定 Annotation 和总大小限制。
		if len(annotations) > 0 {
			// validate updated annotations
			fldPath := field.NewPath("metadata", "annotations")
			allErrs := validation.ValidatePodSpecificAnnotationUpdates(pod, oldAnnotations, fldPath, validation.PodValidationOptions{})
			if err := apimachineryvalidation.ValidateAnnotationsSize(pod.Annotations); err != nil {
				allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, apimachineryvalidation.TotalAnnotationSizeLimitB))
			}
			if len(allErrs) > 0 {
				return nil, errors.NewInvalid(schema.GroupKind{Group: "", Kind: "Binding"}, "", allErrs)
			}
		}

		finalPod = pod
		return pod, nil
	}), dryRun, nil)
	return finalPod, err
}

func copyLabelsWithOverwriting(pod *api.Pod, labels map[string]string) {
	if len(labels) == 0 {
		// nothing to do
		return
	}
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	// Iterate over the binding's labels and copy them across to the Pod.
	for k, v := range labels {
		pod.Labels[k] = v
	}
}

// assignPod assigns the given pod to the given machine.
// assignPod 封装绑定更新，并把底层读写错误转换为 Kubernetes API 的 NotFound/Conflict 状态码。
func (r *BindingREST) assignPod(ctx context.Context, podUID types.UID, podResourceVersion, podID string, machine string, annotations, labels map[string]string, dryRun bool) (err error) {
	if _, err = r.setPodNodeAndMetadata(ctx, podUID, podResourceVersion, podID, machine, annotations, labels, dryRun); err != nil {
		err = storeerr.InterpretGetError(err, api.Resource("pods"), podID)
		err = storeerr.InterpretUpdateError(err, api.Resource("pods"), podID)
		if _, ok := err.(*errors.StatusError); !ok {
			err = errors.NewConflict(api.Resource("pods/binding"), podID, err)
		}
	}
	return
}

var _ = rest.Creater(&LegacyBindingREST{})

// LegacyBindingREST implements the REST endpoint for binding pods to nodes when etcd is in use.
type LegacyBindingREST struct {
	bindingRest *BindingREST
}

// NamespaceScoped fulfill rest.Scoper
func (r *LegacyBindingREST) NamespaceScoped() bool {
	return r.bindingRest.NamespaceScoped()
}

// New creates a new binding resource
func (r *LegacyBindingREST) New() runtime.Object {
	return r.bindingRest.New()
}

// Destroy cleans up resources on shutdown.
func (r *LegacyBindingREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Create ensures a pod is bound to a specific host.
func (r *LegacyBindingREST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (out runtime.Object, err error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, errors.NewBadRequest(fmt.Sprintf("not a Binding object: %T", obj))
	}
	return r.bindingRest.Create(ctx, metadata.GetName(), obj, createValidation, options)
}

func (r *LegacyBindingREST) GetSingularName() string {
	return "binding"
}

// StatusREST implements the REST endpoint for changing the status of a pod.
type StatusREST struct {
	store *genericregistry.Store
}

// New creates a new pod resource
func (r *StatusREST) New() runtime.Object {
	return &api.Pod{}
}

// Destroy cleans up resources on shutdown.
func (r *StatusREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Get retrieves the object from the storage. It is required to support Patch.
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update alters the status subset of an object.
// StatusREST.Update 强制禁止通过更新创建对象；子资源只能修改现存 Pod 的 Status。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// We are explicitly setting forceAllowCreate to false in the call to the underlying storage because
	// subresources should never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields implements rest.ResetFieldsStrategy
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

// EphemeralContainersREST implements the REST endpoint for adding EphemeralContainers
type EphemeralContainersREST struct {
	store *genericregistry.Store
}

var _ = rest.Patcher(&EphemeralContainersREST{})

// Get retrieves the object from the storage. It is required to support Patch.
func (r *EphemeralContainersREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// New creates a new pod resource
func (r *EphemeralContainersREST) New() runtime.Object {
	return &api.Pod{}
}

// Destroy cleans up resources on shutdown.
func (r *EphemeralContainersREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Update alters the EphemeralContainers field in PodSpec
// EphemeralContainersREST.Update 复用独立的临时容器策略，并禁止 update 请求退化为 create。
func (r *EphemeralContainersREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// We are explicitly setting forceAllowCreate to false in the call to the underlying storage because
	// subresources should never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// ResizeREST implements the REST endpoint for resizing Pod containers.
type ResizeREST struct {
	store *genericregistry.Store
}

var _ = rest.Patcher(&ResizeREST{})

// Get retrieves the object from the storage. It is required to support Patch.
func (r *ResizeREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// New creates a new pod resource
func (r *ResizeREST) New() runtime.Object {
	return &api.Pod{}
}

// Destroy cleans up resources on shutdown.
func (r *ResizeREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Update alters the resource fields in PodSpec
// ResizeREST.Update 只开放策略允许的资源/扩缩容字段，并禁止通过该子资源创建 Pod。
func (r *ResizeREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// We are explicitly setting forceAllowCreate to false in the call to the underlying storage because
	// subresources should never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}
