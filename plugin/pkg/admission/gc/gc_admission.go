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

package gc

import (
	"context"
	"fmt"
	"io"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// PluginName 是该准入插件的名称。
const PluginName = "OwnerReferencesPermissionEnforcement"

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		// 旧版 kubelet 会破坏 pods/status 数据，因此本插件忽略该端点；
		// pod status 策略已确保状态更新无法修改 ownerRef。
		whiteList := []whiteListItem{
			{
				groupResource: schema.GroupResource{Resource: "pods"},
				subresource:   "status",
			},
		}
		return &gcPermissionsEnforcement{
			Handler:   admission.NewHandler(admission.Create, admission.Update),
			whiteList: whiteList,
		}, nil
	})
}

// gcPermissionsEnforcement 校验 ownerReference 修改权限的准入插件实现。
type gcPermissionsEnforcement struct {
	*admission.Handler

	authorizer authorizer.UnconditionalAuthorizer

	restMapper meta.RESTMapper

	// 白名单中的资源在准入时跳过检查；
	// 其中每一项必须通过策略保证 ownerRef 不会被修改。
	whiteList []whiteListItem
}

var _ admission.ValidationInterface = &gcPermissionsEnforcement{}

// whiteListItem 描述 GC 权限检查白名单中的一项。
type whiteListItem struct {
	groupResource schema.GroupResource
	subresource   string
}

// isWhiteListed 判断指定资源和子资源是否在白名单中。
func (a *gcPermissionsEnforcement) isWhiteListed(groupResource schema.GroupResource, subresource string) bool {
	for _, item := range a.whiteList {
		if item.groupResource == groupResource && item.subresource == subresource {
			return true
		}
	}
	return false
}

// Validate 校验修改 ownerReference 及 blockOwnerDeletion 的用户是否具备相应删除权限。
func (a *gcPermissionsEnforcement) Validate(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) (err error) {
	// 白名单中的请求跳过修改检查。
	if a.isWhiteListed(attributes.GetResource().GroupResource(), attributes.GetSubresource()) {
		return nil
	}

	// 未修改 ownerReference 时无需额外鉴权，直接放行。
	if !isChangingOwnerReference(attributes.GetObject(), attributes.GetOldObject()) {
		return nil
	}

	// 创建时用户可以自由设置 ownerRef：他不创建该对象就不会产生后果。
	// 但删除权限并不等于阻止其他资源被删除的权限，因此下面仍需检查 blockOwnerDeletion。
	if attributes.GetOperation() != admission.Create {
		deleteAttributes := authorizer.AttributesRecord{
			User:            attributes.GetUserInfo(),
			Verb:            "delete",
			Namespace:       attributes.GetNamespace(),
			APIGroup:        attributes.GetResource().Group,
			APIVersion:      attributes.GetResource().Version,
			Resource:        attributes.GetResource().Resource,
			Subresource:     attributes.GetSubresource(),
			Name:            attributes.GetName(),
			ResourceRequest: true,
			Path:            "",
		}
		decision, reason, err := a.authorizer.Authorize(ctx, deleteAttributes)
		if decision != authorizer.DecisionAllow {
			return admission.NewForbidden(attributes, fmt.Errorf("cannot set an ownerRef on a resource you can't delete: %v, %v", reason, err))
		}
	}

	// 若用户将 ownerReference.blockOwnerDeletion 设为 true，
	// 只有当用户对 _OWNER_ 有删除权限时才允许修改。
	newBlockingRefs := newBlockingOwnerDeletionRefs(attributes.GetObject(), attributes.GetOldObject())
	if len(newBlockingRefs) == 0 {
		return nil
	}

	// 网络不可用时 restMapper 访问 discovery 端点可能超时，
	// 导致用于打通网络的 Pod 无法创建，且表现为请求超时而非明确拒绝。
	// 由于超时包裹在 admission/request 外层，这里先做一次通用检查：
	// 若用户可为任意资源设置 finalizer，则无需再逐一做 RESTMapping。
	if decision, _, _ := a.authorizer.Authorize(ctx, finalizeAnythingRecord(attributes.GetUserInfo())); decision == authorizer.DecisionAllow {
		return nil
	}

	for _, ref := range newBlockingRefs {
		records, err := a.ownerRefToDeleteAttributeRecords(ref, attributes)
		if err != nil {
			return admission.NewForbidden(attributes, fmt.Errorf("cannot set blockOwnerDeletion in this case because cannot find RESTMapping for APIVersion %s Kind %s: %v", ref.APIVersion, ref.Kind, err))
		}
		// ref.Kind 可能映射到多个资源，因此会返回多条记录；
		// 用户必须对所有匹配的资源都有相应权限。
		for _, record := range records {
			decision, reason, err := a.authorizer.Authorize(ctx, record)
			if decision != authorizer.DecisionAllow {
				return admission.NewForbidden(attributes, fmt.Errorf("cannot set blockOwnerDeletion if an ownerReference refers to a resource you can't set finalizers on: %v, %v", reason, err))
			}
		}
	}

	return nil

}

// isChangingOwnerReference 判断新旧对象的 ownerReference 是否发生变化。
func isChangingOwnerReference(newObj, oldObj runtime.Object) bool {
	newMeta, err := meta.Accessor(newObj)
	if err != nil {
		// 拿不到 ObjectMeta 也就无从判断 ownerReference，按未变化处理。
		return false
	}

	if oldObj == nil {
		return len(newMeta.GetOwnerReferences()) > 0
	}
	oldMeta, err := meta.Accessor(oldObj)
	if err != nil {
		// 拿不到 ObjectMeta 也就无从判断 ownerReference，按未变化处理。
		return false
	}

	// 比较新旧 ownerReference：不同则说明试图修改。
	oldOwners := oldMeta.GetOwnerReferences()
	newOwners := newMeta.GetOwnerReferences()
	if len(oldOwners) != len(newOwners) {
		return true
	}
	for i := range oldOwners {
		if !apiequality.Semantic.DeepEqual(oldOwners[i], newOwners[i]) {
			return true
		}
	}

	return false
}

// finalizeAnythingRecord 构造“可更新任意资源 finalizers”的鉴权请求，
// 用于短路检查用户是否拥有通用 finalizer 权限。
func finalizeAnythingRecord(userInfo user.Info) authorizer.AttributesRecord {
	return authorizer.AttributesRecord{
		User:            userInfo,
		Verb:            "update",
		APIGroup:        "*",
		APIVersion:      "*",
		Resource:        "*",
		Subresource:     "finalizers",
		Name:            "*",
		ResourceRequest: true,
		Path:            "",
	}
}

// ownerRefToDeleteAttributeRecords 将 ref 转换为针对其所指对象的鉴权记录。
// OwnerReference 只记录对象 Kind，可能映射到多个资源，
// 因此可能返回多条记录，用户需对所有记录都有权限。
func (a *gcPermissionsEnforcement) ownerRefToDeleteAttributeRecords(ref metav1.OwnerReference, attributes admission.Attributes) ([]authorizer.AttributesRecord, error) {
	var ret []authorizer.AttributesRecord
	groupVersion, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return ret, err
	}
	mappings, err := a.restMapper.RESTMappings(schema.GroupKind{Group: groupVersion.Group, Kind: ref.Kind}, groupVersion.Version)
	if err != nil {
		return ret, err
	}
	for _, mapping := range mappings {
		ar := authorizer.AttributesRecord{
			User:            attributes.GetUserInfo(),
			Verb:            "update",
			APIGroup:        mapping.Resource.Group,
			APIVersion:      mapping.Resource.Version,
			Resource:        mapping.Resource.Resource,
			Subresource:     "finalizers",
			Name:            ref.Name,
			ResourceRequest: true,
			Path:            "",
		}
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			// 命名空间级 owner 必须与依赖对象位于同一命名空间。
			ar.Namespace = attributes.GetNamespace()
		}
		ret = append(ret, ar)
	}
	return ret, nil
}

// blockingOwnerRefs 只保留 blockOwnerDeletion 为 true 的 ownerReference。
func blockingOwnerRefs(refs []metav1.OwnerReference) []metav1.OwnerReference {
	var ret []metav1.OwnerReference
	for _, ref := range refs {
		if ref.BlockOwnerDeletion != nil && *ref.BlockOwnerDeletion {
			ret = append(ret, ref)
		}
	}
	return ret
}

// indexByUID 按 UID 建立 ownerReference 索引，便于与旧对象逐一比对。
func indexByUID(refs []metav1.OwnerReference) map[types.UID]metav1.OwnerReference {
	ret := make(map[types.UID]metav1.OwnerReference)
	for _, ref := range refs {
		ret[ref.UID] = ref
	}
	return ret
}

// newBlockingOwnerDeletionRefs 返回新增的阻塞型 ownerReference，
// 以及 blockOwnerDeletion 从 nil/false 变为 true 的引用。
func newBlockingOwnerDeletionRefs(newObj, oldObj runtime.Object) []metav1.OwnerReference {
	newMeta, err := meta.Accessor(newObj)
	if err != nil {
		// 拿不到 ObjectMeta 也就无从判断 ownerReference。
		return nil
	}
	newRefs := newMeta.GetOwnerReferences()
	blockingNewRefs := blockingOwnerRefs(newRefs)
	if len(blockingNewRefs) == 0 {
		return nil
	}

	if oldObj == nil {
		return blockingNewRefs
	}
	oldMeta, err := meta.Accessor(oldObj)
	if err != nil {
		// 拿不到旧对象的 ObjectMeta，只能把所有阻塞型引用都当作新增的。
		return blockingNewRefs
	}

	var ret []metav1.OwnerReference
	indexedOldRefs := indexByUID(oldMeta.GetOwnerReferences())
	for _, ref := range blockingNewRefs {
		oldRef, ok := indexedOldRefs[ref.UID]
		if !ok {
			// 新增且阻塞删除的引用需要鉴权。
			ret = append(ret, ref)
			continue
		}
		wasNotBlocking := oldRef.BlockOwnerDeletion == nil || *oldRef.BlockOwnerDeletion == false
		if wasNotBlocking {
			ret = append(ret, ref)
		}
	}
	return ret
}

// SetUnconditionalAuthorizer 注入鉴权器。
func (a *gcPermissionsEnforcement) SetUnconditionalAuthorizer(authorizer authorizer.UnconditionalAuthorizer) {
	a.authorizer = authorizer
}

// SetRESTMapper 注入 RESTMapper，用于解析 ownerReference 到具体资源。
func (a *gcPermissionsEnforcement) SetRESTMapper(restMapper meta.RESTMapper) {
	a.restMapper = restMapper
}

// ValidateInitialization 确保鉴权器和 RESTMapper 均已注入。
func (a *gcPermissionsEnforcement) ValidateInitialization() error {
	if a.authorizer == nil {
		return fmt.Errorf("missing authorizer")
	}
	if a.restMapper == nil {
		return fmt.Errorf("missing restMapper")
	}
	return nil
}
