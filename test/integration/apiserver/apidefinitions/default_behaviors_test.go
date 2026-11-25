/*
Copyright The Kubernetes Authors.

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

package apidefinitions

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

// TestAllowCreateOnUpdate 验证 API 是否倾向于设置 AllowCreateOnUpdate() = false。
//
// 理由：POST 专用于创建，PUT 严格用于更新已有资源，以确保生命周期安全且可预测。
// 允许 PUT 创建对象可能导致意外的对象创建。
func TestAllowCreateOnUpdate(t *testing.T) {
	exempt := sets.New(
		// Lease 仅通过 PUT 请求维护。
		"leasecandidates.v1alpha2.coordination.k8s.io",
		"leasecandidates.v1beta1.coordination.k8s.io",
		"leases.v1.coordination.k8s.io",

		// 沿用旧例、无明确理由的 API：
		"clusterrolebindings.v1.rbac.authorization.k8s.io",
		"clusterroles.v1.rbac.authorization.k8s.io",
		"endpoints.v1",
		"events.v1.events.k8s.io",
		"events.v1",
		"limitranges.v1",
		"rolebindings.v1.rbac.authorization.k8s.io",
		"roles.v1.rbac.authorization.k8s.io",
		"runtimeclasses.v1.node.k8s.io",
		"services.v1",
	)
	TestAllDefinitions(t, "allow-create-on-update", func(t *testing.T, api Definition) {
		if !api.HasVerb("create") || !api.HasVerb("get") || !api.HasVerb("update") || !api.HasVerb("delete") {
			t.Skip("Resource does not support create, get, update, and delete")
		}
		client := api.ResourceClient()
		obj := TestObj(t, api.StorageData.Stub, "{}", api.Mapping.GroupVersionKind)
		obj.SetResourceVersion("")
		obj.SetUID("")
		_, err := client.Update(context.TODO(), obj, metav1.UpdateOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("Unexpected error from Update: %v", err)
		}
		assertDefault(t, api.Mapping.Resource, "AllowCreateOnUpdate must be false", apierrors.IsNotFound(err), exempt)
	})
}

// TestGenerateName 验证 API 是否支持 metadata.generateName。
//
// 理由：除非该 API 的新资源名称必须从特定字段值派生，否则 API 应提供 generateName。
func TestGenerateName(t *testing.T) {
	exempt := sets.New(
		// 因命名有特殊要求而不支持 generateName 的 API：
		"apiservices.v1.apiregistration.k8s.io",
		"clustertrustbundles.v1alpha1.certificates.k8s.io",
		"clustertrustbundles.v1beta1.certificates.k8s.io",
		"clustertrustbundles.v1.certificates.k8s.io",
		"customresourcedefinitions.v1.apiextensions.k8s.io",
		"ipaddresses.v1.networking.k8s.io",
		"storageversions.v1alpha1.internal.apiserver.k8s.io",
	)
	TestAllDefinitions(t, "generate-name", func(t *testing.T, api Definition) {
		if !api.HasVerb("create") || !api.HasVerb("get") || !api.HasVerb("update") || !api.HasVerb("delete") {
			t.Skip("Resource does not support create, get, update, and delete")
		}
		client := api.ResourceClient()
		obj := TestObj(t, api.StorageData.Stub, "{}", api.Mapping.GroupVersionKind)
		obj.SetName("")
		obj.SetGenerateName("default-behaviors-")
		obj.SetResourceVersion("")
		obj.SetUID("")
		created, err := client.Create(context.TODO(), obj, metav1.CreateOptions{})
		if err != nil && !apierrors.IsInvalid(err) {
			t.Fatalf("Unexpected error from Create: %v", err)
		}
		generated := err == nil && strings.HasPrefix(created.GetName(), "default-behaviors-")
		assertDefault(t, api.Mapping.Resource, "metadata.generateName must produce a server-generated name", generated, exempt)
	})
}

// TestAllowUnconditionalUpdate 验证 API 是否倾向于设置 AllowUnconditionalUpdate() = false。
//
// 理由：无条件更新可能导致意外覆盖。API 应鼓励用户使用条件更新或 patch 操作。
func TestAllowUnconditionalUpdate(t *testing.T) {
	exempt := sets.New(
		// 沿用旧例的 API：
		"certificatesigningrequests.v1.certificates.k8s.io",
		"clusterrolebindings.v1.rbac.authorization.k8s.io",
		"clusterroles.v1.rbac.authorization.k8s.io",
		"clustertrustbundles.v1alpha1.certificates.k8s.io",
		"configmaps.v1",
		"controllerrevisions.v1.apps",
		"cronjobs.v1.batch",
		"daemonsets.v1.apps",
		"deployments.v1.apps",
		"deviceclasses.v1.resource.k8s.io",
		"deviceclasses.v1beta1.resource.k8s.io",
		"deviceclasses.v1beta2.resource.k8s.io",
		"devicetaintrules.v1alpha3.resource.k8s.io",
		"devicetaintrules.v1beta2.resource.k8s.io",
		"endpoints.v1",
		"endpointslices.v1.discovery.k8s.io",
		"events.v1.events.k8s.io",
		"events.v1",
		"flowschemas.v1.flowcontrol.apiserver.k8s.io",
		"horizontalpodautoscalers.v1.autoscaling",
		"horizontalpodautoscalers.v2.autoscaling",
		"ingressclasses.v1.networking.k8s.io",
		"ingresses.v1.networking.k8s.io",
		"ipaddresses.v1.networking.k8s.io",
		"jobs.v1.batch",
		"limitranges.v1",
		"namespaces.v1",
		"networkpolicies.v1.networking.k8s.io",
		"nodes.v1",
		"pants.v1.custom.fancy.com",
		"persistentvolumeclaims.v1",
		"persistentvolumes.v1",
		"podcertificaterequests.v1alpha1.certificates.k8s.io",
		"pods.v1",
		"podtemplates.v1",
		"priorityclasses.v1.scheduling.k8s.io",
		"prioritylevelconfigurations.v1.flowcontrol.apiserver.k8s.io",
		"replicasets.v1.apps",
		"replicationcontrollers.v1",
		"resourceclaims.v1.resource.k8s.io",
		"resourceclaims.v1beta1.resource.k8s.io",
		"resourceclaims.v1beta2.resource.k8s.io",
		"resourceclaimtemplates.v1.resource.k8s.io",
		"resourceclaimtemplates.v1beta1.resource.k8s.io",
		"resourceclaimtemplates.v1beta2.resource.k8s.io",
		"resourcequotas.v1",
		"resourceslices.v1.resource.k8s.io",
		"resourceslices.v1beta1.resource.k8s.io",
		"resourceslices.v1beta2.resource.k8s.io",
		"rolebindings.v1.rbac.authorization.k8s.io",
		"roles.v1.rbac.authorization.k8s.io",
		"secrets.v1",
		"serviceaccounts.v1",
		"servicecidrs.v1.networking.k8s.io",
		"services.v1",
		"statefulsets.v1.apps",
		"storageclasses.v1.storage.k8s.io",
		"validatingadmissionpolicies.v1beta1.admissionregistration.k8s.io",
		"validatingadmissionpolicybindings.v1beta1.admissionregistration.k8s.io",
		"volumeattributesclasses.v1.storage.k8s.io",
	)
	TestAllDefinitions(t, "unconditional-update", func(t *testing.T, api Definition) {
		if !api.HasVerb("create") || !api.HasVerb("get") || !api.HasVerb("update") || !api.HasVerb("delete") {
			t.Skip("Resource does not support create, get, update, and delete")
		}
		client := api.ResourceClient()
		obj := TestObj(t, api.StorageData.Stub, "{}", api.Mapping.GroupVersionKind)

		created, err := client.Create(context.TODO(), obj, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create: %v", err)
		}

		created.SetResourceVersion("")
		_, err = client.Update(context.TODO(), created, metav1.UpdateOptions{})
		rejected := apierrors.IsConflict(err) || isInvalidResourceVersion(err)
		if err != nil && !rejected {
			t.Fatalf("Unexpected error from Update: %v", err)
		}
		assertDefault(t, api.Mapping.Resource, "AllowUnconditionalUpdate must be false", rejected, exempt)
	})
}

// TestDefaultGarbageCollectionPolicy 验证资源是否倾向于采用默认后台删除的
// GarbageCollectionPolicy 策略。
//
// 理由：默认孤立（orphan）依赖对象会让控制器作者和用户感到困惑，且容易导致资源泄漏。前台删除
// 则允许依赖对象阻塞资源的删除。
func TestDefaultGarbageCollectionPolicy(t *testing.T) {
	// 使用 OrphanDependents 垃圾回收策略的 API
	orphanDependentsExempt := sets.New(
		// 为向后兼容而沿用 OrphanDependants 的旧有 API
		"jobs.v1.batch",
		"replicationcontrollers.v1",

		// 也存在使用 OrphanDependants 的非 GA API（如 cronjobs），但其已不再被提供
	)
	// 使用 DeleteDependents 垃圾回收策略的 API
	foregroundExempt := sets.New[string]()

	// 使用 Unsupported 垃圾回收策略的 API
	unsupportedExempt := sets.New(
		// Event 被设计用于高吞吐量的叶子节点。
		"events.v1",
		"events.v1.events.k8s.io",
		"customresourcedefinitions.v1.apiextensions.k8s.io",
	)
	TestAllDefinitions(t, "default-gc-policy", func(t *testing.T, api Definition) {
		if !api.HasVerb("create") || !api.HasVerb("get") || !api.HasVerb("update") || !api.HasVerb("delete") {
			t.Skip("Resource does not support create, get, update, and delete")
		}
		client := api.ResourceClient()
		obj := TestObj(t, api.StorageData.Stub, "{}", api.Mapping.GroupVersionKind)
		name := obj.GetName()

		if _, err := client.Create(context.TODO(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create: %v", err)
		}
		// 先添加一个阻塞用的 finalizer，确保在我们观察到每次删除的结果之前
		// 对象不会被移除。
		if err := addBlockingFinalizer(client, name); err != nil {
			t.Fatalf("Could not set test finalizer: %v", err)
		}
		if err := client.Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
		afterDefault, err := client.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get after default delete: %v", err)
		}
		hasOrphan := slices.Contains(afterDefault.GetFinalizers(), metav1.FinalizerOrphanDependents)
		hasForegroundDeletion := slices.Contains(afterDefault.GetFinalizers(), metav1.FinalizerDeleteDependents)
		isBackground := !hasOrphan && !hasForegroundDeletion

		assertDefault(t, api.Mapping.Resource, "DefaultGarbageCollectionPolicy must be unset, to indicate that dependents are background deleted", isBackground, orphanDependentsExempt.Union(foregroundExempt))
		resName := ResourceString(api.Mapping.Resource)
		if orphanDependentsExempt.Has(resName) {
			if !hasOrphan {
				t.Errorf("%s: DefaultGarbageCollectionPolicy expected to be OrphanDependents", resName)
			}
		}
		if foregroundExempt.Has(resName) {
			if !hasForegroundDeletion {
				t.Errorf("%s: DefaultGarbageCollectionPolicy expected to be DeleteDependents", resName)
			}
		}
	})

	TestAllDefinitions(t, "default-gc-policy-foreground", func(t *testing.T, api Definition) {
		if !api.HasVerb("create") || !api.HasVerb("get") || !api.HasVerb("update") || !api.HasVerb("delete") {
			t.Skip("Resource does not support create, get, update, and delete")
		}
		client := api.ResourceClient()
		obj := TestObj(t, api.StorageData.Stub, "{}", api.Mapping.GroupVersionKind)
		name := obj.GetName()

		if _, err := client.Create(context.TODO(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create: %v", err)
		}
		// 先添加一个阻塞用的 finalizer，确保在我们观察到每次删除的结果之前
		// 对象不会被移除。
		if err := addBlockingFinalizer(client, name); err != nil {
			t.Fatalf("Could not set test finalizer: %v", err)
		}

		// 同时检查使用 Unsupported 垃圾回收策略的 API
		foreground := metav1.DeletePropagationForeground
		if err := client.Delete(context.TODO(), name, metav1.DeleteOptions{PropagationPolicy: &foreground}); err != nil {
			t.Fatalf("Failed to delete with foreground propagation: %v", err)
		}
		afterForeground, err := client.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get after foreground delete: %v", err)
		}
		hasForegroundDeletion := slices.Contains(afterForeground.GetFinalizers(), metav1.FinalizerDeleteDependents)
		assertDefault(t, api.Mapping.Resource, "DefaultGarbageCollectionPolicy must not return Unsupported, propagationPolicy=Foreground must add FinalizerDeleteDependents", hasForegroundDeletion, unsupportedExempt)
	})
}

// TestCheckGracefulDelete 验证 API 未实现删除宽限期支持。
//
// 理由：使用 finalizer 后立即删除对客户端而言更可预测。
func TestCheckGracefulDelete(t *testing.T) {
	exempt := sets.New(
		// Pod 支持宽限期窗口，以便发送 SIGTERM 并让容器在对象被移除前
		// 干净地关闭。
		"pods.v1",
	)
	TestAllDefinitions(t, "check-graceful-delete", func(t *testing.T, api Definition) {
		if !api.HasVerb("create") || !api.HasVerb("get") || !api.HasVerb("update") || !api.HasVerb("delete") {
			t.Skip("Resource does not support create, get, update, and delete")
		}
		rsc := api.ResourceClient()
		obj := TestObj(t, api.StorageData.Stub, "{}", api.Mapping.GroupVersionKind)
		name := obj.GetName()

		if api.Mapping.Resource.GroupResource().String() == "pods" {
			obj.Object["spec"].(map[string]any)["nodeName"] = "fake-node"
		}

		if _, err := rsc.Create(context.TODO(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create: %v", err)
		}
		gracePeriod := int64(30)
		if err := rsc.Delete(context.TODO(), name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
			t.Fatalf("Failed to delete with grace period: %v", err)
		}
		after, err := rsc.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("Unexpected error from Get after delete: %v", err)
		}

		isGraceful := false
		if err == nil {
			if after.GetDeletionGracePeriodSeconds() != nil && *after.GetDeletionGracePeriodSeconds() > 0 {
				isGraceful = true
			}
		}

		assertDefault(t, api.Mapping.Resource, "CheckGracefulDelete must not be implemented", !isGraceful, exempt)
	})
}

// TestReturnDeletedObject 验证 API 是否倾向于设置 ReturnDeletedObject = false。
//
// 理由：删除时返回整个对象开销高昂，客户端几乎从不需要，而更偏向返回
// ReturnDeletedObject = false 时的状态信息。
func TestReturnDeletedObject(t *testing.T) {
	exempt := sets.New(
		// 历史上删除时会返回完整对象的旧有 API：
		"namespaces.v1",
		"persistentvolumeclaims.v1",
		"persistentvolumes.v1",
		"pods.v1",
		"podtemplates.v1",
		"resourcequotas.v1",
		"serviceaccounts.v1",
		"services.v1",
		"csidrivers.v1.storage.k8s.io",
		"csinodes.v1.storage.k8s.io",
		"storageclasses.v1.storage.k8s.io",
		"volumeattachments.v1.storage.k8s.io",
		"volumeattributesclasses.v1.storage.k8s.io",
		"volumeattributesclasses.v1beta1.storage.k8s.io",
		"deviceclasses.v1.resource.k8s.io",
		"deviceclasses.v1beta1.resource.k8s.io",
		"deviceclasses.v1beta2.resource.k8s.io",
		"devicetaintrules.v1alpha3.resource.k8s.io",
		"devicetaintrules.v1beta2.resource.k8s.io",
		"resourceclaims.v1.resource.k8s.io",
		"resourceclaims.v1beta1.resource.k8s.io",
		"resourceclaims.v1beta2.resource.k8s.io",
		"resourceclaimtemplates.v1.resource.k8s.io",
		"resourceclaimtemplates.v1beta1.resource.k8s.io",
		"resourceclaimtemplates.v1beta2.resource.k8s.io",
		"resourcepoolstatusrequests.v1alpha3.resource.k8s.io",
		"resourceslices.v1.resource.k8s.io",
		"resourceslices.v1beta1.resource.k8s.io",
		"resourceslices.v1beta2.resource.k8s.io",

		// 带有无法移除的强制异步清理 finalizer 的 API：
		"customresourcedefinitions.v1.apiextensions.k8s.io",
	)
	TestAllDefinitions(t, "return-deleted-object", func(t *testing.T, api Definition) {
		if !api.HasVerb("create") || !api.HasVerb("delete") {
			t.Skip("Resource does not support create and delete")
		}

		client := api.ResourceClient()
		obj := TestObj(t, api.StorageData.Stub, "{}", api.Mapping.GroupVersionKind)
		created, err := client.Create(context.TODO(), obj, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create: %v", err)
		}
		name := created.GetName()

		if len(created.GetFinalizers()) > 0 && api.HasVerb("update") {
			created.SetFinalizers(nil)
			_, err := client.Update(context.TODO(), created, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Failed to strip finalizers: %v", err)
			}
		}

		gv := api.Mapping.GroupVersionKind.GroupVersion()
		config := dynamic.ConfigFor(api.Config)
		config.GroupVersion = &gv
		config.APIPath = dynamic.LegacyAPIPathResolverFunc(api.Mapping.GroupVersionKind)
		restClient, err := rest.RESTClientFor(config)
		if err != nil {
			t.Fatalf("Failed to create rest client: %v", err)
		}

		zero := int64(0)
		bg := metav1.DeletePropagationBackground
		delOpts := metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
			PropagationPolicy:  &bg,
		}

		req := restClient.Delete().
			Resource(api.Mapping.Resource.Resource).
			Name(name).
			Body(&delOpts)

		if api.Mapping.Scope == meta.RESTScopeNamespace {
			req.Namespace(api.Namespace)
		}

		result := req.Do(context.TODO())
		if err := result.Error(); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}

		resObj, err := result.Get()
		if err != nil {
			t.Fatalf("Failed to get result object: %v", err)
		}

		var kind string
		if _, ok := resObj.(*metav1.Status); ok {
			kind = "Status"
		} else if unstr, ok := resObj.(*unstructured.Unstructured); ok {
			kind = unstr.GetKind()
		} else {
			kind = resObj.GetObjectKind().GroupVersionKind().Kind
		}

		returnsDeleted := kind != "Status"
		assertDefault(t, api.Mapping.Resource, "ReturnDeletedObject must be false", !returnsDeleted, exempt)
	})
}

// addBlockingFinalizer 为 name 追加一个测试用 finalizer，冲突时重试。
func addBlockingFinalizer(client dynamic.ResourceInterface, name string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := client.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		latest.SetFinalizers(append(latest.GetFinalizers(), "test.k8s.io/block"))
		_, err = client.Update(context.TODO(), latest, metav1.UpdateOptions{})
		return err
	})
}

// isInvalidResourceVersion 判断 err 是否是由 metadata.resourceVersion 引起的 Invalid 状态错误。
func isInvalidResourceVersion(err error) bool {
	if !apierrors.IsInvalid(err) {
		return false
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) || statusErr.ErrStatus.Details == nil {
		return false
	}
	for _, cause := range statusErr.ErrStatus.Details.Causes {
		if cause.Field == "metadata.resourceVersion" {
			return true
		}
	}
	return false
}

// assertDefault 检查预期行为是否符合要求，除非该 gvr 在允许列表中——这种情况下
// 预期该行为继续不符合要求。
func assertDefault(t *testing.T, gvr schema.GroupVersionResource, msg string, conforms bool, allowed sets.Set[string]) {
	t.Helper()
	name := ResourceString(gvr)
	if matchesException(gvr, allowed) {
		if conforms {
			t.Errorf("%s: %s unexpectedly conforms. Remove it from the exception list.", name, msg)
		}
	} else if !conforms {
		t.Errorf("%s: %s", name, msg)
	}
}
