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

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	testNamespace = "not-the-default"
	testFinalizer = "noxu.example.com/finalizer"
)

// TestFinalization 验证 finalizer 会阻止 CR 被删除，移除后才能完成删除。
func TestFinalization(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	require.NoError(t, err)
	defer tearDown()

	noxuDefinition := fixtures.NewNoxuV1CustomResourceDefinition(apiextensionsv1.ClusterScoped)
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	require.NoError(t, err)

	ns := testNamespace
	name := "foo123"
	noxuResourceClient := newNamespacedCustomResourceClient(ns, dynamicClient, noxuDefinition)

	instance := fixtures.NewNoxuInstance(ns, name)
	instance.SetFinalizers([]string{testFinalizer})
	createdNoxuInstance, err := instantiateCustomResource(t, instance, noxuResourceClient, noxuDefinition)
	require.NoError(t, err)

	uid := createdNoxuInstance.GetUID()
	err = noxuResourceClient.Delete(context.TODO(), name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID: &uid,
		},
	})
	require.NoError(t, err)

	// 带 finalizer 的删除只会设置非空 deletionTimestamp，并不会把对象从 apiserver 移除。
	// 这里读回来确认这一点。
	gottenNoxuInstance, err := noxuResourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)

	require.NotNil(t, gottenNoxuInstance.GetDeletionTimestamp())

	// 再次删除，确认 finalizer 仍在时对象不会被移除。
	err = noxuResourceClient.Delete(context.TODO(), name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID: &uid,
		},
	})
	require.NoError(t, err)

	// 移除 finalizer，让随后的删除得以完成。
	// 如果之前的删除错误地移除了对象，这一步会失败。对象会在 finalizer 更新时被删除。
	for {
		gottenNoxuInstance.SetFinalizers(nil)
		_, err = noxuResourceClient.Update(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
		if err == nil {
			break
		}
		if !errors.IsConflict(err) {
			require.NoError(t, err) // 非冲突错误直接失败
		}
		gottenNoxuInstance, err = noxuResourceClient.Get(context.TODO(), name, metav1.GetOptions{})
		require.NoError(t, err)
	}

	// 确认对象确实已消失。
	_, err = noxuResourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	require.Error(t, err)
	require.True(t, errors.IsNotFound(err), "%#v", err)
}

// TestFinalizationAndDeletion 验证删除 CRD 时，仍带 finalizer 的 CR 不会被自动清理；
// 移除 finalizer 后 CR 与 CRD 才会最终消失。
func TestFinalizationAndDeletion(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	require.NoError(t, err)
	defer tearDown()

	// 创建 CRD。
	noxuDefinition := fixtures.NewNoxuV1CustomResourceDefinition(apiextensionsv1.ClusterScoped)
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	require.NoError(t, err)

	// 创建带 finalizer 的 CR。
	ns := testNamespace
	name := "foo123"
	noxuResourceClient := newNamespacedCustomResourceClient(ns, dynamicClient, noxuDefinition)

	instance := fixtures.NewNoxuInstance(ns, name)
	instance.SetFinalizers([]string{testFinalizer})
	createdNoxuInstance, err := instantiateCustomResource(t, instance, noxuResourceClient, noxuDefinition)
	require.NoError(t, err)

	// 删除 CR。因为有 finalizer，此刻不会被真正删除。
	uid := createdNoxuInstance.GetUID()
	err = noxuResourceClient.Delete(context.TODO(), name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID: &uid,
		},
	})
	require.NoError(t, err)

	// 确认 CR 已进入待删除状态。
	gottenNoxuInstance, err := noxuResourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, gottenNoxuInstance.GetDeletionTimestamp())

	// 删除 CRD。
	fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient)

	// 确认 CRD 删除后 CR 仍然存在。
	gottenNoxuInstance, err = noxuResourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)

	// 更新 CR 以移除 finalizer。
	for {
		gottenNoxuInstance.SetFinalizers(nil)
		_, err = noxuResourceClient.Update(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
		if err == nil {
			break
		}
		if !errors.IsConflict(err) {
			require.NoError(t, err) // 非冲突错误直接失败
		}
		gottenNoxuInstance, err = noxuResourceClient.Get(context.TODO(), name, metav1.GetOptions{})
		require.NoError(t, err)
	}

	// 确认 CR 已消失，应返回 NotFound。
	_, err = noxuResourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Fatalf("unable to delete cr: %v", err)
	}

	err = wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		_, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), noxuDefinition.Name, metav1.GetOptions{})
		return errors.IsNotFound(err), err
	})
	if !errors.IsNotFound(err) {
		t.Fatalf("unable to delete crd: %v", err)
	}
}

// TestApplyCRDuringCRDFinalization 验证 CRD 处于 Terminating 时，SSA 创建 CR 会被拒绝。
func TestApplyCRDuringCRDFinalization(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	require.NoError(t, err)
	defer tearDown()

	// 创建带 finalizer 的 CRD，让删除流程停住。
	noxuDefinition := fixtures.NewNoxuV1CustomResourceDefinition(apiextensionsv1.ClusterScoped)
	noxuDefinition.SetFinalizers([]string{testFinalizer})
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	require.NoError(t, err)

	// 删除 CRD。由于存在 finalizer，它会卡在 terminating 状态。
	err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Delete(t.Context(), noxuDefinition.Name, metav1.DeleteOptions{})
	require.NoError(t, err)

	// 等待 CRD 的 Terminating condition 变为 True。
	// handler 通过 IsCRDConditionTrue(crd, apiextensionsv1.Terminating) 阻止创建 CR，
	// 而该 condition 是 CRD finalizer 控制器观察到 DeletionTimestamp 后异步设置的。
	// 不等待的话，Apply 可能抢在控制器设置 condition 之前成功。
	err = wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		crd, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, noxuDefinition.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Terminating), nil
	})
	require.NoError(t, err, "timed out waiting for CRD Terminating condition to be set")

	// 尝试用 SSA 创建 CR，应因 CRD 正在删除而失败。
	ns := testNamespace
	name := "foo123"
	noxuResourceClient := newNamespacedCustomResourceClient(ns, dynamicClient, noxuDefinition)

	wantErr := `create not allowed while custom resource definition is terminating`
	var applyErr error
	instance := fixtures.NewNoxuInstance(ns, name)
	// CRD handler 用 informer 缓存判断是否阻止创建。
	// 上面的 CRD GET 可能先于缓存观察到删除，因此通过本测试要验证的 dry-run apply 路径重试，
	// 直到缓存也反映删除。
	err = wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		_, applyErr = noxuResourceClient.Apply(ctx, name, instance, metav1.ApplyOptions{DryRun: []string{"All"}, FieldManager: "manager"})
		if applyErr == nil {
			return false, nil
		}
		if !errors.IsForbidden(applyErr) {
			return false, applyErr
		}
		return true, nil
	})
	require.NoError(t, err, "timed out waiting for CR apply to be rejected while CRD is deleting")
	require.ErrorContains(t, applyErr, wantErr)
}
