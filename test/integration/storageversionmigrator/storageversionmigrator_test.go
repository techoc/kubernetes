/*
Copyright 2024 The Kubernetes Authors.

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

package storageversionmigrator

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	extensionfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	encryptionconfigcontroller "k8s.io/apiserver/pkg/server/options/encryptionconfig/controller"
	etcd3watcher "k8s.io/apiserver/pkg/storage/etcd3"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientgofeaturegate "k8s.io/client-go/features"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestStorageVersionMigration 是验证 storage version migration 正常工作的集成测试。
// 该测试断言以下场景：
// 1. 启动 API server，开启 encryption at rest 和 encryption config 热重载
// 2. 创建一个 secret
// 3. 更新 encryption config 文件，添加一个新密钥作为 write key
// 4. 对 secrets 执行 Storage Version Migration
// 5. 验证 secret 已迁移为使用新密钥
// 6. 验证 secret 以新的 resource version 更新
// 7. 再次对 secrets 执行 Storage Version Migration
// 8. 验证 secret 的 resource version 未更新，即这是一次 no-op 更新
func TestStorageVersionMigration(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.StorageVersionMigrator:                                  true,
		featuregate.Feature(clientgofeaturegate.InformerResourceVersion): true,
	})

	// 这让测试响应更快。默认值是 1 分钟。
	encryptionconfigcontroller.EncryptionConfigFileChangePollDuration = time.Second

	ctx := ktesting.Init(t)

	svmTest := svmSetup(ctx, t)

	// ToDo: 尝试用 1000 个 secret 进行测试
	secret, err := svmTest.createSecret(ctx, t, secretName, defaultNamespace)
	if err != nil {
		t.Fatalf("Failed to create secret: %v", err)
	}

	metricBeforeUpdate := svmTest.getAutomaticReloadSuccessTotal(ctx, t)
	svmTest.updateFile(t, svmTest.filePathForEncryptionConfig, encryptionConfigFileName, []byte(resources["updatedEncryptionConfig"]))
	if !svmTest.isEncryptionConfigFileUpdated(ctx, t, metricBeforeUpdate) {
		t.Fatalf("Failed to update encryption config file")
	}

	svm, err := svmTest.createSVMResource(
		ctx,
		t,
		svmName,
		metav1.GroupResource{
			Group:    "",
			Resource: "secrets",
		},
	)
	if err != nil {
		t.Fatalf("Failed to create SVM resource: %v", err)
	}
	if !svmTest.waitForResourceMigration(ctx, t, svm.Name, secret.Name, 1) {
		t.Fatalf("Failed to migrate resource %s/%s", secret.Namespace, secret.Name)
	}

	wantPrefix := "k8s:enc:aescbc:v1:key2"
	etcdSecret, err := svmTest.getRawSecretFromETCD(t, secret.Namespace, secret.Name)
	if err != nil {
		t.Fatalf("Failed to get secret from etcd: %v", err)
	}
	// 断言 secret 以新密钥为前缀
	if !bytes.HasPrefix(etcdSecret, []byte(wantPrefix)) {
		t.Fatalf("expected secret to be prefixed with %s, but got %s", wantPrefix, etcdSecret)
	}

	secretAfterMigration, err := svmTest.client.CoreV1().Secrets(secret.Namespace).Get(ctx, secret.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get secret: %v", err)
	}
	// 断言 RV 不同
	// secret 用新密钥重新写入 etcd，因此 rv 预期会不同
	if secret.ResourceVersion == secretAfterMigration.ResourceVersion {
		t.Fatalf("Expected resource version to be different, but got the same, rv before: %s, rv after: %s", secret.ResourceVersion, secretAfterMigration.ResourceVersion)
	}

	secondSVM, err := svmTest.createSVMResource(
		ctx,
		t,
		secondSVMName,
		metav1.GroupResource{
			Group:    "",
			Resource: "secrets",
		},
	)
	if err != nil {
		t.Fatalf("Failed to create SVM resource: %v", err)
	}
	if !svmTest.waitForResourceMigration(ctx, t, secondSVM.Name, secretAfterMigration.Name, 2) {
		t.Fatalf("Failed to migrate resource %s/%s", secretAfterMigration.Namespace, secretAfterMigration.Name)
	}

	secretAfterSecondMigration, err := svmTest.client.CoreV1().Secrets(secretAfterMigration.Namespace).Get(ctx, secretAfterMigration.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get secret: %v", err)
	}
	// 断言 RV 相同
	if secretAfterMigration.ResourceVersion != secretAfterSecondMigration.ResourceVersion {
		t.Fatalf("Expected resource version to be same, but got different, rv before: %s, rv after: %s", secretAfterMigration.ResourceVersion, secretAfterSecondMigration.ResourceVersion)
	}
}

// TestStorageVersionMigrationWithCRD 是验证 storage version migration 可与 CRD 一起工作的集成测试。
// 该测试断言以下场景：
// 1. 创建版本为 v1 的 CRD（serving 和 storage）
// 2. 验证 CR 以 v1 写入并按 v1 存储
// 3. 更新 CRD 引入 v2（仅用于 serving），并添加 conversion webhook
// 4. 验证 CR 以 v2 写入但仍按 v1 存储
// 5. 将 CRD 的 storage version 从 v1 改为 v2
// 6. 验证以 v1 或 v2 版本写入的 CR 都按 v2 存储
// 7. 执行 Storage Version Migration，将所有 v1 CR 迁移到 v2
// 8. 更新 CRD，使其不再 serve v1
// 9. 关闭 conversion webhook
// 10. 验证 CR 的 RV 和 Generation
// 11. 验证以 v2 列出 CR 正常
func TestStorageVersionMigrationWithCRD(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.StorageVersionMigrator:                                  true,
		featuregate.Feature(clientgofeaturegate.InformerResourceVersion): true,
		extensionfeatures.CRDObservedGenerationTracking:                  true,
	})
	// 使用 conversion webhook 时预期会出现 decode error
	etcd3watcher.TestOnlySetFatalOnDecodeError(t, false)
	framework.GoleakCheck(t, // 阻止测试清理，让残留的 watch 完成后再让 decode error 重新变为致命错误
		goleak.IgnoreTopFunction("k8s.io/kubernetes/vendor/gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/moby/spdystream.(*Connection).shutdown"),
	)

	ctx := ktesting.Init(t)

	crVersions := make(map[string]versions)

	// chaos goroutine 在迁移过程中删除对象，patch 时产生预期的 404
	svmTest := svmSetup(ctx, t, http.StatusNotFound)
	certCtx := svmTest.setupServerCert(t)

	// 模拟猴子创建和删除 CR
	svmTest.createChaos(ctx, t)

	// 创建 v1 为 serving 和 storage 版本的 CRD
	crd := svmTest.createCRD(t, crdName, crdGroup, certCtx, v1CRDVersion)

	// 创建 CR
	cr1 := svmTest.createCR(ctx, t, "cr1", "v1")
	if ok := svmTest.isCRStoredAtVersion(t, "v1", cr1.GetName()); !ok {
		t.Fatalf("CR not stored at version v1")
	}
	crVersions[cr1.GetName()] = versions{
		generation:  cr1.GetGeneration(),
		rv:          cr1.GetResourceVersion(),
		isRVUpdated: true,
	}

	// 添加 conversion webhook
	shutdownServer := svmTest.createConversionWebhook(ctx, t, certCtx)

	// 添加仅用于 serving 的 v2
	svmTest.updateCRD(ctx, t, crd.Name, v2CRDVersion, []string{"v1", "v2"}, "v1")

	// 创建另一个 CR
	cr2 := svmTest.createCR(ctx, t, "cr2", "v2")
	if ok := svmTest.isCRStoredAtVersion(t, "v1", cr2.GetName()); !ok {
		t.Fatalf("CR not stored at version v1")
	}
	crVersions[cr2.GetName()] = versions{
		generation:  cr2.GetGeneration(),
		rv:          cr2.GetResourceVersion(),
		isRVUpdated: true,
	}

	// 将 v2 添加为 storage version
	svmTest.updateCRD(ctx, t, crd.Name, v2StorageCRDVersion, []string{"v1", "v2"}, "v2")

	// 用 v1 创建 CR
	var cr3 *unstructured.Unstructured
	// updateCRD 检查 discovery 返回的 storageVersionHash 与 storage version v2 匹配，
	// 以确保 API server 使用 v2，但 CRD controller 可能存在竞争，资源仍可能按 v1 存储。
	// 尝试重建 CR，直到它按 v2 存储。
	// https://github.com/kubernetes/kubernetes/issues/130235
	err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, true, func(waitCtx context.Context) (done bool, err error) {
		cr3 = svmTest.createCR(waitCtx, t, "cr3", "v1")
		if ok := svmTest.isCRStoredAtVersion(t, "v2", cr3.GetName()); !ok {
			svmTest.deleteCR(waitCtx, t, cr3.GetName(), "v1")
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for CR to be stored as v2: %v", err)
	}

	crVersions[cr3.GetName()] = versions{
		generation:  cr3.GetGeneration(),
		rv:          cr3.GetResourceVersion(),
		isRVUpdated: false,
	}

	// 用 v2 创建 CR
	cr4 := svmTest.createCR(ctx, t, "cr4", "v2")
	if ok := svmTest.isCRStoredAtVersion(t, "v2", cr4.GetName()); !ok {
		t.Fatalf("CR not stored at version v2")
	}
	crVersions[cr4.GetName()] = versions{
		generation:  cr4.GetGeneration(),
		rv:          cr4.GetResourceVersion(),
		isRVUpdated: false,
	}

	// 验证 cr1 和 cr2 仍按 v1 存储
	if ok := svmTest.isCRStoredAtVersion(t, "v1", cr1.GetName()); !ok {
		t.Fatalf("CR not stored at version v1")
	}
	if ok := svmTest.isCRStoredAtVersion(t, "v1", cr2.GetName()); !ok {
		t.Fatalf("CR not stored at version v1")
	}

	// 将 CR 从 v1 迁移到 v2
	svm, err := svmTest.createSVMResource(
		ctx, t, "crdsvm",
		metav1.GroupResource{
			Group:    crd.Spec.Group,
			Resource: crd.Spec.Names.Plural,
		})
	if err != nil {
		t.Fatalf("Failed to create SVM resource: %v", err)
	}
	if ok := svmTest.isCRDMigrated(ctx, t, svm.Name, crd.Name, "triggercr"); !ok {
		t.Fatalf("CRD not migrated")
	}

	// 断言所有 CR 在 etcd 中都按正确的版本存储
	if ok := svmTest.isCRStoredAtVersion(t, "v2", cr1.GetName()); !ok {
		t.Fatalf("CR not stored at version v2")
	}
	if ok := svmTest.isCRStoredAtVersion(t, "v2", cr2.GetName()); !ok {
		t.Fatalf("CR not stored at version v2")
	}
	if ok := svmTest.isCRStoredAtVersion(t, "v2", cr3.GetName()); !ok {
		t.Fatalf("CR not stored at version v2")
	}
	if ok := svmTest.isCRStoredAtVersion(t, "v2", cr4.GetName()); !ok {
		t.Fatalf("CR not stored at version v2")
	}

	// 更新 CRD，使 v1 不再 serving 和 storage，然后关闭 webhook
	svmTest.updateCRD(ctx, t, crd.Name, v1NotServingCRDVersion, []string{"v2"}, "v2")
	shutdownServer()

	// 断言 CR 的 RV 和 Generation
	svmTest.validateRVAndGeneration(ctx, t, crVersions, "v2")

	// 断言 v2 CR 可以被列出
	if err := svmTest.listCR(ctx, t, "v2"); err != nil {
		t.Fatalf("Failed to list CRs at version v2: %v", err)
	}
}

// TestStorageVersionMigrationDuringChaos 用作 SVM controller 的压力测试。
// 它创建一个 CRD 以及该资源合理数量的静态实例。
// 同时它会持续创建和删除该资源的实例。
// 在此期间，它尝试对该资源执行多次并行迁移。
// 它断言所有迁移都成功，且所有静态实例在初次创建后都没有变化（因为迁移必须是 no-op）。
func TestStorageVersionMigrationDuringChaos(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.StorageVersionMigrator:                                  true,
		featuregate.Feature(clientgofeaturegate.InformerResourceVersion): true,
		extensionfeatures.CRDObservedGenerationTracking:                  true,
	})

	ctx := ktesting.Init(t)

	// chaos goroutine 在迁移过程中删除对象，patch 时产生预期的 404
	svmTest := svmSetup(ctx, t, http.StatusNotFound)

	svmTest.createChaos(ctx, t)

	crd := svmTest.createCRD(t, crdName, crdGroup, nil, v1CRDVersion)

	crVersions := make(map[string]versions)

	for i := range 50 { // 更贴近实际的资源总数
		cr := svmTest.createCR(ctx, t, "created-cr-"+strconv.Itoa(i), "v1")
		crVersions[cr.GetName()] = versions{
			generation:  cr.GetGeneration(),
			rv:          cr.GetResourceVersion(),
			isRVUpdated: false, // 这些 CR 不应因迁移而改变
		}
	}

	var wg sync.WaitGroup
	const migrations = 10 // 大于 SVM 的 worker 总数
	wg.Add(migrations)
	for i := range migrations {
		go func() {
			defer wg.Done()

			svm, err := svmTest.createSVMResource(
				ctx, t, "chaos-svm-"+strconv.Itoa(i),
				metav1.GroupResource{
					Group:    crd.Spec.Group,
					Resource: crd.Spec.Names.Plural,
				},
			)
			if err != nil {
				t.Errorf("Failed to create SVM resource: %v", err)
				return
			}
			triggerCRName := "chaos-trigger-" + strconv.Itoa(i)
			if ok := svmTest.isCRDMigrated(ctx, t, svm.Name, crd.Name, triggerCRName); !ok {
				t.Errorf("CRD not migrated")
				return
			}
		}()
	}
	wg.Wait()

	svmTest.validateRVAndGeneration(ctx, t, crVersions, "v1")
}
