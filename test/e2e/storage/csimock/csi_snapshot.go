/*
Copyright 2022 The Kubernetes Authors.

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

package csimock

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/storage/drivers"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = utils.SIGDescribe("CSI Mock volume snapshot", func() {
	f := framework.NewDefaultFramework("csi-mock-volumes-snapshot")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	m := newMockDriverSetup(f)

	f.Context("CSI Volume Snapshots", feature.VolumeSnapshotDataSource, func() {
		tests := []struct {
			name               string
			createSnapshotHook func(counter int64) error
		}{
			{
				name: "volumesnapshotcontent and pvc in Bound state with deletion timestamp set should not get deleted while snapshot finalizer exists",
				createSnapshotHook: func(counter int64) error {
					if counter < 8 {
						return status.Error(codes.DeadlineExceeded, "fake error")
					}
					return nil
				},
			},
		}
		for _, test := range tests {
			ginkgo.It(test.name, func(ctx context.Context) {
				var hooks *drivers.Hooks
				if test.createSnapshotHook != nil {
					hooks = createPreHook("CreateSnapshot", test.createSnapshotHook)
				}
				m.init(ctx, testParameters{
					disableAttach:  true,
					registerDriver: true,
					enableSnapshot: true,
					hooks:          hooks,
				})
				sDriver, ok := m.driver.(storageframework.SnapshottableTestDriver)
				if !ok {
					e2eskipper.Skipf("mock driver %s does not support snapshots -- skipping", m.driver.GetDriverInfo().Name)

				}
				ctx, cancel := context.WithTimeout(ctx, csiPodRunningTimeout)
				defer cancel()
				ginkgo.DeferCleanup(m.cleanup)

				sc := m.driver.GetDynamicProvisionStorageClass(ctx, m.config, "")
				ginkgo.By("Creating storage class")
				class, err := m.cs.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
				framework.ExpectNoError(err, "Failed to create class: %v", err)
				m.sc[class.Name] = class
				claim := e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
					// 使用固定名称，以便先于 PVC 创建 volumesnapshot。
					Name:             "snapshot-test-pvc",
					StorageClassName: &(class.Name),
				}, f.Namespace.Name)

				ginkgo.By("Creating snapshot")
				// TODO: 测试使用 Retain policy 的 VolumeSnapshot
				parameters := map[string]string{}
				snapshotClass, snapshot := storageframework.CreateSnapshot(ctx, sDriver, m.config, storageframework.DynamicSnapshotDelete, claim.Name, claim.Namespace, f.Timeouts, parameters)
				framework.ExpectNoError(err, "failed to create snapshot")
				m.vsc[snapshotClass.GetName()] = snapshotClass
				volumeSnapshotName := snapshot.GetName()

				ginkgo.By(fmt.Sprintf("Creating PVC %s/%s", claim.Namespace, claim.Name))
				claim, err = m.cs.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(context.TODO(), claim, metav1.CreateOptions{})
				framework.ExpectNoError(err, "Failed to create claim: %v", err)

				ginkgo.By(fmt.Sprintf("Wait for finalizer to be added to claim %s/%s", claim.Namespace, claim.Name))
				err = e2epv.WaitForPVCFinalizer(ctx, m.cs, claim.Name, claim.Namespace, pvcAsSourceProtectionFinalizer, 1*time.Millisecond, 1*time.Minute)
				framework.ExpectNoError(err)

				ginkgo.By("Wait for PVC to be Bound")
				_, err = e2epv.WaitForPVClaimBoundPhase(ctx, m.cs, []*v1.PersistentVolumeClaim{claim}, 1*time.Minute)
				framework.ExpectNoError(err, "Failed to create claim: %v", err)

				ginkgo.By(fmt.Sprintf("Delete PVC %s", claim.Name))
				err = e2epv.DeletePersistentVolumeClaim(ctx, m.cs, claim.Name, claim.Namespace)
				framework.ExpectNoError(err, "failed to delete pvc")

				ginkgo.By("Get PVC from API server and verify deletion timestamp is set")
				claim, err = m.cs.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Get(context.TODO(), claim.Name, metav1.GetOptions{})
				if err != nil {
					if !apierrors.IsNotFound(err) {
						framework.ExpectNoError(err, "Failed to get claim: %v", err)
					}
					framework.Logf("PVC not found. Continuing to test VolumeSnapshotContent finalizer")
				} else if claim.DeletionTimestamp == nil {
					framework.Failf("Expected deletion timestamp to be set on PVC: %v", claim)
				}

				ginkgo.By(fmt.Sprintf("Get VolumeSnapshotContent bound to VolumeSnapshot %s", snapshot.GetName()))
				snapshotContent := utils.GetSnapshotContentFromSnapshot(ctx, m.config.Framework.DynamicClient, snapshot, f.Timeouts.SnapshotCreate)
				volumeSnapshotContentName := snapshotContent.GetName()

				ginkgo.By(fmt.Sprintf("Verify VolumeSnapshotContent %s contains finalizer %s", snapshot.GetName(), volumeSnapshotContentFinalizer))
				err = utils.WaitForGVRFinalizer(ctx, m.config.Framework.DynamicClient, utils.SnapshotContentGVR, volumeSnapshotContentName, "", volumeSnapshotContentFinalizer, 1*time.Millisecond, 1*time.Minute)
				framework.ExpectNoError(err)

				ginkgo.By(fmt.Sprintf("Delete VolumeSnapshotContent %s", snapshotContent.GetName()))
				err = m.config.Framework.DynamicClient.Resource(utils.SnapshotContentGVR).Delete(ctx, snapshotContent.GetName(), metav1.DeleteOptions{})
				framework.ExpectNoError(err, "Failed to delete snapshotcontent: %v", err)

				ginkgo.By("Get VolumeSnapshotContent from API server and verify deletion timestamp is set")
				snapshotContent, err = m.config.Framework.DynamicClient.Resource(utils.SnapshotContentGVR).Get(context.TODO(), snapshotContent.GetName(), metav1.GetOptions{})
				framework.ExpectNoError(err)

				if snapshotContent.GetDeletionTimestamp() == nil {
					framework.Failf("Expected deletion timestamp to be set on snapshotcontent")
				}

				// 如果 claim 不存在，API server 的 Get() 调用会返回一个
				// 非 nil、但所有字段都未设置的 claim 对象。
				// 参见 https://github.com/kubernetes/kubernetes/pull/99167#issuecomment-781670012
				if claim != nil && claim.Spec.VolumeName != "" {
					ginkgo.By(fmt.Sprintf("Wait for PV %s to be deleted", claim.Spec.VolumeName))
					err = e2epv.WaitForPersistentVolumeDeleted(ctx, m.cs, claim.Spec.VolumeName, framework.Poll, 3*time.Minute)
					framework.ExpectNoError(err, fmt.Sprintf("failed to delete PV %s", claim.Spec.VolumeName))
				}

				ginkgo.By(fmt.Sprintf("Verify VolumeSnapshot %s contains finalizer %s", snapshot.GetName(), volumeSnapshotBoundFinalizer))
				err = utils.WaitForGVRFinalizer(ctx, m.config.Framework.DynamicClient, utils.SnapshotGVR, volumeSnapshotName, f.Namespace.Name, volumeSnapshotBoundFinalizer, 1*time.Millisecond, 1*time.Minute)
				framework.ExpectNoError(err)

				ginkgo.By("Delete VolumeSnapshot")
				err = utils.DeleteAndWaitSnapshot(ctx, m.config.Framework.DynamicClient, f.Namespace.Name, volumeSnapshotName, framework.Poll, framework.SnapshotDeleteTimeout)
				framework.ExpectNoError(err, fmt.Sprintf("failed to delete VolumeSnapshot %s", volumeSnapshotName))

				ginkgo.By(fmt.Sprintf("Wait for VolumeSnapshotContent %s to be deleted", volumeSnapshotContentName))
				err = utils.WaitForGVRDeletion(ctx, m.config.Framework.DynamicClient, utils.SnapshotContentGVR, volumeSnapshotContentName, framework.Poll, framework.SnapshotDeleteTimeout)
				framework.ExpectNoError(err, fmt.Sprintf("failed to delete VolumeSnapshotContent %s", volumeSnapshotContentName))
			})
		}
	})

	f.Context("CSI Volume Snapshots secrets", feature.VolumeSnapshotDataSource, func() {

		var (
			// CSISnapshotterSecretName 是要创建的 secret 的名称
			CSISnapshotterSecretName string = "snapshot-secret"

			// CSISnapshotterSecretNameAnnotation 是 VolumeSnapshotClass.parameters 中 CSI snapshotter secret 名称的 annotation key
			CSISnapshotterSecretNameAnnotation string = "csi.storage.k8s.io/snapshotter-secret-name"

			// CSISnapshotterSecretNamespaceAnnotation 是 VolumeSnapshotClass.parameters 中 CSI snapshotter secret namespace 的 annotation key
			CSISnapshotterSecretNamespaceAnnotation string = "csi.storage.k8s.io/snapshotter-secret-namespace"

			// anotations 保存 annotations 对象
			annotations interface{}
		)

		tests := []struct {
			name               string
			createSnapshotHook func(counter int64) error
		}{
			{
				// 即使前几次尝试失败，也应能使用 secret 成功创建 volume snapshot，
				name: "volume snapshot create/delete with secrets",
				// 前 8 次创建 snapshot 的调用失败，第 9 次成功。
				createSnapshotHook: func(counter int64) error {
					if counter < 8 {
						return status.Error(codes.DeadlineExceeded, "fake error")
					}
					return nil
				},
			},
		}
		for _, test := range tests {
			ginkgo.It(test.name, func(ctx context.Context) {
				hooks := createPreHook("CreateSnapshot", test.createSnapshotHook)
				m.init(ctx, testParameters{
					disableAttach:  true,
					registerDriver: true,
					enableSnapshot: true,
					hooks:          hooks,
				})

				sDriver, ok := m.driver.(storageframework.SnapshottableTestDriver)
				if !ok {
					e2eskipper.Skipf("mock driver does not support snapshots -- skipping")
				}
				ginkgo.DeferCleanup(m.cleanup)

				var sc *storagev1.StorageClass
				if dDriver, ok := m.driver.(storageframework.DynamicPVTestDriver); ok {
					sc = dDriver.GetDynamicProvisionStorageClass(ctx, m.config, "")
				}
				ginkgo.By("Creating storage class")
				class, err := m.cs.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
				framework.ExpectNoError(err, "Failed to create storage class: %v", err)
				m.sc[class.Name] = class
				pvc := e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
					Name:             "snapshot-test-pvc",
					StorageClassName: &(class.Name),
				}, f.Namespace.Name)

				ginkgo.By(fmt.Sprintf("Creating PVC %s/%s", pvc.Namespace, pvc.Name))
				pvc, err = m.cs.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
				framework.ExpectNoError(err, "Failed to create claim: %v", err)

				ginkgo.By("Wait for PVC to be Bound")
				_, err = e2epv.WaitForPVClaimBoundPhase(ctx, m.cs, []*v1.PersistentVolumeClaim{pvc}, 1*time.Minute)
				framework.ExpectNoError(err, "Failed to create claim: %v", err)

				m.pvcs = append(m.pvcs, pvc)

				ginkgo.By("Creating Secret")
				secret := &v1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: f.Namespace.Name,
						Name:      CSISnapshotterSecretName,
					},
					Data: map[string][]byte{
						"secret-data": []byte("secret-value-1"),
					},
				}

				if secret, err := m.cs.CoreV1().Secrets(f.Namespace.Name).Create(context.TODO(), secret, metav1.CreateOptions{}); err != nil {
					framework.Failf("unable to create test secret %s: %v", secret.Name, err)
				}

				ginkgo.By("Creating snapshot with secrets")
				parameters := map[string]string{
					CSISnapshotterSecretNameAnnotation:      CSISnapshotterSecretName,
					CSISnapshotterSecretNamespaceAnnotation: f.Namespace.Name,
				}

				_, snapshot := storageframework.CreateSnapshot(ctx, sDriver, m.config, storageframework.DynamicSnapshotDelete, pvc.Name, pvc.Namespace, f.Timeouts, parameters)
				framework.ExpectNoError(err, "failed to create snapshot")
				snapshotcontent := utils.GetSnapshotContentFromSnapshot(ctx, m.config.Framework.DynamicClient, snapshot, f.Timeouts.SnapshotCreate)
				if annotations, ok = snapshotcontent.Object["metadata"].(map[string]interface{})["annotations"]; !ok {
					framework.Failf("Unable to get volume snapshot content annotations")
				}

				// 检查 delete snapshot secret 的 annotation 是否应用到 VolumeSnapshotContent。
				checkDeleteSnapshotSecrets(m.cs, annotations)

				// 删除 snapshot 并检查其是否已被删除。
				deleteSnapshot(m.cs, m.config, snapshot)
			})
		}
	})

	f.Context("CSI Snapshot Controller metrics", feature.VolumeSnapshotDataSource, func() {
		tests := []struct {
			name    string
			pattern storageframework.TestPattern
		}{
			{
				name:    "snapshot controller should emit dynamic CreateSnapshot, CreateSnapshotAndReady, and DeleteSnapshot metrics",
				pattern: storageframework.DynamicSnapshotDelete,
			},
			{
				name:    "snapshot controller should emit pre-provisioned CreateSnapshot, CreateSnapshotAndReady, and DeleteSnapshot metrics",
				pattern: storageframework.PreprovisionedSnapshotDelete,
			},
		}
		for _, test := range tests {
			ginkgo.It(test.name, func(ctx context.Context) {
				m.init(ctx, testParameters{
					disableAttach:  true,
					registerDriver: true,
					enableSnapshot: true,
				})

				sDriver, ok := m.driver.(storageframework.SnapshottableTestDriver)
				if !ok {
					e2eskipper.Skipf("mock driver does not support snapshots -- skipping")
				}
				ginkgo.DeferCleanup(m.cleanup)

				metricsGrabber, err := e2emetrics.NewMetricsGrabber(ctx, m.config.Framework.ClientSet, nil, f.ClientConfig(), false, false, false, false, false, true)
				if err != nil {
					framework.Failf("Error creating metrics grabber : %v", err)
				}

				// 先获取初始 metrics；如果失败，说明 snapshot controller 的 metrics 未配置，跳过测试。
				_, err = metricsGrabber.GrabFromSnapshotController(ctx, framework.TestContext.SnapshotControllerPodName, framework.TestContext.SnapshotControllerHTTPPort)
				if err != nil {
					e2eskipper.Skipf("Snapshot controller metrics not found -- skipping")
				}

				ginkgo.By("getting all initial metric values")
				metricsTestConfig := newSnapshotMetricsTestConfig("snapshot_controller_operation_total_seconds_count",
					"count",
					m.config.GetUniqueDriverName(),
					"CreateSnapshot",
					"success",
					"",
					test.pattern)
				createSnapshotMetrics := newSnapshotControllerMetrics(metricsTestConfig, metricsGrabber)
				originalCreateSnapshotCount, _ := createSnapshotMetrics.getSnapshotControllerMetricValue(ctx)
				metricsTestConfig.operationName = "CreateSnapshotAndReady"
				createSnapshotAndReadyMetrics := newSnapshotControllerMetrics(metricsTestConfig, metricsGrabber)
				originalCreateSnapshotAndReadyCount, _ := createSnapshotAndReadyMetrics.getSnapshotControllerMetricValue(ctx)

				metricsTestConfig.operationName = "DeleteSnapshot"
				deleteSnapshotMetrics := newSnapshotControllerMetrics(metricsTestConfig, metricsGrabber)
				originalDeleteSnapshotCount, _ := deleteSnapshotMetrics.getSnapshotControllerMetricValue(ctx)

				ginkgo.By("Creating storage class")
				var sc *storagev1.StorageClass
				if dDriver, ok := m.driver.(storageframework.DynamicPVTestDriver); ok {
					sc = dDriver.GetDynamicProvisionStorageClass(ctx, m.config, "")
				}
				class, err := m.cs.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
				framework.ExpectNoError(err, "Failed to create storage class: %v", err)
				m.sc[class.Name] = class
				pvc := e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
					Name:             "snapshot-test-pvc",
					StorageClassName: &(class.Name),
				}, f.Namespace.Name)

				ginkgo.By(fmt.Sprintf("Creating PVC %s/%s", pvc.Namespace, pvc.Name))
				pvc, err = m.cs.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
				framework.ExpectNoError(err, "Failed to create claim: %v", err)

				ginkgo.By("Wait for PVC to be Bound")
				_, err = e2epv.WaitForPVClaimBoundPhase(ctx, m.cs, []*v1.PersistentVolumeClaim{pvc}, 1*time.Minute)
				framework.ExpectNoError(err, "Failed to create claim: %v", err)

				ginkgo.By("Creating snapshot")
				parameters := map[string]string{}
				sr := storageframework.CreateSnapshotResource(ctx, sDriver, m.config, test.pattern, pvc.Name, pvc.Namespace, f.Timeouts, parameters)
				framework.ExpectNoError(err, "failed to create snapshot")

				ginkgo.By("Checking for CreateSnapshot metrics")
				createSnapshotMetrics.waitForSnapshotControllerMetric(ctx, originalCreateSnapshotCount+1.0, f.Timeouts.SnapshotControllerMetrics)

				ginkgo.By("Checking for CreateSnapshotAndReady metrics")
				err = utils.WaitForSnapshotReady(ctx, m.config.Framework.DynamicClient, pvc.Namespace, sr.Vs.GetName(), framework.Poll, f.Timeouts.SnapshotCreate)
				framework.ExpectNoError(err, "failed to wait for snapshot ready")
				createSnapshotAndReadyMetrics.waitForSnapshotControllerMetric(ctx, originalCreateSnapshotAndReadyCount+1.0, f.Timeouts.SnapshotControllerMetrics)

				// 删除 snapshot 并检查其是否已被删除
				deleteSnapshot(m.cs, m.config, sr.Vs)

				ginkgo.By("check for delete metrics")
				metricsTestConfig.operationName = "DeleteSnapshot"
				deleteSnapshotMetrics.waitForSnapshotControllerMetric(ctx, originalDeleteSnapshotCount+1.0, f.Timeouts.SnapshotControllerMetrics)
			})
		}
	})
})

// checkDeleteSnapshotSecrets 检查 delete snapshot secret 的 annotation 是否应用到 VolumeSnapshotContent。
func checkDeleteSnapshotSecrets(cs clientset.Interface, annotations interface{}) error {
	ginkgo.By("checking if delete snapshot secrets annotation is applied to the VolumeSnapshotContent")

	var (
		annDeletionSecretName      string
		annDeletionSecretNamespace string
		ok                         bool
		err                        error

		// CSISnapshotterDeleteSecretNameAnnotation 是 VolumeSnapshotClass.parameters 中 CSI snapshotter 删除 secret 名称的 annotation key
		CSISnapshotterDeleteSecretNameAnnotation string = "snapshot.storage.kubernetes.io/deletion-secret-name"

		// CSISnapshotterDeleteSecretNamespaceAnnotation 是 VolumeSnapshotClass.parameters 中 CSI snapshotter 删除 secret namespace 的 annotation key
		CSISnapshotterDeleteSecretNamespaceAnnotation string = "snapshot.storage.kubernetes.io/deletion-secret-namespace"
	)

	annotationsObj, ok := annotations.(map[string]interface{})
	if !ok {
		framework.Failf("failed to get annotations from annotations object")
	}

	if annDeletionSecretName, ok = annotationsObj[CSISnapshotterDeleteSecretNameAnnotation].(string); !ok {
		framework.Failf("unable to get secret annotation name")
	}
	if annDeletionSecretNamespace, ok = annotationsObj[CSISnapshotterDeleteSecretNamespaceAnnotation].(string); !ok {
		framework.Failf("unable to get secret annotation namespace")
	}

	// 验证 secret 是否存在
	if _, err = cs.CoreV1().Secrets(annDeletionSecretNamespace).Get(context.TODO(), annDeletionSecretName, metav1.GetOptions{}); err != nil {
		framework.Failf("unable to get test secret %s: %v", annDeletionSecretName, err)
	}

	return err
}

func deleteSnapshot(cs clientset.Interface, config *storageframework.PerTestConfig, snapshot *unstructured.Unstructured) {
	// 删除指定的 snapshot
	dc := config.Framework.DynamicClient
	err := dc.Resource(utils.SnapshotGVR).Namespace(snapshot.GetNamespace()).Delete(context.TODO(), snapshot.GetName(), metav1.DeleteOptions{})
	framework.ExpectNoError(err)

	// 检查 snapshot 是否已被删除
	_, err = dc.Resource(utils.SnapshotGVR).Get(context.TODO(), snapshot.GetName(), metav1.GetOptions{})
	gomega.Expect(err).To(gomega.MatchError(apierrors.IsNotFound, "the snapshot is not deleted"))
}

type snapshotMetricsTestConfig struct {
	// 期望值
	metricName      string
	metricType      string
	driverName      string
	operationName   string
	operationStatus string
	snapshotType    string
	le              string
}

type snapshotControllerMetrics struct {
	// metric 的配置
	cfg            snapshotMetricsTestConfig
	metricsGrabber *e2emetrics.Grabber

	// 结果
	countMetrics  map[string]float64
	sumMetrics    map[string]float64
	bucketMetrics map[string]float64
}

func newSnapshotMetricsTestConfig(metricName, metricType, driverName, operationName, operationStatus, le string, pattern storageframework.TestPattern) snapshotMetricsTestConfig {
	var snapshotType string
	switch pattern.SnapshotType {
	case storageframework.DynamicCreatedSnapshot:
		snapshotType = "dynamic"

	case storageframework.PreprovisionedCreatedSnapshot:
		snapshotType = "pre-provisioned"

	default:
		framework.Failf("invalid snapshotType: %v", pattern.SnapshotType)
	}

	return snapshotMetricsTestConfig{
		metricName:      metricName,
		metricType:      metricType,
		driverName:      driverName,
		operationName:   operationName,
		operationStatus: operationStatus,
		snapshotType:    snapshotType,
		le:              le,
	}
}

func newSnapshotControllerMetrics(cfg snapshotMetricsTestConfig, metricsGrabber *e2emetrics.Grabber) *snapshotControllerMetrics {
	return &snapshotControllerMetrics{
		cfg:            cfg,
		metricsGrabber: metricsGrabber,

		countMetrics:  make(map[string]float64),
		sumMetrics:    make(map[string]float64),
		bucketMetrics: make(map[string]float64),
	}
}

func (scm *snapshotControllerMetrics) waitForSnapshotControllerMetric(ctx context.Context, expectedValue float64, timeout time.Duration) {
	metricKey := scm.getMetricKey()
	if successful := utils.WaitUntil(10*time.Second, timeout, func() bool {
		// 获取 metric 值
		actualValue, err := scm.getSnapshotControllerMetricValue(ctx)
		if err != nil {
			return false
		}

		// 前一个测试的操作可能已完成，因此只要求当前值不低于期望值。
		if actualValue < expectedValue {
			return false
		}

		return true
	}); successful {
		return
	}

	scm.showMetricsFailure(metricKey)
	framework.Failf("Unable to get valid snapshot controller metrics after %v", timeout)
}

func (scm *snapshotControllerMetrics) getSnapshotControllerMetricValue(ctx context.Context) (float64, error) {
	metricKey := scm.getMetricKey()

	// 抓取并解析为可读格式
	err := scm.grabSnapshotControllerMetrics(ctx)
	if err != nil {
		return 0, err
	}

	metrics := scm.getMetricsTable()
	actual, ok := metrics[metricKey]
	if !ok {
		return 0, fmt.Errorf("did not find metric for key %s", metricKey)
	}

	return actual, nil
}

func (scm *snapshotControllerMetrics) getMetricsTable() map[string]float64 {
	var metrics map[string]float64
	switch scm.cfg.metricType {
	case "count":
		metrics = scm.countMetrics

	case "sum":
		metrics = scm.sumMetrics

	case "bucket":
		metrics = scm.bucketMetrics
	}

	return metrics
}

func (scm *snapshotControllerMetrics) showMetricsFailure(metricKey string) {
	framework.Logf("failed to find metric key %s inside of the following metrics:", metricKey)

	metrics := scm.getMetricsTable()
	for k, v := range metrics {
		framework.Logf("%s: %v", k, v)
	}
}

func (scm *snapshotControllerMetrics) grabSnapshotControllerMetrics(ctx context.Context) error {
	// 拉取所有 metrics
	metrics, err := scm.metricsGrabber.GrabFromSnapshotController(ctx, framework.TestContext.SnapshotControllerPodName, framework.TestContext.SnapshotControllerHTTPPort)
	if err != nil {
		return err
	}

	for method, samples := range metrics {

		for _, sample := range samples {
			operationName := string(sample.Metric["operation_name"])
			driverName := string(sample.Metric["driver_name"])
			operationStatus := string(sample.Metric["operation_status"])
			snapshotType := string(sample.Metric["snapshot_type"])
			le := string(sample.Metric["le"])
			key := snapshotMetricKey(scm.cfg.metricName, driverName, operationName, operationStatus, snapshotType, le)

			switch method {
			case "snapshot_controller_operation_total_seconds_count":
				for _, sample := range samples {
					scm.countMetrics[key] = float64(sample.Value)
				}

			case "snapshot_controller_operation_total_seconds_sum":
				for _, sample := range samples {
					scm.sumMetrics[key] = float64(sample.Value)
				}

			case "snapshot_controller_operation_total_seconds_bucket":
				for _, sample := range samples {
					scm.bucketMetrics[key] = float64(sample.Value)
				}
			}
		}
	}

	return nil
}

func (scm *snapshotControllerMetrics) getMetricKey() string {
	return snapshotMetricKey(scm.cfg.metricName, scm.cfg.driverName, scm.cfg.operationName, scm.cfg.operationStatus, scm.cfg.snapshotType, scm.cfg.le)
}

func snapshotMetricKey(metricName, driverName, operationName, operationStatus, snapshotType, le string) string {
	key := driverName

	// 构造用于简化 metrics 存储的 key
	for _, s := range []string{metricName, operationName, operationStatus, snapshotType, le} {
		if s != "" {
			key = fmt.Sprintf("%s_%s", key, s)
		}
	}

	return key
}
