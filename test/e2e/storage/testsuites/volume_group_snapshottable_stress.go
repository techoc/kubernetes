/*
Copyright 2025 The Kubernetes Authors.

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

// 这个 suite 在压力条件下测试 volume group snapshot。

package testsuites

import (
	"context"
	"fmt"
	"sync"

	"github.com/onsi/ginkgo/v2"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	errors "k8s.io/apimachinery/pkg/util/errors"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2estatefulset "k8s.io/kubernetes/test/e2e/framework/statefulset"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	storageutils "k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

type volumeGroupSnapshottableStressTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

type volumeGroupSnapshottableStressTest struct {
	config        *storageframework.PerTestConfig
	testOptions   storageframework.VolumeGroupSnapshotStressTestOptions
	driverCleanup func()

	statefulSet     *appsv1.StatefulSet
	volumeResources []*storageframework.VolumeResource
	groupSnapshots  []*storageframework.VolumeGroupSnapshotResource
	// 因为我们并行地在 goroutine 中追加 snapshot 资源。
	groupSnapshotsMutex sync.Mutex

	// 停止并等待所有异步例程。
	wg sync.WaitGroup
}

// InitCustomVolumeGroupSnapshottableStressTestSuite 返回 volumeGroupSnapshottableStressTestSuite，
// 它使用自定义测试模式实现 TestSuite 接口
func InitCustomVolumeGroupSnapshottableStressTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &volumeGroupSnapshottableStressTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "volumegroupsnapshottable-stress",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
			TestTags: []interface{}{feature.VolumeGroupSnapshotDataSource},
		},
	}
}

// InitVolumeGroupSnapshottableStressTestSuite 返回 volumeGroupSnapshottableStressTestSuite，
// 它使用测试套件默认模式实现 TestSuite 接口
func InitVolumeGroupSnapshottableStressTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.VolumeGroupSnapshotDelete,
	}
	return InitCustomVolumeGroupSnapshottableStressTestSuite(patterns)
}

func (t *volumeGroupSnapshottableStressTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *volumeGroupSnapshottableStressTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	driverInfo := driver.GetDriverInfo()
	var ok bool
	if driverInfo.VolumeGroupSnapshotStressTestOptions == nil {
		return fmt.Sprintf("Driver %s doesn't specify volume group snapshot stress test options", driverInfo.Name)
	}
	if driverInfo.VolumeGroupSnapshotStressTestOptions.NumPods <= 0 {
		framework.Failf("NumPods in volume group snapshot stress test options must be a positive integer, received: %d", driverInfo.VolumeGroupSnapshotStressTestOptions.NumPods)
	}
	if driverInfo.VolumeGroupSnapshotStressTestOptions.NumSnapshots <= 0 {
		framework.Failf("NumSnapshots in volume group snapshot stress test options must be a positive integer, received: %d", driverInfo.VolumeGroupSnapshotStressTestOptions.NumSnapshots)
	}
	_, ok = driver.(storageframework.VolumeGroupSnapshottableTestDriver)
	if !driverInfo.Capabilities[storageframework.CapVolumeGroupSnapshot] || !ok {
		return fmt.Sprintf("Driver %q doesn't implement VolumeGroupSnapshottableTestDriver", driverInfo.Name)
	}

	_, ok = driver.(storageframework.DynamicPVTestDriver)
	if !ok {
		return fmt.Sprintf("Driver %s doesn't implement DynamicPVTestDriver", driverInfo.Name)
	}
	return ""
}

func (t *volumeGroupSnapshottableStressTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	var (
		driverInfo                     *storageframework.DriverInfo
		volumeGroupSnapshottableDriver storageframework.VolumeGroupSnapshottableTestDriver
		cs                             clientset.Interface
		stressTest                     *volumeGroupSnapshottableStressTest
	)

	labelKey := "group"
	labelValue := "test-group-stress"

	// 注意它还会注册一个 AfterEach，使 f 不可用。任何使用
	// f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewDefaultFramework("volumegroupsnapshottable-stress")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		driverInfo = driver.GetDriverInfo()
		volumeGroupSnapshottableDriver, _ = driver.(storageframework.VolumeGroupSnapshottableTestDriver)
		cs = f.ClientSet
		config := driver.PrepareTest(ctx, f)

		stressTest = &volumeGroupSnapshottableStressTest{
			config:          config,
			volumeResources: []*storageframework.VolumeResource{},
			groupSnapshots:  []*storageframework.VolumeGroupSnapshotResource{},
			testOptions:     *driverInfo.VolumeGroupSnapshotStressTestOptions,
		}
	}

	createStatefulSetAndVolumes := func(ctx context.Context) {
		// 创建包含 storage class 的 volume 资源
		volumeResource := storageframework.CreateVolumeResource(ctx, driver, stressTest.config, pattern, t.GetTestSuiteInfo().SupportedSizeRange)
		stressTest.volumeResources = append(stressTest.volumeResources, volumeResource)

		// 创建带 volumeClaimTemplates 的 StatefulSet
		// StatefulSet 名称必须 ≤52 字符，这样自动生成的 controller-revision-hash
		// 标签（格式："<name>-<10-char-hash>"）不会超出 Kubernetes 63 字节标签限制。
		statefulSetName := fmt.Sprintf("vgs-stress-%s", f.Namespace.Name)
		replicas := int32(stressTest.testOptions.NumPods)

		statefulSet := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      statefulSetName,
				Namespace: f.Namespace.Name,
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": statefulSetName,
					},
				},
				PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": statefulSetName,
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "test-container",
								Image:   e2epod.GetDefaultTestImage(),
								Command: []string{"sleep", "3600"},
								VolumeMounts: []v1.VolumeMount{
									{
										Name:      "data",
										MountPath: "/mnt/data",
									},
								},
							},
						},
					},
				},
				VolumeClaimTemplates: []v1.PersistentVolumeClaim{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "data",
							Labels: map[string]string{
								labelKey: labelValue,
							},
						},
						Spec: v1.PersistentVolumeClaimSpec{
							AccessModes: []v1.PersistentVolumeAccessMode{
								v1.ReadWriteOnce,
							},
							Resources: v1.VolumeResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceStorage: resource.MustParse(t.GetTestSuiteInfo().SupportedSizeRange.Min),
								},
							},
							StorageClassName: &volumeResource.Sc.Name,
						},
					},
				},
			},
		}

		var err error
		stressTest.statefulSet, err = cs.AppsV1().StatefulSets(f.Namespace.Name).Create(ctx, statefulSet, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create StatefulSet")

		// 等待 StatefulSet 就绪
		framework.Logf("Waiting for StatefulSet %s to be ready with %d replicas", statefulSetName, replicas)
		e2estatefulset.WaitForRunningAndReady(ctx, cs, replicas, stressTest.statefulSet)
		framework.Logf("StatefulSet %s is ready", statefulSetName)
	}

	cleanup := func(ctx context.Context) {
		framework.Logf("Stopping and waiting for all test routines to finish")
		stressTest.wg.Wait()

		var (
			errs []error
			mu   sync.Mutex
			wg   sync.WaitGroup
		)

		// 先清理 StatefulSet
		if stressTest.statefulSet != nil {
			framework.Logf("Deleting StatefulSet %s", stressTest.statefulSet.Name)
			e2estatefulset.DeleteAllStatefulSets(ctx, cs, stressTest.statefulSet.Namespace)
		}

		// 清理 group snapshot
		wg.Add(len(stressTest.groupSnapshots))
		for _, groupSnapshot := range stressTest.groupSnapshots {
			go func(groupSnapshot *storageframework.VolumeGroupSnapshotResource) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()

				framework.Logf("Deleting volume group snapshot %s/%s", groupSnapshot.VGS.GetNamespace(), groupSnapshot.VGS.GetName())
				err := groupSnapshot.CleanupResource(ctx, f.Timeouts)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}(groupSnapshot)
		}
		wg.Wait()

		// 清理 volume
		wg.Add(len(stressTest.volumeResources))
		for _, volume := range stressTest.volumeResources {
			go func(volume *storageframework.VolumeResource) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()

				framework.Logf("Deleting volume %s", volume.Pvc.GetName())
				err := volume.CleanupResource(ctx)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}(volume)
		}
		wg.Wait()

		errs = append(errs, storageutils.TryFunc(stressTest.driverCleanup))

		framework.ExpectNoError(errors.NewAggregate(errs), "while cleaning up resources")
	}

	f.It("should support volume group snapshotting of StatefulSet volumes repeatedly", f.WithSlow(), f.WithSerial(), func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)
		createStatefulSetAndVolumes(ctx)

		// 反复创建 volume group snapshot
		for i := 0; i < stressTest.testOptions.NumSnapshots; i++ {
			stressTest.wg.Add(1)

			go func(snapshotIndex int) {
				defer ginkgo.GinkgoRecover()
				defer stressTest.wg.Done()

				select {
				case <-ctx.Done():
					return
				default:
					framework.Logf("Creating volume group snapshot iteration %d/%d", snapshotIndex, stressTest.testOptions.NumSnapshots-1)

					parameters := map[string]string{"deletionPolicy": pattern.SnapshotDeletionPolicy.String()}
					groupSnapshot := storageframework.CreateVolumeGroupSnapshotResource(
						ctx,
						volumeGroupSnapshottableDriver,
						stressTest.config,
						pattern,
						labelValue,
						f.Namespace.Name,
						f.Timeouts,
						parameters,
					)

					stressTest.groupSnapshotsMutex.Lock()
					defer stressTest.groupSnapshotsMutex.Unlock()
					stressTest.groupSnapshots = append(stressTest.groupSnapshots, groupSnapshot)
					framework.Logf("Successfully created volume group snapshot iteration %d", snapshotIndex)
				}
			}(i)
		}

		stressTest.wg.Wait()
		framework.Logf("Successfully completed %d volume group snapshot iterations", stressTest.testOptions.NumSnapshots)
	})
}
