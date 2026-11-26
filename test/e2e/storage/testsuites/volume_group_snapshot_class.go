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

package testsuites

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

// 默认 VolumeGroupSnapshotClass 选择失败时，group-snapshot sidecar controller
// 发出的错误子串。来源：
// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/common-controller/groupsnapshot_controller_helper.go
const (
	errNoDefaultVGSClass         = "cannot find default group snapshot class"
	errMultipleDefaultVGSClasses = "default snapshot classes were found"

	defaultVGSClassAnnotationKey = "groupsnapshot.storage.kubernetes.io/is-default-class"
)

// VolumeGroupSnapshotClassTestSuite 测试默认 class 选择行为：
// controller 在 VolumeGroupSnapshot 未指定 className 时是否正确选择默认 class，
// 以及是否拒绝歧义配置（无默认或多个默认）。
type VolumeGroupSnapshotClassTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitVolumeGroupSnapshotClassTestSuite 初始化
// VolumeGroupSnapshotClass 默认 class 选择测试的测试套件。
func InitVolumeGroupSnapshotClassTestSuite() storageframework.TestSuite {
	return &VolumeGroupSnapshotClassTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "volumegroupsnapshotclass",
			// 单个模式就足够：这些测试只验证 class 选择
			// 逻辑，不依赖 snapshot 删除策略。
			TestPatterns: []storageframework.TestPattern{
				storageframework.VolumeGroupSnapshotDelete,
			},
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
			TestTags: []interface{}{feature.VolumeGroupSnapshotDataSource},
		},
	}
}

// GetTestSuiteInfo 返回测试套件信息。
func (s *VolumeGroupSnapshotClassTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return s.tsInfo
}

// SkipUnsupportedTests 在 driver 不支持 group snapshot 时跳过测试。
func (s *VolumeGroupSnapshotClassTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	dInfo := driver.GetDriverInfo()
	_, ok := driver.(storageframework.VolumeGroupSnapshottableTestDriver)
	if !dInfo.Capabilities[storageframework.CapVolumeGroupSnapshot] || !ok {
		return fmt.Sprintf("Driver %q does not support group snapshots", dInfo.Name)
	}
	return ""
}

// DefineTests 为 VolumeGroupSnapshotClass 默认 class 选择定义测试用例。
func (s *VolumeGroupSnapshotClassTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	labelKey := "pvc-group"
	labelValue := "test-vgsclass"

	f := framework.NewDefaultFramework("volumegroupsnapshotclass")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Describe("VolumeGroupSnapshotClass", func() {
		var (
			snapshottableDriver storageframework.VolumeGroupSnapshottableTestDriver
			cs                  clientset.Interface
			config              *storageframework.PerTestConfig
		)

		ginkgo.BeforeEach(func(ctx context.Context) {
			snapshottableDriver = driver.(storageframework.VolumeGroupSnapshottableTestDriver)
			cs = f.ClientSet
			config = driver.PrepareTest(ctx, f)
		})

		// createPVCsWithPod 创建两个带标签的 PVC 和一个挂载它们的 pod，
		// 确保 PVC 进入 Bound 状态。
		createPVCsWithPod := func(ctx context.Context) (*storageframework.VolumeResource, *storageframework.VolumeResource, *v1.Pod) {
			ginkgo.By("creating two PVCs with labels for the VGS selector")
			vr1 := storageframework.CreateVolumeResource(ctx, driver, config, pattern, s.GetTestSuiteInfo().SupportedSizeRange)
			vr2 := storageframework.CreateVolumeResource(ctx, driver, config, pattern, s.GetTestSuiteInfo().SupportedSizeRange)
			for _, vr := range []*storageframework.VolumeResource{vr1, vr2} {
				patchData := fmt.Appendf(nil, `{"metadata":{"labels":{"%s":"%s"}}}`, labelKey, labelValue)
				updatedPVC, err := cs.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Patch(ctx, vr.Pvc.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
				framework.ExpectNoError(err, "failed to add label to PVC %s", vr.Pvc.Name)
				vr.Pvc = updatedPVC
			}

			ginkgo.By("creating a pod to bind the PVCs")
			podConfig := e2epod.Config{
				NS:           f.Namespace.Name,
				PVCs:         []*v1.PersistentVolumeClaim{vr1.Pvc, vr2.Pvc},
				SeLinuxLabel: e2epv.SELinuxLabel,
			}
			pod, err := e2epod.MakeSecPod(&podConfig)
			framework.ExpectNoError(err, "failed to make pod config")
			pod, err = cs.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
			framework.ExpectNoError(err, "failed to create pod")
			framework.ExpectNoError(
				e2epod.WaitTimeoutForPodRunningInNamespace(ctx, cs, pod.Name, pod.Namespace, f.Timeouts.PodStartSlow),
				"pod did not become running in time",
			)
			return vr1, vr2, pod
		}

		// createDefaultVGSClass 创建带默认
		// 注解的 VolumeGroupSnapshotClass，并注册 DeferCleanup 删除它。
		createDefaultVGSClass := func(ctx context.Context) *unstructured.Unstructured {
			vgsclassSpec := snapshottableDriver.GetVolumeGroupSnapshotClass(ctx, config, map[string]string{"deletionPolicy": "Delete"})
			annotations := vgsclassSpec.GetAnnotations()
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[defaultVGSClassAnnotationKey] = "true"
			vgsclassSpec.SetAnnotations(annotations)
			vgsclass, err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotClassGVR).Create(ctx, vgsclassSpec, metav1.CreateOptions{})
			framework.ExpectNoError(err, "failed to create VolumeGroupSnapshotClass")
			ginkgo.DeferCleanup(func(ctx context.Context) {
				err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotClassGVR).Delete(ctx, vgsclass.GetName(), metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					framework.ExpectNoError(err, "failed to delete VolumeGroupSnapshotClass %s", vgsclass.GetName())
				}
			})
			return vgsclass
		}

		// createVGSWithoutClassName 创建一个不指定
		// volumeGroupSnapshotClassName 的 VolumeGroupSnapshot，并注册 DeferCleanup 删除它。
		createVGSWithoutClassName := func(ctx context.Context) *unstructured.Unstructured {
			vgs, err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotGVR).Namespace(f.Namespace.Name).Create(ctx,
				storageframework.GetVolumeGroupSnapshot(f.Namespace.Name, map[string]interface{}{labelKey: labelValue}, ""),
				metav1.CreateOptions{})
			framework.ExpectNoError(err, "failed to create VolumeGroupSnapshot")
			ginkgo.DeferCleanup(func(ctx context.Context) {
				err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotGVR).Namespace(f.Namespace.Name).Delete(ctx, vgs.GetName(), metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					framework.ExpectNoError(err, "failed to delete VolumeGroupSnapshot %s", vgs.GetName())
				}
			})
			return vgs
		}

		// removeAndRestoreDefaultVGSClasses 移除同一 CSI driver 下已有
		// VolumeGroupSnapshotClass 的默认注解，并在测试后通过
		// DeferCleanup 恢复它们。这样避免干扰
		// 默认 class 选择测试。
		removeAndRestoreDefaultVGSClasses := func(ctx context.Context) {
			driverName := driver.GetDriverInfo().Name
			list, err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotClassGVR).List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err, "failed to list VolumeGroupSnapshotClasses")

			for _, item := range list.Items {
				classDriver, _, _ := unstructured.NestedString(item.Object, "driver")
				if classDriver != driverName {
					continue
				}
				if item.GetAnnotations()[defaultVGSClassAnnotationKey] != "true" {
					continue
				}
				name := item.GetName()
				framework.Logf("removing default annotation from VolumeGroupSnapshotClass %s for test isolation", name)
				removePatch := []byte(`{"metadata":{"annotations":{"groupsnapshot.storage.kubernetes.io/is-default-class":null}}}`)
				_, err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotClassGVR).Patch(ctx, name, types.MergePatchType, removePatch, metav1.PatchOptions{})
				framework.ExpectNoError(err, "failed to remove default annotation from VolumeGroupSnapshotClass %s", name)
				ginkgo.DeferCleanup(func(ctx context.Context) {
					framework.Logf("restoring default annotation on VolumeGroupSnapshotClass %s", name)
					restorePatch := []byte(`{"metadata":{"annotations":{"groupsnapshot.storage.kubernetes.io/is-default-class":"true"}}}`)
					_, err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotClassGVR).Patch(ctx, name, types.MergePatchType, restorePatch, metav1.PatchOptions{})
					framework.ExpectNoError(err, "failed to restore default annotation on VolumeGroupSnapshotClass %s", name)
				})
			}
		}

		// expectVolumeGroupSnapshotClassName 获取指定名称的 VolumeGroupSnapshot，
		// 并断言 spec.volumeGroupSnapshotClassName 等于 expectedClassName。
		// 传 nil 断言该字段不存在。
		// 传字符串指针断言该字段恰好等于该值，
		// 包括 "" 这一边界情况：VolumeGroupSnapshotClass 带有
		// "groupsnapshot.storage.kubernetes.io/is-default-class"="" 注解。
		expectVolumeGroupSnapshotClassName := func(ctx context.Context, vgsName string, expectedClassName *string) {
			ginkgo.GinkgoHelper()
			vgs, err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotGVR).Namespace(f.Namespace.Name).Get(ctx, vgsName, metav1.GetOptions{})
			framework.ExpectNoError(err, "failed to get VolumeGroupSnapshot %s", vgsName)
			spec, ok := vgs.Object["spec"].(map[string]interface{})
			if !ok {
				ginkgo.Fail("failed to get VolumeGroupSnapshot spec: spec field is not a map[string]interface{}")
			}
			if expectedClassName == nil {
				gomega.Expect(spec["volumeGroupSnapshotClassName"]).To(gomega.BeNil(),
					"VolumeGroupSnapshot %s spec.volumeGroupSnapshotClassName should not be set by the controller", vgsName)
			} else {
				gomega.Expect(spec["volumeGroupSnapshotClassName"]).To(gomega.Equal(*expectedClassName),
					"VolumeGroupSnapshot %s spec.volumeGroupSnapshotClassName should be %q", vgsName, *expectedClassName)
			}
		}

		f.It("should use default VolumeGroupSnapshotClass when no className is specified", f.WithSerial(), func(ctx context.Context) {
			removeAndRestoreDefaultVGSClasses(ctx)

			vr1, vr2, pod := createPVCsWithPod(ctx)
			ginkgo.DeferCleanup(func(ctx context.Context) {
				framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, pod))
				framework.ExpectNoError(vr1.CleanupResource(ctx))
				framework.ExpectNoError(vr2.CleanupResource(ctx))
			})

			ginkgo.By("creating a VolumeGroupSnapshotClass with the default annotation")
			vgsclass := createDefaultVGSClass(ctx)

			ginkgo.By("creating a VolumeGroupSnapshot without specifying volumeGroupSnapshotClassName")
			vgs := createVGSWithoutClassName(ctx)

			ginkgo.DeferCleanup(func(ctx context.Context) {
				sr := &storageframework.VolumeGroupSnapshotResource{
					Config:  config,
					Pattern: storageframework.VolumeGroupSnapshotDelete,
					VGS:     vgs,
				}
				framework.ExpectNoError(sr.CleanupResource(ctx, f.Timeouts), "failed to clean up VolumeGroupSnapshot resources")
			})

			ginkgo.By("verifying the VolumeGroupSnapshot becomes ready using the default class")
			framework.ExpectNoError(utils.WaitForVolumeGroupSnapshotReady(ctx, f.DynamicClient, f.Namespace.Name, vgs.GetName(), framework.Poll, f.Timeouts.SnapshotCreate))

			ginkgo.By("verifying the default VolumeGroupSnapshotClass name was written back to the VolumeGroupSnapshot spec")
			expectedClassName := vgsclass.GetName()
			expectVolumeGroupSnapshotClassName(ctx, vgs.GetName(), &expectedClassName)
		})

		f.It("should report error when VolumeGroupSnapshot is created without className and no default class exists", f.WithSerial(), func(ctx context.Context) {
			removeAndRestoreDefaultVGSClasses(ctx)

			vr1, vr2, pod := createPVCsWithPod(ctx)
			ginkgo.DeferCleanup(func(ctx context.Context) {
				framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, pod))
				framework.ExpectNoError(vr1.CleanupResource(ctx))
				framework.ExpectNoError(vr2.CleanupResource(ctx))
			})

			ginkgo.By("creating a VolumeGroupSnapshot without className and without any default VolumeGroupSnapshotClass")
			vgs := createVGSWithoutClassName(ctx)

			ginkgo.By("verifying the VolumeGroupSnapshot enters an error state")
			framework.ExpectNoError(waitForVolumeGroupSnapshotError(ctx, f.DynamicClient, f.Namespace.Name, vgs.GetName(), framework.Poll, f.Timeouts.SnapshotCreate, errNoDefaultVGSClass))

			ginkgo.By("verifying spec.volumeGroupSnapshotClassName was not set by the controller")
			expectVolumeGroupSnapshotClassName(ctx, vgs.GetName(), nil)
		})

		f.It("should report error when VolumeGroupSnapshot is created without className and multiple default classes exist", f.WithSerial(), func(ctx context.Context) {
			removeAndRestoreDefaultVGSClasses(ctx)

			vr1, vr2, pod := createPVCsWithPod(ctx)
			ginkgo.DeferCleanup(func(ctx context.Context) {
				framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, pod))
				framework.ExpectNoError(vr1.CleanupResource(ctx))
				framework.ExpectNoError(vr2.CleanupResource(ctx))
			})

			ginkgo.By("creating two VolumeGroupSnapshotClasses both with the default annotation")
			createDefaultVGSClass(ctx)
			createDefaultVGSClass(ctx)

			ginkgo.By("creating a VolumeGroupSnapshot without specifying className")
			vgs := createVGSWithoutClassName(ctx)

			ginkgo.By("verifying the VolumeGroupSnapshot enters an error state due to multiple default classes")
			framework.ExpectNoError(waitForVolumeGroupSnapshotError(ctx, f.DynamicClient, f.Namespace.Name, vgs.GetName(), framework.Poll, f.Timeouts.SnapshotCreate, errMultipleDefaultVGSClasses))

			ginkgo.By("verifying spec.volumeGroupSnapshotClassName was not set by the controller")
			expectVolumeGroupSnapshotClassName(ctx, vgs.GetName(), nil)
		})
	})
}

// waitForVolumeGroupSnapshotError 轮询直到给定 VolumeGroupSnapshot 的
// status 中出现非 nil 的 error 字段，然后立即断言消息
// 包含 expectedErrSubstr。分成两个阶段确保 controller 的消息
// 变化会直接导致断言失败，而不是静默
// 超时。用包级错误常量作为 expectedErrSubstr。
func waitForVolumeGroupSnapshotError(ctx context.Context, dc dynamic.Interface, ns, name string, poll, timeout time.Duration, expectedErrSubstr string) error {
	framework.Logf("Waiting up to %v for VolumeGroupSnapshot %s/%s to enter an error state", timeout, ns, name)
	var errorMessage string
	if successful := utils.WaitUntil(poll, timeout, func() bool {
		vgs, err := dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Failed to get VolumeGroupSnapshot %q, retrying: %v", name, err)
			return false
		}
		status, ok := vgs.Object["status"].(map[string]interface{})
		if !ok || status == nil {
			return false
		}
		errObj, ok := status["error"].(map[string]interface{})
		if !ok || errObj == nil {
			return false
		}
		errorMessage, _ = errObj["message"].(string)
		return true
	}); !successful {
		return fmt.Errorf("VolumeGroupSnapshot %s/%s did not enter an error state within %v", ns, name, timeout)
	}

	gomega.Expect(errorMessage).To(gomega.ContainSubstring(expectedErrSubstr),
		"VolumeGroupSnapshot %s/%s has error status but message %q does not contain expected substring %q",
		ns, name, errorMessage, expectedErrSubstr)
	return nil
}
