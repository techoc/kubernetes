/*
Copyright 2018 The Kubernetes Authors.

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
	"strconv"
	"strings"
	"sync"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	storageutils "k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

// StorageClassTest 表示供给测试使用的参数。
// 并非所有测试都会用到全部参数。
type StorageClassTest struct {
	Client               clientset.Interface
	Timeouts             *framework.TimeoutContext
	Claim                *v1.PersistentVolumeClaim
	SourceClaim          *v1.PersistentVolumeClaim
	Class                *storagev1.StorageClass
	Name                 string
	CloudProviders       []string
	Provisioner          string
	Parameters           map[string]string
	DelayBinding         bool
	ClaimSize            string
	ExpectedSize         string
	PvCheck              func(ctx context.Context, claim *v1.PersistentVolumeClaim)
	VolumeMode           v1.PersistentVolumeMode
	AllowVolumeExpansion bool
	NodeSelection        e2epod.NodeSelection
	MountOptions         []string
	ReclaimPolicy        *v1.PersistentVolumeReclaimPolicy
}

type provisioningTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitCustomProvisioningTestSuite 返回实现 TestSuite 接口的 provisioningTestSuite，
// 使用自定义测试模式
func InitCustomProvisioningTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &provisioningTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "provisioning",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
		},
	}
}

// InitProvisioningTestSuite 返回实现 TestSuite 接口的 provisioningTestSuite，
// 使用测试套件的默认模式
func InitProvisioningTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DefaultFsDynamicPV,
		storageframework.BlockVolModeDynamicPV,
		storageframework.NtfsDynamicPV,
	}
	return InitCustomProvisioningTestSuite(patterns)
}

func (p *provisioningTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return p.tsInfo
}

func (p *provisioningTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	// 检查前置条件。
	if pattern.VolType != storageframework.DynamicPV {
		return fmt.Sprintf("Suite %q does not support %v", p.tsInfo.Name, pattern.VolType)
	}
	dInfo := driver.GetDriverInfo()
	if pattern.VolMode == v1.PersistentVolumeBlock && !dInfo.Capabilities[storageframework.CapBlock] {
		return fmt.Sprintf("Driver %s doesn't support %v", dInfo.Name, pattern.VolMode)
	}
	return ""
}

func (p *provisioningTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		testCase  *StorageClassTest
		cs        clientset.Interface
		pvc       *v1.PersistentVolumeClaim
		sourcePVC *v1.PersistentVolumeClaim
		sc        *storagev1.StorageClass

		migrationCheck *migrationOpCheck
	}
	var (
		dInfo   = driver.GetDriverInfo()
		dDriver storageframework.DynamicPVTestDriver
		l       local
	)

	// 注意，它还会注册一个 AfterEach，使 f 失效。任何使用 f 的代码
	// 都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("provisioning", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		l = local{}
		dDriver, _ = driver.(storageframework.DynamicPVTestDriver)
		// 现在执行开销较大的测试初始化。
		l.config = driver.PrepareTest(ctx, f)
		l.migrationCheck = newMigrationOpCheck(ctx, f.ClientSet, f.ClientConfig(), dInfo.InTreePluginName)
		ginkgo.DeferCleanup(l.migrationCheck.validateMigrationVolumeOpCounts)
		l.cs = l.config.Framework.ClientSet
		testVolumeSizeRange := p.GetTestSuiteInfo().SupportedSizeRange
		driverVolumeSizeRange := dDriver.GetDriverInfo().SupportedSizeRange
		claimSize, err := storageutils.GetSizeRangesIntersection(testVolumeSizeRange, driverVolumeSizeRange)
		framework.ExpectNoError(err, "determine intersection of test size range %+v and driver size range %+v", testVolumeSizeRange, driverVolumeSizeRange)

		l.sc = dDriver.GetDynamicProvisionStorageClass(ctx, l.config, pattern.FsType)
		if l.sc == nil {
			e2eskipper.Skipf("Driver %q does not define Dynamic Provision StorageClass - skipping", dInfo.Name)
		}
		l.pvc = e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
			ClaimSize:        claimSize,
			StorageClassName: &(l.sc.Name),
			VolumeMode:       &pattern.VolMode,
		}, l.config.Framework.Namespace.Name)
		l.sourcePVC = e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
			ClaimSize:        claimSize,
			StorageClassName: &(l.sc.Name),
			VolumeMode:       &pattern.VolMode,
		}, l.config.Framework.Namespace.Name)
		framework.Logf("In creating storage class object and pvc objects for driver - sc: %v, pvc: %v, src-pvc: %v", l.sc, l.pvc, l.sourcePVC)
		l.testCase = &StorageClassTest{
			Client:        l.config.Framework.ClientSet,
			Timeouts:      f.Timeouts,
			Claim:         l.pvc,
			SourceClaim:   l.sourcePVC,
			Class:         l.sc,
			Provisioner:   l.sc.Provisioner,
			ClaimSize:     claimSize,
			ExpectedSize:  claimSize,
			VolumeMode:    pattern.VolMode,
			NodeSelection: l.config.ClientNodeSelection,
		}
	}

	ginkgo.It("should provision storage with mount options", func(ctx context.Context) {
		if dInfo.SupportedMountOption == nil {
			e2eskipper.Skipf("Driver %q does not define supported mount option - skipping", dInfo.Name)
		}
		if pattern.VolMode == v1.PersistentVolumeBlock {
			e2eskipper.Skipf("Block volumes do not support mount options - skipping")
		}

		init(ctx)

		l.testCase.Class.MountOptions = dInfo.SupportedMountOption.Union(dInfo.RequiredMountOption).List()
		l.testCase.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
			PVWriteReadSingleNodeCheck(ctx, l.cs, f.Timeouts, claim, l.config.ClientNodeSelection)
		}
		SetupStorageClass(ctx, l.testCase.Client, l.testCase.Class)

		l.testCase.TestDynamicProvisioning(ctx)
	})

	f.It("should provision storage with snapshot data source", feature.VolumeSnapshotDataSource, func(ctx context.Context) {
		if !dInfo.Capabilities[storageframework.CapSnapshotDataSource] {
			e2eskipper.Skipf("Driver %q does not support populating data from snapshot - skipping", dInfo.Name)
		}
		if !dInfo.SupportedFsType.Has(pattern.FsType) {
			e2eskipper.Skipf("Driver %q does not support %q fs type - skipping", dInfo.Name, pattern.FsType)
		}

		sDriver, ok := driver.(storageframework.SnapshottableTestDriver)
		if !ok {
			framework.Failf("Driver %q has CapSnapshotDataSource but does not implement SnapshottableTestDriver", dInfo.Name)
		}

		init(ctx)

		dc := l.config.Framework.DynamicClient
		testConfig := storageframework.ConvertTestConfig(l.config)
		expectedContent := fmt.Sprintf("Hello from namespace %s", f.Namespace.Name)
		dataSourceRef := prepareSnapshotDataSourceForProvisioning(ctx, f, testConfig, l.config, pattern, l.cs, dc, l.pvc, l.sc, sDriver, pattern.VolMode, expectedContent)

		l.pvc.Spec.DataSourceRef = dataSourceRef
		l.testCase.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
			ginkgo.By("checking whether the created volume has the pre-populated data")
			tests := []e2evolume.Test{
				{
					Volume:          *storageutils.CreateVolumeSource(claim.Name, false /* readOnly */),
					Mode:            pattern.VolMode,
					File:            "index.html",
					ExpectedContent: expectedContent,
				},
			}
			e2evolume.TestVolumeClientSlow(ctx, f, testConfig, nil, "", tests)
		}
		l.testCase.TestDynamicProvisioning(ctx)
	})

	f.It("should provision storage with snapshot data source (ROX mode)", feature.VolumeSnapshotDataSource, func(ctx context.Context) {
		if !dInfo.Capabilities[storageframework.CapSnapshotDataSource] {
			e2eskipper.Skipf("Driver %q does not support populating data from snapshot - skipping", dInfo.Name)
		}
		if !dInfo.SupportedFsType.Has(pattern.FsType) {
			e2eskipper.Skipf("Driver %q does not support %q fs type - skipping", dInfo.Name, pattern.FsType)
		}
		if !dInfo.Capabilities[storageframework.CapReadOnlyMany] {
			e2eskipper.Skipf("Driver %q does not support ROX access mode - skipping", dInfo.Name)
		}

		sDriver, ok := driver.(storageframework.SnapshottableTestDriver)
		if !ok {
			framework.Failf("Driver %q has CapSnapshotDataSource but does not implement SnapshottableTestDriver", dInfo.Name)
		}

		init(ctx)

		dc := l.config.Framework.DynamicClient
		testConfig := storageframework.ConvertTestConfig(l.config)
		expectedContent := fmt.Sprintf("Hello from namespace %s", f.Namespace.Name)
		dataSourceRef := prepareSnapshotDataSourceForProvisioning(ctx, f, testConfig, l.config, pattern, l.cs, dc, l.pvc, l.sc, sDriver, pattern.VolMode, expectedContent)

		l.pvc.Spec.DataSourceRef = dataSourceRef
		l.pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{
			v1.PersistentVolumeAccessMode(v1.ReadOnlyMany),
		}
		l.testCase.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
			ginkgo.By("checking whether the created volume has the pre-populated data")
			tests := []e2evolume.Test{
				{
					Volume:          *storageutils.CreateVolumeSource(claim.Name, false /* readOnly */),
					Mode:            pattern.VolMode,
					File:            "index.html",
					ExpectedContent: expectedContent,
				},
			}
			e2evolume.TestVolumeClientSlow(ctx, f, testConfig, nil, "", tests)
		}
		l.testCase.TestDynamicProvisioning(ctx)
	})

	f.It("should provision storage with any volume data source", f.WithSerial(), func(ctx context.Context) {
		if len(dInfo.InTreePluginName) != 0 {
			e2eskipper.Skipf("It only works with CSI drivers - skipping")
		}
		if pattern.VolMode == v1.PersistentVolumeBlock {
			e2eskipper.Skipf("Test for Block volumes is not implemented - skipping")
		}

		init(ctx)

		ginkgo.By("Creating validator namespace")
		valNamespace, err := f.CreateNamespace(ctx, fmt.Sprintf("%s-val", f.Namespace.Name), map[string]string{
			"e2e-framework":      f.BaseName,
			"e2e-test-namespace": f.Namespace.Name,
		})
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(f.DeleteNamespace, valNamespace.Name)

		valManifests := []string{
			"test/e2e/testing-manifests/storage-csi/any-volume-datasource/volume-data-source-validator/rbac-data-source-validator.yaml",
			"test/e2e/testing-manifests/storage-csi/any-volume-datasource/volume-data-source-validator/setup-data-source-validator.yaml",
		}

		crdManifestPath := "test/e2e/testing-manifests/storage-csi/any-volume-datasource/crd/populator.storage.k8s.io_volumepopulators.yaml"
		crdItems, err := storageutils.LoadFromManifests(crdManifestPath)
		framework.ExpectNoError(err, "Failed to load VolumePopulator CRD manifest")
		gomega.Expect(crdItems).To(gomega.HaveLen(1), "Expected exactly one CRD in manifest")

		crd, ok := crdItems[0].(*apiextensionsv1.CustomResourceDefinition)
		gomega.Expect(ok).To(gomega.BeTrueBecause("Resource in loaded manifest file is not a CustomResourceDefinition: %s", crdManifestPath))

		config, err := framework.LoadConfig()
		framework.ExpectNoError(err)
		apiExtensionClient, err := crdclientset.NewForConfig(config)
		framework.ExpectNoError(err)

		ginkgo.By(fmt.Sprintf("Checking if %s CRD exists", crd.Name))
		_, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			ginkgo.By("VolumePopulator CRD not found, test will create it and remove when done")
			valManifests = append(valManifests, crdManifestPath)
		} else if err != nil {
			framework.ExpectNoError(err, "Error checking for VolumePopulator CRD existence")
		}

		ginkgo.By("Deploying validator")
		err = storageutils.CreateFromManifests(ctx, f, valNamespace,
			func(item interface{}) error { return nil },
			valManifests...)

		framework.ExpectNoError(err)

		ginkgo.By("Creating populator namespace")
		popNamespace, err := f.CreateNamespace(ctx, fmt.Sprintf("%s-pop", f.Namespace.Name), map[string]string{
			"e2e-framework":      f.BaseName,
			"e2e-test-namespace": f.Namespace.Name,
		})
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(f.DeleteNamespace, popNamespace.Name)

		ginkgo.By("Deploying hello-populator")
		popManifests := []string{
			"test/e2e/testing-manifests/storage-csi/any-volume-datasource/crd/hello-populator-crd.yaml",
			"test/e2e/testing-manifests/storage-csi/any-volume-datasource/hello-populator-deploy.yaml",
		}
		err = storageutils.CreateFromManifests(ctx, f, popNamespace,
			func(item interface{}) error {
				switch item := item.(type) {
				case *appsv1.Deployment:
					for i, container := range item.Spec.Template.Spec.Containers {
						switch container.Name {
						case "hello":
							args := []string{}
							var foundNS, foundImage bool
							for _, arg := range container.Args {
								if strings.HasPrefix(arg, "--namespace=") {
									args = append(args, fmt.Sprintf("--namespace=%s", popNamespace.Name))
									foundNS = true
								} else if strings.HasPrefix(arg, "--image-name=") {
									args = append(args, fmt.Sprintf("--image-name=%s", container.Image))
									foundImage = true
								} else {
									args = append(args, arg)
								}
							}
							if !foundNS {
								args = append(args, fmt.Sprintf("--namespace=%s", popNamespace.Name))
								framework.Logf("container name: %s", container.Name)
							}
							if !foundImage {
								args = append(args, fmt.Sprintf("--image-name=%s", container.Image))
								framework.Logf("container image: %s", container.Image)
							}
							container.Args = args
							item.Spec.Template.Spec.Containers[i] = container
						default:
						}
					}
				}
				return nil
			},
			popManifests...)

		framework.ExpectNoError(err)

		dc := l.config.Framework.DynamicClient

		// 让 hello-populator 处理 hello.example.com 组中的 Hello 资源
		ginkgo.By("Creating VolumePopulator CR datasource")
		volumePopulatorGVR := schema.GroupVersionResource{Group: "populator.storage.k8s.io", Version: "v1beta1", Resource: "volumepopulators"}
		helloPopulatorCR := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"kind":       "VolumePopulator",
				"apiVersion": "populator.storage.k8s.io/v1beta1",
				"metadata": map[string]interface{}{
					"name": fmt.Sprintf("%s-%s", "hello-populator", f.Namespace.Name),
				},
				"sourceKind": map[string]interface{}{
					"group": "hello.example.com",
					"kind":  "Hello",
				},
			},
		}

		_, err = dc.Resource(volumePopulatorGVR).Create(ctx, helloPopulatorCR, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		defer func() {
			framework.Logf("deleting VolumePopulator CR datasource %q/%q", helloPopulatorCR.GetNamespace(), helloPopulatorCR.GetName())
			err = dc.Resource(volumePopulatorGVR).Delete(ctx, helloPopulatorCR.GetName(), metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				framework.Failf("Error deleting VolumePopulator CR datasource %q. Error: %v", helloPopulatorCR.GetName(), err)
			}
		}()

		// 创建 Hello CR 数据源
		ginkgo.By("Creating Hello CR datasource")
		helloCRName := "example-hello"
		fileName := fmt.Sprintf("example-%s.txt", f.Namespace.Name)
		expectedContent := fmt.Sprintf("Hello from namespace %s", f.Namespace.Name)
		helloGVR := schema.GroupVersionResource{Group: "hello.example.com", Version: "v1alpha1", Resource: "hellos"}
		helloCR := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"kind":       "Hello",
				"apiVersion": "hello.example.com/v1alpha1",
				"metadata": map[string]interface{}{
					"name":      helloCRName,
					"namespace": f.Namespace.Name,
				},
				"spec": map[string]interface{}{
					"fileName":     fileName,
					"fileContents": expectedContent,
				},
			},
		}

		_, err = dc.Resource(helloGVR).Namespace(f.Namespace.Name).Create(ctx, helloCR, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		defer func() {
			framework.Logf("deleting Hello CR datasource %q/%q", helloCR.GetNamespace(), helloCR.GetName())
			err = dc.Resource(helloGVR).Namespace(helloCR.GetNamespace()).Delete(ctx, helloCR.GetName(), metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				framework.Failf("Error deleting Hello CR datasource %q. Error: %v", helloCR.GetName(), err)
			}
		}()

		apiGroup := "hello.example.com"
		l.pvc.Spec.DataSourceRef = &v1.TypedObjectReference{
			APIGroup: &apiGroup,
			Kind:     "Hello",
			Name:     helloCRName,
		}

		testConfig := storageframework.ConvertTestConfig(l.config)
		l.testCase.NodeSelection = testConfig.ClientNodeSelection
		l.testCase.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
			ginkgo.By("checking whether the created volume has the pre-populated data")
			tests := []e2evolume.Test{
				{
					Volume:          *storageutils.CreateVolumeSource(claim.Name, false /* readOnly */),
					Mode:            pattern.VolMode,
					File:            fileName,
					ExpectedContent: expectedContent,
				},
			}
			e2evolume.TestVolumeClientSlow(ctx, f, testConfig, nil, "", tests)
		}

		SetupStorageClass(ctx, l.testCase.Client, l.testCase.Class)

		l.testCase.TestDynamicProvisioning(ctx)
	})

	f.It("should provision correct filesystem size when restoring snapshot to larger size pvc", feature.VolumeSnapshotDataSource, func(ctx context.Context) {

		if pattern.VolMode == "Block" {
			e2eskipper.Skipf("Test is not valid for Block volume mode - skipping")
		}

		if dInfo.Capabilities[storageframework.CapFSResizeFromSourceNotSupported] {
			e2eskipper.Skipf("Driver %q does not support filesystem resizing - skipping", dInfo.Name)
		}

		if !dInfo.Capabilities[storageframework.CapSnapshotDataSource] {
			e2eskipper.Skipf("Driver %q does not support populating data from snapshot - skipping", dInfo.Name)
		}

		if !dInfo.SupportedFsType.Has(pattern.FsType) {
			e2eskipper.Skipf("Driver %q does not support %q fs type - skipping", dInfo.Name, pattern.FsType)
		}

		sDriver, ok := driver.(storageframework.SnapshottableTestDriver)
		if !ok {
			framework.Failf("Driver %q has CapSnapshotDataSource but does not implement SnapshottableTestDriver", dInfo.Name)
		}

		init(ctx)
		pvc2 := l.pvc.DeepCopy()
		l.pvc.Name = "pvc-origin"
		dc := l.config.Framework.DynamicClient
		testConfig := storageframework.ConvertTestConfig(l.config)
		dataSourceRef := prepareSnapshotDataSourceForProvisioning(ctx, f, testConfig, l.config, pattern, l.cs, dc, l.pvc, l.sc, sDriver, pattern.VolMode, "")

		// 获取创建的 PVC，并记录 pv 的实际大小（来自 pvc status）。
		c, err := l.testCase.Client.CoreV1().PersistentVolumeClaims(l.pvc.Namespace).Get(ctx, l.pvc.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get pvc: %v", err)
		actualPVSize := c.Status.Capacity.Storage().Value()

		createdClaims := []*v1.PersistentVolumeClaim{c}
		pod, err := e2epod.CreatePod(ctx, l.testCase.Client, f.Namespace.Name, nil, createdClaims, f.NamespacePodSecurityLevel, "")
		framework.ExpectNoError(err, "Failed to create pod: %v", err)

		// 挂载路径不应为空。
		mountpath := findVolumeMountPath(pod, c)
		gomega.Expect(mountpath).ShouldNot(gomega.BeEmpty())

		// 保存原始 volume 的文件系统大小。
		originFSSize, err := getFilesystemSizeBytes(pod, mountpath)
		framework.ExpectNoError(err, "Failed to obtain filesystem size of a volume mount: %v", err)

		// 为新 PVC 请求一个大于原始 PVC 实际供给大小的尺寸。
		storageRequest := resource.NewQuantity(actualPVSize, resource.BinarySI)
		storageRequest.Add(resource.MustParse("1Gi"))
		pvc2.Spec.Resources.Requests = v1.ResourceList{
			v1.ResourceStorage: *storageRequest,
		}

		// 设置 PVC 的 snapshot 数据源。
		pvc2.Spec.DataSourceRef = dataSourceRef

		// 创建新的 claim 和一个使用该 PVC 的 Pod。
		c2, err := l.testCase.Client.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		framework.ExpectNoError(err, "Failed to create pvc: %v", err)
		createdClaims2 := []*v1.PersistentVolumeClaim{c2}
		pod2, err := e2epod.CreatePod(ctx, l.testCase.Client, f.Namespace.Name, nil, createdClaims2, f.NamespacePodSecurityLevel, "")
		framework.ExpectNoError(err, "Failed to create pod: %v", err)

		// 挂载路径不应为空。
		mountpath2 := findVolumeMountPath(pod2, c2)
		gomega.Expect(mountpath2).ShouldNot(gomega.BeEmpty())

		// 获取恢复后文件系统的实际大小。
		restoredFSSize, err := getFilesystemSizeBytes(pod2, mountpath2)
		framework.ExpectNoError(err, "Failed to obtain filesystem size of a volume mount: %v", err)

		// 恢复后 volume 的文件系统应大于原始文件系统。
		msg := fmt.Sprintf("Filesystem resize failed when restoring from snapshot to PVC with larger size. "+
			"Restored fs size: %v bytes is not larger than origin fs size: %v bytes.\n"+
			"HINT: Your driver needs to check the volume in NodeStageVolume and resize fs if needed.\n"+
			"HINT: For an example patch see: https://github.com/kubernetes/cloud-provider-openstack/pull/1563/files",
			restoredFSSize, originFSSize)
		gomega.Expect(restoredFSSize).Should(gomega.BeNumerically(">", originFSSize), msg)
	})

	ginkgo.It("should provision storage with pvc data source", func(ctx context.Context) {
		if !dInfo.Capabilities[storageframework.CapPVCDataSource] {
			e2eskipper.Skipf("Driver %q does not support cloning - skipping", dInfo.Name)
		}
		init(ctx)

		if l.config.ClientNodeSelection.Name == "" {
			// 将所有 Pod 调度到同一个 topology segment（例如云可用区），因为某些
			// driver 不支持跨 topology segment 克隆。
			if err := ensureTopologyRequirements(ctx, &l.config.ClientNodeSelection, l.cs, dInfo, 1); err != nil {
				framework.Failf("Error setting topology requirements: %v", err)
			}
		}
		testConfig := storageframework.ConvertTestConfig(l.config)
		expectedContent := fmt.Sprintf("Hello from namespace %s", f.Namespace.Name)
		dataSourceRef := preparePVCDataSourceForProvisioning(ctx, f, testConfig, l.cs, l.sourcePVC, l.sc, pattern.VolMode, expectedContent)
		l.pvc.Spec.DataSourceRef = dataSourceRef
		l.testCase.NodeSelection = testConfig.ClientNodeSelection
		l.testCase.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
			ginkgo.By("checking whether the created volume has the pre-populated data")
			tests := []e2evolume.Test{
				{
					Volume:          *storageutils.CreateVolumeSource(claim.Name, false /* readOnly */),
					Mode:            pattern.VolMode,
					File:            "index.html",
					ExpectedContent: expectedContent,
				},
			}
			e2evolume.TestVolumeClientSlow(ctx, f, testConfig, nil, "", tests)
		}
		// 如果源磁盘仍在卸载过程中，克隆会失败，因此在克隆前等待 VolumeAttachment 被移除。
		volumeAttachment := e2evolume.GetVolumeAttachmentName(ctx, f.ClientSet, testConfig, l.testCase.Provisioner, dataSourceRef.Name, l.sourcePVC.Namespace)
		framework.ExpectNoError(e2evolume.WaitForVolumeAttachmentTerminated(ctx, volumeAttachment, f.ClientSet, f.Timeouts.DataSourceProvision))
		l.testCase.TestDynamicProvisioning(ctx)
	})

	ginkgo.It("should provision storage with pvc data source (ROX mode)", func(ctx context.Context) {
		if !dInfo.Capabilities[storageframework.CapPVCDataSource] {
			e2eskipper.Skipf("Driver %q does not support cloning - skipping", dInfo.Name)
		}
		if !dInfo.Capabilities[storageframework.CapReadOnlyMany] {
			e2eskipper.Skipf("Driver %q does not support ROX access mode - skipping", dInfo.Name)
		}
		init(ctx)

		if l.config.ClientNodeSelection.Name == "" {
			// 将所有 Pod 调度到同一个 topology segment（例如云可用区），因为某些
			// driver 不支持跨 topology segment 克隆。
			if err := ensureTopologyRequirements(ctx, &l.config.ClientNodeSelection, l.cs, dInfo, 1); err != nil {
				framework.Failf("Error setting topology requirements: %v", err)
			}
		}
		testConfig := storageframework.ConvertTestConfig(l.config)
		expectedContent := fmt.Sprintf("Hello from namespace %s", f.Namespace.Name)
		dataSourceRef := preparePVCDataSourceForProvisioning(ctx, f, testConfig, l.cs, l.sourcePVC, l.sc, pattern.VolMode, expectedContent)
		l.pvc.Spec.DataSourceRef = dataSourceRef
		l.pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{
			v1.PersistentVolumeAccessMode(v1.ReadOnlyMany),
		}
		l.testCase.NodeSelection = testConfig.ClientNodeSelection
		l.testCase.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
			ginkgo.By("checking whether the created volume has the pre-populated data")
			tests := []e2evolume.Test{
				{
					Volume:          *storageutils.CreateVolumeSource(claim.Name, false /* readOnly */),
					Mode:            pattern.VolMode,
					File:            "index.html",
					ExpectedContent: expectedContent,
				},
			}
			e2evolume.TestVolumeClientSlow(ctx, f, testConfig, nil, "", tests)
		}
		// 如果源磁盘仍在卸载过程中，克隆会失败，因此在克隆前等待 VolumeAttachment 被移除。
		volumeAttachment := e2evolume.GetVolumeAttachmentName(ctx, f.ClientSet, testConfig, l.testCase.Provisioner, dataSourceRef.Name, l.sourcePVC.Namespace)
		framework.ExpectNoError(e2evolume.WaitForVolumeAttachmentTerminated(ctx, volumeAttachment, f.ClientSet, f.Timeouts.DataSourceProvision))
		l.testCase.TestDynamicProvisioning(ctx)
	})

	f.It("should provision storage with pvc data source in parallel", f.WithSlow(), func(ctx context.Context) {
		// 测试多次克隆单个 volume。
		if !dInfo.Capabilities[storageframework.CapPVCDataSource] {
			e2eskipper.Skipf("Driver %q does not support cloning - skipping", dInfo.Name)
		}
		if pattern.VolMode == v1.PersistentVolumeBlock && !dInfo.Capabilities[storageframework.CapBlock] {
			e2eskipper.Skipf("Driver %q does not support block volumes - skipping", dInfo.Name)
		}

		init(ctx)

		if l.config.ClientNodeSelection.Name == "" {
			// 将所有 Pod 调度到同一个 topology segment（例如云可用区），因为某些
			// driver 不支持跨 topology segment 克隆。
			if err := ensureTopologyRequirements(ctx, &l.config.ClientNodeSelection, l.cs, dInfo, 1); err != nil {
				framework.Failf("Error setting topology requirements: %v", err)
			}
		}
		testConfig := storageframework.ConvertTestConfig(l.config)
		expectedContent := fmt.Sprintf("Hello from namespace %s", f.Namespace.Name)
		dataSourceRef := preparePVCDataSourceForProvisioning(ctx, f, testConfig, l.cs, l.sourcePVC, l.sc, pattern.VolMode, expectedContent)
		l.pvc.Spec.DataSourceRef = dataSourceRef

		var wg sync.WaitGroup
		for i := range 5 {
			wg.Add(1)
			go func(i int) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()
				ginkgo.By(fmt.Sprintf("Cloning volume nr. %d", i))
				// 每个 goroutine 必须有自己的 Pod 前缀
				myTestConfig := testConfig
				myTestConfig.Prefix = fmt.Sprintf("%s-%d", myTestConfig.Prefix, i)

				t := *l.testCase
				t.NodeSelection = testConfig.ClientNodeSelection
				t.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
					ginkgo.By(fmt.Sprintf("checking whether the created volume %d has the pre-populated data", i))
					tests := []e2evolume.Test{
						{
							Volume:          *storageutils.CreateVolumeSource(claim.Name, false /* readOnly */),
							Mode:            pattern.VolMode,
							File:            "index.html",
							ExpectedContent: expectedContent,
						},
					}
					e2evolume.TestVolumeClientSlow(ctx, f, myTestConfig, nil, "", tests)
				}
				// 如果源磁盘仍在卸载过程中，克隆会失败，因此在克隆前等待 VolumeAttachment 被移除。
				volumeAttachment := e2evolume.GetVolumeAttachmentName(ctx, f.ClientSet, testConfig, l.testCase.Provisioner, dataSourceRef.Name, l.sourcePVC.Namespace)
				framework.ExpectNoError(e2evolume.WaitForVolumeAttachmentTerminated(ctx, volumeAttachment, f.ClientSet, f.Timeouts.DataSourceProvision))
				t.TestDynamicProvisioning(ctx)
			}(i)
		}
		wg.Wait()
	})

	ginkgo.It("should mount multiple PV pointing to the same storage on the same node", func(ctx context.Context) {
		// csi-hostpath driver 不支持此测试用例。该用例中有 2 个 PV 包含相同的底层存储。
		// 在第二个 volume 的 NodeStage 调用期间，csi-hostpath 会失败，因为它认为该 volume 已在另一路径 stage。
		// 注意：对于 PD CSI 这类 driver，NodeStage 在块模式下是空操作，因此不会有此问题。
		if pattern.VolMode == v1.PersistentVolumeBlock {
			e2eskipper.Skipf("skipping multiple PV mount test for block mode")
		}

		if !dInfo.Capabilities[storageframework.CapMultiplePVsSameID] {
			e2eskipper.Skipf("this driver does not support multiple PVs with the same volumeHandle")
		}

		init(ctx)

		l.testCase.PvCheck = func(ctx context.Context, claim *v1.PersistentVolumeClaim) {
			MultiplePVMountSingleNodeCheck(ctx, l.cs, f.Timeouts, claim, l.config.ClientNodeSelection)
		}
		SetupStorageClass(ctx, l.testCase.Client, l.testCase.Class)

		l.testCase.TestDynamicProvisioning(ctx)
	})
}

// SetupStorageClass 确保 spec 中的 StorageClass 存在；如果已存在，
// 则原样返回；如果不存在则先创建
// 再返回；如果 spec 为 nil 则返回 `default` StorageClass。
func SetupStorageClass(
	ctx context.Context,
	client clientset.Interface,
	class *storagev1.StorageClass,
) *storagev1.StorageClass {
	gomega.Expect(client).NotTo(gomega.BeNil(), "SetupStorageClass.client is required")

	var err error
	var computedStorageClass *storagev1.StorageClass
	if class != nil {
		computedStorageClass, err = client.StorageV1().StorageClasses().Get(ctx, class.Name, metav1.GetOptions{})
		if err == nil {
			// 若 storageclass 已存在则跳过创建
			ginkgo.By("Storage class " + computedStorageClass.Name + " is already created, skipping creation.")
		} else {
			ginkgo.By("Creating a StorageClass")
			class, err = client.StorageV1().StorageClasses().Create(ctx, class, metav1.CreateOptions{})
			framework.ExpectNoError(err)
			computedStorageClass, err = client.StorageV1().StorageClasses().Get(ctx, class.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			clearComputedStorageClass := func(ctx context.Context) {
				framework.Logf("deleting storage class %s", computedStorageClass.Name)
				err := client.StorageV1().StorageClasses().Delete(ctx, computedStorageClass.Name, metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					framework.ExpectNoError(err, "delete storage class")
				}
			}
			ginkgo.DeferCleanup(clearComputedStorageClass)
		}
	} else {
		// StorageClass 为 nil，因此将使用默认的
		scName, err := e2epv.GetDefaultStorageClassName(ctx, client)
		framework.ExpectNoError(err)
		ginkgo.By("Wanted storage class is nil, fetching default StorageClass=" + scName)
		computedStorageClass, err = client.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
		framework.ExpectNoError(err)
	}

	return computedStorageClass
}

// TestDynamicProvisioning 使用指定的 StorageClassTest 测试动态供给
// 假定 StorageClass `t.Class` 已被供给，
// 参见 #ProvisionStorageClass
func (t StorageClassTest) TestDynamicProvisioning(ctx context.Context) *v1.PersistentVolume {
	var err error
	client := t.Client
	gomega.Expect(client).NotTo(gomega.BeNil(), "StorageClassTest.Client is required")
	claim := t.Claim
	gomega.Expect(claim).NotTo(gomega.BeNil(), "StorageClassTest.Claim is required")
	gomega.Expect(claim.GenerateName).NotTo(gomega.BeEmpty(), "StorageClassTest.Claim.GenerateName must not be empty")
	class := t.Class
	gomega.Expect(class).NotTo(gomega.BeNil(), "StorageClassTest.Class is required")
	class, err = client.StorageV1().StorageClasses().Get(ctx, class.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "StorageClass.Class "+class.Name+" couldn't be fetched from the cluster")

	ginkgo.By(fmt.Sprintf("creating claim=%+v", claim))
	claim, err = client.CoreV1().PersistentVolumeClaims(claim.Namespace).Create(ctx, claim, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	defer func() {
		framework.Logf("deleting claim %q/%q", claim.Namespace, claim.Name)
		// 通常此 claim 已被删除
		err = client.CoreV1().PersistentVolumeClaims(claim.Namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Failf("Error deleting claim %q. Error: %v", claim.Name, err)
		}
	}()

	// 确保 claim 引用的是已供给的 StorageClass
	gomega.Expect(*claim.Spec.StorageClassName).To(gomega.Equal(class.Name))

	// 如果配置了延迟绑定，则创建并删除一个 Pod 以供给 volume
	if *class.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		ginkgo.By(fmt.Sprintf("creating a pod referring to the class=%+v claim=%+v", class, claim))
		var podConfig *e2epod.Config = &e2epod.Config{
			NS:            claim.Namespace,
			PVCs:          []*v1.PersistentVolumeClaim{claim},
			NodeSelection: t.NodeSelection,
		}

		var pod *v1.Pod
		pod, err := e2epod.CreateSecPod(ctx, client, podConfig, t.Timeouts.DataSourceProvision)
		// 现在删除 Pod，否则下面无法删除 PV
		framework.ExpectNoError(err)
		e2epod.DeletePodOrFail(ctx, client, pod.Namespace, pod.Name)
	}

	// 运行检查器
	if t.PvCheck != nil {
		t.PvCheck(ctx, claim)
	}

	pv := t.checkProvisioning(ctx, client, claim, class)

	ginkgo.By(fmt.Sprintf("deleting claim %q/%q", claim.Namespace, claim.Name))
	framework.ExpectNoError(client.CoreV1().PersistentVolumeClaims(claim.Namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{}))

	// 如果 reclaim policy 为 Delete，则等待 PV 被删除。（如果是
	// Retain，等待没有意义，因为 PV 不会自动删除，
	// 预期由调用方处理。）从技术上讲，前几次删除
	// 尝试可能失败，因为 volume 仍挂载在节点上，
	// kubelet 正在慢慢清理前一个 Pod，但应在几分钟内
	// 成功。等待 20 分钟（或 t.Timeouts.PVDeleteSlow 中指定的自定义值），
	// 以应对随机的云服务故障。
	if pv != nil && pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		ginkgo.By(fmt.Sprintf("deleting the claim's PV %q", pv.Name))
		framework.ExpectNoError(e2epv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, 5*time.Second, t.Timeouts.PVDeleteSlow))
	}

	return pv
}

// getBoundPV 返回 PV 详细信息。
func getBoundPV(ctx context.Context, client clientset.Interface, pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolume, error) {
	// 获取 claim 的新副本
	claim, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// 获取绑定的 PV
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
	return pv, err
}

// checkProvisioning 验证 claim 已绑定且具有正确的属性
func (t StorageClassTest) checkProvisioning(ctx context.Context, client clientset.Interface, claim *v1.PersistentVolumeClaim, class *storagev1.StorageClass) *v1.PersistentVolume {
	err := e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client, claim.Namespace, claim.Name, framework.Poll, t.Timeouts.ClaimProvision)
	framework.ExpectNoError(err)

	ginkgo.By("checking the claim")
	pv, err := getBoundPV(ctx, client, claim)
	framework.ExpectNoError(err)

	// 检查大小
	expectedCapacity := resource.MustParse(t.ExpectedSize)
	pvCapacity := pv.Spec.Capacity[v1.ResourceName(v1.ResourceStorage)]
	gomega.Expect(pvCapacity.Value()).To(gomega.BeNumerically(">=", expectedCapacity.Value()), "pvCapacity is not greater or equal to expectedCapacity")

	requestedCapacity := resource.MustParse(t.ClaimSize)
	claimCapacity := claim.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	gomega.Expect(claimCapacity.Value()).To(gomega.BeNumerically(">=", requestedCapacity.Value()), "claimCapacity is not greater or equal to requestedCapacity")

	// 检查 PV 属性
	ginkgo.By("checking the PV")

	// PV 中的每个 access mode 都应出现在 PVC 中
	gomega.Expect(pv.Spec.AccessModes).NotTo(gomega.BeZero())
	for _, pvMode := range pv.Spec.AccessModes {
		found := false
		for _, pvcMode := range claim.Spec.AccessModes {
			if pvMode == pvcMode {
				found = true
				break
			}
		}
		if !found {
			framework.Failf("Actual access modes %v are not in claim's access mode", pv.Spec.AccessModes)
		}
	}

	gomega.Expect(pv.Spec.ClaimRef.Name).To(gomega.Equal(claim.ObjectMeta.Name))
	gomega.Expect(pv.Spec.ClaimRef.Namespace).To(gomega.Equal(claim.ObjectMeta.Namespace))
	if class == nil {
		gomega.Expect(pv.Spec.PersistentVolumeReclaimPolicy).To(gomega.Equal(v1.PersistentVolumeReclaimDelete))
	} else {
		gomega.Expect(pv.Spec.PersistentVolumeReclaimPolicy).To(gomega.Equal(*class.ReclaimPolicy))
		gomega.Expect(pv.Spec.MountOptions).To(gomega.Equal(class.MountOptions))
	}
	if claim.Spec.VolumeMode != nil {
		gomega.Expect(pv.Spec.VolumeMode).NotTo(gomega.BeNil())
		gomega.Expect(*pv.Spec.VolumeMode).To(gomega.Equal(*claim.Spec.VolumeMode))
	}
	return pv
}

// PVWriteReadSingleNodeCheck 检查 PV 在单个节点上保留数据
// 并返回该 PV。
//
// 它启动两个 Pod：
// - 第一个 Pod 在一个节点上向 /mnt/test（即 volume）写入 'hello word'。
// - 第二个 Pod 在同一节点上对 /mnt/test 运行 grep 'hello world'。
//
// 第一个 Pod 的节点由 Kubernetes 调度时选择，
// 第二个 Pod 则按名称选择该节点。
//
// 如果两者都成功，说明 Kubernetes 实际分配了跨 Pod
// 持久化的存储。
//
// 这是一个通用测试，可由 StorageClassTest.PvCheck 调用。
func PVWriteReadSingleNodeCheck(ctx context.Context, client clientset.Interface, timeouts *framework.TimeoutContext, claim *v1.PersistentVolumeClaim, node e2epod.NodeSelection) *v1.PersistentVolume {
	ginkgo.By(fmt.Sprintf("checking the created volume is writable on node %+v", node))
	command := "echo 'hello world' > /mnt/test/data"
	pod := StartInPodWithVolume(ctx, client, claim.Namespace, claim.Name, "pvc-volume-tester-writer", command, node)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		// 此时 pod 可能为 nil。
		StopPod(ctx, client, pod)
	})
	framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, client, pod.Name, pod.Namespace, timeouts.PodStartSlow))
	runningPod, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "get pod")
	actualNodeName := runningPod.Spec.NodeName
	StopPod(ctx, client, pod)
	pod = nil // 不要停止两次。

	// 获取 PV 的新副本
	e2evolume, err := getBoundPV(ctx, client, claim)
	framework.ExpectNoError(err)

	ginkgo.By(fmt.Sprintf("checking the created volume has the correct mount options, is readable and retains data on the same node %q", actualNodeName))
	command = "grep 'hello world' /mnt/test/data"

	// 我们让第二个 Pod 额外负责检查 volume 是否
	// 使用了 PV 的挂载选项（如果 PV 配置了挂载选项）。
	for _, option := range e2evolume.Spec.MountOptions {
		// 获取条目，取第 6 个单词处的挂载选项，并将方括号替换为逗号
		command += fmt.Sprintf(" && ( mount | grep 'on /mnt/test' | awk '{print $6}' | sed 's/^(/,/; s/)$/,/' | grep -q ,%s, )", option)
	}
	command += " || (mount | grep 'on /mnt/test'; false)"

	if framework.NodeOSDistroIs("windows") {
		// agnhost 不支持 mount
		command = "grep 'hello world' /mnt/test/data"
	}
	RunInPodWithVolume(ctx, client, timeouts, claim.Namespace, claim.Name, "pvc-volume-tester-reader", command, e2epod.NodeSelection{Name: actualNodeName, Selector: node.Selector})
	return e2evolume
}

// PVMultiNodeCheck 检查 PV 在节点间移动时是否保留数据。
//
// 它启动以下 Pod：
// - 第一个 Pod 在一个节点上向 /mnt/test（即 volume）写入 'hello word'。
// - 第二个 Pod 在另一个节点上对 /mnt/test 运行 grep 'hello world'。
//
// 第一个节点由 Kubernetes 调度第一个 Pod 时选择。第二个 Pod 使用相同标准，
// 但额外添加了对第一个节点的反亲和性。只有集群有多个
// 合适节点时测试才能通过。调用方必须确保这一点。
//
// 如果全部成功，说明 Kubernetes 实际分配了
// 跨 Pod 且跨节点持久化的存储。
//
// 这是一个通用测试，可由 StorageClassTest.PvCheck 调用。
func PVMultiNodeCheck(ctx context.Context, client clientset.Interface, timeouts *framework.TimeoutContext, claim *v1.PersistentVolumeClaim, node e2epod.NodeSelection) {
	gomega.Expect(node.Name).To(gomega.BeZero(), "this test only works when not locked onto a single node")

	var pod *v1.Pod
	defer func() {
		// 传入 pod = nil 是可以的。
		StopPod(ctx, client, pod)
	}()

	ginkgo.By(fmt.Sprintf("checking the created volume is writable on node %+v", node))
	command := "echo 'hello world' > /mnt/test/data"
	pod = StartInPodWithVolume(ctx, client, claim.Namespace, claim.Name, "pvc-writer-node1", command, node)
	framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, client, pod.Name, pod.Namespace, timeouts.PodStartSlow))
	runningPod, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "get pod")
	actualNodeName := runningPod.Spec.NodeName
	StopPod(ctx, client, pod)
	pod = nil // 不要停止两次。

	// 添加 node anti-affinity。
	secondNode := node
	e2epod.SetAntiAffinity(&secondNode, actualNodeName)
	ginkgo.By(fmt.Sprintf("checking the created volume is readable and retains data on another node %+v", secondNode))
	command = "grep 'hello world' /mnt/test/data"
	pod = StartInPodWithVolume(ctx, client, claim.Namespace, claim.Name, "pvc-reader-node2", command, secondNode)
	framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, client, pod.Name, pod.Namespace, timeouts.PodStartSlow))
	runningPod, err = client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "get pod")
	gomega.Expect(runningPod.Spec.NodeName).ToNot(gomega.Equal(actualNodeName), "second pod should have run on a different node")
	StopPod(ctx, client, pod)
	pod = nil
}

// TestBindingWaitForFirstConsumerMultiPVC 测试 WaitForFirstConsumer 模式下的绑定
func (t StorageClassTest) TestBindingWaitForFirstConsumerMultiPVC(ctx context.Context, claims []*v1.PersistentVolumeClaim, nodeSelector map[string]string, expectUnschedulable bool) ([]*v1.PersistentVolume, *v1.Node) {
	var err error
	gomega.Expect(claims).ToNot(gomega.BeEmpty())
	namespace := claims[0].Namespace

	ginkgo.By("creating claims")
	var claimNames []string
	var createdClaims []*v1.PersistentVolumeClaim
	for _, claim := range claims {
		c, err := t.Client.CoreV1().PersistentVolumeClaims(claim.Namespace).Create(ctx, claim, metav1.CreateOptions{})
		claimNames = append(claimNames, c.Name)
		createdClaims = append(createdClaims, c)
		framework.ExpectNoError(err)
	}
	defer func() {
		errors := map[string]error{}
		for _, claim := range createdClaims {
			err := e2epv.DeletePersistentVolumeClaim(ctx, t.Client, claim.Name, claim.Namespace)
			if err != nil {
				errors[claim.Name] = err
			}
		}
		if len(errors) > 0 {
			for claimName, err := range errors {
				framework.Logf("Failed to delete PVC: %s due to error: %v", claimName, err)
			}
		}
	}()

	// 等待 ClaimProvisionTimeout（并行等待所有 PVC），并确保 phase 未变为 Bound，即 Wait 超时报错
	ginkgo.By("checking the claims are in pending state")
	err = e2epv.WaitForPersistentVolumeClaimsPhase(ctx, v1.ClaimBound, t.Client, namespace, claimNames, 2*time.Second /* Poll */, t.Timeouts.ClaimProvisionShort, true)
	gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("not all in phase Bound")))
	verifyPVCsPending(ctx, t.Client, createdClaims)

	ginkgo.By("creating a pod referring to the claims")
	// 创建一个引用该 claim 的 Pod 并等待其进入运行态
	var pod *v1.Pod
	if expectUnschedulable {
		pod, err = e2epod.CreateUnschedulablePod(ctx, t.Client, namespace, nodeSelector, createdClaims, admissionapi.LevelPrivileged, "" /* 命令 */)
	} else {
		pod, err = e2epod.CreatePod(ctx, t.Client, namespace, nil /* nodeSelector */, createdClaims, admissionapi.LevelPrivileged, "" /* 命令 */)
	}
	framework.ExpectNoError(err)
	ginkgo.DeferCleanup(func(ctx context.Context) error {
		e2epod.DeletePodOrFail(ctx, t.Client, pod.Namespace, pod.Name)
		return e2epod.WaitForPodNotFoundInNamespace(ctx, t.Client, pod.Name, pod.Namespace, t.Timeouts.PodDelete)
	})
	if expectUnschedulable {
		// 验证没有 claim 被供给。
		verifyPVCsPending(ctx, t.Client, createdClaims)
		return nil, nil
	}

	// 收集节点详情
	node, err := t.Client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	ginkgo.By("re-checking the claims to see they bound")
	var pvs []*v1.PersistentVolume
	for _, claim := range createdClaims {
		// 获取 claim 的新副本
		claim, err = t.Client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		// 确保 claim 已绑定
		err = e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, t.Client, claim.Namespace, claim.Name, framework.Poll, t.Timeouts.ClaimProvision)
		framework.ExpectNoError(err)

		pv, err := t.Client.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		pvs = append(pvs, pv)
	}
	gomega.Expect(pvs).To(gomega.HaveLen(len(createdClaims)))
	return pvs, node
}

// RunInPodWithVolume 在 Pod 中运行命令，并将给定 claim 挂载到 /mnt 目录。
// 它启动、检查、收集输出并停止。
func RunInPodWithVolume(ctx context.Context, c clientset.Interface, t *framework.TimeoutContext, ns, claimName, podName, command string, node e2epod.NodeSelection) *v1.Pod {
	pod := StartInPodWithVolume(ctx, c, ns, claimName, podName, command, node)
	defer StopPod(ctx, c, pod)
	framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, c, pod.Name, pod.Namespace, t.PodStartSlow))
	// 获取 Pod 的最新状态
	pod, err := c.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err)
	return pod
}

// StartInPodWithVolume 在 Pod 中启动命令，并将给定 claim 挂载到 /mnt 目录
// 调用方负责检查该 Pod 并删除它。
func StartInPodWithVolume(ctx context.Context, c clientset.Interface, ns, claimName, podName, command string, node e2epod.NodeSelection) *v1.Pod {
	return StartInPodWithVolumeSource(ctx, c, v1.VolumeSource{
		PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
			ClaimName: claimName,
		},
	}, ns, podName, command, node)
}

// StartInPodWithVolumeSource 在 Pod 中启动命令，并将给定 volume 挂载到 /mnt 目录
// 调用方负责检查该 Pod 并删除它。
func StartInPodWithVolumeSource(ctx context.Context, c clientset.Interface, volSrc v1.VolumeSource, ns, podName, command string, node e2epod.NodeSelection) *v1.Pod {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: podName + "-",
			Labels: map[string]string{
				"app": podName,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "volume-tester",
					Image:   e2epod.GetDefaultTestImage(),
					Command: e2epod.GenerateScriptCmd(command),
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "my-volume",
							MountPath: "/mnt/test",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name:         "my-volume",
					VolumeSource: volSrc,
				},
			},
		},
	}

	e2epod.SetNodeSelection(&pod.Spec, node)
	pod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Failed to create pod: %v", err)
	return pod
}

// StopPod 先记录 Pod 容器的输出，再删除 Pod 并
// 等待删除成功。
func StopPod(ctx context.Context, c clientset.Interface, pod *v1.Pod) {
	if pod == nil {
		return
	}
	body, err := c.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{}).Do(ctx).Raw()
	if err != nil {
		framework.Logf("Error getting logs for pod %s: %v", pod.Name, err)
	} else {
		framework.Logf("Pod %s has the following logs: %s", pod.Name, body)
	}
	framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, c, pod))
}

// StopPodAndDependents 先记录 Pod 容器的输出，
// 再删除 Pod 并等待成功，同时等待所有归属的
// 资源被删除。
func StopPodAndDependents(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, pod *v1.Pod) {
	if pod == nil {
		return
	}
	body, err := c.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{}).Do(ctx).Raw()
	if err != nil {
		framework.Logf("Error getting logs for pod %s: %v", pod.Name, err)
	} else {
		framework.Logf("Pod %s has the following logs: %s", pod.Name, body)
	}

	// 必须显式等待 generic ephemeral volume 的 PV 被移除。
	// 为此必须先找到它们……
	pvs, err := c.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err, "list PVs")
	var podPVs []v1.PersistentVolume
	for _, pv := range pvs.Items {
		if pv.Spec.ClaimRef == nil ||
			pv.Spec.ClaimRef.Namespace != pod.Namespace {
			continue
		}
		pvc, err := c.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			// 一定是某个无关的 PV，否则 PVC 应该存在。
			continue
		}
		framework.ExpectNoError(err, "get PVC")
		if pv.Spec.ClaimRef.UID == pvc.UID && metav1.IsControlledBy(pvc, pod) {
			podPVs = append(podPVs, pv)
		}
	}

	framework.Logf("Deleting pod %q in namespace %q", pod.Name, pod.Namespace)
	deletionPolicy := metav1.DeletePropagationForeground
	err = c.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name,
		metav1.DeleteOptions{
			// 如果 Pod 是某些资源（如 ephemeral inline volume）的属主，
			// 返回前要确保这些资源也已消失。
			// 使用 metav1.DeletePropagationForeground 阻塞式删除 Pod 可实现这一点。
			PropagationPolicy: &deletionPolicy,
		})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return // 假定 Pod 已被删除
		}
		framework.Logf("pod Delete API error: %v", err)
	}
	framework.Logf("Wait up to %v for pod %q to be fully deleted", timeouts.PodDelete, pod.Name)
	framework.ExpectNoError(e2epod.WaitForPodNotFoundInNamespace(ctx, c, pod.Name, pod.Namespace, timeouts.PodDelete))
	if len(podPVs) > 0 {
		for _, pv := range podPVs {
			// 与 CSI inline volume 一样，这里使用 Pod 删除超时，因为从概念上讲
			// volume 删除也需要这么快（不管"这么快"指什么）。
			framework.Logf("Wait up to %v for pod PV %s to be fully deleted", timeouts.PodDelete, pv.Name)
			framework.ExpectNoError(e2epv.WaitForPersistentVolumeDeleted(ctx, c, pv.Name, 5*time.Second, timeouts.PVDeleteSlow))
		}
	}
}

func verifyPVCsPending(ctx context.Context, client clientset.Interface, pvcs []*v1.PersistentVolumeClaim) {
	for _, claim := range pvcs {
		// 获取 claim 的新副本
		claim, err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(claim.Status.Phase).To(gomega.Equal(v1.ClaimPending))
	}
}

func prepareSnapshotDataSourceForProvisioning(
	ctx context.Context,
	f *framework.Framework,
	config e2evolume.TestConfig,
	perTestConfig *storageframework.PerTestConfig,
	pattern storageframework.TestPattern,
	client clientset.Interface,
	dynamicClient dynamic.Interface,
	initClaim *v1.PersistentVolumeClaim,
	class *storagev1.StorageClass,
	sDriver storageframework.SnapshottableTestDriver,
	mode v1.PersistentVolumeMode,
	injectContent string,
) *v1.TypedObjectReference {
	SetupStorageClass(ctx, client, class)

	if initClaim.ResourceVersion != "" {
		ginkgo.By("Skipping creation of PVC, it already exists")
	} else {
		ginkgo.By("[Initialize dataSource]creating a initClaim")
		updatedClaim, err := client.CoreV1().PersistentVolumeClaims(initClaim.Namespace).Create(ctx, initClaim, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			err = nil
		}
		framework.ExpectNoError(err)
		initClaim = updatedClaim
	}

	// 将命名空间写入 /mnt/test（即 volume）。
	tests := []e2evolume.Test{
		{
			Volume:          *storageutils.CreateVolumeSource(initClaim.Name, false /* readOnly */),
			Mode:            mode,
			File:            "index.html",
			ExpectedContent: injectContent,
		},
	}
	e2evolume.InjectContent(ctx, f, config, nil, "", tests)

	parameters := map[string]string{}
	snapshotResource := storageframework.CreateSnapshotResource(ctx, sDriver, perTestConfig, pattern, initClaim.GetName(), initClaim.GetNamespace(), f.Timeouts, parameters)
	group := "snapshot.storage.k8s.io"
	dataSourceRef := &v1.TypedObjectReference{
		APIGroup: &group,
		Kind:     "VolumeSnapshot",
		Name:     snapshotResource.Vs.GetName(),
	}

	cleanupFunc := func(ctx context.Context) {
		framework.Logf("deleting initClaim %q/%q", initClaim.Namespace, initClaim.Name)
		err := client.CoreV1().PersistentVolumeClaims(initClaim.Namespace).Delete(ctx, initClaim.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Failf("Error deleting initClaim %q. Error: %v", initClaim.Name, err)
		}

		err = snapshotResource.CleanupResource(ctx, f.Timeouts)
		framework.ExpectNoError(err)
	}
	ginkgo.DeferCleanup(cleanupFunc)

	return dataSourceRef
}

func preparePVCDataSourceForProvisioning(
	ctx context.Context,
	f *framework.Framework,
	config e2evolume.TestConfig,
	client clientset.Interface,
	source *v1.PersistentVolumeClaim,
	class *storagev1.StorageClass,
	mode v1.PersistentVolumeMode,
	injectContent string,
) *v1.TypedObjectReference {
	SetupStorageClass(ctx, client, class)

	if source.ResourceVersion != "" {
		ginkgo.By("Skipping creation of PVC, it already exists")
	} else {
		ginkgo.By("[Initialize dataSource]creating a source PVC")
		var err error
		source, err = client.CoreV1().PersistentVolumeClaims(source.Namespace).Create(ctx, source, metav1.CreateOptions{})
		framework.ExpectNoError(err)
	}

	tests := []e2evolume.Test{
		{
			Volume:          *storageutils.CreateVolumeSource(source.Name, false /* readOnly */),
			Mode:            mode,
			File:            "index.html",
			ExpectedContent: injectContent,
		},
	}
	e2evolume.InjectContent(ctx, f, config, nil, "", tests)

	dataSourceRef := &v1.TypedObjectReference{
		Kind: "PersistentVolumeClaim",
		Name: source.GetName(),
	}

	cleanupFunc := func(ctx context.Context) {
		framework.Logf("deleting source PVC %q/%q", source.Namespace, source.Name)
		err := client.CoreV1().PersistentVolumeClaims(source.Namespace).Delete(ctx, source.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Failf("Error deleting source PVC %q. Error: %v", source.Name, err)
		}
	}
	ginkgo.DeferCleanup(cleanupFunc)

	return dataSourceRef
}

// findVolumeMountPath 在 Pod 中查找 claim 名称，并返回其 volume 挂载点的绝对路径。
func findVolumeMountPath(pod *v1.Pod, claim *v1.PersistentVolumeClaim) string {
	// 查找 pod2 分配给 pvc 的 volume 名称。
	volumeName = ""
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim.ClaimName == claim.Name {
			volumeName = volume.Name
			break
		}
	}

	// 查找 Pod 在容器内挂载 volume 的位置。
	containerMountPath := ""
	for _, volumeMount := range pod.Spec.Containers[0].VolumeMounts {
		if volumeMount.Name == volumeName {
			containerMountPath = volumeMount.MountPath
			break
		}
	}
	return containerMountPath
}

// getFilesystemSizeBytes 返回 Pod 内指定挂载路径处文件系统的总容量。
// 它会根据平台（Windows/Linux）选择相应逻辑。
// 此函数依赖 `framework.NodeOSDistroIs` 辅助函数来判断目标 OS。
func getFilesystemSizeBytes(pod *v1.Pod, mountPath string) (int64, error) {
	if framework.NodeOSDistroIs("windows") {
		return getFilesystemSizeBytesWindows(pod, mountPath)
	}
	return getFilesystemSizeBytesLinux(pod, mountPath)
}

// getFilesystemSizeBytesLinux 使用 'stat'（块大小 * 块数）返回文件系统总大小。
func getFilesystemSizeBytesLinux(pod *v1.Pod, mountPath string) (int64, error) {
	// 获取块大小：在 Pod 内执行 'stat -f -c %s <mountPath>'。
	cmd := fmt.Sprintf("stat -f -c %%s %v", mountPath)
	blockSize, err := e2ekubectl.RunKubectl(pod.Namespace, "exec", pod.Name, "--", "/bin/sh", "-c", cmd)
	if err != nil {
		return 0, err
	}

	cmd = fmt.Sprintf("stat -f -c %%b %v", mountPath)
	blockCount, err := e2ekubectl.RunKubectl(pod.Namespace, "exec", pod.Name, "--", "/bin/sh", "-c", cmd)
	if err != nil {
		return 0, err
	}

	bs, err := strconv.Atoi(strings.TrimSuffix(blockSize, "\n"))
	if err != nil {
		return 0, err
	}

	bc, err := strconv.Atoi(strings.TrimSuffix(blockCount, "\n"))
	if err != nil {
		return 0, err
	}

	return int64(bs * bc), nil
}

// getFilesystemSizeBytesWindows 返回 Windows Pod 内指定挂载路径
// 背后文件系统的总大小。
func getFilesystemSizeBytesWindows(pod *v1.Pod, mountPath string) (int64, error) {
	// 通过执行 (Get-Item -Path "<mountPath>").Target 解析底层 volume。
	resolveVolumeIDCmd := fmt.Sprintf(`(Get-Item -Path "%s").Target`, mountPath)
	out, err := e2ekubectl.RunKubectl(
		pod.Namespace,
		"exec", pod.Name, "--",
		"powershell.exe",
		"-NoProfile",
		"-NonInteractive",
		"-Command", fmt.Sprintf("& { %s }", resolveVolumeIDCmd),
	)
	if err != nil {
		return 0, fmt.Errorf("failed resolving underlying volume for mountPath %q: %w", mountPath, err)
	}

	volumeID := strings.Trim(out, "\x00 \r\n\t")

	if volumeID == "" {
		return 0, fmt.Errorf("resolved empty volumeID for mountPath %q", mountPath)
	}
	// 如果是普通盘符 volume（如 "C:" 或 "D:"），
	// 则 Get-Volume -DriveLetter 可以正常工作。
	if len(volumeID) == 2 && volumeID[1] == ':' {
		driveLetter := volumeID[:1] // "C"
		psCmd := fmt.Sprintf(`(Get-Volume -DriveLetter %s).Size`, driveLetter)

		return runGetVolumeSize(pod, psCmd, fmt.Sprintf("drive %s", driveLetter))
	}
	// 否则这是一个真正的 volume GUID：
	// 该路径通常需要先按 CSI Proxy 的逻辑解析为磁盘号。
	// 我们通过执行一系列命令来解析磁盘号并获取大小，以简化最后一步。
	psDiskNumCmd := fmt.Sprintf(`(Get-Volume -UniqueId "%s" | Get-Partition).DiskNumber`, volumeID)

	diskOut, err := e2ekubectl.RunKubectl(
		pod.Namespace,
		"exec", pod.Name, "--",
		"powershell.exe",
		"-NoProfile",
		"-NonInteractive",
		"-Command", fmt.Sprintf("& { %s }", psDiskNumCmd),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to get DiskNumber for volumeID %q: %w", volumeID, err)
	}

	diskNumStr := strings.Trim(diskOut, "\x00 \r\n\t")
	if diskNumStr == "" {
		return 0, fmt.Errorf("DiskNumber is empty for volume %q", volumeID)
	}

	diskSizeCmd := fmt.Sprintf(`(Get-Disk -Number %s).Size`, diskNumStr)
	return runGetVolumeSize(pod, diskSizeCmd, fmt.Sprintf("disk %s (volumeID=%s)", diskNumStr, volumeID))
}

// runGetVolumeSize 是共享辅助函数，执行一条 PowerShell 命令，
// 期望返回单个整数（字节大小）并解析它。
func runGetVolumeSize(pod *v1.Pod, psCmd, debugTag string) (int64, error) {
	out, err := e2ekubectl.RunKubectl(
		pod.Namespace,
		"exec", pod.Name, "--",
		"powershell.exe",
		"-NoProfile",
		"-NonInteractive",
		"-Command", fmt.Sprintf("& { %s }", psCmd),
	)
	if err != nil {
		return 0, fmt.Errorf("failed running PowerShell Get-Volume for %s: %w", debugTag, err)
	}
	// 清理输出中的空字节、回车和空白，以便安全解析。
	cleaned := strings.Trim(out, "\x00 \r\n\t")
	totalBytes, err := strconv.ParseInt(cleaned, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed parsing PowerShell output %q for %s: %w",
			cleaned, debugTag, err)
	}
	return totalBytes, nil
}

// MultiplePVMountSingleNodeCheck 检查指向同一底层存储的多个 PV 能否在单个节点上同时挂载。
//
// Steps:
// - 在节点 N1 上使用 PVC1、PV1（指向底层 volume v）启动 Pod1。
// - 创建 PVC2、PV2 并预绑定。PV2 指向同一底层 volume v。
// - 在节点 N1 上使用 PVC2、PV2（指向底层 volume v）启动 Pod2。
func MultiplePVMountSingleNodeCheck(ctx context.Context, client clientset.Interface, timeouts *framework.TimeoutContext, claim *v1.PersistentVolumeClaim, node e2epod.NodeSelection) {
	pod1Config := e2epod.Config{
		NS:            claim.Namespace,
		NodeSelection: node,
		PVCs:          []*v1.PersistentVolumeClaim{claim},
	}
	pod1, err := e2epod.CreateSecPodWithNodeSelection(ctx, client, &pod1Config, timeouts.PodStart)
	framework.ExpectNoError(err)
	defer func() {
		ginkgo.By(fmt.Sprintf("Deleting Pod %s/%s", pod1.Namespace, pod1.Name))
		framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, client, pod1))
	}()
	ginkgo.By(fmt.Sprintf("Created Pod %s/%s on node %s", pod1.Namespace, pod1.Name, pod1.Spec.NodeName))

	// 创建指向同一底层存储的新 PV。使用 Retain 策略，以便删除第二个 PVC 时不会触发其绑定 PV 和底层存储的删除。
	e2evolume, err := getBoundPV(ctx, client, claim)
	framework.ExpectNoError(err)
	pv2Config := e2epv.PersistentVolumeConfig{
		NamePrefix:       fmt.Sprintf("%s-", "pv"),
		StorageClassName: *claim.Spec.StorageClassName,
		PVSource:         e2evolume.Spec.PersistentVolumeSource,
		AccessModes:      e2evolume.Spec.AccessModes,
		VolumeMode:       e2evolume.Spec.VolumeMode,
		ReclaimPolicy:    v1.PersistentVolumeReclaimRetain,
	}

	pvc2Config := e2epv.PersistentVolumeClaimConfig{
		NamePrefix:       fmt.Sprintf("%s-", "pvc"),
		StorageClassName: &claim.Namespace,
		AccessModes:      e2evolume.Spec.AccessModes,
		VolumeMode:       e2evolume.Spec.VolumeMode,
	}

	pv2, pvc2, err := e2epv.CreatePVCPV(ctx, client, timeouts, pv2Config, pvc2Config, claim.Namespace, true)
	framework.ExpectNoError(err, "PVC, PV creation failed")
	framework.Logf("Created PVC %s/%s and PV %s", pvc2.Namespace, pvc2.Name, pv2.Name)

	pod2Config := e2epod.Config{
		NS:            pvc2.Namespace,
		NodeSelection: e2epod.NodeSelection{Name: pod1.Spec.NodeName, Selector: node.Selector},
		PVCs:          []*v1.PersistentVolumeClaim{pvc2},
	}
	pod2, err := e2epod.CreateSecPodWithNodeSelection(ctx, client, &pod2Config, timeouts.PodStart)
	framework.ExpectNoError(err)
	ginkgo.By(fmt.Sprintf("Created Pod %s/%s on node %s", pod2.Namespace, pod2.Name, pod2.Spec.NodeName))

	ginkgo.By(fmt.Sprintf("Deleting Pod %s/%s", pod2.Namespace, pod2.Name))
	framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, client, pod2))

	err = e2epv.DeletePersistentVolumeClaim(ctx, client, pvc2.Name, pvc2.Namespace)
	framework.ExpectNoError(err, "Failed to delete PVC: %s/%s", pvc2.Namespace, pvc2.Name)

	err = e2epv.DeletePersistentVolume(ctx, client, pv2.Name)
	framework.ExpectNoError(err, "Failed to delete PV: %s", pv2.Name)
}
