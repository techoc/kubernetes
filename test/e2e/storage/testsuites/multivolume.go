/*
Copyright 2019 The Kubernetes Authors.

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
	"reflect"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	storageutils "k8s.io/kubernetes/test/e2e/storage/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

type multiVolumeTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

var _ storageframework.TestSuite = &multiVolumeTestSuite{}

// InitCustomMultiVolumeTestSuite 返回 multiVolumeTestSuite，它使用自定义测试模式
// 实现 TestSuite 接口
func InitCustomMultiVolumeTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &multiVolumeTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "multiVolume",
			TestTags:     []interface{}{framework.WithSlow()},
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
		},
	}
}

// InitMultiVolumeTestSuite 返回 multiVolumeTestSuite，它使用测试套件
// 默认模式实现 TestSuite 接口
func InitMultiVolumeTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.FsVolModePreprovisionedPV,
		storageframework.FsVolModeDynamicPV,
		storageframework.BlockVolModePreprovisionedPV,
		storageframework.BlockVolModeDynamicPV,
		storageframework.Ext4DynamicPV,
		storageframework.XfsDynamicPV,
		storageframework.NtfsDynamicPV,
	}
	return InitCustomMultiVolumeTestSuite(patterns)
}

func (t *multiVolumeTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *multiVolumeTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	dInfo := driver.GetDriverInfo()
	if reason := checkVolTypePatterns(pattern, driver, storageframework.NewVolTypeMap(storageframework.PreprovisionedPV)); reason != "" {
		return reason
	}
	if pattern.VolMode == v1.PersistentVolumeBlock && !dInfo.Capabilities[storageframework.CapBlock] {
		return fmt.Sprintf("Driver %s doesn't support %v", dInfo.Name, pattern.VolMode)
	}
	return ""
}

func (t *multiVolumeTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		cs        clientset.Interface
		ns        *v1.Namespace
		driver    storageframework.TestDriver
		resources []*storageframework.VolumeResource

		migrationCheck *migrationOpCheck
	}
	var (
		dInfo = driver.GetDriverInfo()
		l     local
	)

	// 注意它还会注册一个 AfterEach，使 f 不可用。任何使用
	// f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("multivolume", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		l = local{}
		l.ns = f.Namespace
		l.cs = f.ClientSet
		l.driver = driver

		// 现在做开销更大的测试初始化。
		l.config = driver.PrepareTest(ctx, f)
		l.migrationCheck = newMigrationOpCheck(ctx, f.ClientSet, f.ClientConfig(), dInfo.InTreePluginName)
	}

	cleanup := func(ctx context.Context) {
		var errs []error
		for _, resource := range l.resources {
			errs = append(errs, resource.CleanupResource(ctx))
		}

		framework.ExpectNoError(errors.NewAggregate(errs), "while cleanup resource")
		l.migrationCheck.validateMigrationVolumeOpCounts(ctx)
	}

	// 本测试验证以下配置：
	// [pod1]                            same node       [pod2]
	// [   node1   ]                           ==>        [   node1   ]
	// /    \      <- 相同 volume 模式                   /    \
	// [volume1]  [volume2]                              [volume1]  [volume2]
	ginkgo.It("should access to two volumes with the same volume mode and retain data across pod recreation on the same node", func(ctx context.Context) {
		// 目前预置 volume 通常不支持多 volume，
		// 因为 iSCSI、rbd 等容器化存储服务只返回
		// 容器内的静态 volume，不会按请求真正创建新 volume。
		if pattern.VolType == storageframework.PreprovisionedPV {
			e2eskipper.Skipf("This test doesn't work with pre-provisioned volume -- skipping")
		}

		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		var pvcs []*v1.PersistentVolumeClaim
		numVols := 2

		for range numVols {
			testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
			resource := storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, testVolumeSizeRange)
			l.resources = append(l.resources, resource)
			pvcs = append(pvcs, resource.Pvc)
		}
		TestAccessMultipleVolumesAcrossPodRecreation(ctx, l.config.Framework, l.cs, l.ns.Name,
			l.config.ClientNodeSelection, pvcs, true /* sameNode */)
	})

	// 本测试验证以下配置：
	// [pod1]                       different node       [pod2]
	// [   node1   ]                           ==>        [   node2   ]
	// /    \      <- 相同 volume 模式                   /    \
	// [volume1]  [volume2]                              [volume1]  [volume2]
	ginkgo.It("should access to two volumes with the same volume mode and retain data across pod recreation on different node", func(ctx context.Context) {
		// 目前预置 volume 通常不支持多 volume，
		// 因为 iSCSI、rbd 等容器化存储服务只返回
		// 容器内的静态 volume，不会按请求真正创建新 volume。
		if pattern.VolType == storageframework.PreprovisionedPV {
			e2eskipper.Skipf("This test doesn't work with pre-provisioned volume -- skipping")
		}

		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 检查不同节点测试的前置条件
		if l.driver.GetDriverInfo().Capabilities[storageframework.CapSingleNodeVolume] {
			e2eskipper.Skipf("Driver %s only supports %v -- skipping", l.driver.GetDriverInfo().Name, storageframework.CapSingleNodeVolume)
		}
		if l.config.ClientNodeSelection.Name != "" {
			e2eskipper.Skipf("Driver %q requires to deploy on a specific node - skipping", l.driver.GetDriverInfo().Name)
		}
		if err := ensureTopologyRequirements(ctx, &l.config.ClientNodeSelection, l.cs, dInfo, 2); err != nil {
			framework.Failf("Error setting topology requirements: %v", err)
		}

		var pvcs []*v1.PersistentVolumeClaim
		numVols := 2

		for range numVols {
			testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
			resource := storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, testVolumeSizeRange)
			l.resources = append(l.resources, resource)
			pvcs = append(pvcs, resource.Pvc)
		}

		TestAccessMultipleVolumesAcrossPodRecreation(ctx, l.config.Framework, l.cs, l.ns.Name,
			l.config.ClientNodeSelection, pvcs, false /* sameNode */)
	})

	// 本测试验证以下配置（只测试 <block, filesystem> 模式）：
	// [pod1]                            same node       [pod2]
	// [   node1   ]                          ==>        [   node1   ]
	// /    \      <- 不同 volume 模式             /    \
	// [volume1]  [volume2]                              [volume1]  [volume2]
	ginkgo.It("should access to two volumes with different volume mode and retain data across pod recreation on the same node", func(ctx context.Context) {
		if pattern.VolMode == v1.PersistentVolumeFilesystem {
			e2eskipper.Skipf("Filesystem volume case should be covered by block volume case -- skipping")
		}

		// 目前预置 volume 通常不支持多 volume，
		// 因为 iSCSI、rbd 等容器化存储服务只返回
		// 容器内的静态 volume，不会按请求真正创建新 volume。
		if pattern.VolType == storageframework.PreprovisionedPV {
			e2eskipper.Skipf("This test doesn't work with pre-provisioned volume -- skipping")
		}

		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		var pvcs []*v1.PersistentVolumeClaim
		numVols := 2

		for i := range numVols {
			curPattern := pattern
			if i != 0 {
				// 第一个 volume 应为 block，第二个及之后设置为 filesystem
				curPattern.VolMode = v1.PersistentVolumeFilesystem
			}
			testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
			resource := storageframework.CreateVolumeResource(ctx, driver, l.config, curPattern, testVolumeSizeRange)
			l.resources = append(l.resources, resource)
			pvcs = append(pvcs, resource.Pvc)
		}

		TestAccessMultipleVolumesAcrossPodRecreation(ctx, l.config.Framework, l.cs, l.ns.Name,
			l.config.ClientNodeSelection, pvcs, true /* sameNode */)
	})

	// 本测试验证以下配置（只测试 <block, filesystem> 模式）：
	// [pod1]                      different node       [pod2]
	// [   node1   ]                          ==>        [   node2   ]
	// /    \      <- 不同 volume 模式             /    \
	// [volume1]  [volume2]                              [volume1]  [volume2]
	ginkgo.It("should access to two volumes with different volume mode and retain data across pod recreation on different node", func(ctx context.Context) {
		if pattern.VolMode == v1.PersistentVolumeFilesystem {
			e2eskipper.Skipf("Filesystem volume case should be covered by block volume case -- skipping")
		}

		// 目前预置 volume 通常不支持多 volume，
		// 因为 iSCSI、rbd 等容器化存储服务只返回
		// 容器内的静态 volume，不会按请求真正创建新 volume。
		if pattern.VolType == storageframework.PreprovisionedPV {
			e2eskipper.Skipf("This test doesn't work with pre-provisioned volume -- skipping")
		}

		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 检查不同节点测试的前置条件
		if l.driver.GetDriverInfo().Capabilities[storageframework.CapSingleNodeVolume] {
			e2eskipper.Skipf("Driver %s only supports %v -- skipping", l.driver.GetDriverInfo().Name, storageframework.CapSingleNodeVolume)
		}
		if l.config.ClientNodeSelection.Name != "" {
			e2eskipper.Skipf("Driver %q requires to deploy on a specific node - skipping", l.driver.GetDriverInfo().Name)
		}
		if err := ensureTopologyRequirements(ctx, &l.config.ClientNodeSelection, l.cs, dInfo, 2); err != nil {
			framework.Failf("Error setting topology requirements: %v", err)
		}

		var pvcs []*v1.PersistentVolumeClaim
		numVols := 2

		for i := range numVols {
			curPattern := pattern
			if i != 0 {
				// 第一个 volume 应为 block，第二个及之后设置为 filesystem
				curPattern.VolMode = v1.PersistentVolumeFilesystem
			}
			testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
			resource := storageframework.CreateVolumeResource(ctx, driver, l.config, curPattern, testVolumeSizeRange)
			l.resources = append(l.resources, resource)
			pvcs = append(pvcs, resource.Pvc)
		}

		TestAccessMultipleVolumesAcrossPodRecreation(ctx, l.config.Framework, l.cs, l.ns.Name,
			l.config.ClientNodeSelection, pvcs, false /* sameNode */)
	})

	// 本测试验证以下配置：
	// [pod1] [pod2]
	// [   node1   ]
	// \      /     <- 相同 volume 模式
	// [volume1]
	ginkgo.It("should concurrently access the single volume from pods on the same node", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		numPods := 2

		if !l.driver.GetDriverInfo().Capabilities[storageframework.CapMultiPODs] {
			e2eskipper.Skipf("Driver %q does not support multiple concurrent pods - skipping", dInfo.Name)
		}

		// 创建 volume
		testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
		resource := storageframework.CreateVolumeResource(ctx, l.driver, l.config, pattern, testVolumeSizeRange)
		l.resources = append(l.resources, resource)

		// 测试从不同节点上的 pod 访问 volume
		TestConcurrentAccessToSingleVolume(ctx, l.config.Framework, l.cs, l.ns.Name,
			l.config.ClientNodeSelection, resource.Pvc, numPods, true /* sameNode */, false /* readOnly */)
	})

	// 本测试验证以下配置：
	// [pod1]           [pod2]
	// [        node1        ]
	// |                 |     <- 相同 volume 模式
	// [volume1]   ->  [restored volume1 snapshot]
	f.It("should concurrently access the volume and restored snapshot from pods on the same node [LinuxOnly]", feature.VolumeSnapshotDataSource, feature.VolumeSourceXFS, func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		if !l.driver.GetDriverInfo().Capabilities[storageframework.CapSnapshotDataSource] {
			e2eskipper.Skipf("Driver %q does not support volume snapshots - skipping", dInfo.Name)
		}
		if pattern.SnapshotType == "" {
			e2eskipper.Skipf("Driver %q does not support snapshots - skipping", dInfo.Name)
		}

		// 创建 volume
		testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
		resource := storageframework.CreateVolumeResource(ctx, l.driver, l.config, pattern, testVolumeSizeRange)
		l.resources = append(l.resources, resource)
		pvcs := []*v1.PersistentVolumeClaim{resource.Pvc}

		// 创建它的 snapshot
		expectedContent := fmt.Sprintf("volume content %d", time.Now().UTC().UnixNano())
		sDriver, ok := driver.(storageframework.SnapshottableTestDriver)
		if !ok {
			framework.Failf("Driver %q has CapSnapshotDataSource but does not implement SnapshottableTestDriver", dInfo.Name)
		}
		testConfig := storageframework.ConvertTestConfig(l.config)
		dc := l.config.Framework.DynamicClient
		dataSourceRef := prepareSnapshotDataSourceForProvisioning(ctx, f, testConfig, l.config, pattern, l.cs, dc, resource.Pvc, resource.Sc, sDriver, pattern.VolMode, expectedContent)

		// 创建第二个用于测试的 PVC
		pvc2 := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resource.Pvc.Name + "-restored",
				Namespace: resource.Pvc.Namespace,
			},
		}
		resource.Pvc.Spec.DeepCopyInto(&pvc2.Spec)
		pvc2.Spec.VolumeName = ""
		pvc2.Spec.DataSourceRef = dataSourceRef

		pvc2, err := l.cs.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		pvcs = append(pvcs, pvc2)
		ginkgo.DeferCleanup(framework.IgnoreNotFound(l.cs.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Delete), pvc2.Name, metav1.DeleteOptions{})

		// 测试在同一节点上访问两个 volume。
		TestConcurrentAccessToRelatedVolumes(ctx, l.config.Framework, l.cs, l.ns.Name, l.config.ClientNodeSelection, pvcs, expectedContent)
	})

	// 本测试验证以下配置：
	// [pod1]           [pod2]
	// [        node1        ]
	// |                 |     <- 相同 volume 模式
	// [volume1]   ->  [cloned volume1]
	f.It("should concurrently access the volume and its clone from pods on the same node [LinuxOnly]", feature.VolumeSourceXFS, func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		if !l.driver.GetDriverInfo().Capabilities[storageframework.CapPVCDataSource] {
			e2eskipper.Skipf("Driver %q does not support volume clone - skipping", dInfo.Name)
		}

		// 创建 volume
		expectedContent := fmt.Sprintf("volume content %d", time.Now().UTC().UnixNano())
		testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
		resource := storageframework.CreateVolumeResource(ctx, l.driver, l.config, pattern, testVolumeSizeRange)
		l.resources = append(l.resources, resource)
		pvcs := []*v1.PersistentVolumeClaim{resource.Pvc}
		testConfig := storageframework.ConvertTestConfig(l.config)
		dataSourceRef := preparePVCDataSourceForProvisioning(ctx, f, testConfig, l.cs, resource.Pvc, resource.Sc, pattern.VolMode, expectedContent)

		// 创建第二个用于测试的 PVC
		pvc2 := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resource.Pvc.Name + "-cloned",
				Namespace: resource.Pvc.Namespace,
			},
		}
		resource.Pvc.Spec.DeepCopyInto(&pvc2.Spec)
		pvc2.Spec.VolumeName = ""
		pvc2.Spec.DataSourceRef = dataSourceRef

		pvc2, err := l.cs.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Create(ctx, pvc2, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		pvcs = append(pvcs, pvc2)
		ginkgo.DeferCleanup(framework.IgnoreNotFound(l.cs.CoreV1().PersistentVolumeClaims(pvc2.Namespace).Delete), pvc2.Name, metav1.DeleteOptions{})

		// 测试在同一节点上访问两个 volume。
		TestConcurrentAccessToRelatedVolumes(ctx, l.config.Framework, l.cs, l.ns.Name, l.config.ClientNodeSelection, pvcs, expectedContent)
	})

	// 本测试验证以下配置：
	// [pod1] [pod2]
	// [   node1   ]
	// \      /     <- 相同 volume 模式（只读）
	// [volume1]
	ginkgo.It("should concurrently access the single read-only volume from pods on the same node", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		numPods := 2

		if !l.driver.GetDriverInfo().Capabilities[storageframework.CapMultiPODs] {
			e2eskipper.Skipf("Driver %q does not support multiple concurrent pods - skipping", dInfo.Name)
		}

		if l.driver.GetDriverInfo().Name == "vsphere" && reflect.DeepEqual(pattern, storageframework.BlockVolModeDynamicPV) {
			e2eskipper.Skipf("Driver %q does not support read only raw block volumes - skipping", dInfo.Name)
		}

		// 创建 volume
		testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
		resource := storageframework.CreateVolumeResource(ctx, l.driver, l.config, pattern, testVolumeSizeRange)
		l.resources = append(l.resources, resource)

		// 在 volume 上初始化文件系统 —— 下面会以只读方式挂载。
		initializeVolume(ctx, l.cs, f.Timeouts, l.ns.Name, resource.Pvc, l.config.ClientNodeSelection)

		// 测试在单节点上从 pod 访问 volume
		TestConcurrentAccessToSingleVolume(ctx, l.config.Framework, l.cs, l.ns.Name,
			l.config.ClientNodeSelection, resource.Pvc, numPods, true /* sameNode */, true /* readOnly */)
	})

	// 本测试验证以下配置：
	// [pod1] [pod2]
	// [   node1   ] [   node2   ]
	// \      /     <- 相同 volume 模式
	// [volume1]
	ginkgo.It("should concurrently access the single volume from pods on different node", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		numPods := 2

		if !l.driver.GetDriverInfo().Capabilities[storageframework.CapRWX] {
			e2eskipper.Skipf("Driver %s doesn't support %v -- skipping", l.driver.GetDriverInfo().Name, storageframework.CapRWX)
		}

		// 检查不同节点测试的前置条件
		if l.config.ClientNodeSelection.Name != "" {
			e2eskipper.Skipf("Driver %q requires to deploy on a specific node - skipping", l.driver.GetDriverInfo().Name)
		}
		// 多节点测试需要足够多拥有相同 topology 的节点来调度 pod
		if err := ensureTopologyRequirements(ctx, &l.config.ClientNodeSelection, l.cs, dInfo, 2); err != nil {
			framework.Failf("Error setting topology requirements: %v", err)
		}

		// 创建 volume
		testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
		resource := storageframework.CreateVolumeResourceWithAccessModes(
			ctx,
			l.driver,
			l.config,
			pattern,
			testVolumeSizeRange,
			[]v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			nil /* vacName */)
		l.resources = append(l.resources, resource)

		// 测试从不同节点上的 pod 访问 volume
		TestConcurrentAccessToSingleVolume(ctx, l.config.Framework, l.cs, l.ns.Name,
			l.config.ClientNodeSelection, resource.Pvc, numPods, false /* sameNode */, false /* readOnly */)
	})
}

// testAccessMultipleVolumes 测试在指定节点上从单个 pod 访问多个 volume
// 如果 readSeedBase > 0，先做读测试，假设已有数据写入。
func testAccessMultipleVolumes(ctx context.Context, f *framework.Framework, cs clientset.Interface, ns string,
	node e2epod.NodeSelection, pvcs []*v1.PersistentVolumeClaim, readSeedBase int64, writeSeedBase int64) string {
	ginkgo.By(fmt.Sprintf("Creating pod on %+v with multiple volumes", node))
	podConfig := e2epod.Config{
		NS:            ns,
		PVCs:          pvcs,
		SeLinuxLabel:  e2epod.GetLinuxLabel(),
		NodeSelection: node,
		ImageID:       e2epod.GetDefaultTestImageID(),
	}
	pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, cs, &podConfig, f.Timeouts.PodStart)
	defer func() {
		framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, pod))
	}()
	framework.ExpectNoError(err)

	byteLen := 64
	for i, pvc := range pvcs {
		// CreateSecPodWithNodeSelection 让 volume 可通过 /mnt/volume({i} + 1) 访问
		index := i + 1
		path := fmt.Sprintf("/mnt/volume%d", index)
		ginkgo.By(fmt.Sprintf("Checking if the volume%d exists as expected volume mode (%s)", index, *pvc.Spec.VolumeMode))
		err = e2evolume.CheckVolumeModeOfPath(ctx, f, pod, *pvc.Spec.VolumeMode, path)
		framework.ExpectNoError(err)

		if readSeedBase > 0 {
			ginkgo.By(fmt.Sprintf("Checking if read from the volume%d works properly", index))
			storageutils.CheckReadFromPath(ctx, f, pod, *pvc.Spec.VolumeMode, false, path, byteLen, readSeedBase+int64(i))
		}

		ginkgo.By(fmt.Sprintf("Checking if write to the volume%d works properly", index))
		storageutils.CheckWriteToPath(ctx, f, pod, *pvc.Spec.VolumeMode, false, path, byteLen, writeSeedBase+int64(i))

		ginkgo.By(fmt.Sprintf("Checking if read from the volume%d works properly", index))
		storageutils.CheckReadFromPath(ctx, f, pod, *pvc.Spec.VolumeMode, false, path, byteLen, writeSeedBase+int64(i))
	}

	pod, err = cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "get pod")
	return pod.Spec.NodeName
}

// TestAccessMultipleVolumesAcrossPodRecreation 测试从单个 pod 访问多个 volume，
// 然后根据 requiresSameNode 在相同或不同节点上重建 pod，
// 并重新检查重建的 pod 能否访问这些 volume
func TestAccessMultipleVolumesAcrossPodRecreation(ctx context.Context, f *framework.Framework, cs clientset.Interface, ns string,
	node e2epod.NodeSelection, pvcs []*v1.PersistentVolumeClaim, requiresSameNode bool) {

	// volume 中没有写入数据，所以传负值
	readSeedBase := int64(-1)
	writeSeedBase := time.Now().UTC().UnixNano()
	// 测试在指定节点上访问多个 volume
	nodeName := testAccessMultipleVolumes(ctx, f, cs, ns, node, pvcs, readSeedBase, writeSeedBase)

	// 根据 requiresSameNode 设置 affinity
	if requiresSameNode {
		e2epod.SetAffinity(&node, nodeName)
	} else {
		e2epod.SetAntiAffinity(&node, nodeName)
	}

	// 在上面更新的节点上再次测试访问多个 volume
	// 把之前的 writeSeed 设为当前 readSeed，检查之前的数据被保留
	readSeedBase = writeSeedBase
	// 用新值更新 writeSeed
	writeSeedBase = time.Now().UTC().UnixNano()
	_ = testAccessMultipleVolumes(ctx, f, cs, ns, node, pvcs, readSeedBase, writeSeedBase)
}

// TestConcurrentAccessToSingleVolume 测试多个 pod 访问单个 volume，
// 然后删除最后一个 pod，并在删除后重新检查访问，以确认其他
// pod 删除没有影响。根据 requiresSameNode，pod 部署在同一或不同节点。
// 读写检查跨 pod 进行：在 pod{n} 中读取 pod{n-1} 和 pod{n} 写入的内容。
func TestConcurrentAccessToSingleVolume(ctx context.Context, f *framework.Framework, cs clientset.Interface, ns string,
	node e2epod.NodeSelection, pvc *v1.PersistentVolumeClaim, numPods int, requiresSameNode bool,
	readOnly bool) {

	var pods []*v1.Pod

	// 用 pvc 创建每个 pod
	for i := range numPods {
		index := i + 1
		ginkgo.By(fmt.Sprintf("Creating pod%d with a volume on %+v", index, node))
		podConfig := e2epod.Config{
			NS:            ns,
			PVCs:          []*v1.PersistentVolumeClaim{pvc},
			SeLinuxLabel:  e2epod.GetLinuxLabel(),
			NodeSelection: node,
			PVCsReadOnly:  readOnly,
			ImageID:       e2epod.GetTestImageID(imageutils.GlibcDnsTesting),
		}
		pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, cs, &podConfig, f.Timeouts.PodStart)
		framework.ExpectNoError(err)
		// pod 必须在本函数返回前删除，因为调用方可能会
		// 在测试中删除 volume。pod 继续运行会阻塞删除。
		// 如果测试超时，namespace 删除会处理它。
		defer func() {
			framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, pod))
		}()
		pod, err = cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		pods = append(pods, pod)
		framework.ExpectNoError(err, fmt.Sprintf("get pod%d", index))
		actualNodeName := pod.Spec.NodeName

		// 根据 requiresSameNode 设置 affinity
		if requiresSameNode {
			e2epod.SetAffinity(&node, actualNodeName)
		} else {
			e2epod.SetAntiAffinity(&node, actualNodeName)
		}
	}

	path := "/mnt/volume1"

	var seed int64
	byteLen := 64
	directIO := false
	// Block 模式的 PV 需要直接 IO
	if *pvc.Spec.VolumeMode == v1.PersistentVolumeBlock {
		if len(pods) < 1 {
			framework.Failf("Number of pods shouldn't be less than 1, but got %d", len(pods))
		}
		// byteLen 应为扇区大小，以启用直接 I/O
		byteLen = storageutils.GetSectorSize(ctx, f, pods[0], path)
		directIO = true
	}

	// 检查每个 pod 都能访问 volume
	for i, pod := range pods {
		index := i + 1
		ginkgo.By(fmt.Sprintf("Checking if the volume in pod%d exists as expected volume mode (%s)", index, *pvc.Spec.VolumeMode))
		err := e2evolume.CheckVolumeModeOfPath(ctx, f, pod, *pvc.Spec.VolumeMode, path)
		framework.ExpectNoError(err)

		if readOnly {
			ginkgo.By("Skipping volume content checks, volume is read-only")
			continue
		}

		if i != 0 {
			ginkgo.By(fmt.Sprintf("From pod%d, checking if reading the data that pod%d write works properly", index, index-1))
			// 第一个 pod 没有其他人写入数据，所以跳过读检查
			storageutils.CheckReadFromPath(ctx, f, pod, *pvc.Spec.VolumeMode, directIO, path, byteLen, seed)
		}

		// 更新 seed 并检查读写是否正常
		seed = time.Now().UTC().UnixNano()

		ginkgo.By(fmt.Sprintf("Checking if write to the volume in pod%d works properly", index))
		storageutils.CheckWriteToPath(ctx, f, pod, *pvc.Spec.VolumeMode, directIO, path, byteLen, seed)

		ginkgo.By(fmt.Sprintf("Checking if read from the volume in pod%d works properly", index))
		storageutils.CheckReadFromPath(ctx, f, pod, *pvc.Spec.VolumeMode, directIO, path, byteLen, seed)
	}

	if len(pods) < 2 {
		framework.Failf("Number of pods shouldn't be less than 2, but got %d", len(pods))
	}
	// 删除最后一个 pod 并从 pod 切片中移除
	lastPod := pods[len(pods)-1]
	framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, lastPod))
	pods = pods[:len(pods)-1]

	// 删除最后一个 pod 后，重新检查每个 pod 能否访问 pv
	for i, pod := range pods {
		index := i + 1
		// pod 索引和 pvc 索引匹配，因为 pod 是按上面的方式创建的
		ginkgo.By(fmt.Sprintf("Rechecking if the volume in pod%d exists as expected volume mode (%s)", index, *pvc.Spec.VolumeMode))
		err := e2evolume.CheckVolumeModeOfPath(ctx, f, pod, *pvc.Spec.VolumeMode, "/mnt/volume1")
		framework.ExpectNoError(err)

		if readOnly {
			ginkgo.By("Skipping volume content checks, volume is read-only")
			continue
		}

		if i == 0 {
			// 这次第一个 pod 应该有最后一个 pod 写入的数据
			ginkgo.By(fmt.Sprintf("From pod%d, rechecking if reading the data that last pod write works properly", index))
		} else {
			ginkgo.By(fmt.Sprintf("From pod%d, rechecking if reading the data that pod%d write works properly", index, index-1))
		}
		storageutils.CheckReadFromPath(ctx, f, pod, *pvc.Spec.VolumeMode, directIO, path, byteLen, seed)

		// 更新 seed 并检查读写是否正常
		seed = time.Now().UTC().UnixNano()

		ginkgo.By(fmt.Sprintf("Rechecking if write to the volume in pod%d works properly", index))
		storageutils.CheckWriteToPath(ctx, f, pod, *pvc.Spec.VolumeMode, directIO, path, byteLen, seed)

		ginkgo.By(fmt.Sprintf("Rechecking if read from the volume in pod%d works properly", index))
		storageutils.CheckReadFromPath(ctx, f, pod, *pvc.Spec.VolumeMode, directIO, path, byteLen, seed)
	}
}

// TestConcurrentAccessToRelatedVolumes 测试多个 pod 访问多个 volume。
// 每个提供的 PVC 由一个 pod 使用。测试确保从
// 另一个 volume（clone）或 volume snapshot 创建的 volume 能与原 volume 一起使用。
func TestConcurrentAccessToRelatedVolumes(ctx context.Context, f *framework.Framework, cs clientset.Interface, ns string,
	node e2epod.NodeSelection, pvcs []*v1.PersistentVolumeClaim, expectedContent string) {

	var pods []*v1.Pod

	// 用 pvc 创建每个 pod
	for i := range pvcs {
		index := i + 1
		ginkgo.By(fmt.Sprintf("Creating pod%d with a volume on %+v", index, node))
		podConfig := e2epod.Config{
			NS:            ns,
			PVCs:          []*v1.PersistentVolumeClaim{pvcs[i]},
			SeLinuxLabel:  e2epod.GetLinuxLabel(),
			NodeSelection: node,
			PVCsReadOnly:  false,
			ImageID:       e2epod.GetTestImageID(imageutils.GlibcDnsTesting),
		}
		pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, cs, &podConfig, f.Timeouts.PodStart)
		defer func() {
			framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, pod))
		}()
		framework.ExpectNoError(err)
		pods = append(pods, pod)
		actualNodeName := pod.Spec.NodeName

		// 始终让后续 pod 在同一节点上运行。
		e2epod.SetAffinity(&node, actualNodeName)
	}

	for i, pvc := range pvcs {
		var commands []string

		if *pvc.Spec.VolumeMode == v1.PersistentVolumeBlock {
			fileName := "/mnt/volume1"
			commands = e2evolume.GenerateReadBlockCmd(fileName, len(expectedContent))
			// 检查所有 pod 内容相同
			index := i + 1
			ginkgo.By(fmt.Sprintf("Checking if the volume in pod%d has expected initial content", index))
			_, err := e2eoutput.LookForStringInPodExec(pods[i].Namespace, pods[i].Name, commands, expectedContent, time.Minute)
			framework.ExpectNoError(err, "failed: finding the contents of the block volume %s.", fileName)
		} else {
			fileName := "/mnt/volume1/index.html"
			commands = e2evolume.GenerateReadFileCmd(fileName)
			// 检查所有 pod 内容相同
			index := i + 1
			ginkgo.By(fmt.Sprintf("Checking if the volume in pod%d has expected initial content", index))
			_, err := e2eoutput.LookForStringInPodExec(pods[i].Namespace, pods[i].Name, commands, expectedContent, time.Minute)
			framework.ExpectNoError(err, "failed: finding the contents of the mounted file %s.", fileName)
		}
	}
}

// getCurrentTopologies() 遍历所有 Node，返回唯一的 driver topology 及每个 topology 的 Node 数
func getCurrentTopologiesNumber(cs clientset.Interface, nodes *v1.NodeList, keys []string) ([]topology, []int, error) {
	topos := []topology{}
	topoCount := []int{}

	// TODO: 扩缩容？
	for _, n := range nodes.Items {
		topo := map[string]string{}
		for _, k := range keys {
			v, ok := n.Labels[k]
			if ok {
				topo[k] = v
			}
		}

		found := false
		for i, existingTopo := range topos {
			if topologyEqual(existingTopo, topo) {
				found = true
				topoCount[i]++
				break
			}
		}
		if !found && len(topo) > 0 {
			framework.Logf("found topology %v", topo)
			topos = append(topos, topo)
			topoCount = append(topoCount, 1)
		}
	}
	return topos, topoCount, nil
}

// ensureTopologyRequirements 检查集群中是否有足够的节点供测试使用，
// 并对提供 topology key 的 driver 按给定 key 设置 nodeSelection affinity。
func ensureTopologyRequirements(ctx context.Context, nodeSelection *e2epod.NodeSelection, cs clientset.Interface, driverInfo *storageframework.DriverInfo, minCount int) error {
	nodes, err := e2enode.GetReadySchedulableNodes(ctx, cs)
	framework.ExpectNoError(err)
	if len(nodes.Items) < minCount {
		e2eskipper.Skipf("Number of available nodes is less than %d - skipping", minCount)
	}

	topologyKeys := driverInfo.TopologyKeys
	if len(topologyKeys) == 0 {
		// driver 没有任何 topology 限制
		return nil
	}

	topologyList, topologyCount, err := getCurrentTopologiesNumber(cs, nodes, topologyKeys)
	if err != nil {
		return err
	}
	suitableTopologies := []topology{}
	for i, topo := range topologyList {
		if topologyCount[i] >= minCount {
			suitableTopologies = append(suitableTopologies, topo)
		}
	}
	if len(suitableTopologies) == 0 {
		e2eskipper.Skipf("No topology with at least %d nodes found - skipping", minCount)
	}
	// 取第一个合适的 topology
	e2epod.SetNodeAffinityTopologyRequirement(nodeSelection, suitableTopologies[0])
	return nil
}

// initializeVolume 在给定 volume 上创建文件系统，以便之后以只读方式使用
func initializeVolume(ctx context.Context, cs clientset.Interface, t *framework.TimeoutContext, ns string, pvc *v1.PersistentVolumeClaim, node e2epod.NodeSelection) {
	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode == v1.PersistentVolumeBlock {
		// Block volume 不需要初始化。
		return
	}

	ginkgo.By(fmt.Sprintf("Initializing a filesystem on PVC %s", pvc.Name))
	// 只要以读写方式创建一个带该 volume 的 pod，Kubernetes 会
	// 在文件系统不存在时创建它。
	podConfig := e2epod.Config{
		NS:            ns,
		PVCs:          []*v1.PersistentVolumeClaim{pvc},
		SeLinuxLabel:  e2epod.GetLinuxLabel(),
		NodeSelection: node,
		ImageID:       e2epod.GetDefaultTestImageID(),
	}
	pod, err := e2epod.CreateSecPod(ctx, cs, &podConfig, t.PodStart)
	defer func() {
		framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, cs, pod))
	}()
	framework.ExpectNoError(err)
}
