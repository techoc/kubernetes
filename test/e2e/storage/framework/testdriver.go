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

package framework

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/test/e2e/framework"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
)

// TestDriver 表示在 TestSuite 中待测 driver 的接口。
// 除 GetDriverInfo 外，所有方法都在测试运行时调用，因此
// 可以使用 e2eskipper.Skipf、framework.Fatal、Gomega 断言等。
type TestDriver interface {
	// GetDriverInfo 返回 TestDriver 的 DriverInfo。这些信息必须是静态的。
	//
	GetDriverInfo() *DriverInfo

	// 如果 Testpattern 不适合用该 TestDriver 测试，SkipUnsupportedTest 返回跳过测试的原因。
	// 它在解析测试套件参数之后、
	// framework 初始化之前被调用。
	//
	//
	// 依赖集群连接的操作可以在 framework 就绪后
	// 于 PrepareTest 中完成。
	//
	SkipUnsupportedTest(TestPattern) string

	// PrepareTest 在测试执行时、每个新测试用例即将开始前调用。
	// 它设置所有必要资源，并返回每个测试的配置。
	// 清理通过 PrepareTest 中的 ginkgo.DeferCleanup 处理。
	PrepareTest(ctx context.Context, f *framework.Framework) *PerTestConfig
}

// TestVolume 是 PreprovisionedVolumeTestDriver.CreateVolume 的结果。
// 唯一的通用功能是删除它。各 driver 接口
// 有自己处理其创建 volume 的额外方法。
type TestVolume interface {
	DeleteVolume(ctx context.Context)
}

// PreprovisionedVolumeTestDriver 表示拥有预置 volume 的 TestDriver 接口
type PreprovisionedVolumeTestDriver interface {
	TestDriver
	// CreateVolume 创建所需 volume 类型的预置 volume。
	CreateVolume(ctx context.Context, config *PerTestConfig, volumeType TestVolType) TestVolume
}

// InlineVolumeTestDriver 表示支持 InlineVolume 的 TestDriver 接口
type InlineVolumeTestDriver interface {
	PreprovisionedVolumeTestDriver

	// GetVolumeSource 返回 inline volume 的 volumeSource。
	// 如果 TestDriver 同时支持 readOnly 和 fsType，会把它们设置到 volumeSource。
	// 如果 TestDriver 不支持其中任何一个参数，则返回 nil。
	GetVolumeSource(readOnly bool, fsType string, testVolume TestVolume) *v1.VolumeSource
}

// PreprovisionedPVTestDriver 表示支持 PreprovisionedPV 的 TestDriver 接口
type PreprovisionedPVTestDriver interface {
	PreprovisionedVolumeTestDriver
	// GetPersistentVolumeSource 返回一个带 volume node affinity 的 PersistentVolumeSource，用于预置 PV。
	// 如果 TestDriver 同时支持 readOnly 和 fsType，会把它们设置到 PersistentVolumeSource。
	// 如果 TestDriver 不支持其中任何一个参数，则返回 nil。
	GetPersistentVolumeSource(readOnly bool, fsType string, testVolume TestVolume) (*v1.PersistentVolumeSource, *v1.VolumeNodeAffinity)
}

// DynamicPVTestDriver 表示支持 DynamicPV 的 TestDriver 接口
type DynamicPVTestDriver interface {
	TestDriver
	// GetDynamicProvisionStorageClass 返回一个动态供应 Persistent Volume 的 StorageClass。
	// StorageClass 必须有
	// 唯一的名称，因为每个测试中 GetDynamicProvisionStorageClass 可能
	// 被调用多次。
	// 如果 TestDriver 支持 fsType，会设置到 StorageClass。
	// 如果 TestDriver 不支持，则返回 nil。
	GetDynamicProvisionStorageClass(ctx context.Context, config *PerTestConfig, fsType string) *storagev1.StorageClass
}

// EphemeralTestDriver 表示支持 ephemeral inline volume 的 TestDriver 接口。
type EphemeralTestDriver interface {
	TestDriver

	// GetVolume 返回某个 inline ephemeral volume 的属性，
	// 编号从 #0 开始。有些
	// 测试可能需要多个 volume。它们可以
	// 相同或不同，取决于 driver 支持
	// 和/或想测试什么。
	//
	// 对每个 volume，test driver 可以返回 volume 属性、
	// 结果 volume 是否在不同 pod 间共享（即
	// 一个 pod 中的修改在另一个中可见），以及
	// volume 能否以读写或仅只读方式挂载。
	GetVolume(config *PerTestConfig, volumeNumber int) (attributes map[string]string, shared bool, readOnly bool)

	// GetCSIDriverName 返回向 kubelet 注册时使用的名称。取决于
	// driver 的部署方式，它可能与
	// DriverInfo.Name 不同。从 Kubernetes 1.16 起，必须还有
	// 一个同名的 CSIDriver 对象，其 "mode" 字段要启用
	// 该 driver 用于 ephemeral inline volume。
	GetCSIDriverName(config *PerTestConfig) string
}

// SnapshottableTestDriver 表示支持 DynamicSnapshot 的 TestDriver 接口
type SnapshottableTestDriver interface {
	TestDriver
	// GetSnapshotClass 返回一个用于创建 snapshot 的 SnapshotClass。
	// 如果 TestDriver 不支持，则返回 nil。
	GetSnapshotClass(ctx context.Context, config *PerTestConfig, parameters map[string]string) *unstructured.Unstructured
}

type SnapshotMetadataTestDriver interface {
	TestDriver
}

// VolumeGroupSnapshottableTestDriver 表示支持 DynamicGroupSnapshot 的 TestDriver 接口
type VolumeGroupSnapshottableTestDriver interface {
	TestDriver
	// GetVolumeGroupSnapshotClass 返回一个用于创建 group snapshot 的 VolumeGroupSnapshotClass。
	GetVolumeGroupSnapshotClass(ctx context.Context, config *PerTestConfig, parameters map[string]string) *unstructured.Unstructured
}

// VolumeAttributesClassTestDriver 表示支持
// 通过 VolumeAttributesClass 对象创建和修改 volume 的 TestDriver 接口
type VolumeAttributesClassTestDriver interface {
	TestDriver
	// GetVolumeAttributesClass 返回一个用于创建/修改 PVC 的 VolumeAttributesClass
	// 如果 TestDriver 不支持 VAC，则返回 nil
	GetVolumeAttributesClass(ctx context.Context, config *PerTestConfig) *storagev1.VolumeAttributesClass
}

// CustomTimeoutsTestDriver 表示支持自定义超时的 TestDriver 接口
type CustomTimeoutsTestDriver interface {
	TestDriver
	GetTimeouts() *framework.TimeoutContext
}

// GetDriverTimeouts 返回 driver 操作的超时时间
func GetDriverTimeouts(driver TestDriver) *framework.TimeoutContext {
	if d, ok := driver.(CustomTimeoutsTestDriver); ok {
		return d.GetTimeouts()
	}
	return framework.NewTimeoutContext()
}

// Capability 表示 volume plugin 支持的一项特性
type Capability string

// 与 driver 的能力和行为相关的常量。
const (
	CapPersistence         Capability = "persistence"        // data is persisted across pod restarts
	CapBlock               Capability = "block"              // raw block mode
	CapFsGroup             Capability = "fsGroup"            // volume ownership via fsGroup
	CapVolumeMountGroup    Capability = "volumeMountGroup"   // Driver has the VolumeMountGroup CSI node capability. Because this is a FSGroup feature, the fsGroup capability must also be set to true.
	CapExec                Capability = "exec"               // exec a file in the volume
	CapSnapshotDataSource  Capability = "snapshotDataSource" // support populate data from snapshot
	CapSnapshotMetadata    Capability = "snapshotMetadata"   // support snapshot metadata
	CapVolumeGroupSnapshot Capability = "groupSnapshot"      // support group snapshot
	CapPVCDataSource       Capability = "pvcDataSource"      // support populate data from pvc

	// 同一节点上的多个 pod 可以并发使用同一个 volume；
	// 对于 CSI，参见：
	// - https://github.com/container-storage-interface/spec/pull/150
	// - https://github.com/container-storage-interface/spec/issues/178
	// - 规范中的 NodeStageVolume
	CapMultiPODs Capability = "multipods"

	CapRWX                 Capability = "RWX"                 // support ReadWriteMany access modes
	CapControllerExpansion Capability = "controllerExpansion" // support volume expansion for controller
	CapNodeExpansion       Capability = "nodeExpansion"       // support volume expansion for node

	// 当 controllerExpansion 为 true 时，offlineExpansion 和 onlineExpansion 都默认为 true。
	// 只有在 CSI driver 只能在 volume 挂载到 pod 时扩容的情况下，
	// 才需要把 offlineExpansion 设为 false。反之，如果 driver
	// 只能在 volume 未挂载时扩容，则可把 onlineExpansion 设为 false。
	//
	CapOfflineExpansion Capability = "offlineExpansion" // supports offline volume expansion (default: true)
	CapOnlineExpansion  Capability = "onlineExpansion"  // supports online volume expansion (default: true)

	CapVolumeLimits     Capability = "volumeLimits"     // support volume limits (can be *very* slow)
	CapSingleNodeVolume Capability = "singleNodeVolume" // support volume that can run on single node (like hostpath)
	CapTopology         Capability = "topology"         // support topology

	// driver 发布存储容量信息：当用于动态供应的 storage class
	// 存在时，driver 应为其提供
	// 容量信息。
	CapCapacity Capability = "capacity"

	// 对不支持调整从快照克隆或恢复的 PVC 文件系统的 driver，
	// 这是一个反向能力标记。
	CapFSResizeFromSourceNotSupported Capability = "FSResizeFromSourceNotSupported"

	// 要支持 ReadWriteOncePod，以下 CSI sidecar 必须
	// 更新到这些版本或更高：
	// - csi-provisioner:v3.0.0+
	// - csi-attacher:v3.3.0+
	// - csi-resizer:v1.3.0+
	CapReadWriteOncePod Capability = "readWriteOncePod"

	// driver 能处理两个 VolumeHandle 相同（CSI 规范中的 volume_id）的 PersistentVolume。
	// 对于支持 ReadWriteMany 访问模式的 volume，强烈建议具备此能力，
	// 因为为同一 VolumeHandle 创建多个 PV 常被用于
	// 在多个 namespace 间共享单个 volume。
	// 注意，只有那些突破 CSI 边界、会检查 Kubernetes PersistentVolume 对象的 CSI driver
	// 才需要禁用此能力。只实现 CSI、不以任何方式与 Kubernetes API server
	// 通信的 CSI driver 应保持此能力启用，因为
	// 它们看到的 NodeStage / NodePublish 请求与只有一个 PV 时相同。
	CapMultiplePVsSameID Capability = "multiplePVsSameID"

	// driver 支持 ReadOnlyMany (ROX) 访问模式
	CapReadOnlyMany Capability = "capReadOnlyMany"

	// driver 支持 SELinuxMount 特性
	CapSELinuxMount Capability = "seLinuxMount"
)

// DriverInfo 表示 TestDriver 的静态信息。
type DriverInfo struct {
	// driver 的内部名称，用作测试用例和对象中的
	// 显示名称
	Name string
	// 如果 in-tree plugin 存在，为其在 Kubernetes 中注册的完全限定 plugin 名称；
	// 如果该 DriverInfo 表示 CSI
	// Driver，则为空
	InTreePluginName string
	TestTags         []interface{} // tags for the driver (e.g. framework.WithSlow())

	// 该 driver 支持的最大单文件大小
	MaxFileSize int64
	// 该 driver 支持的磁盘大小范围
	SupportedSizeRange e2evolume.SizeRange
	// 支持的 fs type 的字符串 map
	SupportedFsType sets.String
	// 支持的 mount option 的字符串 map
	SupportedMountOption sets.String
	// [可选] 必需的 mount option 的字符串 map
	RequiredMountOption sets.String
	// 表示 plugin 能力的 map
	Capabilities map[Capability]bool
	// [可选] 供应所需的访问模式列表，未设置时默认为
	// RWO
	RequiredAccessModes []v1.PersistentVolumeAccessMode
	// [可选] driver 支持的 topology key 列表
	TopologyKeys []string
	// [可选] driver 要求的允许 topology 数量。
	// 仅在设置了 TopologyKeys 时相关。默认为 1。
	// 例如：多可用区磁盘至少需要 2 个允许的 topology。
	NumAllowedTopologies int
	// [可选] 压力测试的规模参数。
	// TODO(#96241): 重命名此字段，以反映使用它的测试。
	StressTestOptions *StressTestOptions
	// [可选] volume snapshot 压力测试的规模参数。
	VolumeSnapshotStressTestOptions *VolumeSnapshotStressTestOptions
	// [可选] volume modify 压力测试的规模参数。
	VolumeModifyStressTestOptions *VolumeModifyStressTestOptions
	// [可选] 性能测试参数
	PerformanceTestOptions *PerformanceTestOptions
	// [可选] volume group snapshot 压力测试的规模参数。
	VolumeGroupSnapshotStressTestOptions *VolumeGroupSnapshotStressTestOptions
}

// StressTestOptions 包含压力测试使用的参数。
type StressTestOptions struct {
	// 测试中创建的 pod 数。每个 pod 最多也会创建
	// 1 个 volume。
	NumPods int
	// 每个 Pod 重启的次数。
	NumRestarts int
}

// VolumeSnapshotStressTestOptions 包含 volume snapshot 压力测试使用的参数。
type VolumeSnapshotStressTestOptions struct {
	// 测试中创建的 pod 数。每个 pod 最多也会创建
	// 1 个 volume。
	NumPods int
	// 每个 volume 创建的 snapshot 数量。
	NumSnapshots int
}

// VolumeModifyStressTestOptions 包含 volume modify 压力测试使用的参数。
type VolumeModifyStressTestOptions struct {
	// 测试中创建的 pod 数。每个 pod 最多也会创建
	// 1 个带 volumeAttributesClass 的 volume。
	NumPods int
}

// 用于评估操作性能的指标
// TODO: 添加中位数、众数、标准差、百分位等指标
type Metrics struct {
	AvgLatency time.Duration
	Throughput float64
}

// PerformanceTestProvisioningOptions 包含用于
// 测试供应操作性能的参数。
type PerformanceTestProvisioningOptions struct {
	VolumeSize string
	Count      int
	// 从 PVC 创建到 PVC 变为 Bound 的预期指标。
	ExpectedMetrics *Metrics
}

// PerformanceTestOptions 包含性能测试使用的参数
type PerformanceTestOptions struct {
	ProvisioningOptions *PerformanceTestProvisioningOptions
}

// VolumeGroupSnapshotStressTestOptions 包含 volume group snapshot 压力测试使用的参数。
type VolumeGroupSnapshotStressTestOptions struct {
	// StatefulSet 中创建的 pod 数。这将创建
	// 相同数量的 PVC，带相同标签用于 group snapshot。
	NumPods int
	// 要创建的 volume group snapshot 数量。
	NumSnapshots int
}
