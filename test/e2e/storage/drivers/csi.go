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

/*
 * 本文件为 TestSuites 定义各种 csi volume 测试 driver。
 *
 * 准备测试 driver 有两种方式：
 * 1）使用容器化 server（NFS、Ceph、iSCSI 等）
 * 它会创建一个 server Pod，为测试定义一个卷。
 * 这些测试只有在允许特权容器时才能工作，因为导出
 * 各种文件系统（如 NFS）通常需要在 server Pod 中挂载或
 * 使用其他特权手段。
 *
 * 注意 server 容器仅用于测试，不应
 * 用于生产环境。
 *
 * 2）使用 Kubernetes 之外的 server 或云 provider（Cinder、GCE、AWS、Azure 等）
 * 相应的 server 或云 provider 必须存在于被测 Kubernetes 集群之外的某处。
 * CreateVolume 会创建一个新卷，供 TestSuites 中的 inlineVolume 或 DynamicPV 测试使用。
 *
 */

package drivers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	mockdriver "k8s.io/kubernetes/test/e2e/storage/drivers/csi-test/driver"
	mockservice "k8s.io/kubernetes/test/e2e/storage/drivers/csi-test/mock/service"
	"k8s.io/kubernetes/test/e2e/storage/drivers/proxy"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"

	"google.golang.org/grpc"
)

const (
	// GCEPDCSIDriverName 是 GCE Persistent Disk CSI driver 的名称
	GCEPDCSIDriverName = "pd.csi.storage.gke.io"
	// GCEPDCSIZoneTopologyKey 是 GCE Persistent Disk CSI zone topology 的 key
	GCEPDCSIZoneTopologyKey = "topology.gke.io/zone"

	// mock driver grpc 日志的前缀
	grpcCallPrefix = "gRPCCall:"

	// hostpath CSI driver VolumeAttributesClass 使用的参数
	// 必须通过 --accepted-mutable-parameter-names 传给 driver
	hostpathCSIDriverMutableParameterName  = "e2eVacTest"
	hostpathCSIDriverMutableParameterValue = "test-value"
)

// hostpathCSI
type hostpathCSIDriver struct {
	driverInfo       storageframework.DriverInfo
	manifests        []string
	volumeAttributes []map[string]string
}

func initHostPathCSIDriver(name string, capabilities map[storageframework.Capability]bool, volumeAttributes []map[string]string, manifests ...string) storageframework.TestDriver {
	return &hostpathCSIDriver{
		driverInfo: storageframework.DriverInfo{
			Name:        name,
			MaxFileSize: storageframework.FileSizeMedium,
			SupportedFsType: sets.NewString(
				"", // 默认 fsType
			),
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
			Capabilities: capabilities,
			StressTestOptions: &storageframework.StressTestOptions{
				NumPods:     10,
				NumRestarts: 10,
			},
			VolumeSnapshotStressTestOptions: &storageframework.VolumeSnapshotStressTestOptions{
				NumPods:      10,
				NumSnapshots: 10,
			},
			VolumeModifyStressTestOptions: &storageframework.VolumeModifyStressTestOptions{
				NumPods: 10,
			},
			VolumeGroupSnapshotStressTestOptions: &storageframework.VolumeGroupSnapshotStressTestOptions{
				NumPods:      5,
				NumSnapshots: 5,
			},
			PerformanceTestOptions: &storageframework.PerformanceTestOptions{
				ProvisioningOptions: &storageframework.PerformanceTestProvisioningOptions{
					VolumeSize: "1Mi",
					Count:      300,
					// volume provisioning 指标与较高的基线比较。
					// 若未通过，说明出现了性能回退。
					ExpectedMetrics: &storageframework.Metrics{
						AvgLatency: 2 * time.Minute,
						Throughput: 0.5,
					},
				},
			},
		},
		manifests:        manifests,
		volumeAttributes: volumeAttributes,
	}
}

var _ storageframework.TestDriver = &hostpathCSIDriver{}
var _ storageframework.DynamicPVTestDriver = &hostpathCSIDriver{}
var _ storageframework.SnapshottableTestDriver = &hostpathCSIDriver{}
var _ storageframework.EphemeralTestDriver = &hostpathCSIDriver{}

// InitHostPathCSIDriver 返回实现 TestDriver 接口的 hostpathCSIDriver
func InitHostPathCSIDriver() storageframework.TestDriver {
	capabilities := map[storageframework.Capability]bool{
		storageframework.CapPersistence:                    true,
		storageframework.CapSnapshotDataSource:             true,
		storageframework.CapMultiPODs:                      true,
		storageframework.CapBlock:                          true,
		storageframework.CapPVCDataSource:                  true,
		storageframework.CapControllerExpansion:            true,
		storageframework.CapOfflineExpansion:               true,
		storageframework.CapOnlineExpansion:                true,
		storageframework.CapSingleNodeVolume:               true,
		storageframework.CapReadWriteOncePod:               true,
		storageframework.CapMultiplePVsSameID:              true,
		storageframework.CapFSResizeFromSourceNotSupported: true,
		// csimock/csi_selinux_mount.go 中有大量测试验证 NodeStage / NodePublish 带 -o context 调用，
		// 但 csi-driver-hostpath 无法真正让 -o context 出现在 CapSELinuxMount 测试期望的 mount table 中。
		storageframework.CapSELinuxMount: false,

		// 这是
		// testsuites/volumelimits.go 中 `should support volume limits`
		// 测试所需的。patch deployment 时会加上
		// --maxvolumespernode=10。
		storageframework.CapVolumeLimits: true,
	}
	// TODO: VolumeGroupSnapshot 特性默认启用后可以移除
	if os.Getenv("CSI_PROW_ENABLE_GROUP_SNAPSHOT") == "true" {
		capabilities[storageframework.CapVolumeGroupSnapshot] = true
	}
	if os.Getenv("CSI_PROW_ENABLE_SNAPSHOT_METADATA") == "true" {
		capabilities[storageframework.CapSnapshotMetadata] = true
	}
	return initHostPathCSIDriver("csi-hostpath",
		capabilities,
		// volume 属性无关紧要，但至少要提供一个 map。
		[]map[string]string{
			{"foo": "bar"},
		},
		"test/e2e/testing-manifests/storage-csi/external-attacher/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-provisioner/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-snapshot-metadata/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-snapshotter/csi-snapshotter/rbac-csi-snapshotter.yaml",
		"test/e2e/testing-manifests/storage-csi/external-health-monitor/external-health-monitor-controller/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-resizer/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/hostpath/hostpath/csi-hostpath-driverinfo.yaml",
		"test/e2e/testing-manifests/storage-csi/hostpath/hostpath/csi-hostpath-plugin.yaml",
		"test/e2e/testing-manifests/storage-csi/hostpath/hostpath/e2e-test-rbac.yaml",
	)
}

func (h *hostpathCSIDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &h.driverInfo
}

func (h *hostpathCSIDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	if pattern.VolType == storageframework.CSIInlineVolume && len(h.volumeAttributes) == 0 {
		return fmt.Sprintf("%s has no volume attributes defined, doesn't support ephemeral inline volumes", h.driverInfo.Name)
	}
	return ""
}

func (h *hostpathCSIDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := config.GetUniqueDriverName()
	parameters := map[string]string{}
	ns := config.Framework.Namespace.Name

	return storageframework.GetStorageClass(provisioner, parameters, nil, ns)
}

func (h *hostpathCSIDriver) GetVolume(config *storageframework.PerTestConfig, volumeNumber int) (map[string]string, bool, bool) {
	return h.volumeAttributes[volumeNumber%len(h.volumeAttributes)], false /* not shared */, false /* read-write */
}

func (h *hostpathCSIDriver) GetCSIDriverName(config *storageframework.PerTestConfig) string {
	return config.GetUniqueDriverName()
}

func (h *hostpathCSIDriver) GetSnapshotClass(ctx context.Context, config *storageframework.PerTestConfig, parameters map[string]string) *unstructured.Unstructured {
	snapshotter := config.GetUniqueDriverName()
	ns := config.Framework.Namespace.Name

	return utils.GenerateSnapshotClassSpec(snapshotter, parameters, ns)
}

func (h *hostpathCSIDriver) GetVolumeAttributesClass(_ context.Context, config *storageframework.PerTestConfig) *storagev1.VolumeAttributesClass {
	return storageframework.CopyVolumeAttributesClass(&storagev1.VolumeAttributesClass{
		DriverName: config.GetUniqueDriverName(),
		Parameters: map[string]string{
			hostpathCSIDriverMutableParameterName: hostpathCSIDriverMutableParameterValue,
		},
	}, config.Framework.Namespace.Name, "e2e-vac-hostpath")
}
func (h *hostpathCSIDriver) GetVolumeGroupSnapshotClass(ctx context.Context, config *storageframework.PerTestConfig, parameters map[string]string) *unstructured.Unstructured {
	snapshotter := config.GetUniqueDriverName()
	ns := config.Framework.Namespace.Name

	return utils.GenerateVolumeGroupSnapshotClassSpec(snapshotter, parameters, ns)
}

func (h *hostpathCSIDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	// 创建次要命名空间，用于创建 driver
	driverNamespace := utils.CreateDriverNamespace(ctx, f)
	driverns := driverNamespace.Name

	ginkgo.By(fmt.Sprintf("deploying %s driver", h.driverInfo.Name))
	cancelLogging := utils.StartPodLogs(ctx, f, driverNamespace)
	cs := f.ClientSet

	// hostpath CSI driver 只有在一切都在同一节点上运行时才有效。
	node, err := e2enode.GetRandomReadySchedulableNode(ctx, cs)
	framework.ExpectNoError(err)
	config := &storageframework.PerTestConfig{
		Driver:              h,
		Prefix:              "hostpath",
		Framework:           f,
		ClientNodeSelection: e2epod.NodeSelection{Name: node.Name},
		DriverNamespace:     driverNamespace,
	}

	patches := []utils.PatchCSIOptions{}

	patches = append(patches, utils.PatchCSIOptions{
		OldDriverName:       h.driverInfo.Name,
		NewDriverName:       config.GetUniqueDriverName(),
		DriverContainerName: "hostpath",
		DriverContainerArguments: []string{"--drivername=" + config.GetUniqueDriverName(),
			// 这是
			// testsuites/volumelimits.go 中 `should support volume limits`
			// 测试所需的。
			"--maxvolumespernode=10",
			// 启用卷生命周期检查，若卷未被正确
			// unpublish / unstage 则报告失败。
			"--check-volume-lifecycle=true",
		},
		ProvisionerContainerName: "csi-provisioner",
		SnapshotterContainerName: "csi-snapshotter",
		NodeName:                 node.Name,
	})

	// VAC E2E HostPath patch
	// 在 hostpath CSI driver 中启用 ModifyVolume 支持，并添加一个启用的参数名
	patches = append(patches, utils.PatchCSIOptions{
		DriverContainerName:      "hostpath",
		DriverContainerArguments: []string{"--enable-controller-modify-volume=true", "--accepted-mutable-parameter-names=e2eVacTest"},
	})

	// VAC E2E FeatureGate patches
	// TODO: VolumeAttributesClass 特性默认启用后可移除
	patches = append(patches, utils.PatchCSIOptions{
		DriverContainerName:      "csi-provisioner",
		DriverContainerArguments: []string{"--feature-gates=VolumeAttributesClass=true"},
	})
	patches = append(patches, utils.PatchCSIOptions{
		DriverContainerName:      "csi-resizer",
		DriverContainerArguments: []string{"--feature-gates=VolumeAttributesClass=true"},
	})

	// VGS E2E FeatureGate patches
	// TODO: VolumeGroupSnapshot 特性默认启用后可移除
	if os.Getenv("CSI_PROW_ENABLE_GROUP_SNAPSHOT") == "true" {
		patches = append(patches, utils.PatchCSIOptions{
			DriverContainerName:      "csi-snapshotter",
			DriverContainerArguments: []string{"--feature-gates=CSIVolumeGroupSnapshot=true"},
		})
	}

	// SnapshotMetadata 特性 E2E patches
	// TODO: SnapshotMetadata 特性默认启用后可移除
	if os.Getenv("CSI_PROW_ENABLE_SNAPSHOT_METADATA") == "true" {
		patches = append(patches, utils.PatchCSIOptions{
			DriverContainerName:      "hostpath",
			DriverContainerArguments: []string{"--enable-snapshot-metadata"},
		})
	}

	err = utils.CreateFromManifests(ctx, config.Framework, driverNamespace, func(item interface{}) error {
		for _, o := range patches {
			if err := utils.PatchCSIDeployment(config.Framework, o, item); err != nil {
				return err
			}
		}

		// 移除 csi-external-health-monitor-agent 和
		// csi-external-health-monitor-controller
		// 容器。agent 已废弃。
		// 所有测试都不需要 controller，
		// 而且在大规模集群中运行时它开销过大
		//（参见
		// https://github.com/kubernetes/kubernetes/issues/102452#issuecomment-856991009）。
		switch item := item.(type) {
		case *appsv1.StatefulSet:
			var containers []v1.Container
			var volumes []v1.Volume
			for _, container := range item.Spec.Template.Spec.Containers {
				switch container.Name {
				case "csi-external-health-monitor-agent", "csi-external-health-monitor-controller":
					// 移除这些容器。
				case "csi-snapshot-metadata":
					// 仅在特性启用时保留 snapshot metadata sidecar。
					if h.driverInfo.Capabilities[storageframework.CapSnapshotMetadata] {
						containers = append(containers, container)
					}
				default:
					// 保留其他容器。
					containers = append(containers, container)
				}
			}
			for _, volume := range item.Spec.Template.Spec.Volumes {
				switch volume.Name {
				case "csi-snapshot-metadata-server-certs":
					// 仅在特性启用时保留 snapshot metadata sidecar。
					if h.driverInfo.Capabilities[storageframework.CapSnapshotMetadata] {
						volumes = append(volumes, volume)
					}
				default:
					volumes = append(volumes, volume)
				}
			}
			item.Spec.Template.Spec.Containers = containers
			item.Spec.Template.Spec.Volumes = volumes
		}
		return nil
	}, h.manifests...)

	if err != nil {
		framework.Failf("deploying %s driver: %v", h.driverInfo.Name, err)
	}

	cleanupFunc := generateDriverCleanupFunc(
		f,
		h.driverInfo.Name,
		driverns,
		cancelLogging)
	ginkgo.DeferCleanup(cleanupFunc)

	return config
}

// mockCSI
type mockCSIDriver struct {
	driverInfo                           storageframework.DriverInfo
	manifests                            []string
	podInfo                              *bool
	storageCapacity                      *bool
	attachable                           bool
	attachLimit                          int
	enableTopology                       bool
	enableNodeExpansion                  bool
	hooks                                Hooks
	tokenRequests                        []storagev1.TokenRequest
	requiresRepublish                    *bool
	serviceAccountTokenInSecrets         *bool
	fsGroupPolicy                        *storagev1.FSGroupPolicy
	enableVolumeMountGroup               bool
	enableNodeVolumeCondition            bool
	enableNodeStorageHealth              bool
	embedded                             bool
	calls                                MockCSICalls
	embeddedCSIDriver                    *mockdriver.CSIDriver
	enableSELinuxMount                   *bool
	disableControllerExpansion           bool
	enableMutableCSINodeAllocatableCount bool

	// PrepareTest 期间设置的其他值
	clientSet       clientset.Interface
	driverNamespace *v1.Namespace
}

// 处理 gRPC 调用时要运行的 hook。
//
// 目前只实现了通用的调用前、调用后 hook。
// 这些 hook 可以按需对请求和
// 响应值做类型转换。如有需要，可在具体
// 函数内部添加更多 hook。
type Hooks struct {
	// Pre 在调用 mock driver 的方法实现之前调用。
	// 若返回非 nil reply 或 error，则直接返回给调用方。
	Pre func(ctx context.Context, method string, request interface{}) (reply interface{}, err error)

	// Post 在调用 mock driver 的方法实现之后调用。
	// 其返回值作为实际结果。
	Post func(ctx context.Context, method string, request, reply interface{}, err error) (finalReply interface{}, finalErr error)
}

// MockCSITestDriver 提供 CSI mock driver 特有的函数。
type MockCSITestDriver interface {
	storageframework.DynamicPVTestDriver

	// GetCalls 返回当前观察到的所有 gRPC 调用，仅在
	// PrepareTest 之后有效。
	GetCalls(ctx context.Context) ([]MockCSICall, error)
}

// CSIMockDriverOpts 定义 csi driver 使用的选项
type CSIMockDriverOpts struct {
	RegisterDriver                       bool
	DisableAttach                        bool
	PodInfo                              *bool
	StorageCapacity                      *bool
	AttachLimit                          int
	EnableTopology                       bool
	EnableResizing                       bool
	EnableNodeExpansion                  bool
	DisableControllerExpansion           bool
	EnableSnapshot                       bool
	EnableVolumeMountGroup               bool
	EnableNodeVolumeCondition            bool
	EnableNodeStorageHealth              bool
	TokenRequests                        []storagev1.TokenRequest
	ServiceAccountTokenInSecrets         *bool
	RequiresRepublish                    *bool
	FSGroupPolicy                        *storagev1.FSGroupPolicy
	EnableSELinuxMount                   *bool
	EnableMutableCSINodeAllocatableCount bool

	// Embedded 表示 CSI mock driver 是运行在
	// 集群内部（false，默认值），还是只有 proxy
	// 运行在集群内、所有 gRPC 调用都在
	// e2e.test 二进制内处理。
	Embedded bool

	// 仅当使用 embedded mock driver 时才调用的 hook。注意 hook 会
	// 在不同 goroutine 中异步调用。
	//
	Hooks Hooks
}

// 只从 CSI 调用日志中解析 volume_attributes 和错误码的简易结构体
type MockCSICall struct {
	json string // 完整日志条目

	Method  string
	Request struct {
		VolumeContext map[string]string `json:"volume_context"`
		Secrets       map[string]string `json:"secrets"`
	}
	FullError struct {
		Code    codes.Code `json:"code"`
		Message string     `json:"message"`
	}
	Error string
}

// MockCSICalls 是 MockCSICall 实例的线程安全存储。
type MockCSICalls struct {
	calls []MockCSICall
	mutex sync.Mutex
}

// Get 返回当前记录的所有调用。
func (c *MockCSICalls) Get() []MockCSICall {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.calls[:]
}

// Add 在末尾追加一个新调用。
func (c *MockCSICalls) Add(call MockCSICall) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.calls = append(c.calls, call)
}

// LogGRPC 从 mock CSI driver 取各个参数并记录。
func (c *MockCSICalls) LogGRPC(method string, request, reply interface{}, err error) {
	// 编码为 JSON 再解码，模拟传统的调用捕获方式。
	// 现在大概可以简化了……
	logMessage := struct {
		Method   string
		Request  interface{}
		Response interface{}
		// 字符串形式的错误，为向后兼容。
		// 无错误时为 ""。
		Error string
		// 完整的错误 dump，便于在测试中分别解析出完整的 gRPC 错误码和消息。
		FullError *spb.Status
	}{
		Method:   method,
		Request:  request,
		Response: reply,
	}

	if err != nil {
		logMessage.Error = err.Error()
		logMessage.FullError = grpcstatus.Convert(err).Proto()
	}

	msg, _ := json.Marshal(logMessage)
	call := MockCSICall{
		json: string(msg),
	}
	json.Unmarshal(msg, &call)

	klog.Infof("%s %s", grpcCallPrefix, string(msg))

	// 去掉 gRPC service 名前缀，即 "/csi.v1.Identity/Probe" -> "Probe"
	methodParts := strings.Split(call.Method, "/")
	call.Method = methodParts[len(methodParts)-1]

	c.Add(call)
}

var _ storageframework.TestDriver = &mockCSIDriver{}
var _ storageframework.DynamicPVTestDriver = &mockCSIDriver{}
var _ storageframework.SnapshottableTestDriver = &mockCSIDriver{}

// InitMockCSIDriver 返回实现 TestDriver 接口的 mockCSIDriver
func InitMockCSIDriver(driverOpts CSIMockDriverOpts) MockCSITestDriver {
	driverManifests := []string{
		"test/e2e/testing-manifests/storage-csi/external-attacher/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-provisioner/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-resizer/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-snapshotter/csi-snapshotter/rbac-csi-snapshotter.yaml",
		"test/e2e/testing-manifests/storage-csi/mock/csi-mock-rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/mock/csi-storageclass.yaml",
	}
	if driverOpts.Embedded {
		driverManifests = append(driverManifests, "test/e2e/testing-manifests/storage-csi/mock/csi-mock-proxy.yaml")
	} else {
		driverManifests = append(driverManifests, "test/e2e/testing-manifests/storage-csi/mock/csi-mock-driver.yaml")
	}

	if driverOpts.RegisterDriver {
		driverManifests = append(driverManifests, "test/e2e/testing-manifests/storage-csi/mock/csi-mock-driverinfo.yaml")
	}

	if !driverOpts.DisableAttach {
		driverManifests = append(driverManifests, "test/e2e/testing-manifests/storage-csi/mock/csi-mock-driver-attacher.yaml")
	}

	if driverOpts.EnableResizing {
		driverManifests = append(driverManifests, "test/e2e/testing-manifests/storage-csi/mock/csi-mock-driver-resizer.yaml")
	}

	if driverOpts.EnableSnapshot {
		driverManifests = append(driverManifests, "test/e2e/testing-manifests/storage-csi/mock/csi-mock-driver-snapshotter.yaml")
	}

	return &mockCSIDriver{
		driverInfo: storageframework.DriverInfo{
			Name:        "csi-mock",
			MaxFileSize: storageframework.FileSizeMedium,
			SupportedFsType: sets.NewString(
				"", // 默认 fsType
			),
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:       false,
				storageframework.CapFsGroup:           false,
				storageframework.CapExec:              false,
				storageframework.CapVolumeLimits:      true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
		manifests:                            driverManifests,
		podInfo:                              driverOpts.PodInfo,
		storageCapacity:                      driverOpts.StorageCapacity,
		enableTopology:                       driverOpts.EnableTopology,
		attachable:                           !driverOpts.DisableAttach,
		attachLimit:                          driverOpts.AttachLimit,
		enableNodeExpansion:                  driverOpts.EnableNodeExpansion,
		enableNodeVolumeCondition:            driverOpts.EnableNodeVolumeCondition,
		enableNodeStorageHealth:              driverOpts.EnableNodeStorageHealth,
		disableControllerExpansion:           driverOpts.DisableControllerExpansion,
		tokenRequests:                        driverOpts.TokenRequests,
		requiresRepublish:                    driverOpts.RequiresRepublish,
		serviceAccountTokenInSecrets:         driverOpts.ServiceAccountTokenInSecrets,
		fsGroupPolicy:                        driverOpts.FSGroupPolicy,
		enableVolumeMountGroup:               driverOpts.EnableVolumeMountGroup,
		enableSELinuxMount:                   driverOpts.EnableSELinuxMount,
		enableMutableCSINodeAllocatableCount: driverOpts.EnableMutableCSINodeAllocatableCount,
		embedded:                             driverOpts.Embedded,
		hooks:                                driverOpts.Hooks,
	}
}

func (m *mockCSIDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &m.driverInfo
}

func (m *mockCSIDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (m *mockCSIDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := config.GetUniqueDriverName()
	parameters := map[string]string{}
	ns := config.Framework.Namespace.Name

	return storageframework.GetStorageClass(provisioner, parameters, nil, ns)
}

func (m *mockCSIDriver) GetSnapshotClass(ctx context.Context, config *storageframework.PerTestConfig, parameters map[string]string) *unstructured.Unstructured {
	snapshotter := m.driverInfo.Name + "-" + config.Framework.UniqueName
	ns := config.Framework.Namespace.Name

	return utils.GenerateSnapshotClassSpec(snapshotter, parameters, ns)
}

func (m *mockCSIDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	m.clientSet = f.ClientSet

	// 创建次要命名空间，用于创建 driver
	m.driverNamespace = utils.CreateDriverNamespace(ctx, f)
	driverns := m.driverNamespace.Name

	if m.embedded {
		ginkgo.By("deploying csi mock proxy")
	} else {
		ginkgo.By("deploying csi mock driver")
	}
	cancelLogging := utils.StartPodLogs(ctx, f, m.driverNamespace)
	cs := f.ClientSet

	// Pod 应调度到该节点上
	node, err := e2enode.GetRandomReadySchedulableNode(ctx, cs)
	framework.ExpectNoError(err)

	embeddedCleanup := func() {}
	containerArgs := []string{}
	if m.embedded {
		// 运行 embedded CSI driver。
		//
		// 目前只启动一个实例，同时实现 controller、
		// node 和 identity service。它与我们在集群内运行的那个 Pod 对应。
		// 那个 Pod 的名称和命名空间是确定的，
		// 因此我们知道要连接谁。
		//
		// 长期来看，也可以部署一个中心 controller 和多个
		// node 实例，并在本进程中共享已配置卷的信息。
		//
		podname := "csi-mockplugin-0"
		containername := "mock"

		// 即使测试上下文被取消，也必须继续运行，
		// 以便执行清理回调。
		ctx, cancel := context.WithCancel(context.Background())
		serviceConfig := mockservice.Config{
			DisableAttach:               !m.attachable,
			DriverName:                  "csi-mock-" + f.UniqueName,
			AttachLimit:                 int64(m.attachLimit),
			NodeExpansionRequired:       m.enableNodeExpansion,
			DisableControllerExpansion:  m.disableControllerExpansion,
			NodeVolumeConditionRequired: m.enableNodeVolumeCondition,
			NodeStorageHealthRequired:   m.enableNodeStorageHealth,
			VolumeMountGroupRequired:    m.enableVolumeMountGroup,
			EnableTopology:              m.enableTopology,
			IO: proxy.PodDirIO{
				TCtx:          f.TContext(context.Background() /* 不取消，传统行为！ */),
				Namespace:     m.driverNamespace.Name,
				PodName:       podname,
				ContainerName: "busybox",
			},
		}
		s := mockservice.New(serviceConfig)
		servers := &mockdriver.CSIDriverServers{
			Controller: s,
			Identity:   s,
			Node:       s,
		}
		m.embeddedCSIDriver = mockdriver.NewCSIDriver(servers)
		l, err := proxy.Listen(ctx, f.ClientSet, f.ClientConfig(),
			proxy.Addr{
				Namespace:     m.driverNamespace.Name,
				PodName:       podname,
				ContainerName: containername,
				Port:          9000,
			},
		)

		framework.ExpectNoError(err, "start connecting to proxy pod")
		err = m.embeddedCSIDriver.Start(l, m.interceptGRPC)
		framework.ExpectNoError(err, "start mock driver")

		embeddedCleanup = func() {
			// 杀掉所有 goroutine 并删除 mock driver 的资源。
			m.embeddedCSIDriver.Stop()
			l.Close()
			cancel()
		}
	} else {
		// 在集群内使用 mock driver 时，必须通过命令行参数
		// 重新配置。
		containerArgs = append(containerArgs, "--drivername=csi-mock-"+f.UniqueName)

		if m.attachable {
			containerArgs = append(containerArgs, "--enable-attach")
		}

		if m.enableTopology {
			containerArgs = append(containerArgs, "--enable-topology")
		}

		if m.attachLimit > 0 {
			containerArgs = append(containerArgs, "--attach-limit", strconv.Itoa(m.attachLimit))
		}

		if m.enableNodeExpansion {
			containerArgs = append(containerArgs, "--node-expand-required=true")
		}
	}

	config := &storageframework.PerTestConfig{
		Driver:              m,
		Prefix:              "mock",
		Framework:           f,
		ClientNodeSelection: e2epod.NodeSelection{Name: node.Name},
		DriverNamespace:     m.driverNamespace,
	}

	o := utils.PatchCSIOptions{
		OldDriverName:            "csi-mock",
		NewDriverName:            "csi-mock-" + f.UniqueName,
		DriverContainerName:      "mock",
		DriverContainerArguments: containerArgs,
		ProvisionerContainerName: "csi-provisioner",
		NodeName:                 node.Name,
		PodInfo:                  m.podInfo,
		StorageCapacity:          m.storageCapacity,
		CanAttach:                &m.attachable,
		VolumeLifecycleModes: &[]storagev1.VolumeLifecycleMode{
			storagev1.VolumeLifecyclePersistent,
			storagev1.VolumeLifecycleEphemeral,
		},
		TokenRequests:                m.tokenRequests,
		RequiresRepublish:            m.requiresRepublish,
		ServiceAccountTokenInSecrets: m.serviceAccountTokenInSecrets,
		FSGroupPolicy:                m.fsGroupPolicy,
		SELinuxMount:                 m.enableSELinuxMount,
		Features:                     map[string][]string{},
	}
	if m.enableMutableCSINodeAllocatableCount {
		o.Features["csi-attacher"] = []string{"MutableCSINodeAllocatableCount=true"}
	}
	if m.enableNodeExpansion {
		o.Features["csi-resizer"] = []string{"AnnotateFsResize=true"}
	}

	err = utils.CreateFromManifests(ctx, f, m.driverNamespace, func(item interface{}) error {
		if err := utils.PatchCSIDeployment(config.Framework, o, item); err != nil {
			return err
		}

		switch item := item.(type) {
		case *rbacv1.ClusterRole:
			if strings.HasPrefix(item.Name, "external-snapshotter-runner") {
				// 重新启用 snapshotter sidecar 对 secret 的访问，参见
				// https://github.com/kubernetes/kubernetes/blob/6ede5ca95f78478fa627ecfea8136e0dff34436b/test/e2e/storage/csi_mock_volume.go#L1539-L1548
				// 它在 https://github.com/kubernetes-csi/external-snapshotter/blob/501cc505846c03ee665355132f2da0ce7d5d747d/deploy/kubernetes/csi-snapshotter/rbac-csi-snapshotter.yaml#L26-L32 中被禁用。
				item.Rules = append(item.Rules, rbacv1.PolicyRule{
					APIGroups: []string{""},
					Resources: []string{"secrets"},
					Verbs:     []string{"get", "list"},
				})
			}
		}

		return nil
	}, m.manifests...)

	if err != nil {
		framework.Failf("deploying csi mock driver: %v", err)
	}

	driverCleanupFunc := generateDriverCleanupFunc(
		f,
		"mock",
		driverns,
		cancelLogging)

	ginkgo.DeferCleanup(func(ctx context.Context) {
		embeddedCleanup()
		driverCleanupFunc(ctx)
	})

	return config
}

func (m *mockCSIDriver) interceptGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		// 始终记录调用及其最终结果，
		// 无论结果来自真实实现
		// 还是 hook。
		m.calls.LogGRPC(info.FullMethod, req, resp, err)
	}()

	if m.hooks.Pre != nil {
		resp, err = m.hooks.Pre(ctx, info.FullMethod, req)
		if resp != nil || err != nil {
			return
		}
	}
	resp, err = handler(ctx, req)
	if m.hooks.Post != nil {
		resp, err = m.hooks.Post(ctx, info.FullMethod, req, resp, err)
	}
	return
}

func (m *mockCSIDriver) GetCalls(ctx context.Context) ([]MockCSICall, error) {
	if m.embedded {
		return m.calls.Get(), nil
	}

	if m.driverNamespace == nil {
		return nil, errors.New("PrepareTest not called yet")
	}

	// CSI driver Pod 的名称（位于稳定名称的 StatefulSet 中）
	driverPodName := "csi-mockplugin-0"
	// CSI driver 容器名
	driverContainerName := "mock"

	// 加载 driver Pod 的日志
	log, err := e2epod.GetPodLogs(ctx, m.clientSet, m.driverNamespace.Name, driverPodName, driverContainerName)
	if err != nil {
		return nil, fmt.Errorf("could not load CSI driver logs: %w", err)
	}

	logLines := strings.Split(log, "\n")
	var calls []MockCSICall
	for _, line := range logLines {
		index := strings.Index(line, grpcCallPrefix)
		if index == -1 {
			continue
		}
		line = line[index+len(grpcCallPrefix):]
		call := MockCSICall{
			json: string(line),
		}
		err := json.Unmarshal([]byte(line), &call)
		if err != nil {
			framework.Logf("Could not parse CSI driver log line %q: %s", line, err)
			continue
		}

		// 去掉 gRPC service 名前缀，即 "/csi.v1.Identity/Probe" -> "Probe"
		methodParts := strings.Split(call.Method, "/")
		call.Method = methodParts[len(methodParts)-1]

		calls = append(calls, call)
	}
	return calls, nil
}

// gce-pd
type gcePDCSIDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &gcePDCSIDriver{}
var _ storageframework.DynamicPVTestDriver = &gcePDCSIDriver{}
var _ storageframework.SnapshottableTestDriver = &gcePDCSIDriver{}

// InitGcePDCSIDriver 返回实现 TestDriver 接口的 gcePDCSIDriver
func InitGcePDCSIDriver() storageframework.TestDriver {
	return &gcePDCSIDriver{
		driverInfo: storageframework.DriverInfo{
			Name:        GCEPDCSIDriverName,
			TestTags:    []interface{}{framework.WithSerial(), framework.WithProvider("gce")},
			MaxFileSize: storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "5Gi",
			},
			SupportedFsType: sets.NewString(
				"", // 默认 fsType
				"ext2",
				"ext3",
				"ext4",
				"xfs",
			),
			SupportedMountOption: sets.NewString("debug", "nouid32"),
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence: true,
				storageframework.CapBlock:       true,
				storageframework.CapFsGroup:     true,
				storageframework.CapExec:        true,
				storageframework.CapMultiPODs:   true,
				// GCE 支持卷数量限制，但测试会创建大量
				// 卷，导致测试套件超时。
				storageframework.CapVolumeLimits:                   false,
				storageframework.CapTopology:                       true,
				storageframework.CapControllerExpansion:            true,
				storageframework.CapOfflineExpansion:               true,
				storageframework.CapOnlineExpansion:                true,
				storageframework.CapNodeExpansion:                  true,
				storageframework.CapSnapshotDataSource:             true,
				storageframework.CapReadWriteOncePod:               true,
				storageframework.CapMultiplePVsSameID:              true,
				storageframework.CapFSResizeFromSourceNotSupported: true, //TODO: 等 CI 使用修复后的 driver 后移除：https://github.com/kubernetes-sigs/gcp-compute-persistent-disk-csi-driver/pull/972
			},
			RequiredAccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			TopologyKeys:        []string{GCEPDCSIZoneTopologyKey},
			StressTestOptions: &storageframework.StressTestOptions{
				NumPods:     10,
				NumRestarts: 10,
			},
			VolumeSnapshotStressTestOptions: &storageframework.VolumeSnapshotStressTestOptions{
				// GCE 同一个卷一次只允许创建一个快照，
				// 这会导致测试超时。可通过增加 Pod（和卷）数量、
				// 减少每个卷的快照数量来降低超时概率。
				//
				NumPods:      20,
				NumSnapshots: 2,
			},
		},
	}
}

func (g *gcePDCSIDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &g.driverInfo
}

func (g *gcePDCSIDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	if pattern.FsType == "xfs" {
		if !framework.NodeOSDistroIs("ubuntu", "custom") {
			return "unsupported node OS"
		}

	}
	for _, tag := range pattern.TestTags {
		if tag == feature.Windows {
			return "Skipping tests for windows since CSI does not support it yet"
		}
	}
	return ""
}

func (g *gcePDCSIDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	ns := config.Framework.Namespace.Name
	provisioner := g.driverInfo.Name

	parameters := map[string]string{"type": "pd-standard"}
	if fsType != "" {
		parameters["csi.storage.k8s.io/fstype"] = fsType
	}
	delayedBinding := storagev1.VolumeBindingWaitForFirstConsumer

	return storageframework.GetStorageClass(provisioner, parameters, &delayedBinding, ns)
}

func (g *gcePDCSIDriver) GetSnapshotClass(ctx context.Context, config *storageframework.PerTestConfig, parameters map[string]string) *unstructured.Unstructured {
	snapshotter := g.driverInfo.Name
	ns := config.Framework.Namespace.Name

	return utils.GenerateSnapshotClassSpec(snapshotter, parameters, ns)
}

func (g *gcePDCSIDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	cfg := &storageframework.PerTestConfig{
		Driver:    g,
		Prefix:    "gcepd",
		Framework: f,
	}

	// 检查集群是否已经运行 gce-pd CSI Driver
	deploy, err := f.ClientSet.AppsV1().Deployments("gce-pd-csi-driver").Get(ctx, "csi-gce-pd-controller", metav1.GetOptions{})
	if err == nil && deploy != nil {
		framework.Logf("The csi gce-pd driver is already installed.")
		return cfg
	}
	ginkgo.By("deploying csi gce-pd driver")
	// 创建次要命名空间，用于创建 driver
	driverNamespace := utils.CreateDriverNamespace(ctx, f)
	driverns := driverNamespace.Name

	cancelLogging := utils.StartPodLogs(ctx, f, driverNamespace)
	// 重命名 gcePD driver 会更安全，但以前从未这样做过，
	// 现在尝试这样做会导致 driver 注册出错，因此
	// 通过在下面传入 nil 函数来禁用它。
	//
	//
	// 原本需要使用的选项是：
	// o := utils.PatchCSIOptions{
	// 	OldDriverName:            g.driverInfo.Name,
	// 	NewDriverName:            storageframework.GetUniqueDriverName(g),
	// 	DriverContainerName:      "gce-driver",
	// 	ProvisionerContainerName: "csi-external-provisioner",
	// }
	createGCESecrets(f.ClientSet, driverns)

	manifests := []string{
		"test/e2e/testing-manifests/storage-csi/external-attacher/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/external-provisioner/rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/gce-pd/csi-controller-rbac.yaml",
		"test/e2e/testing-manifests/storage-csi/gce-pd/node_ds.yaml",
		"test/e2e/testing-manifests/storage-csi/gce-pd/controller_ss.yaml",
	}

	err = utils.CreateFromManifests(ctx, f, driverNamespace, nil, manifests...)
	if err != nil {
		framework.Failf("deploying csi gce-pd driver: %v", err)
	}

	if err = WaitForCSIDriverRegistrationOnAllNodes(ctx, GCEPDCSIDriverName, f.ClientSet); err != nil {
		framework.Failf("waiting for csi driver node registration on: %v", err)
	}

	cleanupFunc := generateDriverCleanupFunc(
		f,
		"gce-pd",
		driverns,
		cancelLogging)
	ginkgo.DeferCleanup(cleanupFunc)

	return &storageframework.PerTestConfig{
		Driver:          g,
		Prefix:          "gcepd",
		Framework:       f,
		DriverNamespace: driverNamespace,
	}
}

// WaitForCSIDriverRegistrationOnAllNodes 等待所有可调度节点上的 CSINode 对象
// 更新为包含给定 driver。
func WaitForCSIDriverRegistrationOnAllNodes(ctx context.Context, driverName string, cs clientset.Interface) error {
	nodes, err := e2enode.GetReadySchedulableNodes(ctx, cs)
	if err != nil {
		return err
	}
	for _, node := range nodes.Items {
		if err := WaitForCSIDriverRegistrationOnNode(ctx, node.Name, driverName, cs); err != nil {
			return err
		}
	}
	return nil
}

// WaitForCSIDriverRegistrationOnNode 等待某个节点上由 node-registrar 生成的 CSINode 对象
func WaitForCSIDriverRegistrationOnNode(ctx context.Context, nodeName string, driverName string, cs clientset.Interface) error {
	framework.Logf("waiting for CSIDriver %v to register on node %v", driverName, nodeName)

	// 大约 8.6 分钟超时
	backoff := wait.Backoff{
		Duration: 2 * time.Second,
		Factor:   1.5,
		Steps:    12,
	}

	waitErr := wait.ExponentialBackoff(backoff, func() (bool, error) {
		csiNode, err := cs.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name == driverName {
				return true, nil
			}
		}
		return false, nil
	})
	if waitErr != nil {
		return fmt.Errorf("error waiting for CSI driver %s registration on node %s: %v", driverName, nodeName, waitErr)
	}
	return nil
}

func tryFunc(f func()) error {
	var err error
	if f == nil {
		return nil
	}
	defer func() {
		if recoverError := recover(); recoverError != nil {
			err = fmt.Errorf("%v", recoverError)
		}
	}()
	f()
	return err
}

func generateDriverCleanupFunc(
	f *framework.Framework,
	driverName, driverns string,
	cancelLogging func()) func(ctx context.Context) {

	// 清理 CSI driver 和命名空间。该函数必须幂等，且可由
	// defer（或 AfterEach）和 AfterSuite action hook 并发调用。
	cleanupFunc := func(ctx context.Context) {
		ginkgo.By(fmt.Sprintf("uninstalling csi %s driver", driverName))
		_ = tryFunc(cancelLogging)

		ginkgo.By(fmt.Sprintf("deleting the driver namespace: %s", driverns))
		_ = tryFunc(func() { f.DeleteNamespace(ctx, driverns) })
	}

	return cleanupFunc
}
