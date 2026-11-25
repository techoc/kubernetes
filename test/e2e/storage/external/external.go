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

package external

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	klog "k8s.io/klog/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	e2econfig "k8s.io/kubernetes/test/e2e/framework/config"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/testsuites"
	"k8s.io/kubernetes/test/e2e/storage/utils"

	"github.com/onsi/gomega"
)

// DriverDefinition 需要通过 .yaml 或 .json 文件填写。
// 其方法仅依据该结构体中的信息实现 TestDriver 接口。
type driverDefinition struct {
	// DriverInfo 是 storage testsuite 期望测试驱动提供的静态信息。
	// 详见 test/e2e/storage/testsuites/testdriver.go。
	// 唯一带非零默认值的是支持的文件系统列表（SupportedFsType）：
	// 设置它是为了启用使用默认文件系统的测试。
	DriverInfo storageframework.DriverInfo

	// 必须设置 StorageClass 才会启用动态制备（dynamic provisioning）测试。
	// 默认不运行这些测试。
	StorageClass struct {
		// FromName 设为 true 时，使用以 DriverInfo.Name 为 provisioner、
		// 不带任何参数的 storage class。
		FromName bool

		// FromFile 仅在 FromName 为 false 时使用。
		// 它从给定的 .yaml 或 .json 文件加载 storage class。
		// 文件名由 framework.testfiles 包解析，通常可以是绝对路径，
		// 或相对于测试套件的 --repo-root 参数的路径。
		//
		// 当 storage class 需要附带额外参数时使用此方式。
		FromFile string

		// FromExistingClassName 指定一个预先安装的 StorageClass 的名称，
		// 它会被复制并用于测试。
		FromExistingClassName string
	}

	// 必须设置 VolumeAttributesClass 才会启用卷修改（volume modification）测试。
	// 默认不运行这些测试。
	VolumeAttributesClass struct {
		// FromFile 仅在 FromName 为 false 时使用。
		// 它从给定的 .yaml 或 .json 文件加载 VolumeAttributesClass。
		// 文件名由 framework.testfiles 包解析，通常可以是绝对路径，
		// 或相对于测试套件的 --repo-root 参数的路径。
		//
		// 当 VolumeAttributesClass 需要附带额外参数时使用此方式。
		FromFile string

		// FromExistingClassName 指定一个预先安装的 VolumeAttributesClass 的名称，
		// 它会被复制并用于测试。
		FromExistingClassName string
	}

	// 必须设置 SnapshotClass 才会启用快照（snapshotting）测试。
	// 默认不运行这些测试。
	SnapshotClass struct {
		// FromName 设为 true 时，使用以 DriverInfo.Name 为 provisioner 的
		// snapshotter class。
		FromName bool

		// FromFile 仅在 FromName 为 false 时使用。
		// 它从给定的 .yaml 或 .json 文件加载 snapshot class。
		// 文件名由 framework.testfiles 包解析，通常可以是绝对路径，
		// 或相对于测试套件的 --repo-root 参数的路径。
		//
		// 当 snapshot class 需要附带额外参数时使用此方式。
		FromFile string

		// FromExistingClassName 指定一个预先安装的 SnapshotClass 的名称，
		// 它会被复制并用于测试。
		FromExistingClassName string
	}

	// 必须设置 GroupSnapshotClass 才会启用组快照（groupsnapshotting）测试。
	// 默认不运行这些测试。
	GroupSnapshotClass struct {
		// FromName 设为 true 时，使用以 DriverInfo.Name 为 provisioner 的
		// groupsnapshotter class。
		FromName bool

		// FromFile 仅在 FromName 为 false 时使用。
		// 它从给定的 .yaml 或 .json 文件加载 groupsnapshot class。
		// 文件名由 framework.testfiles 包解析，通常可以是绝对路径，
		// 或相对于测试套件的 --repo-root 参数的路径。
		//
		// 当 groupsnapshot class 需要附带额外参数时使用此方式。
		FromFile string

		// FromExistingClassName 指定一个预先安装的 GroupSnapshotClass 的名称，
		// 它会被复制并用于测试。
		FromExistingClassName string
	}

	// InlineVolumes 定义一个或多个用作 inline ephemeral volume 的卷。
	// 至少要定义一个此类卷才会启用 inline ephemeral volume 测试。
	// 如果测试需要的卷多于已定义的卷，部分已定义的卷会被重复使用。
	//
	// inline volume 中以 DriverInfo.Name 作为驱动名称。
	InlineVolumes []struct {
		// Attributes 会作为 NodePublishVolumeReq.volume_context 传入。
		// 可以为空。
		Attributes map[string]string
		// Shared 定义生成的卷是否在不同 Pod 之间共享
		// （即在一个 Pod 中的修改对另一个 Pod 可见）。
		Shared bool
		// 如果驱动不支持以读/写方式挂载，ReadOnly 必须设为 true。
		ReadOnly bool
	}

	// ClientNodeName 指定用于调度测试 Pod 的节点。
	// 可以留空。多数驱动不需要它，而应通过 topology 确保 Pod 落在正确的节点上。
	ClientNodeName string

	// NodeSelectors 用于指定测试期间 Pod 部署的 nodeSelector 信息。
	// 在需要控制特殊环境中的应用位置时很有用。多数驱动不需要它，
	// 而应通过 topology 确保 Pod 落在正确的节点上。
	NodeSelectors map[string]string

	// Timeouts 包含测试执行期间使用的自定义超时。
	// 此处指定的值会覆盖 framework.TimeoutContext 结构体中的默认值。
	Timeouts map[string]string
}

func init() {
	e2econfig.Flags.Var(testDriverParameter{}, "storage.testdriver", "name of a .yaml or .json file that defines a driver for storage testing, can be used more than once")
}

// testDriverParameter 用于把驱动定义文件的加载和测试实例化挂接到参数解析中：
// 对于可能存在的多个参数，每个都会调用 Set，并立即完成这两件事。
// 在参数解析与测试套件启动之间，没有其他位置可以定义这些测试。
type testDriverParameter struct {
}

var _ flag.Value = testDriverParameter{}

func (t testDriverParameter) String() string {
	return "<.yaml or .json file>"
}

func (t testDriverParameter) Set(filename string) error {
	return AddDriverDefinition(filename)
}

// AddDriverDefinition 为 CSI 驱动定义文件定义 Ginkgo 测试。
// 可以通过 --storage.testdriver 命令行参数或直接调用 AddDriverDefinition
// 来定义测试。
func AddDriverDefinition(filename string) error {
	driver, err := loadDriverDefinition(filename)
	framework.Logf("Driver loaded from path [%s]: %+v", filename, driver)
	if err != nil {
		return err
	}
	if driver.DriverInfo.Name == "" {
		return fmt.Errorf("%q: DriverInfo.Name not set", filename)
	}

	args := []interface{}{"External Storage"}
	args = append(args, storageframework.GetDriverNameWithFeatureTags(driver)...)
	args = append(args, func() {
		storageframework.DefineTestSuites(driver, testsuites.CSISuites)
	})
	framework.Describe(args...)

	return nil
}

func loadDriverDefinition(filename string) (*driverDefinition, error) {
	if filename == "" {
		return nil, fmt.Errorf("missing file name")
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	// 下面设置一些合理的默认值。
	driver := &driverDefinition{
		DriverInfo: storageframework.DriverInfo{
			SupportedFsType: sets.NewString(
				"", // 默认 fsType
			),
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "5Gi",
			},
		},
	}
	// TODO: 等 https://github.com/kubernetes/kubernetes/pull/71589 或类似的改动合并后，
	// 再对文件内容做严格检查。
	if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), data, driver); err != nil {
		return nil, fmt.Errorf("%s: %w", filename, err)
	}

	// 为保持向后兼容：如果启用了 controller expansion，
	// 就同时启用 online 和 offline expansion。
	if _, ok := driver.GetDriverInfo().Capabilities[storageframework.CapOnlineExpansion]; !ok &&
		driver.GetDriverInfo().Capabilities[storageframework.CapControllerExpansion] {
		caps := driver.DriverInfo.Capabilities
		caps[storageframework.CapOnlineExpansion] = true
		driver.DriverInfo.Capabilities = caps
	}
	if _, ok := driver.GetDriverInfo().Capabilities[storageframework.CapOfflineExpansion]; !ok &&
		driver.GetDriverInfo().Capabilities[storageframework.CapControllerExpansion] {
		caps := driver.DriverInfo.Capabilities
		caps[storageframework.CapOfflineExpansion] = true
		driver.DriverInfo.Capabilities = caps
	}
	return driver, nil
}

var _ storageframework.TestDriver = &driverDefinition{}

// 必须实现该接口，因为 dynamic PV 可能支持也可能不支持。
// driverDefinition.SkipUnsupportedTest 会根据实际的驱动定义进行检查。
var _ storageframework.DynamicPVTestDriver = &driverDefinition{}

// 快照接口同理。
var _ storageframework.SnapshottableTestDriver = &driverDefinition{}

// ephemeral volume 接口亦然。
var _ storageframework.EphemeralTestDriver = &driverDefinition{}

var _ storageframework.CustomTimeoutsTestDriver = &driverDefinition{}

// runtime.DecodeInto 需要一个 runtime.Object，但不会对它做反序列化，
// 因此下面的方法都无需实现。
var _ runtime.Object = &driverDefinition{}

func (d *driverDefinition) DeepCopyObject() runtime.Object {
	return nil
}

func (d *driverDefinition) GetObjectKind() schema.ObjectKind {
	return nil
}

func (d *driverDefinition) GetDriverInfo() *storageframework.DriverInfo {
	return &d.DriverInfo
}

func (d *driverDefinition) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	supported := false
	// TODO (?): 支持更多卷类型
	switch pattern.VolType {
	case "":
		supported = true
	case storageframework.DynamicPV, storageframework.GenericEphemeralVolume:
		if d.StorageClass.FromName || d.StorageClass.FromFile != "" || d.StorageClass.FromExistingClassName != "" {
			supported = true
		}
	case storageframework.CSIInlineVolume:
		supported = len(d.InlineVolumes) != 0
	}
	if !supported {
		return fmt.Sprintf("Driver %q does not support volume type %q - skipping", d.DriverInfo.Name, pattern.VolType)
	}
	return ""
}

func (d *driverDefinition) GetDynamicProvisionStorageClass(ctx context.Context, e2econfig *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	var (
		sc  *storagev1.StorageClass
		err error
	)

	f := e2econfig.Framework
	switch {
	case d.StorageClass.FromName:
		sc = &storagev1.StorageClass{Provisioner: d.DriverInfo.Name}
	case d.StorageClass.FromExistingClassName != "":
		sc, err = f.ClientSet.StorageV1().StorageClasses().Get(ctx, d.StorageClass.FromExistingClassName, metav1.GetOptions{})
		framework.ExpectNoError(err, "getting storage class %s", d.StorageClass.FromExistingClassName)
	case d.StorageClass.FromFile != "":
		var ok bool
		items, err := utils.LoadFromManifests(d.StorageClass.FromFile)
		framework.ExpectNoError(err, "load storage class from %s", d.StorageClass.FromFile)
		gomega.Expect(items).To(gomega.HaveLen(1), "exactly one item from %s", d.StorageClass.FromFile)
		err = utils.PatchItems(f, f.Namespace, items...)
		framework.ExpectNoError(err, "patch items")

		sc, ok = items[0].(*storagev1.StorageClass)
		if !ok {
			framework.Failf("storage class from %s", d.StorageClass.FromFile)
		}
	}

	gomega.Expect(sc).ToNot(gomega.BeNil(), "storage class is unexpectantly nil")

	if fsType != "" {
		if sc.Parameters == nil {
			sc.Parameters = map[string]string{}
		}
		// 这使 external storage 测试套件仅适用于 CSI 驱动。
		// 如果将来要把 in-tree 存储测试移出，可能需要重新考虑这一点。
		sc.Parameters["csi.storage.k8s.io/fstype"] = fsType
	}
	return storageframework.CopyStorageClass(sc, f.Namespace.Name, "e2e-sc")
}

func (d *driverDefinition) GetTimeouts() *framework.TimeoutContext {
	timeouts := framework.NewTimeoutContext()
	if d.Timeouts == nil {
		return timeouts
	}

	// 用临时 map 保存 manifest 文件中指定的超时设置。
	c := make(map[string]time.Duration)
	for k, v := range d.Timeouts {
		duration, err := time.ParseDuration(v)
		if err != nil {
			// 这里不能用 ExpectNoError()：该方法可能在 It() 之外被调用，
			// 因此只记录错误并返回默认超时。
			klog.Errorf("Could not parse duration for key %s, will use default values: %v", k, err)
			return timeouts
		}
		c[k] = duration
	}

	// 将保存自定义超时的临时 map 转换为 JSON。
	t, err := json.Marshal(c)
	if err != nil {
		klog.Errorf("Could not marshal custom timeouts, will use default values: %v", err)
		return timeouts
	}

	// 用自定义超时覆盖默认值。
	err = json.Unmarshal(t, &timeouts)
	if err != nil {
		klog.Errorf("Could not unmarshal custom timeouts, will use default values: %v", err)
		return timeouts
	}

	return timeouts
}

func loadSnapshotClass(filename string) (*unstructured.Unstructured, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	snapshotClass := &unstructured.Unstructured{}

	if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), data, snapshotClass); err != nil {
		return nil, fmt.Errorf("%s: %w", filename, err)
	}

	return snapshotClass, nil
}

func loadGroupSnapshotClass(filename string) (*unstructured.Unstructured, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	groupSnapshotClass := &unstructured.Unstructured{}

	if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), data, groupSnapshotClass); err != nil {
		return nil, fmt.Errorf("%s: %w", filename, err)
	}

	return groupSnapshotClass, nil
}

func (d *driverDefinition) GetSnapshotClass(ctx context.Context, e2econfig *storageframework.PerTestConfig, parameters map[string]string) *unstructured.Unstructured {
	if !d.SnapshotClass.FromName && d.SnapshotClass.FromFile == "" && d.SnapshotClass.FromExistingClassName == "" {
		e2eskipper.Skipf("Driver %q does not support snapshotting - skipping", d.DriverInfo.Name)
	}

	f := e2econfig.Framework
	snapshotter := d.DriverInfo.Name
	ns := e2econfig.Framework.Namespace.Name

	switch {
	case d.SnapshotClass.FromName:
		// 不做任何事（只使用空参数）。
	case d.SnapshotClass.FromExistingClassName != "":
		snapshotClass, err := f.DynamicClient.Resource(utils.SnapshotClassGVR).Get(ctx, d.SnapshotClass.FromExistingClassName, metav1.GetOptions{})
		framework.ExpectNoError(err, "getting snapshot class %s", d.SnapshotClass.FromExistingClassName)

		if params, ok := snapshotClass.Object["parameters"].(map[string]interface{}); ok {
			for k, v := range params {
				parameters[k] = v.(string)
			}
		}

		if snapshotProvider, ok := snapshotClass.Object["driver"]; ok {
			snapshotter = snapshotProvider.(string)
		}
	case d.SnapshotClass.FromFile != "":
		snapshotClass, err := loadSnapshotClass(d.SnapshotClass.FromFile)
		framework.ExpectNoError(err, "load snapshot class from %s", d.SnapshotClass.FromFile)

		if params, ok := snapshotClass.Object["parameters"].(map[string]interface{}); ok {
			for k, v := range params {
				parameters[k] = v.(string)
			}
		}

		if snapshotProvider, ok := snapshotClass.Object["driver"]; ok {
			snapshotter = snapshotProvider.(string)
		}
	}

	return utils.GenerateSnapshotClassSpec(snapshotter, parameters, ns)
}

func (d *driverDefinition) GetVolumeGroupSnapshotClass(ctx context.Context, e2econfig *storageframework.PerTestConfig, parameters map[string]string) *unstructured.Unstructured {
	if !d.GroupSnapshotClass.FromName && d.GroupSnapshotClass.FromFile == "" && d.GroupSnapshotClass.FromExistingClassName == "" {
		e2eskipper.Skipf("Driver %q does not support groupsnapshotting - skipping", d.DriverInfo.Name)
	}

	f := e2econfig.Framework
	snapshotter := d.DriverInfo.Name
	ns := e2econfig.Framework.Namespace.Name

	switch {
	case d.GroupSnapshotClass.FromName:
		// 不做任何事（只使用空参数）。
	case d.GroupSnapshotClass.FromExistingClassName != "":
		groupSnapshotClass, err := f.DynamicClient.Resource(utils.VolumeGroupSnapshotClassGVR).Get(ctx, d.GroupSnapshotClass.FromExistingClassName, metav1.GetOptions{})
		framework.ExpectNoError(err, "getting snapshot class %s", d.SnapshotClass.FromExistingClassName)

		if params, ok := groupSnapshotClass.Object["parameters"].(map[string]interface{}); ok {
			for k, v := range params {
				parameters[k] = v.(string)
			}
		}

		if snapshotProvider, ok := groupSnapshotClass.Object["driver"]; ok {
			snapshotter = snapshotProvider.(string)
		}
	case d.GroupSnapshotClass.FromFile != "":
		groupSnapshotClass, err := loadGroupSnapshotClass(d.GroupSnapshotClass.FromFile)
		framework.ExpectNoError(err, "load groupsnapshot class from %s", d.GroupSnapshotClass.FromFile)

		if params, ok := groupSnapshotClass.Object["parameters"].(map[string]interface{}); ok {
			for k, v := range params {
				parameters[k] = v.(string)
			}
		}

		if snapshotProvider, ok := groupSnapshotClass.Object["driver"]; ok {
			snapshotter = snapshotProvider.(string)
		}
	}

	return utils.GenerateVolumeGroupSnapshotClassSpec(snapshotter, parameters, ns)
}

func (d *driverDefinition) GetVolumeAttributesClass(ctx context.Context, e2econfig *storageframework.PerTestConfig) *storagev1.VolumeAttributesClass {
	if d.VolumeAttributesClass.FromFile == "" && d.VolumeAttributesClass.FromExistingClassName == "" {
		e2eskipper.Skipf("Driver %q has no configured VolumeAttributesClass - skipping", d.DriverInfo.Name)
		return nil
	}

	var (
		vac *storagev1.VolumeAttributesClass
		err error
	)

	f := e2econfig.Framework
	switch {
	case d.VolumeAttributesClass.FromExistingClassName != "":
		vac, err = f.ClientSet.StorageV1().VolumeAttributesClasses().Get(ctx, d.VolumeAttributesClass.FromExistingClassName, metav1.GetOptions{})
		framework.ExpectNoError(err, "getting VolumeAttributesClass %s", d.VolumeAttributesClass.FromExistingClassName)
	case d.VolumeAttributesClass.FromFile != "":
		var ok bool
		items, err := utils.LoadFromManifests(d.VolumeAttributesClass.FromFile)
		framework.ExpectNoError(err, "load VolumeAttributesClass from %s", d.VolumeAttributesClass.FromFile)
		gomega.Expect(items).To(gomega.HaveLen(1), "exactly one item from %s", d.VolumeAttributesClass.FromFile)
		err = utils.PatchItems(f, f.Namespace, items...)
		framework.ExpectNoError(err, "patch VolumeAttributesClass from %s", d.VolumeAttributesClass.FromFile)

		vac, ok = items[0].(*storagev1.VolumeAttributesClass)
		if !ok {
			framework.Failf("cast VolumeAttributesClass from %s", d.VolumeAttributesClass.FromFile)
		}
	}

	gomega.Expect(vac).ToNot(gomega.BeNil(), "VolumeAttributesClass is unexpectantly nil")

	return storageframework.CopyVolumeAttributesClass(vac, f.Namespace.Name, "e2e-vac")
}

func (d *driverDefinition) GetVolume(e2econfig *storageframework.PerTestConfig, volumeNumber int) (map[string]string, bool, bool) {
	if len(d.InlineVolumes) == 0 {
		e2eskipper.Skipf("%s does not have any InlineVolumeAttributes defined", d.DriverInfo.Name)
	}
	e2evolume := d.InlineVolumes[volumeNumber%len(d.InlineVolumes)]
	return e2evolume.Attributes, e2evolume.Shared, e2evolume.ReadOnly
}

func (d *driverDefinition) GetCSIDriverName(e2econfig *storageframework.PerTestConfig) string {
	return d.DriverInfo.Name
}

func (d *driverDefinition) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	e2econfig := &storageframework.PerTestConfig{
		Driver:              d,
		Prefix:              "external",
		Framework:           f,
		ClientNodeSelection: e2epod.NodeSelection{Name: d.ClientNodeName},
	}

	if framework.NodeOSDistroIs("windows") {
		e2econfig.ClientNodeSelection.Selector = map[string]string{"kubernetes.io/os": "windows"}
	} else {
		e2econfig.ClientNodeSelection.Selector = map[string]string{"kubernetes.io/os": "linux"}
	}

	// 加入所有提供的 nodeSelector 设置。
	for key, value := range d.NodeSelectors {
		e2econfig.ClientNodeSelection.Selector[key] = value
	}

	return e2econfig
}
