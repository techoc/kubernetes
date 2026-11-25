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
 * 此文件定义了供 TestSuites 使用的各种 in-tree volume 测试 driver。
 *
 * 准备测试 driver 有两种方式：
 * 1) 使用容器化服务器（NFS、Ceph、iSCSI 等）。
 * 它会创建一个 server Pod，为测试定义一个 volume。
 * 这些测试只有在允许特权容器时才有效，因为导出
 * 各种文件系统（如 NFS）通常需要在 server Pod 中进行挂载或其他特权操作。
 *
 * 注意，这些 server 容器仅用于测试，不应
 * 用于生产环境。
 *
 * 2) 使用 Kubernetes 外部的服务器或云提供商（Cinder、GCE、AWS、Azure 等）。
 * 相应的服务器或云提供商必须存在于被测试的 Kubernetes 集群之外。
 * CreateVolume 将创建一个新 volume，供 TestSuites 中的 inlineVolume 或 DynamicPV 测试使用。
 */

package drivers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"

	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eauth "k8s.io/kubernetes/test/e2e/framework/auth"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	// iSCSI IQN 的模板。
	iSCSIIQNTemplate = "iqn.2003-01.io.k8s:e2e.%s"
)

type NFSProtocalVersion string

const (
	NFSv3 NFSProtocalVersion = "3"
	NFSv4 NFSProtocalVersion = "4"
)

// NFS
type nfsDriver struct {
	externalProvisionerPod *v1.Pod
	externalPluginName     string

	// NFS server 导出的路径。
	path       string
	driverInfo storageframework.DriverInfo
}

type nfsVolume struct {
	serverHost string
	serverPod  *v1.Pod
	f          *framework.Framework
}

var _ storageframework.TestDriver = &nfsDriver{}
var _ storageframework.PreprovisionedVolumeTestDriver = &nfsDriver{}
var _ storageframework.InlineVolumeTestDriver = &nfsDriver{}
var _ storageframework.PreprovisionedPVTestDriver = &nfsDriver{}
var _ storageframework.DynamicPVTestDriver = &nfsDriver{}

// InitNFSDriver 返回实现 TestDriver 接口的 nfsDriver
func InitNFSDriver(version NFSProtocalVersion) func() storageframework.TestDriver {
	var driverName, path, mountOption string
	switch version {
	case NFSv3:
		driverName = "nfs3"
		path = "/exports"
		mountOption = "vers=3"
	case NFSv4:
		driverName = "nfs"
		path = "/"
		mountOption = "vers=4.0"
	}

	return func() storageframework.TestDriver {
		return &nfsDriver{
			path: path,
			driverInfo: storageframework.DriverInfo{
				Name:             driverName,
				InTreePluginName: "kubernetes.io/nfs",
				MaxFileSize:      storageframework.FileSizeLarge,
				SupportedSizeRange: e2evolume.SizeRange{
					Min: "1Gi",
				},
				SupportedFsType: sets.NewString(
					"", // Default fsType
				),
				SupportedMountOption: sets.NewString("relatime"),
				RequiredMountOption:  sets.NewString(mountOption),
				Capabilities: map[storageframework.Capability]bool{
					storageframework.CapPersistence:       true,
					storageframework.CapExec:              true,
					storageframework.CapRWX:               true,
					storageframework.CapMultiPODs:         true,
					storageframework.CapMultiplePVsSameID: true,
				},
			},
		}
	}
}

func (n *nfsDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &n.driverInfo
}

func (n *nfsDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (n *nfsDriver) GetVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) *v1.VolumeSource {
	nv, ok := e2evolume.(*nfsVolume)
	if !ok {
		framework.Failf("Failed to cast test volume of type %T to the NFS test volume", e2evolume)
	}
	return &v1.VolumeSource{
		NFS: &v1.NFSVolumeSource{
			Server:   nv.serverHost,
			Path:     n.path,
			ReadOnly: readOnly,
		},
	}
}

func (n *nfsDriver) GetPersistentVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) (*v1.PersistentVolumeSource, *v1.VolumeNodeAffinity) {
	nv, ok := e2evolume.(*nfsVolume)
	if !ok {
		framework.Failf("Failed to cast test volume of type %T to the NFS test volume", e2evolume)
	}
	return &v1.PersistentVolumeSource{
		NFS: &v1.NFSVolumeSource{
			Server:   nv.serverHost,
			Path:     n.path,
			ReadOnly: readOnly,
		},
	}, nil
}

func (n *nfsDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := n.externalPluginName
	mountOptions := strings.Join(n.driverInfo.RequiredMountOption.List(), ",")
	parameters := map[string]string{"mountOptions": mountOptions}
	ns := config.Framework.Namespace.Name

	return storageframework.GetStorageClass(provisioner, parameters, nil, ns)
}

func (n *nfsDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	cs := f.ClientSet
	ns := f.Namespace
	n.externalPluginName = fmt.Sprintf("example.com/nfs-%s", ns.Name)

	// TODO(mkimuram): cluster-admin 权限过大，而 system:persistent-volume-provisioner
	// 权限不足。我们应为测试创建新的 clusterrole。
	cleanupFunc, err := e2eauth.BindClusterRole(ctx, cs.RbacV1(), "cluster-admin", ns.Name,
		rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: ns.Name, Name: "default"})
	framework.ExpectNoError(err)
	ginkgo.DeferCleanup(cleanupFunc)

	err = e2eauth.WaitForAuthorizationUpdate(ctx, cs.AuthorizationV1(),
		serviceaccount.MakeUsername(ns.Name, "default"),
		"", "get", schema.GroupResource{Group: "storage.k8s.io", Resource: "storageclasses"}, true)
	framework.ExpectNoError(err, "Failed to update authorization: %v", err)

	ginkgo.By("creating an external dynamic provisioner pod")
	n.externalProvisionerPod = utils.StartExternalProvisioner(ctx, cs, ns.Name, n.externalPluginName)
	ginkgo.DeferCleanup(e2epod.DeletePodWithWait, cs, n.externalProvisionerPod)

	return &storageframework.PerTestConfig{
		Driver:    n,
		Prefix:    "nfs",
		Framework: f,
	}
}

func (n *nfsDriver) CreateVolume(ctx context.Context, config *storageframework.PerTestConfig, volType storageframework.TestVolType) storageframework.TestVolume {
	f := config.Framework
	cs := f.ClientSet
	ns := f.Namespace

	// NewNFSServer 为 InlineVolume 和 PreprovisionedPV 创建 Pod，
	// startExternalProvisioner 为 DynamicPV 创建 Pod。
	// 因此需要针对 volType 使用不同的 PrepareTest 逻辑。
	switch volType {
	case storageframework.InlineVolume:
		fallthrough
	case storageframework.PreprovisionedPV:
		c, serverPod, serverHost := e2evolume.NewNFSServer(ctx, cs, ns.Name, []string{})
		config.ServerConfig = &c
		return &nfsVolume{
			serverHost: serverHost,
			serverPod:  serverPod,
			f:          f,
		}
	case storageframework.DynamicPV:
		// 不做任何操作
	default:
		framework.Failf("Unsupported volType:%v is specified", volType)
	}
	return nil
}

func (v *nfsVolume) DeleteVolume(ctx context.Context) {
	cleanUpVolumeServer(ctx, v.f, v.serverPod)
}

// iSCSI
// 所有节点上都必须安装 iscsiadm 工具和 iscsi target 内核模块。
type iSCSIDriver struct {
	driverInfo storageframework.DriverInfo
}
type iSCSIVolume struct {
	serverPod *v1.Pod
	serverIP  string
	f         *framework.Framework
	iqn       string
}

var _ storageframework.TestDriver = &iSCSIDriver{}
var _ storageframework.PreprovisionedVolumeTestDriver = &iSCSIDriver{}
var _ storageframework.InlineVolumeTestDriver = &iSCSIDriver{}
var _ storageframework.PreprovisionedPVTestDriver = &iSCSIDriver{}

// InitISCSIDriver 返回实现 TestDriver 接口的 iSCSIDriver
func InitISCSIDriver() storageframework.TestDriver {
	return &iSCSIDriver{
		driverInfo: storageframework.DriverInfo{
			Name:             "iscsi",
			InTreePluginName: "kubernetes.io/iscsi",
			TestTags:         []interface{}{feature.Volumes},
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedFsType: sets.NewString(
				"", // Default fsType
				"ext4",
			),
			TopologyKeys: []string{v1.LabelHostname},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:       true,
				storageframework.CapFsGroup:           true,
				storageframework.CapBlock:             true,
				storageframework.CapExec:              true,
				storageframework.CapMultiPODs:         true,
				storageframework.CapTopology:          true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
	}
}

func (i *iSCSIDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &i.driverInfo
}

func (i *iSCSIDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (i *iSCSIDriver) GetVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) *v1.VolumeSource {
	iv, ok := e2evolume.(*iSCSIVolume)
	if !ok {
		framework.Failf("failed to cast test volume of type %T to the iSCSI test volume", e2evolume)
	}

	volSource := v1.VolumeSource{
		ISCSI: &v1.ISCSIVolumeSource{
			TargetPortal: "127.0.0.1:3260",
			IQN:          iv.iqn,
			Lun:          0,
			ReadOnly:     readOnly,
		},
	}
	if fsType != "" {
		volSource.ISCSI.FSType = fsType
	}
	return &volSource
}

func (i *iSCSIDriver) GetPersistentVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) (*v1.PersistentVolumeSource, *v1.VolumeNodeAffinity) {
	iv, ok := e2evolume.(*iSCSIVolume)
	if !ok {
		framework.Failf("failed to cast test volume of type %T to the iSCSI test volume", e2evolume)
	}

	pvSource := v1.PersistentVolumeSource{
		ISCSI: &v1.ISCSIPersistentVolumeSource{
			TargetPortal: "127.0.0.1:3260",
			IQN:          iv.iqn,
			Lun:          0,
			ReadOnly:     readOnly,
		},
	}
	if fsType != "" {
		pvSource.ISCSI.FSType = fsType
	}
	return &pvSource, nil
}

func (i *iSCSIDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    i,
		Prefix:    "iscsi",
		Framework: f,
	}
}

func (i *iSCSIDriver) CreateVolume(ctx context.Context, config *storageframework.PerTestConfig, volType storageframework.TestVolType) storageframework.TestVolume {
	f := config.Framework
	cs := f.ClientSet
	ns := f.Namespace

	c, serverPod, serverIP, iqn := newISCSIServer(ctx, cs, ns.Name)
	config.ServerConfig = &c
	config.ClientNodeSelection = c.ClientNodeSelection
	return &iSCSIVolume{
		serverPod: serverPod,
		serverIP:  serverIP,
		iqn:       iqn,
		f:         f,
	}
}

// newISCSIServer 是 CreateStorageServer 的 iSCSI 专用包装器。
func newISCSIServer(ctx context.Context, cs clientset.Interface, namespace string) (config e2evolume.TestConfig, pod *v1.Pod, ip, iqn string) {
	// 生成集群范围内唯一的 IQN
	iqn = fmt.Sprintf(iSCSIIQNTemplate, namespace)
	config = e2evolume.TestConfig{
		Namespace:   namespace,
		Prefix:      "iscsi",
		ServerImage: imageutils.GetE2EImage(imageutils.VolumeISCSIServer),
		ServerArgs:  []string{iqn},
		ServerVolumes: map[string]string{
			// iSCSI 容器需要从宿主机插入内核模块
			"/lib/modules": "/lib/modules",
			// iSCSI 容器需要配置内核
			"/sys/kernel": "/sys/kernel",
			// iSCSI 源 "block devices" 必须在宿主机上可用
			"/srv/iscsi": "/srv/iscsi",
			// targetcli 使用 dbus
			"/run/dbus": "/run/dbus",
		},
		ServerReadyMessage: "iscsi target started",
		ServerHostNetwork:  true,
	}
	pod, ip = e2evolume.CreateStorageServer(ctx, cs, config)
	// 确保 client 与 server 运行在同一节点，这样就不需要开放防火墙。
	config.ClientNodeSelection = e2epod.NodeSelection{Name: pod.Spec.NodeName}
	return config, pod, ip, iqn
}

func (v *iSCSIVolume) DeleteVolume(ctx context.Context) {
	cleanUpVolumeServer(ctx, v.f, v.serverPod)
}

// Hostpath
type hostPathDriver struct {
	driverInfo storageframework.DriverInfo
}

type hostPathVolume struct {
	targetPath string
	prepPod    *v1.Pod
	f          *framework.Framework
}

var _ storageframework.TestDriver = &hostPathDriver{}
var _ storageframework.PreprovisionedVolumeTestDriver = &hostPathDriver{}
var _ storageframework.InlineVolumeTestDriver = &hostPathDriver{}

// InitHostPathDriver 返回实现 TestDriver 接口的 hostPathDriver
func InitHostPathDriver() storageframework.TestDriver {
	return &hostPathDriver{
		driverInfo: storageframework.DriverInfo{
			Name:             "hostPath",
			InTreePluginName: "kubernetes.io/host-path",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedFsType: sets.NewString(
				"", // Default fsType
			),
			TopologyKeys: []string{v1.LabelHostname},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:       true,
				storageframework.CapMultiPODs:         true,
				storageframework.CapSingleNodeVolume:  true,
				storageframework.CapTopology:          true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
	}
}

func (h *hostPathDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &h.driverInfo
}

func (h *hostPathDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (h *hostPathDriver) GetVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) *v1.VolumeSource {
	hv, ok := e2evolume.(*hostPathVolume)
	if !ok {
		framework.Failf("Failed to cast test volume of type %T to the Hostpath test volume", e2evolume)
	}

	// hostPath 不支持 readOnly volume
	if readOnly {
		return nil
	}
	return &v1.VolumeSource{
		HostPath: &v1.HostPathVolumeSource{
			Path: hv.targetPath,
		},
	}
}

func (h *hostPathDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    h,
		Prefix:    "hostpath",
		Framework: f,
	}
}

func (h *hostPathDriver) CreateVolume(ctx context.Context, config *storageframework.PerTestConfig, volType storageframework.TestVolType) storageframework.TestVolume {
	f := config.Framework
	cs := f.ClientSet

	targetPath := fmt.Sprintf("/tmp/%v", f.Namespace.Name)
	volumeName := "test-volume"

	// Pod 应被调度到该节点
	node, err := e2enode.GetRandomReadySchedulableNode(ctx, cs)
	framework.ExpectNoError(err)
	config.ClientNodeSelection = e2epod.NodeSelection{Name: node.Name}

	cmd := fmt.Sprintf("mkdir %v -m 777", targetPath)
	privileged := true

	// 启动 Pod 以初始化 hostPath 目录
	prepPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("hostpath-prep-%s", f.Namespace.Name),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    fmt.Sprintf("init-volume-%s", f.Namespace.Name),
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"/bin/sh", "-ec", cmd},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: "/tmp",
						},
					},
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
			},
			NodeName: node.Name,
		},
	}
	// h.prepPod 将在 cleanupDriver 中复用。
	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, prepPod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating hostPath init pod")

	err = e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "while waiting for hostPath init pod to succeed")

	err = e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
	framework.ExpectNoError(err, "while deleting hostPath init pod")
	return &hostPathVolume{
		targetPath: targetPath,
		prepPod:    prepPod,
		f:          f,
	}
}

var _ storageframework.TestVolume = &hostPathVolume{}

// DeleteVolume 实现 storageframework.TestVolume 接口方法
func (v *hostPathVolume) DeleteVolume(ctx context.Context) {
	f := v.f

	cmd := fmt.Sprintf("rm -rf %v", v.targetPath)
	v.prepPod.Spec.Containers[0].Command = []string{"/bin/sh", "-ec", cmd}

	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, v.prepPod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating hostPath teardown pod")

	err = e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "while waiting for hostPath teardown pod to succeed")

	err = e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
	framework.ExpectNoError(err, "while deleting hostPath teardown pod")
}

// HostPathSymlink
type hostPathSymlinkDriver struct {
	driverInfo storageframework.DriverInfo
}

type hostPathSymlinkVolume struct {
	targetPath string
	sourcePath string
	prepPod    *v1.Pod
	f          *framework.Framework
}

var _ storageframework.TestDriver = &hostPathSymlinkDriver{}
var _ storageframework.PreprovisionedVolumeTestDriver = &hostPathSymlinkDriver{}
var _ storageframework.InlineVolumeTestDriver = &hostPathSymlinkDriver{}

// InitHostPathSymlinkDriver 返回实现 TestDriver 接口的 hostPathSymlinkDriver
func InitHostPathSymlinkDriver() storageframework.TestDriver {
	return &hostPathSymlinkDriver{
		driverInfo: storageframework.DriverInfo{
			Name:             "hostPathSymlink",
			InTreePluginName: "kubernetes.io/host-path",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedFsType: sets.NewString(
				"", // Default fsType
			),
			TopologyKeys: []string{v1.LabelHostname},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:       true,
				storageframework.CapMultiPODs:         true,
				storageframework.CapSingleNodeVolume:  true,
				storageframework.CapTopology:          true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
	}
}

func (h *hostPathSymlinkDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &h.driverInfo
}

func (h *hostPathSymlinkDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (h *hostPathSymlinkDriver) GetVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) *v1.VolumeSource {
	hv, ok := e2evolume.(*hostPathSymlinkVolume)
	if !ok {
		framework.Failf("Failed to cast test volume of type %T to the Hostpath Symlink test volume", e2evolume)
	}

	// hostPathSymlink 不支持 readOnly volume
	if readOnly {
		return nil
	}
	return &v1.VolumeSource{
		HostPath: &v1.HostPathVolumeSource{
			Path: hv.targetPath,
		},
	}
}

func (h *hostPathSymlinkDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    h,
		Prefix:    "hostpathsymlink",
		Framework: f,
	}
}

func (h *hostPathSymlinkDriver) CreateVolume(ctx context.Context, config *storageframework.PerTestConfig, volType storageframework.TestVolType) storageframework.TestVolume {
	f := config.Framework
	cs := f.ClientSet

	sourcePath := fmt.Sprintf("/tmp/%v", f.Namespace.Name)
	targetPath := fmt.Sprintf("/tmp/%v-link", f.Namespace.Name)
	volumeName := "test-volume"

	// Pod 应被调度到该节点
	node, err := e2enode.GetRandomReadySchedulableNode(ctx, cs)
	framework.ExpectNoError(err)
	config.ClientNodeSelection = e2epod.NodeSelection{Name: node.Name}

	cmd := fmt.Sprintf("mkdir %v -m 777 && ln -s %v %v", sourcePath, sourcePath, targetPath)
	privileged := true

	// 启动 Pod 以初始化 hostPath 目录和符号链接
	prepPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("hostpath-symlink-prep-%s", f.Namespace.Name),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    fmt.Sprintf("init-volume-%s", f.Namespace.Name),
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"/bin/sh", "-ec", cmd},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: "/tmp",
						},
					},
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
			},
			NodeName: node.Name,
		},
	}
	// h.prepPod 将在 cleanupDriver 中复用。
	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, prepPod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating hostPath init pod")

	err = e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "while waiting for hostPath init pod to succeed")

	err = e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
	framework.ExpectNoError(err, "while deleting hostPath init pod")
	return &hostPathSymlinkVolume{
		sourcePath: sourcePath,
		targetPath: targetPath,
		prepPod:    prepPod,
		f:          f,
	}
}

var _ storageframework.TestVolume = &hostPathSymlinkVolume{}

// DeleteVolume 实现 storageframework.TestVolume 接口方法
func (v *hostPathSymlinkVolume) DeleteVolume(ctx context.Context) {
	f := v.f

	cmd := fmt.Sprintf("rm -rf %v&& rm -rf %v", v.targetPath, v.sourcePath)
	v.prepPod.Spec.Containers[0].Command = []string{"/bin/sh", "-ec", cmd}

	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, v.prepPod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating hostPath teardown pod")

	err = e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "while waiting for hostPath teardown pod to succeed")

	err = e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
	framework.ExpectNoError(err, "while deleting hostPath teardown pod")
}

// emptydir
type emptydirDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &emptydirDriver{}
var _ storageframework.PreprovisionedVolumeTestDriver = &emptydirDriver{}
var _ storageframework.InlineVolumeTestDriver = &emptydirDriver{}

// InitEmptydirDriver 返回实现 TestDriver 接口的 emptydirDriver
func InitEmptydirDriver() storageframework.TestDriver {
	return &emptydirDriver{
		driverInfo: storageframework.DriverInfo{
			Name:             "emptydir",
			InTreePluginName: "kubernetes.io/empty-dir",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedFsType: sets.NewString(
				"", // Default fsType
			),
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapExec:             true,
				storageframework.CapSingleNodeVolume: true,
			},
		},
	}
}

func (e *emptydirDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &e.driverInfo
}

func (e *emptydirDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (e *emptydirDriver) GetVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) *v1.VolumeSource {
	// emptydir 不支持 readOnly volume
	if readOnly {
		return nil
	}
	return &v1.VolumeSource{
		EmptyDir: &v1.EmptyDirVolumeSource{},
	}
}

func (e *emptydirDriver) CreateVolume(ctx context.Context, config *storageframework.PerTestConfig, volType storageframework.TestVolType) storageframework.TestVolume {
	return nil
}

func (e *emptydirDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    e,
		Prefix:    "emptydir",
		Framework: f,
	}
}

// Cinder
// 此测试只覆盖动态供给 volume 的 CSI 迁移。
type cinderDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &cinderDriver{}
var _ storageframework.DynamicPVTestDriver = &cinderDriver{}

// InitCinderDriver 返回实现 TestDriver 接口的 cinderDriver
func InitCinderDriver() storageframework.TestDriver {
	return &cinderDriver{
		driverInfo: storageframework.DriverInfo{
			TestTags:         []any{framework.WithProvider("openstack")},
			Name:             "cinder",
			InTreePluginName: "kubernetes.io/cinder",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			SupportedFsType: sets.NewString(
				"", // Default fsType
			),
			TopologyKeys: []string{v1.LabelFailureDomainBetaZone},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence: true,
				storageframework.CapFsGroup:     true,
				storageframework.CapExec:        true,
				storageframework.CapBlock:       true,
				// Cinder 支持 volume 数量限制，但该测试创建了大量
				// volume，导致测试套件超时。
				storageframework.CapVolumeLimits: false,
				storageframework.CapTopology:     true,
			},
		},
	}
}

func (c *cinderDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &c.driverInfo
}

func (c *cinderDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (c *cinderDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := "kubernetes.io/cinder"
	parameters := map[string]string{}
	if fsType != "" {
		parameters["fsType"] = fsType
	}
	ns := config.Framework.Namespace.Name

	return storageframework.GetStorageClass(provisioner, parameters, nil, ns)
}

func (c *cinderDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    c,
		Prefix:    "cinder",
		Framework: f,
	}
}

// GCE
type gcePdDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &gcePdDriver{}
var _ storageframework.DynamicPVTestDriver = &gcePdDriver{}

// InitGcePdDriver 返回实现 TestDriver 接口的 gcePdDriver
func InitGcePdDriver() storageframework.TestDriver {
	supportedTypes := sets.NewString(
		"", // Default fsType
		"ext2",
		"ext3",
		"ext4",
		"xfs",
	)
	return &gcePdDriver{
		driverInfo: storageframework.DriverInfo{
			TestTags:         []any{framework.WithProvider("gce")},
			Name:             "gcepd",
			InTreePluginName: "kubernetes.io/gce-pd",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			SupportedFsType:      supportedTypes,
			SupportedMountOption: sets.NewString("debug", "nouid32"),
			TopologyKeys:         []string{v1.LabelTopologyZone},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:         true,
				storageframework.CapFsGroup:             true,
				storageframework.CapBlock:               true,
				storageframework.CapExec:                true,
				storageframework.CapMultiPODs:           true,
				storageframework.CapControllerExpansion: true,
				storageframework.CapOfflineExpansion:    true,
				storageframework.CapOnlineExpansion:     true,
				storageframework.CapNodeExpansion:       true,
				// GCE 支持 volume 数量限制，但该测试创建了大量
				// volume，导致测试套件超时。
				storageframework.CapVolumeLimits:      false,
				storageframework.CapTopology:          true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
	}
}

// InitWindowsGcePdDriver 返回在 Windows 集群上运行、实现 TestDriver 接口的 gcePdDriver
// 在当前测试结构中，先初始化 driver，再设置
// 新的 framework，因此这里无法获取正确的 OS 并选择支持的文件系统。
// 因此这里使用一个单独的 Windows in-tree gce pd driver
func InitWindowsGcePdDriver() storageframework.TestDriver {
	supportedTypes := sets.NewString(
		"ntfs",
	)
	return &gcePdDriver{
		driverInfo: storageframework.DriverInfo{
			TestTags:         []any{framework.WithProvider("gce")},
			Name:             "windows-gcepd",
			InTreePluginName: "kubernetes.io/gce-pd",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			SupportedFsType: supportedTypes,
			TopologyKeys:    []string{v1.LabelZoneFailureDomain},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapControllerExpansion: false,
				storageframework.CapPersistence:         true,
				storageframework.CapExec:                true,
				storageframework.CapMultiPODs:           true,
				// GCE 支持 volume 数量限制，但该测试创建了大量
				// volume，导致测试套件超时。
				storageframework.CapVolumeLimits:      false,
				storageframework.CapTopology:          true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
	}
}

func (g *gcePdDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &g.driverInfo
}

func (g *gcePdDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	for _, tag := range pattern.TestTags {
		if tag == feature.Windows {
			if !framework.NodeOSDistroIs("windows") {
				return "not supported on Windows"
			}
		}
	}
	return ""
}

func (g *gcePdDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := "kubernetes.io/gce-pd"
	parameters := map[string]string{}
	if fsType != "" {
		parameters["fsType"] = fsType
	}
	ns := config.Framework.Namespace.Name
	delayedBinding := storagev1.VolumeBindingWaitForFirstConsumer

	return storageframework.GetStorageClass(provisioner, parameters, &delayedBinding, ns)
}

func (g *gcePdDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	config := &storageframework.PerTestConfig{
		Driver:    g,
		Prefix:    "gcepd",
		Framework: f,
	}

	if framework.NodeOSDistroIs("windows") {
		config.ClientNodeSelection = e2epod.NodeSelection{
			Selector: map[string]string{
				"kubernetes.io/os": "windows",
			},
		}
	}
	return config

}

// vSphere
type vSphereDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &vSphereDriver{}
var _ storageframework.DynamicPVTestDriver = &vSphereDriver{}

// InitVSphereDriver 返回实现 TestDriver 接口的 vSphereDriver
func InitVSphereDriver() storageframework.TestDriver {
	return &vSphereDriver{
		driverInfo: storageframework.DriverInfo{
			TestTags:         []any{framework.WithProvider("vsphere")},
			Name:             "vsphere",
			InTreePluginName: "kubernetes.io/vsphere-volume",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			SupportedFsType: sets.NewString(
				"", // Default fsType
				"ext4",
				"ntfs",
			),
			TopologyKeys: []string{v1.LabelFailureDomainBetaZone},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:       true,
				storageframework.CapFsGroup:           true,
				storageframework.CapExec:              true,
				storageframework.CapMultiPODs:         true,
				storageframework.CapTopology:          true,
				storageframework.CapBlock:             true,
				storageframework.CapMultiplePVsSameID: false,
			},
		},
	}
}

func (v *vSphereDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &v.driverInfo
}

func (v *vSphereDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (v *vSphereDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := "kubernetes.io/vsphere-volume"
	parameters := map[string]string{}
	if fsType != "" {
		parameters["fsType"] = fsType
	}
	ns := config.Framework.Namespace.Name

	return storageframework.GetStorageClass(provisioner, parameters, nil, ns)
}

func (v *vSphereDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    v,
		Prefix:    "vsphere",
		Framework: f,
	}
}

// Azure Disk
type azureDiskDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &azureDiskDriver{}
var _ storageframework.DynamicPVTestDriver = &azureDiskDriver{}
var _ storageframework.CustomTimeoutsTestDriver = &azureDiskDriver{}

// InitAzureDiskDriver 返回实现 TestDriver 接口的 azureDiskDriver
func InitAzureDiskDriver() storageframework.TestDriver {
	return &azureDiskDriver{
		driverInfo: storageframework.DriverInfo{
			TestTags:         []any{framework.WithProvider("azure")},
			Name:             "azure-disk",
			InTreePluginName: "kubernetes.io/azure-disk",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			SupportedFsType: sets.NewString(
				"", // Default fsType
				"ext4",
				"xfs",
			),
			TopologyKeys: []string{v1.LabelFailureDomainBetaZone},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence: true,
				storageframework.CapFsGroup:     true,
				storageframework.CapBlock:       true,
				storageframework.CapExec:        true,
				storageframework.CapMultiPODs:   true,
				// Azure 支持 volume 数量限制，但该测试创建了大量
				// volume，导致测试套件超时。
				storageframework.CapVolumeLimits:      false,
				storageframework.CapTopology:          true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
	}
}

func (a *azureDiskDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &a.driverInfo
}

func (a *azureDiskDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (a *azureDiskDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := "kubernetes.io/azure-disk"
	parameters := map[string]string{}
	if fsType != "" {
		parameters["fsType"] = fsType
	}
	ns := config.Framework.Namespace.Name
	delayedBinding := storagev1.VolumeBindingWaitForFirstConsumer

	return storageframework.GetStorageClass(provisioner, parameters, &delayedBinding, ns)
}

func (a *azureDiskDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    a,
		Prefix:    "azure",
		Framework: f,
	}
}

func (a *azureDiskDriver) GetTimeouts() *framework.TimeoutContext {
	timeouts := framework.NewTimeoutContext()
	timeouts.PodStart = time.Minute * 15
	timeouts.PodDelete = time.Minute * 15
	timeouts.PVDelete = time.Minute * 20
	return timeouts
}

// AWS
type awsDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &awsDriver{}
var _ storageframework.DynamicPVTestDriver = &awsDriver{}

// InitAwsDriver 返回实现 TestDriver 接口的 awsDriver
func InitAwsDriver() storageframework.TestDriver {
	return &awsDriver{
		driverInfo: storageframework.DriverInfo{
			TestTags:         []any{framework.WithProvider("aws")},
			Name:             "aws",
			InTreePluginName: "kubernetes.io/aws-ebs",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			SupportedFsType: sets.NewString(
				"", // Default fsType
				"ext4",
				"xfs",
				"ntfs",
			),
			SupportedMountOption: sets.NewString("debug", "nouid32"),
			TopologyKeys:         []string{v1.LabelTopologyZone},
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:         true,
				storageframework.CapFsGroup:             true,
				storageframework.CapBlock:               true,
				storageframework.CapExec:                true,
				storageframework.CapMultiPODs:           true,
				storageframework.CapControllerExpansion: true,
				storageframework.CapNodeExpansion:       true,
				storageframework.CapOfflineExpansion:    true,
				storageframework.CapOnlineExpansion:     true,
				// AWS 支持 volume 数量限制，但该测试创建了大量
				// volume，导致测试套件超时。
				storageframework.CapVolumeLimits:      false,
				storageframework.CapTopology:          true,
				storageframework.CapMultiplePVsSameID: true,
			},
		},
	}
}

func (a *awsDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &a.driverInfo
}

func (a *awsDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (a *awsDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := "kubernetes.io/aws-ebs"
	parameters := map[string]string{}
	if fsType != "" {
		parameters["fsType"] = fsType
	}
	ns := config.Framework.Namespace.Name
	delayedBinding := storagev1.VolumeBindingWaitForFirstConsumer

	return storageframework.GetStorageClass(provisioner, parameters, &delayedBinding, ns)
}

func (a *awsDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	config := &storageframework.PerTestConfig{
		Driver:    a,
		Prefix:    "aws",
		Framework: f,
	}

	if framework.NodeOSDistroIs("windows") {
		config.ClientNodeSelection = e2epod.NodeSelection{
			Selector: map[string]string{
				"kubernetes.io/os": "windows",
			},
		}
	}
	return config
}

// local
type localDriver struct {
	driverInfo storageframework.DriverInfo
	node       *v1.Node
	hostExec   utils.HostExec
	// volumeType 表示我们正在测试的本地 volume 类型，例如 tmpfs、
	// directory、block device。
	volumeType utils.LocalVolumeType
	ltrMgr     utils.LocalTestResourceManager
}

type localVolume struct {
	ltrMgr utils.LocalTestResourceManager
	ltr    *utils.LocalTestResource
}

var (
	// capabilities
	defaultLocalVolumeCapabilities = map[storageframework.Capability]bool{
		storageframework.CapPersistence: true,
		storageframework.CapFsGroup:     true,
		storageframework.CapBlock:       false,
		// 要测试 CapExec，需要有文件系统的 volume。
		// 在端到端（e2e）测试中，我们使用 `/tmp` 目录创建 volume。
		// 然而，最佳实践建议使用 `noexec`、`nodev` 和 `nosuid` 参数挂载 `/tmp`。
		// 该安全措施可防止在 `/tmp` 目录内执行脚本和二进制文件。
		// 这种做法提升了安全性，但也让 e2e 测试依赖于基础设施配置。
		// 当尝试执行 `/tmp` 中的文件时，可能导致 "Permission Denied" 错误。
		// 为此，我们有意跳过某些类型 LocalVolume（如 `dir` 或 `dir-link`）的 exec 测试。
		// 这样可以在不依赖可能严格的安全配置的情况下进行全面测试。
		storageframework.CapExec:              false,
		storageframework.CapMultiPODs:         true,
		storageframework.CapSingleNodeVolume:  true,
		storageframework.CapMultiplePVsSameID: true,
	}
	localVolumeCapabitilies = map[utils.LocalVolumeType]map[storageframework.Capability]bool{
		utils.LocalVolumeTmpfs: {
			storageframework.CapPersistence:       true,
			storageframework.CapFsGroup:           true,
			storageframework.CapBlock:             false,
			storageframework.CapExec:              true,
			storageframework.CapMultiPODs:         true,
			storageframework.CapSingleNodeVolume:  true,
			storageframework.CapMultiplePVsSameID: true,
		},
		utils.LocalVolumeBlock: {
			storageframework.CapPersistence:       true,
			storageframework.CapFsGroup:           true,
			storageframework.CapBlock:             true,
			storageframework.CapExec:              false,
			storageframework.CapMultiPODs:         true,
			storageframework.CapSingleNodeVolume:  true,
			storageframework.CapMultiplePVsSameID: true,
		},
		utils.LocalVolumeBlockFS: {
			storageframework.CapPersistence:       true,
			storageframework.CapFsGroup:           true,
			storageframework.CapBlock:             false,
			storageframework.CapExec:              true,
			storageframework.CapMultiPODs:         true,
			storageframework.CapSingleNodeVolume:  true,
			storageframework.CapMultiplePVsSameID: true,
		},
		utils.LocalVolumeGCELocalSSD: {
			storageframework.CapPersistence:       true,
			storageframework.CapFsGroup:           true,
			storageframework.CapBlock:             false,
			storageframework.CapExec:              true,
			storageframework.CapMultiPODs:         true,
			storageframework.CapSingleNodeVolume:  true,
			storageframework.CapMultiplePVsSameID: true,
		},
	}
	// fstype
	defaultLocalVolumeSupportedFsTypes = sets.NewString("")
	localVolumeSupportedFsTypes        = map[utils.LocalVolumeType]sets.String{
		utils.LocalVolumeBlock: sets.NewString(
			"", // Default fsType
			"ext4",
			// "xfs"，已禁用，参见 issue https://github.com/kubernetes/kubernetes/issues/74095
		),
	}
	// 最大文件大小
	defaultLocalVolumeMaxFileSize = storageframework.FileSizeSmall
	localVolumeMaxFileSizes       = map[utils.LocalVolumeType]int64{}
)

var _ storageframework.TestDriver = &localDriver{}
var _ storageframework.PreprovisionedVolumeTestDriver = &localDriver{}
var _ storageframework.PreprovisionedPVTestDriver = &localDriver{}

// InitLocalDriverWithVolumeType 根据 volume 类型初始化 local driver。
func InitLocalDriverWithVolumeType(volumeType utils.LocalVolumeType) func() storageframework.TestDriver {
	maxFileSize := defaultLocalVolumeMaxFileSize
	if maxFileSizeByVolType, ok := localVolumeMaxFileSizes[volumeType]; ok {
		maxFileSize = maxFileSizeByVolType
	}
	supportedFsTypes := defaultLocalVolumeSupportedFsTypes
	if supportedFsTypesByType, ok := localVolumeSupportedFsTypes[volumeType]; ok {
		supportedFsTypes = supportedFsTypesByType
	}
	capabilities := defaultLocalVolumeCapabilities
	if capabilitiesByType, ok := localVolumeCapabitilies[volumeType]; ok {
		capabilities = capabilitiesByType
	}
	return func() storageframework.TestDriver {
		// 自定义标签，用于与其他 volume 类型的测试区分
		testTags := []interface{}{framework.WithLabel("LocalVolumeType: " + string(volumeType))}
		// 对于 GCE Local SSD volume，必须串行运行
		if volumeType == utils.LocalVolumeGCELocalSSD {
			testTags = append(testTags, framework.WithSerial())
		}
		return &localDriver{
			driverInfo: storageframework.DriverInfo{
				Name:             "local",
				InTreePluginName: "kubernetes.io/local-volume",
				TestTags:         testTags,
				MaxFileSize:      maxFileSize,
				SupportedFsType:  supportedFsTypes,
				Capabilities:     capabilities,
			},
			volumeType: volumeType,
		}
	}
}

func (l *localDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &l.driverInfo
}

func (l *localDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (l *localDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	var err error
	l.node, err = e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
	framework.ExpectNoError(err)

	l.hostExec = utils.NewHostExec(f)
	l.ltrMgr = utils.NewLocalResourceManager("local-driver", l.hostExec, "/tmp")

	// 这无法在 SkipUnsupportedTest 中完成，因为测试框架尚未初始化
	if l.volumeType == utils.LocalVolumeGCELocalSSD {
		ssdInterface := "scsi"
		filesystemType := "fs"
		ssdCmd := fmt.Sprintf("ls -1 /mnt/disks/by-uuid/google-local-ssds-%s-%s/ | wc -l", ssdInterface, filesystemType)
		res, err := l.hostExec.IssueCommandWithResult(ctx, ssdCmd, l.node)
		framework.ExpectNoError(err)
		num, err := strconv.Atoi(strings.TrimSpace(res))
		framework.ExpectNoError(err)
		if num < 1 {
			e2eskipper.Skipf("Requires at least 1 %s %s localSSD ", ssdInterface, filesystemType)
		}
	}

	ginkgo.DeferCleanup(l.hostExec.Cleanup)
	return &storageframework.PerTestConfig{
		Driver:              l,
		Prefix:              "local",
		Framework:           f,
		ClientNodeSelection: e2epod.NodeSelection{Name: l.node.Name},
	}
}

func (l *localDriver) CreateVolume(ctx context.Context, config *storageframework.PerTestConfig, volType storageframework.TestVolType) storageframework.TestVolume {
	switch volType {
	case storageframework.PreprovisionedPV:
		node := l.node
		// 设置此项以将 Pod 调度到该节点
		config.ClientNodeSelection = e2epod.NodeSelection{Name: node.Name}
		return &localVolume{
			ltrMgr: l.ltrMgr,
			ltr:    l.ltrMgr.Create(ctx, node, l.volumeType, nil),
		}
	default:
		framework.Failf("Unsupported volType: %v is specified", volType)
	}
	return nil
}

func (v *localVolume) DeleteVolume(ctx context.Context) {
	v.ltrMgr.Remove(ctx, v.ltr)
}

func (l *localDriver) nodeAffinityForNode(node *v1.Node) *v1.VolumeNodeAffinity {
	nodeKey := "kubernetes.io/hostname"
	if node.Labels == nil {
		framework.Failf("Node does not have labels")
	}
	nodeValue, found := node.Labels[nodeKey]
	if !found {
		framework.Failf("Node does not have required label %q", nodeKey)
	}
	return &v1.VolumeNodeAffinity{
		Required: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      nodeKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{nodeValue},
						},
					},
				},
			},
		},
	}
}

func (l *localDriver) GetPersistentVolumeSource(readOnly bool, fsType string, e2evolume storageframework.TestVolume) (*v1.PersistentVolumeSource, *v1.VolumeNodeAffinity) {
	lv, ok := e2evolume.(*localVolume)
	if !ok {
		framework.Failf("Failed to cast test volume of type %T to the local test volume", e2evolume)
	}
	return &v1.PersistentVolumeSource{
		Local: &v1.LocalVolumeSource{
			Path:   lv.ltr.Path,
			FSType: &fsType,
		},
	}, l.nodeAffinityForNode(lv.ltr.Node)
}

// cleanUpVolumeServer 是 volume server 清理函数的包装器。
func cleanUpVolumeServer(ctx context.Context, f *framework.Framework, serverPod *v1.Pod) {
	framework.Logf("Deleting server pod %q...", serverPod.Name)
	err := e2epod.DeletePodWithWait(ctx, f.ClientSet, serverPod)
	if err != nil {
		framework.Logf("Server pod delete failed: %v", err)
	}
}

// Azure File
type azureFileDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &azureFileDriver{}
var _ storageframework.DynamicPVTestDriver = &azureFileDriver{}

// InitAzureFileDriver 返回实现 TestDriver 接口的 azureFileDriver
func InitAzureFileDriver() storageframework.TestDriver {
	return &azureFileDriver{
		driverInfo: storageframework.DriverInfo{
			TestTags:         []any{framework.WithProvider("azure")},
			Name:             "azure-file",
			InTreePluginName: "kubernetes.io/azure-file",
			MaxFileSize:      storageframework.FileSizeMedium,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			SupportedFsType: sets.NewString(
				"", // Default fsType
			),
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence:         true,
				storageframework.CapExec:                true,
				storageframework.CapRWX:                 true,
				storageframework.CapMultiPODs:           true,
				storageframework.CapControllerExpansion: true,
				storageframework.CapNodeExpansion:       true,
				storageframework.CapMultiplePVsSameID:   true,
			},
		},
	}
}

func (a *azureFileDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &a.driverInfo
}

func (a *azureFileDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) string {
	return ""
}

func (a *azureFileDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	provisioner := "kubernetes.io/azure-file"
	parameters := map[string]string{}
	ns := config.Framework.Namespace.Name
	immediateBinding := storagev1.VolumeBindingImmediate
	return storageframework.GetStorageClass(provisioner, parameters, &immediateBinding, ns)
}

func (a *azureFileDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    a,
		Prefix:    "azure-file",
		Framework: f,
	}
}
