/*
Copyright 2014 The Kubernetes Authors.

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

package hostpath

import (
	"fmt"
	"os"
	"regexp"

	"k8s.io/klog/v2"

	"github.com/opencontainers/selinux/go-selinux"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/recyclerclient"
	"k8s.io/kubernetes/pkg/volume/validation"
	"k8s.io/mount-utils"
)

// ProbeVolumePlugins 是 volume 插件的主要入口。
// volumeConfig 参数用于配置卷行为；使用指针是为了允许传入 nil。
// hostPathPlugin 保存 volumeConfig，并在需要时把它交给执行回收的函数。
// 涉及回收的测试不应使用本函数，而应使用 ProbeRecyclablePlugins() 来覆盖默认行为。
func ProbeVolumePlugins(volumeConfig volume.VolumeConfig) []volume.VolumePlugin {
	return []volume.VolumePlugin{
		&hostPathPlugin{
			host:   nil,
			config: volumeConfig,
		},
	}
}

// FakeProbeVolumePlugins 返回一个跳过类型检查的插件实例，供测试使用。
func FakeProbeVolumePlugins(volumeConfig volume.VolumeConfig) []volume.VolumePlugin {
	return []volume.VolumePlugin{
		&hostPathPlugin{
			host:          nil,
			config:        volumeConfig,
			noTypeChecker: true,
		},
	}
}

// hostPathPlugin 实现 hostPath 卷插件，保存卷主机与插件配置。
type hostPathPlugin struct {
	host          volume.VolumeHost
	config        volume.VolumeConfig
	noTypeChecker bool
}

var _ volume.VolumePlugin = &hostPathPlugin{}
var _ volume.PersistentVolumePlugin = &hostPathPlugin{}
var _ volume.RecyclableVolumePlugin = &hostPathPlugin{}
var _ volume.DeletableVolumePlugin = &hostPathPlugin{}
var _ volume.ProvisionableVolumePlugin = &hostPathPlugin{}

const (
	hostPathPluginName = "kubernetes.io/host-path"
)

// Init 保存卷主机引用，完成插件初始化。
func (plugin *hostPathPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *hostPathPlugin) GetPluginName() string {
	return hostPathPluginName
}

// GetVolumeName 返回卷在宿主上的路径，作为卷名。
func (plugin *hostPathPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return volumeSource.Path, nil
}

// CanSupport 判断该 spec 是否为 hostPath 卷（内联卷或 PV）。
func (plugin *hostPathPlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.HostPath != nil) ||
		(spec.Volume != nil && spec.Volume.HostPath != nil)
}

// RequiresRemount 返回 false：hostPath 不需要周期性重挂。
func (plugin *hostPathPlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回 false：不支持挂载选项。
func (plugin *hostPathPlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 返回 false：不支持带 SELinux context 的挂载。
func (plugin *hostPathPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// GetAccessModes 返回插件支持的访问模式：单节点读写。
func (plugin *hostPathPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
	}
}

// NewMounter 根据 spec 创建 hostPath 的 Mounter。
func (plugin *hostPathPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	hostPathVolumeSource, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	path := hostPathVolumeSource.Path
	pathType := new(v1.HostPathType)
	if hostPathVolumeSource.Type == nil {
		*pathType = v1.HostPathUnset
	} else {
		pathType = hostPathVolumeSource.Type
	}
	kvh, ok := plugin.host.(volume.KubeletVolumeHost)
	if !ok {
		return nil, fmt.Errorf("plugin volume host does not implement KubeletVolumeHost interface")
	}
	return &hostPathMounter{
		hostPath:      &hostPath{path: path, pathType: pathType},
		readOnly:      readOnly,
		mounter:       plugin.host.GetMounter(),
		hu:            kvh.GetHostUtil(),
		noTypeChecker: plugin.noTypeChecker,
	}, nil
}

// NewUnmounter 创建 hostPath 的 Unmounter；hostPath 无需实际卸载，故不包含路径信息。
func (plugin *hostPathPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &hostPathUnmounter{&hostPath{
		path: "",
	}}, nil
}

// Recycle 回收并清理 HostPath 卷。
// Recycle 会阻塞，直到回收 Pod 完成或发生错误。
// HostPath 回收仅在单节点集群中有效，且只用于测试。
func (plugin *hostPathPlugin) Recycle(pvName string, spec *volume.Spec, eventRecorder recyclerclient.RecycleEventRecorder) error {
	if spec.PersistentVolume == nil || spec.PersistentVolume.Spec.HostPath == nil {
		return fmt.Errorf("spec.PersistentVolume.Spec.HostPath is nil")
	}

	pod := plugin.config.RecyclerPodTemplate
	timeout := util.CalculateTimeoutForVolume(plugin.config.RecyclerMinimumTimeout, plugin.config.RecyclerTimeoutIncrement, spec.PersistentVolume)
	// 用当前 PV 的信息覆盖回收 Pod 模板中的默认设置。
	pod.Spec.ActiveDeadlineSeconds = &timeout
	pod.Spec.Volumes[0].VolumeSource = v1.VolumeSource{
		HostPath: &v1.HostPathVolumeSource{
			Path: spec.PersistentVolume.Spec.HostPath.Path,
		},
	}
	return recyclerclient.RecycleVolumeByWatchingPodUntilCompletion(pvName, pod, plugin.host.GetKubeClient(), eventRecorder)
}

// NewDeleter 创建用于删除 hostPath PV 的 Deleter。
func (plugin *hostPathPlugin) NewDeleter(logger klog.Logger, spec *volume.Spec) (volume.Deleter, error) {
	return newDeleter(spec, plugin.host)
}

// NewProvisioner 创建动态供应器；若配置中未启用供应则报错。
func (plugin *hostPathPlugin) NewProvisioner(logger klog.Logger, options volume.VolumeOptions) (volume.Provisioner, error) {
	if !plugin.config.ProvisioningEnabled {
		return nil, fmt.Errorf("provisioning in volume plugin %q is disabled", plugin.GetPluginName())
	}
	return newProvisioner(options, plugin.host, plugin)
}

// ConstructVolumeSpec 根据卷名重建 hostPath 的 VolumeSpec；mountPath 未被使用。
func (plugin *hostPathPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	hostPathVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: volumeName,
			},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(hostPathVolume),
	}, nil
}

// newDeleter 从 PV spec 中取出 hostPath 路径并构造 Deleter。
func newDeleter(spec *volume.Spec, host volume.VolumeHost) (volume.Deleter, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.HostPath == nil {
		return nil, fmt.Errorf("spec.PersistentVolumeSource.HostPath is nil")
	}
	path := spec.PersistentVolume.Spec.HostPath.Path
	return &hostPathDeleter{name: spec.Name(), path: path, host: host}, nil
}

// newProvisioner 构造 hostPath 动态供应器，默认在 /tmp/hostpath_pv 下创建目录。
func newProvisioner(options volume.VolumeOptions, host volume.VolumeHost, plugin *hostPathPlugin) (volume.Provisioner, error) {
	return &hostPathProvisioner{options: options, host: host, plugin: plugin, basePath: "hostpath_pv"}, nil
}

// HostPath 卷表示直接挂载宿主上的文件或目录。
// 指定路径下的内容会直接暴露给容器。
type hostPath struct {
	path     string
	pathType *v1.HostPathType
	volume.MetricsNil
}

// GetPath 返回宿主文件路径。
func (hp *hostPath) GetPath() string {
	return hp.path
}

// hostPathMounter 负责 hostPath 卷的挂载准备与属性描述。
type hostPathMounter struct {
	*hostPath
	readOnly      bool
	mounter       mount.Interface
	hu            hostutil.HostUtils
	noTypeChecker bool
}

var _ volume.Mounter = &hostPathMounter{}

// GetAttributes 返回卷属性；hostPath 不受 kubelet 管理，也不触发 SELinux 重标记。
func (b *hostPathMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       b.readOnly,
		Managed:        false,
		SELinuxRelabel: false,
	}
}

// SetUp 不执行挂载，只校验路径合法性与类型约束。
func (b *hostPathMounter) SetUp(mounterArgs volume.MounterArgs) error {
	err := validation.ValidatePathNoBacksteps(b.GetPath())
	if err != nil {
		return fmt.Errorf("invalid HostPath `%s`: %v", b.GetPath(), err)
	}

	if *b.pathType == v1.HostPathUnset {
		return nil
	}
	if b.noTypeChecker {
		return nil
	} else {
		return checkType(b.GetPath(), b.pathType, b.hu)
	}
}

// SetUpAt 对 hostPath 没有意义，调用它通常是程序错误。
func (b *hostPathMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	return fmt.Errorf("SetUpAt() does not make sense for host paths")
}

// GetPath 返回宿主文件路径。
func (b *hostPathMounter) GetPath() string {
	return b.path
}

// hostPathUnmounter 实现 hostPath 的卸载接口；实际无需卸载操作。
type hostPathUnmounter struct {
	*hostPath
}

var _ volume.Unmounter = &hostPathUnmounter{}

// TearDown 不执行任何操作。
func (c *hostPathUnmounter) TearDown() error {
	return nil
}

// TearDownAt 对 hostPath 没有意义，调用它通常是程序错误。
func (c *hostPathUnmounter) TearDownAt(dir string) error {
	return fmt.Errorf("TearDownAt() does not make sense for host paths")
}

// hostPathProvisioner 为 HostPath 插件实现 Provisioner。
// 该实现只用于测试，且仅适用于单节点集群。
type hostPathProvisioner struct {
	host     volume.VolumeHost
	options  volume.VolumeOptions
	plugin   *hostPathPlugin
	basePath string
}

// hostPath 的 Provision 只需在本地创建 /tmp/%/%s 目录作为新的 PV，默认为 /tmp/hostpath_pv/%s。
// 该 Provisioner 仅用于开发和测试，在多节点集群中无法工作。
func (r *hostPathProvisioner) Provision(selectedNode *v1.Node, allowedTopologies []v1.TopologySelectorTerm) (*v1.PersistentVolume, error) {
	if util.CheckPersistentVolumeClaimModeBlock(r.options.PVC) {
		return nil, fmt.Errorf("%s does not support block volume provisioning", r.plugin.GetPluginName())
	}

	fullpath := fmt.Sprintf("/tmp/%s/%s", r.basePath, uuid.NewUUID())

	capacity := r.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: r.options.PVName,
			Annotations: map[string]string{
				util.VolumeDynamicallyCreatedByKey: "hostpath-dynamic-provisioner",
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: r.options.PersistentVolumeReclaimPolicy,
			AccessModes:                   r.options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): capacity,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: fullpath,
				},
			},
		},
	}
	if len(r.options.PVC.Spec.AccessModes) == 0 {
		pv.Spec.AccessModes = r.plugin.GetAccessModes()
	}

	if err := os.MkdirAll(pv.Spec.HostPath.Path, 0750); err != nil {
		return nil, err
	}
	if selinux.GetEnabled() {
		err := selinux.SetFileLabel(pv.Spec.HostPath.Path, kubeletconfig.KubeletContainersSharedSELinuxLabel)
		if err != nil {
			return nil, fmt.Errorf("failed to set selinux label for %q: %v", pv.Spec.HostPath.Path, err)
		}
	}

	return pv, nil
}

// hostPathDeleter 从集群中删除 hostPath PV。
// 该 deleter 仅适用于单主机集群，且只用于测试。
type hostPathDeleter struct {
	name string
	path string
	host volume.VolumeHost
	volume.MetricsNil
}

// GetPath 返回待删除的宿主路径。
func (r *hostPathDeleter) GetPath() string {
	return r.path
}

// hostPath 的 Delete 只在路径位于 /tmp/* 下时删除本地目录。
// 仅限测试与本地开发使用；除此之外不要使用这个 deleter。
func (r *hostPathDeleter) Delete() error {
	regexp := regexp.MustCompile("/tmp/.+")
	if !regexp.MatchString(r.GetPath()) {
		return fmt.Errorf("host_path deleter only supports /tmp/.+ but received provided %s", r.GetPath())
	}
	return os.RemoveAll(r.GetPath())
}

// getVolumeSource 从内联卷或 PV 中取出 HostPathVolumeSource 及只读标记。
func getVolumeSource(spec *volume.Spec) (*v1.HostPathVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.HostPath != nil {
		return spec.Volume.HostPath, spec.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.HostPath != nil {
		return spec.PersistentVolume.Spec.HostPath, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("spec does not reference an HostPath volume type")
}

// hostPathTypeChecker 抽象出路径类型探测与创建能力，便于测试。
type hostPathTypeChecker interface {
	Exists() bool
	IsFile() bool
	MakeFile() error
	IsDir() bool
	MakeDir() error
	IsBlock() bool
	IsChar() bool
	IsSocket() bool
	GetPath() string
	GetType() (string, error)
}

// fileTypeChecker 通过 HostUtils 查询宿主路径类型。
type fileTypeChecker struct {
	path string
	hu   hostutil.HostUtils
}

// Exists 判断路径是否存在；查询出错时视为不存在。
func (ftc *fileTypeChecker) Exists() bool {
	exists, err := ftc.hu.PathExists(ftc.path)
	return exists && err == nil
}

// GetType 返回 FileType 的字符串表示。
func (ftc *fileTypeChecker) GetType() (string, error) {
	pathType, err := ftc.hu.GetFileType(ftc.path)
	return string(pathType), err
}

// IsFile 判断路径是否为普通文件。
func (ftc *fileTypeChecker) IsFile() bool {
	if !ftc.Exists() {
		return false
	}
	pathType, err := ftc.GetType()
	if err != nil {
		return false
	}
	return pathType == string(v1.HostPathFile)
}

// MakeFile 在路径上创建空文件。
func (ftc *fileTypeChecker) MakeFile() error {
	return makeFile(ftc.path)
}

// IsDir 判断路径是否为目录。
func (ftc *fileTypeChecker) IsDir() bool {
	if !ftc.Exists() {
		return false
	}
	pathType, err := ftc.GetType()
	if err != nil {
		return false
	}
	return pathType == string(v1.HostPathDirectory)
}

// MakeDir 在路径上创建目录。
func (ftc *fileTypeChecker) MakeDir() error {
	return makeDir(ftc.path)
}

// IsBlock 判断路径是否为块设备。
func (ftc *fileTypeChecker) IsBlock() bool {
	blkDevType, err := ftc.GetType()
	if err != nil {
		return false
	}
	return blkDevType == string(v1.HostPathBlockDev)
}

// IsChar 判断路径是否为字符设备。
func (ftc *fileTypeChecker) IsChar() bool {
	charDevType, err := ftc.GetType()
	if err != nil {
		return false
	}
	return charDevType == string(v1.HostPathCharDev)
}

// IsSocket 判断路径是否为 socket 文件。
func (ftc *fileTypeChecker) IsSocket() bool {
	socketType, err := ftc.GetType()
	if err != nil {
		return false
	}
	return socketType == string(v1.HostPathSocket)
}

// GetPath 返回被检查的路径。
func (ftc *fileTypeChecker) GetPath() string {
	return ftc.path
}

// newFileTypeChecker 构造基于 HostUtils 的类型检查器。
func newFileTypeChecker(path string, hu hostutil.HostUtils) hostPathTypeChecker {
	return &fileTypeChecker{path: path, hu: hu}
}

// checkType 校验给定路径是否符合指定的 pathType。
func checkType(path string, pathType *v1.HostPathType, hu hostutil.HostUtils) error {
	return checkTypeInternal(newFileTypeChecker(path, hu), pathType)
}

// typeMatchedOrError 在类型不匹配时生成带实际类型的错误信息。
func typeMatchedOrError(ftc hostPathTypeChecker, match func() bool, expected string) error {
	if !match() {
		pathType, err := ftc.GetType()
		if err != nil {
			return fmt.Errorf("hostPath type check failed, %s is not a %s, unable to determine its type: %w", ftc.GetPath(), expected, err)
		}
		return fmt.Errorf("hostPath type check failed: %s is not a %s, it's a %s", ftc.GetPath(), expected, pathType)
	}
	return nil
}

// checkTypeInternal 按 pathType 逐一校验路径类型；*OrCreate 类型在路径不存在时先创建。
func checkTypeInternal(ftc hostPathTypeChecker, pathType *v1.HostPathType) error {
	switch *pathType {
	case v1.HostPathDirectoryOrCreate:
		if !ftc.Exists() {
			return ftc.MakeDir()
		}
		fallthrough
	case v1.HostPathDirectory:
		return typeMatchedOrError(ftc, ftc.IsDir, "directory")
	case v1.HostPathFileOrCreate:
		if !ftc.Exists() {
			return ftc.MakeFile()
		}
		fallthrough
	case v1.HostPathFile:
		return typeMatchedOrError(ftc, ftc.IsFile, "file")
	case v1.HostPathSocket:
		return typeMatchedOrError(ftc, ftc.IsSocket, "socket file")
	case v1.HostPathCharDev:
		return typeMatchedOrError(ftc, ftc.IsChar, "character device")
	case v1.HostPathBlockDev:
		return typeMatchedOrError(ftc, ftc.IsBlock, "block device")
	default:
		return fmt.Errorf("%s is an invalid volume type", *pathType)
	}
}

// makeDir 创建新目录。
// 若路径已是目录则不报错；若已是文件则返回错误。
func makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

// makeFile 创建空文件。
// 若路径已存在，无论文件还是目录，都不报错。
func makeFile(pathname string) error {
	f, err := os.OpenFile(pathname, os.O_CREATE, os.FileMode(0644))
	if f != nil {
		f.Close()
	}
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}
