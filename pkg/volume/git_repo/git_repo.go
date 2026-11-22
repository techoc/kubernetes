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

package git_repo

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/exec"
	utilstrings "k8s.io/utils/strings"
)

// ProbeVolumePlugins 是卷插件的主入口，返回本包支持的卷插件列表。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&gitRepoPlugin{nil}}
}

// gitRepoPlugin 实现 gitRepo 卷插件。
type gitRepoPlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &gitRepoPlugin{}

// wrappedVolumeSpec 返回底层 EmptyDir 卷规格；gitRepo 卷在其上克隆代码。
func wrappedVolumeSpec() volume.Spec {
	return volume.Spec{
		Volume: &v1.Volume{VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
	}
}

const (
	gitRepoPluginName = "kubernetes.io/git-repo"
)

// Init 保存卷宿主，供后续路径计算使用。
func (plugin *gitRepoPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *gitRepoPlugin) GetPluginName() string {
	return gitRepoPluginName
}

// GetVolumeName 由仓库、版本和子目录组合出卷的唯一名称。
func (plugin *gitRepoPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _ := getVolumeSource(spec)
	if volumeSource == nil {
		return "", fmt.Errorf("Spec does not reference a Git repo volume type")
	}

	return fmt.Sprintf(
		"%v:%v:%v",
		volumeSource.Repository,
		volumeSource.Revision,
		volumeSource.Directory), nil
}

// CanSupport 判断规格是否为 gitRepo 卷。
func (plugin *gitRepoPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.Volume != nil && spec.Volume.GitRepo != nil
}

// RequiresRemount 返回 false：gitRepo 卷不需要周期性重新挂载。
func (plugin *gitRepoPlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回 false：gitRepo 卷不接受挂载选项。
func (plugin *gitRepoPlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 返回 false：不支持以 SELinux 上下文挂载。
func (plugin *gitRepoPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 校验规格并创建 gitRepo 卷的挂载器。
func (plugin *gitRepoPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	if err := validateVolume(spec.Volume.GitRepo); err != nil {
		return nil, err
	}

	return &gitRepoVolumeMounter{
		gitRepoVolume: &gitRepoVolume{
			volName: spec.Name(),
			podUID:  pod.UID,
			plugin:  plugin,
		},
		pod:      *pod,
		source:   spec.Volume.GitRepo.Repository,
		revision: spec.Volume.GitRepo.Revision,
		target:   spec.Volume.GitRepo.Directory,
		exec:     exec.New(),
	}, nil
}

// NewUnmounter 创建 gitRepo 卷的卸载器。
func (plugin *gitRepoPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &gitRepoVolumeUnmounter{
		&gitRepoVolume{
			volName: volName,
			podUID:  podUID,
			plugin:  plugin,
		},
	}, nil
}

// ConstructVolumeSpec 根据名称重建 gitRepo 卷规格；已从磁盘恢复，无需原始参数。
func (plugin *gitRepoPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	gitVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			GitRepo: &v1.GitRepoVolumeSource{},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(gitVolume),
	}, nil
}

// gitRepoVolume 是从 git 仓库预填充的目录，随 Pod 生命周期结束而消失。
type gitRepoVolume struct {
	volName string
	podUID  types.UID
	plugin  *gitRepoPlugin
	volume.MetricsNil
}

var _ volume.Volume = &gitRepoVolume{}

// GetPath 返回卷在 Pod 目录下的挂载路径。
func (gr *gitRepoVolume) GetPath() string {
	name := gitRepoPluginName
	return gr.plugin.host.GetPodVolumeDir(gr.podUID, utilstrings.EscapeQualifiedName(name), gr.volName)
}

// gitRepoVolumeMounter 负责克隆 git 仓库并构建卷目录。
type gitRepoVolumeMounter struct {
	*gitRepoVolume

	pod      v1.Pod
	source   string
	revision string
	target   string
	exec     exec.Interface
}

var _ volume.Mounter = &gitRepoVolumeMounter{}

// GetAttributes 返回卷属性；该卷可写且由 kubelet 管理。
func (b *gitRepoVolumeMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       false,
		Managed:        true,
		SELinuxRelabel: true, // TODO: double check xattr 变更是否确实安全
	}
}

// SetUp 创建目录并克隆 git 仓库。
func (b *gitRepoVolumeMounter) SetUp(mounterArgs volume.MounterArgs) error {
	if !utilfeature.DefaultFeatureGate.Enabled(features.GitRepoVolumeDriver) {
		return fmt.Errorf("git-repo volume plugin has been disabled; if necessary, it may be re-enabled by enabling the feature-gate `GitRepoVolumeDriver`")
	}

	return b.SetUpAt(b.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录创建卷并克隆仓库；已就绪则直接返回。
func (b *gitRepoVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	if volumeutil.IsReady(b.getMetaDir()) {
		return nil
	}

	// 借助 EmptyDir 完成目录创建与权限设置。
	wrapped, err := b.plugin.host.NewWrapperMounter(b.volName, wrappedVolumeSpec(), &b.pod)
	if err != nil {
		return err
	}
	if err := wrapped.SetUpAt(dir, mounterArgs); err != nil {
		return err
	}

	args := []string{"clone", "--", b.source}

	if len(b.target) != 0 {
		args = append(args, b.target)
	}
	if output, err := b.execCommand("git", args, dir); err != nil {
		return fmt.Errorf("failed to exec 'git %s': %s: %v",
			strings.Join(args, " "), output, err)
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	if len(b.revision) == 0 {
		// 无需检出指定版本，克隆完成即就绪。
		volumeutil.SetReady(b.getMetaDir())
		return nil
	}

	var subdir string

	switch {
	case len(b.target) != 0 && filepath.Clean(b.target) == ".":
		// 目标目录为 '.' 时直接在当前目录操作。
		subdir = filepath.Join(dir)
	case len(files) == 1:
		// 未指定目标目录时，克隆产生的唯一子目录即为仓库根目录。
		subdir = filepath.Join(dir, files[0].Name())
	default:
		// 既非 '.' 又生成了多个条目，无法确定仓库根目录。
		return fmt.Errorf("unexpected directory contents: %v", files)
	}

	if output, err := b.execCommand("git", []string{"checkout", b.revision}, subdir); err != nil {
		return fmt.Errorf("failed to exec 'git checkout %s': %s: %v", b.revision, output, err)
	}
	if output, err := b.execCommand("git", []string{"reset", "--hard"}, subdir); err != nil {
		return fmt.Errorf("failed to exec 'git reset --hard': %s: %v", output, err)
	}

	ownershipChanger := volume.NewVolumeOwnership(b, dir, mounterArgs.FsGroup, nil /*fsGroupChangePolicy*/, volumeutil.FSGroupCompleteHook(b.plugin, nil))
	// 插件已废弃，权限变更失败也无需处理。
	_ = ownershipChanger.ChangePermissions()
	volumeutil.SetReady(b.getMetaDir())
	return nil
}

// getMetaDir 返回用于记录卷就绪状态的元数据目录。
func (b *gitRepoVolumeMounter) getMetaDir() string {
	return filepath.Join(b.plugin.host.GetPodPluginDir(b.podUID, utilstrings.EscapeQualifiedName(gitRepoPluginName)), b.volName)
}

// execCommand 在指定目录中执行命令并返回合并输出。
func (b *gitRepoVolumeMounter) execCommand(command string, args []string, dir string) ([]byte, error) {
	cmd := b.exec.Command(command, args...)
	cmd.SetDir(dir)
	return cmd.CombinedOutput()
}

// validateVolume 校验仓库、版本和目录参数，防止以 '-' 开头被当成 git 选项。
func validateVolume(src *v1.GitRepoVolumeSource) error {
	if err := validateNonFlagArgument(src.Repository, "repository"); err != nil {
		return err
	}
	if err := validateNonFlagArgument(src.Revision, "revision"); err != nil {
		return err
	}
	if err := validateNonFlagArgument(src.Directory, "directory"); err != nil {
		return err
	}
	if (src.Revision != "") && (src.Directory != "") {
		cleanedDir := filepath.Clean(src.Directory)
		if strings.Contains(cleanedDir, "/") || (strings.Contains(cleanedDir, "\\")) {
			return fmt.Errorf("%q is not a valid directory, it must not contain a directory separator", src.Directory)
		}
	}
	return nil
}

// gitRepoVolumeUnmounter 负责清理 gitRepo 卷目录。
type gitRepoVolumeUnmounter struct {
	*gitRepoVolume
}

var _ volume.Unmounter = &gitRepoVolumeUnmounter{}

// TearDown 删除卷目录下的一切内容。
func (c *gitRepoVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 委托 EmptyDir 卸载逻辑删除指定目录内容。
func (c *gitRepoVolumeUnmounter) TearDownAt(dir string) error {
	return volumeutil.UnmountViaEmptyDir(dir, c.plugin.host, c.volName, wrappedVolumeSpec(), c.podUID)
}

// getVolumeSource 从规格中提取 gitRepo 卷源及只读标记。
func getVolumeSource(spec *volume.Spec) (*v1.GitRepoVolumeSource, bool) {
	var readOnly bool
	var volumeSource *v1.GitRepoVolumeSource

	if spec.Volume != nil && spec.Volume.GitRepo != nil {
		volumeSource = spec.Volume.GitRepo
		readOnly = spec.ReadOnly
	}

	return volumeSource, readOnly
}

// validateNonFlagArgument 拒绝以 '-' 开头的值，避免被 git 当作命令行选项。
func validateNonFlagArgument(arg, argName string) error {
	if len(arg) > 0 && arg[0] == '-' {
		return fmt.Errorf("%q is an invalid value for %s", arg, argName)
	}
	return nil
}
