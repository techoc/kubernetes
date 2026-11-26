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

package utils

/*
 * 各种本地测试资源实现。
 */

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/onsi/ginkgo/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
)

// LocalVolumeType 表示本地 volume 的类型，例如 tmpfs、directory、
// block 等。
type LocalVolumeType string

const (
	// LocalVolumeDirectory 表示用作本地 volume 的简单目录
	LocalVolumeDirectory LocalVolumeType = "dir"
	// LocalVolumeDirectoryLink 类似 LocalVolumeDirectory，但指向目录的符号链接
	LocalVolumeDirectoryLink LocalVolumeType = "dir-link"
	// LocalVolumeDirectoryBindMounted 类似 LocalVolumeDirectory，但是 bind mount
	LocalVolumeDirectoryBindMounted LocalVolumeType = "dir-bindmounted"
	// LocalVolumeDirectoryLinkBindMounted 类似 LocalVolumeDirectory，但指向自 bind mount 目录的符号链接
	// 注意在符号链接上 bind mount 实际上挂载到它
	// 指向的目录
	LocalVolumeDirectoryLinkBindMounted LocalVolumeType = "dir-link-bindmounted"
	// LocalVolumeTmpfs 表示用作本地 volume 的临时文件系统
	LocalVolumeTmpfs LocalVolumeType = "tmpfs"
	// LocalVolumeBlock 表示块设备，创建本地文件并映射为块设备
	LocalVolumeBlock LocalVolumeType = "block"
	// LocalVolumeBlockFS 表示由块设备支持的文件系统
	LocalVolumeBlockFS LocalVolumeType = "blockfs"
	// LocalVolumeGCELocalSSD 表示由 GCE Local SSD 支持的文件系统，用作本地 volume
	LocalVolumeGCELocalSSD LocalVolumeType = "gce-localssd-scsi-fs"
)

// LocalTestResource 表示本地 volume 的测试资源。
type LocalTestResource struct {
	VolumeType LocalVolumeType
	Node       *v1.Node
	// volume 路径、节点上文件系统或块设备的路径
	Path string
	// 如果 volume 由 loop 设备支持，我们在此目录下
	// 创建 loop 设备存储文件。
	loopDir string
}

// LocalTestResourceManager 表示在节点上创建/销毁本地测试资源的接口
type LocalTestResourceManager interface {
	Create(ctx context.Context, node *v1.Node, volumeType LocalVolumeType, parameters map[string]string) *LocalTestResource
	ExpandBlockDevice(ctx context.Context, ltr *LocalTestResource, mbToAdd int) error
	Remove(ctx context.Context, ltr *LocalTestResource)
}

// ltrMgr 实现 LocalTestResourceManager
type ltrMgr struct {
	prefix   string
	hostExec HostExec
	// hostBase 表示宿主机上一个可写目录，我们在其中
	// 创建测试目录
	hostBase string
}

// NewLocalResourceManager 返回 LocalTestResourceManager 的实例
func NewLocalResourceManager(prefix string, hostExec HostExec, hostBase string) LocalTestResourceManager {
	return &ltrMgr{
		prefix:   prefix,
		hostExec: hostExec,
		hostBase: hostBase,
	}
}

// getTestDir 返回 `hostBase` 目录下带随机名称的测试目录。
func (l *ltrMgr) getTestDir() string {
	testDirName := fmt.Sprintf("%s-%s", l.prefix, string(uuid.NewUUID()))
	return filepath.Join(l.hostBase, testDirName)
}

func (l *ltrMgr) setupLocalVolumeTmpfs(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	hostDir := l.getTestDir()
	ginkgo.By(fmt.Sprintf("Creating tmpfs mount point on node %q at path %q", node.Name, hostDir))
	err := l.hostExec.IssueCommand(ctx, fmt.Sprintf("mkdir -p %q && mount -t tmpfs -o size=10m tmpfs-%q %q", hostDir, hostDir, hostDir), node)
	framework.ExpectNoError(err)
	return &LocalTestResource{
		Node: node,
		Path: hostDir,
	}
}

func (l *ltrMgr) cleanupLocalVolumeTmpfs(ctx context.Context, ltr *LocalTestResource) {
	ginkgo.By(fmt.Sprintf("Unmount tmpfs mount point on node %q at path %q", ltr.Node.Name, ltr.Path))
	err := l.hostExec.IssueCommand(ctx, fmt.Sprintf("umount %q", ltr.Path), ltr.Node)
	framework.ExpectNoError(err)

	ginkgo.By("Removing the test directory")
	err = l.hostExec.IssueCommand(ctx, fmt.Sprintf("rm -r %s", ltr.Path), ltr.Node)
	framework.ExpectNoError(err)
}

// createAndSetupLoopDevice 创建一个空文件并关联一个 loop 设备。
func (l *ltrMgr) createAndSetupLoopDevice(ctx context.Context, dir string, node *v1.Node, size int) {
	ginkgo.By(fmt.Sprintf("Creating block device on node %q using path %q", node.Name, dir))
	mkdirCmd := fmt.Sprintf("mkdir -p %s", dir)
	count := size / 4096
	// xfs 至少需要 4096 个块
	if count < 4096 {
		count = 4096
	}
	ddCmd := fmt.Sprintf("dd if=/dev/zero of=%s/file bs=4096 count=%d", dir, count)
	losetupCmd := fmt.Sprintf("losetup -f %s/file", dir)
	err := l.hostExec.IssueCommand(ctx, fmt.Sprintf("%s && %s && %s", mkdirCmd, ddCmd, losetupCmd), node)
	framework.ExpectNoError(err)
}

// findLoopDevice 通过关联的存储目录查找 loop 设备路径。
func (l *ltrMgr) findLoopDevice(ctx context.Context, dir string, node *v1.Node) string {
	cmd := fmt.Sprintf("E2E_LOOP_DEV=$(losetup | grep %s/file | awk '{ print $1 }') 2>&1 > /dev/null && echo ${E2E_LOOP_DEV}", dir)
	loopDevResult, err := l.hostExec.IssueCommandWithResult(ctx, cmd, node)
	framework.ExpectNoError(err)
	return strings.TrimSpace(loopDevResult)
}

func (l *ltrMgr) setupLocalVolumeBlock(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	loopDir := l.getTestDir()
	l.createAndSetupLoopDevice(ctx, loopDir, node, 20*1024*1024)
	loopDev := l.findLoopDevice(ctx, loopDir, node)
	return &LocalTestResource{
		Node:    node,
		Path:    loopDev,
		loopDir: loopDir,
	}
}

// teardownLoopDevice 通过关联的存储目录拆掉 loop 设备。
func (l *ltrMgr) teardownLoopDevice(ctx context.Context, dir string, node *v1.Node) {
	loopDev := l.findLoopDevice(ctx, dir, node)
	ginkgo.By(fmt.Sprintf("Tear down block device %q on node %q at path %s/file", loopDev, node.Name, dir))
	losetupDeleteCmd := fmt.Sprintf("losetup -d %s", loopDev)
	err := l.hostExec.IssueCommand(ctx, losetupDeleteCmd, node)
	framework.ExpectNoError(err)
	return
}

func (l *ltrMgr) cleanupLocalVolumeBlock(ctx context.Context, ltr *LocalTestResource) {
	l.teardownLoopDevice(ctx, ltr.loopDir, ltr.Node)
	ginkgo.By(fmt.Sprintf("Removing the test directory %s", ltr.loopDir))
	removeCmd := fmt.Sprintf("rm -r %s", ltr.loopDir)
	err := l.hostExec.IssueCommand(ctx, removeCmd, ltr.Node)
	framework.ExpectNoError(err)
}

func (l *ltrMgr) setupLocalVolumeBlockFS(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	ltr := l.setupLocalVolumeBlock(ctx, node, parameters)
	loopDev := ltr.Path
	loopDir := ltr.loopDir
	// 在 loopDir 格式化并挂载，给其他人 rwx 以便读写测试
	cmd := fmt.Sprintf("mkfs -t ext4 %s && mount -t ext4 %s %s && chmod o+rwx %s", loopDev, loopDev, loopDir, loopDir)
	err := l.hostExec.IssueCommand(ctx, cmd, node)
	framework.ExpectNoError(err)
	return &LocalTestResource{
		Node:    node,
		Path:    loopDir,
		loopDir: loopDir,
	}
}

func (l *ltrMgr) cleanupLocalVolumeBlockFS(ctx context.Context, ltr *LocalTestResource) {
	umountCmd := fmt.Sprintf("umount %s", ltr.Path)
	err := l.hostExec.IssueCommand(ctx, umountCmd, ltr.Node)
	framework.ExpectNoError(err)
	l.cleanupLocalVolumeBlock(ctx, ltr)
}

func (l *ltrMgr) setupLocalVolumeDirectory(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	hostDir := l.getTestDir()
	mkdirCmd := fmt.Sprintf("mkdir -p %s", hostDir)
	err := l.hostExec.IssueCommand(ctx, mkdirCmd, node)
	framework.ExpectNoError(err)
	return &LocalTestResource{
		Node: node,
		Path: hostDir,
	}
}

func (l *ltrMgr) cleanupLocalVolumeDirectory(ctx context.Context, ltr *LocalTestResource) {
	ginkgo.By("Removing the test directory")
	removeCmd := fmt.Sprintf("rm -r %s", ltr.Path)
	err := l.hostExec.IssueCommand(ctx, removeCmd, ltr.Node)
	framework.ExpectNoError(err)
}

func (l *ltrMgr) setupLocalVolumeDirectoryLink(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	hostDir := l.getTestDir()
	hostDirBackend := hostDir + "-backend"
	cmd := fmt.Sprintf("mkdir -p %s && ln -s %s %s", hostDirBackend, hostDirBackend, hostDir)
	err := l.hostExec.IssueCommand(ctx, cmd, node)
	framework.ExpectNoError(err)
	return &LocalTestResource{
		Node: node,
		Path: hostDir,
	}
}

func (l *ltrMgr) cleanupLocalVolumeDirectoryLink(ctx context.Context, ltr *LocalTestResource) {
	ginkgo.By("Removing the test directory")
	hostDir := ltr.Path
	hostDirBackend := hostDir + "-backend"
	removeCmd := fmt.Sprintf("rm -r %s && rm -r %s", hostDir, hostDirBackend)
	err := l.hostExec.IssueCommand(ctx, removeCmd, ltr.Node)
	framework.ExpectNoError(err)
}

func (l *ltrMgr) setupLocalVolumeDirectoryBindMounted(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	hostDir := l.getTestDir()
	cmd := fmt.Sprintf("mkdir -p %s && mount --bind %s %s", hostDir, hostDir, hostDir)
	err := l.hostExec.IssueCommand(ctx, cmd, node)
	framework.ExpectNoError(err)
	return &LocalTestResource{
		Node: node,
		Path: hostDir,
	}
}

func (l *ltrMgr) cleanupLocalVolumeDirectoryBindMounted(ctx context.Context, ltr *LocalTestResource) {
	ginkgo.By("Removing the test directory")
	hostDir := ltr.Path
	removeCmd := fmt.Sprintf("umount %s && rm -r %s", hostDir, hostDir)
	err := l.hostExec.IssueCommand(ctx, removeCmd, ltr.Node)
	framework.ExpectNoError(err)
}

func (l *ltrMgr) setupLocalVolumeDirectoryLinkBindMounted(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	hostDir := l.getTestDir()
	hostDirBackend := hostDir + "-backend"
	cmd := fmt.Sprintf("mkdir -p %s && mount --bind %s %s && ln -s %s %s", hostDirBackend, hostDirBackend, hostDirBackend, hostDirBackend, hostDir)
	err := l.hostExec.IssueCommand(ctx, cmd, node)
	framework.ExpectNoError(err)
	return &LocalTestResource{
		Node: node,
		Path: hostDir,
	}
}

func (l *ltrMgr) cleanupLocalVolumeDirectoryLinkBindMounted(ctx context.Context, ltr *LocalTestResource) {
	ginkgo.By("Removing the test directory")
	hostDir := ltr.Path
	hostDirBackend := hostDir + "-backend"
	removeCmd := fmt.Sprintf("rm %s && umount %s && rm -r %s", hostDir, hostDirBackend, hostDirBackend)
	err := l.hostExec.IssueCommand(ctx, removeCmd, ltr.Node)
	framework.ExpectNoError(err)
}

func (l *ltrMgr) setupLocalVolumeGCELocalSSD(ctx context.Context, node *v1.Node, parameters map[string]string) *LocalTestResource {
	res, err := l.hostExec.IssueCommandWithResult(ctx, "ls /mnt/disks/by-uuid/google-local-ssds-scsi-fs/", node)
	framework.ExpectNoError(err)
	dirName := strings.Fields(res)[0]
	hostDir := "/mnt/disks/by-uuid/google-local-ssds-scsi-fs/" + dirName
	return &LocalTestResource{
		Node: node,
		Path: hostDir,
	}
}

func (l *ltrMgr) cleanupLocalVolumeGCELocalSSD(ctx context.Context, ltr *LocalTestResource) {
	// 这个文件系统在集群初始化时挂载，我们清理所有文件以便复用。
	removeCmd := fmt.Sprintf("find '%s' -mindepth 1 -maxdepth 1 -print0 | xargs -r -0 rm -rf", ltr.Path)
	err := l.hostExec.IssueCommand(ctx, removeCmd, ltr.Node)
	framework.ExpectNoError(err)
}

func (l *ltrMgr) expandLocalVolumeBlockFS(ctx context.Context, ltr *LocalTestResource, mbToAdd int) error {
	ddCmd := fmt.Sprintf("dd if=/dev/zero of=%s/file conv=notrunc oflag=append bs=1M count=%d", ltr.loopDir, mbToAdd)
	loopDev := l.findLoopDevice(ctx, ltr.loopDir, ltr.Node)
	losetupCmd := fmt.Sprintf("losetup -c %s", loopDev)
	return l.hostExec.IssueCommand(ctx, fmt.Sprintf("%s && %s", ddCmd, losetupCmd), ltr.Node)
}

func (l *ltrMgr) ExpandBlockDevice(ctx context.Context, ltr *LocalTestResource, mbtoAdd int) error {
	switch ltr.VolumeType {
	case LocalVolumeBlockFS:
		return l.expandLocalVolumeBlockFS(ctx, ltr, mbtoAdd)
	}
	return fmt.Errorf("Failed to expand local test resource, unsupported volume type: %s", ltr.VolumeType)
}

func (l *ltrMgr) Create(ctx context.Context, node *v1.Node, volumeType LocalVolumeType, parameters map[string]string) *LocalTestResource {
	var ltr *LocalTestResource
	switch volumeType {
	case LocalVolumeDirectory:
		ltr = l.setupLocalVolumeDirectory(ctx, node, parameters)
	case LocalVolumeDirectoryLink:
		ltr = l.setupLocalVolumeDirectoryLink(ctx, node, parameters)
	case LocalVolumeDirectoryBindMounted:
		ltr = l.setupLocalVolumeDirectoryBindMounted(ctx, node, parameters)
	case LocalVolumeDirectoryLinkBindMounted:
		ltr = l.setupLocalVolumeDirectoryLinkBindMounted(ctx, node, parameters)
	case LocalVolumeTmpfs:
		ltr = l.setupLocalVolumeTmpfs(ctx, node, parameters)
	case LocalVolumeBlock:
		ltr = l.setupLocalVolumeBlock(ctx, node, parameters)
	case LocalVolumeBlockFS:
		ltr = l.setupLocalVolumeBlockFS(ctx, node, parameters)
	case LocalVolumeGCELocalSSD:
		ltr = l.setupLocalVolumeGCELocalSSD(ctx, node, parameters)
	default:
		framework.Failf("Failed to create local test resource on node %q, unsupported volume type: %v is specified", node.Name, volumeType)
		return nil
	}
	if ltr == nil {
		framework.Failf("Failed to create local test resource on node %q, volume type: %v, parameters: %v", node.Name, volumeType, parameters)
	}
	ltr.VolumeType = volumeType
	return ltr
}

func (l *ltrMgr) Remove(ctx context.Context, ltr *LocalTestResource) {
	switch ltr.VolumeType {
	case LocalVolumeDirectory:
		l.cleanupLocalVolumeDirectory(ctx, ltr)
	case LocalVolumeDirectoryLink:
		l.cleanupLocalVolumeDirectoryLink(ctx, ltr)
	case LocalVolumeDirectoryBindMounted:
		l.cleanupLocalVolumeDirectoryBindMounted(ctx, ltr)
	case LocalVolumeDirectoryLinkBindMounted:
		l.cleanupLocalVolumeDirectoryLinkBindMounted(ctx, ltr)
	case LocalVolumeTmpfs:
		l.cleanupLocalVolumeTmpfs(ctx, ltr)
	case LocalVolumeBlock:
		l.cleanupLocalVolumeBlock(ctx, ltr)
	case LocalVolumeBlockFS:
		l.cleanupLocalVolumeBlockFS(ctx, ltr)
	case LocalVolumeGCELocalSSD:
		l.cleanupLocalVolumeGCELocalSSD(ctx, ltr)
	default:
		framework.Failf("Failed to remove local test resource, unsupported volume type: %v is specified", ltr.VolumeType)
	}
	return
}
