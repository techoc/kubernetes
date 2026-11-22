//go:build linux

/*
Copyright 2016 The Kubernetes Authors.

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

package volume

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"

	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	// rwMask 是可写卷需要的属主/属组读写权限位。
	rwMask = os.FileMode(0660)
	// roMask 是只读卷需要的属主/属组读权限位。
	roMask = os.FileMode(0440)
	// execMask 是目录需要的属主/属组执行（进入）权限位。
	execMask = os.FileMode(0110)
)

var (
	// filePermissionChangeFunc 是 Linux 上真正改权限的函数；
	// 做成变量是为了在测试中替换。
	filePermissionChangeFunc = changeFilePermission
	// progressReportDuration 是递归改权限期间重复上报进度的间隔。
	progressReportDuration = 60 * time.Second
	// firstEventReportDuration 是开始处理后首次上报进度前的等待时间。
	firstEventReportDuration = 30 * time.Second
)

// NewVolumeOwnership 返回可递归修改卷权限与属组的接口实现。
func NewVolumeOwnership(mounter Mounter, dir string, fsGroup *int64, fsGroupChangePolicy *v1.PodFSGroupChangePolicy, completeFunc func(types.CompleteFuncParam)) VolumeOwnershipChanger {
	vo := &VolumeOwnership{
		mounter:             mounter,
		dir:                 dir,
		fsGroup:             fsGroup,
		fsGroupChangePolicy: fsGroupChangePolicy,
		completionCallback:  completeFunc,
	}
	vo.fileCounter.Store(0)
	return vo
}

// AddProgressNotifier 设置进度事件所需的 Pod 与事件记录器。
func (vo *VolumeOwnership) AddProgressNotifier(pod *v1.Pod, recorder record.EventRecorder) VolumeOwnershipChanger {
	vo.pod = pod
	vo.recorder = recorder
	return vo
}

// ChangePermissions 按需递归修改卷内文件的属组与权限。
func (vo *VolumeOwnership) ChangePermissions() error {
	if vo.fsGroup == nil {
		return nil
	}

	if skipPermissionChange(vo.mounter, vo.dir, vo.fsGroup, vo.fsGroupChangePolicy) {
		klog.V(3).InfoS("Skipping permission and ownership change for volume", "path", vo.dir)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 延迟告警：小卷很快就改完，没必要为它制造噪音。
	timer := time.AfterFunc(firstEventReportDuration, func() {
		vo.initiateProgressMonitor(ctx)
	})
	defer timer.Stop()

	return vo.changePermissionsRecursively()
}

// initiateProgressMonitor 发首次告警，并在有事件记录器时启动进度监控。
func (vo *VolumeOwnership) initiateProgressMonitor(ctx context.Context) {
	klog.Warningf("Setting volume ownership for %s and fsGroup set. If the volume has a lot of files then setting volume ownership could be slow, see https://github.com/kubernetes/kubernetes/issues/69699", vo.dir)
	if vo.pod != nil {
		go vo.monitorProgress(ctx)
	}
}

// changePermissionsRecursively 遍历卷内所有文件并逐个改权限，结束时回调完成通知。
func (vo *VolumeOwnership) changePermissionsRecursively() error {
	err := walkDeep(vo.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		vo.fileCounter.Add(1)
		return filePermissionChangeFunc(path, vo.fsGroup, vo.mounter.GetAttributes().ReadOnly, info)
	})

	if vo.completionCallback != nil {
		vo.completionCallback(types.CompleteFuncParam{
			Err: &err,
		})
	}
	return err
}

// monitorProgress 周期性上报处理进度，直到外部调用 cancel 结束。
func (vo *VolumeOwnership) monitorProgress(ctx context.Context) {
	dirName := getDirnameToReport(vo.dir, string(vo.pod.UID))
	msg := fmt.Sprintf("Setting volume ownership for %s is taking longer than expected, consider using OnRootMismatch - https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#configure-volume-permission-and-ownership-change-policy-for-pods", dirName)
	vo.recorder.Event(vo.pod, v1.EventTypeWarning, events.VolumePermissionChangeInProgress, msg)
	ticker := time.NewTicker(progressReportDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			vo.logWarning()
		}
	}
}

// getDirnameToReport 只保留路径中从 podUID 起的部分；找不到 podUID 时返回原路径。
func getDirnameToReport(dir, podUID string) string {
	podUIDIndex := strings.Index(dir, podUID)
	if podUIDIndex == -1 {
		return dir
	}
	return dir[podUIDIndex:]
}

// logWarning 记录当前已处理文件数，并发出进度警告事件。
func (vo *VolumeOwnership) logWarning() {
	dirName := getDirnameToReport(vo.dir, string(vo.pod.UID))
	msg := fmt.Sprintf("Setting volume ownership for %s, processed %d files.", dirName, vo.fileCounter.Load())
	klog.Warning(msg)
	vo.recorder.Event(vo.pod, v1.EventTypeWarning, events.VolumePermissionChangeInProgress, msg)
}

// changeFilePermission 修改单个文件的属组与权限位；符号链接只改链接本身，不改目标。
func changeFilePermission(filename string, fsGroup *int64, readonly bool, info os.FileInfo) error {
	// Lchown 不跟随符号链接，避免改到卷外目标文件的属组。
	err := os.Lchown(filename, -1, int(*fsGroup))
	if err != nil {
		klog.ErrorS(err, "Lchown failed", "path", filename)
	}

	// chmod 会顺着符号链接改到目标文件；符号链接自身的 777 mode 并无实际意义，
	// 真正权限由目标文件决定。若拿这个 mode 去 chmod，会覆盖卷插件初始化好的
	// defaultMode，所以这里对符号链接跳过 chmod。
	if info.Mode()&os.ModeSymlink != 0 {
		return nil
	}

	mask := rwMask
	if readonly {
		mask = roMask
	}

	if info.IsDir() {
		mask |= os.ModeSetgid
		mask |= execMask
	}

	err = os.Chmod(filename, info.Mode()|mask)
	if err != nil {
		klog.ErrorS(err, "chmod failed", "path", filename)
	}

	return nil
}

// skipPermissionChange 判断是否无需递归改权限：未指定 OnRootMismatch 时必须改。
func skipPermissionChange(mounter Mounter, dir string, fsGroup *int64, fsGroupChangePolicy *v1.PodFSGroupChangePolicy) bool {
	if fsGroupChangePolicy == nil || *fsGroupChangePolicy != v1.FSGroupChangeOnRootMismatch {
		klog.V(4).InfoS("Perform recursive ownership change for directory", "path", dir)
		return false
	}
	return !requiresPermissionChange(dir, fsGroup, mounter.GetAttributes().ReadOnly)
}

// requiresPermissionChange 检查根目录的属组与权限是否已满足要求，不满足才需递归修改。
func requiresPermissionChange(rootDir string, fsGroup *int64, readonly bool) bool {
	fsInfo, err := os.Stat(rootDir)
	if err != nil {
		klog.ErrorS(err, "Performing recursive ownership change on rootDir because reading permissions of root volume failed", "path", rootDir)
		return true
	}
	stat, ok := fsInfo.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		klog.ErrorS(nil, "Performing recursive ownership change on rootDir because reading permissions of root volume failed", "path", rootDir)
		return true
	}

	if int(stat.Gid) != int(*fsGroup) {
		klog.V(4).InfoS("Expected group ownership of volume did not match with Gid", "path", rootDir, "GID", stat.Gid)
		return true
	}
	unixPerms := rwMask

	if readonly {
		unixPerms = roMask
	}

	// 根目录不是目录时无法按目录规则判断，直接走递归修改。
	if !fsInfo.IsDir() {
		return true
	}
	unixPerms |= execMask
	filePerm := fsInfo.Mode().Perm()

	// 只需判断目录实际权限是否覆盖 unixPerms：按位与结果等于 unixPerms 即为覆盖。
	// 例如 770&775 = 770（覆盖），770&750 = 750（未覆盖）。
	// 目录还必须设置 setgid，保证新建文件继承同一属组。
	if (unixPerms&filePerm != unixPerms) || (fsInfo.Mode()&os.ModeSetgid == 0) {
		klog.V(4).InfoS("Performing recursive ownership change on rootDir because of mismatching mode", "path", rootDir)
		return true
	}
	return false
}

// readDirNames 读取目录中的条目名，不排序；
// 改权限不依赖顺序，跳过排序可以省下大卷上的开销。
func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	return names, nil
}

// walkDeep 遍历目录树，与 filepath.Walk 有两点差异：
//   - 不排序，避免大卷上不必要的开销
//   - 先处理子目录和文件，最后才对根目录调用 walkFunc
func walkDeep(root string, walkFunc filepath.WalkFunc) error {
	info, err := os.Lstat(root)
	if err != nil {
		return walkFunc(root, nil, err)
	}
	return walk(root, info, walkFunc)
}

func walk(path string, info os.FileInfo, walkFunc filepath.WalkFunc) error {
	if !info.IsDir() {
		return walkFunc(path, info, nil)
	}
	names, err := readDirNames(path)
	if err != nil {
		return err
	}
	for _, name := range names {
		filename := filepath.Join(path, name)
		fileInfo, err := os.Lstat(filename)
		if err != nil {
			if err := walkFunc(filename, fileInfo, err); err != nil {
				return err
			}
		} else {
			err = walk(filename, fileInfo, walkFunc)
			if err != nil {
				return err
			}
		}
	}
	return walkFunc(path, info, nil)
}
