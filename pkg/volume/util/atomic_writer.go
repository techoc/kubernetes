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

package util

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"

	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	maxFileNameLength = 255
	maxPathLength     = 4096
)

// AtomicWriter 负责将一组文件的内容原子地投影到目标目录。
//
// 注意：
//
//  1. AtomicWriter 保留所有以 `..` 开头的路径名。
//  2. AtomicWriter 不提供并发保证，需由调用方自行同步。
//
// 卷中可见的文件是指向 writer 数据目录内文件的符号链接。真实文件存放在隐藏的时间戳目录中，
// 数据目录再以符号链接指向它。时间戳目录和数据目录的符号链接都创建在 writer 的目标目录下。
// 这样只需改变数据目录符号链接的指向，即可原子地更新所有文件。
//
// 目标目录的使用方可以通过 inotify 或 fanotify 监听 ..data 符号链接，从而在卷内容更新时收到事件。
type AtomicWriter struct {
	targetDir  string
	logContext string
}

// FileProjection 包含文件的数据、权限模式及文件系统用户。
type FileProjection struct {
	Data   []byte
	Mode   int32
	FsUser *int64
}

// NewAtomicWriter 创建一个写入指定目标目录的 AtomicWriter；目标目录不存在时返回错误。
func NewAtomicWriter(targetDir string, logContext string) (*AtomicWriter, error) {
	_, err := os.Stat(targetDir)
	if os.IsNotExist(err) {
		return nil, err
	}

	return &AtomicWriter{targetDir: targetDir, logContext: logContext}, nil
}

const (
	dataDirName    = "..data"
	newDataDirName = "..data_tmp"
)

// Write 将给定的 payload 原子地投影到 writer 的目标目录。输入路径不得以 '..' 开头。
// setPerms 是可选的回调，供调用方在文件发布前设置新创建文件的权限；参数为目标目录下新建的时间戳目录名。
//
// Write 算法如下：
//
//  1. 校验 payload；若无效则返回错误。
//
//  2. 读取数据目录符号链接，找到当前的时间戳目录。
//
//  3. 遍历旧版本卷，判断 payload 中是否有部分内容被删除但仍残留在磁盘上。
//
//  4. 比较当前时间戳目录与目标数据，判断是否需要更新数据目录。
//
//  5. 如需更新，则新建一个时间戳目录。
//
//  6. 将 payload 写入新的时间戳目录。
//
//  7. 若 setPerms 非空，对新时间戳目录及其中的文件设置权限。
//
//  8. 创建指向新时间戳目录的符号链接 ..data_tmp，它将成为新的数据目录。
//
//  9. 将新数据目录符号链接重命名替换旧的数据目录；重命名是原子操作。
//
//  10. 按需为新可见文件创建符号链接与目录。
//
//     例如有如下文件：
//     <target-dir>/podName
//     <target-dir>/user/labels
//     <target-dir>/k8s/annotations
//
//     用户可见文件是指向内部数据目录的符号链接：
//     <target-dir>/podName         -> ..data/podName
//     <target-dir>/usr -> ..data/usr
//     <target-dir>/k8s -> ..data/k8s
//
//     数据目录本身则链接到存放真实数据的时间戳目录：
//     <target-dir>/..data          -> ..2016_02_01_15_04_05.12345678/
//     NOTE(claudiub): 这些符号链接必须在其余创建与链接工作完成后才创建。在 Windows 上，
//     若目标当时不存在，后续变成目录时已创建的符号链接将无法正常工作。
//
//  11. 从目标目录的可见部分删除旧路径。
//
//  12. 若存在旧的时间戳目录，则将其删除。
func (w *AtomicWriter) Write(payload map[string]FileProjection, setPerms func(subPath string) error) error {
	// (1)
	cleanPayload, err := validatePayload(payload)
	if err != nil {
		klog.Errorf("%s: invalid payload: %v", w.logContext, err)
		return err
	}

	// (2)
	dataDirPath := filepath.Join(w.targetDir, dataDirName)
	oldTsDir, err := os.Readlink(dataDirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.Errorf("%s: error reading link for data directory: %v", w.logContext, err)
			return err
		}
		// 出错时 Readlink() 返回 ""，但文档未保证该行为，不应依赖；空 oldTsDir 即表示原目录不存在。
		oldTsDir = ""
	}
	oldTsPath := filepath.Join(w.targetDir, oldTsDir)

	var pathsToRemove sets.Set[string]
	shouldWrite := true
	// 没有旧版本时无需删除任何内容。
	if len(oldTsDir) != 0 {
		// (3)
		pathsToRemove, err = w.pathsToRemove(cleanPayload, oldTsPath)
		if err != nil {
			klog.Errorf("%s: error determining user-visible files to remove: %v", w.logContext, err)
			return err
		}

		// (4)
		if should, err := shouldWritePayload(cleanPayload, oldTsPath); err != nil {
			klog.Errorf("%s: error determining whether payload should be written to disk: %v", w.logContext, err)
			return err
		} else if !should && len(pathsToRemove) == 0 {
			klog.V(4).Infof("%s: write not required for data directory %v", w.logContext, oldTsDir)
			// 数据目录已是最新，但仍需确保用户可见的符号链接已创建。
			// 参见 https://github.com/kubernetes/kubernetes/issues/121472 了解更多细节。
			// 将 oldTsDir 置空，避免误删数据目录。
			shouldWrite = false
			oldTsDir = ""
		} else {
			klog.V(4).Infof("%s: write required for target directory %v", w.logContext, w.targetDir)
		}
	}

	if shouldWrite {
		// (5)
		tsDir, err := w.newTimestampDir()
		if err != nil {
			klog.Errorf("%s: error creating new ts data directory: %v", w.logContext, err)
			return err
		}
		tsDirName := filepath.Base(tsDir)

		// (6)
		if err = w.writePayloadToDir(cleanPayload, tsDir); err != nil {
			klog.Errorf("%s: error writing payload to ts data directory %s: %v", w.logContext, tsDir, err)
			return err
		}
		klog.V(4).Infof("%s: performed write of new data to ts data directory: %s", w.logContext, tsDir)

		// (7)
		if setPerms != nil {
			if err := setPerms(tsDirName); err != nil {
				klog.Errorf("%s: error applying ownership settings: %v", w.logContext, err)
				return err
			}
		}

		// (8)
		newDataDirPath := filepath.Join(w.targetDir, newDataDirName)
		if err = os.Symlink(tsDirName, newDataDirPath); err != nil {
			if err := os.RemoveAll(tsDir); err != nil {
				klog.Errorf("%s: error removing new ts directory %s: %v", w.logContext, tsDir, err)
			}
			klog.Errorf("%s: error creating symbolic link for atomic update: %v", w.logContext, err)
			return err
		}

		// (9)
		if runtime.GOOS == "windows" {
			if err := os.Remove(dataDirPath); err != nil {
				klog.Errorf("%s: error removing data dir directory %s: %v", w.logContext, dataDirPath, err)
			}
			err = os.Symlink(tsDirName, dataDirPath)
			if err := os.Remove(newDataDirPath); err != nil {
				klog.Errorf("%s: error removing new data dir directory %s: %v", w.logContext, newDataDirPath, err)
			}
		} else {
			err = os.Rename(newDataDirPath, dataDirPath)
		}
		if err != nil {
			if err := os.Remove(newDataDirPath); err != nil && err != os.ErrNotExist {
				klog.Errorf("%s: error removing new data dir directory %s: %v", w.logContext, newDataDirPath, err)
			}
			if err := os.RemoveAll(tsDir); err != nil {
				klog.Errorf("%s: error removing new ts directory %s: %v", w.logContext, tsDir, err)
			}
			klog.Errorf("%s: error renaming symbolic link for data directory %s: %v", w.logContext, newDataDirPath, err)
			return err
		}
	}

	// (10)
	if err = w.createUserVisibleFiles(cleanPayload); err != nil {
		klog.Errorf("%s: error creating visible symlinks in %s: %v", w.logContext, w.targetDir, err)
		return err
	}

	// (11)
	if err = w.removeUserVisiblePaths(pathsToRemove); err != nil {
		klog.Errorf("%s: error removing old visible symlinks: %v", w.logContext, err)
		return err
	}

	// (12)
	if len(oldTsDir) > 0 {
		if err = os.RemoveAll(oldTsPath); err != nil {
			klog.Errorf("%s: error removing old data directory %s: %v", w.logContext, oldTsDir, err)
			return err
		}
	}

	return nil
}

// validatePayload 校验 payload 中的路径，并返回一份路径已清理的副本；路径非法时返回错误。
func validatePayload(payload map[string]FileProjection) (map[string]FileProjection, error) {
	cleanPayload := make(map[string]FileProjection)
	for k, content := range payload {
		if err := validatePath(k); err != nil {
			return nil, err
		}

		cleanPayload[filepath.Clean(k)] = content
	}

	return cleanPayload, nil
}

// validatePath 校验单个路径，非法时返回错误。路径不得：
//
// 1. 为绝对路径
// 2. 包含 '..' 路径段
// 3. 以 '..' 开头
// 4. 文件名超过 255 字符
// 5. 总长度超过 4096 字符
func validatePath(targetPath string) error {
	// TODO: 设法与类似的 api 校验 validateVolumeSourcePath 统一；两者错误语义差异较大，
	// 短时间内难以找到合适的重构方式复用。
	if targetPath == "" {
		return fmt.Errorf("invalid path: must not be empty: %q", targetPath)
	}
	if path.IsAbs(targetPath) {
		return fmt.Errorf("invalid path: must be relative path: %s", targetPath)
	}

	if len(targetPath) > maxPathLength {
		return fmt.Errorf("invalid path: must be less than or equal to %d characters", maxPathLength)
	}

	items := strings.Split(targetPath, string(os.PathSeparator))
	for _, item := range items {
		if item == ".." {
			return fmt.Errorf("invalid path: must not contain '..': %s", targetPath)
		}
		if len(item) > maxFileNameLength {
			return fmt.Errorf("invalid path: filenames must be less than or equal to %d characters", maxFileNameLength)
		}
	}
	if strings.HasPrefix(items[0], "..") && len(items[0]) > 2 {
		return fmt.Errorf("invalid path: must not start with '..': %s", targetPath)
	}

	return nil
}

// shouldWritePayload 判断 payload 是否需要写入磁盘。
func shouldWritePayload(payload map[string]FileProjection, oldTsDir string) (bool, error) {
	for userVisiblePath, fileProjection := range payload {
		shouldWrite, err := shouldWriteFile(filepath.Join(oldTsDir, userVisiblePath), fileProjection.Data)
		if err != nil {
			return false, err
		}

		if shouldWrite {
			return true, nil
		}
	}

	return false, nil
}

// shouldWriteFile 判断某个文件的新版本是否需要写入磁盘。
func shouldWriteFile(path string, content []byte) (bool, error) {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return true, nil
	}

	contentOnFs, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}

	return !bytes.Equal(content, contentOnFs), nil
}

// pathsToRemove 遍历当前版本的数据目录，确定 payload 写入目标目录后应删除哪些路径。
func (w *AtomicWriter) pathsToRemove(payload map[string]FileProjection, oldTSDir string) (sets.Set[string], error) {
	paths := sets.New[string]()
	visitor := func(path string, info os.FileInfo, err error) error {
		relativePath := strings.TrimPrefix(path, oldTSDir)
		relativePath = strings.TrimPrefix(relativePath, string(os.PathSeparator))
		if relativePath == "" {
			return nil
		}

		paths.Insert(relativePath)
		return nil
	}

	err := filepath.Walk(oldTSDir, visitor)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	klog.V(5).Infof("%s: current paths:   %+v", w.targetDir, sets.List(paths))

	newPaths := sets.New[string]()
	for file := range payload {
		// 将 payload 中所有子路径加入新路径集合，避免误删非空目录。
		for subPath := file; subPath != ""; {
			newPaths.Insert(subPath)
			subPath, _ = filepath.Split(subPath)
			subPath = strings.TrimSuffix(subPath, string(os.PathSeparator))
		}
	}
	klog.V(5).Infof("%s: new paths:       %+v", w.targetDir, sets.List(newPaths))

	result := paths.Difference(newPaths)
	klog.V(5).Infof("%s: paths to remove: %+v", w.targetDir, result)

	return result, nil
}

// newTimestampDir 创建一个新的时间戳目录。
func (w *AtomicWriter) newTimestampDir() (string, error) {
	tsDir, err := os.MkdirTemp(w.targetDir, time.Now().UTC().Format("..2006_01_02_15_04_05."))
	if err != nil {
		klog.Errorf("%s: unable to create new temp directory: %v", w.logContext, err)
		return "", err
	}

	// 需要 0755 权限，以便 group 和其他用户能遍历目录树；显式 chmod 可消除进程 umask 的影响。
	err = os.Chmod(tsDir, 0755)
	if err != nil {
		klog.Errorf("%s: unable to set mode on new temp directory: %v", w.logContext, err)
		return "", err
	}

	return tsDir, nil
}

// writePayloadToDir 将 payload 写入指定目录，该目录必须已存在。
func (w *AtomicWriter) writePayloadToDir(payload map[string]FileProjection, dir string) error {
	for userVisiblePath, fileProjection := range payload {
		content := fileProjection.Data
		mode := os.FileMode(fileProjection.Mode)
		fullPath := filepath.Join(dir, userVisiblePath)
		baseDir, _ := filepath.Split(fullPath)

		if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
			klog.Errorf("%s: unable to create directory %s: %v", w.logContext, baseDir, err)
			return err
		}

		if err := os.WriteFile(fullPath, content, mode); err != nil {
			klog.Errorf("%s: unable to write file %s with mode %v: %v", w.logContext, fullPath, mode, err)
			return err
		}
		// os.WriteFile 底层使用 open(2) 创建文件，实际权限会是 "mode & ~umask"；
		// 这里显式 chmod，确保无论 umask 如何都使用指定权限。
		if err := os.Chmod(fullPath, mode); err != nil {
			klog.Errorf("%s: unable to change file %s with mode %v: %v", w.logContext, fullPath, mode, err)
			return err
		}

		if fileProjection.FsUser == nil {
			continue
		}

		if err := w.lchown(fullPath, int(*fileProjection.FsUser), -1); err != nil {
			klog.Errorf("%s: unable to change file %s with owner %v: %v", w.logContext, fullPath, int(*fileProjection.FsUser), err)
			return err
		}
	}

	return nil
}

// createUserVisibleFiles 为 payload 中所有文件创建相对符号链接；文件路径中的目录不存在时会创建。
//
// 示例：对文件 "bar"、"foo/bar"、"baz/bar"、"foo/baz/blah"，创建如下符号链接：
// bar -> ..data/bar
// foo -> ..data/foo
// baz -> ..data/baz
func (w *AtomicWriter) createUserVisibleFiles(payload map[string]FileProjection) error {
	for userVisiblePath, fileProjection := range payload {
		slashpos := strings.Index(userVisiblePath, string(os.PathSeparator))
		if slashpos == -1 {
			slashpos = len(userVisiblePath)
		}
		linkname := userVisiblePath[:slashpos]
		_, err := os.Readlink(filepath.Join(w.targetDir, linkname))
		if err != nil && os.IsNotExist(err) {
			// 该路径指向数据目录的链接尚不存在，创建它。
			visibleFile := filepath.Join(w.targetDir, linkname)
			dataDirFile := filepath.Join(dataDirName, linkname)

			err = os.Symlink(dataDirFile, visibleFile)
			if err != nil {
				return err
			}

			if fileProjection.FsUser == nil {
				continue
			}

			if err := w.lchown(visibleFile, int(*fileProjection.FsUser), -1); err != nil {
				klog.Errorf("%s: unable to change file %s with owner %v: %v", w.logContext, visibleFile, int(*fileProjection.FsUser), err)
				return err
			}
		}
	}
	return nil
}

// removeUserVisiblePaths 从 writer 目标目录的可见部分删除给定的路径集合。
func (w *AtomicWriter) removeUserVisiblePaths(paths sets.Set[string]) error {
	ps := string(os.PathSeparator)
	var lasterr error
	for p := range paths {
		// 只删除卷根目录下的符号链接（即路径中不含 '/' 的项）。
		if strings.Contains(p, ps) {
			continue
		}
		if err := os.Remove(filepath.Join(w.targetDir, p)); err != nil {
			klog.Errorf("%s: error pruning old user-visible path %s: %v", w.logContext, p, err)
			lasterr = err
		}
	}

	return lasterr
}

// ResolvesFsUser 按 KEP-5936 描述的回退机制确定文件所有者 UID；特性门控关闭时返回 nil。
func ResolvesFsUser(defaultUser, itemUser *int64) (fsUser *int64) {
	if utilfeature.DefaultFeatureGate.Enabled(features.AtomicWriteVolumeUserFields) {
		if itemUser != nil {
			fsUser = itemUser
		} else {
			fsUser = defaultUser
		}
	}
	return
}
