/*
Copyright 2025 The Kubernetes Authors.

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

// Package logrotation 实现按大小轮转、按时间清理的日志 writer，
// 供 kube-log-runner 重定向容器日志时使用。
package logrotation

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// timeLayout 是轮转后文件名中的时间戳格式，便于人工识别与清理时解析。
const timeLayout string = "20060102-150405"

type rotationFile struct {
	// maxSize 是日志文件的最大字节数；0 表示不轮转。
	maxSize int64
	// maxAge 是日志文件的最大保留时长；0 表示不清理。
	maxAge        time.Duration
	filePath      string
	mut           sync.Mutex
	file          *os.File
	currentSize   int64
	lasSyncTime   time.Time
	flushInterval time.Duration
}

// Open 以追加方式打开（必要时创建）日志文件，并返回可轮转/清理的 writer。
// maxSize 为 0 关闭轮转；maxAge 为 0 关闭清理；flushInterval 为 0 表示不主动 fsync。
func Open(filePath string, flushInterval time.Duration, maxSize int64, maxAge time.Duration) (io.WriteCloser, error) {
	w := &rotationFile{
		filePath:      filePath,
		maxSize:       maxSize,
		maxAge:        maxAge,
		flushInterval: flushInterval,
	}

	logFile, err := os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	w.file = logFile

	w.lasSyncTime = time.Now()

	// 以已有文件的当前大小作为起点，否则续写时轮转判断会偏小。
	if w.maxSize > 0 {
		info, err := os.Stat(w.filePath)
		if err != nil {
			return nil, err
		}
		w.currentSize = info.Size()
	}

	return w, nil
}

// Write 实现 io.Writer。写操作串行化，并在需要时执行 fsync 与轮转。
func (w *rotationFile) Write(p []byte) (n int, err error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	n, err = w.file.Write(p)
	if err != nil {
		return 0, err
	}

	// 按间隔 fsync，兼顾日志持久性与写放大；不能每条都 sync。
	if w.flushInterval > 0 && time.Since(w.lasSyncTime) >= w.flushInterval {
		err = w.file.Sync()
		if err != nil {
			return 0, err
		}
		w.lasSyncTime = time.Now()
	}

	if w.maxSize > 0 {
		w.currentSize += int64(len(p))

		// 达到上限即轮转；轮转后新文件从 0 重新累计。
		if w.currentSize >= w.maxSize {
			err = w.rotate()
			if err != nil {
				return 0, err
			}
		}
	}

	return n, nil
}

// rotate 把当前文件重命名为带时间戳的归档文件，并为后续写入创建新文件。
func (w *rotationFile) rotate() error {
	// 取扩展名，归档文件名形如 <basename>-<timestamp><ext>。
	ext := filepath.Ext(w.filePath)

	// 去掉扩展名，便于拼接时间戳。
	pathWithoutExt := strings.TrimSuffix(w.filePath, ext)

	rotateFilePath := pathWithoutExt + "-" + time.Now().Format(timeLayout) + ext

	// 同一秒内多次触发轮转可能得到相同文件名；此时直接返回，避免覆盖。
	if w.filePath == rotateFilePath {
		return nil
	}

	err := w.file.Close()
	if err != nil {
		return err
	}

	err = os.Rename(w.filePath, rotateFilePath)
	if err != nil {
		return err
	}

	w.file, err = os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.currentSize = 0

	// 清理放到后台执行，避免阻塞日志写入路径。
	if w.maxAge > 0 {
		go func() {
			err = w.clean(pathWithoutExt, ext)
		}()
	}

	return nil
}

// Clean up the old log files in the format of
// <basename>-<timestamp><ext>.
// This should be safe enough to avoid false deletion.
// This will work for multiple restarts of the same program.
// clean 只删除文件名中时间戳可解析且修改时间早于 maxAge 的归档文件，
// 从而避免误删同目录下的其他文件。
func (w *rotationFile) clean(pathWithoutExt string, ext string) error {
	ageTime := time.Now().Add(-w.maxAge)

	directory := filepath.Dir(pathWithoutExt)
	basename := filepath.Base(pathWithoutExt) + "-"

	dir, err := os.ReadDir(directory)
	if err != nil {
		return err
	}

	err = nil
	for _, v := range dir {
		if strings.HasPrefix(v.Name(), basename) && strings.HasSuffix(v.Name(), ext) {
			// 去掉前缀和后缀，只留时间戳用于格式校验。
			trimmed := strings.TrimPrefix(v.Name(), basename)
			trimmed = strings.TrimSuffix(trimmed, ext)

			_, err = time.Parse(timeLayout, trimmed)
			if err == nil {
				info, errInfo := v.Info()
				if errInfo != nil {
					err = errInfo
					// 单个文件信息读取失败不阻断其余文件的清理。
					continue
				}

				if ageTime.After(info.ModTime()) {
					err = os.Remove(filepath.Join(directory, v.Name()))
					if err != nil {
						// 单个文件删除失败同样继续处理后续文件。
						continue
					}
				}
			}

		}
	}

	return err
}

// Close 先 fsync 再关闭文件，确保退出前缓冲数据落盘。
func (w *rotationFile) Close() error {
	w.mut.Lock()
	defer w.mut.Unlock()

	// 显式 Sync，保证数据写入磁盘。
	err := w.file.Sync()
	if err != nil {
		return err
	}

	err = w.file.Close()
	if err != nil {
		return err
	}

	return nil
}
