/*
Copyright 2022 The Kubernetes Authors.

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

package nodeshutdown

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"
)

// storage 抽象节点关机的状态持久化。
type storage interface {
	// Store 保存数据。
	Store(data interface{}) (err error)
	// Load 读取数据。
	Load(data interface{}) (err error)
}

// localStorage 把状态保存到本地文件。
type localStorage struct {
	// Path 是状态文件路径。
	Path string
}

// Store 序列化数据并原子写入文件。
func (l localStorage) Store(data interface{}) (err error) {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// 原子写入，避免断电时留下不完整文件。
	return atomicWrite(l.Path, b, 0644)
}

// Load 从文件读取并反序列化数据。
func (l localStorage) Load(data interface{}) (err error) {
	b, err := os.ReadFile(l.Path)
	if err != nil {
		// 文件不存在视为没有保存过状态。
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(b, data)
}

// timestamp 把时间转换为 Unix 秒；零值返回 0。
func timestamp(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.Unix())
}

// state 是持久化的节点关机时间状态。
type state struct {
	// StartTime 是关机流程开始时间。
	StartTime time.Time `json:"startTime"`
	// EndTime 是关机流程结束时间。
	EndTime time.Time `json:"endTime"`
}

// atomicWrite 原子地把数据写入指定文件。
func atomicWrite(filename string, data []byte, perm os.FileMode) error {
	// 先写入同目录临时文件，再重命名以保证原子性。
	f, err := os.CreateTemp(filepath.Dir(filename), ".tmp-"+filepath.Base(filename))
	if err != nil {
		return err
	}
	err = os.Chmod(f.Name(), perm)
	if err != nil {
		f.Close()
		return err
	}
	n, err := f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	if n < len(data) {
		f.Close()
		return io.ErrShortWrite
	}
	// 落盘后再重命名，确保数据真正持久化。
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filename)
}
