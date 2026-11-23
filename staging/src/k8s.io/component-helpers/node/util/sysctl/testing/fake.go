/*
Copyright 2015 The Kubernetes Authors.

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

// Package testing 提供 sysctl 接口的假实现，供不依赖宿主内核 procfs 的单元测试使用。
package testing

import (
	"os"

	"k8s.io/component-helpers/node/util/sysctl"
)

// Fake 是 sysctl.Interface 的 map 版实现，用于测试与 mock。
// 用 map 而不是真实 /proc/sys 写入是为了让测试不依赖宿主内核是否暴露对应项，
// 同时允许用例在没有任何前置条件的情况下预置任意值。
type Fake struct {
	// Settings 保存 sysctl 名到当前值的映射，测试可直接向其中预置条目。
	Settings map[string]int
}

// NewFake 创建一个空配置 map 的 Fake 实例。
func NewFake() *Fake {
	return &Fake{
		Settings: make(map[string]int),
	}
}

// GetSysctl 返回指定 sysctl 项的预置值；未预置时返回 -1 和 os.ErrNotExist。
func (m *Fake) GetSysctl(sysctl string) (int, error) {
	v, found := m.Settings[sysctl]
	if !found {
		// 返回 os.ErrNotExist 对齐真实 sysctl 读取“文件不存在”的语义，
		// 这样生产代码里的 os.IsNotExist 分支在测试中同样会被走到。
		return -1, os.ErrNotExist
	}
	return v, nil
}

// SetSysctl 在 map 中记录指定 sysctl 项的新值。
func (m *Fake) SetSysctl(sysctl string, newVal int) error {
	m.Settings[sysctl] = newVal
	return nil
}

// 编译期断言：一旦 sysctl.Interface 增加方法，这里先失败，
// 免得问题推迟到某个测试把 Fake 传给调用方时才暴露。
var _ = sysctl.Interface(&Fake{})
