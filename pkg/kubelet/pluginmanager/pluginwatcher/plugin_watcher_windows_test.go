//go:build windows

/*
Copyright 2023 The Kubernetes Authors.

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

package pluginwatcher

import (
	"os"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
)

// TestGetStatWindows 验证 Windows 上 getStat 与 os.Stat/os.Lstat 的结果一致。
func TestGetStatWindows(t *testing.T) {
	event := fsnotify.Event{Name: "name", Op: fsnotify.Create}
	fi, err := getStat(event)
	fiExpected, errExpected := os.Stat(event.Name)
	// TODO: This is a workaround for Windows 20H2 issue for os.Stat(). Please see
	// 这是针对 Windows 20H2 上 os.Stat() 问题的临时方案，详见 microsoft/Windows-Containers#97。
	// 该问题修复后，就不需要再回退到 os.Lstat()。
	if errExpected != nil {
		fiExpected, errExpected = os.Lstat(event.Name)
	}

	assert.Equal(t, fi, fiExpected)
	assert.Equal(t, err, errExpected)
}
