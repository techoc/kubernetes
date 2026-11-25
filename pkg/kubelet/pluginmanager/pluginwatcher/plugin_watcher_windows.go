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
	"github.com/fsnotify/fsnotify"
	"k8s.io/kubernetes/pkg/kubelet/util"
	"os"
)

// getStat 获取事件对应路径的文件信息。
func getStat(event fsnotify.Event) (os.FileInfo, error) {
	fi, err := os.Stat(event.Name)
	// TODO: Windows 20H2 的 os.Stat() 存在问题，临时回退到 os.Lstat()；修复后可移除，详见 microsoft/Windows-Containers#97。
	if err != nil {
		fi, err = os.Lstat(event.Name)
	}

	return fi, err
}

// getSocketPath 在 Windows 上对 socket 路径做规范化处理。
var getSocketPath = util.NormalizePath
