/*
Copyright The Kubernetes Authors.

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

package deviceattribute

import (
	"io/fs"
	"os"
)

// machine 是对主机/sysfs 的最小抽象；使用包装结构体是为了支持函数式选项模式。
type machine struct {
	// sysfs 是用于读取 sysfs 文件及符号链接的（只读）文件系统接口。
	sysfs fs.ReadLinkFS
}

// MachineModifier 是用于配置 machine 的函数式选项类型。
type MachineModifier func(*machine)

// WithFS 返回一个把默认 sysfs 替换为调用方所提供文件系统的选项。
func WithFS(sysfs fs.ReadLinkFS) MachineModifier {
	return func(mc *machine) {
		// 直接注入传入的文件系统，便于测试时替换为内存文件系统。
		mc.sysfs = sysfs
	}
}

// WithFSFromRoot 返回一个以 root 为根目录的文件系统选项；用途类似 chroot，
// 仅为方便测试与容器内使用，并不提供安全隔离。
func WithFSFromRoot(root string) MachineModifier {
	return func(mc *machine) {
		// os.DirFS 返回的文件系统实现了 ReadLinkFS，这里断言后存入 mc。
		mc.sysfs = os.DirFS(root).(fs.ReadLinkFS)
	}
}
