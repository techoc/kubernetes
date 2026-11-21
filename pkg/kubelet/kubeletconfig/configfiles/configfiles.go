/*
Copyright 2017 The Kubernetes Authors.

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

package configfiles

import (
	"context"
	"fmt"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
	utilcodec "k8s.io/kubernetes/pkg/kubelet/kubeletconfig/util/codec"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
)

// Loader 从存储层加载配置。
type Loader interface {
	// Load 从存储层加载并返回 KubeletConfiguration，失败时返回错误。
	Load(context.Context) (*kubeletconfig.KubeletConfiguration, error)
	// LoadIntoJSON 从存储层加载 KubeletConfiguration 并以 JSON 字节切片返回，
	// 失败时返回错误。
	LoadIntoJSON() ([]byte, *schema.GroupVersionKind, error)
}

// fsLoader 从指定文件加载配置。
type fsLoader struct {
	// fs 是配置文件所在的文件系统，测试中可替换。
	fs utilfs.Filesystem
	// kubeletCodecs 是用于解码配置文件的 scheme。
	kubeletCodecs *serializer.CodecFactory
	// kubeletFile 是包含序列化 KubeletConfiguration 的文件绝对路径。
	kubeletFile string
}

// NewFsLoader 返回从 kubeletFile 加载 KubeletConfiguration 的 Loader。
func NewFsLoader(fs utilfs.Filesystem, kubeletFile string) (Loader, error) {
	// 启用严格解码，尽早发现未知或重复字段。
	_, kubeletCodecs, err := kubeletscheme.NewSchemeAndCodecs(serializer.EnableStrict)
	if err != nil {
		return nil, err
	}

	return &fsLoader{
		fs:            fs,
		kubeletCodecs: kubeletCodecs,
		kubeletFile:   kubeletFile,
	}, nil
}

// Load 读取配置文件并解码为内部 KubeletConfiguration。
func (loader *fsLoader) Load(ctx context.Context) (*kubeletconfig.KubeletConfiguration, error) {
	data, err := loader.fs.ReadFile(loader.kubeletFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read kubelet config file %q, error: %v", loader.kubeletFile, err)
	}

	// 空配置视为错误：部分参数是必填的。
	if len(data) == 0 {
		return nil, fmt.Errorf("kubelet config file %q was empty", loader.kubeletFile)
	}

	kc, err := utilcodec.DecodeKubeletConfiguration(ctx, loader.kubeletCodecs, data)
	if err != nil {
		return nil, err
	}

	// 把配置中的相对路径全部转换为绝对路径。
	// 相对路径按配置文件所在目录解析。
	resolveRelativePaths(kubeletconfig.KubeletConfigurationPathRefs(kc), filepath.Dir(loader.kubeletFile))
	return kc, nil
}

// LoadIntoJSON 读取配置文件并解码为 JSON 字节切片。
func (loader *fsLoader) LoadIntoJSON() ([]byte, *schema.GroupVersionKind, error) {
	data, err := loader.fs.ReadFile(loader.kubeletFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read drop-in kubelet config file %q, error: %w", loader.kubeletFile, err)
	}

	// 空配置视为错误：部分参数是必填的。
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("kubelet config file %q was empty", loader.kubeletFile)
	}

	return utilcodec.DecodeKubeletConfigurationIntoJSON(loader.kubeletCodecs, data)
}

// resolveRelativePaths 以 root 为基准把相对路径转换为绝对路径。
func resolveRelativePaths(paths []*string, root string) {
	for _, path := range paths {
		// 空路径保持原样（"无路径"是合法输入）；
		// 已经是绝对路径的无需处理。
		if len(*path) > 0 && !utilfs.IsAbs(*path) {
			*path = filepath.Join(root, *path)
		}
	}
}
