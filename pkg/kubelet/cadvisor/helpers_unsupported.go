//go:build !linux

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

package cadvisor

import "errors"

// unsupportedImageFsInfoProvider 是不支持 cAdvisor 的构建中的空实现。
type unsupportedImageFsInfoProvider struct{}

// ImageFsInfoLabel 返回配置运行时的镜像文件系统标签；
// 在不受支持的平台上始终返回错误。
func (i *unsupportedImageFsInfoProvider) ImageFsInfoLabel() (string, error) {
	return "", errors.New("unsupported")
}

// ContainerFsInfoLabel 在不受支持的平台上始终返回错误。
func (i *unsupportedImageFsInfoProvider) ContainerFsInfoLabel() (string, error) {
	return "", errors.New("unsupported")
}

// NewImageFsInfoProvider 返回不受支持平台上的空 provider。
func NewImageFsInfoProvider(runtimeEndpoint string) ImageFsInfoProvider {
	return &unsupportedImageFsInfoProvider{}
}
