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

package scheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletconfigv1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1"
	kubeletconfigv1beta1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1beta1"
)

// 以下是 kubeletconfig API 组的工具函数。

// NewSchemeAndCodecs 返回能识别 kubeletconfig API 组类型的 Scheme 与 CodecFactory。
// 通过 mutators 可以调整 CodecFactory 的行为，例如开启严格解码。
func NewSchemeAndCodecs(mutators ...serializer.CodecFactoryOptionsMutator) (*runtime.Scheme, *serializer.CodecFactory, error) {
	scheme := runtime.NewScheme()
	if err := kubeletconfig.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	if err := kubeletconfigv1beta1.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	if err := kubeletconfigv1.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	codecs := serializer.NewCodecFactory(scheme, mutators...)
	return scheme, &codecs, nil
}
