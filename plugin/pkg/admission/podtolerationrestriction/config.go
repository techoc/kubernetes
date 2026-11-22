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

package podtolerationrestriction

import (
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	internalapi "k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction"
	"k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction/install"
	versionedapi "k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction/v1alpha1"
	"k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction/validation"
)

var (
	scheme = runtime.NewScheme()
	codecs = serializer.NewCodecFactory(scheme)
)

func init() {
	install.Install(scheme)
}

// loadConfiguration 加载并校验配置；未提供配置时返回默认配置。
func loadConfiguration(config io.Reader) (*internalapi.Configuration, error) {
	// 未提供配置时使用 scheme 生成的默认值，避免依赖外部配置文件。
	if config == nil {
		externalConfig := &versionedapi.Configuration{}
		scheme.Default(externalConfig)
		internalConfig := &internalapi.Configuration{}
		if err := scheme.Convert(externalConfig, internalConfig, nil); err != nil {
			return nil, err
		}
		return internalConfig, nil
	}
	// 有配置则按通用解码器解码为内部版本。
	data, err := io.ReadAll(config)
	if err != nil {
		return nil, err
	}
	decoder := codecs.UniversalDecoder()
	decodedObj, err := runtime.Decode(decoder, data)
	if err != nil {
		return nil, err
	}
	externalConfig, ok := decodedObj.(*internalapi.Configuration)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %T", decodedObj)
	}

	if err := validation.ValidateConfiguration(externalConfig); err != nil {
		return nil, err
	}

	return externalConfig, nil
}
