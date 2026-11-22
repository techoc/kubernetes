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

package config

import (
	"fmt"
	"io"
	"path"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission/plugin/manifest"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/config/apis/webhookadmission"
	v1 "k8s.io/apiserver/pkg/admission/plugin/webhook/config/apis/webhookadmission/v1"
)

var (
	// 独立的 scheme 与 codec 工厂，不与全局 scheme 混用，避免 webhook 配置类型污染其他组件。
	scheme = runtime.NewScheme()
	// EnableStrict 让解码拒绝未知字段：配置拼写错误应在启动阶段暴露，而不是被静默忽略。
	codecs = serializer.NewCodecFactory(scheme, serializer.EnableStrict)
)

func init() {
	utilruntime.Must(webhookadmission.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
}

// LoadConfig extracts the webhook admission configuration from configFile.
// configFile 为 nil 时返回空配置而非错误，允许插件在无外部配置的情况下运行。
func LoadConfig(configFile io.Reader) (*webhookadmission.WebhookAdmission, error) {
	if configFile == nil {
		return &webhookadmission.WebhookAdmission{}, nil
	}
	// we have a config so parse it.
	data, err := io.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	// UniversalDecoder 按 apiVersion/kind 选择 v1 并转换到内部版本；
	// 配置来源是本地文件，大小有限，直接全量读入即可。
	decoder := codecs.UniversalDecoder()
	decodedObj, err := runtime.Decode(decoder, data)
	if err != nil {
		return nil, err
	}
	config, ok := decodedObj.(*webhookadmission.WebhookAdmission)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %T", decodedObj)
	}

	// KubeConfigFile may be empty when only staticManifestsDir is configured.
	// 强制绝对路径，避免结果依赖 apiserver 的工作目录——不同启动方式下相对路径含义不同。
	if len(config.KubeConfigFile) > 0 && !path.IsAbs(config.KubeConfigFile) {
		return nil, field.Invalid(field.NewPath("kubeConfigFile"), config.KubeConfigFile, "must be an absolute file path")
	}

	// 静态清单目录与 kubeconfig 可并存，这里统一做路径与特性门控校验。
	if err := manifest.ValidateStaticManifestsDir(config.StaticManifestsDir); err != nil {
		return nil, err
	}

	return config, nil
}
