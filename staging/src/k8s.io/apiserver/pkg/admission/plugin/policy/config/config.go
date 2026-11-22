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

// Package config 负责解码策略类准入插件（ValidatingAdmissionPolicy、
// MutatingAdmissionPolicy）的组件配置文件，并校验其中引用的静态清单目录。
package config

import (
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission/plugin/manifest"
	"k8s.io/apiserver/pkg/admission/plugin/policy/config/apis/policyconfig"
	v1 "k8s.io/apiserver/pkg/admission/plugin/policy/config/apis/policyconfig/v1"
)

var (
	// 使用 EnableStrict 是为了让配置文件中的未知字段直接报错，而不是被静默忽略——
	// 拼写错误的配置项若被忽略，会让运维误以为策略已生效。
	scheme = runtime.NewScheme()
	codecs = serializer.NewCodecFactory(scheme, serializer.EnableStrict)
)

func init() {
	utilruntime.Must(policyconfig.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
}

// PolicyConfig holds the configuration loaded from the config file.
//
// 中文说明：只暴露已校验的字段，调用方无需再关心版本转换细节。
type PolicyConfig struct {
	// StaticManifestsDir is the path to the directory containing static policy manifests.
	StaticManifestsDir string
}

// staticManifestsDirAccessor is implemented by config types that have a StaticManifestsDir field.
//
// 中文说明：内部访问器接口让 loadConfig 能同时处理 validating 与 mutating
// 两种配置类型，而无需在加载逻辑里写重复的类型分支。
type staticManifestsDirAccessor interface {
	runtime.Object
	GetStaticManifestsDir() string
}

// LoadValidatingConfig extracts the validating admission policy configuration from configFile.
//
// 中文说明：类型判定基于内部（无版本）类型，因为 UniversalDecoder 已先完成转换。
func LoadValidatingConfig(configFile io.Reader) (PolicyConfig, error) {
	return loadConfig(configFile, func(obj runtime.Object) bool {
		_, ok := obj.(*policyconfig.ValidatingAdmissionPolicyConfiguration)
		return ok
	})
}

// LoadMutatingConfig extracts the mutating admission policy configuration from configFile.
//
// 中文说明：与 LoadValidatingConfig 共享加载逻辑，仅类型过滤不同。
func LoadMutatingConfig(configFile io.Reader) (PolicyConfig, error) {
	return loadConfig(configFile, func(obj runtime.Object) bool {
		_, ok := obj.(*policyconfig.MutatingAdmissionPolicyConfiguration)
		return ok
	})
}

func loadConfig(configFile io.Reader, isExpectedType func(runtime.Object) bool) (PolicyConfig, error) {
	var cfg PolicyConfig
	// nil reader 表示插件未提供配置文件，视为合法的空配置。
	if configFile == nil {
		return cfg, nil
	}

	data, err := io.ReadAll(configFile)
	if err != nil {
		return cfg, err
	}
	// UniversalDecoder 会完成组/版本识别与到内部版本的转换。
	decoder := codecs.UniversalDecoder()
	decodedObj, err := runtime.Decode(decoder, data)
	if err != nil {
		return cfg, err
	}
	// 类型校验防止运维把 mutating 配置误传给 validating 插件（反之亦然），
	// 否则字段会被静默丢弃。
	if !isExpectedType(decodedObj) {
		return cfg, fmt.Errorf("unexpected type: %T", decodedObj)
	}
	config, ok := decodedObj.(staticManifestsDirAccessor)
	if !ok {
		return cfg, fmt.Errorf("type %T does not implement staticManifestsDirAccessor", decodedObj)
	}

	// 目录在启动期校验一次，后续热加载只比较内容哈希。
	if err := manifest.ValidateStaticManifestsDir(config.GetStaticManifestsDir()); err != nil {
		return cfg, err
	}
	cfg.StaticManifestsDir = config.GetStaticManifestsDir()
	return cfg, nil
}
