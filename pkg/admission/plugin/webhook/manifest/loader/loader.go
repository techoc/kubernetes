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

// Package loader 提供 kube-apiserver 专用的 webhook 清单加载能力。
// 通用加载流程位于 staging loader 包；本包注入 kube-apiserver 的 scheme，
// 负责内部类型转换和 API 校验，使静态 webhook 配置遵循 apiserver 的语义。
package loader

import (
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	stagingloader "k8s.io/apiserver/pkg/admission/plugin/webhook/manifest/loader"
	admissionregistration "k8s.io/kubernetes/pkg/apis/admissionregistration"
	admissionregistrationinstall "k8s.io/kubernetes/pkg/apis/admissionregistration/install"
	"k8s.io/kubernetes/pkg/apis/admissionregistration/validation"
)

// ValidatingLoadResult 重新导出 staging 类型，保持现有调用方兼容。
type ValidatingLoadResult = stagingloader.ValidatingLoadResult

// MutatingLoadResult 重新导出 staging 类型，保持现有调用方兼容。
type MutatingLoadResult = stagingloader.MutatingLoadResult

var (
	scheme = runtime.NewScheme()
	codecs = serializer.NewCodecFactory(scheme, serializer.EnableStrict)
)

func init() {
	admissionregistrationinstall.Install(scheme)
	scheme.AddUnversionedTypes(metav1.SchemeGroupVersion, &metav1.List{}, &metav1.Status{})
}

// LoadValidatingManifests 读取指定目录中的所有 YAML 和 JSON 文件，
// 并将其解析为 ValidatingWebhookConfiguration 资源。
// 文件包含 MutatingWebhookConfiguration 时返回错误；文件按字母序处理，保证结果可复现。
func LoadValidatingManifests(dir string) (*ValidatingLoadResult, error) {
	configs, hash, err := stagingloader.LoadManifests(dir, codecs.UniversalDeserializer(), func(obj runtime.Object) ([]*admissionregistrationv1.ValidatingWebhookConfiguration, error) {
		return acceptObject(obj, extractValidating, defaultAndValidateVWC)
	})
	if err != nil {
		return nil, err
	}
	return &ValidatingLoadResult{Configurations: configs, Hash: hash}, nil
}

// LoadMutatingManifests 读取指定目录中的所有 YAML 和 JSON 文件，
// 并将其解析为 MutatingWebhookConfiguration 资源。
// 文件包含 ValidatingWebhookConfiguration 时返回错误；文件按字母序处理，保证结果可复现。
func LoadMutatingManifests(dir string) (*MutatingLoadResult, error) {
	configs, hash, err := stagingloader.LoadManifests(dir, codecs.UniversalDeserializer(), func(obj runtime.Object) ([]*admissionregistrationv1.MutatingWebhookConfiguration, error) {
		return acceptObject(obj, extractMutating, defaultAndValidateMWC)
	})
	if err != nil {
		return nil, err
	}
	return &MutatingLoadResult{Configurations: configs, Hash: hash}, nil
}

// acceptObject 对解码后的对象做类型分派。
// extract 从单个对象或类型化 List 中取出条目；process 逐项填充默认值并执行 API 校验。
// 类型可识别时返回 (items, nil)，不可识别时返回 (nil, nil)，
// 可识别但处理失败时返回 (nil, err)。
func acceptObject[T runtime.Object](
	obj runtime.Object,
	extract func(runtime.Object) ([]T, bool),
	process func(T) error,
) ([]T, error) {
	items, ok := extract(obj)
	if !ok {
		return nil, fmt.Errorf("unsupported resource type %T", obj)
	}
	for _, item := range items {
		if err := process(item); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func extractValidating(obj runtime.Object) ([]*admissionregistrationv1.ValidatingWebhookConfiguration, bool) {
	if c, ok := obj.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		return []*admissionregistrationv1.ValidatingWebhookConfiguration{c}, true
	}
	if l, ok := obj.(*admissionregistrationv1.ValidatingWebhookConfigurationList); ok {
		items := make([]*admissionregistrationv1.ValidatingWebhookConfiguration, len(l.Items))
		for i := range l.Items {
			items[i] = &l.Items[i]
		}
		return items, true
	}
	return nil, false
}

func extractMutating(obj runtime.Object) ([]*admissionregistrationv1.MutatingWebhookConfiguration, bool) {
	if c, ok := obj.(*admissionregistrationv1.MutatingWebhookConfiguration); ok {
		return []*admissionregistrationv1.MutatingWebhookConfiguration{c}, true
	}
	if l, ok := obj.(*admissionregistrationv1.MutatingWebhookConfigurationList); ok {
		items := make([]*admissionregistrationv1.MutatingWebhookConfiguration, len(l.Items))
		for i := range l.Items {
			items[i] = &l.Items[i]
		}
		return items, true
	}
	return nil, false
}

// defaultAndValidateVWC 对 ValidatingWebhookConfiguration 填充 scheme 默认值，
// 并按内部 API 语义执行标准校验，确保静态配置与 apiserver 的接受范围一致。
func defaultAndValidateVWC(config *admissionregistrationv1.ValidatingWebhookConfiguration) error {
	scheme.Default(config)

	internalObj := &admissionregistration.ValidatingWebhookConfiguration{}
	if err := scheme.Convert(config, internalObj, nil); err != nil {
		return fmt.Errorf("ValidatingWebhookConfiguration %q: conversion error: %w", config.Name, err)
	}
	if errs := validation.ValidateValidatingWebhookConfiguration(internalObj); len(errs) > 0 {
		return fmt.Errorf("ValidatingWebhookConfiguration %q: %w", config.Name, errs.ToAggregate())
	}
	resultConfig := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := scheme.Convert(internalObj, resultConfig, nil); err != nil {
		return fmt.Errorf("ValidatingWebhookConfiguration %q: back-conversion error: %w", config.Name, err)
	}
	*config = *resultConfig
	return nil
}

// defaultAndValidateMWC 对 MutatingWebhookConfiguration 填充 scheme 默认值，
// 并按内部 API 语义执行标准校验，确保静态配置与 apiserver 的接受范围一致。
func defaultAndValidateMWC(config *admissionregistrationv1.MutatingWebhookConfiguration) error {
	scheme.Default(config)

	internalObj := &admissionregistration.MutatingWebhookConfiguration{}
	if err := scheme.Convert(config, internalObj, nil); err != nil {
		return fmt.Errorf("MutatingWebhookConfiguration %q: conversion error: %w", config.Name, err)
	}
	if errs := validation.ValidateMutatingWebhookConfiguration(internalObj); len(errs) > 0 {
		return fmt.Errorf("MutatingWebhookConfiguration %q: %w", config.Name, errs.ToAggregate())
	}
	resultConfig := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := scheme.Convert(internalObj, resultConfig, nil); err != nil {
		return fmt.Errorf("MutatingWebhookConfiguration %q: back-conversion error: %w", config.Name, err)
	}
	*config = *resultConfig
	return nil
}
