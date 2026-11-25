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

// Package loader 提供从清单文件加载策略配置的能力。
// 本文件负责 MutatingAdmissionPolicy（MAP）和 MutatingAdmissionPolicyBinding（MAPB）。
// 解码所用的 scheme / codecs 变量定义在 validating_loader.go 中，由两个文件共享；
// 这样可以保证 VAP 和 MAP 使用同一套 strict 解码规则。
package loader

import (
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
	policyloader "k8s.io/apiserver/pkg/admission/plugin/policy/manifest/loader"
	admissionregistration "k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/apis/admissionregistration/validation"
)

// LoadedMutatingPolicyManifests 保存从清单文件加载出来的 MAP/Binding 配置。
type LoadedMutatingPolicyManifests struct {
	// Policies 是加载到的 MutatingAdmissionPolicy 列表。
	Policies []*admissionregistrationv1.MutatingAdmissionPolicy
	// Bindings 是加载到的 MutatingAdmissionPolicyBinding 列表。
	Bindings []*admissionregistrationv1.MutatingAdmissionPolicyBinding
	// Hash 是所有已加载文件的 sha256 摘要，用于检测目录内容是否发生变化。
	Hash string
}

// LoadMutatingManifestsFromDirectory 读取指定目录下的所有 YAML/JSON 文件，
// 将其解析为 MutatingAdmissionPolicy 或 MutatingAdmissionPolicyBinding 资源。
// 文件按字母序处理，保证加载结果确定且可复现。
//
// 与 VAP 版本一样，文件遍历、List 展开、名称唯一性/静态后缀检查、
// binding 引用检查以及排序均由通用 loader 完成；本文件只提供 MAP 类型的
// 接收函数与默认值/校验逻辑。
func LoadMutatingManifestsFromDirectory(dir string) (*LoadedMutatingPolicyManifests, error) {
	// 注入 MAP 专属的 accept 函数和“从 binding 取 policy 名”的回调，
	// 其余流程复用 policyloader.LoadPolicyManifests。
	policies, bindings, hash, err := policyloader.LoadPolicyManifests(
		dir,
		codecs.UniversalDeserializer(),
		acceptMAP,
		acceptMAPB,
		func(b *admissionregistrationv1.MutatingAdmissionPolicyBinding) string {
			return b.Spec.PolicyName
		},
	)
	if err != nil {
		return nil, err
	}
	return &LoadedMutatingPolicyManifests{
		Policies: policies,
		Bindings: bindings,
		Hash:     hash,
	}, nil
}

// acceptMAP 从解码后的 runtime.Object 中取出 MutatingAdmissionPolicy 项，
// 并逐项执行默认值填充与校验。类型不匹配时返回 ErrUnrecognizedType，
// 让通用 loader 继续尝试 binding 分支。
func acceptMAP(obj runtime.Object) ([]*admissionregistrationv1.MutatingAdmissionPolicy, error) {
	switch config := obj.(type) {
	case *admissionregistrationv1.MutatingAdmissionPolicy:
		if err := defaultAndValidateMAP(config); err != nil {
			return nil, err
		}
		return []*admissionregistrationv1.MutatingAdmissionPolicy{config}, nil
	case *admissionregistrationv1.MutatingAdmissionPolicyList:
		items := make([]*admissionregistrationv1.MutatingAdmissionPolicy, len(config.Items))
		for i := range config.Items {
			// 取元素地址，让默认值和规范化结果直接写回 List 内的元素。
			items[i] = &config.Items[i]
			if err := defaultAndValidateMAP(items[i]); err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		// 不是 MAP，交给通用 loader 去试 MAPB。
		return nil, policyloader.ErrUnrecognizedType
	}
}

// acceptMAPB 从解码后的 runtime.Object 中取出 MutatingAdmissionPolicyBinding 项，
// 并逐项执行默认值填充与校验。类型不匹配时返回 ErrUnrecognizedType。
func acceptMAPB(obj runtime.Object) ([]*admissionregistrationv1.MutatingAdmissionPolicyBinding, error) {
	switch config := obj.(type) {
	case *admissionregistrationv1.MutatingAdmissionPolicyBinding:
		if err := defaultAndValidateMAPB(config); err != nil {
			return nil, err
		}
		return []*admissionregistrationv1.MutatingAdmissionPolicyBinding{config}, nil
	case *admissionregistrationv1.MutatingAdmissionPolicyBindingList:
		items := make([]*admissionregistrationv1.MutatingAdmissionPolicyBinding, len(config.Items))
		for i := range config.Items {
			items[i] = &config.Items[i]
			if err := defaultAndValidateMAPB(items[i]); err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		return nil, policyloader.ErrUnrecognizedType
	}
}

// defaultAndValidateMAP 对单个 MutatingAdmissionPolicy 执行：
// scheme 默认值填充 → 转换为内部类型 → 标准校验 → 转回 v1 → 写回原对象。
//
// 校验函数定义在 internal 类型上，而清单使用 v1 外部类型，因此需要来回转换；
// 回写保证调用方拿到的始终是经过默认值填充与校验的版本。
func defaultAndValidateMAP(policy *admissionregistrationv1.MutatingAdmissionPolicy) error {
	// 先填默认值（例如 failurePolicy、reinvocationPolicy 等字段的缺省值）。
	scheme.Default(policy)

	internalObj := &admissionregistration.MutatingAdmissionPolicy{}
	if err := scheme.Convert(policy, internalObj, nil); err != nil {
		return fmt.Errorf("MutatingAdmissionPolicy %q: conversion error: %w", policy.Name, err)
	}
	if errs := validation.ValidateMutatingAdmissionPolicy(internalObj); len(errs) > 0 {
		// 聚合多条校验错误，并带上资源名，方便定位问题策略。
		return fmt.Errorf("MutatingAdmissionPolicy %q: %w", policy.Name, errs.ToAggregate())
	}
	resultObj := &admissionregistrationv1.MutatingAdmissionPolicy{}
	if err := scheme.Convert(internalObj, resultObj, nil); err != nil {
		return fmt.Errorf("MutatingAdmissionPolicy %q: back-conversion error: %w", policy.Name, err)
	}
	// 把规范化后的对象写回，后续使用者看到的即是校验通过的版本。
	*policy = *resultObj
	return nil
}

// defaultAndValidateMAPB 对单个 MutatingAdmissionPolicyBinding 执行与
// defaultAndValidateMAP 相同的“默认值 → 内部类型 → 校验 → 回写”流程。
func defaultAndValidateMAPB(binding *admissionregistrationv1.MutatingAdmissionPolicyBinding) error {
	scheme.Default(binding)

	internalObj := &admissionregistration.MutatingAdmissionPolicyBinding{}
	if err := scheme.Convert(binding, internalObj, nil); err != nil {
		return fmt.Errorf("MutatingAdmissionPolicyBinding %q: conversion error: %w", binding.Name, err)
	}
	if errs := validation.ValidateMutatingAdmissionPolicyBinding(internalObj); len(errs) > 0 {
		return fmt.Errorf("MutatingAdmissionPolicyBinding %q: %w", binding.Name, errs.ToAggregate())
	}
	resultObj := &admissionregistrationv1.MutatingAdmissionPolicyBinding{}
	if err := scheme.Convert(internalObj, resultObj, nil); err != nil {
		return fmt.Errorf("MutatingAdmissionPolicyBinding %q: back-conversion error: %w", binding.Name, err)
	}
	*binding = *resultObj
	return nil
}
