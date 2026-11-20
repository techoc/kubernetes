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

// Package loader 提供从清单文件（manifest）加载 VAP/MAP 配置的能力，
// 并在加载过程中完成基于 scheme 的默认值填充（defaulting）和校验（validation）。
//
// 这里只负责“单个对象的类型识别 + 默认值 + 内部类型校验”；
// 文件遍历、YAML 多文档拆分、v1.List 展开、名称唯一性/静态后缀检查、
// binding 引用检查以及最终按名称排序，都由 staging 中的通用 loader
// （k8s.io/apiserver/pkg/admission/plugin/policy/manifest/loader）完成。
package loader

import (
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	policyloader "k8s.io/apiserver/pkg/admission/plugin/policy/manifest/loader"
	admissionregistration "k8s.io/kubernetes/pkg/apis/admissionregistration"
	admissionregistrationinstall "k8s.io/kubernetes/pkg/apis/admissionregistration/install"
	"k8s.io/kubernetes/pkg/apis/admissionregistration/validation"
)

var (
	// scheme 是本包装层私有的运行时 scheme。它不复用 apiserver 全局 scheme，
	// 而是只安装 admissionregistration 相关类型，保证解码、默认值、转换行为可控。
	scheme = runtime.NewScheme()
	// codecs 用该 scheme 构造反序列化器。
	// EnableStrict 很关键：清单里出现未知/拼写错的字段会直接报错，
	// 避免静态策略因为字段名写错而“静默生效”，从而产生安全隐患。
	codecs = serializer.NewCodecFactory(scheme, serializer.EnableStrict)
)

func init() {
	// 安装 admissionregistration 的内部（internal）类型以及 v1 外部类型的转换/默认值注册。
	// 只有先 Install，后面 scheme.Default / scheme.Convert 才能找到对应函数。
	admissionregistrationinstall.Install(scheme)
	// 通用 loader 会展开 v1.List 这样的无版本类型，必须显式注册，
	// 否则 UniversalDeserializer 不认识 "apiVersion: v1, kind: List"。
	// Status 一并注册，便于解码器处理常见元类型。
	scheme.AddUnversionedTypes(metav1.SchemeGroupVersion, &metav1.List{}, &metav1.Status{})
}

// LoadedValidatingPolicyManifests 保存从清单文件加载出来的 VAP/Binding 配置。
type LoadedValidatingPolicyManifests struct {
	// Policies 是加载到的 ValidatingAdmissionPolicy 列表。
	Policies []*admissionregistrationv1.ValidatingAdmissionPolicy
	// Bindings 是加载到的 ValidatingAdmissionPolicyBinding 列表。
	Bindings []*admissionregistrationv1.ValidatingAdmissionPolicyBinding
	// Hash 是所有已加载文件的 sha256 摘要，用于检测目录内容是否发生变化。
	Hash string
}

// LoadValidatingManifestsFromDirectory 读取指定目录下的所有 YAML/JSON 文件，
// 将其解析为 ValidatingAdmissionPolicy 或 ValidatingAdmissionPolicyBinding 资源。
// 文件按字母序处理，保证加载结果可复现（deterministic）。
//
// 实际流程（由 policyloader.LoadPolicyManifests 完成）：
//  1. 读取目录下所有文件并计算 Hash；
//  2. 逐个文档解码，遇到 v1.List 则递归展开每个 item；
//  3. 依次调用 acceptVAP / acceptVAPB 做类型识别与校验；
//  4. 校验清单名称（必须以 .static.k8s.io 结尾且全局唯一）；
//  5. 校验静态清单约束（不支持 paramKind / paramRef，policyName 必须有静态后缀）；
//  6. 校验每个 binding 引用的 policy 确实在同一目录的清单中存在；
//  7. 对 policies / bindings 按名称排序后返回。
func LoadValidatingManifestsFromDirectory(dir string) (*LoadedValidatingPolicyManifests, error) {
	// 把“怎么读文件、怎么展开 List、怎么校验名字和引用”交给通用 loader；
	// 这里只注入与该资源类型相关的三件事：
	//   - 反序列化器：codecs.UniversalDeserializer()
	//   - 对象接收函数：acceptVAP / acceptVAPB
	//   - 从 binding 中取出被引用的 policy 名字的回调
	policies, bindings, hash, err := policyloader.LoadPolicyManifests(
		dir,
		codecs.UniversalDeserializer(),
		acceptVAP,
		acceptVAPB,
		func(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) string {
			return b.Spec.PolicyName
		},
	)
	if err != nil {
		return nil, err
	}
	return &LoadedValidatingPolicyManifests{
		Policies: policies,
		Bindings: bindings,
		Hash:     hash,
	}, nil
}

// acceptVAP 从解码后的 runtime.Object 中取出 ValidatingAdmissionPolicy 项，
// 并逐项执行默认值填充与校验。
// 类型不匹配时返回 ErrUnrecognizedType，让通用 loader 继续尝试 binding 分支。
func acceptVAP(obj runtime.Object) ([]*admissionregistrationv1.ValidatingAdmissionPolicy, error) {
	// 单个资源和 List 资源都要能接受：List 常见于从文件批量加载的场景。
	switch config := obj.(type) {
	case *admissionregistrationv1.ValidatingAdmissionPolicy:
		if err := defaultAndValidateVAP(config); err != nil {
			return nil, err
		}
		return []*admissionregistrationv1.ValidatingAdmissionPolicy{config}, nil
	case *admissionregistrationv1.ValidatingAdmissionPolicyList:
		items := make([]*admissionregistrationv1.ValidatingAdmissionPolicy, len(config.Items))
		for i := range config.Items {
			// 取元素地址而不是拷贝值，后续 default/validate 的结果直接写回 List 的元素。
			items[i] = &config.Items[i]
			if err := defaultAndValidateVAP(items[i]); err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		// 不是 VAP，交给通用 loader 去试 VAPB，而不是立刻报错。
		return nil, policyloader.ErrUnrecognizedType
	}
}

// acceptVAPB 从解码后的 runtime.Object 中取出 ValidatingAdmissionPolicyBinding 项，
// 并逐项执行默认值填充与校验。类型不匹配时同样返回 ErrUnrecognizedType。
func acceptVAPB(obj runtime.Object) ([]*admissionregistrationv1.ValidatingAdmissionPolicyBinding, error) {
	switch config := obj.(type) {
	case *admissionregistrationv1.ValidatingAdmissionPolicyBinding:
		if err := defaultAndValidateVAPB(config); err != nil {
			return nil, err
		}
		return []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{config}, nil
	case *admissionregistrationv1.ValidatingAdmissionPolicyBindingList:
		items := make([]*admissionregistrationv1.ValidatingAdmissionPolicyBinding, len(config.Items))
		for i := range config.Items {
			items[i] = &config.Items[i]
			if err := defaultAndValidateVAPB(items[i]); err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		return nil, policyloader.ErrUnrecognizedType
	}
}

// defaultAndValidateVAP 对单个 ValidatingAdmissionPolicy 执行：
// scheme 默认值填充 → 转换为内部类型 → 标准校验 → 转回 v1 → 写回原对象。
//
// 之所以要“v1 → internal → 校验 → v1”来回转换，是因为校验函数定义在内部类型上，
// 而清单使用 v1 外部类型；转换过程同时会规范化字段（比如默认值在内部类型上补齐）。
func defaultAndValidateVAP(policy *admissionregistrationv1.ValidatingAdmissionPolicy) error {
	// 先填默认值（例如 failurePolicy、matchPolicy 等字段的缺省值）。
	scheme.Default(policy)

	internalObj := &admissionregistration.ValidatingAdmissionPolicy{}
	if err := scheme.Convert(policy, internalObj, nil); err != nil {
		return fmt.Errorf("ValidatingAdmissionPolicy %q: conversion error: %w", policy.Name, err)
	}
	if errs := validation.ValidateValidatingAdmissionPolicy(internalObj); len(errs) > 0 {
		// 校验错误聚合成一个 error，并带上资源名，便于定位是哪条策略出问题。
		return fmt.Errorf("ValidatingAdmissionPolicy %q: %w", policy.Name, errs.ToAggregate())
	}
	resultObj := &admissionregistrationv1.ValidatingAdmissionPolicy{}
	if err := scheme.Convert(internalObj, resultObj, nil); err != nil {
		return fmt.Errorf("ValidatingAdmissionPolicy %q: back-conversion error: %w", policy.Name, err)
	}
	// 把规范化后的结果写回调用方持有的对象，保证后续使用的是校验过的版本。
	*policy = *resultObj
	return nil
}

// defaultAndValidateVAPB 对单个 ValidatingAdmissionPolicyBinding 执行与
// defaultAndValidateVAP 相同的“默认值 → 内部类型 → 校验 → 回写”流程。
func defaultAndValidateVAPB(binding *admissionregistrationv1.ValidatingAdmissionPolicyBinding) error {
	scheme.Default(binding)

	internalObj := &admissionregistration.ValidatingAdmissionPolicyBinding{}
	if err := scheme.Convert(binding, internalObj, nil); err != nil {
		return fmt.Errorf("ValidatingAdmissionPolicyBinding %q: conversion error: %w", binding.Name, err)
	}
	if errs := validation.ValidateValidatingAdmissionPolicyBinding(internalObj); len(errs) > 0 {
		return fmt.Errorf("ValidatingAdmissionPolicyBinding %q: %w", binding.Name, errs.ToAggregate())
	}
	resultObj := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
	if err := scheme.Convert(internalObj, resultObj, nil); err != nil {
		return fmt.Errorf("ValidatingAdmissionPolicyBinding %q: back-conversion error: %w", binding.Name, err)
	}
	*binding = *resultObj
	return nil
}
