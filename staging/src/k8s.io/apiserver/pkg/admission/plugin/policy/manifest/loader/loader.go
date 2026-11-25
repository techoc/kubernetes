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

// Package loader 提供从清单文件加载策略与绑定配置的通用功能，
// 负责文件读取、YAML/JSON 解码、清单名称校验（后缀 + 唯一性）、
// 针对策略的清单约束以及绑定引用检查。与类型相关的默认化和校验
// （例如基于 scheme 的默认化）由调用方通过 AcceptObjectFunc 回调注入。
//
// 本包刻意不绑定具体 API 类型，VAP/MAP 各自的校验逻辑通过回调注入，
// 从而让静态清单加载逻辑可以同时服务两者，也便于测试用最小 scheme。
package loader

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission/plugin/manifest"
)

// ErrUnrecognizedType 由 AcceptObjectFunc 在对象类型未被处理时返回，
// 使得配置了多个 accept 函数的调用方可以尝试下一个。
//
// 这是一个哨兵错误，必须用 errors.Is 判断；调用方据此区分“类型不匹配”
// 与真正的解析失败。
var ErrUnrecognizedType = fmt.Errorf("unrecognized resource type")

// AcceptObjectFunc 从解码后的 runtime.Object 中提取带类型的条目，
// 并完成默认化与校验；若该函数未处理此对象类型，返回 ErrUnrecognizedType。
//
// 返回切片是为了兼容一个对象展开为多个条目的情况（例如列表或组合类型）。
type AcceptObjectFunc[T metav1.Object] func(obj runtime.Object) ([]T, error)

// LoadPolicyManifests 是一个泛型辅助函数，从目录加载策略与绑定清单。
// 负责文件 I/O、解码、v1.List 解包、清单名称校验、针对策略的清单约束、
// 绑定引用检查，以及按名称的确定性排序。decoder 应由调用方按合适的
// scheme 创建（例如 kube-apiserver 安装完整的内部类型，其他使用者
// 可用仅含 v1 的最小 scheme）。
//
// 返回的 hash 覆盖目录内容，用于判断清单是否变化；排序保证输出稳定，
// 使策略执行顺序可复现。任一文件出错都会整体失败——静态清单属于启动期配置，
// 部分加载会让安全策略静默缺失。
func LoadPolicyManifests[P, B metav1.Object](
	dir string,
	decoder runtime.Decoder,
	acceptPolicy AcceptObjectFunc[P],
	acceptBinding AcceptObjectFunc[B],
	getBindingPolicyName func(B) string,
) ([]P, []B, string, error) {
	fileDocs, hash, err := manifest.LoadFiles(dir)
	if err != nil {
		return nil, nil, "", err
	}

	policies := make([]P, 0)
	bindings := make([]B, 0)
	// 记录名字到文件路径的映射，用于报错时指出重复定义的位置。
	seenPolicyNames := map[string]string{}
	seenBindingNames := map[string]string{}

	for _, fd := range fileDocs {
		obj, _, err := decoder.Decode(fd.Doc, nil, nil)
		if err != nil {
			return nil, nil, "", fmt.Errorf("error loading %s: %w", fd.FilePath, err)
		}

		newPolicies, newBindings, err := acceptFileObject(obj, fd.FilePath, seenPolicyNames, seenBindingNames, decoder, acceptPolicy, acceptBinding)
		if err != nil {
			return nil, nil, "", fmt.Errorf("error loading %s: %w", fd.FilePath, err)
		}
		policies = append(policies, newPolicies...)
		bindings = append(bindings, newBindings...)
	}

	// 引用完整性必须整体校验：binding 可以引用后续文件中的 policy。
	if err := validateBindingReferences(policies, bindings, getBindingPolicyName); err != nil {
		return nil, nil, "", err
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].GetName() < policies[j].GetName()
	})
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].GetName() < bindings[j].GetName()
	})

	return policies, bindings, hash, nil
}

// acceptFileObject 对解码后的对象做类型分发，包括通用的 v1.List 解包、
// 清单名称校验、针对策略的清单约束校验，以及默认的不支持类型错误。
func acceptFileObject[P, B metav1.Object](
	obj runtime.Object,
	filePath string,
	seenPolicyNames, seenBindingNames map[string]string,
	decoder runtime.Decoder,
	acceptPolicy AcceptObjectFunc[P],
	acceptBinding AcceptObjectFunc[B],
) ([]P, []B, error) {
	// v1.List 的条目是 RawExtension，需要再次解码；递归复用同一套校验。
	if list, ok := obj.(*metav1.List); ok {
		var allPolicies []P
		var allBindings []B
		for _, rawItem := range list.Items {
			itemObj, _, err := decoder.Decode(rawItem.Raw, nil, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to decode list item: %w", err)
			}
			p, b, err := acceptFileObject(itemObj, filePath, seenPolicyNames, seenBindingNames, decoder, acceptPolicy, acceptBinding)
			if err != nil {
				return nil, nil, err
			}
			allPolicies = append(allPolicies, p...)
			allBindings = append(allBindings, b...)
		}
		return allPolicies, allBindings, nil
	}

	// 先试 policy 再试 binding；只有 ErrUnrecognizedType 才继续尝试下一种，
	// 其他错误（默认值/校验失败）直接上抛。
	policies, pErr := acceptPolicy(obj)
	if pErr != nil && !errors.Is(pErr, ErrUnrecognizedType) {
		return nil, nil, pErr
	}
	if pErr == nil {
		for _, item := range policies {
			if err := validateAcceptedItem(item, filePath, seenPolicyNames); err != nil {
				return nil, nil, err
			}
		}
		return policies, nil, nil
	}

	// 再尝试 binding
	bindings, bErr := acceptBinding(obj)
	if bErr != nil && !errors.Is(bErr, ErrUnrecognizedType) {
		return nil, nil, bErr
	}
	if bErr == nil {
		for _, item := range bindings {
			if err := validateAcceptedItem(item, filePath, seenBindingNames); err != nil {
				return nil, nil, err
			}
		}
		return nil, bindings, nil
	}

	// 两者都无法识别该类型
	return nil, nil, fmt.Errorf("unsupported resource type %T", obj)
}

// validateAcceptedItem 执行清单名称校验与针对策略的清单约束校验。
func validateAcceptedItem[T metav1.Object](item T, filePath string, seenNames map[string]string) error {
	name := item.GetName()
	if err := manifest.ValidateManifestName(name, filePath, seenNames); err != nil {
		return err
	}
	// 约束校验按具体类型分支，因此需要 runtime.Object；metav1.Object 本身不保证这一点。
	obj, ok := any(item).(runtime.Object)
	if !ok {
		return fmt.Errorf("type %T does not implement runtime.Object", item)
	}
	return validatePolicyManifestConstraints(obj, name)
}

// validatePolicyManifestConstraints 校验策略与绑定类型的清单专属约束
// （paramKind、paramRef、policyName 后缀）。
//
// 静态清单没有 API 写入路径去解析 paramKind/paramRef 引用，
// 因此这两项在清单模式下直接拒绝，避免出现“配置被接受但永不生效”。
func validatePolicyManifestConstraints(obj runtime.Object, name string) error {
	switch c := obj.(type) {
	case *admissionregistrationv1.ValidatingAdmissionPolicy:
		if c.Spec.ParamKind != nil {
			return fmt.Errorf("ValidatingAdmissionPolicy %q: spec.paramKind is not supported for static manifests", name)
		}
	case *admissionregistrationv1.ValidatingAdmissionPolicyBinding:
		if err := validateBindingConstraints(name, c.Spec.PolicyName, c.Spec.ParamRef != nil); err != nil {
			return err
		}
	case *admissionregistrationv1.MutatingAdmissionPolicy:
		if c.Spec.ParamKind != nil {
			return fmt.Errorf("MutatingAdmissionPolicy %q: spec.paramKind is not supported for static manifests", name)
		}
	case *admissionregistrationv1.MutatingAdmissionPolicyBinding:
		if err := validateBindingConstraints(name, c.Spec.PolicyName, c.Spec.ParamRef != nil); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported policy type %T", obj)
	}
	return nil
}

// validateBindingConstraints 检查 binding 清单的通用约束：
// policyName 必须非空、带静态后缀，且 paramRef 必须为 nil。
//
// 后缀检查用于防止清单 binding 与 API binding 混淆；policyName 为空则
// binding 没有意义，尽早报错。
func validateBindingConstraints(bindingName, policyName string, hasParamRef bool) error {
	if len(policyName) == 0 {
		return fmt.Errorf("binding %q must reference a policy (spec.policyName)", bindingName)
	}
	if !strings.HasSuffix(policyName, manifest.StaticConfigSuffix) {
		return fmt.Errorf("binding %q: spec.policyName %q must end with %q", bindingName, policyName, manifest.StaticConfigSuffix)
	}
	if hasParamRef {
		return fmt.Errorf("binding %q: spec.paramRef is not supported for static manifests", bindingName)
	}
	return nil
}

// 只在清单集合内校验，静态 binding 不允许引用 API 中定义的策略，
// 以保证“文件即事实来源”且重载结果可预测。
func validateBindingReferences[P, B metav1.Object](policies []P, bindings []B, getBindingPolicyName func(B) string) error {
	policyNames := sets.New[string]()
	for _, policy := range policies {
		policyNames.Insert(policy.GetName())
	}
	for _, binding := range bindings {
		if !policyNames.Has(getBindingPolicyName(binding)) {
			return fmt.Errorf("binding %q references policy %q which does not exist in the manifest directory", binding.GetName(), getBindingPolicyName(binding))
		}
	}
	return nil
}
