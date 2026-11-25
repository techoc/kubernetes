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

// Package loader 提供从清单文件加载 webhook 配置的通用功能，
// 负责文件读取、YAML/JSON 解码与通用清单校验。与类型相关的默认化和校验
// （例如基于 scheme 的默认化）由调用方通过 AcceptObjectFunc 回调注入。
//
// 泛型参数 T 让 validating/mutating 两类配置共用同一套文件遍历与校验逻辑，
// 类型相关的默认值与字段校验则通过回调注入，避免在包内做类型分支。
package loader

import (
	"fmt"
	"sort"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission/plugin/manifest"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
)

// AcceptObjectFunc 从解码后的 runtime.Object 中提取带类型的配置项，
// 并完成默认化与校验。返回提取出的配置项；若对象类型无法识别
// 或处理失败则返回错误。
//
// 返回切片是因为一个文件对象可能展开为多个配置项；由调用方决定默认值
// 与校验规则，loader 只负责结构性检查。
type AcceptObjectFunc[T metav1.Object] func(obj runtime.Object) ([]T, error)

// LoadManifests 使用给定的 decoder 与 acceptObject 回调，从目录 dir 中的
// 清单文件加载 webhook 配置。负责文件 I/O、YAML 拆分、解码、v1.List 解包、
// 清单名称校验、webhook 客户端配置校验，以及按名称的确定性排序。
//
// 返回的 hash 覆盖全部文件内容，供上层做变更检测；排序保证相同的文件集合
// 总是产生相同的顺序，便于比较与测试。
func LoadManifests[T metav1.Object](
	dir string,
	decoder runtime.Decoder,
	acceptObject AcceptObjectFunc[T],
) ([]T, string, error) {
	fileDocs, hash, err := manifest.LoadFiles(dir)
	if err != nil {
		return nil, "", err
	}

	configs := make([]T, 0)
	// seenNames 记录名字到文件路径的映射，用于跨文件检测重名。
	seenNames := map[string]string{}

	for _, fd := range fileDocs {
		obj, _, err := decoder.Decode(fd.Doc, nil, nil)
		if err != nil {
			return nil, "", fmt.Errorf("error loading %s: %w", fd.FilePath, err)
		}

		items, err := acceptFileObject(obj, fd.FilePath, seenNames, decoder, acceptObject)
		if err != nil {
			return nil, "", fmt.Errorf("error loading %s: %w", fd.FilePath, err)
		}
		configs = append(configs, items...)
	}

	// 按名字稳定排序，消除文件系统遍历顺序的不确定性。
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].GetName() < configs[j].GetName()
	})

	return configs, hash, nil
}

// acceptFileObject 对解码后的对象做类型分发，包括通用的 v1.List 解包、
// 清单名称校验、配置项校验，以及默认的不支持类型错误。
func acceptFileObject[T metav1.Object](
	obj runtime.Object,
	filePath string,
	seenNames map[string]string,
	decoder runtime.Decoder,
	acceptObject AcceptObjectFunc[T],
) ([]T, error) {
	// 递归处理 List，使生效中的校验对嵌套项同样适用。
	if list, ok := obj.(*metav1.List); ok {
		var allItems []T
		for _, rawItem := range list.Items {
			itemObj, _, err := decoder.Decode(rawItem.Raw, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to decode list item: %w", err)
			}
			items, err := acceptFileObject(itemObj, filePath, seenNames, decoder, acceptObject)
			if err != nil {
				return nil, err
			}
			allItems = append(allItems, items...)
		}
		return allItems, nil
	}

	items, err := acceptObject(obj)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if err := validateAcceptedItem(item, filePath, seenNames); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// validateAcceptedItem 执行清单名称校验与 webhook 客户端配置校验。
func validateAcceptedItem[T metav1.Object](item T, filePath string, seenNames map[string]string) error {
	name := item.GetName()
	if err := manifest.ValidateManifestName(name, filePath, seenNames); err != nil {
		return err
	}
	// 对 webhook 配置类型执行 webhook 客户端配置校验。
	obj, ok := any(item).(runtime.Object)
	if !ok {
		return fmt.Errorf("type %T does not implement runtime.Object", item)
	}
	return validateWebhookClientConfigs(obj, name, filePath)
}

// validateWebhookClientConfigs 校验配置中的所有 webhook 均使用基于 URL 的
// 客户端配置（清单不支持 service 引用）。
// 静态清单没有 Service 解析能力，因此明确拒绝 service 引用，只允许 url。
func validateWebhookClientConfigs(obj runtime.Object, configName, filePath string) error {
	switch c := obj.(type) {
	case *admissionregistrationv1.ValidatingWebhookConfiguration:
		for _, wh := range c.Webhooks {
			if err := ValidateWebhookClientConfig(wh.Name, configName, filePath, wh.ClientConfig); err != nil {
				return err
			}
		}
	case *admissionregistrationv1.MutatingWebhookConfiguration:
		for _, wh := range c.Webhooks {
			if err := ValidateWebhookClientConfig(wh.Name, configName, filePath, wh.ClientConfig); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported webhook configuration type %T in file %q", obj, filePath)
	}
	return nil
}

// ValidateWebhookClientConfig 检查 webhook 是否使用基于 URL 的客户端配置
// （静态清单不支持 service 引用）。
// 错误信息包含 webhook 名、配置名与文件路径，便于在大量清单中定位问题来源。
func ValidateWebhookClientConfig(webhookName, configName, filePath string, cc admissionregistrationv1.WebhookClientConfig) error {
	if cc.Service != nil {
		return fmt.Errorf("webhook %q in %q (file %q): clientConfig.service is not supported for static manifests; use clientConfig.url instead", webhookName, configName, filePath)
	}
	if cc.URL == nil || len(*cc.URL) == 0 {
		return fmt.Errorf("webhook %q in %q (file %q): clientConfig.url is required for static manifests", webhookName, configName, filePath)
	}
	return nil
}

// ValidatingLoadResult 保存从清单文件加载的 validating webhook 配置。
type ValidatingLoadResult struct {
	// Configurations 是已加载的 validating webhook 配置列表。
	Configurations []*admissionregistrationv1.ValidatingWebhookConfiguration
	// Hash 是所有已加载文件的 sha256 哈希，用于变更检测。
	Hash string
}

// MutatingLoadResult 保存从清单文件加载的 mutating webhook 配置。
type MutatingLoadResult struct {
	// Configurations 是已加载的 mutating webhook 配置列表。
	Configurations []*admissionregistrationv1.MutatingWebhookConfiguration
	// Hash 是所有已加载文件的 sha256 哈希，用于变更检测。
	Hash string
}

// BuildValidatingAccessors 从 validating webhook 配置构建 webhook 访问器。
// uid 中拼接出现次数（names[n]），是为了在同一配置内存在同名 webhook 时
// 仍能生成唯一标识，而不是让后者覆盖前者。
func BuildValidatingAccessors(configs []*admissionregistrationv1.ValidatingWebhookConfiguration) []webhook.WebhookAccessor {
	var accessors []webhook.WebhookAccessor
	for _, config := range configs {
		names := map[string]int{}
		for i := range config.Webhooks {
			w := &config.Webhooks[i]
			n := w.Name
			uid := fmt.Sprintf("manifest/%s/%s/%d", config.Name, n, names[n])
			names[n]++
			accessors = append(accessors, webhook.NewValidatingWebhookAccessor(uid, config.Name, w))
		}
	}
	return accessors
}

// BuildMutatingAccessors 从 mutating webhook 配置构建 webhook 访问器。
// 与 validating 版本结构一致，仅构造函数不同。
func BuildMutatingAccessors(configs []*admissionregistrationv1.MutatingWebhookConfiguration) []webhook.WebhookAccessor {
	var accessors []webhook.WebhookAccessor
	for _, config := range configs {
		names := map[string]int{}
		for i := range config.Webhooks {
			w := &config.Webhooks[i]
			n := w.Name
			uid := fmt.Sprintf("manifest/%s/%s/%d", config.Name, n, names[n])
			names[n]++
			accessors = append(accessors, webhook.NewMutatingWebhookAccessor(uid, config.Name, w))
		}
	}
	return accessors
}

// GetWebhookAccessors 返回所有 validating webhook 的 webhook 访问器。
func (r *ValidatingLoadResult) GetWebhookAccessors() []webhook.WebhookAccessor {
	return BuildValidatingAccessors(r.Configurations)
}

// GetWebhookAccessors 返回所有 mutating webhook 的 webhook 访问器。
func (r *MutatingLoadResult) GetWebhookAccessors() []webhook.WebhookAccessor {
	return BuildMutatingAccessors(r.Configurations)
}
