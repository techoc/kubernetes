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

// Package devicemetadata provides consumer-side utilities for reading and decoding
// DRA device metadata files. It is intended for any Go program that needs
// to consume metadata written by a DRA kubelet plugin, including sidecars,
// monitoring tools, and in-container agents.
// Package devicemetadata 提供消费侧工具，用于读取并解码 DRA 设备元数据文件。
// 适用于任何需要消费 DRA kubelet 插件所写元数据的 Go 程序，包括 sidecar、
// 监控工具以及容器内代理。
package devicemetadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/dynamic-resource-allocation/api/metadata"
	"k8s.io/dynamic-resource-allocation/api/metadata/install"
	metadatav1alpha1 "k8s.io/dynamic-resource-allocation/api/metadata/v1alpha1"
	metadatav1beta1 "k8s.io/dynamic-resource-allocation/api/metadata/v1beta1"
)

// scheme 与 codecFactory 是基于已注册元数据 API 构建的全局 scheme 和编解码工厂，
// 包内所有解码操作共享它们，避免重复初始化。
var (
	scheme       = install.NewScheme()
	codecFactory = runtimeserializer.NewCodecFactory(scheme)
)

// DecodeMetadataFromStream reads the first compatible object from a DRA
// device metadata stream. A metadata file may contain the same data encoded
// in multiple API versions (see k8s.io/dynamic-resource-allocation/api/metadata
// for the file format); this function tries each in order and populates dest
// with the first one it can successfully decode and convert.
//
// Entries whose apiVersion is not registered in the scheme (e.g. a newer
// version written by an upgraded driver) are skipped silently so that a
// driver upgrade does not break older consumers. Any other failure is
// fatal. dest is only populated on a nil return.
//
// dest must be a pointer to a type registered in the metadata scheme (e.g.,
// *v1alpha1.DeviceMetadata or *metadata.DeviceMetadata). The internal type
// is recommended because it can be decoded with less conversions and source
// code does not need to be updated when new API versions get added or (at
// some point) removed.
//
// By default, the input data does not get validated before converting it to the
// desired type. Validation can be enabled with [DecodeMetadataWithValidationResult]
// which will store a validation error separately, if there is any.
// Callers which want to know what was decoded can request to get
// the GVK stored via [DecodeMetadataStoreGVKResult].
// DecodeMetadataFromStream 从 DRA 设备元数据 JSON 流中读取第一个兼容的对象。
// 元数据文件可能用多个 API 版本编码同一份数据，本函数按出现顺序逐个尝试，
// 将首个能成功解码并转换的对象写入 dest。
// apiVersion 未在 scheme 中注册的对象（例如升级后的驱动写入的新版本）会被静默跳过，
// 以免驱动升级破坏旧版消费者；其他任何失败均为致命错误。
// 只有返回 nil 时 dest 才会被写入。
// dest 必须是指向已在元数据 scheme 中注册的类型的指针
// （例如 *v1alpha1.DeviceMetadata 或 *metadata.DeviceMetadata）。
// 推荐使用内部类型，因为它转换次数更少，且新增或移除 API 版本时无需改动源代码。
// 默认情况下，数据在转换为目标类型前不做校验；可通过
// [DecodeMetadataWithValidationResult] 启用校验，校验错误会被单独存放。
// 想了解实际解码内容时，可用 [DecodeMetadataStoreGVKResult] 获取 GVK。
func DecodeMetadataFromStream(decoder *json.Decoder, dest runtime.Object, opts ...DecodeMetadataOption) error {
	// 初始化选项结构体并依次应用调用方传入的各个解码选项。
	var options decodeMetadataOptions
	for _, opt := range opts {
		opt(&options)
	}

	// 确定 dest 的类型在 scheme 中对应的 GVK，失败则无法继续转换。
	gvks, _, err := scheme.ObjectKinds(dest)
	if err != nil {
		return fmt.Errorf("determine target type: %w", err)
	}

	// 获取通用反序列化器，它能依据对象自带的 apiVersion/kind 完成解码。
	deserializer := codecFactory.UniversalDeserializer()

	// 记录流中遇到的未知版本，便于最后给出更有信息量的错误。
	var unknownVersions []string
	// 逐个读取流中的 JSON 对象，处理到第一个成功的对象为止。
	for decoder.More() {
		var raw json.RawMessage
		// 先解码为原始消息，再交给通用反序列化器识别其中的 GVK。
		if err := decoder.Decode(&raw); err != nil {
			return fmt.Errorf("read metadata object from stream: %w", err)
		}
		// 依据原始消息里的 GVK 反序列化为对应的版本化对象。
		obj, gvk, err := deserializer.Decode(raw, nil, nil)
		if err != nil {
			// 连 GVK 都无法识别，属于无效输入，直接报错。
			if gvk == nil || gvk.Version == "" {
				return fmt.Errorf("decode metadata object: %w", err)
			}
			// 仅当错误是“类型未注册”且版本确实未注册时，才视为可跳过的未知版本。
			if runtime.IsNotRegisteredError(err) && !scheme.IsVersionRegistered(gvk.GroupVersion()) {
				unknownVersions = append(unknownVersions, gvk.GroupVersion().String())
				// 静默跳过驱动写入的、本消费者尚不认识的新版本。
				continue
			}
			// 其他解码失败（如已注册版本但内容不合法）是致命错误。
			return fmt.Errorf("decode %s: %w", gvk.GroupVersion(), err)
		}

		// 调用方要求校验时，按对象的具体版本执行对应的声明式校验。
		if options.validationResult != nil {
			// Error wrapping is left to the caller, they know that a validation error
			// is because of a failed validation and can add the GKV if they want to.
			switch obj := obj.(type) {
			case *metadatav1alpha1.DeviceMetadata:
				// 对 v1alpha1 对象按 Create 操作执行声明式校验并聚合成单个错误。
				if err := metadatav1alpha1.Validate_DeviceMetadata(context.Background(), operation.Operation{Type: operation.Create}, nil, obj, nil).ToAggregate(); err != nil {
					*options.validationResult = err
				}
			case *metadatav1beta1.DeviceMetadata:
				// 对 v1beta1 对象执行同样的声明式校验。
				if err := metadatav1beta1.Validate_DeviceMetadata(context.Background(), operation.Operation{Type: operation.Create}, nil, obj, nil).ToAggregate(); err != nil {
					*options.validationResult = err
				}
			default:
				// 未知版本化类型无法校验，记录相应错误。
				*options.validationResult = fmt.Errorf("cannot validate object of type %T", obj)
			}
		}

		// 只向 dest 写入一次，采用最可能成功的转换方式。
		// 因为不确定第二次尝试能否正确覆盖第一次可能产生的部分转换结果。
		// Only write into dest once, using the method that is most likely to work.
		// It's uncertain if partial conversion would be properly overwritten by
		// a second attempt.
		if reflect.TypeOf(dest) != reflect.TypeFor[*metadata.DeviceMetadata]() &&
			reflect.TypeOf(dest) != reflect.TypeOf(obj) {
			// 目标类型既不是内部版本，又与源对象类型不同，
			// 因此尝试先转成内部版本，再间接转到目标类型。
			// The destination type is not the internal version and
			// the two objects have different types, so maybe we
			// can convert indirectly.
			var intermediate metadata.DeviceMetadata
			if err := scheme.Convert(obj, &intermediate, nil); err != nil {
				return fmt.Errorf("convert %s into internal version: %w", gvk, err)
			}
			if err := scheme.Convert(&intermediate, dest, nil); err != nil {
				return fmt.Errorf("convert internal version into %s: %w", gvk, err)
			}
		} else {
			// 目标就是内部版本或与源类型相同，直接转换一次即可。
			if err := scheme.Convert(obj, dest, nil); err != nil {
				return fmt.Errorf("convert %s: %w", gvk, err)
			}
		}

		// scheme.Convert 不会传播 TypeMeta。对版本化类型显式设置它，
		// 让调用方看到期望的 apiVersion/kind；内部类型保留空的 TypeMeta。
		// scheme.Convert does not propagate TypeMeta. Set it explicitly
		// for versioned types so that callers see the expected
		// apiVersion/kind. Internal types keep an empty TypeMeta.
		if gvks[0].Version != runtime.APIVersionInternal {
			dest.GetObjectKind().SetGroupVersionKind(gvks[0])
		}

		// 调用方要求时，把实际解码出的 GVK 写回指定位置。
		if options.gvkResult != nil {
			*options.gvkResult = *gvk
		}
		// 已成功解码并填充 dest，正常返回。
		return nil
	}
	// 流中没有可兼容对象时，区分“空流”与“仅含未知版本”两种情况报错。
	if len(unknownVersions) == 0 {
		return fmt.Errorf("no metadata objects found in stream")
	}
	return fmt.Errorf("no compatible metadata version found in stream (unknown versions: %s)", strings.Join(unknownVersions, ", "))
}

// DecodeMetadataOption 是用于配置 DecodeMetadataFromStream 行为的函数选项类型。
type DecodeMetadataOption func(*decodeMetadataOptions)

// DecodeMetadataWithValidationResult enables or disables validation in [DecodeMetadataFromStream].
// In case of a validation error, it'll be stored where requested here.
// Providing a nil pointer disables validation.
// The error comes straight from the underlying declarative validation with no wrapping
// or explains why validation is not possible.
// DecodeMetadataWithValidationResult 用于开启或关闭解码时的校验。
// 发生校验错误时，错误会被写入 err 指向的位置；传入 nil 指针表示关闭校验。
// 返回的错误直接来自底层声明式校验，不做任何包装，也不解释无法校验的原因。
func DecodeMetadataWithValidationResult(err *error) DecodeMetadataOption {
	return func(options *decodeMetadataOptions) {
		// 记录校验结果的存放位置，nil 即表示不启用校验。
		options.validationResult = err
	}
}

// DecodeMetadataStoreGVKResult tells [DecodeMetadataFromStream] where to store
// the group/version/kind that was decoded from the input in case of success.
// DecodeMetadataStoreGVKResult 用于告知 DecodeMetadataFromStream，
// 解码成功时把输入对象对应的 GVK（组/版本/类别）存放到 gvk 指向的位置。
func DecodeMetadataStoreGVKResult(gvk *schema.GroupVersionKind) DecodeMetadataOption {
	return func(options *decodeMetadataOptions) {
		// 记录 GVK 的存放位置，供解码成功后写入。
		options.gvkResult = gvk
	}
}

// decodeMetadataOptions 保存解码过程的可选配置：
// validationResult 指向校验错误的存放位置（nil 表示不校验），
// gvkResult 指向解码所得 GVK 的存放位置（nil 表示不记录）。
type decodeMetadataOptions struct {
	validationResult *error
	gvkResult        *schema.GroupVersionKind
}

// ReadResourceClaimMetadataWithDriverName reads and decodes the metadata file
// for a directly referenced ResourceClaim from a specific driver.
// driverName identifies which driver's metadata to read, since each driver
// writes a separate file per request.
// ReadResourceClaimMetadataWithDriverName 读取并解码指定驱动的、
// 直接引用的 ResourceClaim 的元数据文件。
// driverName 指明要读取哪个驱动的文件，因为每个驱动会为每个请求单独写一个文件。
func ReadResourceClaimMetadataWithDriverName(driverName, claimName, requestName string) (*metadata.DeviceMetadata, error) {
	return readMetadata(metadata.ResourceClaimFilePath(driverName, claimName, requestName))
}

// ReadResourceClaimTemplateMetadataWithDriverName reads and decodes the
// metadata file for a template-generated claim from a specific driver.
// podClaimName is the pod-local name (pod.spec.resourceClaims[].name).
// driverName identifies which driver's metadata to read, since each driver
// writes a separate file per request.
// ReadResourceClaimTemplateMetadataWithDriverName 读取并解码指定驱动的、
// 模板生成型 ResourceClaim 的元数据文件。
// podClaimName 是 Pod 本地名称（pod.spec.resourceClaims[].name）。
// driverName 指明要读取哪个驱动的文件，因为每个驱动会为每个请求单独写一个文件。
func ReadResourceClaimTemplateMetadataWithDriverName(driverName, podClaimName, requestName string) (*metadata.DeviceMetadata, error) {
	return readMetadata(metadata.ResourceClaimTemplateFilePath(driverName, podClaimName, requestName))
}

// ReadResourceClaimMetadata reads and decodes all metadata files for a
// directly referenced ResourceClaim request, regardless of which driver(s)
// wrote them. It globs the request directory for all *-metadata.json files
// and merges the results into a single [metadata.DeviceMetadata].
// ReadResourceClaimMetadata 读取并解码直接引用的 ResourceClaim 请求的所有元数据文件，
// 不区分是哪个驱动写入的。它在请求目录下匹配所有 *-metadata.json 文件，
// 并把结果合并成单个 [metadata.DeviceMetadata]。
func ReadResourceClaimMetadata(claimName, requestName string) (*metadata.DeviceMetadata, error) {
	dir := filepath.Join(metadata.ContainerDir, metadata.ResourceClaimsSubDir, claimName, requestName)
	return readRequestDir(dir)
}

// ReadResourceClaimTemplateMetadata reads and decodes all metadata files
// for a template-generated claim request, regardless of which driver(s) wrote
// them. It globs the request directory for all *-metadata.json files and
// merges the results into a single [metadata.DeviceMetadata].
// ReadResourceClaimTemplateMetadata 读取并解码模板生成型 ResourceClaim 请求的
// 所有元数据文件，不区分是哪个驱动写入的。它在请求目录下匹配所有
// *-metadata.json 文件，并把结果合并成单个 [metadata.DeviceMetadata]。
func ReadResourceClaimTemplateMetadata(podClaimName, requestName string) (*metadata.DeviceMetadata, error) {
	dir := filepath.Join(metadata.ContainerDir, metadata.ResourceClaimTemplatesSubDir, podClaimName, requestName)
	return readRequestDir(dir)
}

// readRequestDir 读取目录中的所有元数据文件，并将它们合并成一个 DeviceMetadata。
func readRequestDir(dir string) (*metadata.DeviceMetadata, error) {
	// 按元数据文件后缀匹配目录中的所有相关文件。
	matches, err := filepath.Glob(filepath.Join(dir, "*"+metadata.MetadataFileSuffix))
	if err != nil {
		return nil, fmt.Errorf("glob metadata files in %s: %w", dir, err)
	}
	// 没有任何匹配文件时直接报错。
	if len(matches) == 0 {
		return nil, fmt.Errorf("no metadata files found in %s", dir)
	}

	// merged 聚合所有文件的元数据；mergedRequestIndex 记录请求名到 merged.Requests 下标的映射。
	var merged metadata.DeviceMetadata
	mergedRequestIndex := map[string]int{}
	// 逐个读取并解码匹配到的文件。
	for _, path := range matches {
		dm, err := readMetadata(path)
		if err != nil {
			return nil, err
		}
		// PodClaimName 只取第一个非空值。
		if merged.PodClaimName == nil {
			merged.PodClaimName = dm.PodClaimName
		}
		// 按请求名合并：同名请求的设备追加到一起，新请求则整体加入。
		for _, request := range dm.Requests {
			if idx, ok := mergedRequestIndex[request.Name]; ok {
				merged.Requests[idx].Devices = append(merged.Requests[idx].Devices, request.Devices...)
				continue
			}
			// 新请求：记录其下标后追加到结果中。
			mergedRequestIndex[request.Name] = len(merged.Requests)
			merged.Requests = append(merged.Requests, request)
		}
	}
	return &merged, nil
}

// readMetadata 打开并解码指定路径的单个元数据文件为内部类型。
func readMetadata(path string) (*metadata.DeviceMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open metadata file: %w", err)
	}

	// 用 JSON 流解码器解码文件内容。
	var dm metadata.DeviceMetadata
	decodeErr := DecodeMetadataFromStream(json.NewDecoder(f), &dm)
	// 关闭文件；仅当解码本身成功时才把关闭失败当作错误返回。
	if closeErr := f.Close(); closeErr != nil && decodeErr == nil {
		return nil, fmt.Errorf("close metadata file: %w", closeErr)
	}
	if decodeErr != nil {
		return nil, fmt.Errorf("%s: %w", path, decodeErr)
	}
	return &dm, nil
}
