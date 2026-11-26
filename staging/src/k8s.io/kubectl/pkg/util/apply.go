/*
Copyright 2014 The Kubernetes Authors.

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

package util

import (
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

var metadataAccessor = meta.NewAccessor()

// 从注解中取回对象的原始配置，
// 没有注解时返回 nil。
func GetOriginalConfiguration(obj runtime.Object) ([]byte, error) {
	annots, err := metadataAccessor.Annotations(obj)
	if err != nil {
		return nil, err
	}

	if annots == nil {
		return nil, nil
	}

	original, ok := annots[v1.LastAppliedConfigAnnotation]
	if !ok {
		return nil, nil
	}

	return []byte(original), nil
}

// 把对象的原始配置写入注解，
// 供之后计算三方 patch 使用。
func setOriginalConfiguration(obj runtime.Object, original []byte) error {
	if len(original) < 1 {
		return nil
	}

	annots, err := metadataAccessor.Annotations(obj)
	if err != nil {
		return err
	}

	if annots == nil {
		annots = map[string]string{}
	}

	annots[v1.LastAppliedConfigAnnotation] = string(original)
	return metadataAccessor.SetAnnotations(obj, annots)
}

// 取回对象修改后的配置。
// annotate 为真时把结果作为注解嵌入修改后的配置；
// 对象来自命令行输入时使用输入中的版本，
// 否则使用服务端版本。
func GetModifiedConfiguration(obj runtime.Object, annotate bool, codec runtime.Encoder) ([]byte, error) {
	// 先序列化不含注解的对象以避免递归，
	// 再把序列化结果作为注解写入并重新序列化。
	var modified []byte

	// 否则使用对象的服务端版本。
	// 取回对象当前的注解。
	annots, err := metadataAccessor.Annotations(obj)
	if err != nil {
		return nil, err
	}

	if annots == nil {
		annots = map[string]string{}
	}

	original := annots[v1.LastAppliedConfigAnnotation]
	delete(annots, v1.LastAppliedConfigAnnotation)
	if err := metadataAccessor.SetAnnotations(obj, annots); err != nil {
		return nil, err
	}

	modified, err = runtime.Encode(codec, obj)
	if err != nil {
		return nil, err
	}

	if annotate {
		annots[v1.LastAppliedConfigAnnotation] = string(modified)
		if err := metadataAccessor.SetAnnotations(obj, annots); err != nil {
			return nil, err
		}

		modified, err = runtime.Encode(codec, obj)
		if err != nil {
			return nil, err
		}
	}

	// 把对象恢复到原状态。
	annots[v1.LastAppliedConfigAnnotation] = original
	if err := metadataAccessor.SetAnnotations(obj, annots); err != nil {
		return nil, err
	}

	return modified, nil
}

// 已有 last-applied 配置注解时，updateApplyAnnotation
// 调用 CreateApplyAnnotation，否则不做处理。
func updateApplyAnnotation(obj runtime.Object, codec runtime.Encoder) error {
	if original, err := GetOriginalConfiguration(obj); err != nil || len(original) <= 0 {
		return err
	}
	return CreateApplyAnnotation(obj, codec)
}

// 取回对象修改后的配置（不再嵌入注解），
// 再作为注解写回对象。
func CreateApplyAnnotation(obj runtime.Object, codec runtime.Encoder) error {
	modified, err := GetModifiedConfiguration(obj, false, codec)
	if err != nil {
		return err
	}
	return setOriginalConfiguration(obj, modified)
}

// 仅当 createAnnotation 为真时才创建 kubectl apply
// 使用的注解；
// 否则只在注解已存在时更新它。
func CreateOrUpdateAnnotation(createAnnotation bool, obj runtime.Object, codec runtime.Encoder) error {
	if createAnnotation {
		return CreateApplyAnnotation(obj, codec)
	}
	return updateApplyAnnotation(obj, codec)
}
