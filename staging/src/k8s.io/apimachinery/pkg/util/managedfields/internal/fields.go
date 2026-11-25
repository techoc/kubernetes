/*
Copyright 2018 The Kubernetes Authors.

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

package internal

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// EmptyFields 表示不含任何路径的集合，
// 其表现形式类似于 metav1.Fields{Raw: []byte("{}")}。
// 中文说明：使用包级初始化一次性计算，避免每次重置 managedFields 都重新
// 序列化空集合。
var EmptyFields = func() metav1.FieldsV1 {
	f, err := SetToFields(*fieldpath.NewSet())
	if err != nil {
		panic("should never happen")
	}
	return f
}()

// FieldsToSet 从字段 trie 创建路径集合。
func FieldsToSet(f metav1.FieldsV1) (s fieldpath.Set, err error) {
	err = s.FromJSON(f.GetRawReader())
	return s, err
}

// SetToFields 从路径集合创建字段 trie。
// 中文说明：即使 ToJSON 出错也仍返回已设置的 raw bytes，调用方需同时检查
// err 与返回值。
func SetToFields(s fieldpath.Set) (f metav1.FieldsV1, err error) {
	raw, err := s.ToJSON()
	f.SetRawBytes(raw)
	return f, err
}
