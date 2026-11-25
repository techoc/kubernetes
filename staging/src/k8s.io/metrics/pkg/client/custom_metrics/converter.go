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

package custom_metrics

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"

	cmint "k8s.io/metrics/pkg/apis/custom_metrics"
	cmv1beta1 "k8s.io/metrics/pkg/apis/custom_metrics/v1beta1"
	cmv1beta2 "k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
	"k8s.io/metrics/pkg/client/custom_metrics/scheme"
)

var (
	// MetricVersions 是转换器接受的自定义指标版本集合。
	// 顺序不影响解码，但按新旧排列便于人读；内部版本也在此列，供兼容旧适配器使用。
	MetricVersions = []schema.GroupVersion{
		cmv1beta2.SchemeGroupVersion,
		cmv1beta1.SchemeGroupVersion,
		cmint.SchemeGroupVersion,
	}
)

// MetricConverter 在自定义指标 API 的外部版本之间转换 MetricValue。
// 本类型把 scheme、codecs、版本器打包，避免调用方各自拼一套解码/转换流程。
type MetricConverter struct {
	scheme            *runtime.Scheme
	codecs            serializer.CodecFactory
	internalVersioner runtime.GroupVersioner
}

// NewMetricConverter 创建可在自定义指标 API 不同版本间转换对象的 MetricConverter。
// 多版本器把三个 group 名收敛到内部版本，因为它们的 Kind 集合相同、仅 group 不同。
func NewMetricConverter() *MetricConverter {
	return &MetricConverter{
		scheme: scheme.Scheme,
		codecs: serializer.NewCodecFactory(scheme.Scheme),
		internalVersioner: runtime.NewMultiGroupVersioner(
			scheme.SchemeGroupVersion,
			schema.GroupKind{Group: cmint.GroupName, Kind: ""},
			schema.GroupKind{Group: cmv1beta1.GroupName, Kind: ""},
			schema.GroupKind{Group: cmv1beta2.GroupName, Kind: ""},
		),
	}
}

// Scheme 返回此指标转换器使用的 scheme。
func (c *MetricConverter) Scheme() *runtime.Scheme {
	return c.scheme
}

// Codecs 返回此指标转换器使用的编解码器。
func (c *MetricConverter) Codecs() serializer.CodecFactory {
	return c.codecs
}

// ConvertListOptionsToVersion 把一组 MetricListOptions 转换为指定的 GroupVersion。
// 先转内部版本再转目标版本，是为了让 list option 字段在各版本间有单一转换路径。
func (c *MetricConverter) ConvertListOptionsToVersion(opts *cmint.MetricListOptions, version schema.GroupVersion) (runtime.Object, error) {
	paramObj, err := c.UnsafeConvertToVersionVia(opts, version)
	if err != nil {
		return nil, err
	}
	return paramObj, nil
}

// ConvertResultToVersion 把 REST 查询结果转换为指定的 GroupVersion。
// 用 UniversalDecoder 接收所有已知指标版本，避免适配器仍返回旧版本时直接报错。
func (c *MetricConverter) ConvertResultToVersion(res rest.Result, gv schema.GroupVersion) (runtime.Object, error) {
	if err := res.Error(); err != nil {
		return nil, err
	}

	metricBytes, err := res.Raw()
	if err != nil {
		return nil, err
	}

	decoder := c.codecs.UniversalDecoder(MetricVersions...)
	rawMetricObj, err := runtime.Decode(decoder, metricBytes)
	if err != nil {
		return nil, err
	}

	metricObj, err := c.UnsafeConvertToVersionVia(rawMetricObj, gv)
	if err != nil {
		return nil, err
	}
	return metricObj, nil
}

// UnsafeConvertToVersionVia 类似 Scheme.UnsafeConvertToVersion，但先经内部版本中转，再转到目标外部版本。
// 这样客户端内部可用 v1beta2，同时保留对现有自定义指标适配器的兼容；
// 直接从 v1beta2 转到 v1beta1 会漏掉内部版本上的兼容处理。
func (c *MetricConverter) UnsafeConvertToVersionVia(obj runtime.Object, externalVersion schema.GroupVersion) (runtime.Object, error) {
	objInt, err := c.scheme.UnsafeConvertToVersion(obj, schema.GroupVersion{Group: externalVersion.Group, Version: runtime.APIVersionInternal})
	if err != nil {
		return nil, fmt.Errorf("failed to convert the given object to the internal version: %v", err)
	}

	objExt, err := c.scheme.UnsafeConvertToVersion(objInt, externalVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to convert the given object back to the external version: %v", err)
	}

	return objExt, err
}
