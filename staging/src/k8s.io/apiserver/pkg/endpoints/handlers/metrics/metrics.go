/*
Copyright 2022 The Kubernetes Authors.

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

// Package metrics 暴露 API 请求体大小的直方图指标。
// 请求体大小是准入 webhook 与 etcd 写入成本的主要驱动因素之一，
// 单独度量可用于容量规划与定位超大请求。
package metrics

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// RequestBodyVerb 限定可记录的动词集合，避免高基数标签值。
type RequestBodyVerb string

const (
	// Patch 对应 PATCH 请求。
	Patch RequestBodyVerb = "patch"
	// Delete 对应 DELETE 请求。
	Delete RequestBodyVerb = "delete"
	// Update 对应 PUT 请求。
	Update RequestBodyVerb = "update"
	// Create 对应 POST 请求。
	Create RequestBodyVerb = "create"
	// DeleteCollection 对应批量删除请求。
	DeleteCollection RequestBodyVerb = "delete_collection"
)

var (
	// RequestBodySizes 记录按资源与动词拆分的请求体字节数。
	RequestBodySizes = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: "apiserver",
			Name:      "request_body_size_bytes",
			Help:      "Apiserver request body size in bytes broken out by resource and verb.",
			// we use 0.05 KB as the smallest bucket with 0.1 KB increments up to the
			// apiserver limit.
			// 桶从 50KB 起、每档 100KB、共 31 档，覆盖到约 3MB 的默认请求上限。
			Buckets:        metrics.LinearBuckets(50000, 100000, 31),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "resource", "verb"},
	)
)

// registerMetrics 保证重复调用 Register 不会因重复注册而 panic。
var registerMetrics sync.Once

// Register 注册全部指标；并发安全，重复调用无副作用。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(RequestBodySizes)
	})
}

// RecordRequestBodySize 记录一次请求体大小。
// group/resource/verb 作为标签；调用方需保证 verb 取自上方的受控枚举。
func RecordRequestBodySize(ctx context.Context, groupResource schema.GroupResource, verb RequestBodyVerb, size int) {
	RequestBodySizes.WithContext(ctx).WithLabelValues(groupResource.Group, groupResource.Resource, string(verb)).Observe(float64(size))
}
