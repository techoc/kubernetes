/*
Copyright 2020 The Kubernetes Authors.

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

// Package metrics 定义 envelope 加密（KMS）相关的指标：DEK 缓存命中、
// KMS 调用延迟、keyID 使用情况等。
//
// 安全注意：keyID 与 apiserver 身份都只以 SHA-256 哈希形式作为标签，
// 避免把密钥标识或节点标识原样暴露到指标端点。
package metrics

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/utils/lru"
)

const (
	namespace        = "apiserver"
	subsystem        = "envelope_encryption"
	FromStorageLabel = "from_storage"
	ToStorageLabel   = "to_storage"
)

// metricLabels 用作 LRU 的 key：它唯一标识一组指标标签，value 无意义，
// 缓存实际充当“已注册标签集合”，用于在基数超限时回收旧标签。
type metricLabels struct {
	transformationType string
	providerName       string
	keyIDHash          string
	apiServerIDHash    string
}

/*
 * By default, all the following metrics are defined as falling under
 * ALPHA stability level https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * Promoting the stability level of the metric is a responsibility of the component owner, since it
 * involves explicitly acknowledging support for the metric across multiple releases, in accordance with
 * the metric stability policy.
 */
var (
	lockLastFromStorage   sync.Mutex
	lockLastToStorage     sync.Mutex
	lockRecordKeyID       sync.Mutex
	lockRecordKeyIDStatus sync.Mutex

	lastFromStorage                                 time.Time
	lastToStorage                                   time.Time
	keyIDHashTotalMetricLabels                      *lru.Cache
	keyIDHashStatusLastTimestampSecondsMetricLabels *lru.Cache
	cacheSize                                       = 100

	// 此指标仅用于 KMS v1 API。
	dekCacheFillPercent = metrics.NewGauge(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dek_cache_fill_percent",
			Help:           "Percent of the cache slots currently occupied by cached DEKs.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// 此指标仅用于 KMS v1 API。
	dekCacheInterArrivals = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dek_cache_inter_arrival_time_seconds",
			Help:           "Time (in seconds) of inter arrival of transformation requests.",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(60, 2, 10),
		},
		[]string{"transformation_type"},
	)

	// 这些指标公开以供单元测试使用。
	KMSOperationsLatencyMetric = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "kms_operations_latency_seconds",
			Help:           "KMS operation duration with gRPC error code status total.",
			StabilityLevel: metrics.ALPHA,
			// 使用自定义 buckets，因为默认 buckets 对 KMS 操作太小。
			// 从 0.1ms 开始，最后一个 bucket 为 [~52s, +Inf)
			Buckets: metrics.ExponentialBuckets(0.0001, 2, 20),
		},
		[]string{"provider_name", "method_name", "grpc_status_code"},
	)

	// keyIDHashTotal 是 keyID 被使用的次数
	// e.g. apiserver_envelope_encryption_key_id_hash_total counter
	// apiserver_envelope_encryption_key_id_hash_total{apiserver_id_hash="sha256",key_id_hash="sha256",
	// provider_name="providerName",transformation_type="from_storage"} 1
	KeyIDHashTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "key_id_hash_total",
			Help:           "Number of times a keyID is used split by transformation type, provider, and apiserver identity.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"transformation_type", "provider_name", "key_id_hash", "apiserver_id_hash"},
	)

	// keyIDHashLastTimestampSeconds 是 keyID 上次使用的时间（秒）
	// e.g. apiserver_envelope_encryption_key_id_hash_last_timestamp_seconds{apiserver_id_hash="sha256",key_id_hash="sha256", provider_name="providerName",transformation_type="from_storage"} 1.674865558833728e+09
	KeyIDHashLastTimestampSeconds = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "key_id_hash_last_timestamp_seconds",
			Help:           "The last time in seconds when a keyID was used.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"transformation_type", "provider_name", "key_id_hash", "apiserver_id_hash"},
	)

	// keyIDHashStatusLastTimestampSeconds 是 Status RPC 上次返回某 keyID 的时间（秒）。
	// e.g. apiserver_envelope_encryption_key_id_hash_status_last_timestamp_seconds{apiserver_id_hash="sha256",key_id_hash="sha256", provider_name="providerName"} 1.674865558833728e+09
	KeyIDHashStatusLastTimestampSeconds = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "key_id_hash_status_last_timestamp_seconds",
			Help:           "The last time in seconds when a keyID was returned by the Status RPC call.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"provider_name", "key_id_hash", "apiserver_id_hash"},
	)

	InvalidKeyIDFromStatusTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "invalid_key_id_from_status_total",
			Help:           "Number of times an invalid keyID is returned by the Status RPC call split by error.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"provider_name", "error"},
	)

	DekSourceCacheSize = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dek_source_cache_size",
			Help:           "Number of records in data encryption key (DEK) source cache. On a restart, this value is an approximation of the number of decrypt RPC calls the server will make to the KMS plugin.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"provider_name"},
	)
)

var registerMetricsFunc sync.Once
var hashPool *sync.Pool

func registerLRUMetrics() {
	// 重新注册时先清空旧缓存，避免上次注册的回调仍引用旧指标对象。
	if keyIDHashTotalMetricLabels != nil {
		keyIDHashTotalMetricLabels.Clear()
	}
	if keyIDHashStatusLastTimestampSecondsMetricLabels != nil {
		keyIDHashStatusLastTimestampSecondsMetricLabels.Clear()
	}

	keyIDHashTotalMetricLabels = lru.NewWithEvictionFunc(cacheSize, func(key lru.Key, _ interface{}) {
		item := key.(metricLabels)
		if deleted := KeyIDHashTotal.DeleteLabelValues(item.transformationType, item.providerName, item.keyIDHash, item.apiServerIDHash); deleted {
			klog.InfoS("Deleted keyIDHashTotalMetricLabels", "transformationType", item.transformationType,
				"providerName", item.providerName, "keyIDHash", item.keyIDHash, "apiServerIDHash", item.apiServerIDHash)
		}
		if deleted := KeyIDHashLastTimestampSeconds.DeleteLabelValues(item.transformationType, item.providerName, item.keyIDHash, item.apiServerIDHash); deleted {
			klog.InfoS("Deleted keyIDHashLastTimestampSecondsMetricLabels", "transformationType", item.transformationType,
				"providerName", item.providerName, "keyIDHash", item.keyIDHash, "apiServerIDHash", item.apiServerIDHash)
		}
	})
	keyIDHashStatusLastTimestampSecondsMetricLabels = lru.NewWithEvictionFunc(cacheSize, func(key lru.Key, _ interface{}) {
		item := key.(metricLabels)
		if deleted := KeyIDHashStatusLastTimestampSeconds.DeleteLabelValues(item.providerName, item.keyIDHash, item.apiServerIDHash); deleted {
			klog.InfoS("Deleted keyIDHashStatusLastTimestampSecondsMetricLabels", "providerName", item.providerName, "keyIDHash", item.keyIDHash, "apiServerIDHash", item.apiServerIDHash)
		}
	})
}

// RegisterMetrics 幂等注册所有指标，并初始化哈希对象池。
// 哈希池复用 sha256 实例，降低高频加密路径上的分配压力。
func RegisterMetrics() {
	registerMetricsFunc.Do(func() {
		registerLRUMetrics()
		hashPool = &sync.Pool{
			New: func() interface{} {
				return sha256.New()
			},
		}
		legacyregistry.MustRegister(dekCacheFillPercent)
		legacyregistry.MustRegister(dekCacheInterArrivals)
		legacyregistry.MustRegister(DekSourceCacheSize)
		legacyregistry.MustRegister(KeyIDHashTotal)
		legacyregistry.MustRegister(KeyIDHashLastTimestampSeconds)
		legacyregistry.MustRegister(KeyIDHashStatusLastTimestampSeconds)
		legacyregistry.MustRegister(InvalidKeyIDFromStatusTotal)
		legacyregistry.MustRegister(KMSOperationsLatencyMetric)
	})
}

// RecordKeyID 记录 KeyID 用于 TransformFromStorage 与 TransformToStorage 的总次数与最近时间（秒）
func RecordKeyID(transformationType, providerName, keyID, apiServerID string) {
	lockRecordKeyID.Lock()
	defer lockRecordKeyID.Unlock()

	keyIDHash, apiServerIDHash := addLabelToCache(keyIDHashTotalMetricLabels, transformationType, providerName, keyID, apiServerID)
	KeyIDHashTotal.WithLabelValues(transformationType, providerName, keyIDHash, apiServerIDHash).Inc()
	KeyIDHashLastTimestampSeconds.WithLabelValues(transformationType, providerName, keyIDHash, apiServerIDHash).SetToCurrentTime()
}

// RecordKeyIDFromStatus 记录 Status RPC 上次返回 KeyID 的时间（秒）。
// 与 RecordKeyID 使用独立的锁和 LRU，因为该指标的标签维度少一个
// transformation_type。
func RecordKeyIDFromStatus(providerName, keyID, apiServerID string) {
	lockRecordKeyIDStatus.Lock()
	defer lockRecordKeyIDStatus.Unlock()

	keyIDHash, apiServerIDHash := addLabelToCache(keyIDHashStatusLastTimestampSecondsMetricLabels, "", providerName, keyID, apiServerID)
	KeyIDHashStatusLastTimestampSeconds.WithLabelValues(providerName, keyIDHash, apiServerIDHash).SetToCurrentTime()
}

func RecordInvalidKeyIDFromStatus(providerName, errCode string) {
	InvalidKeyIDFromStatusTotal.WithLabelValues(providerName, errCode).Inc()
}

// RecordArrival 记录相邻两次转换请求的间隔，用于估算 DEK 缓存命中带来的
// KMS 调用节省。首次请求没有前驱，间隔记为 0。
func RecordArrival(transformationType string, start time.Time) {
	switch transformationType {
	case FromStorageLabel:
		lockLastFromStorage.Lock()
		defer lockLastFromStorage.Unlock()

		if lastFromStorage.IsZero() {
			lastFromStorage = start
		}
		dekCacheInterArrivals.WithLabelValues(transformationType).Observe(start.Sub(lastFromStorage).Seconds())
		lastFromStorage = start
	case ToStorageLabel:
		lockLastToStorage.Lock()
		defer lockLastToStorage.Unlock()

		if lastToStorage.IsZero() {
			lastToStorage = start
		}
		dekCacheInterArrivals.WithLabelValues(transformationType).Observe(start.Sub(lastToStorage).Seconds())
		lastToStorage = start
	}
}

// RecordDekCacheFillPercent 记录 KMS v1 DEK 缓存占用比例。
func RecordDekCacheFillPercent(percent float64) {
	dekCacheFillPercent.Set(percent)
}

// RecordDekSourceCacheSize 记录 KMS v2 的 DEK 来源缓存条目数；
// 重启后该值近似于需要向 KMS 发起的解密调用次数。
func RecordDekSourceCacheSize(providerName string, size int) {
	DekSourceCacheSize.WithLabelValues(providerName).Set(float64(size))
}

// RecordKMSOperationLatency 记录 KMS 操作延迟。
func RecordKMSOperationLatency(providerName, methodName string, duration time.Duration, err error) {
	KMSOperationsLatencyMetric.WithLabelValues(providerName, methodName, getErrorCode(err)).Observe(duration.Seconds())
}

// gRPCError 抽象出能返回 gRPC status 的错误，兼容 status.Error 以及
// 被 fmt.Errorf("%w") 包装后的形式。
type gRPCError interface {
	GRPCStatus() *status.Status
}

// getErrorCode 把错误归一化为 gRPC 状态码字符串；非 gRPC 错误单独标记，
// 以便区分“KMS 返回错误”与“调用前就失败”。
func getErrorCode(err error) string {
	if err == nil {
		return codes.OK.String()
	}

	// 处理 fmt.Errorf 等包装的错误
	var s gRPCError
	if errors.As(err, &s) {
		return s.GRPCStatus().Code().String()
	}

	// 这不是 gRPC 错误，说明操作在调用 gRPC 方法前就失败了，
	// 否则会拿到 gRPC 错误。
	return "unknown-non-grpc"
}

// getHash 返回带 "sha256:" 前缀的十六进制哈希；空输入返回空串，
// 避免为“未配置”的身份生成无意义标签。
func getHash(data string) string {
	if len(data) == 0 {
		return ""
	}
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	h.Write([]byte(data))
	dataHash := fmt.Sprintf("sha256:%x", h.Sum(nil))
	hashPool.Put(h)
	return dataHash
}

// addLabelToCache 把标签组合放入 LRU，返回哈希后的 keyID 与 apiserverID。
// 当缓存淘汰某个组合时，对应的指标标签值会被删除，从而为基数设上限。
func addLabelToCache(c *lru.Cache, transformationType, providerName, keyID, apiServerID string) (string, string) {
	keyIDHash := getHash(keyID)
	apiServerIDHash := getHash(apiServerID)
	c.Add(metricLabels{
		transformationType: transformationType,
		providerName:       providerName,
		keyIDHash:          keyIDHash,
		apiServerIDHash:    apiServerIDHash,
	}, nil) // 值无关紧要，这是集合而非 map
	return keyIDHash, apiServerIDHash
}
