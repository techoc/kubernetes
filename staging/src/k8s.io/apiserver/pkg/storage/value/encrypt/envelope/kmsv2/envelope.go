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

// Package kmsv2 transforms values for storage at rest using a Envelope v2 provider
//
// 信封加密：数据用本地生成的 DEK 加密，DEK 再由外部 KMS 加密后随数据一起存放。
// 好处是批量数据加解密不经过 KMS，只有 DEK 轮换时才调用远端；代价是必须严格校验
// KMS 返回的密钥标识与注解，防止被替换成攻击者控制的密钥。
package kmsv2

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"time"
	"unsafe"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/crypto/cryptobyte"
	"google.golang.org/protobuf/proto"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage/value"
	aestransformer "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
	kmstypes "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2/v2"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope/metrics"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
	kmsservice "k8s.io/kms/pkg/service"
	"k8s.io/utils/clock"
)

func init() {
	value.RegisterMetrics()
	metrics.RegisterMetrics()
}

// nowFuncPerKMS allows us to swap the NowFunc for KMS providers in tests
// Note: it cannot be set by an end user
var nowFuncPerKMS sync.Map // map[string]func() time.Time, KMS name -> NowFunc

// SetNowFuncForTests should only be called in tests to swap the NowFunc for KMS providers
// Caller must guarantee that all KMS providers have distinct names across all tests.
//
// 返回的清理函数必须 defer 调用，否则会污染后续测试的时钟。
func SetNowFuncForTests(kmsName string, fn func() time.Time) func() {
	if len(kmsName) == 0 { // guarantee that GetNowFunc("") returns the default value
		panic("empty KMS name used in test")
	}
	nowFuncPerKMS.Store(kmsName, fn)
	return func() { nowFuncPerKMS.Delete(kmsName) }
}

// GetNowFunc 返回给定 KMS provider 名称对应的时间函数；未注册时回退到 time.Now，
// 保证生产路径永远可用。
func GetNowFunc(kmsName string) func() time.Time {
	nowFunc, ok := nowFuncPerKMS.Load(kmsName)
	if !ok {
		return time.Now
	}
	return nowFunc.(func() time.Time)
}

const (
	// KMSAPIVersionv2 is a version of the KMS API.
	KMSAPIVersionv2 = "v2"
	// KMSAPIVersionv2beta1 is a version of the KMS API.
	KMSAPIVersionv2beta1 = "v2beta1"
	// annotationsMaxSize is the maximum size of the annotations.
	annotationsMaxSize = 32 * 1024 // 32 kB
	// KeyIDMaxSize is the maximum size of the keyID.
	KeyIDMaxSize = 1 * 1024 // 1 kB
	// encryptedDEKSourceMaxSize is the maximum size of the encrypted DEK source.
	encryptedDEKSourceMaxSize = 1 * 1024 // 1 kB
	// cacheTTL is the default time-to-live for the cache entry.
	// this allows the cache to grow to an infinite size for up to a day.
	// there is unlikely to be any meaningful memory impact on the server
	// because the cache will likely never have more than a few thousand entries.
	// each entry can be large due to an internal cache that maps the DEK seed to individual
	// DEK entries, but that cache has an aggressive TTL to keep the size under control.
	// with DEK/seed reuse and no storage migration, the number of entries in this cache
	// would be approximated by unique key IDs used by the KMS plugin
	// combined with the number of server restarts.  If storage migration
	// is performed after key ID changes, and the number of restarts
	// is limited, this cache size may be as small as the number of API
	// servers in use (once old entries expire out from the TTL).
	cacheTTL = 24 * time.Hour
	// key ID related error codes for metrics
	errKeyIDOKCode      ErrCodeKeyID = "ok"
	errKeyIDEmptyCode   ErrCodeKeyID = "empty"
	errKeyIDTooLongCode ErrCodeKeyID = "too_long"
)

// StateFunc 返回某个时刻 KMS 插件的状态（当前用于加密的 DEK 与密钥 ID）。
type StateFunc func() (State, error)

// ErrCodeKeyID 是密钥 ID 校验结果的低基数枚举值，直接作为指标标签使用。
type ErrCodeKeyID string

// State 描述一次 KMS 状态快照，供加密与陈旧判断使用。
type State struct {
	Transformer value.Transformer

	EncryptedObjectKeyID                  string
	EncryptedObjectEncryptedDEKSource     []byte
	EncryptedObjectAnnotations            map[string][]byte
	EncryptedObjectEncryptedDEKSourceType kmstypes.EncryptedDEKSourceType

	UID string

	// ExpirationTimestamp 是 DEK 的过期时间；过期后拒绝加密，强制重新向 KMS 取密钥。
	ExpirationTimestamp time.Time

	// CacheKey is the key used to cache the DEK/seed in envelopeTransformer.cache.
	CacheKey []byte

	// KMSProviderName is used to dynamically look up the time function for tests
	KMSProviderName string
}

// ValidateEncryptCapability 在加密前检查 DEK 是否已过期。
// 只用本进程的时钟判断，避免依赖 KMS 的时钟一致性。
func (s *State) ValidateEncryptCapability() error {
	nowFunc := GetNowFunc(s.KMSProviderName)
	if now := nowFunc(); now.After(s.ExpirationTimestamp) {
		return fmt.Errorf("encryptedDEKSource with keyID hash %q expired at %s (current time is %s)",
			GetHashIfNotEmpty(s.EncryptedObjectKeyID), s.ExpirationTimestamp.Format(time.RFC3339), now.Format(time.RFC3339))
	}
	return nil
}

// envelopeTransformer 实现 KEK-DEK 信封加密的读写转换。
//
// cache 以"密文 DEK + keyID + 注解"为键缓存已解密的 transformer；
// 这样同一批数据无需反复调用远端 KMS，同时 keyID 变化会自然产生新的缓存项。
type envelopeTransformer struct {
	envelopeService kmsservice.Service
	providerName    string
	stateFunc       StateFunc

	// cache is a thread-safe expiring lru cache which caches decrypted DEKs indexed by their encrypted form.
	cache       *simpleCache
	apiServerID string
}

// NewEnvelopeTransformer returns a transformer which implements a KEK-DEK based envelope encryption scheme.
// It uses envelopeService to encrypt and decrypt DEKs. Respective DEKs (in encrypted form) are prepended to
// the data items they encrypt.
func NewEnvelopeTransformer(envelopeService kmsservice.Service, providerName string, stateFunc StateFunc, apiServerID string) value.Transformer {
	return newEnvelopeTransformerWithClock(envelopeService, providerName, stateFunc, apiServerID, cacheTTL, clock.RealClock{})
}

// newEnvelopeTransformerWithClock 允许测试注入假时钟，以验证缓存 TTL 行为。
func newEnvelopeTransformerWithClock(envelopeService kmsservice.Service, providerName string, stateFunc StateFunc, apiServerID string, cacheTTL time.Duration, clock clock.Clock) value.Transformer {
	return &envelopeTransformer{
		envelopeService: envelopeService,
		providerName:    providerName,
		stateFunc:       stateFunc,
		cache:           newSimpleCache(clock, cacheTTL, providerName),
		apiServerID:     apiServerID,
	}
}

// TransformFromStorage decrypts data encrypted by this transformer using envelope encryption.
func (t *envelopeTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	ctx, span := tracing.Start(ctx, "TransformFromStorage with envelopeTransformer",
		attribute.String("transformer.provider.name", t.providerName),
		// The service.instance_id of the apiserver is already available in the trace
		/*
			{
			"key": "service.instance.id",
			"type": "string",
			"value": "apiserver-zsteyir5lyrtdcmqqmd5kzze6m"
			}
		*/
	)
	defer span.End(500 * time.Millisecond)

	span.AddEvent("About to decode encrypted object")
	// 从数据中反序列化出 EncryptedObject。
	// 解码同时做结构校验：字段缺失、类型未知或尺寸超限都视为数据损坏，绝不猜测。
	encryptedObject, err := t.doDecode(data)
	if err != nil {
		span.AddEvent("Decoding encrypted object failed")
		span.RecordError(err)
		return nil, false, err
	}
	span.AddEvent("Decoded encrypted object")

	useSeed := encryptedObject.EncryptedDEKSourceType == kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED

	// TODO: consider marking state.EncryptedDEK != encryptedObject.EncryptedDEK as a stale read to support DEK defragmentation
	//  at a minimum we should have a metric that helps the user understand if DEK fragmentation is high
	//
	// 读取路径不校验加密能力（DEK 过期只影响写），否则历史数据在轮换后可能再也读不出来。
	state, err := t.stateFunc() // no need to call state.ValidateEncryptCapability on reads
	if err != nil {
		return nil, false, err
	}

	encryptedObjectCacheKey, err := generateCacheKey(encryptedObject.EncryptedDEKSourceType, encryptedObject.EncryptedDEKSource, encryptedObject.KeyID, encryptedObject.Annotations)
	if err != nil {
		return nil, false, err
	}

	// Look up the decrypted DEK from cache first
	transformer := t.cache.get(encryptedObjectCacheKey)

	// 若本地没有对应的 transformer，则回退到 envelope service（远端 KMS）。
	// 缓存未命中才访问远端 KMS；miss 计数用于观察重启/轮换后的冷启动成本。
	if transformer == nil {
		span.AddEvent("About to decrypt DEK using remote service")
		value.RecordCacheMiss()

		requestInfo := getRequestInfoFromContext(ctx)
		uid := string(uuid.NewUUID())
		klog.V(6).InfoS("decrypting content using envelope service", "uid", uid, "key", string(dataCtx.AuthenticatedData()),
			"group", requestInfo.APIGroup, "version", requestInfo.APIVersion, "resource", requestInfo.Resource, "subresource", requestInfo.Subresource,
			"verb", requestInfo.Verb, "namespace", requestInfo.Namespace, "name", requestInfo.Name)

		key, err := t.envelopeService.Decrypt(ctx, uid, &kmsservice.DecryptRequest{
			Ciphertext:  encryptedObject.EncryptedDEKSource,
			KeyID:       encryptedObject.KeyID,
			Annotations: encryptedObject.Annotations,
		})
		if err != nil {
			span.AddEvent("DEK decryption failed")
			span.RecordError(err)
			return nil, false, fmt.Errorf("failed to decrypt DEK, error: %w", err)
		}
		span.AddEvent("DEK decryption succeeded")

		transformer, err = t.addTransformerForDecryption(encryptedObjectCacheKey, key, useSeed)
		if err != nil {
			return nil, false, err
		}
	}
	metrics.RecordKeyID(metrics.FromStorageLabel, t.providerName, encryptedObject.KeyID, t.apiServerID)

	span.AddEvent("About to decrypt data using DEK")
	out, stale, err := transformer.TransformFromStorage(ctx, encryptedObject.EncryptedData, dataCtx)
	if err != nil {
		span.AddEvent("Data decryption failed")
		span.RecordError(err)
		return nil, false, err
	}

	span.AddEvent("Data decryption succeeded")
	// 若 keyID 与我们当前用于写入的 transformer 不匹配，则认为数据已经陈旧。
	// keyID 或 DEK 类型与当前写状态不一致即视为陈旧，从而触发后台重写（存储迁移）。
	return out,
		stale ||
			encryptedObject.KeyID != state.EncryptedObjectKeyID ||
			encryptedObject.EncryptedDEKSourceType != state.EncryptedObjectEncryptedDEKSourceType,
		nil
}

// TransformToStorage encrypts data to be written to disk using envelope encryption.
func (t *envelopeTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	ctx, span := tracing.Start(ctx, "TransformToStorage with envelopeTransformer",
		attribute.String("transformer.provider.name", t.providerName),
		// The service.instance_id of the apiserver is already available in the trace
		/*
			{
			"key": "service.instance.id",
			"type": "string",
			"value": "apiserver-zsteyir5lyrtdcmqqmd5kzze6m"
			}
		*/
	)
	defer span.End(500 * time.Millisecond)

	state, err := t.stateFunc()
	if err != nil {
		return nil, err
	}
	if err := state.ValidateEncryptCapability(); err != nil {
		return nil, err
	}

	// this prevents a cache miss every time the DEK rotates
	// this has the side benefit of causing the cache to perform a GC
	// TODO see if we can do this inside the stateFunc control loop
	//
	// 写路径顺便把当前 transformer 放入缓存，使"刚写的数据马上被读"不必往返 KMS。
	t.cache.set(state.CacheKey, state.Transformer)

	requestInfo := getRequestInfoFromContext(ctx)
	klog.V(6).InfoS("encrypting content using DEK", "uid", state.UID, "key", string(dataCtx.AuthenticatedData()),
		"group", requestInfo.APIGroup, "version", requestInfo.APIVersion, "resource", requestInfo.Resource, "subresource", requestInfo.Subresource,
		"verb", requestInfo.Verb, "namespace", requestInfo.Namespace, "name", requestInfo.Name)

	span.AddEvent("About to encrypt data using DEK")
	result, err := state.Transformer.TransformToStorage(ctx, data, dataCtx)
	if err != nil {
		span.AddEvent("Data encryption failed")
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Data encryption succeeded")

	metrics.RecordKeyID(metrics.ToStorageLabel, t.providerName, state.EncryptedObjectKeyID, t.apiServerID)

	encObjectCopy := &kmstypes.EncryptedObject{
		KeyID:                  state.EncryptedObjectKeyID,
		EncryptedDEKSource:     state.EncryptedObjectEncryptedDEKSource,
		Annotations:            state.EncryptedObjectAnnotations,
		EncryptedDEKSourceType: state.EncryptedObjectEncryptedDEKSourceType,
		EncryptedData:          result,
	}

	span.AddEvent("About to encode encrypted object")
	// Serialize the EncryptedObject to a byte array.
	out, err := t.doEncode(encObjectCopy)
	if err != nil {
		span.AddEvent("Encoding encrypted object failed")
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Encoded encrypted object")

	return out, nil
}

// addTransformerForDecryption inserts a new transformer to the Envelope cache of DEKs for future reads.
func (t *envelopeTransformer) addTransformerForDecryption(cacheKey []byte, key []byte, useSeed bool) (value.Read, error) {
	var transformer value.Read
	var err error
	if useSeed {
		// 这里的输入密钥被认为可以安全使用，因为它来自 KMS plugin / etcd。
		// seed 模式用 HKDF 派生扩展 nonce 密钥，避免 GCM 的 nonce 复用风险。
		transformer, err = aestransformer.NewHKDFExtendedNonceGCMTransformer(key)
	} else {
		var block cipher.Block
		block, err = aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		// this is compatible with NewGCMTransformerWithUniqueKeyUnsafe for decryption
		// it would use random nonces for encryption but we never do that
		transformer, err = aestransformer.NewGCMTransformer(block)
	}
	if err != nil {
		return nil, err
	}
	t.cache.set(cacheKey, transformer)
	return transformer, nil
}

// doEncode 把 EncryptedObject 编码为字节数组。编码前再校验一次，
// 防止内存中被错误修改的对象落盘。
func (t *envelopeTransformer) doEncode(request *kmstypes.EncryptedObject) ([]byte, error) {
	if err := ValidateEncryptedObject(request); err != nil {
		return nil, err
	}
	return proto.Marshal(request)
}

// doDecode 把字节数组解码为 EncryptedObject。解码后立即校验，
// 使后续逻辑可以信任字段的尺寸与类型。
func (t *envelopeTransformer) doDecode(originalData []byte) (*kmstypes.EncryptedObject, error) {
	o := &kmstypes.EncryptedObject{}
	if err := proto.Unmarshal(originalData, o); err != nil {
		return nil, err
	}
	if err := ValidateEncryptedObject(o); err != nil {
		return nil, err
	}

	return o, nil
}

// GenerateTransformer generates a new transformer and encrypts the DEK/seed using the envelope service.
// It returns the transformer, the encrypted DEK/seed, cache key and error.
//
// useSeed 决定 DEK 形态：种子可从同一 KMS 响应派生多个 nonce，减少 KMS 调用；
// 非种子模式直接使用一段随机 AES 密钥。
func GenerateTransformer(ctx context.Context, uid string, envelopeService kmsservice.Service, useSeed bool) (value.Transformer, *kmstypes.EncryptedObject, []byte, error) {
	newTransformerFunc := func() (value.Transformer, []byte, error) {
		seed, err := aestransformer.GenerateKey(aestransformer.MinSeedSizeExtendedNonceGCM)
		if err != nil {
			return nil, nil, err
		}
		transformer, err := aestransformer.NewHKDFExtendedNonceGCMTransformer(seed)
		if err != nil {
			return nil, nil, err
		}
		return transformer, seed, nil
	}
	if !useSeed {
		newTransformerFunc = aestransformer.NewGCMTransformerWithUniqueKeyUnsafe
	}
	transformer, newKey, err := newTransformerFunc()
	if err != nil {
		return nil, nil, nil, err
	}

	klog.V(6).InfoS("encrypting content using envelope service", "uid", uid)

	resp, err := envelopeService.Encrypt(ctx, uid, newKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to encrypt DEK, error: %w", err)
	}

	o := &kmstypes.EncryptedObject{
		KeyID:              resp.KeyID,
		EncryptedDEKSource: resp.Ciphertext,
		EncryptedData:      []byte{0}, // any non-empty value to pass validation
		Annotations:        resp.Annotations,
	}

	if useSeed {
		o.EncryptedDEKSourceType = kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED
	} else {
		o.EncryptedDEKSourceType = kmstypes.EncryptedDEKSourceType_AES_GCM_KEY
	}

	if err := ValidateEncryptedObject(o); err != nil {
		return nil, nil, nil, err
	}

	cacheKey, err := generateCacheKey(o.EncryptedDEKSourceType, resp.Ciphertext, resp.KeyID, resp.Annotations)
	if err != nil {
		return nil, nil, nil, err
	}

	// 清空占位数据：调用方必须在真正加密后填入 EncryptedData，
	// 避免占位值被误当真实密文使用。
	o.EncryptedData = nil // make sure that later code that uses this encrypted object sets this field

	return transformer, o, cacheKey, nil
}

// ValidateEncryptedObject 校验从存储读回或即将写入的信封结构。
//
// 这是防篡改的关键：只要任一字段尺寸或类型异常就拒绝，不能尝试用可疑密钥解密。
func ValidateEncryptedObject(o *kmstypes.EncryptedObject) error {
	if o == nil {
		return fmt.Errorf("encrypted object is nil")
	}
	switch t := o.EncryptedDEKSourceType; t {
	case kmstypes.EncryptedDEKSourceType_AES_GCM_KEY:
	case kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED:
	default:
		return fmt.Errorf("unknown encryptedDEKSourceType: %d", t)
	}
	if len(o.EncryptedData) == 0 {
		return fmt.Errorf("encrypted data is empty")
	}
	if err := validateEncryptedDEKSource(o.EncryptedDEKSource); err != nil {
		return fmt.Errorf("failed to validate encrypted DEK source: %w", err)
	}
	if _, err := ValidateKeyID(o.KeyID); err != nil {
		return fmt.Errorf("failed to validate key id: %w", err)
	}
	if err := validateAnnotations(o.Annotations); err != nil {
		return fmt.Errorf("failed to validate annotations: %w", err)
	}
	return nil
}

// validateEncryptedDEKSource 校验以下两点：
// 1. 加密后的 DEK source 不为空。
// 2. 加密后的 DEK source 大小小于 1 kB。
// 尺寸上限防止畸形数据导致后续分配与 KMS 调用放大（解压缩/分片攻击面）。
func validateEncryptedDEKSource(encryptedDEKSource []byte) error {
	if len(encryptedDEKSource) == 0 {
		return fmt.Errorf("encrypted DEK source is empty")
	}
	if len(encryptedDEKSource) > encryptedDEKSourceMaxSize {
		return fmt.Errorf("encrypted DEK source is %d bytes, which exceeds the max size of %d", len(encryptedDEKSource), encryptedDEKSourceMaxSize)
	}
	return nil
}

// validateAnnotations tests the following:
//  1. checks if the annotation key is fully qualified
//  2. The size of annotations keys + values is less than 32 kB.
//
// 要求全限定域名是为了让注解命名空间归属明确，避免不同 KMS 插件相互覆盖。
// 逐项收集错误后聚合返回，便于一次性报告所有问题。
func validateAnnotations(annotations map[string][]byte) error {
	var errs []error
	var totalSize uint64
	for k, v := range annotations {
		if fieldErr := validation.IsFullyQualifiedDomainName(field.NewPath("annotations"), k); fieldErr != nil {
			errs = append(errs, fieldErr.ToAggregate())
		}
		totalSize += uint64(len(k)) + uint64(len(v))
	}
	if totalSize > annotationsMaxSize {
		errs = append(errs, fmt.Errorf("total size of annotations is %d, which exceeds the max size of %d", totalSize, annotationsMaxSize))
	}
	return utilerrors.NewAggregate(errs)
}

// ValidateKeyID 校验以下两点：
// 1. keyID 不为空。
// 2. keyID 的大小小于 1 kB。
// 返回的 ErrCodeKeyID 可直接作为指标标签，避免把 keyID 本身暴露为高基数标签。
func ValidateKeyID(keyID string) (ErrCodeKeyID, error) {
	if len(keyID) == 0 {
		return errKeyIDEmptyCode, fmt.Errorf("keyID is empty")
	}
	if len(keyID) > KeyIDMaxSize {
		return errKeyIDTooLongCode, fmt.Errorf("keyID is %d bytes, which exceeds the max size of %d", len(keyID), KeyIDMaxSize)
	}
	return errKeyIDOKCode, nil
}

// getRequestInfoFromContext 返回上下文中记录的请求信息；缺失时返回空结构，
// 使日志字段（group/resource 等）始终存在而不必做 nil 判断。
func getRequestInfoFromContext(ctx context.Context) *genericapirequest.RequestInfo {
	if reqInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		return reqInfo
	}
	return &genericapirequest.RequestInfo{}
}

// generateCacheKey returns a key for the cache.
// The key is a concatenation of:
//  0. encryptedDEKSourceType
//  1. encryptedDEKSource
//  2. keyID
//  3. length of annotations
//  4. annotations (sorted by key) - each annotation is a concatenation of:
//     a. annotation key
//     b. annotation value
//
// 注解按 key 排序，保证相同逻辑输入产生相同缓存键，否则 map 迭代顺序会导致缓存失效。
// 使用长度前缀而非分隔符，避免键值拼接产生歧义（与 compare 的注入攻击同理）。
func generateCacheKey(encryptedDEKSourceType kmstypes.EncryptedDEKSourceType, encryptedDEKSource []byte, keyID string, annotations map[string][]byte) ([]byte, error) {
	// TODO(aramase): use sync pool buffer to avoid allocations
	b := cryptobyte.NewBuilder(nil)
	b.AddUint32(uint32(encryptedDEKSourceType))
	b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		b.AddBytes(encryptedDEKSource)
	})
	b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		b.AddBytes(toBytes(keyID))
	})
	if len(annotations) == 0 {
		return b.Bytes()
	}

	// add the length of annotations to the cache key
	b.AddUint32(uint32(len(annotations)))

	// Sort the annotations by key.
	keys := make([]string, 0, len(annotations))
	for k := range annotations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		// The maximum size of annotations is annotationsMaxSize (32 kB) so we can safely
		// assume that the length of the key and value will fit in a uint16.
		b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes(toBytes(k))
		})
		b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes(annotations[k])
		})
	}

	return b.Bytes()
}

// toBytes 为避免内存分配而执行了一些“不洁”的 unsafe 操作。
// 仅在生命周期确定短于源字符串时使用；空字符串返回 nil，因为 unsafe.StringData
// 对空串的行为未定义。
func toBytes(s string) []byte {
	// unsafe.StringData is unspecified for the empty string, so we provide a strict interpretation
	if len(s) == 0 {
		return nil
	}
	// Copied from go 1.20.1 os.File.WriteString
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/os/file.go#L246
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// GetHashIfNotEmpty 在 data 非空时返回其 sha256 哈希。
// 错误信息中使用哈希而非原始 keyID，既可用于比对又不泄露密钥标识。
func GetHashIfNotEmpty(data string) string {
	if len(data) > 0 {
		return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(data)))
	}
	return ""
}
