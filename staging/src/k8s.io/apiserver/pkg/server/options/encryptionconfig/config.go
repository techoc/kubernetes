/*
Copyright 2017 The Kubernetes Authors.

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

package encryptionconfig

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/apiserver/pkg/apis/apiserver/validation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig/metrics"
	storagevalue "k8s.io/apiserver/pkg/storage/value"
	aestransformer "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope"
	envelopekmsv2 "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2"
	kmstypes "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2/v2"
	envelopemetrics "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/metrics"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	"k8s.io/apiserver/pkg/storage/value/encrypt/secretbox"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	kmsservice "k8s.io/kms/pkg/service"
)

const (
	aesCBCTransformerPrefixV1    = "k8s:enc:aescbc:v1:"
	aesGCMTransformerPrefixV1    = "k8s:enc:aesgcm:v1:"
	secretboxTransformerPrefixV1 = "k8s:enc:secretbox:v1:"
	kmsTransformerPrefixV1       = "k8s:enc:kms:v1:"
	kmsTransformerPrefixV2       = "k8s:enc:kms:v2:"

	// these constants relate to how the KMS v2 plugin status poll logic
	// and the DEK/seed generation logic behave.  In particular, the positive
	// interval and max TTL are closely related as the difference between
	// these values defines the worst case window in which the write DEK/seed
	// could expire due to the plugin going into an error state.  The
	// worst case window divided by the negative interval defines the
	// minimum amount of times the server will attempt to return to a
	// healthy state before the DEK/seed expires and writes begin to fail.
	//
	// For now, these values are kept small and hardcoded to support being
	// able to perform a "passive" storage migration while tolerating some
	// amount of plugin downtime.
	//
	// With the current approach, a user can update the key ID their plugin
	// is using and then can simply schedule a migration for 3 + N + M minutes
	// later where N is how long it takes their plugin to pick up new config
	// and M is extra buffer to allow the API server to process the config.
	// At that point, they are guaranteed to either migrate to the new key
	// or get errors during the migration.
	//
	// If the API server coasted forever on the last DEK/seed, they would need
	// to actively check if it had observed the new key ID before starting
	// a migration - otherwise it could keep using the old DEK/seed and their
	// storage migration would not do what they thought it did.
	kmsv2PluginHealthzPositiveInterval = 1 * time.Minute
	kmsv2PluginHealthzNegativeInterval = 10 * time.Second
	kmsv2PluginWriteDEKSourceMaxTTL    = 3 * time.Minute

	kmsPluginHealthzNegativeTTL = 3 * time.Second
	kmsPluginHealthzPositiveTTL = 20 * time.Second
	kmsAPIVersionV1             = "v1"
	kmsAPIVersionV2             = "v2"
	// this name is used for two different healthz endpoints:
	// - when one or more KMS v2 plugins are in use and no KMS v1 plugins are in use
	//   in this case, all v2 plugins are probed via this single endpoint
	// - when automatic reload of encryption config is enabled
	//   in this case, all KMS plugins are probed via this single endpoint
	//   the endpoint is present even if there are no KMS plugins configured (it is a no-op then)
	kmsReloadHealthCheckName = "kms-providers"
)

var codecs serializer.CodecFactory

// this map allows us to swap enablement of the KMSv2KDF feature in tests
// as the feature gate is now locked to true starting with v1.29
// Note: it cannot be set by an end user
// KDF enablement is tracked per KMS provider to allow tests to run in parallel.
//
// 按 KMS 名称分键而非全局开关，是为了让并行集成测试互不干扰。
var kdfEnabledPerKMS sync.Map // map[string]bool, KMS name -> KDF enabled

// this function should only be called in tests to swap enablement of the KMSv2KDF feature
// Caller must guarantee that all KMS providers have distinct names across all tests.
// 返回的函数用于恢复默认行为（删除该条目的记录）。
func SetKDFForTests(kmsName string, b bool) func() {
	if len(kmsName) == 0 { // guarantee that GetKDF("") returns the default value
		panic("empty KMS name used in test")
	}
	if _, loaded := kdfEnabledPerKMS.LoadOrStore(kmsName, b); loaded {
		panic("duplicate KMS name used in test")
	}
	return func() { kdfEnabledPerKMS.Delete(kmsName) }
}

// this function should be used to determine enablement of the KMSv2KDF feature
// instead of getting it from DefaultFeatureGate as the feature gate is now locked
// to true starting with v1.29
// to allow integration tests to run in parallel, this "feature flag" can be set
// per KMS provider as long as all providers use distinct names.
func GetKDF(kmsName string) bool {
	kdfEnabled, ok := kdfEnabledPerKMS.Load(kmsName)
	if !ok {
		return true // explicit config is missing, but KDF is enabled by default
	}
	return kdfEnabled.(bool) // this will panic if a non-bool ever gets stored, which should never happen
}

// init 初始化配置编解码器并注册所有加密相关指标；指标必须先注册，
// 否则后续访问会因重复注册而 panic。
func init() {
	configScheme := runtime.NewScheme()
	utilruntime.Must(apiserver.AddToScheme(configScheme))
	utilruntime.Must(apiserverv1.AddToScheme(configScheme))
	codecs = serializer.NewCodecFactory(configScheme, serializer.EnableStrict)
	envelopemetrics.RegisterMetrics()
	storagevalue.RegisterMetrics()
	metrics.RegisterMetrics()
}

// kmsPluginHealthzResponse 缓存最近一次探针结果及其时间，用于 TTL 内的短路。
type kmsPluginHealthzResponse struct {
	err      error
	received time.Time
}

// kmsPluginProbe 是 KMS v1 的健康探针：通过 encrypt/decrypt "ping" 验证链路。
type kmsPluginProbe struct {
	name         string
	ttl          time.Duration
	service      envelope.Service
	lastResponse *kmsPluginHealthzResponse
	l            *sync.Mutex
}

// kmsv2PluginProbe 是 KMS v2 的健康探针：调用 Status 并根据 key ID 维护 DEK/seed 状态。
type kmsv2PluginProbe struct {
	// state 用 atomic 发布，后台轮询与存储读写可并发访问。
	state        atomic.Pointer[envelopekmsv2.State]
	name         string
	ttl          time.Duration
	service      kmsservice.Service
	lastResponse *kmsPluginHealthzResponse
	l            *sync.Mutex
	apiServerID  string
	version      string
}

// kmsHealthChecker 把多个探针聚合成单个健康检查端点，避免逐个暴露。
type kmsHealthChecker []healthz.HealthChecker

// Name 返回聚合端点的固定名称。
func (k kmsHealthChecker) Name() string {
	return kmsReloadHealthCheckName
}

// Check 串行执行全部探针并合并错误；串行是有意的，避免对 KMS 插件造成突发压力。
func (k kmsHealthChecker) Check(req *http.Request) error {
	var errs []error

	for i := range k {
		checker := k[i]
		if err := checker.Check(req); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", checker.Name(), err))
		}
	}

	return utilerrors.Reduce(utilerrors.NewAggregate(errs))
}

// toHealthzCheck 把 v1 探针适配为带序号名称的 healthz 检查。
func (h *kmsPluginProbe) toHealthzCheck(idx int) healthz.HealthChecker {
	return healthz.NamedCheck(fmt.Sprintf("kms-provider-%d", idx), func(r *http.Request) error {
		return h.check()
	})
}

// toHealthzCheck 把 v2 探针适配为 healthz 检查，透传请求上下文用于取消。
func (h *kmsv2PluginProbe) toHealthzCheck(idx int) healthz.HealthChecker {
	return healthz.NamedCheck(fmt.Sprintf("kms-provider-%d", idx), func(r *http.Request) error {
		return h.check(r.Context())
	})
}

// EncryptionConfiguration represents the parsed and normalized encryption configuration for the apiserver.
type EncryptionConfiguration struct {
	// Transformers is a list of value.Transformer that will be used to encrypt and decrypt data.
	Transformers map[schema.GroupResource]storagevalue.Transformer

	// HealthChecks is a list of healthz.HealthChecker that will be used to check the health of the encryption providers.
	HealthChecks []healthz.HealthChecker

	// EncryptionFileContentHash is the hash of the encryption config file.
	// 只保留哈希而不保留原文，避免明文密钥长期驻留内存。
	EncryptionFileContentHash string

	// KMSCloseGracePeriod is the duration we will wait before closing old transformers.
	// We wait for any in-flight requests to finish by using the duration which is longer than their timeout.
	KMSCloseGracePeriod time.Duration
}

// LoadEncryptionConfig parses and validates the encryption config specified by filepath.
// It may launch multiple go routines whose lifecycle is controlled by ctx.
// In case of an error, the caller is responsible for canceling ctx to clean up any go routines that may have been launched.
// If reload is true, or KMS v2 plugins are used with no KMS v1 plugins, the returned slice of health checkers will always be of length 1.
func LoadEncryptionConfig(ctx context.Context, filepath string, reload bool, apiServerID string) (*EncryptionConfiguration, error) {
	config, contentHash, err := loadConfig(filepath, reload)
	if err != nil {
		return nil, fmt.Errorf("error while parsing file: %w", err)
	}

	transformers, kmsHealthChecks, kmsUsed, err := getTransformerOverridesAndKMSPluginHealthzCheckers(ctx, config, apiServerID)
	if err != nil {
		return nil, fmt.Errorf("error while building transformers: %w", err)
	}

	// 动态重载或仅使用 v2 时统一暴露一个聚合端点，避免配置变更导致健康检查名称漂移。
	if reload || (kmsUsed.v2Used && !kmsUsed.v1Used) {
		kmsHealthChecks = []healthz.HealthChecker{kmsHealthChecker(kmsHealthChecks)}
	}

	// KMSCloseGracePeriod is the duration we will wait before closing old transformers.
	// The way we calculate is as follows:
	// 1. Sum all timeouts across all KMS plugins. (check kmsPrefixTransformer for differences between v1 and v2)
	// 2. Multiply that by 2 (to allow for some buffer)
	// The reason we sum all timeout is because kmsHealthChecker() will run all health checks serially
	return &EncryptionConfiguration{
		Transformers:              transformers,
		HealthChecks:              kmsHealthChecks,
		EncryptionFileContentHash: contentHash,
		KMSCloseGracePeriod:       2 * kmsUsed.kmsTimeoutSum,
	}, nil
}

// getTransformerOverridesAndKMSPluginHealthzCheckers creates the set of transformers and KMS healthz checks based on the given config.
// It may launch multiple go routines whose lifecycle is controlled by ctx.
// In case of an error, the caller is responsible for canceling ctx to clean up any go routines that may have been launched.
func getTransformerOverridesAndKMSPluginHealthzCheckers(ctx context.Context, config *apiserver.EncryptionConfiguration, apiServerID string) (map[schema.GroupResource]storagevalue.Transformer, []healthz.HealthChecker, *kmsState, error) {
	var kmsHealthChecks []healthz.HealthChecker
	transformers, probes, kmsUsed, err := getTransformerOverridesAndKMSPluginProbes(ctx, config, apiServerID)
	if err != nil {
		return nil, nil, nil, err
	}
	// 下标用于生成稳定的检查名（kms-provider-N）。
	for i := range probes {
		probe := probes[i]
		kmsHealthChecks = append(kmsHealthChecks, probe.toHealthzCheck(i))
	}

	return transformers, kmsHealthChecks, kmsUsed, nil
}

// healthChecker 抽象“可转换为 healthz 检查”的探针。
type healthChecker interface {
	toHealthzCheck(idx int) healthz.HealthChecker
}

// getTransformerOverridesAndKMSPluginProbes creates the set of transformers and KMS probes based on the given config.
// It may launch multiple go routines whose lifecycle is controlled by ctx.
// In case of an error, the caller is responsible for canceling ctx to clean up any go routines that may have been launched.
func getTransformerOverridesAndKMSPluginProbes(ctx context.Context, config *apiserver.EncryptionConfiguration, apiServerID string) (map[schema.GroupResource]storagevalue.Transformer, []healthChecker, *kmsState, error) {
	resourceToPrefixTransformer := map[schema.GroupResource][]storagevalue.PrefixTransformer{}
	var probes []healthChecker
	var kmsUsed kmsState

	// For each entry in the configuration
	for _, resourceConfig := range config.Resources {
		transformers, p, used, err := prefixTransformersAndProbes(ctx, resourceConfig, apiServerID)
		if err != nil {
			return nil, nil, nil, err
		}
		kmsUsed.accumulate(used)

		// For each resource, create a list of providers to use
		for _, resource := range resourceConfig.Resources {
			gr := schema.ParseGroupResource(resource)

			// check if resource is masked by *.group rule
			anyResourceInGroup := schema.GroupResource{Group: gr.Group, Resource: "*"}
			if _, masked := resourceToPrefixTransformer[anyResourceInGroup]; masked {
				// an earlier rule already configured a transformer for *.group, masking this rule
				// return error since this is not allowed
				// 前面的通配规则已覆盖该资源，后续更具体的规则将永不生效，属于配置错误。
				return nil, nil, nil, fmt.Errorf("resource %q is masked by earlier rule %q", grYAMLString(gr), grYAMLString(anyResourceInGroup))
			}

			if _, masked := resourceToPrefixTransformer[anyGroupAnyResource]; masked {
				// an earlier rule already configured a transformer for *.*, masking this rule
				// return error since this is not allowed
				return nil, nil, nil, fmt.Errorf("resource %q is masked by earlier rule %q", grYAMLString(gr), grYAMLString(anyGroupAnyResource))
			}

			resourceToPrefixTransformer[gr] = append(resourceToPrefixTransformer[gr], transformers...)
		}

		probes = append(probes, p...)
	}

	transformers := make(map[schema.GroupResource]storagevalue.Transformer, len(resourceToPrefixTransformer))
	for gr, transList := range resourceToPrefixTransformer {
		// 按配置顺序尝试各前缀；都不匹配时报错，而不是静默返回明文。
		transformers[gr] = storagevalue.NewPrefixTransformers(fmt.Errorf("no matching prefix found"), transList...)
	}

	return transformers, probes, &kmsUsed, nil
}

// check encrypts and decrypts test data against KMS-Plugin's gRPC endpoint.
func (h *kmsPluginProbe) check() error {
	h.l.Lock()
	defer h.l.Unlock()

	// TTL 内直接复用上次结果（无论成功失败），避免每个健康检查请求都打到插件。
	if (time.Since(h.lastResponse.received)) < h.ttl {
		return h.lastResponse.err
	}

	p, err := h.service.Encrypt([]byte("ping"))
	if err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return fmt.Errorf("failed to perform encrypt section of the healthz check for KMS Provider %s, error: %w", h.name, err)
	}

	// 解密刚加密的数据，验证加解密链路完整（包括密钥可用性）。
	if _, err := h.service.Decrypt(p); err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return fmt.Errorf("failed to perform decrypt section of the healthz check for KMS Provider %s, error: %w", h.name, err)
	}

	h.lastResponse = &kmsPluginHealthzResponse{err: nil, received: time.Now()}
	// 成功时使用较长的 TTL，减少对插件的调用频率。
	h.ttl = kmsPluginHealthzPositiveTTL
	return nil
}

// check gets the healthz status of the KMSv2-Plugin using the Status() method.
func (h *kmsv2PluginProbe) check(ctx context.Context) error {
	h.l.Lock()
	defer h.l.Unlock()

	if time.Since(h.lastResponse.received) < h.ttl {
		return h.lastResponse.err
	}

	p, err := h.service.Status(ctx)
	if err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return fmt.Errorf("failed to perform status section of the healthz check for KMS Provider %s, error: %w", h.name, err)
	}

	if err := h.isKMSv2ProviderHealthyAndMaybeRotateDEK(ctx, p); err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return err
	}

	h.lastResponse = &kmsPluginHealthzResponse{err: nil, received: time.Now()}
	h.ttl = kmsPluginHealthzPositiveTTL
	return nil
}

// rotateDEKOnKeyIDChange tries to rotate to a new DEK/seed if the key ID returned by Status does not match the
// current state.  If a successful rotation is performed, the new DEK/seed and keyID overwrite the existing state.
// On any failure during rotation (including mismatch between status and encrypt calls), the current state is
// preserved and will remain valid to use for encryption until its expiration (the system attempts to coast).
// If the key ID returned by Status matches the current state, the expiration of the current state is extended
// and no rotation is performed.
func (h *kmsv2PluginProbe) rotateDEKOnKeyIDChange(ctx context.Context, statusKeyID, uid string) error {
	// we do not check ValidateEncryptCapability here because it is fine to re-use an old key
	// that was marked as expired during an unhealthy period.  As long as the key ID matches
	// what we expect then there is no need to rotate here.
	state, errState := h.getCurrentState()

	// allow reads indefinitely in all cases
	// allow writes indefinitely as long as there is no error
	// allow writes for only up to kmsv2PluginWriteDEKSourceMaxTTL from now when there are errors
	// we start the timer before we make the network call because kmsv2PluginWriteDEKSourceMaxTTL is meant to be the upper bound
	expirationTimestamp := envelopekmsv2.GetNowFunc(h.name)().Add(kmsv2PluginWriteDEKSourceMaxTTL)

	// dynamically check if we want to use KDF seed to derive DEKs or just a single DEK
	// this gate can only change during tests, but the check is cheap enough to always make
	// this allows us to easily exercise both modes without restarting the API server
	// TODO integration test that this dynamically takes effect
	useSeed := GetKDF(h.name)
	stateUseSeed := state.EncryptedObjectEncryptedDEKSourceType == kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED

	// state is valid and status keyID is unchanged from when we generated this DEK/seed so there is no need to rotate it
	// just move the expiration of the current state forward by the reuse interval
	// useSeed can only change at runtime during tests, so we check it here to allow us to easily exercise both modes
	if errState == nil && state.EncryptedObjectKeyID == statusKeyID && stateUseSeed == useSeed {
		state.ExpirationTimestamp = expirationTimestamp
		h.state.Store(&state)
		return nil
	}

	transformer, encObject, cacheKey, errGen := envelopekmsv2.GenerateTransformer(ctx, uid, h.service, useSeed)

	if encObject == nil {
		encObject = &kmstypes.EncryptedObject{} // avoid nil panics
	}

	// happy path, should be the common case
	// TODO maybe add success metrics?
	if errGen == nil && encObject.KeyID == statusKeyID {
		// 只有 Status 与 Encrypt 返回同一 key ID 时才发布新状态，
		// 否则视为轮换未完成，继续使用旧状态。
		h.state.Store(&envelopekmsv2.State{
			Transformer:                           transformer,
			EncryptedObjectKeyID:                  encObject.KeyID,
			EncryptedObjectEncryptedDEKSource:     encObject.EncryptedDEKSource,
			EncryptedObjectAnnotations:            encObject.Annotations,
			EncryptedObjectEncryptedDEKSourceType: encObject.EncryptedDEKSourceType,
			UID:                                   uid,
			ExpirationTimestamp:                   expirationTimestamp,
			CacheKey:                              cacheKey,
			KMSProviderName:                       h.name,
		})

		// it should be logically impossible for the new state to be invalid but check just in case
		_, errGen = h.getCurrentState()
		if errGen == nil {
			klogV6 := klog.V(6)
			if klogV6.Enabled() {
				// 只输出 key ID 的哈希，避免把密钥标识写入日志。
				klogV6.InfoS("successfully rotated DEK",
					"uid", uid,
					"useSeed", useSeed,
					"newKeyIDHash", envelopekmsv2.GetHashIfNotEmpty(encObject.KeyID),
					"oldKeyIDHash", envelopekmsv2.GetHashIfNotEmpty(state.EncryptedObjectKeyID),
					"expirationTimestamp", expirationTimestamp.Format(time.RFC3339),
				)
			}
			return nil
		}
	}

	//nolint:errorlint // printing the <nil> error is intentional
	return fmt.Errorf("failed to rotate DEK uid=%q, useSeed=%v, errState=%v, errGen=%v, statusKeyIDHash=%q, encryptKeyIDHash=%q, stateKeyIDHash=%q, expirationTimestamp=%s",
		uid, useSeed, errState, errGen, envelopekmsv2.GetHashIfNotEmpty(statusKeyID), envelopekmsv2.GetHashIfNotEmpty(encObject.KeyID), envelopekmsv2.GetHashIfNotEmpty(state.EncryptedObjectKeyID), state.ExpirationTimestamp.Format(time.RFC3339))
}

// getCurrentState returns the latest state from the last status and encrypt calls.
// If the returned error is nil, the state is considered valid indefinitely for read requests.
// For write requests, the caller must also check that state.ValidateEncryptCapability does not error.
func (h *kmsv2PluginProbe) getCurrentState() (envelopekmsv2.State, error) {
	state := *h.state.Load()

	if state.Transformer == nil {
		return envelopekmsv2.State{}, fmt.Errorf("got unexpected nil transformer")
	}

	// 用一个占位的非空 EncryptedData 走一遍校验，确保状态字段自洽。
	encryptedObjectCopy := kmstypes.EncryptedObject{
		KeyID:                  state.EncryptedObjectKeyID,
		EncryptedDEKSource:     state.EncryptedObjectEncryptedDEKSource,
		Annotations:            state.EncryptedObjectAnnotations,
		EncryptedDEKSourceType: state.EncryptedObjectEncryptedDEKSourceType,
		EncryptedData:          []byte{0}, // any non-empty value to pass validation
	}
	if err := envelopekmsv2.ValidateEncryptedObject(&encryptedObjectCopy); err != nil {
		return envelopekmsv2.State{}, fmt.Errorf("got invalid EncryptedObject: %w", err)
	}

	if state.ExpirationTimestamp.IsZero() {
		return envelopekmsv2.State{}, fmt.Errorf("got unexpected zero expirationTimestamp")
	}

	if len(state.CacheKey) == 0 {
		return envelopekmsv2.State{}, fmt.Errorf("got unexpected empty cacheKey")
	}

	return state, nil
}

// isKMSv2ProviderHealthyAndMaybeRotateDEK 校验 Status 响应并触发必要的 DEK 轮换。
func (h *kmsv2PluginProbe) isKMSv2ProviderHealthyAndMaybeRotateDEK(ctx context.Context, response *kmsservice.StatusResponse) error {
	var errs []error
	if response.Healthz != "ok" {
		errs = append(errs, fmt.Errorf("got unexpected healthz status: %s", response.Healthz))
	}
	if response.Version != envelopekmsv2.KMSAPIVersionv2 && response.Version != envelopekmsv2.KMSAPIVersionv2beta1 {
		errs = append(errs, fmt.Errorf("expected KMSv2 API version %s, got %s", envelopekmsv2.KMSAPIVersionv2, response.Version))
	} else {
		// set version for the first status response
		if len(h.version) == 0 {
			h.version = response.Version
		}
		// 插件在运行期切换 API 版本会让客户端语义不一致，视为不健康。
		if h.version != response.Version {
			errs = append(errs, fmt.Errorf("KMSv2 API version should not change after the initial status response version %s, got %s", h.version, response.Version))
		}
	}

	if errCode, err := envelopekmsv2.ValidateKeyID(response.KeyID); err != nil {
		envelopemetrics.RecordInvalidKeyIDFromStatus(h.name, string(errCode))
		// KeyID 可能敏感，只记录哈希。
		errs = append(errs, fmt.Errorf("got invalid KMSv2 KeyID hash %q: %w", envelopekmsv2.GetHashIfNotEmpty(response.KeyID), err))
	} else {
		envelopemetrics.RecordKeyIDFromStatus(h.name, response.KeyID, h.apiServerID)
		// unconditionally append as we filter out nil errors below
		errs = append(errs, h.rotateDEKOnKeyIDChange(ctx, response.KeyID, string(uuid.NewUUID())))
	}

	if err := utilerrors.Reduce(utilerrors.NewAggregate(errs)); err != nil {
		return fmt.Errorf("kmsv2 Provider %s is not healthy, error: %w", h.name, err)
	}
	return nil
}

// loadConfig parses the encryption configuration file at filepath and returns the parsed config and hash of the file.
func loadConfig(filepath string, reload bool) (*apiserver.EncryptionConfiguration, string, error) {
	data, contentHash, err := loadDataAndHash(filepath)
	if err != nil {
		return nil, "", fmt.Errorf("error while loading file: %w", err)
	}

	// 严格模式解码：拒绝未知字段，避免拼写错误导致安全配置被静默忽略。
	configObj, gvk, err := codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("error decoding encryption provider configuration file %q: %w", filepath, err)
	}
	config, ok := configObj.(*apiserver.EncryptionConfiguration)
	if !ok {
		return nil, "", fmt.Errorf("got unexpected config type: %v", gvk)
	}

	return config, contentHash, validation.ValidateEncryptionConfiguration(config, reload).ToAggregate()
}

// loadDataAndHash 读取文件内容并计算哈希；空文件视为错误。
func loadDataAndHash(filepath string) ([]byte, string, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, "", fmt.Errorf("error reading encryption provider configuration file %q: %w", filepath, err)
	}
	if len(data) == 0 {
		return nil, "", fmt.Errorf("encryption provider configuration file %q is empty", filepath)
	}

	return data, computeEncryptionConfigHash(data), nil
}

// GetEncryptionConfigHash reads the encryption configuration file at filepath and returns the hash of the file.
// It does not attempt to decode or load the config, and serves as a cheap check to determine if the file has changed.
func GetEncryptionConfigHash(filepath string) (string, error) {
	_, contentHash, err := loadDataAndHash(filepath)
	return contentHash, err
}

// prefixTransformersAndProbes creates the set of transformers and KMS probes based on the given resource config.
// It may launch multiple go routines whose lifecycle is controlled by ctx.
// In case of an error, the caller is responsible for canceling ctx to clean up any go routines that may have been launched.
func prefixTransformersAndProbes(ctx context.Context, config apiserver.ResourceConfiguration, apiServerID string) ([]storagevalue.PrefixTransformer, []healthChecker, *kmsState, error) {
	var transformers []storagevalue.PrefixTransformer
	var probes []healthChecker
	var kmsUsed kmsState

	for _, provider := range config.Providers {
		var (
			transformer    storagevalue.PrefixTransformer
			transformerErr error
			probe          healthChecker
			used           *kmsState
		)

		switch {
		case provider.AESGCM != nil:
			// 推荐默认：AEAD，性能与完整性兼得。
			transformer, transformerErr = aesPrefixTransformer(provider.AESGCM, aestransformer.NewGCMTransformer, aesGCMTransformerPrefixV1)

		case provider.AESCBC != nil:
			// CBC 仅用于兼容读取历史数据。
			cbcTransformer := func(block cipher.Block) (storagevalue.Transformer, error) {
				return aestransformer.NewCBCTransformer(block), nil
			}
			transformer, transformerErr = aesPrefixTransformer(provider.AESCBC, cbcTransformer, aesCBCTransformerPrefixV1)

		case provider.Secretbox != nil:
			transformer, transformerErr = secretboxPrefixTransformer(provider.Secretbox)

		case provider.KMS != nil:
			transformer, probe, used, transformerErr = kmsPrefixTransformer(ctx, provider.KMS, apiServerID)
			if transformerErr == nil {
				probes = append(probes, probe)
				kmsUsed.accumulate(used)
			}

		case provider.Identity != nil:
			// identity 即“不加密但校验”，用于迁移期间保留明文读写能力。
			transformer = storagevalue.PrefixTransformer{
				Transformer: identity.NewEncryptCheckTransformer(),
				Prefix:      []byte{},
			}

		default:
			return nil, nil, nil, errors.New("provider does not contain any of the expected providers: KMS, AESGCM, AESCBC, Secretbox, Identity")
		}

		if transformerErr != nil {
			return nil, nil, nil, transformerErr
		}

		transformers = append(transformers, transformer)
	}

	return transformers, probes, &kmsUsed, nil
}

// blockTransformerFunc 抽象“由 cipher.Block 构造 Transformer”的差异（GCM 会失败，CBC 不会）。
type blockTransformerFunc func(cipher.Block) (storagevalue.Transformer, error)

// aesPrefixTransformer 支持多密钥轮换：每个密钥有独立前缀，第一个密钥用于写，全部密钥可用于读。
func aesPrefixTransformer(config *apiserver.AESConfiguration, fn blockTransformerFunc, prefix string) (storagevalue.PrefixTransformer, error) {
	var result storagevalue.PrefixTransformer

	if len(config.Keys) == 0 {
		return result, fmt.Errorf("aes provider has no valid keys")
	}
	// 先整体校验再构造，避免构造到一半才发现后面的密钥非法。
	for _, key := range config.Keys {
		if key.Name == "" {
			return result, fmt.Errorf("key with invalid name provided")
		}
		if key.Secret == "" {
			return result, fmt.Errorf("key %v has no provided secret", key.Name)
		}
	}

	keyTransformers := []storagevalue.PrefixTransformer{}

	for _, keyData := range config.Keys {
		key, err := base64.StdEncoding.DecodeString(keyData.Secret)
		if err != nil {
			return result, fmt.Errorf("could not obtain secret for named key %s: %w", keyData.Name, err)
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return result, fmt.Errorf("error while creating cipher for named key %s: %w", keyData.Name, err)
		}
		transformer, err := fn(block)
		if err != nil {
			return result, fmt.Errorf("error while creating transformer for named key %s: %w", keyData.Name, err)
		}

		// Create a new PrefixTransformer for this key
		// 前缀包含密钥名，读取时据此分流到正确密钥；写入始终使用第一个。
		keyTransformers = append(keyTransformers,
			storagevalue.PrefixTransformer{
				Transformer: transformer,
				Prefix:      []byte(keyData.Name + ":"),
			})
	}

	// Create a prefixTransformer which can choose between these keys
	keyTransformer := storagevalue.NewPrefixTransformers(
		fmt.Errorf("no matching key was found for the provided AES transformer"), keyTransformers...)

	// Create a PrefixTransformer which shall later be put in a list with other providers
	result = storagevalue.PrefixTransformer{
		Transformer: keyTransformer,
		Prefix:      []byte(prefix),
	}
	return result, nil
}

// secretboxPrefixTransformer 与 AES 版本同构，但密钥长度固定为 32 字节（256 位）。
func secretboxPrefixTransformer(config *apiserver.SecretboxConfiguration) (storagevalue.PrefixTransformer, error) {
	var result storagevalue.PrefixTransformer

	if len(config.Keys) == 0 {
		return result, fmt.Errorf("secretbox provider has no valid keys")
	}
	for _, key := range config.Keys {
		if key.Name == "" {
			return result, fmt.Errorf("key with invalid name provided")
		}
		if key.Secret == "" {
			return result, fmt.Errorf("key %v has no provided secret", key.Name)
		}
	}

	keyTransformers := []storagevalue.PrefixTransformer{}

	for _, keyData := range config.Keys {
		key, err := base64.StdEncoding.DecodeString(keyData.Secret)
		if err != nil {
			return result, fmt.Errorf("could not obtain secret for named key %s: %s", keyData.Name, err)
		}

		// 长度不符直接报错，而不是截断或补零（那会静默降低安全性）。
		if len(key) != 32 {
			return result, fmt.Errorf("expected key size 32 for secretbox provider, got %v", len(key))
		}

		keyArray := [32]byte{}
		copy(keyArray[:], key)

		// Create a new PrefixTransformer for this key
		keyTransformers = append(keyTransformers,
			storagevalue.PrefixTransformer{
				Transformer: secretbox.NewSecretboxTransformer(keyArray),
				Prefix:      []byte(keyData.Name + ":"),
			})
	}

	// Create a prefixTransformer which can choose between these keys
	keyTransformer := storagevalue.NewPrefixTransformers(
		fmt.Errorf("no matching key was found for the provided Secretbox transformer"), keyTransformers...)

	// Create a PrefixTransformer which shall later be put in a list with other providers
	result = storagevalue.PrefixTransformer{
		Transformer: keyTransformer,
		Prefix:      []byte(secretboxTransformerPrefixV1),
	}
	return result, nil
}

var (
	// The factory to create kms service. This is to make writing test easier.
	envelopeServiceFactory = envelope.NewGRPCService

	// The factory to create kmsv2 service.  Exported for integration tests.
	EnvelopeKMSv2ServiceFactory = envelopekmsv2.NewGRPCService
)

// kmsState 汇总一组资源使用的 KMS 版本与超时，用于计算关闭旧 transformer 的宽限期。
type kmsState struct {
	v1Used, v2Used bool
	kmsTimeoutSum  time.Duration
}

// accumulate computes the KMS state by:
//   - determining which KMS plugin versions are in use
//   - calculating kmsTimeoutSum which is used as transformTracker.kmsCloseGracePeriod
//     DynamicTransformers.Set waits for this period before closing old transformers after a config reload
func (s *kmsState) accumulate(other *kmsState) {
	s.v1Used = s.v1Used || other.v1Used
	s.v2Used = s.v2Used || other.v2Used
	s.kmsTimeoutSum += other.kmsTimeoutSum
}

// kmsPrefixTransformer creates a KMS transformer and probe based on the given KMS config.
// It may launch multiple go routines whose lifecycle is controlled by ctx.
// In case of an error, the caller is responsible for canceling ctx to clean up any go routines that may have been launched.
func kmsPrefixTransformer(ctx context.Context, config *apiserver.KMSConfiguration, apiServerID string) (storagevalue.PrefixTransformer, healthChecker, *kmsState, error) {
	kmsName := config.Name
	switch config.APIVersion {
	case kmsAPIVersionV1:
		// KMSv1 已弃用：默认关闭，必须显式开启特性门控才能使用。
		if !utilfeature.DefaultFeatureGate.Enabled(features.KMSv1) {
			return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("KMSv1 is deprecated and will only receive security updates going forward. Use KMSv2 instead.  Set --feature-gates=KMSv1=true to use the deprecated KMSv1 feature.")
		}
		klog.InfoS("KMSv1 is deprecated and will only receive security updates going forward. Use KMSv2 instead.")

		envelopeService, err := envelopeServiceFactory(ctx, config.Endpoint, config.Timeout.Duration)
		if err != nil {
			return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("could not configure KMSv1-Plugin's probe %q, error: %w", kmsName, err)
		}

		probe := &kmsPluginProbe{
			name:         kmsName,
			ttl:          kmsPluginHealthzNegativeTTL,
			service:      envelopeService,
			l:            &sync.Mutex{},
			lastResponse: &kmsPluginHealthzResponse{},
		}

		transformer := envelopePrefixTransformer(config, envelopeService, kmsTransformerPrefixV1)

		return transformer, probe, &kmsState{
			v1Used: true,
			// for v1 we will do encrypt and decrypt for health check. Since these are serial operations, we will double the timeout.
			kmsTimeoutSum: 2 * config.Timeout.Duration,
		}, nil

	case kmsAPIVersionV2:
		envelopeService, err := EnvelopeKMSv2ServiceFactory(ctx, config.Endpoint, config.Name, config.Timeout.Duration)
		if err != nil {
			return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("could not configure KMSv2-Plugin's probe %q, error: %w", kmsName, err)
		}

		probe := &kmsv2PluginProbe{
			name:         kmsName,
			ttl:          kmsPluginHealthzNegativeTTL,
			service:      envelopeService,
			l:            &sync.Mutex{},
			lastResponse: &kmsPluginHealthzResponse{},
			apiServerID:  apiServerID,
		}
		// initialize state so that Load always works
		// 先放入仅含 provider 名的空状态，避免 transformer 构造后读到 nil。
		probe.state.Store(&envelopekmsv2.State{
			KMSProviderName: kmsName,
		})

		primeAndProbeKMSv2(ctx, probe, kmsName)
		transformer := storagevalue.PrefixTransformer{
			Transformer: envelopekmsv2.NewEnvelopeTransformer(envelopeService, kmsName, probe.getCurrentState, apiServerID),
			Prefix:      []byte(kmsTransformerPrefixV2 + kmsName + ":"),
		}

		return transformer, probe, &kmsState{
			v2Used:        true,
			kmsTimeoutSum: config.Timeout.Duration,
		}, nil

	default:
		return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("could not configure KMS plugin %q, unsupported KMS API version %q", kmsName, config.APIVersion)
	}
}

// primeAndProbeKMSv2 启动后台轮询：尽快拿到首个 key ID/DEK，并持续在 key 变更时轮换。
// 插件不可用不会阻断 apiserver 启动，但写入会在 DEK 过期后失败。
func primeAndProbeKMSv2(ctx context.Context, probe *kmsv2PluginProbe, kmsName string) {
	runProbeCheckAndLog := func(ctx context.Context, depth int) error {
		if err := probe.check(ctx); err != nil {
			klog.VDepth(1+depth, 2).ErrorS(err, "kms plugin failed health check probe", "name", kmsName)
			return err
		}
		return nil
	}

	blockAndProbeFastUntilSuccess := func(ctx context.Context) {
		_ = wait.PollUntilWithContext(
			ctx,
			kmsv2PluginHealthzNegativeInterval,
			func(ctx context.Context) (bool, error) {
				return runProbeCheckAndLog(ctx, 1) == nil, nil
			},
		)
	}

	// on the happy path where the plugin is healthy and available on server start,
	// prime keyID and DEK by running the check inline once (this also prevents unit tests from flaking)
	errPrime := runProbeCheckAndLog(ctx, 0)

	// if our initial attempt to prime failed, start trying to get to a valid state in the background ASAP
	// this prevents a slow start when the external healthz checker is configured to ignore the KMS healthz endpoint
	// since we want to support the plugin starting up async with the API server, this error is not fatal
	if errPrime != nil {
		go blockAndProbeFastUntilSuccess(ctx) // separate go routine to avoid blocking
	}

	// make sure that the plugin's key ID is reasonably up-to-date
	// also, make sure that our DEK is up-to-date to with said key ID (if it expires the server will fail all writes)
	// if this background loop ever stops running, the server will become unfunctional after kmsv2PluginWriteDEKSourceMaxTTL
	go wait.PollUntilWithContext(
		ctx,
		kmsv2PluginHealthzPositiveInterval,
		func(ctx context.Context) (bool, error) {
			if err := runProbeCheckAndLog(ctx, 0); err == nil {
				return false, nil
			}

			// TODO add integration test for quicker error poll on failure
			// if we fail, block the outer polling and start a new quicker poll inline
			// this limits the chance that our DEK expires during a transient failure
			blockAndProbeFastUntilSuccess(ctx)

			return false, nil
		})
}

// envelopePrefixTransformer 构造 KMS v1 信封转换器：DEK 由 KMS 加密，数据用 DEK 加密。
func envelopePrefixTransformer(config *apiserver.KMSConfiguration, envelopeService envelope.Service, prefix string) storagevalue.PrefixTransformer {
	baseTransformerFunc := func(block cipher.Block) (storagevalue.Transformer, error) {
		gcm, err := aestransformer.NewGCMTransformer(block)
		if err != nil {
			return nil, err
		}

		// v1.24: write using AES-CBC only but support reads via AES-CBC and AES-GCM (so we can move to AES-GCM)
		// v1.25: write using AES-GCM only but support reads via AES-GCM and fallback to AES-CBC for backwards compatibility
		// TODO(aramase): Post v1.25: We cannot drop CBC read support until we automate storage migration.
		// We could have a release note that hard requires users to perform storage migration.
		// 第一个元素负责加密（GCM），CBC 仅用于读取历史数据。
		return unionTransformers{gcm, aestransformer.NewCBCTransformer(block)}, nil
	}

	return storagevalue.PrefixTransformer{
		Transformer: envelope.NewEnvelopeTransformer(envelopeService, int(*config.CacheSize), baseTransformerFunc),
		Prefix:      []byte(prefix + config.Name + ":"),
	}
}

// unionTransformers 依次尝试多个 transformer，第一个成功者胜出。
type unionTransformers []storagevalue.Transformer

// TransformFromStorage 返回 stale=true 表示数据是用非首选算法读出的，
// 从而促使上层重写为当前格式。
func (u unionTransformers) TransformFromStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) (out []byte, stale bool, err error) {
	var errs []error
	for i := range u {
		transformer := u[i]
		result, stale, err := transformer.TransformFromStorage(ctx, data, dataCtx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// when i != 0, we have transformed the data from storage using the new transformer,
		// we want to issue a write to etcd even if the contents of the data haven't changed
		return result, stale || i != 0, nil
	}
	if err := utilerrors.Reduce(utilerrors.NewAggregate(errs)); err != nil {
		return nil, false, err
	}
	return nil, false, fmt.Errorf("unionTransformers: unable to transform from storage")
}

// TransformToStorage 始终使用首选（第一个）transformer 写入。
func (u unionTransformers) TransformToStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) (out []byte, err error) {
	return u[0].TransformToStorage(ctx, data, dataCtx)
}

// computeEncryptionConfigHash returns the expected hash for an encryption config file that has been loaded as bytes.
// We use a hash instead of the raw file contents when tracking changes to avoid holding any encryption keys in memory outside of their associated transformers.
func computeEncryptionConfigHash(data []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

// 编译期断言：动态 transformer 同时是资源转换器与健康检查器。
var _ storagevalue.ResourceTransformers = &DynamicTransformers{}
var _ healthz.HealthChecker = &DynamicTransformers{}

// DynamicTransformers holds transformers that may be dynamically updated via a single external actor, likely a controller.
// This struct must avoid locks (even read write locks) as it is inline to all calls to storage.
// 因此用 atomic.Value 整体替换快照，读路径零锁。
type DynamicTransformers struct {
	transformTracker *atomic.Value
}

// transformTracker 是不可变快照：更新时整体替换，避免读者看到半更新的状态。
type transformTracker struct {
	transformerOverrides  map[schema.GroupResource]storagevalue.Transformer
	kmsPluginHealthzCheck healthz.HealthChecker
	closeTransformers     context.CancelFunc
	kmsCloseGracePeriod   time.Duration
}

// NewDynamicTransformers returns transformers, health checks for kms providers and an ability to close transformers.
func NewDynamicTransformers(
	transformerOverrides map[schema.GroupResource]storagevalue.Transformer,
	kmsPluginHealthzCheck healthz.HealthChecker,
	closeTransformers context.CancelFunc,
	kmsCloseGracePeriod time.Duration,
) *DynamicTransformers {
	dynamicTransformers := &DynamicTransformers{
		transformTracker: &atomic.Value{},
	}

	tracker := &transformTracker{
		transformerOverrides:  transformerOverrides,
		kmsPluginHealthzCheck: kmsPluginHealthzCheck,
		closeTransformers:     closeTransformers,
		kmsCloseGracePeriod:   kmsCloseGracePeriod,
	}
	dynamicTransformers.transformTracker.Store(tracker)

	return dynamicTransformers
}

// Check implements healthz.HealthChecker
func (d *DynamicTransformers) Check(req *http.Request) error {
	return d.transformTracker.Load().(*transformTracker).kmsPluginHealthzCheck.Check(req)
}

// Name implements healthz.HealthChecker
func (d *DynamicTransformers) Name() string {
	return kmsReloadHealthCheckName
}

// TransformerForResource returns the transformer for the given resource.
func (d *DynamicTransformers) TransformerForResource(resource schema.GroupResource) storagevalue.Transformer {
	// 返回轻量代理而非当前 transformer：后续 Set 后仍能取到最新实现。
	return &resourceTransformer{
		resource:         resource,
		transformTracker: d.transformTracker,
	}
}

// Set sets the transformer overrides. This method is not go routine safe and must only be called by the same, single caller throughout the lifetime of this object.
func (d *DynamicTransformers) Set(
	transformerOverrides map[schema.GroupResource]storagevalue.Transformer,
	closeTransformers context.CancelFunc,
	kmsPluginHealthzCheck healthz.HealthChecker,
	kmsCloseGracePeriod time.Duration,
) {
	// store new values
	newTransformTracker := &transformTracker{
		transformerOverrides:  transformerOverrides,
		closeTransformers:     closeTransformers,
		kmsPluginHealthzCheck: kmsPluginHealthzCheck,
		kmsCloseGracePeriod:   kmsCloseGracePeriod,
	}

	// update new transformer overrides
	oldTransformTracker := d.transformTracker.Swap(newTransformTracker).(*transformTracker)

	// close old transformers once we wait for grpc request to finish any in-flight requests.
	// by the time we spawn this go routine, the new transformers have already been set and will be used for new requests.
	// if the server starts shutting down during sleep duration then the transformers will correctly closed early because their lifetime is tied to the api-server drain notifier.
	go func() {
		// 延迟关闭是必要的：旧 transformer 可能仍被在读请求持有。
		time.Sleep(oldTransformTracker.kmsCloseGracePeriod)
		oldTransformTracker.closeTransformers()
	}()
}

// 编译期断言：resourceTransformer 实现 Transformer。
var _ storagevalue.Transformer = &resourceTransformer{}

// resourceTransformer 每次调用都从最新快照解析实际 transformer，
// 使配置热加载对已注册的存储层透明。
type resourceTransformer struct {
	resource         schema.GroupResource
	transformTracker *atomic.Value
}

// TransformFromStorage 委托给最新的 transformer。
func (r *resourceTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, bool, error) {
	return r.transformer().TransformFromStorage(ctx, data, dataCtx)
}

// TransformToStorage 委托给最新的 transformer。
func (r *resourceTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, error) {
	return r.transformer().TransformToStorage(ctx, data, dataCtx)
}

// transformer 按资源名在最新快照中查找匹配项。
func (r *resourceTransformer) transformer() storagevalue.Transformer {
	return transformerFromOverrides(r.transformTracker.Load().(*transformTracker).transformerOverrides, r.resource)
}

// 编译期断言：StaticTransformers 实现 ResourceTransformers。
var _ storagevalue.ResourceTransformers = &StaticTransformers{}

// StaticTransformers 是启动期固定不变的资源到 transformer 映射。
type StaticTransformers map[schema.GroupResource]storagevalue.Transformer

// TransformerForResource 按资源查找，遵循同样的通配回退规则。
func (s StaticTransformers) TransformerForResource(resource schema.GroupResource) storagevalue.Transformer {
	return transformerFromOverrides(s, resource)
}

// anyGroupAnyResource 是最终兜底的 "*.*" 规则键。
var anyGroupAnyResource = schema.GroupResource{
	Group:    "*",
	Resource: "*",
}

// transformerFromOverrides 按 精确资源 -> group/* -> *.* -> identity 的顺序回退，
// 并统一包一层 requestInfoTransformer 以便审计/指标拿到资源信息。
func transformerFromOverrides(transformerOverrides map[schema.GroupResource]storagevalue.Transformer, resource schema.GroupResource) (out storagevalue.Transformer) {
	defer func() { out = newRequestInfoTransformer(resource, out) }()

	if transformer := transformerOverrides[resource]; transformer != nil {
		return transformer
	}

	if transformer := transformerOverrides[schema.GroupResource{
		Group:    resource.Group,
		Resource: "*",
	}]; transformer != nil {
		return transformer
	}

	if transformer := transformerOverrides[anyGroupAnyResource]; transformer != nil {
		return transformer
	}

	// 无匹配规则时保持明文（但会校验写入数据），兼容未配置加密的集群。
	return identity.NewEncryptCheckTransformer()
}

// grYAMLString 生成配置中使用的资源字符串，core 组的 "*" 表示为 "*."。
func grYAMLString(gr schema.GroupResource) string {
	if gr.Group == "" && gr.Resource == "*" {
		return "*."
	}

	return gr.String()
}

// 编译期断言：requestInfoTransformer 实现 Transformer。
var _ storagevalue.Transformer = &requestInfoTransformer{}

// requestInfoTransformer 为底层 transformer 注入资源信息，
// 使 KMS/指标能在没有 HTTP 上下文的场景下识别资源。
type requestInfoTransformer struct {
	baseValueCtx context.Context
	delegate     storagevalue.Transformer
}

func newRequestInfoTransformer(resource schema.GroupResource, delegate storagevalue.Transformer) *requestInfoTransformer {
	return &requestInfoTransformer{
		baseValueCtx: request.WithRequestInfo(context.Background(), &request.RequestInfo{IsResourceRequest: true, APIGroup: resource.Group, Resource: resource.Resource}),
		delegate:     delegate,
	}
}

// TransformFromStorage 使用合并后的上下文调用底层实现。
func (l *requestInfoTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, bool, error) {
	return l.delegate.TransformFromStorage(l.withBaseValueCtx(ctx), data, dataCtx)
}

// TransformToStorage 使用合并后的上下文调用底层实现。
func (l *requestInfoTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, error) {
	return l.delegate.TransformToStorage(l.withBaseValueCtx(ctx), data, dataCtx)
}

// withBaseValueCtx 合并请求上下文与预置资源信息；真实请求上下文优先。
func (l *requestInfoTransformer) withBaseValueCtx(ctx context.Context) context.Context {
	return &joinValueContext{Context: ctx, baseValueCtx: l.baseValueCtx}
}

// joinValueContext 实现上下文值回退：先查请求上下文，再查预置上下文。
type joinValueContext struct {
	context.Context
	baseValueCtx context.Context
}

// Value 按上述回退顺序查找 key。
func (j *joinValueContext) Value(key any) any {
	if val := j.Context.Value(key); val != nil {
		return val
	}
	return j.baseValueCtx.Value(key)
}
