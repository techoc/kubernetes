/*
Copyright 2024 The Kubernetes Authors.

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

package plugin

import (
	"context"
	"crypto/x509"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	externaljwtv1 "k8s.io/externaljwt/apis/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/serviceaccount"
	externaljwtmetrics "k8s.io/kubernetes/pkg/serviceaccount/externaljwt/metrics"
)

// fallbackRefreshDuration 是密钥同步失败后的回退刷新间隔。
// 当外部签名器返回错误时，使用固定 10 秒间隔重试，而不是使用签名器建议的间隔。
const fallbackRefreshDuration = 10 * time.Second

// keyCache 缓存外部 JWT 签名器的公钥，实现 serviceaccount.PublicKeysGetter 接口。
// 定期从外部签名器同步密钥，使用 singleflight 避免并发重复请求。
// 通过 Listener 机制通知依赖方（如 OIDC 元数据提供者）密钥变化。
type keyCache struct {
	client externaljwtv1.ExternalJWTSignerClient

	syncGroup     singleflight.Group
	listenersLock sync.Mutex
	listeners     []serviceaccount.Listener

	verificationKeys atomic.Pointer[VerificationKeys]
}

// newKeyCache 创建外部 JWT 签名器的公钥缓存。
func newKeyCache(client externaljwtv1.ExternalJWTSignerClient) *keyCache {
	cache := &keyCache{
		client: client,
	}
	cache.verificationKeys.Store(&VerificationKeys{})
	return cache
}

// initialFill 执行首次密钥获取，建立与外部签名器的连接并获取初始密钥集。
// 启动时同步调用，确保后续 GetPublicKeys 不会返回空缓存。
func (p *keyCache) initialFill(ctx context.Context) error {
	if err := p.syncKeys(ctx); err != nil {
		return fmt.Errorf("while performing initial cache fill: %w", err)
	}
	return nil
}

// scheduleSync 在后台循环中定期同步密钥。
// 使用外部签名器建议的刷新间隔，失败时回退到 fallbackRefreshDuration。
// 在 goroutine 中运行，直到 ctx 被取消。
func (p *keyCache) scheduleSync(ctx context.Context, keySyncTimeout time.Duration) {
	timer := time.NewTimer(p.verificationKeys.Load().NextRefreshHint.Sub(time.Now()))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.InfoS("Key cache shutting down")
			return
		case <-timer.C:
		}

		timedCtx, cancel := context.WithTimeout(ctx, keySyncTimeout)
		if err := p.syncKeys(timedCtx); err != nil {
			klog.Errorf("when syncing supported public keys(Stale set of keys will be supported): %v", err)
			timer.Reset(fallbackRefreshDuration)
		} else {
			timer.Reset(p.verificationKeys.Load().NextRefreshHint.Sub(time.Now()))
		}
		cancel()
	}
}

func (p *keyCache) AddListener(listener serviceaccount.Listener) {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.listeners = append(p.listeners, listener)
}

// GetCacheAgeMaxSeconds 返回当前缓存可以保留的秒数。
// 基于外部签名器建议的下次刷新时间计算，如果已过期则返回 0。
func (p *keyCache) GetCacheAgeMaxSeconds() int {
	val := int(p.verificationKeys.Load().NextRefreshHint.Sub(time.Now()).Seconds())
	if val < 0 {
		return 0
	}
	return val
}

// GetPublicKeys 根据 keyID 获取公钥。如果缓存中没有找到指定 keyID，触发一次同步。
// keyID 为空时返回所有公钥。实现 serviceaccount.PublicKeysGetter 接口。
func (p *keyCache) GetPublicKeys(ctx context.Context, keyID string) []serviceaccount.PublicKey {
	pubKeys, ok := p.findKeyForKeyID(keyID)
	if ok {
		return pubKeys
	}

	// If we didn't find it, trigger a sync.
	if err := p.syncKeys(ctx); err != nil {
		klog.ErrorS(err, "Error while syncing keys")
		return []serviceaccount.PublicKey{}
	}

	pubKeys, ok = p.findKeyForKeyID(keyID)
	if ok {
		return pubKeys
	}

	// If we still didn't find it, then it's an unknown keyID.
	klog.Errorf("Key id %q not found after refresh", keyID)
	return []serviceaccount.PublicKey{}
}

func (p *keyCache) findKeyForKeyID(keyID string) ([]serviceaccount.PublicKey, bool) {
	if len(p.verificationKeys.Load().Keys) == 0 {
		klog.Error("No keys currently in cache. Initial fill has not completed")
		return nil, false
	}

	if keyID == "" {
		return p.verificationKeys.Load().Keys, true
	}

	keysToReturn := []serviceaccount.PublicKey{}
	for _, key := range p.verificationKeys.Load().Keys {
		if key.KeyID == keyID {
			keysToReturn = append(keysToReturn, key)
		}
	}

	return keysToReturn, len(keysToReturn) > 0
}

// syncKeys 从外部签名器同步所有公钥，完全替换当前缓存。
// 使用 singleflight 避免并发重复请求。如果密钥发生变化，通知所有 Listener。
func (p *keyCache) syncKeys(ctx context.Context) error {
	_, err, _ := p.syncGroup.Do("", func() (any, error) {
		oldPublicKeys := p.verificationKeys.Load()
		newPublicKeys, err := p.getTokenVerificationKeys(ctx)
		externaljwtmetrics.RecordFetchKeysAttempt(err)
		if err != nil {
			return nil, fmt.Errorf("while fetching token verification keys: %w", err)
		}

		p.verificationKeys.Store(newPublicKeys)
		externaljwtmetrics.RecordKeyDataTimeStamp(float64(newPublicKeys.DataTimestamp.UnixNano()) / float64(1000000000))

		if keysChanged(oldPublicKeys, newPublicKeys) {
			p.broadcastUpdate()
		}

		return nil, nil
	})
	return err
}

// keysChanged 判断新旧密钥集是否有变化。
// 比较时间戳、数量、keyID 顺序和 OIDC 排除标志，但不比较密钥内容本身（避免深度比较开销）。
func keysChanged(oldPublicKeys, newPublicKeys *VerificationKeys) bool {
	// If the timestamp changed, we changed
	if !oldPublicKeys.DataTimestamp.Equal(newPublicKeys.DataTimestamp) {
		return true
	}
	// Avoid deepequal checks on key content itself.
	// If the number of keys changed, we changed
	if len(oldPublicKeys.Keys) != len(newPublicKeys.Keys) {
		return true
	}
	// If the order, key id, or oidc discovery flag changed, we changed.
	for i := range oldPublicKeys.Keys {
		if oldPublicKeys.Keys[i].KeyID != newPublicKeys.Keys[i].KeyID {
			return true
		}
		if oldPublicKeys.Keys[i].ExcludeFromOIDCDiscovery != newPublicKeys.Keys[i].ExcludeFromOIDCDiscovery {
			return true
		}
	}
	return false
}

func (p *keyCache) broadcastUpdate() {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	for _, l := range p.listeners {
		// don't block on a slow listener
		go l.Enqueue()
	}
}

// getTokenVerificationKeys 从外部签名器获取所有公钥。
// 验证返回的 refreshHint、dataTimestamp 和每个密钥的 keyID、key 内容。
// 将 PKIX 序列化的公钥解析为 crypto.PublicKey。
func (p *keyCache) getTokenVerificationKeys(ctx context.Context) (*VerificationKeys, error) {
	req := &externaljwtv1.FetchKeysRequest{}
	resp, err := p.client.FetchKeys(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("while getting externally supported jwt signing keys: %w", err)
	}
	// Validate the refresh hint.
	if resp.RefreshHintSeconds <= 0 {
		return nil, fmt.Errorf("found invalid refresh hint (%ds)", resp.RefreshHintSeconds)
	}

	if len(resp.Keys) == 0 {
		return nil, fmt.Errorf("found no keys")
	}
	if err := resp.DataTimestamp.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid data timestamp: %w", err)
	}
	keys := make([]serviceaccount.PublicKey, 0, len(resp.Keys))
	for _, protoKey := range resp.Keys {
		if protoKey == nil {
			return nil, fmt.Errorf("found nil public key")
		}
		if len(protoKey.KeyId) == 0 || len(protoKey.KeyId) > 1024 {
			return nil, fmt.Errorf("found invalid public key id %q", protoKey.KeyId)
		}
		if len(protoKey.Key) == 0 {
			return nil, fmt.Errorf("found empty public key")
		}
		parsedPublicKey, err := x509.ParsePKIXPublicKey(protoKey.Key)
		if err != nil {
			return nil, fmt.Errorf("while parsing external public keys: %w", err)
		}

		keys = append(keys, serviceaccount.PublicKey{
			KeyID:                    protoKey.KeyId,
			PublicKey:                parsedPublicKey,
			ExcludeFromOIDCDiscovery: protoKey.ExcludeFromOidcDiscovery,
		})
	}

	vk := &VerificationKeys{
		Keys:            keys,
		DataTimestamp:   resp.DataTimestamp.AsTime(),
		NextRefreshHint: time.Now().Add(time.Duration(resp.RefreshHintSeconds) * time.Second),
	}

	return vk, nil
}
