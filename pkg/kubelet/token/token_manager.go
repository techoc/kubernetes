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

// Package token 实现节点上 Pod 使用的 serviceaccount token 管理器。
package token

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// maxTTL 是 token 的最大有效期；gcPeriod 是清理周期；maxJitter 是刷新抖动上限。
	maxTTL    = 24 * time.Hour
	gcPeriod  = time.Minute
	maxJitter = 10 * time.Second
)

// NewManager 返回新的 token 管理器。
func NewManager(c clientset.Interface) *Manager {
	// 检查服务端是否支持 token 请求，以便给出更友好的错误信息。
	supported := false
	once := &sync.Once{}
	tokenRequestsSupported := func() bool {
		// 只探测一次服务端能力，避免每次请求都执行 discovery。
		once.Do(func() {
			resources, err := c.Discovery().ServerResourcesForGroupVersionWithContext(context.TODO(), ("v1"))
			if err != nil {
				return
			}
			for _, resource := range resources.APIResources {
				if resource.Name == "serviceaccounts/token" {
					supported = true
					return
				}
			}
		})
		return supported
	}

	m := &Manager{
		getToken: func(name, namespace string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
			if c == nil {
				return nil, errors.New("cannot use TokenManager when kubelet is in standalone mode")
			}
			tokenRequest, err := c.CoreV1().ServiceAccounts(namespace).CreateToken(context.TODO(), name, tr, metav1.CreateOptions{})
			if apierrors.IsNotFound(err) && !tokenRequestsSupported() {
				return nil, fmt.Errorf("the API server does not have TokenRequest endpoints enabled")
			}
			return tokenRequest, err
		},
		cache: make(map[string]*authenticationv1.TokenRequest),
		clock: clock.RealClock{},
	}
	// 后台周期性清理过期 token。
	go wait.Forever(m.cleanup, gcPeriod)
	return m
}

// Manager 管理 Pod 的 service account token。
type Manager struct {

	// cacheMutex 保护 cache。
	cacheMutex sync.RWMutex
	cache      map[string]*authenticationv1.TokenRequest

	// getToken 与 clock 在测试中会被替换。
	getToken func(name, namespace string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error)
	clock    clock.Clock
}

// GetServiceAccountToken 从缓存或 TokenRequest API 为 Pod 获取 service account token，
// 流程如下：
// * 在缓存中查找当前的 token 请求。
// * 若 token 存在且无需刷新，直接返回当前 token。
// * 尝试刷新 token。
// * 刷新成功时把 token 存入缓存并返回。
// * 刷新失败但旧 token 仍有效时，记录错误并返回旧 token。
// * 刷新失败且旧 token 已失效时，返回错误。
func (m *Manager) GetServiceAccountToken(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	// TODO: pass ctx to GetServiceAccountToken after switching pkg/volume to contextual logging
	ctx := context.TODO()
	key := keyFunc(name, namespace, tr)

	ctr, ok := m.get(key)

	// 优先使用缓存中仍然新鲜、无需刷新的 token。
	if ok && !m.requiresRefresh(ctx, ctr) {
		return ctr, nil
	}

	tr, err := m.getToken(name, namespace, tr)
	if err != nil {
		// 刷新失败时根据缓存状态决定：无旧 token 或旧 token 已过期则报错，否则降级使用旧 token。
		switch {
		case !ok:
			return nil, fmt.Errorf("failed to fetch token: %v", err)
		case m.expired(ctr):
			return nil, fmt.Errorf("token %s expired and refresh failed: %v", key, err)
		default:
			logger := klog.FromContext(ctx)
			logger.Error(err, "Couldn't update token", "cacheKey", key)
			return ctr, nil
		}
	}

	m.set(key, tr)
	return tr, nil
}

// DeleteServiceAccountToken 应在 Pod 被删除时调用，用于清理 token 管理器缓存。
func (m *Manager) DeleteServiceAccountToken(podUID types.UID) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	for k, tr := range m.cache {
		// 按 BoundObjectRef 中的 Pod UID 精确删除，避免误删其他 Pod 的 token。
		if tr.Spec.BoundObjectRef.UID == podUID {
			delete(m.cache, k)
		}
	}
}

// cleanup 清理缓存中已过期的 token。
func (m *Manager) cleanup() {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	for k, tr := range m.cache {
		if m.expired(tr) {
			delete(m.cache, k)
		}
	}
}

// get 从缓存读取 token。
func (m *Manager) get(key string) (*authenticationv1.TokenRequest, bool) {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()
	ctr, ok := m.cache[key]
	return ctr, ok
}

// set 将 token 写入缓存。
func (m *Manager) set(key string, tr *authenticationv1.TokenRequest) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	m.cache[key] = tr
}

// expired 判断 token 是否已过期。
func (m *Manager) expired(t *authenticationv1.TokenRequest) bool {
	return m.clock.Now().After(t.Status.ExpirationTimestamp.Time)
}

// requiresRefresh 在 token 已超过总 TTL 的 80%，或已超过 24 小时时返回 true。
func (m *Manager) requiresRefresh(ctx context.Context, tr *authenticationv1.TokenRequest) bool {
	// ExpirationSeconds 缺失说明 token 请求异常，记录错误后不主动刷新。
	if tr.Spec.ExpirationSeconds == nil {
		cpy := tr.DeepCopy()
		cpy.Status.Token = ""
		logger := klog.FromContext(ctx)
		logger.Error(nil, "Expiration seconds was nil for token request", "tokenRequest", cpy)
		return false
	}
	now := m.clock.Now()
	exp := tr.Status.ExpirationTimestamp.Time
	// 由过期时间倒推签发时间。
	iat := exp.Add(-1 * time.Duration(*tr.Spec.ExpirationSeconds) * time.Second)

	// 加入随机抖动，避免大量 Pod 同时刷新。
	jitter := time.Duration(rand.Float64()*maxJitter.Seconds()) * time.Second
	if now.After(iat.Add(maxTTL - jitter)) {
		return true
	}
	// 距离过期不足 TTL 的 20%（含抖动）时要求刷新。
	if now.After(exp.Add(-1*time.Duration((*tr.Spec.ExpirationSeconds*20)/100)*time.Second - jitter)) {
		return true
	}
	return false
}

// 缓存键不包含机密信息，可以安全地记录日志。
func keyFunc(name, namespace string, tr *authenticationv1.TokenRequest) string {
	var exp int64
	if tr.Spec.ExpirationSeconds != nil {
		exp = *tr.Spec.ExpirationSeconds
	}

	var ref authenticationv1.BoundObjectReference
	if tr.Spec.BoundObjectRef != nil {
		ref = *tr.Spec.BoundObjectRef
	}

	var uid types.UID
	if len(tr.UID) > 0 {
		// 若 token 请求中设置了 UID，它会作为前置条件，确保请求针对同一个
		// service account。这可以防止 service account 被删除并以相同名称重建后
		// 仍返回旧的 token。
		uid = tr.UID
	}

	return fmt.Sprintf("%q/%q/%#v/%#v/%#v/%q", name, namespace, tr.Spec.Audiences, exp, ref, uid)
}
