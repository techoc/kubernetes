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

package credentialprovider

import (
	"testing"
	"time"
)

// testProvider 是记录调用次数的假凭据提供者。
type testProvider struct {
	Count int
}

// Enabled 实现 dockerConfigProvider 接口。
func (d *testProvider) Enabled() bool {
	return true
}

// Provide 实现 dockerConfigProvider 接口，并累计调用次数。
func (d *testProvider) Provide(image string) DockerConfig {
	d.Count++
	return DockerConfig{}
}

// TestCachingProvider 验证缓存提供者在有效期内只调用底层提供者一次。
func TestCachingProvider(t *testing.T) {
	provider := &testProvider{
		Count: 0,
	}

	// 设置一秒生命周期，便于快速验证缓存过期行为。
	cache := &CachingDockerConfigProvider{
		Provider: provider,
		Lifetime: 1 * time.Second,
	}

	image := "image"

	if provider.Count != 0 {
		t.Errorf("Unexpected number of Provide calls: %v", provider.Count)
	}
	cache.Provide(image)
	cache.Provide(image)
	cache.Provide(image)
	cache.Provide(image)
	if provider.Count != 1 {
		t.Errorf("Unexpected number of Provide calls: %v", provider.Count)
	}

	// 等待缓存过期，验证过期后会重新获取凭据。
	time.Sleep(cache.Lifetime)
	cache.Provide(image)
	cache.Provide(image)
	cache.Provide(image)
	cache.Provide(image)
	if provider.Count != 2 {
		t.Errorf("Unexpected number of Provide calls: %v", provider.Count)
	}

	time.Sleep(cache.Lifetime)
	cache.Provide(image)
	cache.Provide(image)
	cache.Provide(image)
	cache.Provide(image)
	if provider.Count != 3 {
		t.Errorf("Unexpected number of Provide calls: %v", provider.Count)
	}
}
