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

package plugin

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	credentialproviderapi "k8s.io/kubelet/pkg/apis/credentialprovider"
	credentialproviderv1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
	credentialproviderv1alpha1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1alpha1"
	credentialproviderv1beta1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1beta1"
	"k8s.io/kubernetes/pkg/credentialprovider"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// fakeExecPlugin 是不运行外部进程的假插件，直接返回预设的凭证与缓存参数。
type fakeExecPlugin struct {
	cacheKeyType  credentialproviderapi.PluginCacheKeyType
	cacheDuration time.Duration

	auth map[string]credentialproviderapi.AuthConfig
}

// countingFakeExecPlugin 在 fakeExecPlugin 基础上统计 ExecPlugin 的调用次数，
// 并故意睡眠一秒模拟慢插件，让并发调用重叠以触发 singleflight 合并。
// 用于验证 perPodPluginProvider 中的 singleflight 行为。
type countingFakeExecPlugin struct {
	fakeExecPlugin
	mu    sync.Mutex
	count int
}

// ExecPlugin 返回预设响应，不执行真正的外部插件。
func (f *fakeExecPlugin) ExecPlugin(ctx context.Context, image, serviceAccountToken string, serviceAccountAnnotations map[string]string) (*credentialproviderapi.CredentialProviderResponse, error) {
	return &credentialproviderapi.CredentialProviderResponse{
		CacheKeyType: f.cacheKeyType,
		CacheDuration: &metav1.Duration{
			Duration: f.cacheDuration,
		},
		Auth: f.auth,
	}, nil
}

// ExecPlugin 记录调用次数并模拟慢插件，便于验证并发请求是否被合并。
func (f *countingFakeExecPlugin) ExecPlugin(ctx context.Context, image, serviceAccountToken string, serviceAccountAnnotations map[string]string) (*credentialproviderapi.CredentialProviderResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	// 故意放慢插件，让并发调用重叠，从而验证 singleflight 是否合并请求。
	time.Sleep(time.Second)
	return f.fakeExecPlugin.ExecPlugin(ctx, image, serviceAccountToken, serviceAccountAnnotations)
}

// TestSingleflightProvide 验证并发取凭证时请求被合并，token 不同则各自调用插件。
func TestSingleflightProvide(t *testing.T) {
	tclock := clock.RealClock{}

	// 准备带计数的假插件，便于检查插件实际被调用的次数。
	execPlugin := &countingFakeExecPlugin{
		fakeExecPlugin: fakeExecPlugin{
			cacheKeyType: credentialproviderapi.RegistryPluginCacheKeyType,
			auth: map[string]credentialproviderapi.AuthConfig{
				"test.registry.io": {Username: "user", Password: "password"},
			},
		},
	}

	// 构造按 Pod 隔离的 provider，模拟真实调用路径。
	pluginProvider := &pluginProvider{
		plugin:         execPlugin,
		group:          singleflight.Group{},
		clock:          tclock,
		lastCachePurge: tclock.Now(),
		matchImages:    []string{"test.registry.io"},
		cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
	}
	dynamicProvider := &perPodPluginProvider{
		provider: pluginProvider,

		podName:            "pod-name",
		podNamespace:       "pod-namespace",
		podUID:             "pod-uid",
		serviceAccountName: "service-account-name",
	}

	image := "test.registry.io"
	var wg sync.WaitGroup
	const concurrentCalls = 5
	results := make([]credentialprovider.DockerConfig, concurrentCalls)

	// serviceAccountProvider 为空时不应影响取凭证流程。
	for i := 0; i < concurrentCalls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, _ := dynamicProvider.provideWithCoordinates(image)
			results[i] = result
		}(i)
	}
	wg.Wait()

	// 并发调用应被 singleflight 合并成一次插件调用。
	if execPlugin.count != 1 {
		t.Errorf("expected ExecPlugin to be called once, but was called %d times", execPlugin.count)
	}

	// 再验证带 serviceAccountProvider 时同样只调用一次插件。
	pluginProvider.serviceAccountProvider = &serviceAccountProvider{
		audience: "audience",
		getServiceAccountFunc: func(namespace, name string) (*v1.ServiceAccount, error) {
			return &v1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
					UID:       "service-account-uid",
				},
			}, nil
		},
		getServiceAccountTokenFunc: func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
			return &authenticationv1.TokenRequest{}, nil
		},
	}

	execPlugin.count = 0 // 重置计数，避免污染下一组断言。
	for i := 0; i < concurrentCalls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, _ := dynamicProvider.provideWithCoordinates(image)
			results[i] = result
		}(i)
	}
	wg.Wait()

	// 再次确认插件只被调用一次。
	if execPlugin.count != 1 {
		t.Errorf("expected ExecPlugin to be called once with serviceAccountProvider, but was called %d times", execPlugin.count)
	}

	// token 不同意味着缓存键不同，预期无法合并，需为每次调用分别取凭证。
	pluginProvider.serviceAccountProvider.getServiceAccountTokenFunc = func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		return &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{Token: rand.String(10)}}, nil
	}

	execPlugin.count = 0 // 重置计数，避免污染下一组断言。
	for i := 0; i < concurrentCalls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, _ := dynamicProvider.provideWithCoordinates(image)
			results[i] = result
		}(i)
	}
	wg.Wait()

	// token 各不相同，singleflight 不应合并请求。
	if execPlugin.count != concurrentCalls {
		t.Errorf("expected ExecPlugin to be called %d times with different serviceaccount tokens, but was called %d times", concurrentCalls, execPlugin.count)
	}
}

// Test_ProvideWithCoordinates 覆盖镜像匹配、缓存键类型及服务账号模式下的凭证与日志输出。
func Test_ProvideWithCoordinates(t *testing.T) {
	klog.InitFlags(nil)
	if err := flag.Set("v", "6"); err != nil {
		t.Fatalf("failed to set log level: %v", err)
	}
	flag.Parse()

	tclock := clock.RealClock{}
	testcases := []struct {
		name                   string
		pluginProvider         *perPodPluginProvider
		image                  string
		dockerconfig           credentialprovider.DockerConfig
		wantLog                string
		expectedServiceAccount *credentialprovider.ServiceAccountCoordinates
	}{
		{
			name: "exact image match, with Registry cache key",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"test.registry.io"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.RegistryPluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"test.registry.io": {
								Username: "user",
								Password: "password",
							},
						},
					},
				},
			},
			image: "test.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{
				"test.registry.io": credentialprovider.DockerConfigEntry{
					Username: "user",
					Password: "password",
				},
			},
		},
		{
			name: "exact image match, with Image cache key",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"test.registry.io/foo/bar"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.ImagePluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"test.registry.io/foo/bar": {
								Username: "user",
								Password: "password",
							},
						},
					},
				},
			},
			image: "test.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{
				"test.registry.io/foo/bar": credentialprovider.DockerConfigEntry{
					Username: "user",
					Password: "password",
				},
			},
		},
		{
			name: "exact image match, with Global cache key",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"test.registry.io"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.GlobalPluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"test.registry.io": {
								Username: "user",
								Password: "password",
							},
						},
					},
				},
			},
			image: "test.registry.io",
			dockerconfig: credentialprovider.DockerConfig{
				"test.registry.io": credentialprovider.DockerConfigEntry{
					Username: "user",
					Password: "password",
				},
			},
		},
		{
			name: "wild card image match, with Registry cache key",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"*.registry.io:8080"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.RegistryPluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"*.registry.io:8080": {
								Username: "user",
								Password: "password",
							},
						},
					},
				},
			},
			image: "test.registry.io:8080/foo",
			dockerconfig: credentialprovider.DockerConfig{
				"*.registry.io:8080": credentialprovider.DockerConfigEntry{
					Username: "user",
					Password: "password",
				},
			},
		},
		{
			name: "wild card image match, with Image cache key",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"*.*.registry.io"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.ImagePluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"*.*.registry.io": {
								Username: "user",
								Password: "password",
							},
						},
					},
				},
			},
			image: "foo.bar.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{
				"*.*.registry.io": credentialprovider.DockerConfigEntry{
					Username: "user",
					Password: "password",
				},
			},
		},
		{
			name: "wild card image match, with Global cache key",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"*.registry.io"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.GlobalPluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"*.registry.io": {
								Username: "user",
								Password: "password",
							},
						},
					},
				},
			},
			image: "test.registry.io",
			dockerconfig: credentialprovider.DockerConfig{
				"*.registry.io": credentialprovider.DockerConfigEntry{
					Username: "user",
					Password: "password",
				},
			},
		},
		{
			name: "[service account mode] sa name required but empty",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					name:        "test-plugin",
					matchImages: []string{"test.registry.io"},
					serviceAccountProvider: &serviceAccountProvider{
						requireServiceAccount: true,
					},
				},
				podName:      "pod-name",
				podNamespace: "ns",
				podUID:       "pod-uid",
			},
			image:        "test.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{},
			wantLog:      `Service account name is empty" provider="test-plugin" image="test.registry.io/foo/bar" pod="ns/pod-name" podUID="pod-uid"`,
		},
		{
			name: "[service account mode] sa does not have required annotations",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					name:        "test-plugin",
					matchImages: []string{"test.registry.io"},
					serviceAccountProvider: &serviceAccountProvider{
						requireServiceAccount: true,
						requiredServiceAccountAnnotationKeys: []string{
							"domain.io/identity-id",
						},
						getServiceAccountFunc: func(_, _ string) (*v1.ServiceAccount, error) {
							return &v1.ServiceAccount{
								ObjectMeta: metav1.ObjectMeta{
									Name:        "sa-name",
									Namespace:   "ns",
									Annotations: map[string]string{},
								},
							}, nil
						},
					},
				},
				podName:            "pod-name",
				podNamespace:       "ns",
				podUID:             "pod-uid",
				serviceAccountName: "sa-name",
			},
			image:        "test.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{},
			wantLog:      `"Failed to get service account data" err="required annotation domain.io/identity-id not found" provider="test-plugin" image="test.registry.io/foo/bar" pod="ns/pod-name" podUID="pod-uid" serviceAccount="ns/sa-name"`,
		},
		{
			name: "[service account mode] failed to get service account token",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					name:        "test-plugin",
					matchImages: []string{"test.registry.io"},
					serviceAccountProvider: &serviceAccountProvider{
						requireServiceAccount: true,
						requiredServiceAccountAnnotationKeys: []string{
							"domain.io/identity-id",
						},
						optionalServiceAccountAnnotationKeys: []string{
							"domain.io/identity-type",
						},
						getServiceAccountFunc: func(_, _ string) (*v1.ServiceAccount, error) {
							return &v1.ServiceAccount{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "sa-name",
									Namespace: "ns",
									Annotations: map[string]string{
										"domain.io/identity-id": "id",
									},
								},
							}, nil
						},
						getServiceAccountTokenFunc: func(_, _ string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
							return nil, errors.New("failed to get token")
						},
					},
				},
				podName:            "pod-name",
				podNamespace:       "ns",
				podUID:             "pod-uid",
				serviceAccountName: "sa-name",
			},
			image:        "test.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{},
			wantLog:      `"Failed to provide credentials for image" err="failed to get service account token: failed to get token" provider="test-plugin" image="test.registry.io/foo/bar" pod="ns/pod-name" podUID="pod-uid" serviceAccount="ns/sa-name"`,
		},
		{
			name: "[service account mode] cache type not token but service account echoed back",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					name:           "test-plugin",
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"test.registry.io"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.ImagePluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"test.registry.io/foo/bar": {
								Username: "user",
								Password: "token", // 故意让插件回显 SA token，验证非 Token 缓存类型下的拒绝逻辑。
							},
						},
					},
					serviceAccountProvider: &serviceAccountProvider{
						requireServiceAccount: true,
						requiredServiceAccountAnnotationKeys: []string{
							"domain.io/identity-id",
						},
						optionalServiceAccountAnnotationKeys: []string{
							"domain.io/identity-type",
						},
						getServiceAccountFunc: func(_, _ string) (*v1.ServiceAccount, error) {
							return &v1.ServiceAccount{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "sa-name",
									Namespace: "ns",
									Annotations: map[string]string{
										"domain.io/identity-id": "id",
									},
								},
							}, nil
						},
						getServiceAccountTokenFunc: func(_, _ string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
							return &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{Token: "token"}}, nil
						},
					},
				},
				podName:            "pod-name",
				podNamespace:       "ns",
				podUID:             "pod-uid",
				serviceAccountName: "sa-name",
			},
			image:        "test.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{},
			wantLog:      `"Failed to provide credentials for image" err="credential provider plugin returned the service account token as the password which is not allowed when service account cache type is not set to 'Token'" provider="test-plugin" image="test.registry.io/foo/bar" pod="ns/pod-name" podUID="pod-uid" serviceAccount="ns/sa-name"`,
		},
		{
			name: "[service account mode] exact image match",
			pluginProvider: &perPodPluginProvider{
				provider: &pluginProvider{
					clock:          tclock,
					lastCachePurge: tclock.Now(),
					matchImages:    []string{"test.registry.io"},
					cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
					plugin: &fakeExecPlugin{
						cacheKeyType: credentialproviderapi.ImagePluginCacheKeyType,
						auth: map[string]credentialproviderapi.AuthConfig{
							"test.registry.io/foo/bar": {
								Username: "user",
								Password: "password",
							},
						},
					},
					serviceAccountProvider: &serviceAccountProvider{
						requireServiceAccount: true,
						requiredServiceAccountAnnotationKeys: []string{
							"domain.io/identity-id",
						},
						optionalServiceAccountAnnotationKeys: []string{
							"domain.io/identity-type",
						},
						getServiceAccountFunc: func(_, _ string) (*v1.ServiceAccount, error) {
							return &v1.ServiceAccount{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "sa-name",
									Namespace: "ns",
									UID:       "sa-uid",
									Annotations: map[string]string{
										"domain.io/identity-id":   "id",
										"domain.io/identity-type": "type",
									},
								},
							}, nil
						},
						getServiceAccountTokenFunc: func(_, _ string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
							return &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{Token: "token"}}, nil
						},
					},
				},
				podName:            "pod-name",
				podNamespace:       "ns",
				serviceAccountName: "sa-name",
			},
			image: "test.registry.io/foo/bar",
			dockerconfig: credentialprovider.DockerConfig{
				"test.registry.io/foo/bar": credentialprovider.DockerConfigEntry{
					Username: "user",
					Password: "password",
				},
			},
			expectedServiceAccount: &credentialprovider.ServiceAccountCoordinates{
				Namespace: "ns",
				Name:      "sa-name",
				UID:       "sa-uid",
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			// 用内存缓冲捕获 klog，便于断言失败场景下的具体日志原因。
			var buf bytes.Buffer
			klog.SetOutput(&buf)
			klog.LogToStderr(false)
			defer klog.LogToStderr(true)

			gotConfig, gotCoordinates := testcase.pluginProvider.provideWithCoordinates(testcase.image)
			if !reflect.DeepEqual(gotConfig, testcase.dockerconfig) {
				t.Errorf("unexpected docker config from ProvideWithCoordinates: %v, expected: %v", gotConfig, testcase.dockerconfig)
			}

			if testcase.expectedServiceAccount != nil {
				if !reflect.DeepEqual(gotCoordinates, testcase.expectedServiceAccount) {
					t.Errorf("unexpected service account coordinates: %v, expected: %v", gotCoordinates, testcase.expectedServiceAccount)
				}
			} else if gotCoordinates != nil {
				t.Errorf("expected nil service account coordinates but got: %v", gotCoordinates)
			}

			klog.Flush()
			klog.SetOutput(&bytes.Buffer{}) // prevent further writes into buf
			capturedOutput := buf.String()

			if len(testcase.wantLog) > 0 && !strings.Contains(capturedOutput, testcase.wantLog) {
				t.Log("Captured output:", capturedOutput)
				t.Errorf("expected log message %q not found in captured output: %q", testcase.wantLog, capturedOutput)
			}
		})
	}
}

// Test_ProvideParallel 并发为不同 registry 与镜像取凭证。
// 目的是检测缓存读写过程中是否存在竞态条件。
func Test_ProvideParallel(t *testing.T) {
	tclock := clock.RealClock{}

	testcases := []struct {
		name     string
		registry string
	}{
		{
			name:     "provide for registry 1",
			registry: "test1.registry.io",
		},
		{
			name:     "provide for registry 2",
			registry: "test2.registry.io",
		},
		{
			name:     "provide for registry 3",
			registry: "test3.registry.io",
		},
		{
			name:     "provide for registry 4",
			registry: "test4.registry.io",
		},
	}

	pluginProvider := &perPodPluginProvider{
		provider: &pluginProvider{
			clock:          tclock,
			lastCachePurge: tclock.Now(),
			matchImages:    []string{"test1.registry.io", "test2.registry.io", "test3.registry.io", "test4.registry.io"},
			cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
			plugin: &fakeExecPlugin{
				cacheDuration: time.Minute * 1,
				cacheKeyType:  credentialproviderapi.RegistryPluginCacheKeyType,
				auth: map[string]credentialproviderapi.AuthConfig{
					"test.registry.io": {
						Username: "user",
						Password: "password",
					},
				},
			},
		},
	}

	dockerconfig := credentialprovider.DockerConfig{
		"test.registry.io": credentialprovider.DockerConfigEntry{
			Username: "user",
			Password: "password",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			t.Parallel()
			var wg sync.WaitGroup
			wg.Add(5)

			for i := 0; i < 5; i++ {
				go func(w *sync.WaitGroup) {
					image := fmt.Sprintf(testcase.registry+"/%s", rand.String(5))
					dockerconfigResponse, _ := pluginProvider.provideWithCoordinates(image)
					if !reflect.DeepEqual(dockerconfigResponse, dockerconfig) {
						t.Errorf("unexpected docker config for image %s: %v, expected: %v", image, dockerconfigResponse, dockerconfig)
					}
					w.Done()
				}(&wg)
			}
			wg.Wait()

		})
	}
}

// Test_getCachedCredentials 验证缓存凭证的过期判断、到期清理与剩余键数量。
func Test_getCachedCredentials(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	p := &pluginProvider{
		clock:          fakeClock,
		lastCachePurge: fakeClock.Now(),
		cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: fakeClock}),
		plugin:         &fakeExecPlugin{},
	}

	testcases := []struct {
		name             string
		step             time.Duration
		cacheEntry       cacheEntry
		expectedResponse credentialprovider.DockerConfig
		keyLength        int
		getKey           string
	}{
		{
			name:      "It should return not expired credential",
			step:      1 * time.Second,
			keyLength: 1,
			getKey:    "image1",
			expectedResponse: map[string]credentialprovider.DockerConfigEntry{
				"image1": {
					Username: "user1",
					Password: "pass1",
				},
			},
			cacheEntry: cacheEntry{
				key:       "\x00\x06image1\x00\x00",
				expiresAt: fakeClock.Now().Add(1 * time.Minute),
				credentials: map[string]credentialprovider.DockerConfigEntry{
					"image1": {
						Username: "user1",
						Password: "pass1",
					},
				},
			},
		},

		{
			name:      "It should not return expired credential",
			step:      2 * time.Minute,
			getKey:    "image2",
			keyLength: 1,
			cacheEntry: cacheEntry{
				key:       "\x00\x06image2\x00\x00",
				expiresAt: fakeClock.Now(),
				credentials: map[string]credentialprovider.DockerConfigEntry{
					"image2": {
						Username: "user2",
						Password: "pass2",
					},
				},
			},
		},

		{
			name:      "It should delete expired credential during purge",
			step:      18 * time.Minute,
			keyLength: 0,
			// 用随机键触发查询时会顺带执行缓存清理，从而删除已过期的 image3 凭证。
			// 这里不能用 image3 作为查询键，否则查询阶段就会删除它，无法验证清理逻辑。
			getKey: "random",
			cacheEntry: cacheEntry{
				key:       "\x00\x06image3\x00\x00",
				expiresAt: fakeClock.Now().Add(2 * time.Minute),
				credentials: map[string]credentialprovider.DockerConfigEntry{
					"image3": {
						Username: "user3",
						Password: "pass3",
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 先写入缓存项，再用假时钟推进时间，精确控制过期与清理时机。
			p.cache.Add(&tc.cacheEntry)
			fakeClock.Step(tc.step)

			// 查询应只返回未过期的凭证。
			res, _, err := p.getCachedCredentials(tc.getKey, "")
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			if !reflect.DeepEqual(res, tc.expectedResponse) {
				t.Logf("response %v", res)
				t.Logf("expected response %v", tc.expectedResponse)
				t.Errorf("Unexpected response")
			}

			// ListKeys 会返回缓存中的所有键，包括已过期的键。
			if len(p.cache.ListKeys()) != tc.keyLength {
				t.Errorf("Unexpected cache key length")
			}
		})
	}
}

// Test_getCachedCredentials_pluginUsingServiceAccount 验证启用服务账号时缓存键包含 SA 身份信息。
func Test_getCachedCredentials_pluginUsingServiceAccount(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())

	p := &pluginProvider{
		clock:          fakeClock,
		lastCachePurge: fakeClock.Now(),
		cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: fakeClock}),
		plugin:         &fakeExecPlugin{},
		serviceAccountProvider: &serviceAccountProvider{
			audience: "audience",
			getServiceAccountFunc: func(namespace, name string) (*v1.ServiceAccount, error) {
				return &v1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: namespace,
						UID:       "service-account-uid",
					},
				}, nil
			},
			getServiceAccountTokenFunc: func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
				return &authenticationv1.TokenRequest{}, nil
			},
		},
	}

	c := cacheKeyParams{
		namespace:          "namespace",
		serviceAccountName: "serviceAccountName",
		serviceAccountUID:  "service-account-uid",
		saAnnotations: map[string]string{
			"prefix.io/annotation-1": "value1",
			"prefix.io/annotation-2": "value2",
		},
		podName: "pod-name",
		podUID:  "pod-uid",
	}
	serviceAccountCacheKey, err := generateServiceAccountCacheKey(c)
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	cacheKey1, err := generateCacheKey("image1", serviceAccountCacheKey)
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	cacheKey2, err := generateCacheKey("image2", serviceAccountCacheKey)
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	testcases := []struct {
		name             string
		step             time.Duration
		cacheEntry       cacheEntry
		expectedResponse credentialprovider.DockerConfig
		keyLength        int
		getKey           string
	}{
		{
			name:      "It should return not expired credential",
			step:      1 * time.Second,
			keyLength: 1,
			getKey:    "image1",
			expectedResponse: map[string]credentialprovider.DockerConfigEntry{
				"image1": {
					Username: "user1",
					Password: "pass1",
				},
			},
			cacheEntry: cacheEntry{
				key:       cacheKey1,
				expiresAt: fakeClock.Now().Add(1 * time.Minute),
				credentials: map[string]credentialprovider.DockerConfigEntry{
					"image1": {
						Username: "user1",
						Password: "pass1",
					},
				},
			},
		},

		{
			name:      "It should not return expired credential",
			step:      2 * time.Minute,
			getKey:    "image2",
			keyLength: 1,
			cacheEntry: cacheEntry{
				key:       cacheKey2,
				expiresAt: fakeClock.Now(),
				credentials: map[string]credentialprovider.DockerConfigEntry{
					"image2": {
						Username: "user2",
						Password: "pass2",
					},
				},
			},
		},

		{
			name:      "It should delete expired credential during purge",
			step:      18 * time.Minute,
			keyLength: 0,
			// 用随机键触发查询时会顺带执行缓存清理，从而删除已过期的 image3 凭证。
			// 这里不能用 image3 作为查询键，否则查询阶段就会删除它，无法验证清理逻辑。
			getKey: "random",
			cacheEntry: cacheEntry{
				key:       "image3",
				expiresAt: fakeClock.Now().Add(2 * time.Minute),
				credentials: map[string]credentialprovider.DockerConfigEntry{
					"image3": {
						Username: "user3",
						Password: "pass3",
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if err := p.cache.Add(&tc.cacheEntry); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			fakeClock.Step(tc.step)

			// 查询应只返回未过期的凭证。
			res, _, err := p.getCachedCredentials(tc.getKey, serviceAccountCacheKey)
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			if !reflect.DeepEqual(res, tc.expectedResponse) {
				t.Logf("response %v", res)
				t.Logf("expected response %v", tc.expectedResponse)
				t.Errorf("Unexpected response")
			}

			// ListKeys 会返回缓存中的所有键，包括已过期的键。
			if len(p.cache.ListKeys()) != tc.keyLength {
				t.Errorf("Unexpected cache key length")
			}
		})
	}
}

// Test_encodeRequest 验证请求编码结果与各 API 版本的序列化格式一致。
func Test_encodeRequest(t *testing.T) {
	testcases := []struct {
		name         string
		apiVersion   schema.GroupVersion
		request      *credentialproviderapi.CredentialProviderRequest
		expectedData []byte
		expectedErr  bool
	}{
		{
			name:       "successful with v1alpha1",
			apiVersion: credentialproviderv1alpha1.SchemeGroupVersion,
			request: &credentialproviderapi.CredentialProviderRequest{
				Image: "test.registry.io/foobar",
			},
			expectedData: []byte(`{"kind":"CredentialProviderRequest","apiVersion":"credentialprovider.kubelet.k8s.io/v1alpha1","image":"test.registry.io/foobar"}
`),
			expectedErr: false,
		},
		{
			name:       "successful with v1beta1",
			apiVersion: credentialproviderv1beta1.SchemeGroupVersion,
			request: &credentialproviderapi.CredentialProviderRequest{
				Image: "test.registry.io/foobar",
			},
			expectedData: []byte(`{"kind":"CredentialProviderRequest","apiVersion":"credentialprovider.kubelet.k8s.io/v1beta1","image":"test.registry.io/foobar"}
`),
			expectedErr: false,
		},
		{
			name:       "successful with v1",
			apiVersion: credentialproviderv1.SchemeGroupVersion,
			request: &credentialproviderapi.CredentialProviderRequest{
				Image: "test.registry.io/foobar",
			},
			expectedData: []byte(`{"kind":"CredentialProviderRequest","apiVersion":"credentialprovider.kubelet.k8s.io/v1","image":"test.registry.io/foobar"}
`),
			expectedErr: false,
		},
		{
			name:       "successful with v1, with service account token and annotations",
			apiVersion: credentialproviderv1.SchemeGroupVersion,
			request: &credentialproviderapi.CredentialProviderRequest{
				Image:               "test.registry.io/foobar",
				ServiceAccountToken: "service-account-token",
				ServiceAccountAnnotations: map[string]string{
					"domain.io/annotation1": "value1",
				},
			},
			expectedData: []byte(`{"kind":"CredentialProviderRequest","apiVersion":"credentialprovider.kubelet.k8s.io/v1","image":"test.registry.io/foobar","serviceAccountToken":"service-account-token","serviceAccountAnnotations":{"domain.io/annotation1":"value1"}}
`),
			expectedErr: false,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			mediaType := "application/json"
			info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), mediaType)
			if !ok {
				t.Fatalf("unsupported media type: %s", mediaType)
			}

			e := &execPlugin{
				encoder: codecs.EncoderForVersion(info.Serializer, testcase.apiVersion),
			}

			data, err := e.encodeRequest(testcase.request)
			if err != nil && !testcase.expectedErr {
				t.Fatalf("unexpected error: %v", err)
			}

			if err == nil && testcase.expectedErr {
				t.Fatalf("expected error %v but got nil", testcase.expectedErr)
			}

			if !reflect.DeepEqual(data, testcase.expectedData) {
				t.Errorf("actual encoded data: %v", string(data))
				t.Errorf("expected encoded data: %v", string(testcase.expectedData))
				t.Errorf("unexpected encoded response")
			}
		})
	}
}

// Test_decodeResponse 验证各版本响应能正确解码，错误的 Kind 或 Group 会被拒绝。
func Test_decodeResponse(t *testing.T) {
	testcases := []struct {
		name             string
		data             []byte
		expectedResponse *credentialproviderapi.CredentialProviderResponse
		expectedErr      bool
	}{
		{
			name: "success with v1",
			data: []byte(`{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1","cacheKeyType":"Registry","cacheDuration":"1m","auth":{"*.registry.io":{"username":"user","password":"password"}}}`),
			expectedResponse: &credentialproviderapi.CredentialProviderResponse{
				CacheKeyType: credentialproviderapi.RegistryPluginCacheKeyType,
				CacheDuration: &metav1.Duration{
					Duration: time.Minute,
				},
				Auth: map[string]credentialproviderapi.AuthConfig{
					"*.registry.io": {
						Username: "user",
						Password: "password",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "success with v1beta1",
			data: []byte(`{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1beta1","cacheKeyType":"Registry","cacheDuration":"1m","auth":{"*.registry.io":{"username":"user","password":"password"}}}`),
			expectedResponse: &credentialproviderapi.CredentialProviderResponse{
				CacheKeyType: credentialproviderapi.RegistryPluginCacheKeyType,
				CacheDuration: &metav1.Duration{
					Duration: time.Minute,
				},
				Auth: map[string]credentialproviderapi.AuthConfig{
					"*.registry.io": {
						Username: "user",
						Password: "password",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "success with v1alpha1",
			data: []byte(`{"kind":"CredentialProviderResponse","apiVersion":"credentialprovider.kubelet.k8s.io/v1alpha1","cacheKeyType":"Registry","cacheDuration":"1m","auth":{"*.registry.io":{"username":"user","password":"password"}}}`),
			expectedResponse: &credentialproviderapi.CredentialProviderResponse{
				CacheKeyType: credentialproviderapi.RegistryPluginCacheKeyType,
				CacheDuration: &metav1.Duration{
					Duration: time.Minute,
				},
				Auth: map[string]credentialproviderapi.AuthConfig{
					"*.registry.io": {
						Username: "user",
						Password: "password",
					},
				},
			},
			expectedErr: false,
		},
		{
			name:             "wrong Kind",
			data:             []byte(`{"kind":"WrongKind","apiVersion":"credentialprovider.kubelet.k8s.io/v1beta1","cacheKeyType":"Registry","cacheDuration":"1m","auth":{"*.registry.io":{"username":"user","password":"password"}}}`),
			expectedResponse: nil,
			expectedErr:      true,
		},
		{
			name:             "wrong Group",
			data:             []byte(`{"kind":"CredentialProviderResponse","apiVersion":"foobar.kubelet.k8s.io/v1beta1","cacheKeyType":"Registry","cacheDuration":"1m","auth":{"*.registry.io":{"username":"user","password":"password"}}}`),
			expectedResponse: nil,
			expectedErr:      true,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			e := &execPlugin{}

			decodedResponse, err := e.decodeResponse(testcase.data)
			if err != nil && !testcase.expectedErr {
				t.Fatalf("unexpected error: %v", err)
			}

			if err == nil && testcase.expectedErr {
				t.Fatalf("expected error %v but not nil", testcase.expectedErr)
			}

			if !reflect.DeepEqual(decodedResponse, testcase.expectedResponse) {
				t.Logf("actual decoded response: %#v", decodedResponse)
				t.Logf("expected decoded response: %#v", testcase.expectedResponse)
				t.Errorf("unexpected decoded response")
			}
		})
	}
}

// Test_NoCacheResponse 验证缓存时长为 0 时仍返回凭证且不写入缓存。
func Test_NoCacheResponse(t *testing.T) {
	tclock := clock.RealClock{}
	pluginProvider := &perPodPluginProvider{
		provider: &pluginProvider{
			clock:          tclock,
			lastCachePurge: tclock.Now(),
			matchImages:    []string{"*.registry.io"},
			cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
			plugin: &fakeExecPlugin{
				cacheKeyType:  credentialproviderapi.GlobalPluginCacheKeyType,
				cacheDuration: 0, // 缓存时长为 0，不写入缓存。
				auth: map[string]credentialproviderapi.AuthConfig{
					"*.registry.io": {
						Username: "user",
						Password: "password",
					},
				},
			},
		},
	}

	expectedDockerConfig := credentialprovider.DockerConfig{
		"*.registry.io": credentialprovider.DockerConfigEntry{
			Username: "user",
			Password: "password",
		},
	}

	dockerConfig, _ := pluginProvider.provideWithCoordinates("test.registry.io/foo/bar")
	if !reflect.DeepEqual(dockerConfig, expectedDockerConfig) {
		t.Logf("actual docker config: %v", dockerConfig)
		t.Logf("expected docker config: %v", expectedDockerConfig)
		t.Fatal("unexpected docker config")
	}

	expectedCacheKeys := []string{}
	cacheKeys := pluginProvider.provider.cache.ListKeys()
	if !reflect.DeepEqual(cacheKeys, expectedCacheKeys) {
		t.Logf("actual cache keys: %v", cacheKeys)
		t.Logf("expected cache keys: %v", expectedCacheKeys)
		t.Error("unexpected cache keys")
	}
}

// Test_ExecPluginEnvVars 验证系统环境与插件配置环境的合并与覆盖顺序。
func Test_ExecPluginEnvVars(t *testing.T) {
	testcases := []struct {
		name            string
		systemEnvVars   []string
		execPlugin      *execPlugin
		expectedEnvVars []string
	}{
		{
			name:          "positive append system env vars",
			systemEnvVars: []string{"HOME=/home/foo", "PATH=/usr/bin"},
			execPlugin: &execPlugin{
				envVars: []kubeletconfig.ExecEnvVar{
					{
						Name:  "SUPER_SECRET_STRONG_ACCESS_KEY",
						Value: "123456789",
					},
				},
			},
			expectedEnvVars: []string{
				"HOME=/home/foo",
				"PATH=/usr/bin",
				"SUPER_SECRET_STRONG_ACCESS_KEY=123456789",
			},
		},
		{
			name:          "positive no env vars provided in plugin",
			systemEnvVars: []string{"HOME=/home/foo", "PATH=/usr/bin"},
			execPlugin:    &execPlugin{},
			expectedEnvVars: []string{
				"HOME=/home/foo",
				"PATH=/usr/bin",
			},
		},
		{
			name: "positive no system env vars but env vars are provided in plugin",
			execPlugin: &execPlugin{
				envVars: []kubeletconfig.ExecEnvVar{
					{
						Name:  "SUPER_SECRET_STRONG_ACCESS_KEY",
						Value: "123456789",
					},
				},
			},
			expectedEnvVars: []string{
				"SUPER_SECRET_STRONG_ACCESS_KEY=123456789",
			},
		},
		{
			name:            "positive no system or plugin provided env vars",
			execPlugin:      &execPlugin{},
			expectedEnvVars: nil,
		},
		{
			name:          "positive plugin provided vars takes priority",
			systemEnvVars: []string{"HOME=/home/foo", "PATH=/usr/bin", "SUPER_SECRET_STRONG_ACCESS_KEY=1111"},
			execPlugin: &execPlugin{
				envVars: []kubeletconfig.ExecEnvVar{
					{
						Name:  "SUPER_SECRET_STRONG_ACCESS_KEY",
						Value: "123456789",
					},
				},
			},
			expectedEnvVars: []string{
				"HOME=/home/foo",
				"PATH=/usr/bin",
				"SUPER_SECRET_STRONG_ACCESS_KEY=1111",
				"SUPER_SECRET_STRONG_ACCESS_KEY=123456789",
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			testcase.execPlugin.environ = func() []string {
				return testcase.systemEnvVars
			}

			var configVars []string
			for _, envVar := range testcase.execPlugin.envVars {
				configVars = append(configVars, fmt.Sprintf("%s=%s", envVar.Name, envVar.Value))
			}
			// 合并两类环境变量，验证插件配置能覆盖系统同名变量。
			merged := mergeEnvVars(testcase.systemEnvVars, configVars)

			err := validate(testcase.expectedEnvVars, merged)
			if err != nil {
				t.Logf("unexpecged error %v", err)
			}
		})
	}
}

// validate 比较两个环境变量切片是否完全一致。
func validate(expected, actual []string) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("actual env var length [%d] and expected env var length [%d] don't match",
			len(actual), len(expected))
	}

	for i := range actual {
		if actual[i] != expected[i] {
			return fmt.Errorf("mismatch in expected env var %s and actual env var %s", actual[i], expected[i])
		}
	}

	return nil
}

// TestGenerateServiceAccountCacheKey_Deterministic 验证相同 SA 身份生成相同键，不同身份生成不同键。
func TestGenerateServiceAccountCacheKey_Deterministic(t *testing.T) {
	namespace1 := "default"
	name1 := "service-account1"
	uid1 := types.UID("633a81d0-0f58-4a43-9e84-113145201b72")
	annotations1 := map[string]string{"domain.io/identity-id": "1234567890", "domain.io/role": "admin"}

	namespace2 := "kube-system"
	name2 := "service-account2"
	uid2 := types.UID("1408a4e6-e40b-4bbf-9019-4d86bfea73ae")
	annotations2 := map[string]string{"domain.io/identity-id": "0987654321", "domain.io/role": "viewer"}

	testCases := []struct {
		serviceAccountNamespace string
		serviceAccountName      string
		serviceAccountUID       types.UID
		requiredAnnotations     map[string]string
	}{
		{namespace1, name1, uid1, annotations1},
		{namespace1, name1, uid1, annotations2},
		{namespace1, name2, uid1, annotations1},
		{namespace1, name2, uid1, annotations2},
		{namespace2, name1, uid1, annotations1},
		{namespace2, name1, uid1, annotations2},
		{namespace2, name2, uid1, annotations1},
		{namespace2, name2, uid1, annotations2},
		{namespace1, name1, uid2, annotations1},
		{namespace1, name1, uid2, annotations2},
		{namespace1, name2, uid2, annotations1},
		{namespace1, name2, uid2, annotations2},
		{namespace2, name1, uid2, annotations1},
		{namespace2, name1, uid2, annotations2},
		{namespace2, name2, uid2, annotations1},
		{namespace2, name2, uid2, annotations2},
	}

	// 两两组合所有参数，确保缓存键只随影响身份的字段变化。
	for _, tc := range testCases {
		for _, tc2 := range testCases {
			t.Run(fmt.Sprintf("%+v-%+v", tc, tc2), func(t *testing.T) {
				serviceAccountCacheKey1, err1 := generateServiceAccountCacheKey(cacheKeyParams{
					namespace:          tc.serviceAccountNamespace,
					serviceAccountName: tc.serviceAccountName,
					serviceAccountUID:  tc.serviceAccountUID,
					saAnnotations:      tc.requiredAnnotations})
				serviceAccountCacheKey2, err2 := generateServiceAccountCacheKey(cacheKeyParams{
					namespace:          tc2.serviceAccountNamespace,
					serviceAccountName: tc2.serviceAccountName,
					serviceAccountUID:  tc2.serviceAccountUID,
					saAnnotations:      tc2.requiredAnnotations})

				if err1 != nil || err2 != nil {
					t.Errorf("expected no error, but got err1=%v, err2=%v", err1, err2)
				}

				if bytes.Equal([]byte(serviceAccountCacheKey1), []byte(serviceAccountCacheKey2)) != reflect.DeepEqual(tc, tc2) {
					t.Errorf("expected %v, got %v", reflect.DeepEqual(tc, tc2), bytes.Equal([]byte(serviceAccountCacheKey1), []byte(serviceAccountCacheKey2)))
				}

				cacheKey1, err1 := generateCacheKey("registry.io/image", serviceAccountCacheKey1)
				cacheKey2, err2 := generateCacheKey("registry.io/image", serviceAccountCacheKey2)

				if err1 != nil || err2 != nil {
					t.Errorf("expected no error, but got err1=%v, err2=%v", err1, err2)
				}

				if bytes.Equal([]byte(cacheKey1), []byte(cacheKey2)) != reflect.DeepEqual(tc, tc2) {
					t.Errorf("expected %v, got %v", reflect.DeepEqual(tc, tc2), bytes.Equal([]byte(cacheKey1), []byte(cacheKey2)))
				}
			})
		}
	}
}

// TestGenerateServiceAccountCacheKey 验证 SA 缓存键的字节格式、注解排序与 token 哈希。
func TestGenerateServiceAccountCacheKey(t *testing.T) {
	tests := []struct {
		name          string
		saNamespace   string
		saName        string
		saUID         types.UID
		saAnnotations map[string]string
		cacheType     kubeletconfig.ServiceAccountTokenCacheType
		want          string
	}{
		{
			name:          "no annotations",
			saNamespace:   "namespace",
			saName:        "service-account",
			saUID:         "service-account-uid",
			saAnnotations: nil,
			want:          "\x00\tnamespace\x00\x0fservice-account\x00\x13service-account-uid\x00\x00\x00\x00",
		},
		{
			name:          "single annotation",
			saNamespace:   "namespace",
			saName:        "service-account",
			saUID:         "service-account-uid",
			saAnnotations: map[string]string{"domain.io/annotation-1": "value1"},
			want:          "\x00\tnamespace\x00\x0fservice-account\x00\x13service-account-uid\x00\x00\x00\x01\x00\x16domain.io/annotation-1\x00\x06value1",
		},
		{
			name:          "multiple annotations",
			saNamespace:   "namespace",
			saName:        "service-account",
			saUID:         "service-account-uid",
			saAnnotations: map[string]string{"domain.io/annotation-1": "value1", "domain.io/annotation-2": "value2"},
			want:          "\x00\tnamespace\x00\x0fservice-account\x00\x13service-account-uid\x00\x00\x00\x02\x00\x16domain.io/annotation-1\x00\x06value1\x00\x16domain.io/annotation-2\x00\x06value2",
		},
		{
			name:          "annotations with different order should be sorted",
			saNamespace:   "namespace",
			saName:        "service-account",
			saUID:         "service-account-uid",
			saAnnotations: map[string]string{"domain.io/annotation-2": "value2", "domain.io/annotation-1": "value1"},
			want:          "\x00\tnamespace\x00\x0fservice-account\x00\x13service-account-uid\x00\x00\x00\x02\x00\x16domain.io/annotation-1\x00\x06value1\x00\x16domain.io/annotation-2\x00\x06value2",
		},
		{
			name:          "token hash included",
			saNamespace:   "namespace",
			saName:        "service-account",
			saUID:         "service-account-uid",
			saAnnotations: map[string]string{"domain.io/annotation-2": "value2", "domain.io/annotation-1": "value1"},
			cacheType:     kubeletconfig.TokenServiceAccountTokenCacheType,
			want:          "\x00\tnamespace\x00\x0fservice-account\x00\x13service-account-uid\x00\x00\x00\x02\x00\x16domain.io/annotation-1\x00\x06value1\x00\x16domain.io/annotation-2\x00\x06value2\x00\x05token\x00Gsha256:f7a41acb9633520e657fe58dedcd9e6a784e2d97e4c646492b45c61e28595c70",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := cacheKeyParams{
				namespace:          tc.saNamespace,
				serviceAccountName: tc.saName,
				serviceAccountUID:  tc.saUID,
				saAnnotations:      tc.saAnnotations,
				podName:            "pod-name",
				podUID:             "pod-uid",
				saTokenHash:        "sha256:f7a41acb9633520e657fe58dedcd9e6a784e2d97e4c646492b45c61e28595c70",
				cacheType:          tc.cacheType,
			}

			got, err := generateServiceAccountCacheKey(c)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("expected %q, got '%q'", tc.want, got)
			}
		})
	}
}

// TestGenerateCacheKey 验证镜像基础键与 SA 缓存键拼接后的字节格式。
func TestGenerateCacheKey(t *testing.T) {
	tests := []struct {
		name                   string
		baseKey                string
		serviceAccountCacheKey string
		want                   string
	}{
		{
			name:                   "empty service account cache key",
			baseKey:                "registry.io/image",
			serviceAccountCacheKey: "",
			want:                   "\x00\x11registry.io/image\x00\x00",
		},
		{
			name:                   "combined key",
			baseKey:                "registry.io/image",
			serviceAccountCacheKey: "\x00\tnamespace\x00\x0fservice-account\x00\x13service-account-uid\x00\x00\x00\x02\x00\x16domain.io/annotation-1\x00\x06value1\x00\x16domain.io/annotation-2\x00\x06value2",
			want:                   "\x00\x11registry.io/image\x00u\x00\tnamespace\x00\x0fservice-account\x00\x13service-account-uid\x00\x00\x00\x02\x00\x16domain.io/annotation-1\x00\x06value1\x00\x16domain.io/annotation-2\x00\x06value2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := generateCacheKey(tc.baseKey, tc.serviceAccountCacheKey)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

// TestRequiredAnnotationNotFoundErr 验证缺少必需注解的错误能被正确识别。
func TestRequiredAnnotationNotFoundErr(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "required annotation not found error",
			err:      requiredAnnotationNotFoundError("something"),
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("some other error"),
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isRequiredAnnotationNotFoundError(test.err); got != test.expected {
				t.Errorf("expected %v, got %v", test.expected, got)
			}
		})
	}
}

// Test_CacheKeyGeneration 验证插件缓存键类型与 SA 缓存类型的组合均生成预期键。
func Test_CacheKeyGeneration(t *testing.T) {
	type cacheKeyTestCase struct {
		name                    string
		pluginCacheKeyType      credentialproviderapi.PluginCacheKeyType
		serviceAccountCacheType kubeletconfig.ServiceAccountTokenCacheType // 空字符串表示未启用 SA 模式。
		expectedCacheKeyFunc    func(t *testing.T, image string, saParams *cacheKeyParams) string
	}

	testCases := []cacheKeyTestCase{
		{
			name:                    "image-no-sa",
			pluginCacheKeyType:      credentialproviderapi.ImagePluginCacheKeyType,
			serviceAccountCacheType: "",
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				return generateExpectedCacheKey(t, image, "")
			},
		},
		{
			name:                    "image-sa-serviceaccount",
			pluginCacheKeyType:      credentialproviderapi.ImagePluginCacheKeyType,
			serviceAccountCacheType: kubeletconfig.ServiceAccountServiceAccountTokenCacheType,
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				saKey := generateExpectedSACacheKey(t, saParams, kubeletconfig.ServiceAccountServiceAccountTokenCacheType)
				return generateExpectedCacheKey(t, image, saKey)
			},
		},
		{
			name:                    "image-sa-token",
			pluginCacheKeyType:      credentialproviderapi.ImagePluginCacheKeyType,
			serviceAccountCacheType: kubeletconfig.TokenServiceAccountTokenCacheType,
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				saKey := generateExpectedSACacheKey(t, saParams, kubeletconfig.TokenServiceAccountTokenCacheType)
				return generateExpectedCacheKey(t, image, saKey)
			},
		},
		{
			name:                    "registry-no-sa",
			pluginCacheKeyType:      credentialproviderapi.RegistryPluginCacheKeyType,
			serviceAccountCacheType: "",
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				registry := parseRegistry(image)
				return generateExpectedCacheKey(t, registry, "")
			},
		},
		{
			name:                    "registry-sa-serviceaccount",
			pluginCacheKeyType:      credentialproviderapi.RegistryPluginCacheKeyType,
			serviceAccountCacheType: kubeletconfig.ServiceAccountServiceAccountTokenCacheType,
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				registry := parseRegistry(image)
				saKey := generateExpectedSACacheKey(t, saParams, kubeletconfig.ServiceAccountServiceAccountTokenCacheType)
				return generateExpectedCacheKey(t, registry, saKey)
			},
		},
		{
			name:                    "registry-sa-token",
			pluginCacheKeyType:      credentialproviderapi.RegistryPluginCacheKeyType,
			serviceAccountCacheType: kubeletconfig.TokenServiceAccountTokenCacheType,
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				registry := parseRegistry(image)
				saKey := generateExpectedSACacheKey(t, saParams, kubeletconfig.TokenServiceAccountTokenCacheType)
				return generateExpectedCacheKey(t, registry, saKey)
			},
		},
		{
			name:                    "global-no-sa",
			pluginCacheKeyType:      credentialproviderapi.GlobalPluginCacheKeyType,
			serviceAccountCacheType: "",
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				return generateExpectedCacheKey(t, globalCacheKey, "")
			},
		},
		{
			name:                    "global-sa-serviceaccount",
			pluginCacheKeyType:      credentialproviderapi.GlobalPluginCacheKeyType,
			serviceAccountCacheType: kubeletconfig.ServiceAccountServiceAccountTokenCacheType,
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				saKey := generateExpectedSACacheKey(t, saParams, kubeletconfig.ServiceAccountServiceAccountTokenCacheType)
				return generateExpectedCacheKey(t, globalCacheKey, saKey)
			},
		},
		{
			name:                    "global-sa-token",
			pluginCacheKeyType:      credentialproviderapi.GlobalPluginCacheKeyType,
			serviceAccountCacheType: kubeletconfig.TokenServiceAccountTokenCacheType,
			expectedCacheKeyFunc: func(t *testing.T, image string, saParams *cacheKeyParams) string {
				saKey := generateExpectedSACacheKey(t, saParams, kubeletconfig.TokenServiceAccountTokenCacheType)
				return generateExpectedCacheKey(t, globalCacheKey, saKey)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var saTokenCacheType *kubeletconfig.ServiceAccountTokenCacheType
			if tc.serviceAccountCacheType != "" {
				saTokenCacheType = &tc.serviceAccountCacheType
			}
			pluginProvider := createTestPerPodPluginProvider(t, tc.pluginCacheKeyType, saTokenCacheType)

			// 使用固定镜像与 SA 参数，便于精确比较生成的缓存键。
			testImage := "test.registry.io/foo/bar"
			var cacheTypeValue kubeletconfig.ServiceAccountTokenCacheType
			if saTokenCacheType != nil {
				cacheTypeValue = *saTokenCacheType
			}
			saParams := &cacheKeyParams{
				namespace:          "namespace",
				serviceAccountName: "service-account-name",
				serviceAccountUID:  "service-account-uid",
				saAnnotations: map[string]string{
					"prefix.io/annotation-1": "value1",
					"prefix.io/annotation-2": "value2",
				},
				podName:     "pod-name",
				podUID:      "pod-uid",
				saTokenHash: getHashIfNotEmpty("token"),
				cacheType:   cacheTypeValue,
			}

			dockerConfig, _ := pluginProvider.provideWithCoordinates(testImage)
			verifyDockerConfig(t, dockerConfig)

			// 断言缓存键由插件缓存类型与 SA 缓存类型共同决定。
			expectedCacheKey := tc.expectedCacheKeyFunc(t, testImage, saParams)
			actualCacheKeys := pluginProvider.provider.cache.ListKeys()

			if len(actualCacheKeys) != 1 || actualCacheKeys[0] != expectedCacheKey {
				t.Errorf("Expected cache key %q, got %v", expectedCacheKey, actualCacheKeys)
			}

			// 清空插件后再次取凭证，确保第二次结果来自缓存。
			pluginProvider.provider.plugin = nil
			cachedConfig, _ := pluginProvider.provideWithCoordinates(testImage)
			verifyDockerConfig(t, cachedConfig)
		})
	}
}

// createTestPerPodPluginProvider 构造带缓存及可选 SA provider 的测试用 provider。
func createTestPerPodPluginProvider(t *testing.T, pluginCacheKeyType credentialproviderapi.PluginCacheKeyType, saTokenCacheType *kubeletconfig.ServiceAccountTokenCacheType) *perPodPluginProvider {
	t.Helper()

	tclock := clock.RealClock{}

	provider := &pluginProvider{
		clock:          tclock,
		lastCachePurge: tclock.Now(),
		matchImages:    []string{"*.registry.io"},
		cache:          cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: tclock}),
		plugin: &fakeExecPlugin{
			cacheKeyType:  pluginCacheKeyType,
			cacheDuration: time.Hour,
			auth: map[string]credentialproviderapi.AuthConfig{
				"*.registry.io": {
					Username: "user",
					Password: "password",
				},
			},
		},
	}

	// 仅在指定 SA 缓存类型时装配 SA provider，覆盖未启用 SA 的场景。
	if saTokenCacheType != nil {
		provider.serviceAccountProvider = &serviceAccountProvider{
			audience:                             "audience",
			requiredServiceAccountAnnotationKeys: []string{"prefix.io/annotation-1", "prefix.io/annotation-2"},
			cacheType:                            *saTokenCacheType,
			getServiceAccountFunc: func(namespace, name string) (*v1.ServiceAccount, error) {
				return &v1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
						Name:      name,
						UID:       "service-account-uid",
						Annotations: map[string]string{
							"prefix.io/annotation-1": "value1",
							"prefix.io/annotation-2": "value2",
						},
					},
				}, nil
			},
			getServiceAccountTokenFunc: func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
				return &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{Token: "token"}}, nil
			},
		}
	}

	return &perPodPluginProvider{
		provider:           provider,
		podName:            "pod-name",
		podNamespace:       "namespace",
		podUID:             types.UID("pod-uid"),
		serviceAccountName: "service-account-name",
	}
}

// generateExpectedSACacheKey 按给定缓存类型生成期望的 SA 缓存键。
func generateExpectedSACacheKey(t *testing.T, params *cacheKeyParams, cacheType kubeletconfig.ServiceAccountTokenCacheType) string {
	t.Helper()

	params.cacheType = cacheType
	key, err := generateServiceAccountCacheKey(*params)
	if err != nil {
		t.Fatalf("Failed to generate SA cache key: %v", err)
	}
	return key
}

// generateExpectedCacheKey 拼接基础键与 SA 缓存键，生成期望的缓存键。
func generateExpectedCacheKey(t *testing.T, baseKey, saCacheKey string) string {
	t.Helper()

	key, err := generateCacheKey(baseKey, saCacheKey)
	if err != nil {
		t.Fatalf("Failed to generate cache key: %v", err)
	}
	return key
}

// verifyDockerConfig 断言凭证内容与测试夹具中预设的一致。
func verifyDockerConfig(t *testing.T, config credentialprovider.DockerConfig) {
	t.Helper()

	expected := credentialprovider.DockerConfig{
		"*.registry.io": credentialprovider.DockerConfigEntry{
			Username: "user",
			Password: "password",
		},
	}

	if !reflect.DeepEqual(config, expected) {
		t.Errorf("Docker config mismatch. Got %v, expected %v", config, expected)
	}
}
