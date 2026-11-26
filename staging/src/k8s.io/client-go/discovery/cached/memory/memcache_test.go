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

package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apidiscovery "k8s.io/api/apidiscovery/v2"
	errorsutil "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	"k8s.io/client-go/rest"
	testutil "k8s.io/client-go/util/testing"
	"k8s.io/klog/v2/ktesting"
)

// resourceMapEntry 保存某个 groupVersion 下的资源列表及获取时产生的错误，用来模拟服务端发现接口的响应。
type resourceMapEntry struct {
	list *metav1.APIResourceList
	err  error
}

// fakeDiscovery 是内存缓存客户端的最小化假实现。
// 它刻意只嵌入 discovery.DiscoveryInterface（而不是 *fake.FakeDiscovery），
// 因为被测代码只依赖并部分覆盖该接口；如果继承完整的 FakeDiscovery，
// 还需要额外覆盖 ServerGroupsWithContext 等方法，得不偿失。
type fakeDiscovery struct {
	// 有意只实现 discovery.DiscoveryInterface：被测代码只认识并部分覆盖该接口。
	// 若嵌入 *fake.FakeDiscovery，还得额外覆盖继承来的 ServerGroupsWithContext 等方法。
	discovery.DiscoveryInterface

	lock         sync.Mutex
	groupList    *metav1.APIGroupList
	groupListErr error
	resourceMap  map[string]*resourceMapEntry
}

// ServerResourcesForGroupVersion 返回指定 groupVersion 下预先注入的资源列表；
// 未注入的 groupVersion 一律返回 "doesn't exist" 错误。
func (c *fakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if rl, ok := c.resourceMap[groupVersion]; ok {
		return rl.list, rl.err
	}
	return nil, errors.New("doesn't exist")
}

// ServerGroups 返回预先注入的 group 列表；groupList 为 nil 时表示该数据尚未就绪，返回错误。
func (c *fakeDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.groupList == nil {
		return nil, errors.New("doesn't exist")
	}
	return c.groupList, c.groupListErr
}

// fakeDiscoveryWithContext 是 fakeDiscovery 的一个变体，只公开支持 *WithContext 系列方法。
// 非 Context 版本被刻意实现为 panic，以便断言被测代码确实走的是带 Context 的路径。
type fakeDiscoveryWithContext struct {
	*fakeDiscovery
	discovery.DiscoveryInterfaceWithContext

	// 每次 ServerGroupsWithContext 返回前调用。
	serverGroupsWithContextPostCallback func()
}

// ServerResourcesForGroupVersion 被禁用：若被调用说明客户端错误地退回了非 Context 接口。
func (c *fakeDiscoveryWithContext) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	panic("fakeDiscoveryWithContext.ServerResourcesForGroupVersion should not have been called")
}

// ServerGroups 被禁用：若被调用说明客户端错误地退回了非 Context 接口。
func (c *fakeDiscoveryWithContext) ServerGroups() (*metav1.APIGroupList, error) {
	panic("fakeDiscoveryWithContext.ServerGroups should not have been called")
}

// ServerResourcesForGroupVersionWithContext 先检查取消状态，再委托给内嵌的 fakeDiscovery。
func (c *fakeDiscoveryWithContext) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.fakeDiscovery.ServerResourcesForGroupVersion(groupVersion)
}

// ServerGroupsWithContext 先检查取消状态再返回数据；回调在返回前执行，用于在刷新过程中模拟取消。
func (c *fakeDiscoveryWithContext) ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error) {
	if c.serverGroupsWithContextPostCallback != nil {
		defer c.serverGroupsWithContextPostCallback()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.fakeDiscovery.ServerGroups()
}

// TestClient 对基于 DiscoveryInterface 与 DiscoveryInterfaceWithContext 的两条代码路径做相同的行为验证。
func TestClient(t *testing.T) {
	// newFake 构造一份包含单个 group 与单个资源条目的固定发现数据。
	newFake := func() *fakeDiscovery {
		return &fakeDiscovery{
			groupList: &metav1.APIGroupList{
				Groups: []metav1.APIGroup{{
					Name: "astronomy",
					Versions: []metav1.GroupVersionForDiscovery{{
						GroupVersion: "astronomy/v8beta1",
						Version:      "v8beta1",
					}},
				}},
			},
			resourceMap: map[string]*resourceMapEntry{
				"astronomy/v8beta1": {
					list: &metav1.APIResourceList{
						GroupVersion: "astronomy/v8beta1",
						APIResources: []metav1.APIResource{{
							Name:         "dwarfplanets",
							SingularName: "dwarfplanet",
							Namespaced:   true,
							Kind:         "DwarfPlanet",
							ShortNames:   []string{"dp"},
						}},
					},
				},
			},
		}
	}
	fake := newFake()
	fakeWithContext := &fakeDiscoveryWithContext{fakeDiscovery: newFake()}

	t.Run("DiscoveryInterface", func(t *testing.T) { testClient(t, fake, fake) })
	t.Run("DiscoveryInterfaceWithContext", func(t *testing.T) { testClient(t, fakeWithContext.fakeDiscovery, fakeWithContext) })
}

// testClient 验证缓存客户端的基本生命周期：初始不新鲜、读取后变新鲜、失效后重新拉取。
// fake 用于断言返回内容，delegate 是实际传给缓存客户端的发现接口实现。
func testClient(t *testing.T, fake *fakeDiscovery, delegate discovery.DiscoveryInterface) {
	c := NewMemCacheClient(delegate)
	// 尚未加载任何数据，缓存应报告不新鲜。
	if c.Fresh() {
		t.Errorf("Expected not fresh.")
	}
	g, err := c.ServerGroups()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.groupList, g; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
	// 成功拉取后缓存应变新鲜。
	if !c.Fresh() {
		t.Errorf("Expected fresh.")
	}
	c.Invalidate()
	// Invalidate 后缓存应立即回到不新鲜状态。
	if c.Fresh() {
		t.Errorf("Expected not fresh.")
	}

	g, err = c.ServerGroups()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.groupList, g; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
	if !c.Fresh() {
		t.Errorf("Expected fresh.")
	}
	r, err := c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.resourceMap["astronomy/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}

	// 替换底层数据源，模拟服务端资源列表发生了变化。
	fake.lock.Lock()
	fake.resourceMap = map[string]*resourceMapEntry{
		"astronomy/v8beta1": {
			list: &metav1.APIResourceList{
				GroupVersion: "astronomy/v8beta1",
				APIResources: []metav1.APIResource{{
					Name:         "stars",
					SingularName: "star",
					Namespaced:   true,
					Kind:         "Star",
					ShortNames:   []string{"s"},
				}},
			},
		},
	}
	fake.lock.Unlock()

	// 只有先失效缓存，才能读到新数据。
	c.Invalidate()
	r, err = c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.resourceMap["astronomy/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
}

// TestRetryAfterCancellation 验证刷新过程中 context 被取消后，用新 context 重试能够成功。
// 这覆盖了按 groupVersion 分别处理错误的代码路径。
func TestRetryAfterCancellation(t *testing.T) {
	fake := &fakeDiscoveryWithContext{
		fakeDiscovery: &fakeDiscovery{
			groupList: &metav1.APIGroupList{
				Groups: []metav1.APIGroup{{
					Name: "astronomy",
					Versions: []metav1.GroupVersionForDiscovery{{
						GroupVersion: "astronomy/v8beta1",
						Version:      "v8beta1",
					}},
				}},
			},
			resourceMap: map[string]*resourceMapEntry{
				"astronomy/v8beta1": {
					list: &metav1.APIResourceList{
						GroupVersion: "astronomy/v8beta1",
						APIResources: []metav1.APIResource{{
							Name:         "dwarfplanets",
							SingularName: "dwarfplanet",
							Namespaced:   true,
							Kind:         "DwarfPlanet",
							ShortNames:   []string{"dp"},
						}},
					},
				},
			},
		},
	}

	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := NewMemCacheClientWithContext(fake)

	// 在 refreshLocked 中途取消 context，以覆盖按 groupVersion 处理错误的代码路径。
	fake.serverGroupsWithContextPostCallback = cancel
	_, err := c.ServerResourcesForGroupVersionWithContext(cancelCtx, "astronomy/v8beta1")
	if err == nil {
		t.Errorf("Expected error")
	}

	// 使用未取消的 context 重试：缓存不应把中途取消留下的错误当作永久失败。
	r, err := c.ServerResourcesForGroupVersionWithContext(ctx, "astronomy/v8beta1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.resourceMap["astronomy/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
	if !c.FreshWithContext(ctx) {
		t.Errorf("Expected not fresh.")
	}
}

// TestServerGroupsFails 验证 ServerGroups 失败时缓存保持不新鲜，后续资源查询会重新触发刷新。
func TestServerGroupsFails(t *testing.T) {
	fake := &fakeDiscovery{
		groupList: &metav1.APIGroupList{
			Groups: []metav1.APIGroup{{
				Name: "astronomy",
				Versions: []metav1.GroupVersionForDiscovery{{
					GroupVersion: "astronomy/v8beta1",
					Version:      "v8beta1",
				}},
			}},
		},
		groupListErr: errors.New("some error"),
		resourceMap: map[string]*resourceMapEntry{
			"astronomy/v8beta1": {
				list: &metav1.APIResourceList{
					GroupVersion: "astronomy/v8beta1",
					APIResources: []metav1.APIResource{{
						Name:         "dwarfplanets",
						SingularName: "dwarfplanet",
						Namespaced:   true,
						Kind:         "DwarfPlanet",
						ShortNames:   []string{"dp"},
					}},
				},
			},
		},
	}

	c := NewMemCacheClient(fake)
	if c.Fresh() {
		t.Errorf("Expected not fresh.")
	}
	_, err := c.ServerGroups()
	if err == nil {
		t.Errorf("Expected error")
	}
	// 拉取失败不应把缓存标记为新鲜，否则后续查询会一直命中坏数据。
	if c.Fresh() {
		t.Errorf("Expected not fresh.")
	}
	fake.lock.Lock()
	fake.groupListErr = nil
	fake.lock.Unlock()
	r, err := c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.resourceMap["astronomy/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
	if !c.Fresh() {
		t.Errorf("Expected not fresh.")
	}
}

// TestPartialPermanentFailure 验证某个 groupVersion 永久失败不影响其他 groupVersion，
// 且永久错误会被缓存、在 Invalidate 之前不会重试。
func TestPartialPermanentFailure(t *testing.T) {
	fake := &fakeDiscovery{
		groupList: &metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "astronomy",
					Versions: []metav1.GroupVersionForDiscovery{{
						GroupVersion: "astronomy/v8beta1",
						Version:      "v8beta1",
					}},
				},
				{
					Name: "astronomy2",
					Versions: []metav1.GroupVersionForDiscovery{{
						GroupVersion: "astronomy2/v8beta1",
						Version:      "v8beta1",
					}},
				},
			},
		},
		resourceMap: map[string]*resourceMapEntry{
			"astronomy/v8beta1": {
				err: errors.New("some permanent error"),
			},
			"astronomy2/v8beta1": {
				list: &metav1.APIResourceList{
					GroupVersion: "astronomy2/v8beta1",
					APIResources: []metav1.APIResource{{
						Name:         "dwarfplanets",
						SingularName: "dwarfplanet",
						Namespaced:   true,
						Kind:         "DwarfPlanet",
						ShortNames:   []string{"dp"},
					}},
				},
			},
		},
	}

	c := NewMemCacheClient(fake)
	if c.Fresh() {
		t.Errorf("Expected not fresh.")
	}
	r, err := c.ServerResourcesForGroupVersion("astronomy2/v8beta1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.resourceMap["astronomy2/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
	_, err = c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

	fake.lock.Lock()
	fake.resourceMap["astronomy/v8beta1"] = &resourceMapEntry{
		list: &metav1.APIResourceList{
			GroupVersion: "astronomy/v8beta1",
			APIResources: []metav1.APIResource{{
				Name:         "dwarfplanets",
				SingularName: "dwarfplanet",
				Namespaced:   true,
				Kind:         "DwarfPlanet",
				ShortNames:   []string{"dp"},
			}},
		},
		err: nil,
	}
	fake.lock.Unlock()
	// 永久错误不应重试，因此这里仍应失败。
	_, err = c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	c.Invalidate()

	// Invalidate 后应重新尝试。
	r, err = c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.resourceMap["astronomy/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
}

// TestPartialRetryableFailure 验证 retryable 错误（如 503）会在下次查询时自动重试，
// 而成功后即使底层数据再变坏也应命中缓存、不再重试。
func TestPartialRetryableFailure(t *testing.T) {
	fake := &fakeDiscovery{
		groupList: &metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "astronomy",
					Versions: []metav1.GroupVersionForDiscovery{{
						GroupVersion: "astronomy/v8beta1",
						Version:      "v8beta1",
					}},
				},
				{
					Name: "astronomy2",
					Versions: []metav1.GroupVersionForDiscovery{{
						GroupVersion: "astronomy2/v8beta1",
						Version:      "v8beta1",
					}},
				},
			},
		},
		resourceMap: map[string]*resourceMapEntry{
			"astronomy/v8beta1": {
				err: &errorsutil.StatusError{
					ErrStatus: metav1.Status{
						Message: "Some retryable error",
						Code:    int32(http.StatusServiceUnavailable),
						Reason:  metav1.StatusReasonServiceUnavailable,
					},
				},
			},
			"astronomy2/v8beta1": {
				list: &metav1.APIResourceList{
					GroupVersion: "astronomy2/v8beta1",
					APIResources: []metav1.APIResource{{
						Name:         "dwarfplanets",
						SingularName: "dwarfplanet",
						Namespaced:   true,
						Kind:         "DwarfPlanet",
						ShortNames:   []string{"dp"},
					}},
				},
			},
		},
	}

	c := NewMemCacheClient(fake)
	if c.Fresh() {
		t.Errorf("Expected not fresh.")
	}
	r, err := c.ServerResourcesForGroupVersion("astronomy2/v8beta1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if e, a := fake.resourceMap["astronomy2/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
	_, err = c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

	fake.lock.Lock()
	fake.resourceMap["astronomy/v8beta1"] = &resourceMapEntry{
		list: &metav1.APIResourceList{
			GroupVersion: "astronomy/v8beta1",
			APIResources: []metav1.APIResource{{
				Name:         "dwarfplanets",
				SingularName: "dwarfplanet",
				Namespaced:   true,
				Kind:         "DwarfPlanet",
				ShortNames:   []string{"dp"},
			}},
		},
		err: nil,
	}
	fake.lock.Unlock()
	// retryable 错误无需 Invalidate 也会重试，因此这里不应报错。
	r, err = c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if e, a := fake.resourceMap["astronomy/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}

	// 确认最后一次的结果已缓存，后续不再重试。
	fake.lock.Lock()
	fake.resourceMap["astronomy/v8beta1"].err = errors.New("some permanent error")
	fake.lock.Unlock()
	r, err = c.ServerResourcesForGroupVersion("astronomy/v8beta1")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if e, a := fake.resourceMap["astronomy/v8beta1"].list, r; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %#v, got %#v", e, a)
	}
}

// 该测试确认：OpenAPI 客户端与解析出的 schema 会被缓存，连续调用返回同一指针；
// 而 Invalidate 之后必须重新构造，返回内容相同但指针不同。
func TestOpenAPIMemCache(t *testing.T) {
	fakeServer, err := testutil.NewFakeOpenAPIV3Server("../../testdata")
	require.NoError(t, err)
	defer fakeServer.HttpServer.Close()

	require.NotEmpty(t, fakeServer.ServedDocuments)

	client := NewMemCacheClient(
		discovery.NewDiscoveryClientForConfigOrDie(
			&rest.Config{Host: fakeServer.HttpServer.URL},
		),
	)
	openapiClient := client.OpenAPIV3()

	paths, err := openapiClient.Paths()
	require.NoError(t, err)

	contentTypes := []string{
		runtime.ContentTypeJSON, openapi.ContentTypeOpenAPIV3PB,
	}

	for _, contentType := range contentTypes {
		t.Run(contentType, func(t *testing.T) {
			for k, v := range paths {
				original, err := v.Schema(contentType)
				if !assert.NoError(t, err) {
					continue
				}

				// 这是最初的 OpenAPI 客户端，不受后续 Invalidate 影响。
				pathsAgain, err := openapiClient.Paths()
				if !assert.NoError(t, err) {
					continue
				}

				schemaAgain, err := pathsAgain[k].Schema(contentType)
				if !assert.NoError(t, err) {
					continue
				}

				// map 指针可能不同，但内容应保持一致。
				assert.Equal(t, reflect.ValueOf(paths[k]).Pointer(), reflect.ValueOf(pathsAgain[k]).Pointer())
				assert.Equal(t, reflect.ValueOf(original).Pointer(), reflect.ValueOf(schemaAgain).Pointer())

				// Invalidate 后重新获取，此时指针不应相同。
				client.Invalidate()

				pathsAgain, err = client.OpenAPIV3().Paths()
				if !assert.NoError(t, err) {
					continue
				}

				schemaAgain, err = pathsAgain[k].Schema(contentType)
				if !assert.NoError(t, err) {
					continue
				}

				assert.NotEqual(t, reflect.ValueOf(paths[k]).Pointer(), reflect.ValueOf(pathsAgain[k]).Pointer())
				assert.NotEqual(t, reflect.ValueOf(original).Pointer(), reflect.ValueOf(schemaAgain).Pointer())
				assert.Equal(t, original, schemaAgain)
			}
		})
	}
}

// 与 TestOpenAPIMemCache 类似，但走 *WithContext API：此时连 paths map 本身也会被缓存。
func TestOpenAPIMemCacheWithContext(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	fakeServer, err := testutil.NewFakeOpenAPIV3Server("../../testdata")
	require.NoError(t, err)
	defer fakeServer.HttpServer.Close()

	require.NotEmpty(t, fakeServer.ServedDocuments)

	client := NewMemCacheClientWithContext(
		discovery.NewDiscoveryClientForConfigOrDie(
			&rest.Config{Host: fakeServer.HttpServer.URL},
		),
	)
	openapiClient := client.OpenAPIV3WithContext(ctx)

	paths, err := openapiClient.PathsWithContext(ctx)
	require.NoError(t, err)

	contentTypes := []string{
		runtime.ContentTypeJSON, openapi.ContentTypeOpenAPIV3PB,
	}

	for _, contentType := range contentTypes {
		t.Run(contentType, func(t *testing.T) {
			//nolint:testifylint // "for error assertions use require" is a false positive here, we abort the iteration on failures.
			for k, v := range paths {
				original, err := v.SchemaWithContext(ctx, contentType)
				if !assert.NoError(t, err) {
					continue
				}

				// 这是最初的 OpenAPI 客户端，不受后续 Invalidate 影响。
				pathsAgain, err := openapiClient.PathsWithContext(ctx)
				if !assert.NoError(t, err) {
					continue
				}

				schemaAgain, err := pathsAgain[k].SchemaWithContext(ctx, contentType)
				if !assert.NoError(t, err) {
					continue
				}

				// 使用 *WithContext API 时，连 map 本身也会被缓存。
				assert.Equal(t, reflect.ValueOf(paths).Pointer(), reflect.ValueOf(pathsAgain).Pointer())
				assert.Equal(t, reflect.ValueOf(original).Pointer(), reflect.ValueOf(schemaAgain).Pointer())

				// Invalidate 后重新获取，此时指针不应相同。
				client.InvalidateWithContext(ctx)

				pathsAgain, err = client.OpenAPIV3WithContext(ctx).PathsWithContext(ctx)
				if !assert.NoError(t, err) {
					continue
				}

				schemaAgain, err = pathsAgain[k].SchemaWithContext(ctx, contentType)
				if !assert.NoError(t, err) {
					continue
				}

				assert.NotEqual(t, reflect.ValueOf(paths).Pointer(), reflect.ValueOf(pathsAgain).Pointer())
				assert.NotEqual(t, reflect.ValueOf(original).Pointer(), reflect.ValueOf(schemaAgain).Pointer())
				assert.Equal(t, original, schemaAgain)
			}
		})
	}
}

// 覆盖旧版（分两次请求 /api 与 /apis）发现格式；该格式不返回资源，故 resourcesMap 恒为 nil。
func TestMemCacheGroupsAndMaybeResources(t *testing.T) {
	tests := []struct {
		name                  string
		corev1                *metav1.APIVersions
		apis                  *metav1.APIGroupList
		expectedGroupNames    []string
		expectedGroupVersions []string
	}{
		{
			name: "Legacy discovery format: 1 version at /api, 1 group at /apis",
			corev1: &metav1.APIVersions{
				Versions: []string{
					"v1",
				},
			},
			apis: &metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "extensions",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "extensions/v1beta1"},
						},
					},
				},
			},
			expectedGroupNames:    []string{"", "extensions"},
			expectedGroupVersions: []string{"v1", "extensions/v1beta1"},
		},
		{
			name: "Legacy discovery format: 1 version at /api, 2 groups/1 version at /apis",
			corev1: &metav1.APIVersions{
				Versions: []string{
					"v1",
				},
			},
			apis: &metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "apps",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "apps/v1"},
						},
					},
					{
						Name: "extensions",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "extensions/v1beta1"},
						},
					},
				},
			},
			expectedGroupNames:    []string{"", "apps", "extensions"},
			expectedGroupVersions: []string{"v1", "apps/v1", "extensions/v1beta1"},
		},
		{
			name: "Legacy discovery format: 1 version at /api, 2 groups/2 versions at /apis",
			corev1: &metav1.APIVersions{
				Versions: []string{
					"v1",
				},
			},
			apis: &metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "batch",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "batch/v1"},
						},
					},
					{
						Name: "batch",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "batch/v1beta1"},
						},
					},
					{
						Name: "extensions",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "extensions/v1beta1"},
						},
					},
					{
						Name: "extensions",
						Versions: []metav1.GroupVersionForDiscovery{
							{GroupVersion: "extensions/v1alpha1"},
						},
					},
				},
			},
			expectedGroupNames: []string{
				"",
				"batch",
				"extensions",
			},
			expectedGroupVersions: []string{
				"v1",
				"batch/v1",
				"batch/v1beta1",
				"extensions/v1beta1",
				"extensions/v1alpha1",
			},
		},
	}

	for _, test := range tests {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var body interface{}
			switch req.URL.Path {
			case "/api":
				body = test.corev1
			case "/apis":
				body = test.apis
			default:
				w.WriteHeader(http.StatusNotFound)
				return
			}
			output, err := json.Marshal(body)
			if err != nil {
				t.Errorf("unexpected error %v", err)
				return
			}
			// 声明旧版未聚合发现格式，该响应不返回资源。
			w.Header().Set("Content-Type", discovery.AcceptV1)
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		}))
		defer server.Close()
		client := discovery.NewDiscoveryClientForConfigOrDie(&rest.Config{Host: server.URL})
		memClient := memCacheClient{
			delegate:               client,
			groupToServerResources: map[string]*cacheEntry{},
		}
		assert.False(t, memClient.Fresh())
		apiGroupList, resourcesMap, failedGVs, err := memClient.GroupsAndMaybeResources()
		require.NoError(t, err)
		// 旧版未聚合发现格式下资源列表恒为 nil。
		assert.Nil(t, resourcesMap)
		assert.Emptyf(t, failedGVs, "expected empty failed GroupVersions, got (%d)", len(failedGVs))
		assert.False(t, memClient.receivedAggregatedDiscovery)
		assert.True(t, memClient.Fresh())
		// 校验返回的 group 集合。
		expectedGroupNames := sets.NewString(test.expectedGroupNames...)
		actualGroupNames := sets.NewString(groupNamesFromList(apiGroupList)...)
		assert.True(t, expectedGroupNames.Equal(actualGroupNames),
			"%s: Expected groups (%s), got (%s)", test.name, expectedGroupNames.List(), actualGroupNames.List())
		// 校验返回的 group/version 集合。
		expectedGroupVersions := sets.NewString(test.expectedGroupVersions...)
		actualGroupVersions := sets.NewString(groupVersionsFromGroups(apiGroupList)...)
		assert.True(t, expectedGroupVersions.Equal(actualGroupVersions),
			"%s: Expected group/versions (%s), got (%s)", test.name, expectedGroupVersions.List(), actualGroupVersions.List())
		// 使缓存失效后重新获取 group 与资源。
		memClient.Invalidate()
		assert.False(t, memClient.Fresh())
		apiGroupList, resourcesMap, _, err = memClient.GroupsAndMaybeResources()
		require.NoError(t, err)
		assert.Nil(t, resourcesMap)
		assert.False(t, memClient.receivedAggregatedDiscovery)
		// 校验返回的 group 集合。
		actualGroupNames = sets.NewString(groupNamesFromList(apiGroupList)...)
		assert.True(t, expectedGroupNames.Equal(actualGroupNames),
			"%s: Expected after invalidation groups (%s), got (%s)", test.name, expectedGroupNames.List(), actualGroupNames.List())
	}
}

// 覆盖 v2 aggregated discovery：一次响应即包含 group 与资源，并验证 stale 版本被计入 failedGVs。
func TestAggregatedMemCacheGroupsAndMaybeResources(t *testing.T) {
	tests := []struct {
		name                  string
		corev1                *apidiscovery.APIGroupDiscoveryList
		apis                  *apidiscovery.APIGroupDiscoveryList
		expectedGroupNames    []string
		expectedGroupVersions []string
		expectedGVKs          []string
		expectedFailedGVs     []string
	}{
		{
			name: "Aggregated discovery: 1 group/1 resources at /api, 1 group/1 resources at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroupNames:    []string{"", "apps"},
			expectedGroupVersions: []string{"v1", "apps/v1"},
			expectedGVKs: []string{
				"/v1/Pod",
				"apps/v1/Deployment",
			},
			expectedFailedGVs: []string{},
		},
		{
			name: "Aggregated discovery: 1 group/1 resources at /api, 1 group/2 versions/1 resources/1 stale GV at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
							{
								// 已失效的版本不应出现在发现结果中。
								Version: "v2",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v2",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
								Freshness: apidiscovery.DiscoveryFreshnessStale,
							},
						},
					},
				},
			},
			expectedGroupNames:    []string{"", "apps"},
			expectedGroupVersions: []string{"v1", "apps/v1"},
			expectedGVKs: []string{
				"/v1/Pod",
				"apps/v1/Deployment",
			},
			expectedFailedGVs: []string{"apps/v2"},
		},
		{
			name: "Aggregated discovery: 1 group/2 resources at /api, 1 group/2 resources at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "services",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Service",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "StatefulSet",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroupNames:    []string{"", "apps"},
			expectedGroupVersions: []string{"v1", "apps/v1"},
			expectedGVKs: []string{
				"/v1/Pod",
				"/v1/Service",
				"apps/v1/Deployment",
				"apps/v1/StatefulSet",
			},
			expectedFailedGVs: []string{},
		},
		{
			name: "Aggregated discovery: 1 group/2 resources at /api, 2 group/2 resources/1 stale GV at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "services",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Service",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "StatefulSet",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
							{
								// 已失效的版本不应出现在发现结果中。
								Version: "v1beta1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1beta1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1beta1",
											Kind:    "StatefulSet",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
								Freshness: apidiscovery.DiscoveryFreshnessStale,
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "batch",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "jobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1",
											Kind:    "Job",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "cronjobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1",
											Kind:    "CronJob",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroupNames:    []string{"", "apps", "batch"},
			expectedGroupVersions: []string{"v1", "apps/v1", "batch/v1"},
			expectedGVKs: []string{
				"/v1/Pod",
				"/v1/Service",
				"apps/v1/Deployment",
				"apps/v1/StatefulSet",
				"batch/v1/Job",
				"batch/v1/CronJob",
			},
			expectedFailedGVs: []string{"apps/v1beta1"},
		},
		{
			name:   "Aggregated discovery: /api returns nothing, 2 groups/2 resources/2 stale GV at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							// 已失效的 v1 不应出现在发现结果中。
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "StatefulSet",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
								Freshness: apidiscovery.DiscoveryFreshnessStale,
							},
							{
								Version: "v1beta1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1beta1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1beta1",
											Kind:    "StatefulSet",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "batch",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "jobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1",
											Kind:    "Job",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "cronjobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1",
											Kind:    "CronJob",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
							{
								// 已失效的版本不应出现在发现结果中。
								Version: "v1beta1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "jobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1beta1",
											Kind:    "Job",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
								Freshness: apidiscovery.DiscoveryFreshnessStale,
							},
						},
					},
				},
			},
			expectedGroupNames:    []string{"apps", "batch"},
			expectedGroupVersions: []string{"apps/v1beta1", "batch/v1"},
			expectedGVKs: []string{
				"apps/v1beta1/Deployment",
				"apps/v1beta1/StatefulSet",
				"batch/v1/Job",
				"batch/v1/CronJob",
			},
			expectedFailedGVs: []string{"apps/v1", "batch/v1beta1"},
		},
	}

	for _, test := range tests {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var agg *apidiscovery.APIGroupDiscoveryList
			switch req.URL.Path {
			case "/api":
				agg = test.corev1
			case "/apis":
				agg = test.apis
			default:
				w.WriteHeader(http.StatusNotFound)
				return
			}
			output, err := json.Marshal(agg)
			if err != nil {
				t.Errorf("unexpected error %v", err)
				return
			}
			// 声明聚合发现格式。
			w.Header().Set("Content-Type", discovery.AcceptV2)
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		}))
		defer server.Close()
		client := discovery.NewDiscoveryClientForConfigOrDie(&rest.Config{Host: server.URL})
		memClient := memCacheClient{
			delegate:               client,
			groupToServerResources: map[string]*cacheEntry{},
		}
		assert.False(t, memClient.Fresh())
		apiGroupList, resourcesMap, failedGVs, err := memClient.GroupsAndMaybeResources()
		require.NoError(t, err)
		assert.True(t, memClient.receivedAggregatedDiscovery)
		assert.True(t, memClient.Fresh())
		// 校验返回的 group 集合。
		expectedGroupNames := sets.NewString(test.expectedGroupNames...)
		actualGroupNames := sets.NewString(groupNamesFromList(apiGroupList)...)
		assert.True(t, expectedGroupNames.Equal(actualGroupNames),
			"%s: Expected groups (%s), got (%s)", test.name, expectedGroupNames.List(), actualGroupNames.List())
		// 校验返回的 group/version 集合。
		expectedGroupVersions := sets.NewString(test.expectedGroupVersions...)
		actualGroupVersions := sets.NewString(groupVersionsFromGroups(apiGroupList)...)
		assert.True(t, expectedGroupVersions.Equal(actualGroupVersions),
			"%s: Expected group/versions (%s), got (%s)", test.name, expectedGroupVersions.List(), actualGroupVersions.List())
		// 校验返回的资源列表。
		expectedGVKs := sets.NewString(test.expectedGVKs...)
		resources := []*metav1.APIResourceList{}
		for _, resourceList := range resourcesMap {
			resources = append(resources, resourceList)
		}
		actualGVKs := sets.NewString(groupVersionKinds(resources)...)
		assert.True(t, expectedGVKs.Equal(actualGVKs),
			"%s: Expected GVKs (%s), got (%s)", test.name, expectedGVKs.List(), actualGVKs.List())
		// 校验返回的失败 GroupVersion 集合。
		expectedFailedGVs := sets.NewString(test.expectedFailedGVs...)
		actualFailedGVs := sets.NewString(failedGroupVersions(failedGVs)...)
		assert.True(t, expectedFailedGVs.Equal(actualFailedGVs),
			"%s: Expected Failed GroupVersions (%s), got (%s)", test.name, expectedFailedGVs.List(), actualFailedGVs.List())
		// 使缓存失效后重新获取 group 列表。
		memClient.Invalidate()
		assert.False(t, memClient.Fresh())
		apiGroupList, _, _, err = memClient.GroupsAndMaybeResources()

		require.NoError(t, err)
		// 校验返回的 group 集合。
		actualGroupNames = sets.NewString(groupNamesFromList(apiGroupList)...)
		assert.True(t, expectedGroupNames.Equal(actualGroupNames),
			"%s: Expected after invalidation groups (%s), got (%s)", test.name, expectedGroupNames.List(), actualGroupNames.List())
	}
}

// 额外验证 preferred version 的取舍：列表中的第一个版本被视为首选版本。
func TestMemCacheAggregatedServerGroups(t *testing.T) {
	tests := []struct {
		name                      string
		corev1                    *apidiscovery.APIGroupDiscoveryList
		apis                      *apidiscovery.APIGroupDiscoveryList
		expectedGroupNames        []string
		expectedGroupVersions     []string
		expectedPreferredVersions []string
	}{
		{
			name: "Aggregated discovery: 1 group/1 version at /api, 1 group/1 version at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroupNames:        []string{"", "apps"},
			expectedGroupVersions:     []string{"v1", "apps/v1"},
			expectedPreferredVersions: []string{"v1", "apps/v1"},
		},
		{
			name: "Aggregated discovery: 1 group/1 version at /api, 1 group/2 versions at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							// 排在首位，因此选 v2 为首选版本。
							{
								Version: "v2",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v2",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroupNames:        []string{"", "apps"},
			expectedGroupVersions:     []string{"v1", "apps/v1", "apps/v2"},
			expectedPreferredVersions: []string{"v1", "apps/v2"},
		},
		{
			name:   "Aggregated discovery: /api returns nothing, 2 groups at /apis",
			corev1: &apidiscovery.APIGroupDiscoveryList{},
			apis: &apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "StatefulSet",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "batch",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							// 排在首位，因此选 v1 为首选版本。
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "jobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1",
											Kind:    "Job",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "cronjobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1",
											Kind:    "CronJob",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
							{
								Version: "v1beta1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "jobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1beta1",
											Kind:    "Job",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "cronjobs",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "batch",
											Version: "v1beta1",
											Kind:    "CronJob",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroupNames:        []string{"apps", "batch"},
			expectedGroupVersions:     []string{"apps/v1", "batch/v1", "batch/v1beta1"},
			expectedPreferredVersions: []string{"apps/v1", "batch/v1"},
		},
	}

	for _, test := range tests {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var agg *apidiscovery.APIGroupDiscoveryList
			switch req.URL.Path {
			case "/api":
				agg = test.corev1
			case "/apis":
				agg = test.apis
			default:
				w.WriteHeader(http.StatusNotFound)
				return
			}
			output, err := json.Marshal(agg)
			if err != nil {
				t.Errorf("unexpected error %v", err)
				return
			}
			// 声明聚合发现格式。
			w.Header().Set("Content-Type", discovery.AcceptV2)
			w.WriteHeader(http.StatusOK)
			w.Write(output)
		}))
		defer server.Close()
		client := discovery.NewDiscoveryClientForConfigOrDie(&rest.Config{Host: server.URL})
		memCacheClient := NewMemCacheClient(client)
		assert.False(t, memCacheClient.Fresh())
		apiGroupList, err := memCacheClient.ServerGroups()
		require.NoError(t, err)
		assert.True(t, memCacheClient.Fresh())
		// 校验返回的 group 集合。
		expectedGroupNames := sets.NewString(test.expectedGroupNames...)
		actualGroupNames := sets.NewString(groupNamesFromList(apiGroupList)...)
		assert.True(t, expectedGroupNames.Equal(actualGroupNames),
			"%s: Expected groups (%s), got (%s)", test.name, expectedGroupNames.List(), actualGroupNames.List())
		// 校验返回的 group/version 集合。
		expectedGroupVersions := sets.NewString(test.expectedGroupVersions...)
		actualGroupVersions := sets.NewString(groupVersionsFromGroups(apiGroupList)...)
		assert.True(t, expectedGroupVersions.Equal(actualGroupVersions),
			"%s: Expected group/versions (%s), got (%s)", test.name, expectedGroupVersions.List(), actualGroupVersions.List())
		// 校验每个 group 的首选版本。
		expectedPreferredVersions := sets.NewString(test.expectedPreferredVersions...)
		actualPreferredVersions := sets.NewString(preferredVersionsFromList(apiGroupList)...)
		assert.True(t, expectedPreferredVersions.Equal(actualPreferredVersions),
			"%s: Expected preferred group/version (%s), got (%s)", test.name, expectedPreferredVersions.List(), actualPreferredVersions.List())
		// 使缓存失效后重新获取 group 列表。
		memCacheClient.Invalidate()
		assert.False(t, memCacheClient.Fresh())
		apiGroupList, err = memCacheClient.ServerGroups()
		require.NoError(t, err)
		// 校验返回的 group 集合。
		actualGroupNames = sets.NewString(groupNamesFromList(apiGroupList)...)
		assert.True(t, expectedGroupNames.Equal(actualGroupNames),
			"%s: Expected after invalidation groups (%s), got (%s)", test.name, expectedGroupNames.List(), actualGroupNames.List())
	}
}

// groupNamesFromList 提取 group 列表中的名称，便于用 sets 做无序比较。
func groupNamesFromList(groups *metav1.APIGroupList) []string {
	result := []string{}
	for _, group := range groups.Groups {
		result = append(result, group.Name)
	}
	return result
}

// preferredVersionsFromList 提取每个 group 的 preferred groupVersion，用于断言首选版本。
func preferredVersionsFromList(groups *metav1.APIGroupList) []string {
	result := []string{}
	for _, group := range groups.Groups {
		preferredGV := group.PreferredVersion.GroupVersion
		result = append(result, preferredGV)
	}
	return result
}

// groupVersionsFromGroups 摊平所有 group 的所有版本，得到完整的 groupVersion 集合。
func groupVersionsFromGroups(groups *metav1.APIGroupList) []string {
	result := []string{}
	for _, group := range groups.Groups {
		for _, version := range group.Versions {
			result = append(result, version.GroupVersion)
		}
	}
	return result
}

// groupVersionKinds 把资源列表展开为 "group/version/kind" 形式的字符串集合。
func groupVersionKinds(resources []*metav1.APIResourceList) []string {
	result := []string{}
	for _, resourceList := range resources {
		for _, resource := range resourceList.APIResources {
			gvk := fmt.Sprintf("%s/%s/%s", resource.Group, resource.Version, resource.Kind)
			result = append(result, gvk)
		}
	}
	return result
}

// failedGroupVersions 把失败的 groupVersion 映射转成字符串集合，方便断言。
func failedGroupVersions(gvs map[schema.GroupVersion]error) []string {
	result := []string{}
	for gv := range gvs {
		result = append(result, gv.String())
	}
	return result
}
