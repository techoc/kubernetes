/*
Copyright 2016 The Kubernetes Authors.

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

package filters

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	apifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	fcmetrics "k8s.io/apiserver/pkg/util/flowcontrol/metrics"
)

// createMaxInflightServer 构造带 max-in-flight 过滤器的测试服务器。
// callsWg 用于等待请求到达，blockWg 用于把请求卡在处理中，从而稳定地复现并发饱和场景。
func createMaxInflightServer(t *testing.T, callsWg, blockWg *sync.WaitGroup, disableCallsWg *bool, disableCallsWgMutex *sync.Mutex, nonMutating, mutating int) *httptest.Server {
	fcmetrics.Register()
	longRunningRequestCheck := BasicLongRunningRequestCheck(sets.NewString("watch"), sets.NewString("proxy"))

	requestInfoFactory := &apirequest.RequestInfoFactory{APIPrefixes: sets.NewString("apis", "api"), GrouplessAPIPrefixes: sets.NewString("api")}
	handler := WithMaxInFlightLimit(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 不等待 block WaitGroup 的短请求（计入配额）。
			// dontwait 路径用于验证未饱和时请求可正常通过。
			if strings.Contains(r.URL.Path, "dontwait") {
				return
			}
			disableCallsWgMutex.Lock()
			waitForCalls := *disableCallsWg
			disableCallsWgMutex.Unlock()
			if waitForCalls {
				callsWg.Done()
			}
			t.Logf("About to blockWg.Wait(), requestURI=%v, remoteAddr=%v", r.RequestURI, r.RemoteAddr)
			blockWg.Wait()
			t.Logf("Returned from blockWg.Wait(), requestURI=%v, remoteAddr=%v", r.RequestURI, r.RemoteAddr)
		}),
		nonMutating,
		mutating,
		longRunningRequestCheck,
	)
	handler = withFakeUser(handler)
	handler = apifilters.WithRequestInfo(handler, requestInfoFactory)

	return httptest.NewServer(handler)
}

// withFakeUser 通过 Groups 请求头把测试请求标记为特权组，用于验证 system:masters 豁免。
func withFakeUser(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header["Groups"]) > 0 {
			r = r.WithContext(apirequest.WithUser(r.Context(), &user.DefaultInfo{
				Groups: r.Header["Groups"],
			}))
		}
		handler.ServeHTTP(w, r)
	})
}

// 验证 MaxInFlightLimit 生效：
//   - 被正则识别的 proxy/watch 等“长”请求即使长期挂起也不计入并发；
//   - “短”请求被正确计数：任意时刻在途数量不超过构造函数传入的 channel 容量；
//   - 后续的“短”请求会立即被相应的错误拒绝；
//   - 后续的“长”请求仍正常处理；
//   - 部分“短”请求完成后能正确恢复，即可以继续处理新请求。
//
// 长请求（watch/proxy）不占并发配额，即使挂起也不影响计数；
// 短请求达到上限后立即返回 429，且拒绝路径不阻塞；请求完成后配额应释放。
func TestMaxInFlightNonMutating(t *testing.T) {
	const AllowedNonMutatingInflightRequestsNo = 3

	// Calls 用于等待所有服务端调用到达。测试会发送
	// 与“短”请求数量相同的“长”请求。
	// 'short' accounted ones.
	calls := &sync.WaitGroup{}
	calls.Add(AllowedNonMutatingInflightRequestsNo * 2)

	// Responses 用于等待所有响应
	// 收到。这可以避免部分异步请求收到 EOF
	// 后过早关闭服务端。
	responses := &sync.WaitGroup{}
	responses.Add(AllowedNonMutatingInflightRequestsNo * 2)

	// Block 用于按需让请求保持在途；所有请求会
	// 在同一时刻解除阻塞。
	block := &sync.WaitGroup{}
	block.Add(1)

	waitForCalls := true
	waitForCallsMutex := sync.Mutex{}

	server := createMaxInflightServer(t, calls, block, &waitForCalls, &waitForCallsMutex, AllowedNonMutatingInflightRequestsNo, 1)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartMaxInFlightWatermarkMaintenance(ctx.Done())

	// 这些请求应挂起但不影响计数，使用 query param 匹配。
	for i := 0; i < AllowedNonMutatingInflightRequestsNo; i++ {
		// 这些请求应挂起等待 block……
		go func() {
			if err := expectHTTPGet(server.URL+"/api/v1/namespaces/default/wait?watch=true", http.StatusOK); err != nil {
				t.Error(err)
			}
			responses.Done()
		}()
	}

	// 确认未被计入的请求不会让服务端饱和。
	if err := expectHTTPGet(server.URL+"/dontwait", http.StatusOK); err != nil {
		t.Error(err)
	}

	// 这些请求应挂起且被计数，即使服务端饱和。
	for i := 0; i < AllowedNonMutatingInflightRequestsNo; i++ {
		// 这些请求应挂起等待 block……
		go func() {
			if err := expectHTTPGet(server.URL, http.StatusOK); err != nil {
				t.Error(err)
			}
			responses.Done()
		}()
	}
	// 等待服务端收到所有调用。
	calls.Wait()
	// 关闭服务端的调用通知。
	waitForCallsMutex.Lock()
	waitForCalls = false
	waitForCallsMutex.Unlock()

	// 多试几次，证明被限流拒绝的请求不会阻塞。
	for i := 0; i < 2; i++ {
		if err := expectHTTPGet(server.URL, http.StatusTooManyRequests); err != nil {
			t.Error(err)
		}
	}
	// 验证未被计入的 URL 仍可用，使用路径正则匹配。
	if err := expectHTTPGet(server.URL+"/api/v1/watch/namespaces/default/dontwait", http.StatusOK); err != nil {
		t.Error(err)
	}

	// 应允许单个变更请求。
	if err := expectHTTPPost(server.URL+"/dontwait", http.StatusOK); err != nil {
		t.Error(err)
	}

	// 让所有挂起的请求结束。
	block.Done()

	// 验证从饱和状态恢复。
	// 为避免不稳定，需等到至少一个请求真正结束。
	responses.Wait()
	if err := expectHTTPGet(server.URL, http.StatusOK); err != nil {
		t.Error(err)
	}
}

// TestMaxInFlightMutating 验证变更请求使用独立的并发配额：
// 变更配额耗尽时返回 429，但读请求仍可正常通过。
func TestMaxInFlightMutating(t *testing.T) {
	const AllowedMutatingInflightRequestsNo = 3

	calls := &sync.WaitGroup{}
	calls.Add(AllowedMutatingInflightRequestsNo)

	responses := &sync.WaitGroup{}
	responses.Add(AllowedMutatingInflightRequestsNo)

	// Block 用于让请求保持在途；所有请求同时解除阻塞。
	//
	block := &sync.WaitGroup{}
	block.Add(1)

	waitForCalls := true
	waitForCallsMutex := sync.Mutex{}

	server := createMaxInflightServer(t, calls, block, &waitForCalls, &waitForCallsMutex, 1, AllowedMutatingInflightRequestsNo)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartMaxInFlightWatermarkMaintenance(ctx.Done())

	// 这些请求应挂起且被计数，即使服务端饱和。
	for i := 0; i < AllowedMutatingInflightRequestsNo; i++ {
		// 这些请求应挂起等待 block……
		go func() {
			if err := expectHTTPPost(server.URL+"/foo/bar", http.StatusOK); err != nil {
				t.Error(err)
			}
			responses.Done()
		}()
	}
	// 等待服务端收到所有调用。
	calls.Wait()
	// 关闭服务端的调用通知。
	//
	waitForCallsMutex.Lock()
	waitForCalls = false
	waitForCallsMutex.Unlock()

	// 多试几次，证明被限流拒绝的请求不会阻塞。
	for i := 0; i < 2; i++ {
		if err := expectHTTPPost(server.URL+"/foo/bar/", http.StatusTooManyRequests); err != nil {
			t.Error(err)
		}
	}
	// 验证非变更 URL 仍可用，使用路径正则匹配。
	if err := expectHTTPGet(server.URL+"/dontwait", http.StatusOK); err != nil {
		t.Error(err)
	}

	// 让所有挂起的请求结束。
	block.Done()

	// 验证从饱和状态恢复。
	// 为避免不稳定，需等到至少一个请求真正结束。
	responses.Wait()
	if err := expectHTTPPost(server.URL+"/foo/bar", http.StatusOK); err != nil {
		t.Error(err)
	}
}

// 用 GET 作为非变更请求示例。
func expectHTTPGet(url string, code int) error {
	r, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("unexpected error: %v", err)
	}
	if r.StatusCode != code {
		return fmt.Errorf("unexpected response: %v", r.StatusCode)
	}
	return nil
}

// 用 POST 作为变更请求示例。
func expectHTTPPost(url string, code int, groups ...string) error {
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader("foo bar"))
	if err != nil {
		return err
	}
	for _, group := range groups {
		req.Header.Add("Groups", group)
	}

	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("unexpected error: %v", err)
	}
	if r.StatusCode != code {
		return fmt.Errorf("unexpected response: %v", r.StatusCode)
	}
	return nil
}

// TestMaxInFlightSkipsMasters 验证 system:masters 组绕过 max-in-flight 限制：
// 即使变更配额已耗尽，特权请求仍直接放行。
func TestMaxInFlightSkipsMasters(t *testing.T) {
	const AllowedMutatingInflightRequestsNo = 3

	calls := &sync.WaitGroup{}
	calls.Add(AllowedMutatingInflightRequestsNo)

	responses := &sync.WaitGroup{}
	responses.Add(AllowedMutatingInflightRequestsNo)

	// Block 用于让请求保持在途；所有请求同时解除阻塞。
	//
	block := &sync.WaitGroup{}
	block.Add(1)

	waitForCalls := true
	waitForCallsMutex := sync.Mutex{}

	server := createMaxInflightServer(t, calls, block, &waitForCalls, &waitForCallsMutex, 1, AllowedMutatingInflightRequestsNo)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartMaxInFlightWatermarkMaintenance(ctx.Done())

	// 这些请求应挂起且被计数，即使服务端饱和。
	for i := 0; i < AllowedMutatingInflightRequestsNo; i++ {
		// 这些请求应挂起等待 block……
		go func() {
			if err := expectHTTPPost(server.URL+"/foo/bar", http.StatusOK); err != nil {
				t.Error(err)
			}
			responses.Done()
		}()
	}
	// 等待服务端收到所有调用。
	calls.Wait()
	// 关闭服务端的调用通知。
	//
	waitForCallsMutex.Lock()
	waitForCalls = false
	waitForCallsMutex.Unlock()

	// 多试几次，证明被限流拒绝的请求不会阻塞。
	for i := 0; i < 2; i++ {
		if err := expectHTTPPost(server.URL+"/dontwait", http.StatusOK, user.SystemPrivilegedGroup); err != nil {
			t.Error(err)
		}
	}

	// 让所有挂起的请求结束。
	block.Done()

	responses.Wait()
}
