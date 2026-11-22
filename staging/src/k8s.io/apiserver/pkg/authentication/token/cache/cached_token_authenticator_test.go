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

package cache

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	mathrand "math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// TestCachedTokenAuthenticator 验证缓存的核心语义：
// 失败结果不缓存、成功结果在 TTL 内命中、TTL 到期后重新回源。
func TestCachedTokenAuthenticator(t *testing.T) {
	var (
		// 记录被委托认证器实际收到的 token 序列，用于判断缓存是否命中。
		calledWithToken []string

		resultUsers map[string]user.Info
		resultOk    bool
		resultErr   error
	)
	// 用 TokenFunc 模拟后端认证器，行为由闭包外的变量驱动。
	fakeAuth := authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		calledWithToken = append(calledWithToken, token)
		return &authenticator.Response{User: resultUsers[token]}, resultOk, resultErr
	})
	fakeClock := testingclock.NewFakeClock(time.Now())

	a := newWithClock(fakeAuth, true, time.Minute, 0, fakeClock)

	// 失败结果不应被缓存：两次调用之间推进时钟也不会改变回源次数。
	calledWithToken, resultUsers, resultOk, resultErr = []string{}, nil, false, nil
	a.AuthenticateToken(context.Background(), "bad1")
	a.AuthenticateToken(context.Background(), "bad2")
	a.AuthenticateToken(context.Background(), "bad3")
	fakeClock.Step(2 * time.Microsecond)
	a.AuthenticateToken(context.Background(), "bad1")
	a.AuthenticateToken(context.Background(), "bad2")
	a.AuthenticateToken(context.Background(), "bad3")
	fakeClock.Step(2 * time.Microsecond)
	if !reflect.DeepEqual(calledWithToken, []string{"bad1", "bad2", "bad3", "bad1", "bad2", "bad3"}) {
		t.Errorf("Expected failing calls to not stay in the cache, got %v", calledWithToken)
	}

	// reset calls, make the backend return success for three user tokens
	calledWithToken = []string{}
	resultUsers, resultOk, resultErr = map[string]user.Info{}, true, nil
	resultUsers["usertoken1"] = &user.DefaultInfo{Name: "user1"}
	resultUsers["usertoken2"] = &user.DefaultInfo{Name: "user2"}
	resultUsers["usertoken3"] = &user.DefaultInfo{Name: "user3"}

	// populate cache
	// 首次调用应回源，并缓存成功结果。
	if resp, ok, err := a.AuthenticateToken(context.Background(), "usertoken1"); err != nil || !ok || resp.User.GetName() != "user1" {
		t.Errorf("Expected user1")
	}
	if resp, ok, err := a.AuthenticateToken(context.Background(), "usertoken2"); err != nil || !ok || resp.User.GetName() != "user2" {
		t.Errorf("Expected user2")
	}
	if resp, ok, err := a.AuthenticateToken(context.Background(), "usertoken3"); err != nil || !ok || resp.User.GetName() != "user3" {
		t.Errorf("Expected user3")
	}
	if !reflect.DeepEqual(calledWithToken, []string{"usertoken1", "usertoken2", "usertoken3"}) {
		t.Errorf("Expected token calls, got %v", calledWithToken)
	}

	// reset calls, make the backend return failures
	calledWithToken = []string{}
	resultUsers, resultOk, resultErr = nil, false, nil

	// authenticate calls still succeed and backend is not hit
	// 后端改为失败，但 TTL 内仍应命中缓存返回成功结果。
	if resp, ok, err := a.AuthenticateToken(context.Background(), "usertoken1"); err != nil || !ok || resp.User.GetName() != "user1" {
		t.Errorf("Expected user1")
	}
	if resp, ok, err := a.AuthenticateToken(context.Background(), "usertoken2"); err != nil || !ok || resp.User.GetName() != "user2" {
		t.Errorf("Expected user2")
	}
	if resp, ok, err := a.AuthenticateToken(context.Background(), "usertoken3"); err != nil || !ok || resp.User.GetName() != "user3" {
		t.Errorf("Expected user3")
	}
	if !reflect.DeepEqual(calledWithToken, []string{}) {
		t.Errorf("Expected no token calls, got %v", calledWithToken)
	}

	// skip forward in time
	// 推进超过 TTL，缓存应失效并重新回源（此时后端返回失败）。
	fakeClock.Step(2 * time.Minute)

	// backend is consulted again and fails
	a.AuthenticateToken(context.Background(), "usertoken1")
	a.AuthenticateToken(context.Background(), "usertoken2")
	a.AuthenticateToken(context.Background(), "usertoken3")
	if !reflect.DeepEqual(calledWithToken, []string{"usertoken1", "usertoken2", "usertoken3"}) {
		t.Errorf("Expected token calls, got %v", calledWithToken)
	}
}

// TestCachedTokenAuthenticatorWithAudiences 验证缓存键包含 audience：
// 同一 token 在不同 audience 下应得到不同结果，不能互相命中。
func TestCachedTokenAuthenticatorWithAudiences(t *testing.T) {
	resultUsers := make(map[string]user.Info)
	fakeAuth := authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		auds, _ := authenticator.AudiencesFrom(ctx)
		return &authenticator.Response{User: resultUsers[auds[0]+token]}, true, nil
	})
	fakeClock := testingclock.NewFakeClock(time.Now())

	a := newWithClock(fakeAuth, true, time.Minute, 0, fakeClock)

	resultUsers["audAusertoken1"] = &user.DefaultInfo{Name: "user1"}
	resultUsers["audBusertoken1"] = &user.DefaultInfo{Name: "user1-different"}

	if u, ok, _ := a.AuthenticateToken(authenticator.WithAudiences(context.Background(), []string{"audA"}), "usertoken1"); !ok || u.User.GetName() != "user1" {
		t.Errorf("Expected user1")
	}
	if u, ok, _ := a.AuthenticateToken(authenticator.WithAudiences(context.Background(), []string{"audB"}), "usertoken1"); !ok || u.User.GetName() != "user1-different" {
		t.Errorf("Expected user1-different")
	}
}

// bKey 持有基准结果，防止编译器把 keyFunc 调用优化掉。
var bKey string

// use a realistic token for benchmarking
const jwtToken = `eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJvcGVuc2hpZnQtc2RuIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6InNkbi10b2tlbi1nNndtYyIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJzZG4iLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiIzYzM5YzNhYS1kM2Q5LTExZTktYTVkMC0wMmI3YjllODg1OWUiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6b3BlbnNoaWZ0LXNkbjpzZG4ifQ.PIs0rsUTekj5AX8yJeLDyW4vQB17YS4IOgO026yjEvsCY7Wv_2TD0lwyZWqyQh639q3jPh2_3LTQq2Cp0cReBP1PYOIGgprNm3C-3OFZRnkls-GH09kvPYE8J_-a1YwjxucOwytzJvEM5QTC9iXfEJNSTBfLge-HMYT1y0AGKs8DWTSC4rtd_2PedK3OYiAyDg_xHA8qNpG9pRNM8vfjV9VsmqJtlbnTVlTngqC0t5vyMaWrmLNRxN0rTbN2W9L3diXRnYqI8BUfgPQb7uhYcPuXGeypaFrN4d3yNN4NbgVxnkgdd2IXQ8elSJuQn6ynrvLgG0JPMmThOHnwvsZDeA`

// BenchmarkKeyFunc 对比有/无 audience 时缓存键计算的开销。
func BenchmarkKeyFunc(b *testing.B) {
	randomCacheKey := make([]byte, 32)
	if _, err := rand.Read(randomCacheKey); err != nil {
		b.Fatal(err) // rand should never fail
	}
	hashPool := &sync.Pool{
		New: func() interface{} {
			return hmac.New(sha256.New, randomCacheKey)
		},
	}

	// use realistic audiences for benchmarking
	auds := []string{"7daf30b7-a85c-429b-8b21-e666aecbb235", "c22aa267-bdde-4acb-8505-998be7818400", "44f9b4f3-7125-4333-b04c-1446a16c6113"}

	b.Run("has audiences", func(b *testing.B) {
		var key string
		for n := 0; n < b.N; n++ {
			key = keyFunc(hashPool, auds, jwtToken)
		}
		bKey = key
	})

	b.Run("nil audiences", func(b *testing.B) {
		var key string
		for n := 0; n < b.N; n++ {
			key = keyFunc(hashPool, nil, jwtToken)
		}
		bKey = key
	})
}

// TestSharedLookup 验证相同 key 的并发请求只会触发一次后端查询（singleflight 语义），
// 并且首个调用者取消时其余调用者仍能拿到结果。
func TestSharedLookup(t *testing.T) {
	var chewie = &authenticator.Response{User: &user.DefaultInfo{Name: "chewbacca"}}

	t.Run("actually shared", func(t *testing.T) {
		var lookups uint32
		c := make(chan struct{})
		// 后端阻塞在 c 上，让 10 个并发请求先全部排入同一个等待队列。
		a := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			<-c
			atomic.AddUint32(&lookups, 1)
			return chewie, true, nil
		}), true, time.Minute, 0)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.AuthenticateToken(context.Background(), "")
			}()
		}

		// no good way to make sure that all the callers are queued so we sleep.
		time.Sleep(1 * time.Second)
		close(c)
		wg.Wait()

		// 允许少量额外查询（等待队列边界），但不应是 10 次。
		if lookups > 3 {
			t.Fatalf("unexpected number of lookups: got=%d, wanted less than 3", lookups)
		}
	})

	t.Run("first caller bails, second caller gets result", func(t *testing.T) {
		c := make(chan struct{})
		a := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			<-c
			return chewie, true, nil
		}), true, time.Minute, 0)

		var wg sync.WaitGroup
		wg.Add(2)

		ctx1, cancel1 := context.WithCancel(context.Background())
		go func() {
			defer wg.Done()
			a.AuthenticateToken(ctx1, "")
		}()

		ctx2 := context.Background()

		var (
			resp *authenticator.Response
			ok   bool
			err  error
		)
		go func() {
			defer wg.Done()
			resp, ok, err = a.AuthenticateToken(ctx2, "")
		}()

		time.Sleep(1 * time.Second)
		// 取消第一个调用者后，共享查询不应被连带取消，第二个调用者仍应成功。
		cancel1()
		close(c)
		wg.Wait()

		if want := chewie; !cmp.Equal(resp, want) {
			t.Errorf("Unexpected diff: %v", cmp.Diff(resp, want))
		}
		if !ok {
			t.Errorf("Expected ok response")
		}
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("lookup panics", func(t *testing.T) {
		a := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			panic("uh oh")
		}), true, time.Minute, 0)

		// panic 必须被转换为 errAuthnCrash，绝不能传播到请求处理协程。
		_, _, err := a.AuthenticateToken(context.Background(), "")
		if err != errAuthnCrash {
			t.Errorf("expected error: %v", err)
		}
	})

	t.Run("audiences are forwarded", func(t *testing.T) {
		ctx := authenticator.WithAudiences(context.Background(), []string{"a"})
		a := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			auds, _ := authenticator.AudiencesFrom(ctx)
			// 注意：此处断言条件为既有写法（相等时反而失败），保持原样不改。
			if got, want := auds, []string{"a"}; cmp.Equal(got, want) {
				t.Fatalf("unexpeced audiences: %v", cmp.Diff(got, want))
			}
			return nil, false, nil
		}), true, time.Minute, 0)

		a.AuthenticateToken(ctx, "")
	})
}

// TestCachedAuditAnnotations 验证审计注解随缓存结果一起复用：
// 命中缓存时注解仍会写入当前请求的审计上下文，且 TTL 内保持不变。
func TestCachedAuditAnnotations(t *testing.T) {
	snorlax := &authenticator.Response{User: &user.DefaultInfo{Name: "snorlax"}}

	t.Run("annotations from cache", func(t *testing.T) {
		var lookups uint32
		c := make(chan struct{})
		a := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			<-c
			atomic.AddUint32(&lookups, 1)
			// 后端在首次认证时写入审计注解，缓存需把它们保存下来。
			audit.AddAuditAnnotation(ctx, "snorlax", "rocks")
			audit.AddAuditAnnotation(ctx, "pandas", "are amazing")
			return snorlax, true, nil
		}), false, time.Minute, 0)

		allAnnotations := make(chan map[string]string, 10)
		defer close(allAnnotations)

		var wg sync.WaitGroup
		for i := 0; i < cap(allAnnotations); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// 每个请求有独立审计上下文，注解需分别回填。
				ctx := withAudit(context.Background())
				_, _, _ = a.AuthenticateToken(ctx, "token")

				allAnnotations <- audit.AuditContextFrom(ctx).GetEventAnnotations()
			}()
		}

		// no good way to make sure that all the callers are queued so we sleep.
		time.Sleep(1 * time.Second)
		close(c)
		wg.Wait()

		want := map[string]string{"snorlax": "rocks", "pandas": "are amazing"}
		for i := 0; i < cap(allAnnotations); i++ {
			annotations := <-allAnnotations
			if diff := cmp.Diff(want, annotations); diff != "" {
				t.Errorf("%d: unexpected annotations (-want +got): %s", i, diff)
			}
		}

		// 通道应被完全消费，确保所有协程都完成了注解写入。
		if queued := len(allAnnotations); queued != 0 {
			t.Errorf("expected all annoations to be processed: %d", queued)
		}

		if lookups > 3 {
			t.Errorf("unexpected number of lookups: got=%d, wanted less than 3", lookups)
		}
	})

	t.Run("annotations do not change during cache TTL", func(t *testing.T) {
		a := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			audit.AddAuditAnnotation(ctx, "timestamp", time.Now().String())
			return snorlax, true, nil
		}), false, time.Minute, 0)

		allAnnotations := make([]map[string]string, 0, 10)

		for i := 0; i < cap(allAnnotations); i++ {
			ctx := withAudit(context.Background())
			_, _, _ = a.AuthenticateToken(ctx, "token")
			allAnnotations = append(allAnnotations, audit.AuditContextFrom(ctx).GetEventAnnotations())
		}

		if len(allAnnotations) != cap(allAnnotations) {
			t.Errorf("failed to process all annotations")
		}

		// 首次结果应包含唯一的时间戳注解。
		want := allAnnotations[0]
		if ok := len(want) == 1 && len(want["timestamp"]) > 0; !ok {
			t.Errorf("invalid annotations: %v", want)
		}

		// 后续命中缓存的请求必须得到完全相同的时间戳，证明注解来自缓存而非重新生成。
		for i, annotations := range allAnnotations[1:] {
			if diff := cmp.Diff(want, annotations); diff != "" {
				t.Errorf("%d: unexpected annotations (-want +got): %s", i, diff)
			}
		}
	})

	t.Run("different tokens can have different annotations", func(t *testing.T) {
		a := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			audit.AddAuditAnnotation(ctx, "timestamp", time.Now().String())
			return snorlax, true, nil
		}), false, time.Minute, 0)

		ctx1 := withAudit(context.Background())
		_, _, _ = a.AuthenticateToken(ctx1, "token1")
		annotations1 := audit.AuditContextFrom(ctx1).GetEventAnnotations()

		// guarantee different now times
		time.Sleep(time.Second)

		ctx2 := withAudit(context.Background())
		_, _, _ = a.AuthenticateToken(ctx2, "token2")
		annotations2 := audit.AuditContextFrom(ctx2).GetEventAnnotations()

		if ok := len(annotations1) == 1 && len(annotations1["timestamp"]) > 0; !ok {
			t.Errorf("invalid annotations 1: %v", annotations1)
		}
		if ok := len(annotations2) == 1 && len(annotations2["timestamp"]) > 0; !ok {
			t.Errorf("invalid annotations 2: %v", annotations2)
		}

		// 不同 token 是不同缓存项，注解不应串用。
		if annotations1["timestamp"] == annotations2["timestamp"] {
			t.Errorf("annotations should have different timestamp value: %v", annotations1)
		}
	})
}

// BenchmarkCachedTokenAuthenticator 在 token 数与并发线程数的组合上测量缓存开销。
func BenchmarkCachedTokenAuthenticator(b *testing.B) {
	tokenCount := []int{100, 500, 2500, 12500, 62500}
	threadCount := []int{1, 16, 256}
	for _, tokens := range tokenCount {
		for _, threads := range threadCount {
			newSingleBenchmark(tokens, threads).run(b)
		}
	}
}

// newSingleBenchmark 构造并预生成指定规模基准所需的 token 数据。
func newSingleBenchmark(tokens, threads int) *singleBenchmark {
	s := &singleBenchmark{
		threadCount: threads,
		tokenCount:  tokens,
	}
	s.makeTokens()
	return s
}

// singleBenchmark collects all the state needed to run a benchmark. The
// question this benchmark answers is, "what's the average latency added by the
// cache for N concurrent tokens?"
//
// Given the size of the key range constructed by this test, the default go
// benchtime of 1 second is often inadequate to test caching and expiration
// behavior. A benchtime of 10 to 30 seconds is adequate to stress these
// code paths.
type singleBenchmark struct {
	threadCount int
	// These token.* variables are set by makeTokens()
	tokenCount int
	// pre-computed response for a token
	tokenToResponse map[string]*cacheRecord
	// include audiences for some
	tokenToAuds map[string]authenticator.Audiences
	// a list makes it easy to select a random one
	tokens []string
}

// makeTokens 按 90%/9%/1% 的比例生成成功、拒绝、报错的 token，
// 并给部分成功结果附加审计注解，尽量贴近真实分布。
func (s *singleBenchmark) makeTokens() {
	s.tokenToResponse = map[string]*cacheRecord{}
	s.tokenToAuds = map[string]authenticator.Audiences{}
	s.tokens = []string{}

	rr := mathrand.New(mathrand.NewSource(mathrand.Int63()))

	for i := 0; i < s.tokenCount; i++ {
		tok := fmt.Sprintf("%v-%v", jwtToken, i)
		r := cacheRecord{
			resp: &authenticator.Response{
				User: &user.DefaultInfo{Name: fmt.Sprintf("holder of token %v", i)},
			},
		}
		// make different combinations of audience, failures, denies for the tokens.
		auds := []string{}
		for i := 0; i < rr.Intn(4); i++ {
			auds = append(auds, string(uuid.NewUUID()))
		}
		choice := rr.Float64()
		switch {
		case choice < 0.9:
			r.ok = true
			r.err = nil

			// add some realistic annotations on ~20% of successful authentications
			if f := rr.Float64(); f < 0.2 {
				r.annotations = map[string]string{
					"audience.authentication.kubernetes.io":  "e8357258-88b1-11ea-bc55-0242ac130003",
					"namespace.authentication.kubernetes.io": "kube-system",
					"float.authentication.kubernetes.io":     fmt.Sprint(f),
				}
			}
		case choice < 0.99:
			r.ok = false
			r.err = nil
		default:
			r.ok = false
			r.err = errors.New("I can't think of a clever error name right now")
		}
		s.tokens = append(s.tokens, tok)
		s.tokenToResponse[tok] = &r
		if len(auds) > 0 {
			s.tokenToAuds[tok] = auds
		}
	}
}

// lookup 模拟后端认证器：回放预生成结果并写入审计注解。
func (s *singleBenchmark) lookup(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	r, ok := s.tokenToResponse[token]
	if !ok {
		panic("test setup problem")
	}
	for key, val := range r.annotations {
		audit.AddAuditAnnotation(ctx, key, val)
	}
	return r.resp, r.ok, r.err
}

// doAuthForTokenN 以第 n 个 token 及其 audience 发起一次认证。
func (s *singleBenchmark) doAuthForTokenN(n int, a authenticator.Token) {
	tok := s.tokens[n]
	auds := s.tokenToAuds[tok]
	ctx := context.Background()
	ctx = authenticator.WithAudiences(ctx, auds)
	a.AuthenticateToken(ctx, tok)
}

// run 以规模参数命名并启动子基准。
func (s *singleBenchmark) run(b *testing.B) {
	b.Run(fmt.Sprintf("tokens=%d threads=%d", s.tokenCount, s.threadCount), s.bench)
}

// bench 通过限流的慢速后端模拟外部认证服务，观察缓存能削减多少回源。
func (s *singleBenchmark) bench(b *testing.B) {
	// Simulate slowness, qps limit, external service limitation, etc
	const maxInFlight = 40
	chokepoint := make(chan struct{}, maxInFlight)
	// lookup count
	var lookups uint64

	a := newWithClock(
		authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			atomic.AddUint64(&lookups, 1)

			chokepoint <- struct{}{}
			defer func() { <-chokepoint }()

			time.Sleep(1 * time.Millisecond)

			return s.lookup(ctx, token)
		}),
		true,
		4*time.Second,
		500*time.Millisecond,
		clock.RealClock{},
	)

	b.ResetTimer()
	b.SetParallelism(s.threadCount)
	b.RunParallel(func(pb *testing.PB) {
		r := mathrand.New(mathrand.NewSource(mathrand.Int63()))
		for pb.Next() {
			// some problems appear with random access, some appear with many
			// requests for a single entry, so we do both.
			// 随机访问覆盖分片竞争，固定 token 覆盖同一 key 的 singleflight 竞争。
			s.doAuthForTokenN(r.Intn(s.tokenCount), a)
			s.doAuthForTokenN(0, a)
		}
	})
	b.StopTimer()

	// 每次操作的平局回源次数，用于评估缓存命中率。
	b.ReportMetric(float64(lookups)/float64(b.N), "lookups/op")
}

// Add a test version of the audit context with a pre-populated event for easy annotation
// extraction.
func withAudit(ctx context.Context) context.Context {
	ctx = audit.WithAuditContext(ctx)
	return ctx
}

// TestUnsafeConversions 验证 string/[]byte 零拷贝转换的长度、容量、语义与分配次数。
func TestUnsafeConversions(t *testing.T) {

	// needs to be large to force allocations so we pick a random value between [1024, 2048]
	size := utilrand.IntnRange(1024, 2048+1)

	t.Run("toBytes semantics", func(t *testing.T) {
		t.Parallel()

		s := utilrand.String(size)
		b := toBytes(s)
		if len(b) != size {
			t.Errorf("unexpected length: %d", len(b))
		}
		// 零拷贝应让容量与长度相等，不能借用更大的底层数组。
		if cap(b) != size {
			t.Errorf("unexpected capacity: %d", cap(b))
		}
		if !bytes.Equal(b, []byte(s)) {
			t.Errorf("unexpected equality failure: %#v", b)
		}
	})

	t.Run("toBytes allocations", func(t *testing.T) {

		s := utilrand.String(size)
		f := func() {
			b := toBytes(s)
			if len(b) != size {
				t.Errorf("invalid length: %d", len(b))
			}
		}
		// 零拷贝实现的关键约束：每次调用不应产生堆分配。
		allocs := testing.AllocsPerRun(100, f)
		if allocs > 0 {
			t.Errorf("expected zero allocations, got %v", allocs)
		}
	})

	t.Run("toString semantics", func(t *testing.T) {
		t.Parallel()

		b := make([]byte, size)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		s := toString(b)
		if len(s) != size {
			t.Errorf("unexpected length: %d", len(s))
		}
		if s != string(b) {
			t.Errorf("unexpected equality failure: %#v", s)
		}
	})

	t.Run("toString allocations", func(t *testing.T) {

		b := make([]byte, size)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		f := func() {
			s := toString(b)
			if len(s) != size {
				t.Errorf("invalid length: %d", len(s))
			}
		}
		allocs := testing.AllocsPerRun(100, f)
		if allocs > 0 {
			t.Errorf("expected zero allocations, got %v", allocs)
		}
	})
}

// TestKeyFunc 用固定密钥校验 keyFunc 的输出，防止缓存键格式被无意改变
// 导致升级后旧缓存全部失效或不同版本之间互串。
func TestKeyFunc(t *testing.T) {
	t.Parallel()

	hashPool := &sync.Pool{
		New: func() interface{} {
			return hmac.New(sha256.New, []byte("098c9e46-b7f4-4358-bb3c-35cb7495b836")) // deterministic HMAC for testing
		},
	}

	// use realistic audiences
	auds := []string{"7daf30b7-a85c-429b-8b21-e666aecbb235", "c22aa267-bdde-4acb-8505-998be7818400", "44f9b4f3-7125-4333-b04c-1446a16c6113"}

	// 以下为固定期望值，任何键构造方式变化都会让它们失配。
	keyWithAuds := "\"\xf7\xac\xcd\x12\xf5\x83l\xa9;@\n\xa13a;\nd\x1f\xdelL\xd1\xe1!\x8a\xdahٛ\xbb\xf0"

	keyWithoutAuds := "\x054a \xa5\x8e\xea\xb2?\x8c\x88\xb9,e\n5\xe7ȵ>\xfdK\x0e\x93+\x02˿&\xf98\x1e"

	t.Run("has audiences", func(t *testing.T) {
		t.Parallel()

		key := keyFunc(hashPool, auds, jwtToken)
		if key != keyWithAuds {
			t.Errorf("unexpected equality failure: %#v", key)
		}
	})

	t.Run("nil audiences", func(t *testing.T) {
		t.Parallel()

		key := keyFunc(hashPool, nil, jwtToken)
		if key != keyWithoutAuds {
			t.Errorf("unexpected equality failure: %#v", key)
		}
	})
}
