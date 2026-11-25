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
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sync/singleflight"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

var errAuthnCrash = apierrors.NewInternalError(errors.New("authentication failed unexpectedly"))

// sharedLookupTimeout 是单次共享认证查询的超时。
// 查询脱离原请求上下文（singleflight 共享给多个调用方），
// 因此必须有自己的超时，否则一个挂起的认证器会永久占用该 key 的 flight。
const sharedLookupTimeout = 30 * time.Second

// cacheRecord 保存 authenticator.Token 的 AuthenticateToken 方法的三个返回值。
// 额外保存审计注解与警告，是因为缓存命中时无法重放认证器的副作用——
// 只能把首次查询时产生的副作用随记录一起缓存，命中时再重新施加到当前请求。
type cacheRecord struct {
	resp *authenticator.Response
	ok   bool
	err  error

	// this cache assumes token authn has no side-effects or temporal dependence.
	// neither of these are true for audit annotations set via AddAuditAnnotation.
	//
	// for audit annotations, the assumption is that for some period of time (cache TTL),
	// all requests with the same API audiences and the same bearer token result in the
	// same annotations.  This may not be true if the authenticator sets an annotation
	// based on the current time, but that may be okay since cache TTLs are generally
	// small (seconds).
	annotations map[string]string
	warnings    []*cacheWarning
}

// cacheWarning 缓存一条 warning，避免跨请求共享可变字符串切片。
type cacheWarning struct {
	agent string
	text  string
}

// cachedTokenAuthenticator 包装底层 Token 认证器，缓存其结果。
// 并发控制有两层——cache 自身线程安全，singleflight 保证同一 key
// 在缓存未命中时只有一次真实认证，其余调用方等待并共享结果，避免惊群。
// hashPool 复用 hash.Hash，减少每请求分配；HMAC + 随机密钥的用途见字段注释。
type cachedTokenAuthenticator struct {
	authenticator authenticator.Token

	cacheErrs  bool
	successTTL time.Duration
	failureTTL time.Duration

	cache cache
	group singleflight.Group

	// hashPool is a per authenticator pool of hash.Hash (to avoid allocations from building the Hash)
	// HMAC with SHA-256 and a random key is used to prevent precomputation and length extension attacks
	// It also mitigates hash map DOS attacks via collisions (the inputs are supplied by untrusted users)
	hashPool *sync.Pool
}

// cache 是缓存的最小接口，便于测试替换实现。
type cache interface {
	// given a key, return the record, and whether or not it existed
	get(key string) (value *cacheRecord, exists bool)
	// caches the record for the key
	set(key string, value *cacheRecord, ttl time.Duration)
	// removes the record for the key
	remove(key string)
}

// New 返回一个 token 认证器，它缓存指定认证器的结果。ttl 为 0 表示绕过缓存。
// successTTL/failureTTL 为 0 时对应类别不缓存——失败不缓存可避免
// 短暂故障被放大成 TTL 时长的持续拒绝。
func New(authenticator authenticator.Token, cacheErrs bool, successTTL, failureTTL time.Duration) authenticator.Token {
	return newWithClock(authenticator, cacheErrs, successTTL, failureTTL, clock.RealClock{})
}

// newWithClock 允许注入时钟，便于测试 TTL 行为而不必真实等待。
func newWithClock(authenticator authenticator.Token, cacheErrs bool, successTTL, failureTTL time.Duration, clock clock.Clock) authenticator.Token {
	registerMetrics()
	// 随机 HMAC 密钥：即使攻击者知道 token 格式，也无法离线预计算哈希值，
	// 从而防止用碰撞或彩虹表冲击缓存。
	randomCacheKey := make([]byte, 32)
	if _, err := rand.Read(randomCacheKey); err != nil {
		panic(err) // rand should never fail
	}

	return &cachedTokenAuthenticator{
		authenticator: authenticator,
		cacheErrs:     cacheErrs,
		successTTL:    successTTL,
		failureTTL:    failureTTL,
		// Cache performance degrades noticeably when the number of
		// tokens in operation exceeds the size of the cache. It is
		// cheap to make the cache big in the second dimension below,
		// the memory is only consumed when that many tokens are being
		// used. Currently we advertise support 5k nodes and 10k
		// namespaces; a 32k entry cache is therefore a 2x safety
		// margin.
		cache: newStripedCache(32, fnvHashFunc, func() cache { return newSimpleCache(clock) }),

		hashPool: &sync.Pool{
			New: func() interface{} {
				return hmac.New(sha256.New, randomCacheKey)
			},
		},
	}
}

// AuthenticateToken 实现 authenticator.Token 接口。
// 缓存命中时不返回缓存的 err，而是把注解/警告重新施加到当前请求后
// 返回成功响应；未命中/失败路径则原样返回。这样上层看到的语义与未缓存时一致。
func (a *cachedTokenAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	record := a.doAuthenticateToken(ctx, token)
	if !record.ok || record.err != nil {
		return nil, false, record.err
	}
	for key, value := range record.annotations {
		audit.AddAuditAnnotation(ctx, key, value)
	}
	for _, w := range record.warnings {
		warning.AddWarning(ctx, w.agent, w.text)
	}
	return record.resp, true, nil
}

// doAuthenticateToken 实现「查缓存 → 未命中则 singleflight 认证 → 写缓存」。
// key 由 audiences + token 共同决定，因为同一 token 在不同 audience 下
// 可能得到不同结论；只用 token 作 key 会导致跨 audience 的错误复用。
func (a *cachedTokenAuthenticator) doAuthenticateToken(ctx context.Context, token string) *cacheRecord {
	doneAuthenticating := stats.authenticating(ctx)

	auds, audsOk := authenticator.AudiencesFrom(ctx)

	key := keyFunc(a.hashPool, auds, token)
	if record, ok := a.cache.get(key); ok {
		// Record cache hit
		doneAuthenticating(true)
		return record
	}

	// Record cache miss
	doneBlocking := stats.blocking(ctx)
	defer doneBlocking()
	defer doneAuthenticating(false)

	c := a.group.DoChan(key, func() (val interface{}, _ error) {
		// always use one place to read and write the output of AuthenticateToken
		record := &cacheRecord{}

		doneFetching := stats.fetching(ctx)
		// 我们已经离开请求处理调用栈，因此需要自己处理崩溃：panic 时打印堆栈并返回 500。
		// 认证器运行在独立 goroutine 中，panic 不会传播回调用栈；
		// 必须自行 recover，否则整个进程会崩溃。限制栈大小避免日志爆炸。
		defer func() {
			if r := recover(); r != nil {
				// make sure to always return a record
				record.err = errAuthnCrash
				val = record

				// Same as stdlib http server code. Manually allocate stack
				// trace buffer size to prevent excessively large logs
				const size = 64 << 10
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				klog.Errorf("%v\n%s", r, buf)
			}
			doneFetching(record.err == nil)
		}()

		// Check again for a cached record. We may have raced with a fetch.
		if record, ok := a.cache.get(key); ok {
			return record, nil
		}

		// 上下文需要脱离调用方，因为这次查询可能被多个调用方共享；但 audience 必须继续传递。
		// 不能复用任一调用方的 context——它可能先于其他等待者被取消；
		// 但 audience 是认证语义的一部分，必须显式透传。
		ctx, cancel := context.WithTimeout(context.Background(), sharedLookupTimeout)
		defer cancel()

		if audsOk {
			ctx = authenticator.WithAudiences(ctx, auds)
		}
		recorder := &recorder{}
		ctx = warning.WithWarningRecorder(ctx, recorder)

		ctx = audit.WithAuditContext(ctx)
		ac := audit.AuditContextFrom(ctx)

		record.resp, record.ok, record.err = a.authenticator.AuthenticateToken(ctx, token)
		record.annotations = ac.GetEventAnnotations()
		record.warnings = recorder.extractWarnings()

		// cacheErrs 为 false 时错误不进缓存，让下一次请求重新尝试。
		if !a.cacheErrs && record.err != nil {
			return record, nil
		}

		switch {
		case record.ok && a.successTTL > 0:
			a.cache.set(key, record, a.successTTL)
		case !record.ok && a.failureTTL > 0:
			a.cache.set(key, record, a.failureTTL)
		}

		return record, nil
	})

	select {
	case result := <-c:
		// we always set Val and never set Err
		return result.Val.(*cacheRecord)
	case <-ctx.Done():
		// 调用方上下文取消时伪造一条记录：不再等待共享查询（查询本身仍会完成并填充缓存），
		// 返回一个只含错误的记录，语义与真实认证失败一致。
		return &cacheRecord{err: ctx.Err()}
	}
}

// keyFunc 通过对输入做哈希来生成字符串 key，从而降低缓存的内存需求，并避免 token 留在内存中。
// 所有字段都带长度前缀，避免拼接歧义（"xy"+"z" 与 "x"+"yz"）。
// audiences 先编码数量再逐个编码，防止不同分组方式产生相同输入。
// 结果不做 base64，直接使用原始字节的字符串视图以省一次分配。
func keyFunc(hashPool *sync.Pool, auds []string, token string) string {
	h := hashPool.Get().(hash.Hash)

	h.Reset()

	// try to force stack allocation
	var a [4]byte
	b := a[:]

	writeLengthPrefixedString(h, b, token)
	// encode the length of audiences to avoid ambiguities
	writeLength(h, b, len(auds))
	for _, aud := range auds {
		writeLengthPrefixedString(h, b, aud)
	}

	key := toString(h.Sum(nil)) // skip base64 encoding to save an allocation

	hashPool.Put(h)

	return key
}

// writeLengthPrefixedString writes s with a length prefix to prevent ambiguities, i.e. "xy" + "z" == "x" + "yz"
// the length of b is assumed to be 4 (b is mutated by this function to store the length of s)
func writeLengthPrefixedString(w io.Writer, b []byte, s string) {
	writeLength(w, b, len(s))
	if _, err := w.Write(toBytes(s)); err != nil {
		panic(err) // Write() on hash never fails
	}
}

// writeLength encodes length into b and then writes it via the given writer
// the length of b is assumed to be 4
func writeLength(w io.Writer, b []byte, length int) {
	binary.BigEndian.PutUint32(b, uint32(length))
	if _, err := w.Write(b); err != nil {
		panic(err) // Write() on hash never fails
	}
}

// toBytes 用 unsafe 把 string 的底层字节直接交给哈希写入以避免 []byte(s) 拷贝分配。
// 只在调用方保证不修改返回切片的前提下安全；哈希写入恰好只读。
func toBytes(s string) []byte {
	// unsafe.StringData is unspecified for the empty string, so we provide a strict interpretation
	if len(s) == 0 {
		return nil
	}
	// Copied from go 1.20.1 os.File.WriteString
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/os/file.go#L246
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// toString 用 unsafe 避免分配：把哈希结果字节切片零拷贝转成 string 作为缓存键。
// 哈希输出不会再被修改，因此该别名是安全的；空切片单独处理，
// 因为 unsafe.SliceData 对空切片返回未定义指针。
func toString(b []byte) string {
	// unsafe.SliceData relies on cap whereas we want to rely on len
	if len(b) == 0 {
		return ""
	}
	// Copied from go 1.20.1 strings.Builder.String
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/strings/builder.go#L48
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// recorder 是一个只追加 warning 的简单记录器。
// warning 可能由认证器内部多个 goroutine 产生，故需要互斥；
// extractWarnings 在取出后清空，防止同一条 warning 被重复附加到后续请求。
type recorder struct {
	mu       sync.Mutex
	warnings []*cacheWarning
}

// AddWarning adds a warning to recorder.
func (r *recorder) AddWarning(agent, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warnings = append(r.warnings, &cacheWarning{agent: agent, text: text})
}

// extractWarnings 返回并清空已收集的 warning。
func (r *recorder) extractWarnings() []*cacheWarning {
	r.mu.Lock()
	defer r.mu.Unlock()
	warnings := r.warnings
	r.warnings = nil
	return warnings
}
