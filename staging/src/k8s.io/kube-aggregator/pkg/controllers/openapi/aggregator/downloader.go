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

package aggregator

import (
	"crypto/sha512"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/util/responsewriter"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// CacheableDownloader 把下载能力与“缓存上次结果”绑定在一起。
// 聚合器需要把同一 APIService 的 spec 和 etag 一起缓存，以便下次带 If-None-Match 请求；
// 抽成接口后，SpecAggregator 只依赖抽象，便于在测试中替换为假实现。
type CacheableDownloader interface {
	UpdateHandler(http.Handler)
	Get() (*spec.Swagger, string, error)
}

// cacheableDownloader is a downloader that will always return the data
// and the etag.
// handler 用 atomic.Pointer 保存：代理证书轮换时 UpdateHandler 会被并发调用替换 handler，
// 而 Get 从另一个 goroutine 读取；原子指针让替换对读侧可见且无需加锁阻塞下载。
type cacheableDownloader struct {
	name       string
	downloader *Downloader
	// handler is the http Handler for the apiservice that can be replaced
	handler atomic.Pointer[http.Handler]
	etag    string
	spec    *spec.Swagger
}

// NewCacheableDownloader creates a downloader that also returns the etag, making it useful to use as a cached dependency.
// 每个 APIService 对应一个实例，因此 etag/spec 是该 APIService 独有的缓存；
// 这样不同 APIService 即使内容相同也不会互相污染缓存状态。
func NewCacheableDownloader(apiServiceName string, downloader *Downloader, handler http.Handler) CacheableDownloader {
	c := &cacheableDownloader{
		name:       apiServiceName,
		downloader: downloader,
	}
	c.handler.Store(&handler)
	return c
}
func (d *cacheableDownloader) UpdateHandler(handler http.Handler) {
	d.handler.Store(&handler)
}

func (d *cacheableDownloader) Get() (*spec.Swagger, string, error) {
	spec, etag, err := d.get()
	if err != nil {
		return spec, etag, fmt.Errorf("failed to download %v: %v", d.name, err)
	}
	return spec, etag, err
}

// get 负责与远端交换 etag 并维护本地缓存。
// 这里不是纯函数：304 表示远端没变化，保留旧 spec 只刷新缓存状态；
// 404 被当作“该服务不提供 spec”，转成 ErrAPIServiceNotFound 让上层停止重试。
func (d *cacheableDownloader) get() (*spec.Swagger, string, error) {
	h := *d.handler.Load()
	swagger, etag, status, err := d.downloader.Download(h, d.etag)
	if err != nil {
		return nil, "", err
	}
	switch status {
	case http.StatusNotModified:
		// Nothing has changed, do nothing.
	case http.StatusOK:
		if swagger != nil {
			d.etag = etag
			d.spec = swagger
			break
		}
		fallthrough
	case http.StatusNotFound:
		// 404 说明服务根本不提供 OpenAPI，没必要继续按失败重试；
		// 转换成明确错误让上层识别并停止队列轮询。
		return nil, "", ErrAPIServiceNotFound
	default:
		return nil, "", fmt.Errorf("invalid status code: %v", status)
	}
	return d.spec, d.etag, nil
}

// Downloader is the OpenAPI downloader type. It will try to download spec from /openapi/v2 or /swagger.json endpoint.
// 无状态类型：每次下载所需的 handler 与 etag 都由调用方传入，
// 因此可以安全地在不同 APIService 之间共享同一个 Downloader 实例。
type Downloader struct {
}

// NewDownloader creates a new OpenAPI Downloader.
func NewDownloader() Downloader {
	return Downloader{}
}

// handlerWithUser 给请求注入聚合器身份，再交给原始 handler。
// 聚合服务通常要求 callers 有读 OpenAPI 的权限；使用固定系统身份可以避免
// 因为没有最终用户上下文而被鉴权拒绝。
func (s *Downloader) handlerWithUser(handler http.Handler, info user.Info) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req = req.WithContext(request.WithUser(req.Context(), info))
		handler.ServeHTTP(w, req)
	})
}

// etagFor 为不返回 Etag 的服务生成稳定的本地 etag。
// 用内容哈希而非时间戳，保证相同内容总能得到相同 tag；带上特殊前缀则用于区分
// “这是本地生成的”，避免把它当作服务端 etag 发给对方（服务端不会认可）。
func etagFor(data []byte) string {
	return fmt.Sprintf("%s%X\"", locallyGeneratedEtagPrefix, sha512.Sum512(data))
}

// Download downloads openAPI spec from /openapi/v2 endpoint of the given handler.
// httpStatus is only valid if err == nil
// 直接调用 handler（而不是发真实 HTTP 请求）拿到响应，这样可复用聚合层的代理路由；
// 外层套 TimeoutHandler 防止某个聚合服务挂住拖垮整个轮询周期。
func (s *Downloader) Download(handler http.Handler, etag string) (returnSpec *spec.Swagger, newEtag string, httpStatus int, err error) {
	handler = s.handlerWithUser(handler, &user.DefaultInfo{Name: aggregatorUser})
	handler = http.TimeoutHandler(handler, specDownloadTimeout, "request timed out")

	req, err := http.NewRequest("GET", "/openapi/v2", nil)
	if err != nil {
		return nil, "", 0, err
	}
	req.Header.Add("Accept", "application/json")

	// Only pass eTag if it is not generated locally
	// 本地生成的 etag 只是我们用来判断内容是否变化的手段，服务端不认识它；
	// 若发过去会被忽略甚至误解，因此只在确实来自服务端时带上 If-None-Match。
	if len(etag) > 0 && !strings.HasPrefix(etag, locallyGeneratedEtagPrefix) {
		req.Header.Add("If-None-Match", etag)
	}

	writer := responsewriter.NewInMemoryResponseWriter()
	handler.ServeHTTP(writer, req)

	switch writer.RespCode() {
	case http.StatusNotModified:
		if len(etag) == 0 {
			return nil, etag, http.StatusNotModified, fmt.Errorf("http.StatusNotModified is not allowed in absence of etag")
		}
		return nil, etag, http.StatusNotModified, nil
	case http.StatusNotFound:
		// Gracefully skip 404, assuming the server won't provide any spec
		return nil, "", http.StatusNotFound, nil
	case http.StatusOK:
		openAPISpec := &spec.Swagger{}
		if err := openAPISpec.UnmarshalJSON(writer.Data()); err != nil {
			return nil, "", 0, err
		}
		newEtag = writer.Header().Get("Etag")
		if len(newEtag) == 0 {
			// 服务端不提供 Etag 时，只能靠内容哈希自己判断变化；
			// 这仍能把“内容没变”识别为 304，避免不必要的重复聚合。
			newEtag = etagFor(writer.Data())
			if len(etag) > 0 && strings.HasPrefix(etag, locallyGeneratedEtagPrefix) {
				// The function call with an etag and server does not report an etag.
				// That means this server does not support etag and the etag that passed
				// to the function generated previously by us. Just compare etags and
				// return StatusNotModified if they are the same.
				// 上次传进来的 etag 是我们自己生成的，说明该服务不支持 etag；
				// 此时比较本地哈希即可判断是否真的变化，内容未变就伪装成 304。
				if etag == newEtag {
					return nil, etag, http.StatusNotModified, nil
				}
			}
		}
		return openAPISpec, newEtag, http.StatusOK, nil
	default:
		return nil, "", 0, fmt.Errorf("failed to retrieve openAPI spec, http error: %s", writer.String())
	}
}
