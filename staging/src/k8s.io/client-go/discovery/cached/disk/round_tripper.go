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

// Package disk 实现基于本地磁盘缓存的 discovery 缓存（CachedDiscoveryInterface）。
// 缓存通过 HTTP 语义（ETag / If-None-Match）与服务端协商，从而把 discovery 请求的
// 网络开销降到 304 响应的量级。
package disk

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/peterbourgon/diskv"
	"k8s.io/client-go/third_party/forked/httpcache"
	"k8s.io/klog/v2"
)

// cacheRoundTripper 把 httpcache.Transport 包装成可直接嵌入 rest.Config 的 RoundTripper。
type cacheRoundTripper struct {
	rt *httpcache.Transport
}

// newCacheRoundTripper 创建 roundtripper：读取响应头中的 ETag，并在后续
// 对应请求中带上 If-None-Match，让服务端用 304 响应省去重复传输。
//
// 缓存目录权限收紧到 0750/0660：缓存内容可能包含服务端资源结构等信息，
// 不应对其他系统用户可读。
func newCacheRoundTripper(cacheDir string, rt http.RoundTripper) http.RoundTripper {
	// TempDir 放在缓存目录内部，保证临时文件与最终文件位于同一文件系统，rename 才是原子的。
	d := diskv.New(diskv.Options{
		PathPerm: os.FileMode(0750),
		FilePerm: os.FileMode(0660),
		BasePath: cacheDir,
		TempDir:  filepath.Join(cacheDir, ".diskv-temp"),
	})
	t := httpcache.NewTransport(&sumDiskCache{disk: d})
	t.Transport = rt

	return &cacheRoundTripper{rt: t}
}

// RoundTrip 交给 httpcache 处理：命中新鲜缓存直接返回，过期时补上 If-None-Match 再请求。
func (rt *cacheRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt.rt.RoundTrip(req)
}

// CancelRequest 仅当底层 transport 支持取消时转发；老式 CancelRequest 接口已不保证存在。
func (rt *cacheRoundTripper) CancelRequest(req *http.Request) {
	type canceler interface {
		CancelRequest(*http.Request)
	}
	if cr, ok := rt.rt.Transport.(canceler); ok {
		cr.CancelRequest(req)
	} else {
		klog.FromContext(req.Context()).Error(nil, "CancelRequest not implemented by round-tripper", "type", fmt.Sprintf("%T", rt.rt.Transport))
	}
}

// WrappedRoundTripper 暴露被包装的 transport，供 rest 层做能力探测。
func (rt *cacheRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.rt.Transport }

// sumDiskCache 是 httpcache 的缓存后端，类似其 diskcache 包，但用 SHA256 校验和
// sumDiskCache 是 httpcache 的缓存后端，类似 httpcache 的 diskcache 包，
// 但选择在读取时通过 SHA256 校验完整性，而不是对每条缓存 fsync 以提高写入
// 持久化概率。这样避免 MacOS 上明显的性能退化。
//
// See https://github.com/kubernetes/kubernetes/issues/110753 for more.
type sumDiskCache struct {
	disk *diskv.Diskv
}

// Get 从磁盘缓存读取指定 key。若读取出错，或返回值不是 SHA256 校验和加上
// 校验和匹配的响应字节，则返回 false 表示缓存未命中。
func (c *sumDiskCache) Get(key string) ([]byte, bool) {
	// 短于校验和长度的文件不可能合法，直接视为 miss。
	b, err := c.disk.Read(sanitize(key))
	if err != nil || len(b) < sha256.Size {
		return []byte{}, false
	}

	// 读取时校验而非写入时 fsync：磁盘上留下半写文件时只会退化为 miss，不会返回损坏数据。
	response := b[sha256.Size:]
	want := b[:sha256.Size] // The first 32 bytes of the file should be the SHA256 sum.
	got := sha256.Sum256(response)
	if !bytes.Equal(want, got[:]) {
		return []byte{}, false
	}

	return response, true
}

// Set 把响应写入磁盘文件，文件名是 key 的 SHA256 值；文件内容为响应字节的
// SHA256 校验和加上响应字节本身，读取时可据此校验完整性。
func (c *sumDiskCache) Set(key string, response []byte) {
	// append 可能复用 s 的底层数组，但由于 s 刚刚创建且容量恰好，这里不会污染调用方数据。
	s := sha256.Sum256(response)
	_ = c.disk.Write(sanitize(key), append(s[:], response...)) // Nothing we can do with this error.
}

// Delete 从磁盘移除缓存项；错误被忽略，因为删除失败最多导致一次多余的缓存命中。
func (c *sumDiskCache) Delete(key string) {
	_ = c.disk.Erase(sanitize(key)) // Nothing we can do with this error.
}

// Sanitize 把 httpcache 的键转换为可用作 diskv 键的合法文件名。按 httpcache.cacheKey
// Sanitize 把 httpcache 的键转换成可用作 diskv 键的合法文件名。按
// httpcache.cacheKey 的规则，GET 请求的键是 URL，其他方法是 "<method> <url>"。
// 用哈希而不是转义后的原文作为文件名，既规避 URL 中的路径分隔符等非法字符，
// 也避免超长文件名。
func sanitize(key string) string {
	// 这些键本身不含敏感信息；用 sha256 是为了避免（可能是恶意的）碰撞导致
	// 读写到错误的缓存数据。
	return fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
}
