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

package disk

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/peterbourgon/diskv"
	"github.com/stretchr/testify/assert"
)

// testRoundTripper 是记录请求并返回预置响应的桩，便于观察缓存层是否真正发出了请求。
// 抄自 k8s.io/client-go/transport/round_trippers_test.go。
type testRoundTripper struct {
	Request  *http.Request
	Response *http.Response
	Err      error
}

func (rt *testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.Request = req
	return rt.Response, rt.Err
}

// BenchmarkDiskCache 衡量底层磁盘缓存 Set/Get/Delete 的吞吐。
func BenchmarkDiskCache(b *testing.B) {
	cacheDir, err := os.MkdirTemp("", "cache-rt")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(cacheDir)

	d := diskv.New(diskv.Options{
		PathPerm: os.FileMode(0750),
		FilePerm: os.FileMode(0660),
		BasePath: cacheDir,
		TempDir:  filepath.Join(cacheDir, ".diskv-temp"),
	})

	k := "localhost:8080/apis/batch/v1.json"
	v, err := os.ReadFile("../../testdata/apis/batch/v1.json")
	if err != nil {
		b.Fatal(err)
	}

	c := sumDiskCache{disk: d}

	for n := 0; n < b.N; n++ {
		c.Set(k, v)
		c.Get(k)
		c.Delete(k)
	}
}

// TestCacheRoundTripper 验证首次请求写入磁盘缓存，而二次请求在收到 304 时复用缓存内容。
func TestCacheRoundTripper(t *testing.T) {
	rt := &testRoundTripper{}
	cacheDir, err := os.MkdirTemp("", "cache-rt")
	defer os.RemoveAll(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	cache := newCacheRoundTripper(cacheDir, rt)

	// 首次调用应写入缓存。
	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Host: "localhost"},
	}
	rt.Response = &http.Response{
		Header:     http.Header{"ETag": []string{`"123456"`}},
		Body:       io.NopCloser(bytes.NewReader([]byte("Content"))),
		StatusCode: http.StatusOK,
	}
	resp, err := cache.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "Content" {
		t.Errorf(`Expected Body to be "Content", got %q`, string(content))
	}

	// 二次调用应返回缓存内容。
	req = &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Host: "localhost"},
	}
	rt.Response = &http.Response{
		StatusCode: http.StatusNotModified,
		Body:       io.NopCloser(bytes.NewReader([]byte("Other Content"))),
	}

	resp, err = cache.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}

	// 读取响应体，确认拿到的仍是首次缓存的内容。
	content, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "Content" {
		t.Errorf("Invalid content read from cache %q", string(content))
	}
}

// TestCacheRoundTripperPathPerm 校验写入的目录与文件权限分别固定为 0750/0660。
func TestCacheRoundTripperPathPerm(t *testing.T) {
	assert := assert.New(t)

	rt := &testRoundTripper{}
	cacheDir, err := os.MkdirTemp("", "cache-rt")
	os.RemoveAll(cacheDir)
	defer os.RemoveAll(cacheDir)

	if err != nil {
		t.Fatal(err)
	}
	cache := newCacheRoundTripper(cacheDir, rt)

	// 首次调用应写入缓存。
	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Host: "localhost"},
	}
	rt.Response = &http.Response{
		Header:     http.Header{"ETag": []string{`"123456"`}},
		Body:       io.NopCloser(bytes.NewReader([]byte("Content"))),
		StatusCode: http.StatusOK,
	}
	resp, err := cache.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "Content" {
		t.Errorf(`Expected Body to be "Content", got %q`, string(content))
	}

	err = filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			assert.Equal(os.FileMode(0750), info.Mode().Perm())
		} else {
			assert.Equal(os.FileMode(0660), info.Mode().Perm())
		}
		return nil
	})
	assert.NoError(err)
}

// TestSumDiskCache 覆盖带校验和的磁盘缓存的各种边界：
// 缺失文件、空文件、校验和不匹配都应视为 miss；重复写入应覆盖旧值；删除幂等。
func TestSumDiskCache(t *testing.T) {
	assert := assert.New(t)

	// 底层文件不存在时应视为缓存未命中。
	t.Run("NoSuchKey", func(t *testing.T) {
		cacheDir, err := os.MkdirTemp("", "cache-test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(cacheDir)
		d := diskv.New(diskv.Options{BasePath: cacheDir, TempDir: filepath.Join(cacheDir, ".diskv-temp")})
		c := &sumDiskCache{disk: d}

		key := "testing"

		got, ok := c.Get(key)
		assert.False(ok)
		assert.Equal([]byte{}, got)
	})

	// 底层文件为空时应视为缓存未命中。
	t.Run("EmptyFile", func(t *testing.T) {
		cacheDir, err := os.MkdirTemp("", "cache-test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(cacheDir)
		d := diskv.New(diskv.Options{BasePath: cacheDir, TempDir: filepath.Join(cacheDir, ".diskv-temp")})
		c := &sumDiskCache{disk: d}

		key := "testing"

		f, err := os.Create(filepath.Join(cacheDir, sanitize(key)))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()

		got, ok := c.Get(key)
		assert.False(ok)
		assert.Equal([]byte{}, got)
	})

	// 底层文件校验和无效时应视为缓存未命中。
	t.Run("InvalidChecksum", func(t *testing.T) {
		cacheDir, err := os.MkdirTemp("", "cache-test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(cacheDir)
		d := diskv.New(diskv.Options{BasePath: cacheDir, TempDir: filepath.Join(cacheDir, ".diskv-temp")})
		c := &sumDiskCache{disk: d}

		key := "testing"
		value := []byte("testing")
		mismatchedValue := []byte("testink")
		sum := sha256.Sum256(value)

		// 文件内容用 value 的校验和拼接 mismatchedValue 的字节，制造校验和冲突。
		f, err := os.Create(filepath.Join(cacheDir, sanitize(key)))
		if err != nil {
			t.Fatal(err)
		}
		f.Write(sum[:])
		f.Write(mismatchedValue)
		f.Close()

		// 校验和不匹配应导致缓存未命中。
		got, ok := c.Get(key)
		assert.False(ok)
		assert.Equal([]byte{}, got)
	})

	// 磁盘缓存应允许覆盖已有值：Get 发现校验和错误时返回 miss，httpcache 便会回源，
	// 再把新值写入缓存以覆盖损坏条目。自检机制依赖这一行为才能自愈。
	t.Run("OverwriteExistingKey", func(t *testing.T) {
		cacheDir, err := os.MkdirTemp("", "cache-test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(cacheDir)
		d := diskv.New(diskv.Options{BasePath: cacheDir, TempDir: filepath.Join(cacheDir, ".diskv-temp")})
		c := &sumDiskCache{disk: d}

		key := "testing"
		value := []byte("cool value!")

		// 写入一个值。
		c.Set(key, value)
		got, ok := c.Get(key)

		// 确认写入后能读回原值。
		assert.True(ok)
		assert.Equal(value, got)

		differentValue := []byte("I'm different!")

		// 再写入另一个值。
		c.Set(key, differentValue)
		got, ok = c.Get(key)

		// 确认读到的是后写入的新值。
		assert.True(ok)
		assert.Equal(differentValue, got)
	})

	// 确认删除 key 后数据确实被移除。
	t.Run("DeleteKey", func(t *testing.T) {
		cacheDir, err := os.MkdirTemp("", "cache-test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(cacheDir)
		d := diskv.New(diskv.Options{BasePath: cacheDir, TempDir: filepath.Join(cacheDir, ".diskv-temp")})
		c := &sumDiskCache{disk: d}

		key := "testing"
		value := []byte("coolValue")

		c.Set(key, value)

		// 确认成功写入值。
		got, ok := c.Get(key)
		assert.True(ok)
		assert.Equal(value, got)

		c.Delete(key)

		// 确认值已被删除。
		got, ok = c.Get(key)
		assert.False(ok)
		assert.Equal([]byte{}, got)

		// 删除不存在的 key 应是空操作。
		c.Delete(key)
	})
}
