/*
Copyright 2022 The Kubernetes Authors.

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

// 最初从 pkg/credentialproviders/gcp 复制
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	credentialproviderv1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
)

const (
	maxReadLength = 10 * 1 << 20 // 10MB
)

var containerRegistryUrls = []string{"container.cloud.google.com", "gcr.io", "*.gcr.io", "*.pkg.dev"}

// HTTPError 把非 StatusOK 的错误码包装成 error。
type HTTPError struct {
	StatusCode int
	URL        string
}

var _ error = &HTTPError{}

// Error 实现 error 接口
func (h *HTTPError) Error() string {
	return fmt.Sprintf("http status code: %d while fetching url %s",
		h.StatusCode, h.URL)
}

// TokenBlob 用于解码包含 access token 的 JSON blob
// 它由 GCE metadata 返回。
type TokenBlob struct {
	AccessToken string `json:"access_token"`
}

type provider struct {
	client        *http.Client
	tokenEndpoint string
}

func (p *provider) Provide(image string) (map[string]credentialproviderv1.AuthConfig, error) {
	cfg := map[string]credentialproviderv1.AuthConfig{}

	tokenJSONBlob, err := readURL(p.tokenEndpoint, p.client)
	if err != nil {
		return cfg, err
	}

	var parsedBlob TokenBlob
	if err := json.Unmarshal(tokenJSONBlob, &parsedBlob); err != nil {
		return cfg, err
	}

	authConfig := credentialproviderv1.AuthConfig{
		Username: "_token",
		Password: parsedBlob.AccessToken,
	}

	// 为每个支持的容器仓库 URL 添加我们的条目
	for _, k := range containerRegistryUrls {
		cfg[k] = authConfig
	}
	return cfg, nil
}

func readURL(url string, client *http.Client) (body []byte, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header = http.Header{
		"Metadata-Flavor": []string{"Google"},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        url,
		}
	}

	limitedReader := &io.LimitedReader{R: resp.Body, N: maxReadLength}
	contents, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, err
	}

	if limitedReader.N <= 0 {
		return nil, errors.New("the read limit is reached")
	}

	return contents, nil
}
