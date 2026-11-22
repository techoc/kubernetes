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

package clientcmd

import (
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// ParseTimeout returns a parsed duration from a string
// A duration string value must be a positive integer, optionally followed by a corresponding time unit (s|m|h).
//
// 先在无单位时按“秒”解释，再退回 Go 的 time.ParseDuration。
// 这样既兼容 kubeconfig 中历史遗留的纯数字（如 "30"），也支持 "1s"、"2m" 等写法。
func ParseTimeout(duration string) (time.Duration, error) {
	if i, err := strconv.ParseInt(duration, 10, 64); err == nil && i >= 0 {
		return (time.Duration(i) * time.Second), nil
	}
	if requestTimeout, err := time.ParseDuration(duration); err == nil {
		return requestTimeout, nil
	}
	return 0, fmt.Errorf("Invalid timeout value. Timeout must be a single integer in seconds, or an integer followed by a corresponding time unit (e.g. 1s | 2m | 3h)")
}

// parseProxyURL 解析并校验 proxy-url。
//
// 只允许 http/https/socks5 三种 scheme：其他协议（如直接写 host:port）
// 会在这里被拒绝，避免到传输层才以晦涩的错误失败。
func parseProxyURL(proxyURL string) (*url.URL, error) {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("could not parse: %v", proxyURL)
	}

	switch u.Scheme {
	case "http", "https", "socks5":
	default:
		return nil, fmt.Errorf("unsupported scheme %q, must be http, https, or socks5", u.Scheme)
	}
	return u, nil
}
