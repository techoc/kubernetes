/*
Copyright 2019 The Kubernetes Authors.

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

// Package dynamiccertificates 提供可在运行期热更新的证书、CA bundle 与
// 密钥内容提供器，避免修改文件后必须重启 apiserver。
package dynamiccertificates

import (
	"bytes"
	"context"
	"crypto/x509"
	"strings"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// unionCAContent 把多个 CA 提供器合并为一个逻辑提供器，用于同时信任
// 多组客户端 CA（例如轮换期间新旧 CA 并存）。它是不可变切片，添加
// 提供器只在构造时发生，因此读取无需加锁。
type unionCAContent []CAContentProvider

var _ CAContentProvider = &unionCAContent{}
var _ ControllerRunner = &unionCAContent{}

// NewUnionCAContentProvider returns a CAContentProvider that is a union of other CAContentProviders
//
// NewUnionCAContentProvider 组合多个 CA 提供器；合并后的 bundle 按提供器
// 顺序拼接，任一提供器的更新都会通过监听器传播。
func NewUnionCAContentProvider(caContentProviders ...CAContentProvider) CAContentProvider {
	return unionCAContent(caContentProviders)
}

// Name is just an identifier
//
// Name 用逗号连接各提供器名称，便于日志与指标中识别来源。
func (c unionCAContent) Name() string {
	names := []string{}
	for _, curr := range c {
		names = append(names, curr.Name())
	}
	return strings.Join(names, ",")
}

// CurrentCABundleContent provides ca bundle byte content
//
// CurrentCABundleContent 逐个去除空白并丢弃空 bundle，再用换行拼接，
// 保证 PEM 块之间格式合法且不会出现空行导致的解析异常。
func (c unionCAContent) CurrentCABundleContent() []byte {
	caBundles := [][]byte{}
	for _, curr := range c {
		if currCABytes := curr.CurrentCABundleContent(); len(currCABytes) > 0 {
			caBundles = append(caBundles, []byte(strings.TrimSpace(string(currCABytes))))
		}
	}

	return bytes.Join(caBundles, []byte("\n"))
}

// CurrentCABundleContent provides ca bundle byte content
//
// VerifyOptions 基于合并后的 bundle 构建 x509 校验选项。注释中的函数名
// 是历史遗留（应为 VerifyOptions）；bundle 为空时返回 false 表示不可用。
func (c unionCAContent) VerifyOptions() (x509.VerifyOptions, bool) {
	currCABundle := c.CurrentCABundleContent()
	if len(currCABundle) == 0 {
		return x509.VerifyOptions{}, false
	}

	// TODO make more efficient.  This isn't actually used in any of our mainline paths.  It's called to build the TLSConfig
	// TODO on file changes, but the actual authentication runs against the individual items, not the union.
	// 各子提供器内容此前已校验，合并后仍失败说明是代码缺陷，
	// 因此 panic 而不是静默返回错误，避免带着半可用 TLS 配置继续运行。
	ret, err := newCABundleAndVerifier(c.Name(), c.CurrentCABundleContent())
	if err != nil {
		// because we're made up of already vetted values, this indicates some kind of coding error
		panic(err)
	}

	return ret.verifyOptions, true
}

// AddListener adds a listener to be notified when the CA content changes.
//
// AddListener 把监听器注册到每个子提供器，任一来源变化都能触发通知。
func (c unionCAContent) AddListener(listener Listener) {
	for _, curr := range c {
		curr.AddListener(listener)
	}
}

// AddListener adds a listener to be notified when the CA content changes.
//
// RunOnce 对实现了 ControllerRunner 的子提供器各执行一次同步；
// 不支持的提供器跳过。错误聚合成一个返回给调用方。
func (c unionCAContent) RunOnce(ctx context.Context) error {
	errors := []error{}
	for _, curr := range c {
		if controller, ok := curr.(ControllerRunner); ok {
			if err := controller.RunOnce(ctx); err != nil {
				errors = append(errors, err)
			}
		}
	}

	return utilerrors.NewAggregate(errors)
}

// Run runs the controller
//
// Run 为每个可运行的子提供器启动独立 goroutine，使各自的重试与 watch
// 互不阻塞。
func (c unionCAContent) Run(ctx context.Context, workers int) {
	for _, curr := range c {
		if controller, ok := curr.(ControllerRunner); ok {
			go controller.Run(ctx, workers)
		}
	}
}
