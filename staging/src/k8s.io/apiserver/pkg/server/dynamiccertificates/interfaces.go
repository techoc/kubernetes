/*
Copyright 2021 The Kubernetes Authors.

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

package dynamiccertificates

import (
	"crypto/x509"
)

// Listener 用于在内容变化时通知订阅者。
//
// 监听方只负责把自己标记为“需要重算”，真正的读取发生在下次 Enqueue 之后，
// 从而把变更通知与昂贵的证书解析解耦。
type Listener interface {
	// 输入可能变化时应调用 Enqueue
	Enqueue()
}

// Notifier 提供添加 listener 的方式
type Notifier interface {
	// AddListener 添加一个监听输入变化的 listener。
	// 对静态 provider 这是空操作。
	//
	// 静态提供者永远不会变化，因此无需注册监听，调用应为无操作。
	AddListener(listener Listener)
}

// CAContentProvider 提供 CA bundle 字节内容
type CAContentProvider interface {
	Notifier

	// Name 只是标识符。
	Name() string
	// CurrentCABundleContent 提供 CA bundle 字节内容。错误可
	// 限制在初始化该值的 controller 内。走到这里时，
	// 返回值应保证不会失败。
	//
	// 这里不返回 error：初始化阶段的错误由控制器内部记录并降级处理，
	// 调用点必须能拿到一个可用的（可能是空或旧的）内容。
	CurrentCABundleContent() []byte
	// VerifyOptions 为认证器提供 VerifyOptions。
	VerifyOptions() (x509.VerifyOptions, bool)
}

// CertKeyContentProvider 提供证书与匹配的私钥。
type CertKeyContentProvider interface {
	Notifier

	// Name 只是标识符。
	Name() string
	// CurrentCertKeyContent 提供证书与私钥字节内容。
	CurrentCertKeyContent() ([]byte, []byte)
}

// SNICertKeyContentProvider 提供证书、匹配私钥，
// 以及可选的直接指定名称。
type SNICertKeyContentProvider interface {
	Notifier

	CertKeyContentProvider
	// SNINames 提供 SNI 使用的名称，可返回 nil。
	SNINames() []string
}
