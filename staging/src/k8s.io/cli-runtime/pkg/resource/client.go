/*
Copyright 2018 The Kubernetes Authors.

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

package resource

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// TODO require negotiatedSerializer.  leaving it optional lets us plumb current behavior and deal with the difference after major plumbing is complete
//
// clientForGroupVersion 按 GroupVersion 构造类型化 REST client。
// 返回值中的 APIPath 必须与 group 是否为空对应：核心组走 /api，
// 其余 API 组走 /apis，否则请求 URL 会拼错。
func (clientConfigFn ClientConfigFunc) clientForGroupVersion(gv schema.GroupVersion, negotiatedSerializer runtime.NegotiatedSerializer) (RESTClient, error) {
	cfg, err := clientConfigFn()
	if err != nil {
		return nil, err
	}
	// 调用方未指定 serializer 时沿用配置里的默认值，避免强行覆盖。
	if negotiatedSerializer != nil {
		cfg.ContentConfig.NegotiatedSerializer = negotiatedSerializer
	}
	cfg.GroupVersion = &gv
	if len(gv.Group) == 0 {
		cfg.APIPath = "/api"
	} else {
		cfg.APIPath = "/apis"
	}

	return rest.RESTClientFor(cfg)
}

// unstructuredClientForGroupVersion 构造统一使用 unstructured 编解码的 REST client，
// 适合事先不知道具体类型（CRD、聚合资源）的场景。
func (clientConfigFn ClientConfigFunc) unstructuredClientForGroupVersion(gv schema.GroupVersion) (RESTClient, error) {
	cfg, err := clientConfigFn()
	if err != nil {
		return nil, err
	}
	cfg.ContentConfig = UnstructuredPlusDefaultContentConfig()
	cfg.GroupVersion = &gv
	if len(gv.Group) == 0 {
		cfg.APIPath = "/api"
	} else {
		cfg.APIPath = "/apis"
	}

	return rest.RESTClientFor(cfg)
}

// withStdinUnavailable 包装 ClientConfigFunc，标记 exec 认证插件不可使用 stdin。
// 原因：stdin 已被 -f - 的资源读取占用，若认证插件再读 stdin 会与之竞争；
// 这里通过 StdinUnavailableMessage 给用户一个可诊断的原因说明。
func (clientConfigFn ClientConfigFunc) withStdinUnavailable(stdinUnavailable bool) ClientConfigFunc {
	return func() (*rest.Config, error) {
		cfg, err := clientConfigFn()
		if stdinUnavailable && cfg != nil && cfg.ExecProvider != nil {
			cfg.ExecProvider.StdinUnavailable = stdinUnavailable
			cfg.ExecProvider.StdinUnavailableMessage = "used by stdin resource manifest reader"
		}
		return cfg, err
	}
}
