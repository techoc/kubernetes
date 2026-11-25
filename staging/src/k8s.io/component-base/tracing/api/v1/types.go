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

package v1

// TracingConfiguration 为 OpenTelemetry 追踪客户端提供带版本的配置。
type TracingConfiguration struct {
	// +optional
	//
	// 接受 host:port、dns:// 与 unix:// 形式；http(s):// 前缀会被校验拒绝。
	// 当前连接未加密，尚不支持 TLS。建议不设置该字段，此时使用 OTLP gRPC 默认端点 localhost:4317。
	Endpoint *string `json:"endpoint,omitempty"`

	// SamplingRatePerMillion 表示每百万个 span 中采集的样本数。
	// +optional
	//
	// 未设置时沿用父 span 的采样决策；取值超出 [0,1000000] 会被校验拒绝。
	SamplingRatePerMillion *int32 `json:"samplingRatePerMillion,omitempty"`
}
