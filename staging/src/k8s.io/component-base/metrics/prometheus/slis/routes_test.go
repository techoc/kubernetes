/*
Copyright 2024 The Kubernetes Authors.

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

package slis

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockMux 记录 Install 注册的路径，替代真实 ServeMux。
type mockMux struct {
	handledPaths []string
}

// Handle 实现 Install 所需的最小 mux 接口。
func (m *mockMux) Handle(path string, handler http.Handler) {
	m.handledPaths = append(m.handledPaths, path)
}

// TestSLIMetrics_Install 验证 Install 会把 /metrics/slis 挂到 mux；
// 重复调用会重复注册，说明 Install 本身不做幂等保护，调用方需自行保证一次。
func TestSLIMetrics_Install(t *testing.T) {
	m := &mockMux{}
	s := SLIMetrics{}

	s.Install(m)
	assert.Equal(t, []string{"/metrics/slis"}, m.handledPaths)

	s.Install(m)
	// 固定记录该行为：两次调用 Install 会重复挂载路径，而不是去重。
	assert.Equal(t, []string{"/metrics/slis", "/metrics/slis"}, m.handledPaths, "Should handle the path twice.")
}
