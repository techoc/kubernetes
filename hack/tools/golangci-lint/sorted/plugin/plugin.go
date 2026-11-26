/*
Copyright 2025 The Kubernetes Authors.

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

// 必须是 package main，golangci-lint 插件入口的要求
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"golang.org/x/tools/go/analysis"

	"k8s.io/kubernetes/hack/tools/golangci-lint/sorted/pkg"
)

type analyzerPlugin struct{}

func (*analyzerPlugin) GetAnalyzers() []*analysis.Analyzer {
	return []*analysis.Analyzer{pkg.NewAnalyzer()}
}

// AnalyzerPlugin 是 golangci-lint 的插件入口。
var AnalyzerPlugin analyzerPlugin

// settings 定义 sorted linter 的配置项
type settings struct {
	// Debug 开启调试日志
	Debug bool `json:"debug"`
	// Files 指定待检查的文件
	Files []string `json:"files"`
}

// 检查 feature gate 排序时默认使用的文件列表
var defaultTargetFiles = []string{
	"pkg/features/kube_features.go",
	"staging/src/k8s.io/apiserver/pkg/features/kube_features.go",
	"staging/src/k8s.io/client-go/features/known_features.go",
	"staging/src/k8s.io/controller-manager/pkg/features/kube_features.go",
	"staging/src/k8s.io/apiextensions-apiserver/pkg/features/kube_features.go",
	"test/e2e/feature/feature.go",
	"test/e2e/environment/environment.go",
}

// New 是 golangci-lint 插件系统的入口
func New(pluginSettings interface{}) ([]*analysis.Analyzer, error) {
	// 使用默认配置
	config := pkg.Config{}

	// 解析传入的配置
	if pluginSettings != nil {
		var s settings
		// 借由 JSON 往返转换，避免手工逐字段解析
		var buffer bytes.Buffer
		if err := json.NewEncoder(&buffer).Encode(pluginSettings); err != nil {
			return nil, fmt.Errorf("encoding settings as internal JSON buffer: %v", err)
		}

		decoder := json.NewDecoder(&buffer)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&s); err != nil {
			return nil, fmt.Errorf("decoding settings from internal JSON buffer: %v", err)
		}

		// 把配置应用到 config
		config.Debug = s.Debug
		config.Files = append(config.Files, s.Files...)
		if len(config.Files) == 0 {
			// 未指定文件时回退到默认目标文件
			config.Files = defaultTargetFiles
		}

		if config.Debug {
			fmt.Printf("sorted settings: %+v\n", s)
			fmt.Printf("final config: %+v\n", config)
		}
	}

	// 按配置创建 analyzer
	analyzer := pkg.NewAnalyzerWithConfig(config)

	// 返回 analyzer
	return []*analysis.Analyzer{analyzer}, nil
}
