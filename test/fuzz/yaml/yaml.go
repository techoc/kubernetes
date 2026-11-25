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

// Package yaml 为 Kubernetes 中的 yaml 反序列化例程
// 实现模糊测试。这些 target 兼容
// github.com/dvyukov/go-fuzz 模糊测试框架。
package yaml

import (
	"fmt"
	"strings"

	yaml "go.yaml.in/yaml/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sigyaml "sigs.k8s.io/yaml"
)

// FuzzDurationStrict 是针对 "k8s.io/apimachinery/pkg/apis/meta/v1" 中定义的
// Duration 严格反序列化的模糊测试 target。该 target 还检查反序列化结果
// 能否重新序列化回输入。
func FuzzDurationStrict(b []byte) int {
	var durationHolder struct {
		D metav1.Duration `json:"d"`
	}
	if err := sigyaml.UnmarshalStrict(b, &durationHolder); err != nil {
		return 0
	}
	result, err := sigyaml.Marshal(&durationHolder)
	if err != nil {
		panic(err)
	}
	// 结果格式为 "d: <duration>\n"，因此去掉末尾换行，
	// 并将 durationHolder.D 转换为期望的格式。
	resultStr := strings.TrimSpace(string(result[:]))
	inputStr := fmt.Sprintf("d: %s", durationHolder.D.Duration)
	if resultStr != inputStr {
		panic(fmt.Sprintf("result(%v) != input(%v)", resultStr, inputStr))
	}
	return 1
}

// FuzzMicroTimeStrict 是针对 "k8s.io/apimachinery/pkg/apis/meta/v1" 中定义的
// MicroTime 严格反序列化的模糊测试 target。该 target 还检查
// 反序列化结果能否重新序列化回输入。
func FuzzMicroTimeStrict(b []byte) int {
	var microTimeHolder struct {
		T metav1.MicroTime `json:"t"`
	}
	if err := sigyaml.UnmarshalStrict(b, &microTimeHolder); err != nil {
		return 0
	}
	result, err := sigyaml.Marshal(&microTimeHolder)
	if err != nil {
		panic(err)
	}
	// 结果格式为 "t: <time>\n"，因此去掉末尾换行，
	// 并将 microTimeHolder.T 转换为期望的格式。若时间为零，
	// 该值会被序列化为 "null"。
	resultStr := strings.TrimSpace(string(result[:]))
	var inputStr string
	if microTimeHolder.T.Time.IsZero() {
		inputStr = "t: null"
	} else {
		inputStr = fmt.Sprintf("t: %s", microTimeHolder.T.Time)
	}
	if resultStr != inputStr {
		panic(fmt.Sprintf("result(%v) != input(%v)", resultStr, inputStr))
	}
	return 1
}

// FuzzSigYaml 是针对 "sigs.k8s.io/yaml" 反序列化的模糊测试 target。
func FuzzSigYaml(b []byte) int {
	t := struct{}{}
	m := map[string]interface{}{}
	var out int
	if err := sigyaml.Unmarshal(b, &m); err == nil {
		out = 1
	}
	if err := sigyaml.Unmarshal(b, &t); err == nil {
		out = 1
	}
	return out
}

// FuzzTimeStrict 是针对 "k8s.io/apimachinery/pkg/apis/meta/v1" 中定义的
// Time 严格反序列化的模糊测试 target。该 target 还检查
// 反序列化结果能否重新序列化回输入。
func FuzzTimeStrict(b []byte) int {
	var timeHolder struct {
		T metav1.Time `json:"t"`
	}
	if err := sigyaml.UnmarshalStrict(b, &timeHolder); err != nil {
		return 0
	}
	result, err := sigyaml.Marshal(&timeHolder)
	if err != nil {
		panic(err)
	}
	// 结果格式为 "t: <time>\n"，因此去掉末尾换行，
	// 并将 timeHolder.T 转换为期望的格式。若时间为零，
	// 该值会被序列化为 "null"。
	resultStr := strings.TrimSpace(string(result[:]))
	var inputStr string
	if timeHolder.T.Time.IsZero() {
		inputStr = "t: null"
	} else {
		inputStr = fmt.Sprintf("t: %s", timeHolder.T.Time)
	}
	if resultStr != inputStr {
		panic(fmt.Sprintf("result(%v) != input(%v)", resultStr, inputStr))
	}
	return 1
}

// FuzzYamlV2 是针对 "go.yaml.in/yaml/v2" 反序列化的模糊测试 target。
func FuzzYamlV2(b []byte) int {
	t := struct{}{}
	m := map[string]interface{}{}
	var out int
	if err := yaml.Unmarshal(b, &m); err == nil {
		out = 1
	}
	if err := yaml.Unmarshal(b, &t); err == nil {
		out = 1
	}
	return out
}
