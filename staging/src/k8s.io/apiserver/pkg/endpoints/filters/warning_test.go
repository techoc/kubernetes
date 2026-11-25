/*
Copyright 2020 The Kubernetes Authors.

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

package filters

import (
	"net/http/httptest"
	"reflect"
	"testing"
)

// Test_recorder_AddWarning 覆盖 Warning 头的序列化与去重。
// agent 为空的警告格式为 `299 - "text"`；同一 text 只保留第一次出现，
// 非法 agent/含换行 text 因会破坏 HTTP 头语法而被丢弃。
func Test_recorder_AddWarning(t *testing.T) {
	type args struct {
		agent string
		text  string
	}
	tests := []struct {
		name   string
		args   []args
		expect []string
	}{
		{
			name:   "empty",
			args:   []args{},
			expect: nil,
		},
		{
			name:   "empty values",
			args:   []args{{agent: "", text: ""}},
			expect: nil,
		},
		{
			name:   "empty agent",
			args:   []args{{agent: "", text: "mytext"}},
			expect: []string{`299 - "mytext"`},
		},
		{
			name:   "simple",
			args:   []args{{agent: "myagent", text: "mytext"}},
			expect: []string{`299 myagent "mytext"`},
		},
		{
			name: "duplicate text",
			args: []args{
				{agent: "myagent", text: "mytext"},
				{agent: "myagent2", text: "mytext"},
			},
			expect: []string{`299 myagent "mytext"`},
		},
		{
			name: "multiple",
			args: []args{
				{agent: "", text: "mytext1"},
				{agent: "", text: "mytext2"},
				{agent: "", text: "mytext3"},
			},
			expect: []string{
				`299 - "mytext1"`,
				`299 - "mytext2"`,
				`299 - "mytext3"`,
			},
		},

		{
			name:   "invalid agent",
			args:   []args{{agent: "my agent", text: "mytext"}},
			expect: nil,
		},
		{
			name:   "invalid text",
			args:   []args{{agent: "myagent", text: "my\ntext"}},
			expect: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responseRecorder := httptest.NewRecorder()
			warningRecorder := &recorder{writer: responseRecorder}
			for _, arg := range tt.args {
				warningRecorder.AddWarning(arg.agent, arg.text)
			}
			if !reflect.DeepEqual(tt.expect, responseRecorder.Header()["Warning"]) {
				t.Errorf("expected\n%#v\ngot\n%#v", tt.expect, responseRecorder.Header()["Warning"])
			}
		})
	}
}

// TestTruncation 验证警告总量/单项长度超限时的截断策略：先按单项截断，
// 再按总量截断最早条目，且保持原有顺序。
func TestTruncation(t *testing.T) {
	// 临时缩小阈值，避免测试依赖生产常量；defer 恢复以免影响其它用例。
	originalTotalRunes, originalItemRunes := truncateAtTotalRunes, truncateItemRunes
	truncateAtTotalRunes, truncateItemRunes = 25, 5
	defer func() {
		truncateAtTotalRunes, truncateItemRunes = originalTotalRunes, originalItemRunes
	}()

	responseRecorder := httptest.NewRecorder()
	warningRecorder := &recorder{writer: responseRecorder}

	// 添加超过单条长度上限的条目
	warningRecorder.AddWarning("", "aaaaaaaaaa")  // 长条目
	warningRecorder.AddWarning("-", "aaaaaaaaaa") // 重复条目
	warningRecorder.AddWarning("", "bb")          // 短条目
	warningRecorder.AddWarning("", "ccc")         // 短条目
	warningRecorder.AddWarning("", "Iñtërnâtiô")  // 长条目
	// 确认它们被原样保留
	if e, a := []string{`299 - "aaaaaaaaaa"`, `299 - "bb"`, `299 - "ccc"`, `299 - "Iñtërnâtiô"`}, responseRecorder.Header()["Warning"]; !reflect.DeepEqual(e, a) {
		t.Errorf("expected\n%#v\ngot\n%#v", e, a)
	}
	// 添加超出长度上限并触发截断的条目，把已有条目缩减到 15 个 rune
	warningRecorder.AddWarning("", "e")          // 触发截断的条目，累计 16
	warningRecorder.AddWarning("", "ffffffffff") // 会被截断的长条目，累计 21
	warningRecorder.AddWarning("", "ffffffffff") // 重复条目
	warningRecorder.AddWarning("", "gggggggggg") // 会被截断的条目，累计 26
	warningRecorder.AddWarning("", "h")          // 超出上限后会被忽略的条目
	// 确认已有条目被截断且顺序保持不变
	if e, a := []string{`299 - "aaaaa"`, `299 - "bb"`, `299 - "ccc"`, `299 - "Iñtër"`, `299 - "e"`, `299 - "fffff"`, `299 - "ggggg"`}, responseRecorder.Header()["Warning"]; !reflect.DeepEqual(e, a) {
		t.Errorf("expected\n%#v\ngot\n%#v", e, a)
	}
}
