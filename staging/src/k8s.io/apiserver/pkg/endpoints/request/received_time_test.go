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

package request

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// TestWithRequestReceiveTime 验证请求到达时间在 context 中的存取：
// 非零时间必须能原样取回，零值时间必须被视为“未设置”，
// 以便超时过滤器区分“已有到达时间”和“从未记录”。
func TestWithRequestReceiveTime(t *testing.T) {
	tests := []struct {
		name              string
		receivedTimestamp time.Time
		expected          bool
	}{
		{
			name:              "request received time is set",
			receivedTimestamp: time.Now(),
			expected:          true,
		},
		{
			name:              "request received time is empty",
			receivedTimestamp: time.Time{},
			expected:          false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent := context.TODO()
			ctx := WithReceivedTimestamp(parent, test.receivedTimestamp)
			// 空时间不应被写入 context，否则下游会把零值当作真实时间。
			if ctx == nil {
				t.Fatal("WithReceivedTimestamp: expected a non nil context, got nil")
			}

			receivedTimestampGot, ok := ReceivedTimestampFrom(ctx)
			if test.expected != ok {
				t.Errorf("ReceivedTimestampFrom: expected=%s got=%s", strconv.FormatBool(test.expected), strconv.FormatBool(ok))
			}

			if test.receivedTimestamp != receivedTimestampGot {
				t.Errorf("ReceivedTimestampFrom: received timestamp expected=%s but got=%s", test.receivedTimestamp, receivedTimestampGot)
			}
		})
	}
}
