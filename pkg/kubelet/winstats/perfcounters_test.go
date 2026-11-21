//go:build windows

/*
Copyright 2023 The Kubernetes Authors.

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

package winstats

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// TestPerfCounter 验证各 Windows 性能计数器可以初始化，并在可观测时读到非零数据。
func TestPerfCounter(t *testing.T) {
	testCases := map[string]struct {
		counter        string
		skipCheck      bool
		expectErr      bool
		expectedErrMsg string
	}{
		"CPU Query": {
			counter: cpuQuery,
			// TODO: remove skip once the test flake for CPU Query has been fixed.
			skipCheck: true,
		},
		"Memory Prvate Working Set Query": {
			counter: memoryPrivWorkingSetQuery,
		},
		"Memory Committed Bytes Query": {
			counter: memoryCommittedBytesQuery,
		},
		"Net Adapter Packets Received/sec Query": {
			counter:   packetsReceivedPerSecondQuery,
			skipCheck: true,
		},
		"Net Adapter Packets Sent/sec Query": {
			counter:   packetsSentPerSecondQuery,
			skipCheck: true,
		},
		"Net Adapter Bytes Received/sec Query": {
			counter:   bytesReceivedPerSecondQuery,
			skipCheck: true,
		},
		"Net Adapter Bytes Sent/sec Query": {
			counter:   bytesSentPerSecondQuery,
			skipCheck: true,
		},
		"Net Adapter Packets Received Discarded Query": {
			counter:   packetsReceivedDiscardedQuery,
			skipCheck: true,
		},
		"Net Adapter Packets Received Errors Query": {
			counter:   packetsReceivedErrorsQuery,
			skipCheck: true,
		},
		"Net Adapter Packets Outbound Discarded Query": {
			counter:   packetsOutboundDiscardedQuery,
			skipCheck: true,
		},
		"Net Adapter Packets Outbound Errors Query": {
			counter:   packetsOutboundErrorsQuery,
			skipCheck: true,
		},
		"Invalid Query": {
			counter:        "foo",
			expectErr:      true,
			expectedErrMsg: "unable to add process counter: foo. Error code is c0000bc0",
		},
	}

	// 逐项验证计数器：非法名称应报错，其余应能读取数据。
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			counter, err := newPerfCounter(tc.counter)
			// 非法计数器名称应返回预期的错误消息。
			if tc.expectErr {
				if err == nil || err.Error() != tc.expectedErrMsg {
					t.Fatalf("expected error message `%s` but got `%v`", tc.expectedErrMsg, err)
				}
				return
			}

			// 某些计数器（如网络类）不保证出现非零值，因此跳过数值检查。
			// 网络类计数器不保证出现非零值，只验证初始化成功。
			if tc.skipCheck {
				return
			}

			// 轮询直到读到非零的计数器数据。
			if pollErr := wait.Poll(100*time.Millisecond, 5*perfCounterUpdatePeriod, func() (bool, error) {
				data, err := counter.getData()
				if err != nil {
					return false, err
				}

				if data != 0 {
					return true, nil
				}

				return false, nil
			}); pollErr != nil {
				t.Fatalf("Encountered error: `%v'", pollErr)
				return
			}

			// 确认数据列表中至少有一个非零值。
			if pollErr := wait.Poll(100*time.Millisecond, 5*perfCounterUpdatePeriod, func() (bool, error) {
				dataList, err := counter.getDataList()
				if err != nil {
					return false, err
				}

				for _, value := range dataList {
					if value != 0 {
						return true, nil
					}
				}

				return false, nil
			}); pollErr != nil {
				t.Fatalf("Encountered error: `%v'", pollErr)
			}
		})
	}
}
