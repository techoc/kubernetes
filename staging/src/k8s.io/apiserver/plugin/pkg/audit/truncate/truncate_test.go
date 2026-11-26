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

package truncate

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	"k8s.io/apiserver/plugin/pkg/audit/fake"
	// 仅为获取 schema 定义而导入。
	_ "k8s.io/apiserver/plugin/pkg/audit/webhook"
)

var (
	// defaultConfig 模拟生产默认值：批次上限 4MB，单事件上限 100KB。
	defaultConfig = Config{
		MaxBatchSize: 4 * 1024 * 1024,
		MaxEventSize: 100 * 1024,
	}
)

// TestTruncatingEvents 验证 truncate 后端对超限事件的处理策略：
// 事件体（request/response object）过大时截断并打注解；
// 元数据（如 annotations）本身过大时无法通过截断挽救，只能丢弃整个事件。
func TestTruncatingEvents(t *testing.T) {
	testCases := []struct {
		desc          string
		event         *auditinternal.Event
		wantDropped   bool
		wantTruncated bool
	}{
		{
			// 空事件体积很小，无需处理
			desc:  "Empty event should not be truncated",
			event: &auditinternal.Event{},
		},
		{
			// RequestObject 过大：可截断，事件仍应送达
			desc: "Event with too large body should be truncated",
			event: &auditinternal.Event{
				Level: auditinternal.LevelRequest,
				RequestObject: &runtime.Unknown{
					Raw: []byte("\"" + strings.Repeat("A", int(defaultConfig.MaxEventSize)) + "\""),
				},
			},
			wantTruncated: true,
		},
		{
			// annotations 过大：截断 body 也无济于事，只能丢弃
			desc: "Event with too large metadata should be dropped",
			event: &auditinternal.Event{
				Annotations: map[string]string{
					"key": strings.Repeat("A", int(defaultConfig.MaxEventSize)),
				},
			},
			wantDropped: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			var event *auditinternal.Event

			fb := &fake.Backend{
				OnRequest: func(events []*auditinternal.Event) {
					require.Len(t, events, 1, "Expected single event in batch")
					event = events[0]
				},
			}
			b := NewBackend(fb, defaultConfig, auditv1.SchemeGroupVersion)
			b.ProcessEvents(tc.event)

			require.Equal(t, !tc.wantDropped, event != nil, "Incorrect event presence")
			if tc.wantTruncated {
				// 截断后必须留下注解说明，且两个对象体都被清空。
				require.Equal(t, annotationValue, event.Annotations[annotationKey], "Annotation should be present")
				require.Nil(t, event.RequestObject, "After truncation request should be nil")
				require.Nil(t, event.ResponseObject, "After truncation response should be nil")
			}
		})
	}
}

// TestSplittingBatches 验证 truncate 后端会把超出 MaxBatchSize 的事件拆成多个批次。
func TestSplittingBatches(t *testing.T) {
	testCases := []struct {
		desc           string
		config         Config
		events         []*auditinternal.Event
		wantBatchCount int
	}{
		{
			// 单事件在默认 4MB 批次内，不应拆分
			desc:           "Events fitting in one batch should not be split",
			config:         defaultConfig,
			events:         []*auditinternal.Event{{}},
			wantBatchCount: 1,
		},
		{
			// 批次上限设为 1 字节，两个事件必然各成一批
			desc: "Events not fitting in one batch should be split",
			config: Config{
				MaxEventSize: defaultConfig.MaxEventSize,
				MaxBatchSize: 1,
			},
			events: []*auditinternal.Event{
				{Annotations: map[string]string{"key": strings.Repeat("A", int(50))}},
				{Annotations: map[string]string{"key": strings.Repeat("A", int(50))}},
			},
			wantBatchCount: 2,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			var gotBatchCount int
			fb := &fake.Backend{
				OnRequest: func(events []*auditinternal.Event) {
					gotBatchCount++
				},
			}
			b := NewBackend(fb, tc.config, auditv1.SchemeGroupVersion)
			b.ProcessEvents(tc.events...)

			require.Equal(t, tc.wantBatchCount, gotBatchCount)
		})
	}
}
