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

package conditions

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TestPodRunning 以表驱动方式验证 PodRunning 对各类 watch 事件的判断结果，
// 覆盖删除事件、运行中、失败终态以及非 Pod 对象等分支。
func TestPodRunning(t *testing.T) {
	tests := []struct {
		// name 是用例名称，作为子测试名；event 是输入的 watch 事件；
		// want 是期望的布尔返回值；wantErr 表示期望是否返回错误。
		name    string
		event   watch.Event
		want    bool
		wantErr bool
	}{
		{
			// 删除事件应返回 NotFound 错误，而非普通 false。
			name: "Watch type is deleted",
			event: watch.Event{
				Type: watch.Deleted,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Pod Status type is PodRunning",
			event: watch.Event{
				Type: watch.Added,
				Object: &corev1.Pod{
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			// 终态要用 ErrPodCompleted 与"尚未运行"区分，故本例期望出错。
			name: "Pod Status is PodFailed",
			event: watch.Event{
				Type: watch.Added,
				Object: &corev1.Pod{
					Status: corev1.PodStatus{
						Phase: corev1.PodFailed,
					},
				},
			},
			want:    false,
			wantErr: true,
		},
		{
			// 非 Pod 对象不匹配类型断言，应安全地返回 false 而非 panic。
			name: "Object type is not pod",
			event: watch.Event{
				Type:   watch.Added,
				Object: &corev1.Node{},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PodRunning(tt.event)
			// 以"是否出错"做比较而非直接比对错误对象，因为不同分支
			// 可能返回不同类型的错误，此处只关心语义。
			if (err != nil) != tt.wantErr {
				t.Errorf("PodRunning() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PodRunning() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPodCompleted 以表驱动方式验证 PodCompleted 对各类 watch 事件的判断，
// 覆盖删除事件、成功终态、运行中相位以及非 Pod 对象等分支。
func TestPodCompleted(t *testing.T) {
	tests := []struct {
		// name 是用例名称，作为子测试名；event 是输入的 watch 事件；
		// want 是期望的布尔返回值；wantErr 表示期望是否返回错误。
		name    string
		event   watch.Event
		want    bool
		wantErr bool
	}{
		{
			// 删除事件意味着对象已不存在，应返回 NotFound 错误。
			name: "Watch type is deleted",
			event: watch.Event{
				Type: watch.Deleted,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Pod Status is PodSucceeded",
			event: watch.Event{
				Type: watch.Added,
				Object: &corev1.Pod{
					Status: corev1.PodStatus{
						Phase: corev1.PodSucceeded,
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			// Running 只是中间态，不能算作已完成，故期望 false 且无错误。
			name: "Pod Status is PodRunning",
			event: watch.Event{
				Type: watch.Added,
				Object: &corev1.Pod{
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			// 非 Pod 对象不命中类型断言，应返回 false 而非 panic。
			name: "Object type is not pod",
			event: watch.Event{
				Type:   watch.Added,
				Object: &corev1.Node{},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PodCompleted(tt.event)
			// 只校验错误的"有无"，因为不同分支的错误类型并不相同。
			if (err != nil) != tt.wantErr {
				t.Errorf("PodCompleted() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PodCompleted() = %v, want %v", got, tt.want)
			}
		})
	}
}
