/*
Copyright The Kubernetes Authors.

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

// Package cache 是客户端缓存机制，用于减少发往服务端的请求。Reflector 监听服务端并更新 Store；
// 包内提供两种 Store：一种只缓存对象（如供调度器列出可用节点），另一种还兼作 FIFO 队列
// （如供调度器处理新来的 Pod）。
package cache

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	podsGVR       = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	configMapsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
)

// TestNewInformerName 验证 InformerName 的全局唯一性约束：
// 非空名称成功注册，重复名称报错，Release 后名称可复用。
func TestNewInformerName(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		inName  string
		wantErr bool
	}{
		{
			// 首个使用该名称的 informer 应注册成功。
			name:    "first name is unique",
			setup:   func() {},
			inName:  "my-informer",
			wantErr: false,
		},
		{
			// 同名 informer 再次注册应被拒绝，避免指标/日志混淆。
			name: "duplicate name returns error",
			setup: func() {
				_, _ = NewInformerName("my-informer")
			},
			inName:  "my-informer",
			wantErr: true,
		},
		{
			// 空名称没有可观测的身份，直接报错。
			name:    "empty name returns error",
			setup:   func() {},
			inName:  "",
			wantErr: true,
		},
		{
			// 不同名称互不影响。
			name: "different name is unique",
			setup: func() {
				_, _ = NewInformerName("informer-1")
			},
			inName:  "informer-2",
			wantErr: false,
		},
		{
			// Release 应释放全局占位，使名称可以重新注册。
			name: "released name can be reused",
			setup: func() {
				n, _ := NewInformerName("my-informer")
				n.Release()
			},
			inName:  "my-informer",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 名称注册表是包级全局状态，每个用例前必须清空以保证独立。
			ResetInformerNamesForTesting()
			tt.setup()

			_, err := NewInformerName(tt.inName)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewInformerName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestWithResource 验证 WithResource 对同一 (name, GVR) 组合的幂等性：
// 首次绑定返回已保留的标识，重复绑定不再视为新保留。
func TestWithResource(t *testing.T) {
	tests := []struct {
		name         string
		setup        func() *InformerName
		gvr          schema.GroupVersionResource
		wantReserved bool
	}{
		{
			// 某个名称首次绑定某 GVR，应视为新保留。
			name: "first GVR is reserved",
			setup: func() *InformerName {
				n, _ := NewInformerName("my-informer")
				return n
			},
			gvr:          podsGVR,
			wantReserved: true,
		},
		{
			// 同一 (name, GVR) 再次绑定是幂等的，不算新保留。
			name: "same GVR second time is not reserved",
			setup: func() *InformerName {
				n, _ := NewInformerName("my-informer")
				_ = n.WithResource(podsGVR)
				return n
			},
			gvr:          podsGVR,
			wantReserved: false,
		},
		{
			// 同名 informer 绑定新的 GVR，属于新的保留。
			name: "different GVR is reserved",
			setup: func() *InformerName {
				n, _ := NewInformerName("my-informer")
				_ = n.WithResource(podsGVR)
				return n
			},
			gvr:          configMapsGVR,
			wantReserved: true,
		},
		{
			// nil InformerName 允许调用 WithResource 而不 panic，但永远不会“保留”。
			name: "nil InformerName returns not reserved",
			setup: func() *InformerName {
				return nil
			},
			gvr:          podsGVR,
			wantReserved: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ResetInformerNamesForTesting()
			n := tt.setup()

			id := n.WithResource(tt.gvr)
			if got := id.Reserved(); got != tt.wantReserved {
				t.Errorf("Reserved() = %v, want %v", got, tt.wantReserved)
			}
		})
	}
}

// TestRelease 验证 Release 会同时释放名称与已绑定的资源标识，
// 且释放后可以重新注册同名 informer。
func TestRelease(t *testing.T) {
	ResetInformerNamesForTesting()

	n, err := NewInformerName("my-informer")
	if err != nil {
		t.Fatalf("NewInformerName() error = %v", err)
	}

	// 获取一个已保留的标识。
	id := n.WithResource(podsGVR)
	if !id.Reserved() {
		t.Error("Expected Reserved() = true before Release()")
	}

	// 释放名称。
	n.Release()

	// 标识应不再处于保留状态。
	if id.Reserved() {
		t.Error("Expected Reserved() = false after Release()")
	}

	// 名称应可重新使用。
	n2, err := NewInformerName("my-informer")
	if err != nil {
		t.Errorf("NewInformerName() after Release() error = %v", err)
	}

	// 由新名称产生的新标识应处于保留状态。
	id2 := n2.WithResource(podsGVR)
	if !id2.Reserved() {
		t.Error("Expected Reserved() = true for new InformerName after Release()")
	}
}
