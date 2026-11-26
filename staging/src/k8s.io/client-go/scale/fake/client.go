/*
Copyright 2017 The Kubernetes Authors.

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

// Package fake 为任意 Kubernetes API 提供 fake 客户端接口，暴露通用的高层操作与
// metadata.
//
// Package fake 实现 scale.ScalesGetter 的测试替身：请求通过 testing.Fake
// 的 reactor 分发，调用方可按需注入返回的 Scale 对象或错误。
package fake

import (
	"context"

	autoscalingapi "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/testing"
)

// FakeScaleClient 提供 scale.ScalesGetter 的 fake 实现。
type FakeScaleClient struct {
	testing.Fake
}

// Scales 返回绑定到 namespace 的 ScaleInterface；每个调用都共享同一个
// testing.Fake，因此 reactor 注册在各个 namespace 之间通用。
func (f *FakeScaleClient) Scales(namespace string) scale.ScaleInterface {
	return &fakeNamespacedScaleClient{
		namespace: namespace,
		fake:      &f.Fake,
	}
}

// fakeNamespacedScaleClient 把 namespace 与共享的 testing.Fake 组合，
// 实现 namespaced 的 scale 操作。
type fakeNamespacedScaleClient struct {
	namespace string
	fake      *testing.Fake
}

// Get 通过 GetSubresourceAction 请求 scale 子资源。
// resource.WithVersion("") 清空版本，因为 ScaleInterface 只以 GroupResource 定位。
func (f *fakeNamespacedScaleClient) Get(ctx context.Context, resource schema.GroupResource, name string, opts metav1.GetOptions) (*autoscalingapi.Scale, error) {
	obj, err := f.fake.
		Invokes(testing.NewGetSubresourceAction(resource.WithVersion(""), f.namespace, "scale", name), &autoscalingapi.Scale{})

	if err != nil {
		return nil, err
	}

	return obj.(*autoscalingapi.Scale), err
}

// Update 通过 UpdateSubresourceAction 更新 scale 子资源。
func (f *fakeNamespacedScaleClient) Update(ctx context.Context, resource schema.GroupResource, scale *autoscalingapi.Scale, opts metav1.UpdateOptions) (*autoscalingapi.Scale, error) {
	obj, err := f.fake.
		Invokes(testing.NewUpdateSubresourceAction(resource.WithVersion(""), "scale", f.namespace, scale), &autoscalingapi.Scale{})

	if err != nil {
		return nil, err
	}

	return obj.(*autoscalingapi.Scale), err
}

// Patch 通过 PatchSubresourceAction 打补丁；此处接收完整 GVR，
// 因为补丁需要明确版本才能构造正确的资源路径。
func (f *fakeNamespacedScaleClient) Patch(ctx context.Context, gvr schema.GroupVersionResource, name string, pt types.PatchType, patch []byte, opts metav1.PatchOptions) (*autoscalingapi.Scale, error) {
	obj, err := f.fake.
		Invokes(testing.NewPatchSubresourceAction(gvr, f.namespace, name, pt, patch, "scale"), &autoscalingapi.Scale{})

	if err != nil {
		return nil, err
	}

	return obj.(*autoscalingapi.Scale), err
}
