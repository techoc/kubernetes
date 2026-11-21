/*
Copyright 2014 The Kubernetes Authors.

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

package config

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestConfigurationChannels 验证同名来源返回同一个通道，不同名称返回不同通道。
func TestConfigurationChannels(t *testing.T) {
	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("TestConfigurationChannels completed")

	// 使用无 merger 的 mux 测试通道复用。
	mux := newMux(nil)
	channelOne := mux.ChannelWithContext(tCtx, "one")
	if channelOne != mux.ChannelWithContext(tCtx, "one") {
		t.Error("Didn't get the same muxuration channel back with the same name")
	}
	channelTwo := mux.ChannelWithContext(tCtx, "two")
	if channelOne == channelTwo {
		t.Error("Got back the same muxuration channel for different names")
	}
}

// TestMergeInvoked 验证写入通道的更新会触发 merger 回调。
func TestMergeInvoked(t *testing.T) {
	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("TestMergeInvoked completed")

	// 校验 merge 收到的来源与更新内容。
	const expectedSource = "one"
	done := make(chan interface{})
	var merger mergeFunc = func(ctx context.Context, source string, update sourceUpdate) error {
		if expectedSource != source {
			t.Errorf("Expected %s, Got %s", expectedSource, source)
		}
		expectedUpdate := fakeUpdate(expectedSource)
		if !reflect.DeepEqual(expectedUpdate, update) {
			t.Errorf("Expected %v, Got %v", expectedUpdate, update)
		}
		close(done)
		return nil
	}

	mux := newMux(&merger)

	mux.ChannelWithContext(tCtx, expectedSource) <- fakeUpdate(expectedSource)

	// 等待 Merge 被调用。
	select {
	case <-done:
		// 测试完成。
	case <-tCtx.Done():
		t.Fatal("Test context canceled before completion")
	}
}

// mergeFunc 实现 Merger 接口。
type mergeFunc func(ctx context.Context, source string, update sourceUpdate) error

// Merge 调用底层函数完成合并。
func (f mergeFunc) Merge(ctx context.Context, source string, update sourceUpdate) error {
	return f(ctx, source, update)
}

// TestSimultaneousMerge 验证多个来源可以并发触发 merge。
func TestSimultaneousMerge(t *testing.T) {
	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("TestSimultaneousMerge completed")

	// 两个来源各触发一次 merge，通道缓冲 2 个信号。
	ch := make(chan bool, 2)
	mux := newMux(mergeFunc(func(ctx context.Context, source string, update sourceUpdate) error {
		if nsSource := update.Pods[0].Namespace; nsSource != source {
			t.Errorf("Expected %s, Got %s", source, nsSource)
		}
		ch <- true
		return nil
	}))
	source := mux.ChannelWithContext(tCtx, "one")
	source2 := mux.ChannelWithContext(tCtx, "two")
	source <- fakeUpdate("one")
	source2 <- fakeUpdate("two")
	<-ch
	<-ch
}

// fakeUpdate 为指定来源构造一个测试用 Pod 更新。
func fakeUpdate(source string) sourceUpdate {
	return sourceUpdate{[]*v1.Pod{{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-pod", source),
			Namespace: source,
			UID:       types.UID(fmt.Sprintf("%s-pod-uid", source)),
		},
	}}}
}
