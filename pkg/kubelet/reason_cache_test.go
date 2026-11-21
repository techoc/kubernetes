/*
Copyright 2016 The Kubernetes Authors.

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

package kubelet

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// TestReasonCache 验证 ReasonCache 只记录带错误的 StartContainer 同步结果，
// 并在移除后不再返回缓存项。
func TestReasonCache(t *testing.T) {
	// 构造测试用的同步结果。
	syncResult := kubecontainer.PodSyncResult{}
	results := []*kubecontainer.SyncResult{
		// StartContainer 且带错误的 SyncResult 应写入 reason 缓存。
		kubecontainer.NewSyncResult(kubecontainer.StartContainer, "container_1"),
		// StartContainer 但无错误的 SyncResult 不应写入缓存。
		kubecontainer.NewSyncResult(kubecontainer.StartContainer, "container_2"),
		// 其他动作的 SyncResult 不应写入缓存。
		kubecontainer.NewSyncResult(kubecontainer.KillContainer, "container_3"),
	}
	results[0].Fail(kubecontainer.ErrRunContainer, "message_1")
	results[2].Fail(kubecontainer.ErrKillContainer, "message_3")
	syncResult.AddSyncResult(results...)
	uid := types.UID("pod_1")

	reasonCache := NewReasonCache()
	reasonCache.Update(uid, syncResult)
	assertReasonInfo(t, reasonCache, uid, results[0], true)
	assertReasonInfo(t, reasonCache, uid, results[1], false)
	assertReasonInfo(t, reasonCache, uid, results[2], false)

	reasonCache.Remove(uid, results[0].Target.(string))
	assertReasonInfo(t, reasonCache, uid, results[0], false)
}

// assertReasonInfo 断言缓存中是否存在指定同步结果，并核对错误与消息内容。
func assertReasonInfo(t *testing.T, cache *ReasonCache, uid types.UID, result *kubecontainer.SyncResult, found bool) {
	name := result.Target.(string)
	actualReason, ok := cache.Get(uid, name)
	if ok && !found {
		t.Fatalf("unexpected cache hit: %v, %q", actualReason.Err, actualReason.Message)
	}
	if !ok && found {
		t.Fatalf("corresponding reason info not found")
	}
	if !found {
		return
	}
	reason := result.Error
	message := result.Message
	if actualReason.Err != reason || actualReason.Message != message {
		t.Errorf("expected %v %q, got %v %q", reason, message, actualReason.Err, actualReason.Message)
	}
}
