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

package bootstrap

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// newTokenCleaner 搭建一个基于 fake 客户端和共享 informer 的 TokenCleaner 测试环境。
//
// 返回 informer 是为了让测试直接向本地索引器塞入 Secret，这样无需启动
// informer 的 List/Watch 循环，也能精确控制 evalSecret 看到的缓存内容。
func newTokenCleaner() (*TokenCleaner, *fake.Clientset, coreinformers.SecretInformer, error) {
	options := DefaultTokenCleanerOptions()
	cl := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(cl, options.SecretResync)
	secrets := informerFactory.Core().V1().Secrets()
	tcc, err := NewTokenCleaner(cl, secrets, options)
	if err != nil {
		return nil, nil, nil, err
	}
	return tcc, cl, secrets, nil
}

// TestCleanerNoExpiration 验证没有过期时间的引导令牌 Secret 会被保留。
//
// 没有 expiration 注解意味着令牌永不过期，evalSecret 不应发出任何删除动作；
// 用空 Action 列表断言，确保控制器不会“顺手”清理长期有效的令牌。
func TestCleanerNoExpiration(t *testing.T) {
	cleaner, cl, secrets, err := newTokenCleaner()
	if err != nil {
		t.Fatalf("error creating TokenCleaner: %v", err)
	}

	secret := newTokenSecret("tokenID", "tokenSecret")
	secrets.Informer().GetIndexer().Add(secret)

	cleaner.evalSecret(context.TODO(), secret)

	expected := []core.Action{}

	verifyActions(t, expected, cl.Actions())
}

// TestCleanerExpired 验证已过期的令牌 Secret 会被删除。
//
// 将过期时间设在一小时前，断言产生一个带 UID 前置条件的删除动作：
// UID 前置条件能防止误删同名的新 Secret，是安全清理的关键语义。
func TestCleanerExpired(t *testing.T) {
	cleaner, cl, secrets, err := newTokenCleaner()
	if err != nil {
		t.Fatalf("error creating TokenCleaner: %v", err)
	}

	secret := newTokenSecret("tokenID", "tokenSecret")
	addSecretExpiration(secret, timeString(-time.Hour))
	secrets.Informer().GetIndexer().Add(secret)

	cleaner.evalSecret(context.TODO(), secret)

	expected := []core.Action{
		core.NewDeleteActionWithOptions(
			schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
			api.NamespaceSystem,
			secret.ObjectMeta.Name,
			metav1.DeleteOptions{
				Preconditions: metav1.NewUIDPreconditions(string(secret.UID)),
			}),
	}

	verifyActions(t, expected, cl.Actions())
}

// TestCleanerNotExpired 验证尚未到期的令牌 Secret 不会被删除。
//
// 过期时间设在一小时后，与 TestCleanerExpired 形成对照，确认判断逻辑
// 依据的是时间与阈值的比较结果，而不是 Secret 上是否存在过期注解。
func TestCleanerNotExpired(t *testing.T) {
	cleaner, cl, secrets, err := newTokenCleaner()
	if err != nil {
		t.Fatalf("error creating TokenCleaner: %v", err)
	}

	secret := newTokenSecret("tokenID", "tokenSecret")
	addSecretExpiration(secret, timeString(time.Hour))
	secrets.Informer().GetIndexer().Add(secret)

	cleaner.evalSecret(context.TODO(), secret)

	expected := []core.Action{}

	verifyActions(t, expected, cl.Actions())
}

// TestCleanerExpiredAt 验证“即将过期”的 Secret 会先被重新入队，到期后才真正删除。
//
// 把过期时间设为两秒后，第一次处理时仍未到期，因此队列应被清空（没有额外动作）；
// 控制器靠定时把未到期的 Secret 放回队列来等待时间流逝，所以测试轮询队列长度，
// 直到它再次出现，再执行处理，断言此时才产生删除动作。
func TestCleanerExpiredAt(t *testing.T) {
	cleaner, cl, secrets, err := newTokenCleaner()
	if err != nil {
		t.Fatalf("error creating TokenCleaner: %v", err)
	}

	secret := newTokenSecret("tokenID", "tokenSecret")
	addSecretExpiration(secret, timeString(2*time.Second))
	secrets.Informer().GetIndexer().Add(secret)
	cleaner.enqueueSecrets(secret)
	expected := []core.Action{}
	// verifyFunc 处理一个工作项并立即核对当前动作列表，方便在“未到期”和“到期”两个阶段复用同一断言。
	verifyFunc := func() {
		cleaner.processNextWorkItem(context.TODO())
		verifyActions(t, expected, cl.Actions())
	}
	// 此刻令牌尚未过期，不应产生删除动作。
	verifyFunc()

	if cleaner.queue.Len() != 0 {
		t.Errorf("not using the queue, the length should be 0, now: %v", cleaner.queue.Len())
	}

	var conditionFunc = func() (bool, error) {
		if cleaner.queue.Len() == 1 {
			return true, nil
		}
		return false, nil
	}

	err = wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, conditionFunc)
	if err != nil {
		t.Fatalf("secret is put back into the queue, the queue length should be 1, error: %v\n", err)
	}

	// 时间流逝后令牌最终过期，此时处理工作项应发出删除动作。
	expected = []core.Action{
		core.NewDeleteActionWithOptions(
			schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
			api.NamespaceSystem,
			secret.ObjectMeta.Name,
			metav1.DeleteOptions{
				Preconditions: metav1.NewUIDPreconditions(string(secret.UID)),
			}),
	}
	verifyFunc()
}
