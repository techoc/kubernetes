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

package certificates

import (
	"context"
	"testing"
	"time"

	certificates "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
)

// TestCertificateController 验证证书控制器能把 informer 队列中的 CSR 交给处理函数，
// 并确实向 API 发出 approval 子资源的更新请求。
// TODO: 补充更多场景：缓存中找不到 CSR、未自动批准等。
//
// 处理函数会为 CSR 追加 Approved 条件，这里借此断言控制器确实调用了它，
// 且请求落在 approval 子资源上而不是普通更新。
func TestCertificateController(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	csr := &certificates.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-csr",
		},
	}

	client := fake.NewSimpleClientset(csr)
	informerFactory := informers.NewSharedInformerFactory(fake.NewSimpleClientset(csr), controller.NoResyncPeriodFunc())
	handler := func(ctx context.Context, csr *certificates.CertificateSigningRequest) error {
		csr.Status.Conditions = append(csr.Status.Conditions, certificates.CertificateSigningRequestCondition{
			Type:    certificates.CertificateApproved,
			Reason:  "test reason",
			Message: "test message",
		})
		_, err := client.CertificatesV1().CertificateSigningRequests().UpdateApproval(context.TODO(), csr.Name, csr, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		return nil
	}

	// 用 stub 代替同步检查：测试只运行一次 processNextWorkItem，
	// 无需等待真实 informer 缓存同步完成。
	controller := NewCertificateController(
		ctx,
		"test",
		client,
		informerFactory.Certificates().V1().CertificateSigningRequests(),
		handler,
	)
	controller.csrsSynced = func() bool { return true }

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)
	// 轮询等待 informer 的 Add 事件入队，避免依赖不可预期的调度时序。
	wait.PollUntil(10*time.Millisecond, func() (bool, error) {
		return controller.queue.Len() >= 1, nil
	}, stopCh)
	// 直接处理一个工作项，而非启动完整 worker，便于精确断言产生的 API 调用。
	controller.processNextWorkItem(ctx)

	actions := client.Actions()
	// 断言只有一次 API 调用：额外调用意味着处理函数被重复执行或走了错误路径。
	if len(actions) != 1 {
		t.Errorf("expected 1 actions")
	}
	// 批准状态必须通过 approval 子资源更新，普通更新不会改变 CSR 的批准结果。
	if a := actions[0]; !a.Matches("update", "certificatesigningrequests") ||
		a.GetSubresource() != "approval" {
		t.Errorf("unexpected action: %#v", a)
	}

}
