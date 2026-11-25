/*
Copyright 2024 The Kubernetes Authors.

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

package leaderelection

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
)

// testcase 汇总 LeaseCandidate 测试所需的标识与版本信息。
type testcase struct {
	candidateName, candidateNamespace, leaseName string
	binaryVersion, emulationVersion              string
}

// TestLeaseCandidateCreation 校验 candidate 启动后会创建 LeaseCandidate 并写入版本字段。
func TestLeaseCandidateCreation(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	tc := testcase{
		candidateName:      "foo",
		candidateNamespace: "default",
		leaseName:          "lease",
		binaryVersion:      "1.30.0",
		emulationVersion:   "1.30.0",
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	client := fake.NewSimpleClientset()
	candidate, _, err := NewCandidate(
		client,
		tc.candidateNamespace,
		tc.candidateName,
		tc.leaseName,
		tc.binaryVersion,
		tc.emulationVersion,
		v1.OldestEmulationVersion,
	)
	if err != nil {
		t.Fatal(err)
	}

	go candidate.Run(ctx)
	err = pollForLease(ctx, tc, client, nil)
	if err != nil {
		t.Fatal(err)
	}
}

// TestLeaseCandidateAck 校验收到 PingTime（服务端要求续约）后会刷新 RenewTime。
func TestLeaseCandidateAck(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	tc := testcase{
		candidateName:      "foo",
		candidateNamespace: "default",
		leaseName:          "lease",
		binaryVersion:      "1.30.0",
		emulationVersion:   "1.30.0",
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	client := fake.NewSimpleClientset()

	candidate, _, err := NewCandidate(
		client,
		tc.candidateNamespace,
		tc.candidateName,
		tc.leaseName,
		tc.binaryVersion,
		tc.emulationVersion,
		v1.OldestEmulationVersion,
	)
	if err != nil {
		t.Fatal(err)
	}

	go candidate.Run(ctx)
	err = pollForLease(ctx, tc, client, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 更新 PingTime 并校验客户端会续约
	ensureAfter := &metav1.MicroTime{Time: time.Now()}
	lc, err := client.CoordinationV1beta1().LeaseCandidates(tc.candidateNamespace).Get(ctx, tc.candidateName, metav1.GetOptions{})
	if err == nil {
		if lc.Spec.PingTime == nil {
			c := lc.DeepCopy()
			c.Spec.PingTime = &metav1.MicroTime{Time: time.Now()}
			_, err = client.CoordinationV1beta1().LeaseCandidates(tc.candidateNamespace).Update(ctx, c, metav1.UpdateOptions{})
			if err != nil {
				t.Error(err)
			}
		}
	}
	err = pollForLease(ctx, tc, client, ensureAfter)
	if err != nil {
		t.Fatal(err)
	}
}

// pollForLease 轮询直到 LeaseCandidate 出现且字段符合预期；t 非 nil 时还要求 RenewTime 晚于该时刻。
func pollForLease(ctx context.Context, tc testcase, client *fake.Clientset, t *metav1.MicroTime) error {
	return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (done bool, err error) {
		lc, err := client.CoordinationV1beta1().LeaseCandidates(tc.candidateNamespace).Get(ctx, tc.candidateName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return true, err
		}
		if lc.Spec.BinaryVersion == tc.binaryVersion &&
			lc.Spec.EmulationVersion == tc.emulationVersion &&
			lc.Spec.LeaseName == tc.leaseName &&
			lc.Spec.RenewTime != nil {
			// 确保在提供了时间的情况下，renewTime 发生在所给时间之后。
			if t != nil && t.After(lc.Spec.RenewTime.Time) {
				return false, nil
			}
			return true, nil
		}
		return false, nil
	})
}
