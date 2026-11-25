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

package apps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2estatefulset "k8s.io/kubernetes/test/e2e/framework/statefulset"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	"k8s.io/kubernetes/test/e2e/upgrades"
)

const cassandraManifestPath = "test/e2e/testing-manifests/statefulset/cassandra"

// CassandraUpgradeTest 启动 Cassandra StatefulSet，并验证其在升级过程中表现正常。
type CassandraUpgradeTest struct {
	ip               string
	successfulWrites int
}

// Name 返回该测试用于跟踪的名称。
func (CassandraUpgradeTest) Name() string { return "cassandra-upgrade" }

// Skip 在该测试可以跳过时返回 true。
func (CassandraUpgradeTest) Skip(upgCtx upgrades.UpgradeContext) bool {
	minVersion := version.MustParseSemantic("1.6.0")
	for _, vCtx := range upgCtx.Versions {
		if vCtx.Version.LessThan(minVersion) {
			return true
		}
	}
	return false
}

func cassandraKubectlCreate(ns, file string) {
	data, err := e2etestfiles.Read(filepath.Join(cassandraManifestPath, file))
	if err != nil {
		framework.Fail(err.Error())
	}
	input := string(data)
	e2ekubectl.RunKubectlOrDieInput(ns, input, "create", "-f", "-")
}

// Setup 创建 Cassandra StatefulSet 和 PDB，同时启动测试用 ReplicaSet、
// 关联的 Service 和 PDB，以保证升级期间的可用性。
// 它会等待系统稳定，然后添加两个用户以验证连通性。
func (t *CassandraUpgradeTest) Setup(ctx context.Context, f *framework.Framework) {
	ns := f.Namespace.Name
	statefulsetPoll := 30 * time.Second
	statefulsetTimeout := 10 * time.Minute

	ginkgo.By("Creating a PDB")
	cassandraKubectlCreate(ns, "pdb.yaml")

	ginkgo.By("Creating a Cassandra StatefulSet")
	e2estatefulset.CreateStatefulSet(ctx, f.ClientSet, cassandraManifestPath, ns)

	ginkgo.By("Creating a cassandra-test-server deployment")
	cassandraKubectlCreate(ns, "tester.yaml")

	ginkgo.By("Getting the ingress IPs from the services")
	err := wait.PollUntilContextTimeout(ctx, statefulsetPoll, statefulsetTimeout, true, func(ctx context.Context) (bool, error) {
		if t.ip = t.getServiceIP(ctx, f, ns, "test-server"); t.ip == "" {
			return false, nil
		}
		if _, err := t.listUsers(); err != nil {
			framework.Logf("Service endpoint is up but isn't responding")
			return false, nil
		}
		return true, nil
	})
	framework.ExpectNoError(err)
	framework.Logf("Service endpoint is up")

	ginkgo.By("Adding 2 dummy users")
	err = t.addUser("Alice")
	framework.ExpectNoError(err)
	err = t.addUser("Bob")
	framework.ExpectNoError(err)
	t.successfulWrites = 2

	ginkgo.By("Verifying that the users exist")
	users, err := t.listUsers()
	framework.ExpectNoError(err)
	gomega.Expect(users).To(gomega.HaveLen(2))
}

// listUsers 通过 tester Service 从数据库获取用户列表。
func (t *CassandraUpgradeTest) listUsers() ([]string, error) {
	r, err := http.Get(fmt.Sprintf("http://%s/list", net.JoinHostPort(t.ip, "8080")))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(b))
	}
	var names []string
	if err := json.NewDecoder(r.Body).Decode(&names); err != nil {
		return nil, err
	}
	return names, nil

}

// addUser 通过 tester Service 向数据库添加用户。
func (t *CassandraUpgradeTest) addUser(name string) error {
	val := map[string][]string{"name": {name}}
	r, err := http.PostForm(fmt.Sprintf("http://%s/add", net.JoinHostPort(t.ip, "8080")), val)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		return errors.New(string(b))
	}
	return nil
}

// getServiceIP 是从 Service 中提取 Ingress IP 的辅助方法。
func (t *CassandraUpgradeTest) getServiceIP(ctx context.Context, f *framework.Framework, ns, svcName string) string {
	svc, err := f.ClientSet.CoreV1().Services(ns).Get(ctx, svcName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	ingress := svc.Status.LoadBalancer.Ingress
	if len(ingress) == 0 {
		return ""
	}
	return ingress[0].IP
}

// Test 在升级期间调用。
// 它启动两个 goroutine：一个持续向数据库写入，另一个持续从数据库读取。
// 每次尝试都会计数，最后验证成功率是否超过阈值（0.75），
// 同时验证读回的记录数不少于成功写入的数量。
func (t *CassandraUpgradeTest) Test(ctx context.Context, f *framework.Framework, done <-chan struct{}, upgrade upgrades.UpgradeType) {
	ginkgo.By("Continuously polling the database during upgrade.")
	var (
		success, failures, writeAttempts, lastUserCount int
		mu                                              sync.Mutex
		errors                                          = map[string]int{}
	)
	// 写入循环。
	go wait.Until(func() {
		writeAttempts++
		if err := t.addUser(fmt.Sprintf("user-%d", writeAttempts)); err != nil {
			framework.Logf("Unable to add user: %v", err)
			mu.Lock()
			errors[err.Error()]++
			mu.Unlock()
			return
		}
		t.successfulWrites++
	}, 10*time.Millisecond, done)
	// 读取循环。
	wait.Until(func() {
		users, err := t.listUsers()
		if err != nil {
			framework.Logf("Could not retrieve users: %v", err)
			failures++
			mu.Lock()
			errors[err.Error()]++
			mu.Unlock()
			return
		}
		success++
		lastUserCount = len(users)
	}, 10*time.Millisecond, done)
	framework.Logf("got %d users; want >=%d", lastUserCount, t.successfulWrites)
	gomega.Expect(lastUserCount).To(gomega.BeNumerically(">=", t.successfulWrites), "lastUserCount is too small")
	ratio := float64(success) / float64(success+failures)
	framework.Logf("Successful gets %d/%d=%v", success, success+failures, ratio)
	ratio = float64(t.successfulWrites) / float64(writeAttempts)
	framework.Logf("Successful writes %d/%d=%v", t.successfulWrites, writeAttempts, ratio)
	framework.Logf("Errors: %v", errors)
	// TODO(maisem): 积累几次测试运行后再调整该值。
	gomega.Expect(ratio).To(gomega.BeNumerically(">", 0.75), "ratio too small")
}

// Teardown 对数据可用性做最后一次检查。
func (t *CassandraUpgradeTest) Teardown(ctx context.Context, f *framework.Framework) {
	users, err := t.listUsers()
	framework.ExpectNoError(err)
	gomega.Expect(len(users)).To(gomega.BeNumerically(">=", t.successfulWrites), "len(users) is too small")
}
