/*
Copyright 2020 The Kubernetes Authors.

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

package netpol

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	netutils "k8s.io/utils/net"
)

// probeConnectivityArgs 是 probeConnectivity 的参数集合
type probeConnectivityArgs struct {
	nsFrom              string
	podFrom             string
	addrTo              string
	protocol            v1.Protocol
	toPort              int
	expectConnectivity  bool
	timeoutSeconds      int
	pollIntervalSeconds int
	pollTimeoutSeconds  int
}

// 与 k8smanager.go 解耦
type Prober interface {
	probeConnectivity(args *probeConnectivityArgs) (bool, string, error)
}

// ProbeJob 封装 pod->pod 连通性探测的输入数据
type ProbeJob struct {
	PodFrom            TestPod
	PodTo              TestPod
	PodToServiceIP     string
	ToPort             int
	ToPodDNSDomain     string
	Protocol           v1.Protocol
	ExpectConnectivity bool
}

// ProbeJobResults 封装 pod->pod 连通性探测的结果数据
type ProbeJobResults struct {
	Job         *ProbeJob
	IsConnected bool
	Err         error
	Command     string
}

// ProbePodToPodConnectivity 在 kube 中运行一系列探测，并将结果记录到 `testCase.Reachability` 中
func ProbePodToPodConnectivity(prober Prober, allPods []TestPod, dnsDomain string, testCase *TestCase) {
	size := len(allPods) * len(allPods)
	jobs := make(chan *ProbeJob, size)
	results := make(chan *ProbeJobResults, size)
	for i := 0; i < getWorkers(); i++ {
		go probeWorker(prober, jobs, results)
	}
	for _, podFrom := range allPods {
		for _, podTo := range allPods {
			// 为探测任务设置连通性预期，以便观测值与预期不符时重试探测。
			expectConnectivity := testCase.Reachability.Expected.Get(podFrom.PodString().String(), podTo.PodString().String())

			jobs <- &ProbeJob{
				PodFrom:            podFrom,
				PodTo:              podTo,
				ToPort:             testCase.ToPort,
				ToPodDNSDomain:     dnsDomain,
				Protocol:           testCase.Protocol,
				ExpectConnectivity: expectConnectivity,
			}
		}
	}
	close(jobs)

	for range size {
		result := <-results
		job := result.Job
		if result.Err != nil {
			framework.Logf("unable to perform probe %s -> %s: %v", job.PodFrom.PodString(), job.PodTo.PodString(), result.Err)
		}
		testCase.Reachability.Observe(job.PodFrom.PodString(), job.PodTo.PodString(), result.IsConnected)
		expected := testCase.Reachability.Expected.Get(job.PodFrom.PodString().String(), job.PodTo.PodString().String())
		if result.IsConnected != expected {
			framework.Logf("Validation of %s -> %s FAILED !!!", job.PodFrom.PodString(), job.PodTo.PodString())
			framework.Logf("error %v ", result.Err)
			if expected {
				framework.Logf("Expected allowed pod connection was instead BLOCKED --- run '%v'", result.Command)
			} else {
				framework.Logf("Expected blocked pod connection was instead ALLOWED --- run '%v'", result.Command)
			}
		}
	}
}

// probeWorker 持续轮询 pod 连通性状态，直到 "jobs" channel 关闭，并将结果写回 "results" channel。
// 它只向 channel 写入成功/失败状态，不会产生失败副作用；这是刻意设计的，因为我们不想在 goroutine 内触发测试失败。
func probeWorker(prober Prober, jobs <-chan *ProbeJob, results chan<- *ProbeJobResults) {
	defer ginkgo.GinkgoRecover()
	for job := range jobs {
		podFrom := job.PodFrom
		// 防御性编程：initializeClusterFromModel 中已做过检查，这里理论上不会发生
		if netutils.ParseIPSloppy(job.PodTo.ServiceIP) == nil {
			results <- &ProbeJobResults{
				Job:         job,
				IsConnected: false,
				Err:         fmt.Errorf("empty service ip"),
			}
		}
		// 注意，可以像下面这样用 dnsName 代替 ServiceIP 进行探测：
		// 后来为了支持未启用 DNS 的集群中的 netpol 测试而停用了这种做法，但将来可能会重新启用。
		// dnsName := job.PodTo.QualifiedServiceAddress(job.ToPodDNSDomain)

		// TODO: 让它在双栈集群上也能工作……
		connected, command, err := prober.probeConnectivity(&probeConnectivityArgs{
			nsFrom:              podFrom.Namespace,
			podFrom:             podFrom.Name,
			addrTo:              job.PodTo.ServiceIP,
			protocol:            job.Protocol,
			toPort:              job.ToPort,
			expectConnectivity:  job.ExpectConnectivity,
			timeoutSeconds:      getProbeTimeoutSeconds(),
			pollIntervalSeconds: getPollIntervalSeconds(),
			pollTimeoutSeconds:  getPollTimeoutSeconds(),
		})
		result := &ProbeJobResults{
			Job:         job,
			IsConnected: connected,
			Err:         err,
			Command:     command,
		}
		results <- result
	}
}
