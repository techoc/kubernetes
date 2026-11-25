/*
Copyright 2015 The Kubernetes Authors.

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

package network

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	"k8s.io/kubernetes/test/e2e/network/common"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

type durations []time.Duration

func (d durations) Len() int           { return len(d) }
func (d durations) Less(i, j int) bool { return d[i] < d[j] }
func (d durations) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

var _ = common.SIGDescribe("Service endpoints latency", func() {
	f := framework.NewDefaultFramework("svc-latency")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		Release: v1.9
		Testname: Service endpoint latency, thresholds
		Description: 运行 100 次创建 Service（Pod 运行 pause 镜像），测量从创建 Service 到该 Service 名称对应的 endpoint 可用所需的时间。记录 100 次耗时后排序，计算第 50、90 和 99 百分位数。单次延迟 MUST 不超过宽松设定的阈值：第 50 百分位数 20s，第 90 百分位数 50s。
	*/
	framework.ConformanceIt("should not be very high", func(ctx context.Context) {
		const (
			// 这些阈值非常宽松，理想情况下未来应大幅收紧。参见
			// https://issues.k8s.io/10436。
			limitMedian = time.Second * 20
			limitTail   = time.Second * 50

			// 这些数值的选取是为了让测试尽快完成。样本量实际上不足以
			// 可靠测量尾部延迟（可能出现误报，但不会漏报），
			// 不过足以发现显而易见的问题。
			//
			// 注意这些都是固定值，与集群规模无关。把 parallelTrials 调大
			// 会使测量结果失真；HA 集群中或许并非如此。
			totalTrials    = 200
			parallelTrials = 15
			minSampleSize  = 100

			// 获取 Service 延迟时可接受的失败比例。
			acceptableFailureRatio = .05
		)

		// 关闭速率限制，它会干扰测量。
		cfg, err := framework.LoadConfig()
		if err != nil {
			framework.Failf("Unable to load config: %v", err)
		}
		cfg.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()
		f.ClientSet = kubernetes.NewForConfigOrDie(cfg)

		failing := sets.NewString()
		d, err := runServiceLatencies(ctx, f, parallelTrials, totalTrials, acceptableFailureRatio)
		if err != nil {
			failing.Insert(fmt.Sprintf("Not all RC/pod/service trials succeeded: %v", err))
		}
		dSorted := durations(d)
		sort.Sort(dSorted)
		n := len(dSorted)
		if n < minSampleSize {
			failing.Insert(fmt.Sprintf("Did not get a good sample size: %v", dSorted))
		}
		if n < 2 {
			failing.Insert("Less than two runs succeeded; aborting.")
			framework.Fail(strings.Join(failing.List(), "\n"))
		}
		percentile := func(p int) time.Duration {
			est := n * p / 100
			if est >= n {
				return dSorted[n-1]
			}
			return dSorted[est]
		}
		framework.Logf("Latencies: %v", dSorted)
		p50 := percentile(50)
		p90 := percentile(90)
		p99 := percentile(99)
		framework.Logf("50 %%ile: %v", p50)
		framework.Logf("90 %%ile: %v", p90)
		framework.Logf("99 %%ile: %v", p99)
		framework.Logf("Total sample count: %v", len(dSorted))

		if p50 > limitMedian {
			failing.Insert("Median latency should be less than " + limitMedian.String())
		}
		if p99 > limitTail {
			failing.Insert("Tail (99 percentile) latency should be less than " + limitTail.String())
		}
		if failing.Len() > 0 {
			errList := strings.Join(failing.List(), "\n")
			helpfulInfo := fmt.Sprintf("\n50, 90, 99 percentiles: %v %v %v", p50, p90, p99)
			framework.Fail(errList + helpfulInfo)
		}
	})
})

func runServiceLatencies(ctx context.Context, f *framework.Framework, inParallel, total int, acceptableFailureRatio float32) (output []time.Duration, err error) {
	name := "svc-latency-rc"
	deploymentConf := e2edeployment.NewDeployment(name, 1, map[string]string{"name": name}, name, imageutils.GetPauseImageName(), appsv1.RecreateDeploymentStrategyType)
	deployment, err := f.ClientSet.AppsV1().Deployments(f.Namespace.Name).Create(ctx, deploymentConf, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	err = e2edeployment.WaitForDeploymentComplete(f.ClientSet, deployment)
	framework.ExpectNoError(err)
	// 只运行一个 watcher，减少 API 调用次数，以尽量降低计时误差。
	// kube-proxy 正是这样消费 endpoint 数据的，因此这样测试更贴近实际。
	endpointQueries := newQuerier()
	startEndpointWatcher(ctx, f, endpointQueries)
	defer close(endpointQueries.stop)

	// 先跑一次并丢弃结果，确保 Pod 的 ready 状态已经传播开。
	_, err = singleServiceLatency(ctx, f, name, endpointQueries)
	framework.ExpectNoError(err)

	// 这些 channel 不会关闭，且每次尝试只会向其中一个发送数据，
	// 因此两者收到的总数恰好等于 total。
	errs := make(chan error, total)
	durations := make(chan time.Duration, total)

	blocker := make(chan struct{}, inParallel)
	for range total {
		go func() {
			defer ginkgo.GinkgoRecover()
			blocker <- struct{}{}
			defer func() { <-blocker }()
			if d, err := singleServiceLatency(ctx, f, name, endpointQueries); err != nil {
				errs <- err
			} else {
				durations <- d
			}
		}()
	}

	errCount := 0
	for range total {
		select {
		case e := <-errs:
			framework.Logf("Got error: %v", e)
			errCount++
		case d := <-durations:
			output = append(output, d)
		}
	}
	if errCount != 0 {
		framework.Logf("Got %d errors out of %d tries", errCount, total)
		errRatio := float32(errCount) / float32(total)
		if errRatio > acceptableFailureRatio {
			return output, fmt.Errorf("error ratio %g is higher than the acceptable ratio %g", errRatio, acceptableFailureRatio)
		}
	}
	return output, nil
}

type endpointQuery struct {
	serviceName string
	slice       *discoveryv1.EndpointSlice
	result      chan<- struct{}
}

type endpointQueries struct {
	requests map[string]*endpointQuery

	stop        chan struct{}
	requestChan chan *endpointQuery
	seenChan    chan *discoveryv1.EndpointSlice
}

func newQuerier() *endpointQueries {
	eq := &endpointQueries{
		requests: map[string]*endpointQuery{},

		stop:        make(chan struct{}, 100),
		requestChan: make(chan *endpointQuery),
		seenChan:    make(chan *discoveryv1.EndpointSlice, 100),
	}
	go eq.join()
	return eq
}

// join 合并进入的 request 流与新增 endpoint 流。它具有以下有用特性：
//   - 如果 endpoint 先于 request 到达，会记住该 endpoint。
//   - 停止时会关闭所有未完成的 request（返回 nil）。
//
// 注意本测试用例使用单栈 Service 且只有一个 endpoint，因此可以确定
// 它只有一个 endpoint IP，也就只有一个 EndpointSlice。
func (eq *endpointQueries) join() {
	defer func() {
		// 终止所有待处理的 request，避免 goroutine 永久阻塞。
		for _, req := range eq.requests {
			if req.result != nil {
				close(req.result)
			}
		}
	}()

	for {
		select {
		case <-eq.stop:
			return
		case req := <-eq.requestChan:
			if cur, ok := eq.requests[req.serviceName]; ok && cur.slice != nil {
				// 结果已经拿到，可以立即满足该 request。
				delete(eq.requests, req.serviceName)
				req.slice = cur.slice
				close(req.result)
			} else {
				// 保存该 request。
				eq.requests[req.serviceName] = req
			}
		case got := <-eq.seenChan:
			serviceName := got.Labels[discoveryv1.LabelServiceName]
			if req, ok := eq.requests[serviceName]; ok {
				if req.result != nil {
					// 满足一个 request。
					delete(eq.requests, serviceName)
					req.slice = got
					close(req.result)
				}
				// 已记录结果但还没收到对应 request，只保留第一个结果。
			} else {
				// 还没收到对应的 request，先保存该结果。
				eq.requests[serviceName] = &endpointQuery{
					slice: got,
				}
			}
		}
	}
}

// request 会阻塞，直到看到所请求的 endpoint。
func (eq *endpointQueries) request(serviceName string) *discoveryv1.EndpointSlice {
	result := make(chan struct{})
	req := &endpointQuery{
		serviceName: serviceName,
		result:      result,
	}
	eq.requestChan <- req
	<-result
	return req.slice
}

// added 将 e 标记为已添加；不会阻塞。
func (eq *endpointQueries) added(e *discoveryv1.EndpointSlice) {
	eq.seenChan <- e
}

// startEndpointWatcher 会阻塞，直到同步完成。
func startEndpointWatcher(ctx context.Context, f *framework.Framework, q *endpointQueries) {
	_, controller := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				obj, err := f.ClientSet.DiscoveryV1().EndpointSlices(f.Namespace.Name).List(ctx, options)
				return runtime.Object(obj), err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return f.ClientSet.DiscoveryV1().EndpointSlices(f.Namespace.Name).Watch(ctx, options)
			},
		},
		&discoveryv1.EndpointSlice{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if e, ok := obj.(*discoveryv1.EndpointSlice); ok {
					if len(e.Endpoints) > 0 {
						q.added(e)
					}
				}
			},
			UpdateFunc: func(old, cur interface{}) {
				if e, ok := cur.(*discoveryv1.EndpointSlice); ok {
					if len(e.Endpoints) > 0 {
						q.added(e)
					}
				}
			},
		},
	)

	go controller.Run(q.stop)

	// 等待 controller 完成同步，避免把预热时间计入测量。
	for !controller.HasSynced() {
		time.Sleep(100 * time.Millisecond)
	}
}

func singleServiceLatency(ctx context.Context, f *framework.Framework, name string, q *endpointQueries) (time.Duration, error) {
	// 创建一个指向该 Pod 的 Service。
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "latency-svc-",
		},
		Spec: v1.ServiceSpec{
			Ports:           []v1.ServicePort{{Protocol: v1.ProtocolTCP, Port: 80}},
			Selector:        map[string]string{"name": name},
			Type:            v1.ServiceTypeClusterIP,
			SessionAffinity: v1.ServiceAffinityNone,
		},
	}
	startTime := time.Now()
	gotSvc, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return 0, err
	}
	framework.Logf("Created: %v", gotSvc.Name)

	if e := q.request(gotSvc.Name); e == nil {
		return 0, fmt.Errorf("never got a result for endpoint %v", gotSvc.Name)
	}
	stopTime := time.Now()
	d := stopTime.Sub(startTime)
	framework.Logf("Got endpoints: %v [%v]", gotSvc.Name, d)
	return d, nil
}
