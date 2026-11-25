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

package debug

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	clientset "k8s.io/client-go/kubernetes"

	"k8s.io/kubernetes/test/e2e/framework"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

const (
	// 轮询组件日志大小的最小间隔
	pollingPeriod            = 60 * time.Second
	workersNo                = 5
	kubeletLogsPath          = "/var/log/kubelet.log"
	kubeProxyLogsPath        = "/var/log/kube-proxy.log"
	kubeAddonsLogsPath       = "/var/log/kube-addons.log"
	kubeMasterAddonsLogsPath = "/var/log/kube-master-addons.log"
	apiServerLogsPath        = "/var/log/kube-apiserver.log"
	controllersLogsPath      = "/var/log/kube-controller-manager.log"
	schedulerLogsPath        = "/var/log/kube-scheduler.log"
)

var (
	nodeLogsToCheck   = []string{kubeletLogsPath, kubeProxyLogsPath}
	masterLogsToCheck = []string{kubeletLogsPath, kubeAddonsLogsPath, kubeMasterAddonsLogsPath,
		apiServerLogsPath, controllersLogsPath, schedulerLogsPath}
)

// TimestampedSize 保存一个大小及其测量时间。
type TimestampedSize struct {
	timestamp time.Time
	size      int
}

// LogSizeGatherer 是 worker，从 channel 取出 WorkItem 并执行相应的采集工作。
type LogSizeGatherer struct {
	stopChannel chan bool
	data        *LogsSizeData
	wg          *sync.WaitGroup
	workChannel chan WorkItem
}

// LogsSizeVerifier 从 master 和 node 机器上采集日志文件大小数据，
// 管理 <workersNo> 个执行采集的 worker。
type LogsSizeVerifier struct {
	client      clientset.Interface
	stopChannel chan bool
	// data 按 IP 和 log_path 分组存放 LogSizeData
	data          *LogsSizeData
	masterAddress string
	nodeAddresses []string
	wg            sync.WaitGroup
	workChannel   chan WorkItem
	workers       []*LogSizeGatherer
}

// SingleLogSummary 保存平均生成速率和探测次数。
type SingleLogSummary struct {
	AverageGenerationRate int
	NumberOfProbes        int
}

// LogSizeDataTimeseries 是按时间戳记录大小的 map。
type LogSizeDataTimeseries map[string]map[string][]TimestampedSize

// LogsSizeDataSummary 是日志摘要的 map。
// node -> file -> data
type LogsSizeDataSummary map[string]map[string]SingleLogSummary

// PrintHumanReadable 返回易读格式的日志大小数据摘要。
// TODO: 确认这里是否不需要加锁
func (s *LogsSizeDataSummary) PrintHumanReadable() string {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 1, 0, 1, ' ', 0)
	fmt.Fprintf(w, "host\tlog_file\taverage_rate (B/s)\tnumber_of_probes\n")
	for k, v := range *s {
		fmt.Fprintf(w, "%v\t\t\t\n", k)
		for path, data := range v {
			fmt.Fprintf(w, "\t%v\t%v\t%v\n", path, data.AverageGenerationRate, data.NumberOfProbes)
		}
	}
	w.Flush()
	return buf.String()
}

// PrintJSON 以 JSON 格式返回日志大小数据摘要。
func (s *LogsSizeDataSummary) PrintJSON() string {
	return framework.PrettyPrintJSON(*s)
}

// SummaryKind 返回日志大小数据摘要的类别名。
func (s *LogsSizeDataSummary) SummaryKind() string {
	return "LogSizeSummary"
}

// LogsSizeData 保存日志大小的时间序列数据及相应的锁。
type LogsSizeData struct {
	data LogSizeDataTimeseries
	lock sync.Mutex
}

// WorkItem 是发给 worker 的任务，包含要采集数据的机器 IP 和所有关心的文件路径。
type WorkItem struct {
	ip                string
	paths             []string
	backoffMultiplier int
}

func prepareData(masterAddress string, nodeAddresses []string) *LogsSizeData {
	data := make(LogSizeDataTimeseries)
	ips := append(nodeAddresses, masterAddress)
	for _, ip := range ips {
		data[ip] = make(map[string][]TimestampedSize)
	}
	return &LogsSizeData{
		data: data,
		lock: sync.Mutex{},
	}
}

func (d *LogsSizeData) addNewData(ip, path string, timestamp time.Time, size int) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.data[ip][path] = append(
		d.data[ip][path],
		TimestampedSize{
			timestamp: timestamp,
			size:      size,
		},
	)
}

// NewLogsVerifier 创建 LogsSizeVerifier，stopChannel 关闭时它会停止。
func NewLogsVerifier(ctx context.Context, c clientset.Interface) *LogsSizeVerifier {
	nodeAddresses, err := e2essh.NodeSSHHosts(ctx, c)
	framework.ExpectNoError(err)
	instanceAddress := framework.APIAddress() + ":22"

	workChannel := make(chan WorkItem, len(nodeAddresses)+1)
	workers := make([]*LogSizeGatherer, workersNo)

	verifier := &LogsSizeVerifier{
		client:        c,
		data:          prepareData(instanceAddress, nodeAddresses),
		masterAddress: instanceAddress,
		nodeAddresses: nodeAddresses,
		wg:            sync.WaitGroup{},
		workChannel:   workChannel,
		workers:       workers,
	}
	verifier.wg.Add(workersNo)
	for i := range workersNo {
		workers[i] = &LogSizeGatherer{
			data:        verifier.data,
			wg:          &verifier.wg,
			workChannel: workChannel,
		}
	}
	return verifier
}

// GetSummary 返回 LogSizeVerifier 采集数据的摘要（平均生成速率和探测次数）。
func (s *LogsSizeVerifier) GetSummary() *LogsSizeDataSummary {
	result := make(LogsSizeDataSummary)
	for k, v := range s.data.data {
		result[k] = make(map[string]SingleLogSummary)
		for path, data := range v {
			if len(data) > 1 {
				last := data[len(data)-1]
				first := data[0]
				rate := (last.size - first.size) / int(last.timestamp.Sub(first.timestamp)/time.Second)
				result[k][path] = SingleLogSummary{
					AverageGenerationRate: rate,
					NumberOfProbes:        len(data),
				}
			}
		}
	}
	return &result
}

// Run 开始采集日志大小：为每个 worker 启动一个 goroutine，
// 然后阻塞直到 stopChannel 关闭。
func (s *LogsSizeVerifier) Run(ctx context.Context) {
	s.workChannel <- WorkItem{
		ip:                s.masterAddress,
		paths:             masterLogsToCheck,
		backoffMultiplier: 1,
	}
	for _, node := range s.nodeAddresses {
		s.workChannel <- WorkItem{
			ip:                node,
			paths:             nodeLogsToCheck,
			backoffMultiplier: 1,
		}
	}
	for _, worker := range s.workers {
		go worker.Run(ctx)
	}
	<-s.stopChannel
	s.wg.Wait()
}

// Run 开始采集日志大小。
func (g *LogSizeGatherer) Run(ctx context.Context) {
	for g.Work(ctx) {
	}
}

func (g *LogSizeGatherer) pushWorkItem(workItem WorkItem) {
	select {
	case <-time.After(time.Duration(workItem.backoffMultiplier) * pollingPeriod):
		g.workChannel <- workItem
	case <-g.stopChannel:
		return
	}
}

// Work 执行一轮工作：从队列中取出 WorkItem，SSH 到目标机器采集数据，
// 写入共享的 <data> map，并启动 goroutine 在 <pollingPeriod> 延迟后
// 把任务重新放回队列。worker 应退出时返回 false。
func (g *LogSizeGatherer) Work(ctx context.Context) bool {
	var workItem WorkItem
	select {
	case <-g.stopChannel:
		g.wg.Done()
		return false
	case workItem = <-g.workChannel:
	}
	sshResult, err := e2essh.SSH(
		ctx,
		fmt.Sprintf("ls -l %v | awk '{print $9, $5}' | tr '\n' ' '", strings.Join(workItem.paths, " ")),
		workItem.ip,
		framework.TestContext.Provider,
	)
	if err != nil {
		framework.Logf("Error while trying to SSH to %v, skipping probe. Error: %v", workItem.ip, err)
		// 连续失败就放弃。
		if workItem.backoffMultiplier >= 128 {
			framework.Logf("Failed to ssh to a node %v multiple times in a row. Giving up.", workItem.ip)
			g.wg.Done()
			return false
		}
		workItem.backoffMultiplier *= 2
		go g.pushWorkItem(workItem)
		return true
	}
	workItem.backoffMultiplier = 1
	results := strings.Split(sshResult.Stdout, " ")

	now := time.Now()
	for i := 0; i+1 < len(results); i = i + 2 {
		path := results[i]
		size, err := strconv.Atoi(results[i+1])
		if err != nil {
			framework.Logf("Error during conversion to int: %v, skipping data. Error: %v", results[i+1], err)
			continue
		}
		g.data.addNewData(workItem.ip, path, now, size)
	}
	go g.pushWorkItem(workItem)
	return true
}
