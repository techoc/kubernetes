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

package resconsumerctrl

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"

	"github.com/spf13/cobra"

	"k8s.io/kubernetes/test/images/agnhost/dns"
	"k8s.io/kubernetes/test/images/resource-consumer/common"
)

// CmdResourceConsumerController 由 agnhost Cobra 使用。
var CmdResourceConsumerController = &cobra.Command{
	Use:   "resource-consumer-controller",
	Short: "Starts a HTTP server that spreads requests around resource consumers",
	Long:  "Starts a HTTP server that spreads requests around resource consumers. The HTTP server has the same endpoints and usage as the one spawned by the \"resource-consumer\" subcommand.",
	Args:  cobra.MaximumNArgs(0),
	Run:   main,
}

var (
	port                     int
	consumerPort             int
	consumerServiceName      string
	consumerServiceNamespace string
	dnsDomain                string
)

// getDNSDomain 遍历 DNS 配置，查找 "svc.foo" 记录，其中 "foo" 是当前配置的 DNS 后缀。
// 然后提取其中的 'foo' 部分并返回给调用方。
func getDNSDomain() string {
	if dnsDomain != "" {
		return dnsDomain
	}
	dnsSuffixList := dns.GetDNSSuffixList()
	r, _ := regexp.Compile("^svc.")
	for _, currentDNSSuffix := range dnsSuffixList {
		if r.MatchString(currentDNSSuffix) {
			// 保存 DNS 后缀，去掉 'svc.' 部分
			dnsDomain = currentDNSSuffix[4:]
			break
		}
	}
	if dnsDomain == "" {
		panic("Could not find DNS suffix starting with 'svc.' substring")
	}
	return dnsDomain
}

func init() {
	CmdResourceConsumerController.Flags().IntVar(&port, "port", 8080, "Port number.")
	CmdResourceConsumerController.Flags().IntVar(&consumerPort, "consumer-port", 8080, "Port number of consumers.")
	CmdResourceConsumerController.Flags().StringVar(&consumerServiceName, "consumer-service-name", "resource-consumer", "Name of service containing resource consumers.")
	CmdResourceConsumerController.Flags().StringVar(&consumerServiceNamespace, "consumer-service-namespace", "default", "Namespace of service containing resource consumers.")
}

func main(cmd *cobra.Command, args []string) {
	mgr := newController()
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), mgr))
}

type controller struct {
	responseWriterLock sync.Mutex
	waitGroup          sync.WaitGroup
}

func newController() *controller {
	c := &controller{}
	return c
}

func (c *controller) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, common.BadRequest, http.StatusBadRequest)
		return
	}
	// 解析 POST 请求数据和 URL 数据
	if err := req.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// 处理 consumeCPU
	if req.URL.Path == common.ConsumeCPUAddress {
		c.handleConsumeCPU(w, req.Form)
		return
	}
	// 处理 consumeMem
	if req.URL.Path == common.ConsumeMemAddress {
		c.handleConsumeMem(w, req.Form)
		return
	}
	// 处理 bumpMetric
	if req.URL.Path == common.BumpMetricAddress {
		c.handleBumpMetric(w, req.Form)
		return
	}
	http.Error(w, common.UnknownFunction, http.StatusNotFound)
}

func (c *controller) handleConsumeCPU(w http.ResponseWriter, query url.Values) {
	// 获取 consumeCPU 的字符串数据
	durationSecString := query.Get(common.DurationSecQuery)
	millicoresString := query.Get(common.MillicoresQuery)
	requestSizeInMillicoresString := query.Get(common.RequestSizeInMillicoresQuery)
	if durationSecString == "" || millicoresString == "" || requestSizeInMillicoresString == "" {
		http.Error(w, common.NotGivenFunctionArgument, http.StatusBadRequest)
		return
	}

	// 为 consumeCPU 转换数据（字符串转整数）
	durationSec, durationSecError := strconv.Atoi(durationSecString)
	millicores, millicoresError := strconv.Atoi(millicoresString)
	requestSizeInMillicores, requestSizeInMillicoresError := strconv.Atoi(requestSizeInMillicoresString)
	if durationSecError != nil || millicoresError != nil || requestSizeInMillicoresError != nil || requestSizeInMillicores <= 0 {
		http.Error(w, common.IncorrectFunctionArgument, http.StatusBadRequest)
		return
	}

	count := millicores / requestSizeInMillicores
	rest := millicores - count*requestSizeInMillicores
	fmt.Fprintf(w, "RC manager: sending %v requests to consume %v millicores each and 1 request to consume %v millicores\n", count, requestSizeInMillicores, rest)
	if count > 0 {
		c.waitGroup.Add(count)
		c.sendConsumeCPURequests(w, count, requestSizeInMillicores, durationSec)
	}
	if rest > 0 {
		c.waitGroup.Add(1)
		go c.sendOneConsumeCPURequest(w, rest, durationSec)
	}
	c.waitGroup.Wait()
}

func (c *controller) handleConsumeMem(w http.ResponseWriter, query url.Values) {
	// 获取 consumeMem 的字符串数据
	durationSecString := query.Get(common.DurationSecQuery)
	megabytesString := query.Get(common.MegabytesQuery)
	requestSizeInMegabytesString := query.Get(common.RequestSizeInMegabytesQuery)
	if durationSecString == "" || megabytesString == "" || requestSizeInMegabytesString == "" {
		http.Error(w, common.NotGivenFunctionArgument, http.StatusBadRequest)
		return
	}

	// 为 consumeMem 转换数据（字符串转整数）
	durationSec, durationSecError := strconv.Atoi(durationSecString)
	megabytes, megabytesError := strconv.Atoi(megabytesString)
	requestSizeInMegabytes, requestSizeInMegabytesError := strconv.Atoi(requestSizeInMegabytesString)
	if durationSecError != nil || megabytesError != nil || requestSizeInMegabytesError != nil || requestSizeInMegabytes <= 0 {
		http.Error(w, common.IncorrectFunctionArgument, http.StatusBadRequest)
		return
	}

	count := megabytes / requestSizeInMegabytes
	rest := megabytes - count*requestSizeInMegabytes
	fmt.Fprintf(w, "RC manager: sending %v requests to consume %v MB each and 1 request to consume %v MB\n", count, requestSizeInMegabytes, rest)
	if count > 0 {
		c.waitGroup.Add(count)
		c.sendConsumeMemRequests(w, count, requestSizeInMegabytes, durationSec)
	}
	if rest > 0 {
		c.waitGroup.Add(1)
		go c.sendOneConsumeMemRequest(w, rest, durationSec)
	}
	c.waitGroup.Wait()
}

func (c *controller) handleBumpMetric(w http.ResponseWriter, query url.Values) {
	// 获取 handleBumpMetric 的字符串数据
	metric := query.Get(common.MetricNameQuery)
	deltaString := query.Get(common.DeltaQuery)
	durationSecString := query.Get(common.DurationSecQuery)
	requestSizeCustomMetricString := query.Get(common.RequestSizeCustomMetricQuery)
	if durationSecString == "" || metric == "" || deltaString == "" || requestSizeCustomMetricString == "" {
		http.Error(w, common.NotGivenFunctionArgument, http.StatusBadRequest)
		return
	}

	// 为 handleBumpMetric 转换数据（字符串转整数/浮点数）
	durationSec, durationSecError := strconv.Atoi(durationSecString)
	delta, deltaError := strconv.Atoi(deltaString)
	requestSizeCustomMetric, requestSizeCustomMetricError := strconv.Atoi(requestSizeCustomMetricString)
	if durationSecError != nil || deltaError != nil || requestSizeCustomMetricError != nil || requestSizeCustomMetric <= 0 {
		http.Error(w, common.IncorrectFunctionArgument, http.StatusBadRequest)
		return
	}

	count := delta / requestSizeCustomMetric
	rest := delta - count*requestSizeCustomMetric
	fmt.Fprintf(w, "RC manager: sending %v requests to bump custom metric by %v each and 1 request to bump by %v\n", count, requestSizeCustomMetric, rest)
	if count > 0 {
		c.waitGroup.Add(count)
		c.sendConsumeCustomMetric(w, metric, count, requestSizeCustomMetric, durationSec)
	}
	if rest > 0 {
		c.waitGroup.Add(1)
		go c.sendOneConsumeCustomMetric(w, metric, rest, durationSec)
	}
	c.waitGroup.Wait()
}

func (c *controller) sendConsumeCPURequests(w http.ResponseWriter, requests, millicores, durationSec int) {
	for range requests {
		go c.sendOneConsumeCPURequest(w, millicores, durationSec)
	}
}

func (c *controller) sendConsumeMemRequests(w http.ResponseWriter, requests, megabytes, durationSec int) {
	for range requests {
		go c.sendOneConsumeMemRequest(w, megabytes, durationSec)
	}
}

func (c *controller) sendConsumeCustomMetric(w http.ResponseWriter, metric string, requests, delta, durationSec int) {
	for range requests {
		go c.sendOneConsumeCustomMetric(w, metric, delta, durationSec)
	}
}

func createConsumerURL(suffix string) string {
	// NOTE: 由于 Windows 平台不支持 PQDN，因此使用完整的 DNS 名称。
	return fmt.Sprintf("http://%s.%s.svc.%s:%d%s", consumerServiceName, consumerServiceNamespace, getDNSDomain(), consumerPort, suffix)
}

// sendOneConsumeCPURequest 发送用于消耗 CPU 的 POST 请求
func (c *controller) sendOneConsumeCPURequest(w http.ResponseWriter, millicores int, durationSec int) {
	defer c.waitGroup.Done()
	query := createConsumerURL(common.ConsumeCPUAddress)
	_, err := http.PostForm(query, url.Values{common.MillicoresQuery: {strconv.Itoa(millicores)}, common.DurationSecQuery: {strconv.Itoa(durationSec)}})
	c.responseWriterLock.Lock()
	defer c.responseWriterLock.Unlock()
	if err != nil {
		fmt.Fprintf(w, "Failed to connect to consumer: %v\n", err)
		return
	}
	fmt.Fprintf(w, "Consumed %d millicores\n", millicores)
}

// sendOneConsumeMemRequest 发送用于消耗内存的 POST 请求
func (c *controller) sendOneConsumeMemRequest(w http.ResponseWriter, megabytes int, durationSec int) {
	defer c.waitGroup.Done()
	query := createConsumerURL(common.ConsumeMemAddress)
	_, err := http.PostForm(query, url.Values{common.MegabytesQuery: {strconv.Itoa(megabytes)}, common.DurationSecQuery: {strconv.Itoa(durationSec)}})
	c.responseWriterLock.Lock()
	defer c.responseWriterLock.Unlock()
	if err != nil {
		fmt.Fprintf(w, "Failed to connect to consumer: %v\n", err)
		return
	}
	fmt.Fprintf(w, "Consumed %d megabytes\n", megabytes)
}

// sendOneConsumeCustomMetric 发送用于消耗自定义指标的 POST 请求
func (c *controller) sendOneConsumeCustomMetric(w http.ResponseWriter, customMetricName string, delta int, durationSec int) {
	defer c.waitGroup.Done()
	query := createConsumerURL(common.BumpMetricAddress)
	_, err := http.PostForm(query,
		url.Values{common.MetricNameQuery: {customMetricName}, common.DurationSecQuery: {strconv.Itoa(durationSec)}, common.DeltaQuery: {strconv.Itoa(delta)}})
	c.responseWriterLock.Lock()
	defer c.responseWriterLock.Unlock()
	if err != nil {
		fmt.Fprintf(w, "Failed to connect to consumer: %v\n", err)
		return
	}
	fmt.Fprintf(w, "Bumped metric %s by %d\n", customMetricName, delta)
}
