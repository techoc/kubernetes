/*
Copyright 2018 The Kubernetes Authors.

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

package ipamperf

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam"
	controllerutil "k8s.io/kubernetes/pkg/controller/util/node"
)

// Config 表示本次测试使用的配置。
type Config struct {
	CreateQPS     int                     // 节点创建速率
	KubeQPS       int                     // 与 kubernetes API 通信的速率
	CloudQPS      int                     // 与 cloud endpoint 通信的速率
	NumNodes      int                     // 要创建并监控的节点数
	AllocatorType ipam.CIDRAllocatorType  // 使用的 allocator 类型
	Cloud         cloudprovider.Interface // cloud provider
}

type nodeTime struct {
	added     time.Time // 观察到节点加入的时间
	allocated time.Time // 观察到节点分配 podCIDR 的时间
	podCIDR   string    // 分配到的 podCIDR 范围
}

// Observer 是测试观察者的句柄，监控节点变化并记录时序。
type Observer struct {
	numAdded     int                  // 观察到加入的节点数
	numAllocated int                  // 观察到已分配 podCIDR 的节点数
	timing       map[string]*nodeTime // 每个节点的时序数据
	numNodes     int                  // 期望的节点总数
	stopChan     chan struct{}        // 用于关闭 shared informer
	wg           sync.WaitGroup
	clientSet    *clientset.Clientset
}

// JSONDuration 是 time.Duration 的别名，用于支持自定义的 Marshal 逻辑。
type JSONDuration time.Duration

// NodeDuration 记录单个节点的 CIDR 分配耗时。
type NodeDuration struct {
	Name     string       // 节点名
	PodCIDR  string       // 分配给该节点的 podCIDR
	Duration JSONDuration // 分配 podCIDR 耗时
}

// Results 表示观察到的测试结果。
type Results struct {
	Name           string         // 测试名
	Config         *Config        // 测试配置
	Succeeded      bool           // 是否所有节点都分配到了 podCIDR
	MaxAllocTime   JSONDuration   // 单节点分配的最大耗时
	TotalAllocTime JSONDuration   // 从首次加入到最后一次分配的时长
	NodeAllocTime  []NodeDuration // 按节点名记录的分配耗时
}

// NewObserver 根据给定的 Clientset 创建一个观察者。
func NewObserver(clientSet *clientset.Clientset, numNodes int) *Observer {
	o := &Observer{
		timing:    map[string]*nodeTime{},
		numNodes:  numNodes,
		clientSet: clientSet,
		stopChan:  make(chan struct{}),
	}
	return o
}

// StartObserving 启动异步循环监控节点变化。
// 启动后调用 Results() 获取测试结果。
func (o *Observer) StartObserving() error {
	o.monitor()
	klog.Infof("Test observer started")
	return nil
}

// Results 等待观察者完成并返回汇总后的测试结果。
func (o *Observer) Results(name string, config *Config) *Results {
	var (
		firstAdd       time.Time // 最早的节点加入时间
		lastAssignment time.Time // 最晚的节点 CIDR 分配时间
	)
	o.wg.Wait()
	close(o.stopChan) // 关闭 shared informer

	results := &Results{
		Name:          name,
		Config:        config,
		Succeeded:     o.numAdded == o.numNodes && o.numAllocated == o.numNodes,
		MaxAllocTime:  0,
		NodeAllocTime: []NodeDuration{},
	}
	for name, nTime := range o.timing {
		addFound := !nTime.added.IsZero()
		if addFound && (firstAdd.IsZero() || nTime.added.Before(firstAdd)) {
			firstAdd = nTime.added
		}
		cidrFound := !nTime.allocated.IsZero()
		if cidrFound && nTime.allocated.After(lastAssignment) {
			lastAssignment = nTime.allocated
		}
		if addFound && cidrFound {
			allocTime := nTime.allocated.Sub(nTime.added)
			if allocTime > time.Duration(results.MaxAllocTime) {
				results.MaxAllocTime = JSONDuration(allocTime)
			}
			results.NodeAllocTime = append(results.NodeAllocTime, NodeDuration{
				Name: name, PodCIDR: nTime.podCIDR, Duration: JSONDuration(allocTime),
			})
		}
	}
	results.TotalAllocTime = JSONDuration(lastAssignment.Sub(firstAdd))
	sort.Slice(results.NodeAllocTime, func(i, j int) bool {
		return results.NodeAllocTime[i].Duration > results.NodeAllocTime[j].Duration
	})
	return results
}

func (o *Observer) monitor() {
	o.wg.Add(1)

	sharedInformer := informers.NewSharedInformerFactory(o.clientSet, 1*time.Second)
	nodeInformer := sharedInformer.Core().V1().Nodes().Informer()

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controllerutil.CreateAddNodeHandler(func(node *v1.Node) (err error) {
			name := node.GetName()
			if node.Spec.PodCIDR != "" {
				// 忽略已有 PodCIDR 的节点（可能是上次运行残留未清理的数据）。
				return
			}
			nTime := &nodeTime{}
			o.timing[name] = nTime
			nTime.added = time.Now()
			o.numAdded = o.numAdded + 1
			return
		}),
		UpdateFunc: controllerutil.CreateUpdateNodeHandler(func(oldNode, newNode *v1.Node) (err error) {
			name := newNode.GetName()
			nTime, found := o.timing[name]
			if !found {
				return // 一致性检查：忽略未观察到 add 事件的节点
			}
			// 只在首次分配 CIDR 时记录，忽略重复更新。
			if newNode.Spec.PodCIDR != "" && nTime.podCIDR == "" {
				nTime.allocated = time.Now()
				nTime.podCIDR = newNode.Spec.PodCIDR
				o.numAllocated++
				if o.numAllocated%10 == 0 {
					klog.Infof("progress: %d/%d - %.2d%%", o.numAllocated, o.numNodes, (o.numAllocated * 100.0 / o.numNodes))
				}
				// 仅在 numAllocated 变化时做此检查，否则重复更新会多次调用 wg.Done() 引发 panic。
				if o.numAdded == o.numNodes && o.numAllocated == o.numNodes {
					klog.Info("All nodes assigned podCIDR")
					o.wg.Done()
				}
			}
			return
		}),
	})
	sharedInformer.Start(o.stopChan)
}

// String 实现 Stringer 接口，以多行形式返回测试结果。
func (results *Results) String() string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "\n  TestName: %s", results.Name)
	fmt.Fprintf(&b, "\n  NumNodes: %d, CreateQPS: %d, KubeQPS: %d, CloudQPS: %d, Allocator: %v",
		results.Config.NumNodes, results.Config.CreateQPS, results.Config.KubeQPS,
		results.Config.CloudQPS, results.Config.AllocatorType)
	fmt.Fprintf(&b, "\n  Succeeded: %v, TotalAllocTime: %v, MaxAllocTime: %v",
		results.Succeeded, time.Duration(results.TotalAllocTime), time.Duration(results.MaxAllocTime))
	fmt.Fprintf(&b, "\n  %5s %-20s %-20s %s", "Num", "Node", "PodCIDR", "Duration (s)")
	for i, d := range results.NodeAllocTime {
		fmt.Fprintf(&b, "\n  %5d %-20s %-20s %10.3f", i+1, d.Name, d.PodCIDR, time.Duration(d.Duration).Seconds())
	}
	return b.String()
}

// MarshalJSON 实现 json.Marshaler 接口。
func (jDuration *JSONDuration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", time.Duration(*jDuration).String())), nil
}

// UnmarshalJSON 实现 json.Unmarshaler 接口。
func (jDuration *JSONDuration) UnmarshalJSON(b []byte) (err error) {
	var d time.Duration
	if d, err = time.ParseDuration(string(b[1 : len(b)-1])); err == nil {
		*jDuration = JSONDuration(d)
	}
	return
}
