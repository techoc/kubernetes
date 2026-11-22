/*
Copyright 2019 The Kubernetes Authors.

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

// Package clientgo 通过空白导入集中注册 client-go 运行时的指标：
// FIFO、leader election、REST client 和 workqueue。
// 组件只需导入本包，这些子系统的指标便会注册到全局 registry。
package clientgo

// 空白导入的副作用即注册过程：导入顺序不影响结果，各子包自行完成 init 注册。
import (
	_ "k8s.io/component-base/metrics/prometheus/clientgo/fifo"           // load fifo metrics
	_ "k8s.io/component-base/metrics/prometheus/clientgo/leaderelection" // load leaderelection metrics
	_ "k8s.io/component-base/metrics/prometheus/restclient"              // load restclient metrics
	_ "k8s.io/component-base/metrics/prometheus/workqueue"               // load the workqueue metrics
)
