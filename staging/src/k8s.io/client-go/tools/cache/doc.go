/*
Copyright 2014 The Kubernetes Authors.

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

// Package cache 实现客户端本地缓存：Reflector 对服务端执行 watch，
// 把增量事件写入 Store，从而减少直接调用 API Server 的次数。
// 提供两类 Store：纯缓存型（如调度器查看可用节点）与带 FIFO 语义的队列型
// （如调度器按序处理新到达的 Pod）。缓存与事件处理均非事务性，调用方需
// 自行容忍短暂的陈旧数据与事件重复投递。
package cache
