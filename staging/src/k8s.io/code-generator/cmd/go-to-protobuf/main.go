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

// // go-to-protobuf 从 Go 结构体生成 Protobuf IDL，并保留结构体上已有的 IDL 标签。
// existing IDL tags on the Go struct.
package main

import (
	goflag "flag"

	flag "github.com/spf13/pflag"
	"k8s.io/code-generator/cmd/go-to-protobuf/protobuf"
	"k8s.io/klog/v2"
)

var g = protobuf.New()

// init 在 pflag 上注册参数，并把 klog 的标准库 flag 合并进来，
// 使 --v 等日志参数与生成器参数在同一命令行解析。
func init() {
	klog.InitFlags(nil)
	g.BindFlags(flag.CommandLine)
	goflag.Set("logtostderr", "true")
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
}

// main 解析参数后交由 protobuf.Run 执行生成流程。
func main() {
	flag.Parse()
	protobuf.Run(g)
}
