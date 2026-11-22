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

package resource_test

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

// ExampleFormat 演示 Quantity 如何按构造时指定的 Format 选择可读后缀：
// BinarySI 用 Gi，DecimalSI 用 G，毫核用 m。
func ExampleFormat() {
	// 5Gi = 5*1024^3 字节，BinarySI 输出二进制单位。
	memorySize := resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)
	fmt.Printf("memorySize = %v\n", memorySize)

	// 5G = 5*1000^3 字节，DecimalSI 输出十进制单位。
	diskSize := resource.NewQuantity(5*1000*1000*1000, resource.DecimalSI)
	fmt.Printf("diskSize = %v\n", diskSize)

	// MilliQuantity 以毫为单位构造，5300m 即 5.3 核。
	cores := resource.NewMilliQuantity(5300, resource.DecimalSI)
	fmt.Printf("cores = %v\n", cores)

	// Output:
	// memorySize = 5Gi
	// diskSize = 5G
	// cores = 5300m
}

// ExampleMustParse 演示从字符串解析 Quantity 后如何读取内部数值和推断出的 Format。
func ExampleMustParse() {
	// BinarySI：内部 Value() 返回的是字节数。
	memorySize := resource.MustParse("5Gi")
	fmt.Printf("memorySize = %v (%v)\n", memorySize.Value(), memorySize.Format)

	diskSize := resource.MustParse("5G")
	fmt.Printf("diskSize = %v (%v)\n", diskSize.Value(), diskSize.Format)

	// 带 m 后缀会直接按毫单位解析。
	cores := resource.MustParse("5300m")
	fmt.Printf("milliCores = %v (%v)\n", cores.MilliValue(), cores.Format)

	// 小数核在解析时按十进制标度归一化为毫核。
	cores2 := resource.MustParse("5.4")
	fmt.Printf("milliCores = %v (%v)\n", cores2.MilliValue(), cores2.Format)

	// Output:
	// memorySize = 5368709120 (BinarySI)
	// diskSize = 5000000000 (DecimalSI)
	// milliCores = 5300 (DecimalSI)
	// milliCores = 5400 (DecimalSI)
}
