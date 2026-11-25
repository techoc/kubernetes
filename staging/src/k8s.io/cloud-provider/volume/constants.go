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

// Package volume 定义云提供商卷插件的共享常量。
package volume

const (
	// ProvisionedVolumeName 是外部云厂商中正在供给（provision）的卷名，
	// Kubernetes 的其余部分应当忽略它。
	//
	// 动态供给期间存在一个“占位”卷，此时真实卷尚未就绪；使用固定名字
	// 让各控制器能够识别并跳过它，避免把中间状态当成真实卷处理。
	ProvisionedVolumeName = "placeholder-for-provisioning"

	// LabelMultiZoneDelimiter 用于分隔卷的多个可用区。
	//
	// PV 的 zone 标签只能存一个字符串，多可用区用 "__" 拼接；
	// 选用双下划线是为了避免与常见可用区名（含 '-'）冲突。
	LabelMultiZoneDelimiter = "__"
)
