/*
Copyright 2023 The Kubernetes Authors.

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

// Package common 定义文件系统配额的通用标识，以及各配额机制的公共接口。
package common

// QuotaID 是配额的通用标识符。
// 数据类型取自 quotactl(2)，以便与内核的配额语义保持一致。
type QuotaID int32

const (
	// UnknownQuotaID 表示无法确定配额是否生效。
	UnknownQuotaID QuotaID = -1
	// BadQuotaID 表示无效配额。
	BadQuotaID QuotaID = 0
)
