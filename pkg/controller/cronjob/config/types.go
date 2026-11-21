/*
Copyright 2020 The Kubernetes Authors.

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

package config

// CronJobControllerConfiguration 包含描述 CronJobControllerV2 的配置项。
type CronJobControllerConfiguration struct {
	// ConcurrentCronJobSyncs 是允许同时同步的 CronJob 对象数量。
	// 数值越大，任务响应越及时，但 CPU 和网络负载也越高。
	ConcurrentCronJobSyncs int32
}
