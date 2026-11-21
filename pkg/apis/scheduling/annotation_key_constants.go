/*
Copyright The Kubernetes Authors.

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

package scheduling

const (
	// GroupTemplateNameAnnotation 把子资源映射到父 Workload 中定义的
	// PodGroupTemplate 或 CompositePodGroupTemplate。
	// 例如在 TrainJob -> JobSet -> Job 这样的层级中，该注解告诉子 Job
	// 应使用根 Workload 的哪个模板来创建自己的 PodGroup。
	GroupTemplateNameAnnotation = GroupName + "/group-template-name"

	// ParentCompositePodGroupAnnotation 指定子对象的 PodGroup 必须挂载到的
	// CompositePodGroup 运行实例的精确名称。
	//
	// 该注解仅在“生命周期委派”模型下才需要：父对象（如 JobSet）创建
	// Workload 和 CompositePodGroup，而子对象（如 Job）自行创建 PodGroup。
	// 若由父对象统一管理 CompositePodGroup 和所有 PodGroup，则不需要此注解。
	ParentCompositePodGroupAnnotation = GroupName + "/parent-compositepodgroup"
)
