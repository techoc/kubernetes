/*
Copyright 2021 The Kubernetes Authors.

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

/*
Package applyconfigurations 提供用于构造服务端 Apply（SSA）请求的 Go 类型。

常规 API 结构体的必填字段即使未显式赋值也会被零值序列化，SSA 会把它们当成
“本 field manager 声明置零”，可能意外清掉其他 manager 的字段。这里所有字段均为
指针，未设置的字段序列化后直接缺席，才能准确表达“不声明该字段”的语义。
各 <Kind> 构造函数与 With<Field> 方法用于安全地拼装请求；Extract<Kind> 可从
已读取的对象还原出仅含指定 field manager 所拥有字段的配置。注意这些类型由
代码生成器产出，手改无效。

# Basics

typed client（见 k8s.io/client-go/kubernetes/typed 包）中的 Apply 函数提供类型安全的
服务端 Apply 调用方式。每个 Apply 函数接受一个 "apply configuration" 类型的参数，
它是 Apply 请求的结构化表示。例如：

	import (
	     ...
	     v1ac "k8s.io/client-go/applyconfigurations/autoscaling/v1"
	)
	hpaApplyConfig := v1ac.HorizontalPodAutoscaler(autoscalerName, ns).
	     WithSpec(v1ac.HorizontalPodAutoscalerSpec().
	              WithMinReplicas(0)
	     )
	return hpav1client.Apply(ctx, hpaApplyConfig, metav1.ApplyOptions{FieldManager: "mycontroller", Force: true})

注意此例中 HorizontalPodAutoscaler 来自 "applyconfigurations" 包。每个 "apply
configuration" 类型对应与 Go 结构体相同的 Kubernetes 对象 kind，但字段全为指针，
因此可以省略未设置的字段，准确表达 Apply 请求。上例的配置序列化为 YAML 后是：

	apiVersion: autoscaling/v1
	kind: HorizontalPodAutoscaler
	metadata:
	    name: myHPA
	    namespace: myNamespace
	spec:
	    minReplicas: 0

为什么需要这些类型？因为上述 YAML 无法由 v1.HorizontalPodAutoscaler 结构体产出。
例如：

	hpa := v1.HorizontalPodAutoscaler{
	     TypeMeta: metav1.TypeMeta{
	              APIVersion: "autoscaling/v1",
	              Kind:       "HorizontalPodAutoscaler",
	     },
	     ObjectMeta: ObjectMeta{
	              Namespace: ns,
	              Name:      autoscalerName,
	     },
	     Spec: v1.HorizontalPodAutoscalerSpec{
	              MinReplicas: pointer.Int32Ptr(0),
	     },
	}

上面的代码试图声明与前例相同的配置，但序列化为 YAML 后得到的是：

	kind: HorizontalPodAutoscaler
	apiVersion: autoscaling/v1
	metadata:
	  name: myHPA
	  namespace: myNamespace
	spec:
	  scaleTargetRef:
	    kind: ""
	    name: ""
	  minReplicas: 0
	  maxReplicas: 0

注意其中 spec.maxReplicas 被置为 0。调用方本未涉及该字段，但生产环境中这可能让
autoscaler 把副本数缩到零，后果严重。根因在于 Go 结构体的必填字段未显式赋值时
会带零值：create/update 场景下没问题，但与 apply 的“只声明我拥有的字段”语义
根本冲突，因此才引入生成的 "apply configuration" 类型。

With<FieldName> 便捷方法让调用方不必手工处理指针字段："MinReplicas: &0" 不是合法
的 Go 代码，借助库方法可以写成 "MinReplicas: pointer.Int32Ptr(0)"，但 corev1.Protocol
这类字符串枚举通用库无法覆盖。更重要的是，这些方法隔离了底层表示，未来调整内部
结构以支持新特性时不会波及调用方代码。

# Controller Support

新版 client-go 通过两种机制简化了控制器中使用服务端 Apply 的方式。

Mechanism 1:

编写使用服务端 Apply 的新控制器时，推荐每次 reconcile 都重建对象的 apply
configuration，确保完整声明自己负责的所有字段。通常应在 ApplyOptions 中设置
"Force: true" 无条件接管自己拥有的字段，并为调用 Apply 的 reconcile 循环指定
唯一的 FieldManager 名称。

把现有控制器迁移到服务端 Apply 时通常也适用这一方案：改为每次 reconcile 都重建
配置。若不奏效，参见机制 2。

Mechanism 2:

迁移这类控制器有风险：若某次 apply 配置漏掉了之前请求中包含的字段，该字段会被
意外删除。此时可用机制 2 替代：把 "read/modify-in-place/update"（或 patch）流程
改成 "extract/modify-in-place/apply"——先用 Extract 取出自己拥有的字段，再原地
修改并 apply。示例如下：

	    fieldMgr := "my-field-manager"
	    deploymentClient := clientset.AppsV1().Deployments("default")
	    // 读取对象；也可以从共享 informer 获取
	    deployment, err := deploymentClient.Get(ctx, "example-deployment", metav1.GetOptions{})
	    if err != nil {
	      // handle error
	    }
	    // 提取本 field manager 拥有的字段
	    deploymentApplyConfig, err := appsv1ac.ExtractDeployment(deployment, fieldMgr)
	    if err != nil {
	      // handle error
	    }
	    // 原地修改
	    deploymentApplyConfig.Spec.Template.Spec.WithContainers(corev1ac.Container().
		WithName("modify-slice").
		WithImage("nginx:1.14.2"),
	    )
	    // 提交 apply
	    applied, err := deploymentClient.Apply(ctx, extractedDeployment, metav1.ApplyOptions{FieldManager: fieldMgr})
*/
package applyconfigurations
