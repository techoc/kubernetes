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

package bootstrap

import (
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	flowcontrol "k8s.io/api/flowcontrol/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/utils/ptr"
)

// 这些对象定义 apiserver 的初始行为。默认的默认值填充流程不会改动这些对象
// （这一点在 internalbootstrap 包的单测中验证；在本包无法验证，
// 因为那需要导入 k8s.io/kubernetes）。
//
// 因为那需要导入 k8s.io/kubernetes）。
//
// 强制对象必须在 APIServer 启动时存在且不可删除，否则流控策略出现缺口；
// 它们不参与自动更新（见 AutoUpdateAnnotationKey 的语义）。
var (
	MandatoryPriorityLevelConfigurations = []*flowcontrol.PriorityLevelConfiguration{
		MandatoryPriorityLevelConfigurationCatchAll,
		MandatoryPriorityLevelConfigurationExempt,
	}
	MandatoryFlowSchemas = []*flowcontrol.FlowSchema{
		MandatoryFlowSchemaExempt,
		MandatoryFlowSchemaCatchAll,
	}
)

// 以下对象定义当前建议的附加配置。
//
// 建议对象在启动时写入、之后由系统定期校正；管理员可修改其参数，但不应删除，
// 删除后会被重新创建。
var (
	SuggestedPriorityLevelConfigurations = []*flowcontrol.PriorityLevelConfiguration{
		// "system" 优先级面向影响集群自维护能力的系统组件，以及运行中的 Pod 的可用性，
		// 包括 kubelet 和 kube-proxy。
		// kube-proxy.
		SuggestedPriorityLevelConfigurationSystem,
		// "node-high" 优先级用于节点健康上报。它与 "system" 分开，是为了在 kube-apiserver
		// 忙于处理 Pod 启动（拉取 secret、写 event 等）带来的负载时，节点仍能上报健康状态。
		// 处理 Pod 启动（拉取 secret、event 等）带来的负载。
		// 注意：在大型集群中，50% - 90% 的 API 调用使用该优先级。
		SuggestedPriorityLevelConfigurationNodeHigh,
		// "leader-election" 专用于控制器的选主，主要影响集群内各控制器的可用性。
		// 集群中运行的任何控制器的可用性。
		SuggestedPriorityLevelConfigurationLeaderElection,
		// "workload-high" 用于优先级较高、但失败不会直接影响集群中现有运行中 Pod 的工作负载，
		// 包括 kube-scheduler，以及 "deployments"、"replicasets" 等众所周知的内置工作负载
		// 和其他对集群重要的底层自定义工作负载。
		// 对集群重要。
		SuggestedPriorityLevelConfigurationWorkloadHigh,
		// "workload-low" 用于优先级较低、其可用性对集群影响较小的工作负载。
		// 对集群影响较小。
		SuggestedPriorityLevelConfigurationWorkloadLow,
		// "global-default" 承接上述建议流控模式未覆盖的其余流量。
		SuggestedPriorityLevelConfigurationGlobalDefault,
	}
	SuggestedFlowSchemas = []*flowcontrol.FlowSchema{
		SuggestedFlowSchemaSystemNodes,               // 引用 "system" 优先级
		SuggestedFlowSchemaSystemNodeHigh,            // 引用 "node-high" 优先级
		SuggestedFlowSchemaProbes,                    // 引用 "exempt" 优先级
		SuggestedFlowSchemaSystemLeaderElection,      // 引用 "leader-election" 优先级
		SuggestedFlowSchemaKubeControllerManager,     // 引用 "workload-high" 优先级
		SuggestedFlowSchemaKubeScheduler,             // 引用 "workload-high" 优先级
		SuggestedFlowSchemaKubeSystemServiceAccounts, // 引用 "workload-high" 优先级
		SuggestedFlowSchemaServiceAccounts,           // 引用 "workload-low" 优先级
		SuggestedFlowSchemaGlobalDefault,             // 引用 "global-default" 优先级
	}
)

// 必需的 PriorityLevelConfiguration 对象
var (
	// Exempt 级别不占用并发额度，任何请求都不会被排队或拒绝；
	// 份额与借出比例显式置 0，表示该级别不参与公平分配。
	MandatoryPriorityLevelConfigurationExempt = newPriorityLevelConfiguration(
		flowcontrol.PriorityLevelConfigurationNameExempt,
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementExempt,
			Exempt: &flowcontrol.ExemptPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(0)),
				LendablePercent:          ptr.To(int32(0)),
			},
		},
	)
	// CatchAll 是最后兜底级别：份额极小（5）且不借出，超出即拒绝而非排队，
	// 防止未知流量无限占用队列内存。
	MandatoryPriorityLevelConfigurationCatchAll = newPriorityLevelConfiguration(
		flowcontrol.PriorityLevelConfigurationNameCatchAll,
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(5)),
				LendablePercent:          ptr.To(int32(0)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeReject,
				},
			},
		})
)

// 必需的 FlowSchema 对象
var (
	// "exempt" priority-level 用于防止优先级反转并确保系统管理员的请求始终可提交；
	// 该 FlowSchema 优先级最高（precedence 1），覆盖所有动词、资源与非资源 URL，
	// 因此 system:masters 的请求永远不会因流控被拒绝。
	MandatoryFlowSchemaExempt = newFlowSchema(
		"exempt",
		flowcontrol.PriorityLevelConfigurationNameExempt,
		1,  // 匹配优先级
		"", // 区分方法类型
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: groups(user.SystemPrivilegedGroup),
			ResourceRules: []flowcontrol.ResourcePolicyRule{
				resourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.APIGroupAll},
					[]string{flowcontrol.ResourceAll},
					[]string{flowcontrol.NamespaceEvery},
					true,
				),
			},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll},
				),
			},
		},
	)
	// "catch-all" 优先级只保留最小的正并发份额；理想情况下不应命中，除非故意删除了建议的 "global-default"。
	// precedence 10000 保证它在所有其他 FlowSchema 之后匹配。
	MandatoryFlowSchemaCatchAll = newFlowSchema(
		flowcontrol.FlowSchemaNameCatchAll,
		flowcontrol.PriorityLevelConfigurationNameCatchAll,
		10000, // matchingPrecedence
		flowcontrol.FlowDistinguisherMethodByUserType, // distinguisherMethodType
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: groups(user.AllUnauthenticated, user.AllAuthenticated),
			ResourceRules: []flowcontrol.ResourcePolicyRule{
				resourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.APIGroupAll},
					[]string{flowcontrol.ResourceAll},
					[]string{flowcontrol.NamespaceEvery},
					true,
				),
			},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll},
				),
			},
		},
	)
)

// 建议的 PriorityLevelConfiguration 对象
var (
	// system priority-level：
	// 允许借出 33% 的并发额度给其他级别，系统组件繁忙时仍能回收闲置份额。
	SuggestedPriorityLevelConfigurationSystem = newPriorityLevelConfiguration(
		"system",
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(30)),
				LendablePercent:          ptr.To(int32(33)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeQueue,
					Queuing: &flowcontrol.QueuingConfiguration{
						Queues:           64,
						HandSize:         6,
						QueueLengthLimit: 50,
					},
				},
			},
		})
	// node-high 份额高于 system，因为节点状态上报是集群自愈的前提；
	// 借出比例 25% 限制其他级别可抢占的空间。
	SuggestedPriorityLevelConfigurationNodeHigh = newPriorityLevelConfiguration(
		"node-high",
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(40)),
				LendablePercent:          ptr.To(int32(25)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeQueue,
					Queuing: &flowcontrol.QueuingConfiguration{
						Queues:           64,
						HandSize:         6,
						QueueLengthLimit: 50,
					},
				},
			},
		})
	// leader-election priority-level：
	// 不借出额度：选主请求被其他级别挤占会导致控制器整体失联，代价过高。
	SuggestedPriorityLevelConfigurationLeaderElection = newPriorityLevelConfiguration(
		"leader-election",
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(10)),
				LendablePercent:          ptr.To(int32(0)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeQueue,
					Queuing: &flowcontrol.QueuingConfiguration{
						Queues:           16,
						HandSize:         4,
						QueueLengthLimit: 50,
					},
				},
			},
		})
	// workload-high priority-level：
	// 队列数较多（128），因为该级别承载多种不同控制器，需要更细的隔离粒度。
	SuggestedPriorityLevelConfigurationWorkloadHigh = newPriorityLevelConfiguration(
		"workload-high",
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(40)),
				LendablePercent:          ptr.To(int32(50)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeQueue,
					Queuing: &flowcontrol.QueuingConfiguration{
						Queues:           128,
						HandSize:         6,
						QueueLengthLimit: 50,
					},
				},
			},
		})
	// workload-low priority-level：
	// 份额最大（100）但可借出 90%：空闲时可被高优先级级别完全借用，实现按需分配。
	SuggestedPriorityLevelConfigurationWorkloadLow = newPriorityLevelConfiguration(
		"workload-low",
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(100)),
				LendablePercent:          ptr.To(int32(90)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeQueue,
					Queuing: &flowcontrol.QueuingConfiguration{
						Queues:           128,
						HandSize:         6,
						QueueLengthLimit: 50,
					},
				},
			},
		})
	// global-default priority-level：
	// 与 workload-low 相同的可借出比例，作为未被其他 FlowSchema 命中的普通认证用户的归宿。
	SuggestedPriorityLevelConfigurationGlobalDefault = newPriorityLevelConfiguration(
		"global-default",
		flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(20)),
				LendablePercent:          ptr.To(int32(50)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeQueue,
					Queuing: &flowcontrol.QueuingConfiguration{
						Queues:           128,
						HandSize:         6,
						QueueLengthLimit: 50,
					},
				},
			},
		})
)

// 建议的 FlowSchema 对象。
// 按匹配优先级排序，便于阅读源码时理清各对象之间的相互作用。
// 便于阅读源码时理清相互作用。
var (
	// 探针放行到 exempt 级别：kubelet 依赖探针结果决定是否重启容器，
	// 若被流控排队会引发误判与连锁重启。
	SuggestedFlowSchemaProbes = newFlowSchema(
		"probes", "exempt", 2,
		"", // 区分方法类型
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: groups(user.AllUnauthenticated, user.AllAuthenticated),
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{"get"},
					[]string{"/healthz", "/readyz", "/livez"}),
			},
		},
	)
	SuggestedFlowSchemaSystemLeaderElection = newFlowSchema(
		"system-leader-election", "leader-election", 100,
		flowcontrol.FlowDistinguisherMethodByUserType,
		// 控制平面组件 + kube-system 中的 ServiceAccount 都可能是选主参与者；
		// 限定 lease 的 get/create/update，避免把同一主体的其他请求也吸入该级别。
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: append(
				users(user.KubeControllerManager, user.KubeScheduler),
				kubeSystemServiceAccount(flowcontrol.NameAll)...),
			ResourceRules: []flowcontrol.ResourcePolicyRule{
				resourceRule(
					[]string{"get", "create", "update"},
					[]string{coordinationv1.GroupName},
					[]string{"leases"},
					[]string{flowcontrol.NamespaceEvery},
					false),
			},
		},
	)
	// system-node-high 只覆盖节点状态上报与节点 lease，节点上的 Pod 启动流量走 system-nodes，
	// 这样 API server 过载时节点仍能报告健康状态。
	SuggestedFlowSchemaSystemNodeHigh = newFlowSchema(
		"system-node-high", "node-high", 400,
		flowcontrol.FlowDistinguisherMethodByUserType,
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: groups(user.NodesGroup), // nodes 组
			ResourceRules: []flowcontrol.ResourcePolicyRule{
				resourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{corev1.GroupName},
					[]string{"nodes", "nodes/status"},
					[]string{flowcontrol.NamespaceEvery},
					true),
				resourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{coordinationv1.GroupName},
					[]string{"leases"},
					[]string{flowcontrol.NamespaceEvery},
					false),
			},
		},
	)
	SuggestedFlowSchemaSystemNodes = newFlowSchema(
		"system-nodes", "system", 500,
		flowcontrol.FlowDistinguisherMethodByUserType,
		// 节点组的其余全部流量进入 system 级别，按节点身份区分队列，
		// 单个异常节点不会挤占其他节点。
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: groups(user.NodesGroup), // nodes 组
			ResourceRules: []flowcontrol.ResourcePolicyRule{resourceRule(
				[]string{flowcontrol.VerbAll},
				[]string{flowcontrol.APIGroupAll},
				[]string{flowcontrol.ResourceAll},
				[]string{flowcontrol.NamespaceEvery},
				true)},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll}),
			},
		},
	)
	SuggestedFlowSchemaKubeControllerManager = newFlowSchema(
		"kube-controller-manager", "workload-high", 800,
		// 按命名空间区分：一个命名空间内的控制器批量操作不会拖慢其他命名空间。
		flowcontrol.FlowDistinguisherMethodByNamespaceType,
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: users(user.KubeControllerManager),
			ResourceRules: []flowcontrol.ResourcePolicyRule{resourceRule(
				[]string{flowcontrol.VerbAll},
				[]string{flowcontrol.APIGroupAll},
				[]string{flowcontrol.ResourceAll},
				[]string{flowcontrol.NamespaceEvery},
				true)},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll}),
			},
		},
	)
	SuggestedFlowSchemaKubeScheduler = newFlowSchema(
		"kube-scheduler", "workload-high", 800,
		// 与 kube-controller-manager 相同的隔离策略；两者共用 precedence 800，
		// 因为主体不重叠，匹配顺序不影响结果。
		flowcontrol.FlowDistinguisherMethodByNamespaceType,
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: users(user.KubeScheduler),
			ResourceRules: []flowcontrol.ResourcePolicyRule{resourceRule(
				[]string{flowcontrol.VerbAll},
				[]string{flowcontrol.APIGroupAll},
				[]string{flowcontrol.ResourceAll},
				[]string{flowcontrol.NamespaceEvery},
				true)},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll}),
			},
		},
	)
	SuggestedFlowSchemaKubeSystemServiceAccounts = newFlowSchema(
		"kube-system-service-accounts", "workload-high", 900,
		// kube-system 中的组件（如 kube-dns、metrics-server）是集群基础设施，
		// 因此给予 workload-high 而非普通 ServiceAccount 的 workload-low。
		flowcontrol.FlowDistinguisherMethodByNamespaceType,
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: kubeSystemServiceAccount(flowcontrol.NameAll),
			ResourceRules: []flowcontrol.ResourcePolicyRule{resourceRule(
				[]string{flowcontrol.VerbAll},
				[]string{flowcontrol.APIGroupAll},
				[]string{flowcontrol.ResourceAll},
				[]string{flowcontrol.NamespaceEvery},
				true)},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll}),
			},
		},
	)
	SuggestedFlowSchemaServiceAccounts = newFlowSchema(
		"service-accounts", "workload-low", 9000,
		// 普通命名空间中的 ServiceAccount 统一走 workload-low；
		// 按用户类型区分可避免所有 SA 挤在同一队列。
		flowcontrol.FlowDistinguisherMethodByUserType,
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: groups(serviceaccount.AllServiceAccountsGroup),
			ResourceRules: []flowcontrol.ResourcePolicyRule{resourceRule(
				[]string{flowcontrol.VerbAll},
				[]string{flowcontrol.APIGroupAll},
				[]string{flowcontrol.ResourceAll},
				[]string{flowcontrol.NamespaceEvery},
				true)},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll}),
			},
		},
	)
	SuggestedFlowSchemaGlobalDefault = newFlowSchema(
		"global-default", "global-default", 9900,
		// precedence 9900 使其位于 catch-all（10000）之前，作为绝大多数请求的默认归宿。
		flowcontrol.FlowDistinguisherMethodByUserType,
		flowcontrol.PolicyRulesWithSubjects{
			Subjects: groups(user.AllUnauthenticated, user.AllAuthenticated),
			ResourceRules: []flowcontrol.ResourcePolicyRule{resourceRule(
				[]string{flowcontrol.VerbAll},
				[]string{flowcontrol.APIGroupAll},
				[]string{flowcontrol.ResourceAll},
				[]string{flowcontrol.NamespaceEvery},
				true)},
			NonResourceRules: []flowcontrol.NonResourcePolicyRule{
				nonResourceRule(
					[]string{flowcontrol.VerbAll},
					[]string{flowcontrol.NonResourceAll}),
			},
		},
	)
)

// newPriorityLevelConfiguration 构造一个带自动更新注解的 PriorityLevelConfiguration；
// 注解让内置控制器在升级时校正参数，而不是与管理员的手工修改冲突。
func newPriorityLevelConfiguration(name string, spec flowcontrol.PriorityLevelConfigurationSpec) *flowcontrol.PriorityLevelConfiguration {
	return &flowcontrol.PriorityLevelConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				flowcontrol.AutoUpdateAnnotationKey: "true",
			},
		},
		Spec: spec,
	}
}

// newFlowSchema 构造 FlowSchema。dmType 为空表示不做 distinguisher，
// 即该 FlowSchema 的所有流量共享同一个队列（仅用于 exempt 与 probes 这类全放行场景）。
func newFlowSchema(name, plName string, matchingPrecedence int32, dmType flowcontrol.FlowDistinguisherMethodType, rules ...flowcontrol.PolicyRulesWithSubjects) *flowcontrol.FlowSchema {
	var dm *flowcontrol.FlowDistinguisherMethod
	if dmType != "" {
		dm = &flowcontrol.FlowDistinguisherMethod{Type: dmType}
	}
	return &flowcontrol.FlowSchema{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				flowcontrol.AutoUpdateAnnotationKey: "true",
			},
		},
		Spec: flowcontrol.FlowSchemaSpec{
			PriorityLevelConfiguration: flowcontrol.PriorityLevelConfigurationReference{
				Name: plName,
			},
			MatchingPrecedence:  matchingPrecedence,
			DistinguisherMethod: dm,
			Rules:               rules},
	}

}

// groups 把组名转换为 Group 类型的 Subject 列表。
func groups(names ...string) []flowcontrol.Subject {
	ans := make([]flowcontrol.Subject, len(names))
	for idx, name := range names {
		ans[idx] = flowcontrol.Subject{
			Kind: flowcontrol.SubjectKindGroup,
			Group: &flowcontrol.GroupSubject{
				Name: name,
			},
		}
	}
	return ans
}

// users 把用户名转换为 User 类型的 Subject 列表。
func users(names ...string) []flowcontrol.Subject {
	ans := make([]flowcontrol.Subject, len(names))
	for idx, name := range names {
		ans[idx] = flowcontrol.Subject{
			Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{
				Name: name,
			},
		}
	}
	return ans
}

// kubeSystemServiceAccount 生成 kube-system 命名空间下的 ServiceAccount 主体；
// 命名空间固定为 kube-system，因为这是内置组件的约定位置。
func kubeSystemServiceAccount(names ...string) []flowcontrol.Subject {
	subjects := []flowcontrol.Subject{}
	for _, name := range names {
		subjects = append(subjects, flowcontrol.Subject{
			Kind: flowcontrol.SubjectKindServiceAccount,
			ServiceAccount: &flowcontrol.ServiceAccountSubject{
				Name:      name,
				Namespace: metav1.NamespaceSystem,
			},
		})
	}
	return subjects
}

// resourceRule 构造资源规则；clusterScoped=true 表示只匹配集群作用域资源。
func resourceRule(verbs []string, groups []string, resources []string, namespaces []string, clusterScoped bool) flowcontrol.ResourcePolicyRule {
	return flowcontrol.ResourcePolicyRule{
		Verbs:        verbs,
		APIGroups:    groups,
		Resources:    resources,
		Namespaces:   namespaces,
		ClusterScope: clusterScoped,
	}
}

// nonResourceRule 构造非资源 URL 规则（如 /healthz）。
func nonResourceRule(verbs []string, nonResourceURLs []string) flowcontrol.NonResourcePolicyRule {
	return flowcontrol.NonResourcePolicyRule{Verbs: verbs, NonResourceURLs: nonResourceURLs}
}
