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

package completion

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/utils/ptr"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/kubectl/pkg/cmd/apiresources"
	"k8s.io/kubectl/pkg/cmd/get"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
)

var factory cmdutil.Factory

// 保存补全函数所需的 factory。
// 并非所有命令都能访问 factory，因此无法直接传给补全函数。
func SetFactoryForCompletion(f cmdutil.Factory) {
	factory = f
}

// 返回的补全函数按 toComplete 前缀补全资源类型
// 和资源名，支持 <type>/<name> 形式。
func ResourceTypeAndNameCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return resourceTypeAndNameCompletionFunc(f, nil, true)
}

// 返回的补全函数只补全 allowedTypes 中列出的资源类型，
// 并按 toComplete 前缀补全资源名。
// 允许补全多个资源，支持 <type>/<name> 形式。
func SpecifiedResourceTypeAndNameCompletionFunc(f cmdutil.Factory, allowedTypes []string) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return resourceTypeAndNameCompletionFunc(f, allowedTypes, true)
}

// 返回的补全函数只补全 allowedTypes 中列出的资源类型，
// 并按 toComplete 前缀补全资源名。
// 只允许补全一个资源，支持 <type>/<name> 形式。
func SpecifiedResourceTypeAndNameNoRepeatCompletionFunc(f cmdutil.Factory, allowedTypes []string) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return resourceTypeAndNameCompletionFunc(f, allowedTypes, false)
}

// 返回的补全函数按 resourceType 指定的类型，
// 把匹配 toComplete 前缀的资源名补全为首个参数。
// 不支持 <type>/<name> 形式，供不支持该形式的命令使用；
// 适用于 Pod 且支持 <type>/<name> 的命令
// 应使用 PodResourceNameCompletionFunc()。
func ResourceNameCompletionFunc(f cmdutil.Factory, resourceType string) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		if len(args) == 0 {
			comps = CompGetResource(f, resourceType, toComplete)
		}
		return comps, cobra.ShellCompDirectiveNoFileComp
	}
}

// 返回的补全函数补全：
// 1. 匹配 toComplete 前缀的 Pod 名；
// 2. 名称匹配前缀且包含 Pod 的资源类型。
func PodResourceNameCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		directive := cobra.ShellCompDirectiveNoFileComp
		if len(args) == 0 {
			comps, directive = doPodResourceCompletion(f, toComplete)
		}
		return comps, directive
	}
}

// 返回的补全函数把以下内容补全为首个参数：
// 1. 匹配 toComplete 前缀的资源；
// 2. 特定资源的端口：Pod 是容器端口，Service 是服务端口。
func ResourceAndPortCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		var resourceType string
		directive := cobra.ShellCompDirectiveNoFileComp
		if len(args) == 0 {
			comps, directive = doPodResourceCompletion(f, toComplete)
		} else if len(args) == 1 {
			t := strings.Split(args[0], "/")
			// 直接指定 Pod 时资源类型取 pod，否则取用户
			// 传入的资源类型。
			if len(t) == 1 {
				resourceType = "pod"
			} else {
				resourceType = t[0]
			}
			if resourceType == "service" || resourceType == "services" || resourceType == "svc" {
				comps = CompGetServicePorts(f, t[1], toComplete)
			} else {
				podName := convertResourceNameToPodName(f, args[0])
				comps = CompGetPodContainerPorts(f, podName, toComplete)
			}
		}
		return comps, directive
	}
}

// 返回的补全函数把以下内容补全为首个参数：
// 1. 匹配 toComplete 前缀的 Pod 名；
// 2. 名称匹配前缀且包含 Pod 的资源类型。
// 并把指定 Pod 中的容器补全为第二个参数。
func PodResourceNameAndContainerCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		directive := cobra.ShellCompDirectiveNoFileComp
		if len(args) == 0 {
			comps, directive = doPodResourceCompletion(f, toComplete)
		} else if len(args) == 1 {
			podName := convertResourceNameToPodName(f, args[0])
			comps = CompGetContainers(f, podName, toComplete)
		}
		return comps, directive
	}
}

// 返回的补全函数按首个参数指定的 Pod 补全容器名；
// 包含该 Pod 的资源可用 <type>/<name> 形式指定。
func ContainerCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		// 补全容器名需要 Pod 名，必须在 args[0] 中。
		// 首个参数也可能是 <type>/<name> 形式，需要先转换。
		if len(args) > 0 {
			podName := convertResourceNameToPodName(f, args[0])
			comps = CompGetContainers(f, podName, toComplete)
		}
		return comps, cobra.ShellCompDirectiveNoFileComp
	}
}

// 按 toComplete 前缀补全 context 名，
// 并作为首个参数。
func ContextCompletionFunc(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) == 0 {
		return ListContextsInConfig(toComplete), cobra.ShellCompDirectiveNoFileComp
	}
	return nil, cobra.ShellCompDirectiveNoFileComp
}

// 按 toComplete 前缀补全 cluster 名，
// 并作为首个参数。
func ClusterCompletionFunc(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) == 0 {
		return ListClustersInConfig(toComplete), cobra.ShellCompDirectiveNoFileComp
	}
	return nil, cobra.ShellCompDirectiveNoFileComp
}

// 按 toComplete 前缀补全 user 名，
// 并作为首个参数。
func UserCompletionFunc(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) == 0 {
		return ListUsersInConfig(toComplete), cobra.ShellCompDirectiveNoFileComp
	}
	return nil, cobra.ShellCompDirectiveNoFileComp
}

// 取指定资源中名称以 `toComplete` 开头的列表。
func CompGetResource(f cmdutil.Factory, resourceName string, toComplete string) []string {
	template := "{{ range .items  }}{{ .metadata.name }} {{ end }}"
	return CompGetFromTemplate(&template, f, "", []string{resourceName}, toComplete)
}

// 取指定 Pod 中名称以 `toComplete` 开头的容器列表。
func CompGetContainers(f cmdutil.Factory, podName string, toComplete string) []string {
	template := "{{ range .spec.initContainers }}{{ .name }} {{end}}{{ range .spec.containers  }}{{ .name }} {{ end }}"
	return CompGetFromTemplate(&template, f, "", []string{"pod", podName}, toComplete)
}

// 取指定 Pod 容器中名称以 `toComplete` 开头的端口列表。
func CompGetPodContainerPorts(f cmdutil.Factory, podName string, toComplete string) []string {
	var template string
	exposedPort := strings.Split(toComplete, ":")[0]

	if toComplete == "" {
		exposedPort = "{{ .containerPort }}"
	}
	template = fmt.Sprintf("{{ range .spec.containers }}{{ range .ports }}%s:{{ .containerPort }} {{ end }}{{ end }}", exposedPort)
	return CompGetFromTemplate(&template, f, "", []string{"pod", podName}, toComplete)
}

// 取指定 Service 中名称以 `toComplete` 开头的端口列表。
func CompGetServicePorts(f cmdutil.Factory, serviceName string, toComplete string) []string {
	var template string
	exposedPort := strings.Split(toComplete, ":")[0]

	if toComplete == "" {
		exposedPort = "{{ .port }}"
	}
	template = fmt.Sprintf("{{ range .spec.ports }}%s:{{ .port }} {{ end }}", exposedPort)
	return CompGetFromTemplate(&template, f, "", []string{"service", serviceName}, toComplete)
}

// 用给定模板和参数执行 Get，返回其中以 `toComplete`
// 开头的结果。
func CompGetFromTemplate(template *string, f cmdutil.Factory, namespace string, args []string, toComplete string) []string {
	buf := new(bytes.Buffer)
	streams := genericiooptions.IOStreams{In: os.Stdin, Out: buf, ErrOut: io.Discard}
	o := get.NewGetOptions("kubectl", streams)

	// 取指定资源的名称列表。
	o.PrintFlags.TemplateFlags.GoTemplatePrintFlags.TemplateArgument = template
	format := "go-template"
	o.PrintFlags.OutputFormat = &format

	// 执行 Complete() 本应完成的步骤。
	// 不能直接调用 Complete() 或 Validate()，因为它们会检查
	// 标志是否存在，而补全场景下这些标志并不存在。
	if namespace != "" {
		o.Namespace = namespace
		o.ExplicitNamespace = true
	} else {
		var err error
		o.Namespace, o.ExplicitNamespace, err = f.ToRawKubeConfigLoader().Namespace()
		if err != nil {
			return nil
		}
	}

	o.ToPrinter = func(mapping *meta.RESTMapping, outputObjects *bool, withNamespace bool, withKind bool) (printers.ResourcePrinterFunc, error) {
		printer, err := o.PrintFlags.ToPrinter()
		if err != nil {
			return nil, err
		}
		return printer.PrintObj, nil
	}

	o.Run(f, args)

	var comps []string
	resources := strings.Split(buf.String(), " ")
	for _, res := range resources {
		if res != "" && strings.HasPrefix(res, toComplete) {
			comps = append(comps, res)
		}
	}
	return comps
}

// 返回配置中以 `toComplete` 开头的 context 名列表。
func ListContextsInConfig(toComplete string) []string {
	config, err := factory.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return nil
	}
	var ret []string
	for name := range config.Contexts {
		if strings.HasPrefix(name, toComplete) {
			ret = append(ret, name)
		}
	}
	return ret
}

// 返回配置中以 `toComplete` 开头的 cluster 名列表。
func ListClustersInConfig(toComplete string) []string {
	config, err := factory.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return nil
	}
	var ret []string
	for name := range config.Clusters {
		if strings.HasPrefix(name, toComplete) {
			ret = append(ret, name)
		}
	}
	return ret
}

// 返回配置中以 `toComplete` 开头的 user 名列表。
func ListUsersInConfig(toComplete string) []string {
	config, err := factory.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return nil
	}
	var ret []string
	for name := range config.AuthInfos {
		if strings.HasPrefix(name, toComplete) {
			ret = append(ret, name)
		}
	}
	return ret
}

// 返回名称以 `toComplete` 开头的 API 资源列表。
func compGetResourceList(restClientGetter genericclioptions.RESTClientGetter, cmd *cobra.Command, toComplete string) []string {
	buf := new(bytes.Buffer)
	streams := genericiooptions.IOStreams{In: os.Stdin, Out: buf, ErrOut: io.Discard}
	o := apiresources.NewAPIResourceOptions(streams)

	// 取资源列表。
	o.PrintFlags.OutputFormat = ptr.To("name")
	o.Cached = true
	o.Verbs = []string{"get"}
	// TODO: 应设置 --request-timeout=5s。

	if err := o.Complete(restClientGetter, cmd, nil); err != nil {
		return []string{}
	}

	// 忽略错误，因为输出仍可能有效。
	_ = o.RunAPIResources()

	// 资源可以是逗号分隔的列表，此时应补全
	// 最后一个元素。例如 toComplete=="pods,secre"
	// 应返回 "pods,secrets"。
	prefix := ""
	suffix := toComplete
	lastIdx := strings.LastIndex(toComplete, ",")
	if lastIdx != -1 {
		prefix = toComplete[0 : lastIdx+1]
		suffix = toComplete[lastIdx+1:]
	}
	var comps []string
	resources := strings.Split(buf.String(), "\n")
	for _, res := range resources {
		if res != "" && strings.HasPrefix(res, suffix) {
			comps = append(comps, fmt.Sprintf("%s%s", prefix, res))
		}
	}
	return comps
}

// 返回的补全函数按 toComplete 前缀补全资源类型
// 和资源名，支持 <type>/<name> 形式。
func resourceTypeAndNameCompletionFunc(f cmdutil.Factory, allowedTypes []string, allowRepeat bool) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		directive := cobra.ShellCompDirectiveNoFileComp

		if len(args) > 0 && !strings.Contains(args[0], "/") {
			// 首个参数为 <type> 形式（如 pods）；
			// 后续参数都是资源名。
			if allowRepeat || len(args) == 1 {
				comps = CompGetResource(f, args[0], toComplete)

				// 移除命令行上已有的候选项。
				if len(args) > 1 {
					comps = cmdutil.Difference(comps, args[1:])
				}
			}
		} else {
			slashIdx := strings.Index(toComplete, "/")
			if slashIdx == -1 {
				if len(args) == 0 {
					// 正在补全首个参数，默认采用普通的
					// <type> 形式，而非 <type>/<name>。
					// 因此只建议资源类型，由 shell 在补全后
					// 补一个空格。
					if len(allowedTypes) == 0 {
						comps = compGetResourceList(f, cmd, toComplete)
					} else {
						for _, c := range allowedTypes {
							if strings.HasPrefix(c, toComplete) {
								comps = append(comps, c)
							}
						}
					}
				} else {
					// 首个参数已含 /（<type>/<name> 形式），
					// 其他参数也必须用这种形式。
					if allowRepeat {
						// toComplete 中尚无 /，说明正在补全
						// 资源类型：补全后不加空格，而是补 /。
						directive |= cobra.ShellCompDirectiveNoSpace

						if len(allowedTypes) == 0 {
							typeComps := compGetResourceList(f, cmd, toComplete)
							for _, c := range typeComps {
								comps = append(comps, fmt.Sprintf("%s/", c))
							}
						} else {
							for _, c := range allowedTypes {
								if strings.HasPrefix(c, toComplete) {
									comps = append(comps, fmt.Sprintf("%s/", c))
								}
							}
						}
					}
				}
			} else {
				// 正在补全 <type>/<name> 形式参数；
				// 已有 /，因此补全的是资源名。
				if allowRepeat || len(args) == 0 {
					resourceType := toComplete[:slashIdx]
					toComplete = toComplete[slashIdx+1:]
					nameComps := CompGetResource(f, resourceType, toComplete)
					for _, c := range nameComps {
						comps = append(comps, fmt.Sprintf("%s/%s", resourceType, c))
					}

					// 移除命令行上已有的候选项。
					if len(args) > 0 {
						comps = cmdutil.Difference(comps, args[0:])
					}
				}
			}
		}
		return comps, directive
	}
}

// 返回以下补全项：
// 1. 匹配 toComplete 前缀的 Pod 名；
// 2. 名称匹配前缀且包含 Pod 的资源类型。
func doPodResourceCompletion(f cmdutil.Factory, toComplete string) ([]string, cobra.ShellCompDirective) {
	var comps []string
	directive := cobra.ShellCompDirectiveNoFileComp
	slashIdx := strings.Index(toComplete, "/")
	if slashIdx == -1 {
		// 常规情况：补全 Pod 名。
		comps = CompGetResource(f, "pod", toComplete)

		// 也提供 <type>/<name> 形式的资源候选项，
		// 但只包含可包含 Pod 的资源。
		resourcesWithPods := []string{
			"daemonsets",
			"deployments",
			"pods",
			"jobs",
			"replicasets",
			"replicationcontrollers",
			"services",
			"statefulsets"}

		if len(comps) == 0 {
			// 没有 Pod 可补全时只需补全 <type>/，
			// 此时 / 后不应加空格。
			directive |= cobra.ShellCompDirectiveNoSpace
		}

		for _, resource := range resourcesWithPods {
			if strings.HasPrefix(resource, toComplete) {
				comps = append(comps, fmt.Sprintf("%s/", resource))
			}
		}
	} else {
		// 处理 <type>/<name> 形式时使用指定的资源类型。
		resourceType := toComplete[:slashIdx]
		toComplete = toComplete[slashIdx+1:]
		nameComps := CompGetResource(f, resourceType, toComplete)
		for _, c := range nameComps {
			comps = append(comps, fmt.Sprintf("%s/%s", resourceType, c))
		}
	}
	return comps, directive
}

// 把资源名转换为 Pod 名。
// <type>/<name> 形式时用
// polymorphichelpers.AttachablePodForObjectFn() 转换，否则资源名
// 本身就是 Pod 名。
func convertResourceNameToPodName(f cmdutil.Factory, resourceName string) string {
	var podName string
	if !strings.Contains(resourceName, "/") {
		// 没有 <type>/<name> 形式时，资源名即 Pod 名；
		podName = resourceName
	} else {
		// 有该形式时需转换为 Pod 名。
		ns, _, err := f.ToRawKubeConfigLoader().Namespace()
		if err != nil {
			return ""
		}

		resourceWithPod, err := f.NewBuilder().
			WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
			ContinueOnError().
			NamespaceParam(ns).DefaultNamespace().
			ResourceNames("pods", resourceName).
			Do().Object()
		if err != nil {
			return ""
		}

		// 补全场景使用较短的超时。
		forwardablePod, err := polymorphichelpers.AttachablePodForObjectFn(f, resourceWithPod, 100*time.Millisecond)
		if err != nil {
			return ""
		}
		podName = forwardablePod.Name
	}
	return podName
}
