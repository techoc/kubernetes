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

package autoscale

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	autoscalingv1client "k8s.io/client-go/kubernetes/typed/autoscaling/v1"
	autoscalingv2client "k8s.io/client-go/kubernetes/typed/autoscaling/v2"
	"k8s.io/client-go/scale"
	"k8s.io/klog/v2"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	autoscaleLong = templates.LongDesc(i18n.T(`
		Creates an autoscaler that automatically chooses and sets the number of pods that run in a Kubernetes cluster.
		The command will attempt to use the autoscaling/v2 API first, in case of an error, it will fall back to autoscaling/v1 API.

		Looks up a deployment, replica set, stateful set, or replication controller by name and creates an autoscaler that uses the given resource as a reference.
		An autoscaler can automatically increase or decrease number of pods deployed within the system as needed.`))

	autoscaleExample = templates.Examples(i18n.T(`
		# Auto scale a deployment "foo", with the number of pods between 2 and 10, no target CPU utilization specified so a default autoscaling policy will be used
        kubectl autoscale deployment foo --min=2 --max=10

		# Auto scale a replication controller "foo", with the number of pods between 1 and 5, target CPU utilization at 80%
        kubectl autoscale rc foo --max=5 --cpu=80%

		# Auto scale a deployment "bar", with the number of pods between 3 and 6, target average CPU of 500m and memory of 200Mi
		kubectl autoscale deployment bar --min=3 --max=6 --cpu=500m --memory=200Mi
		
		# Auto scale a deployment "bar", with the number of pods between 2 and 8, target CPU utilization 60% and memory utilization 70%
        kubectl autoscale deployment bar --min=2 --max=8 --cpu=60% --memory=70%`))
)

// AutoscaleOptions 声明 Autoscale 命令接受的参数
type AutoscaleOptions struct {
	FilenameOptions *resource.FilenameOptions

	RecordFlags *genericclioptions.RecordFlags
	Recorder    genericclioptions.Recorder

	PrintFlags *genericclioptions.PrintFlags
	ToPrinter  func(string) (printers.ResourcePrinter, error)

	Name       string
	Min        int32
	Max        int32
	CPUPercent int32
	CPU        string
	Memory     string

	createAnnotation bool
	args             []string
	enforceNamespace bool
	namespace        string
	dryRunStrategy   cmdutil.DryRunStrategy
	builder          *resource.Builder
	fieldManager     string

	HPAClientV1       autoscalingv1client.HorizontalPodAutoscalersGetter
	HPAClientV2       autoscalingv2client.HorizontalPodAutoscalersGetter
	scaleKindResolver scale.ScaleKindResolver

	genericiooptions.IOStreams
}

// NewAutoscaleOptions 创建 autoscale 的选项
func NewAutoscaleOptions(ioStreams genericiooptions.IOStreams) *AutoscaleOptions {
	return &AutoscaleOptions{
		PrintFlags:      genericclioptions.NewPrintFlags("autoscaled").WithTypeSetter(scheme.Scheme),
		FilenameOptions: &resource.FilenameOptions{},
		RecordFlags:     genericclioptions.NewRecordFlags(),
		Recorder:        genericclioptions.NoopRecorder{},

		IOStreams: ioStreams,
	}
}

// NewCmdAutoscale 返回 autoscale Cobra 命令
func NewCmdAutoscale(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewAutoscaleOptions(ioStreams)

	validArgs := []string{"deployment", "replicaset", "replicationcontroller", "statefulset"}

	cmd := &cobra.Command{
		Use:                   "autoscale (-f FILENAME | TYPE NAME | TYPE/NAME) [--min=MINPODS] --max=MAXPODS [--cpu=CPU] [--memory=MEMORY]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Auto-scale a deployment, replica set, stateful set, or replication controller"),
		Long:                  autoscaleLong,
		Example:               autoscaleExample,
		ValidArgsFunction:     completion.SpecifiedResourceTypeAndNameCompletionFunc(f, validArgs),
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}

	// 绑定 flag 结构体
	o.RecordFlags.AddFlags(cmd)
	o.PrintFlags.AddFlags(cmd)
	cmd.Flags().Int32Var(&o.Min, "min", -1, "The lower limit for the number of pods that can be set by the autoscaler. If it's not specified or negative, the server will apply a default value.")
	cmd.Flags().Int32Var(&o.Max, "max", -1, "The upper limit for the number of pods that can be set by the autoscaler. Required.")
	cmd.MarkFlagRequired("max")
	cmd.Flags().Int32Var(&o.CPUPercent, "cpu-percent", -1, "The target average CPU utilization (represented as a percent of requested CPU) over all the pods. If it's not specified or negative, a default autoscaling policy will be used.")
	cmd.Flags().StringVar(&o.CPU, "cpu", "", `Target CPU utilization over all the pods. When specified as a percentage (e.g."70%" for 70% of requested CPU) it will target average utilization. When specified as quantity (e.g."500m" for 500 milliCPU) it will target average value. Value without units is treated as a quantity with miliCPU being the unit (e.g."500" is "500m").`)
	cmd.Flags().StringVar(&o.Memory, "memory", "", `Target memory utilization over all the pods. When specified  as a percentage (e.g."60%" for 60% of requested memory) it will target average utilization. When specified as quantity (e.g."200Mi" for 200 MiB, "1Gi" for 1 GiB) it will target average value. Value without units is treated as a quantity with mebibytes being the unit (e.g."200" is "200Mi").`)
	cmd.Flags().StringVar(&o.Name, "name", "", i18n.T("The name for the newly created object. If not specified, the name of the input resource will be used."))
	_ = cmd.Flags().MarkDeprecated("cpu-percent",
		"Use --cpu with percentage or resource quantity format (e.g., '70%' for utilization or '500m' for milliCPU).")
	cmdutil.AddDryRunFlag(cmd)
	cmdutil.AddFilenameOptionFlags(cmd, o.FilenameOptions, "identifying the resource to autoscale.")
	cmdutil.AddApplyAnnotationFlags(cmd)
	cmdutil.AddFieldManagerFlagVar(cmd, &o.fieldManager, "kubectl-autoscale")
	return cmd
}

// Complete 校验命令行参数并从命令环境中加载数据
func (o *AutoscaleOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	var err error
	o.dryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}
	discoveryClient, err := f.ToDiscoveryClient()
	if err != nil {
		return err
	}
	o.createAnnotation = cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag)
	o.builder = f.NewBuilder()
	o.scaleKindResolver = scale.NewDiscoveryScaleKindResolver(discoveryClient)
	o.args = args
	o.RecordFlags.Complete(cmd)

	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	kubeClient, err := f.KubernetesClientSet()
	if err != nil {
		return err
	}
	o.HPAClientV2 = kubeClient.AutoscalingV2()
	o.HPAClientV1 = kubeClient.AutoscalingV1()

	o.namespace, o.enforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.ToPrinter = func(operation string) (printers.ResourcePrinter, error) {
		o.PrintFlags.NamePrintFlags.Operation = operation
		cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.dryRunStrategy)

		return o.PrintFlags.ToPrinter()
	}

	return nil
}

// Validate 检查所提供的 attach 选项是否已指定。
func (o *AutoscaleOptions) Validate() error {
	if o.Max < 1 {
		return fmt.Errorf("--max=MAXPODS is required and must be at least 1, max: %d", o.Max)
	}
	if o.Max < o.Min {
		return fmt.Errorf("--max=MAXPODS must be larger or equal to --min=MINPODS, max: %d, min: %d", o.Max, o.Min)
	}
	// CPUPercent 或 CPU 参数只允许指定其中一个
	if o.CPUPercent > 0 && o.CPU != "" {
		return fmt.Errorf("--cpu-percent and --cpu are mutually exclusive")
	}
	// 如果指定了 CPU 目标，则进行校验
	if o.CPU != "" {
		if _, _, _, err := parseResourceInput(o.CPU, corev1.ResourceCPU); err != nil {
			return err
		}
	}
	// 如果指定了 Memory 目标，则进行校验
	if o.Memory != "" {
		if _, _, _, err := parseResourceInput(o.Memory, corev1.ResourceMemory); err != nil {
			return err
		}
	}
	return nil
}

func (o *AutoscaleOptions) Run() error {
	r := o.builder.
		Unstructured().
		ContinueOnError().
		NamespaceParam(o.namespace).DefaultNamespace().
		FilenameParam(o.enforceNamespace, o.FilenameOptions).
		ResourceTypeOrNameArgs(false, o.args...).
		Flatten().
		Do()
	if err := r.Err(); err != nil {
		return err
	}

	count := 0
	err := r.Visit(func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}

		mapping := info.ResourceMapping()
		gvr := mapping.GroupVersionKind.GroupVersion().WithResource(mapping.Resource.Resource)
		if _, err = o.scaleKindResolver.ScaleForResource(gvr); err != nil {
			return fmt.Errorf("cannot autoscale a %s: %w", mapping.GroupVersionKind.Kind, err)
		}

		// 处理 autoscaling/v2 和 autoscaling/v1 API 下 HorizontalPodAutoscaler 对象的创建。
		// 如果 autoscaling/v2 API 失败，则尝试使用 autoscaling/v1 API 创建并处理 HorizontalPodAutoscaler
		var hpaV2 runtime.Object
		hpaV2, err = o.createHorizontalPodAutoscalerV2(info.Name, mapping)
		if err != nil {
			return fmt.Errorf("failed to create HorizontalPodAutoscaler using autoscaling/v2 API: %w", err)
		}
		if err = o.handleHPA(hpaV2); err != nil {
			klog.V(1).Infof("Encountered an error with the autoscaling/v2 HorizontalPodAutoscaler: %v. "+
				"Falling back to try the autoscaling/v1 HorizontalPodAutoscaler", err)
			// 检查是否可以使用 v1 API 创建 HPA。
			if ok, err := o.canCreateHPAV1(); !ok {
				return fmt.Errorf("failed to create autoscaling/v2 HPA and the configuration is incompatible with autoscaling/v1: %w", err)
			}
			hpaV1 := o.createHorizontalPodAutoscalerV1(info.Name, mapping)
			if err := o.handleHPA(hpaV1); err != nil {
				return err
			}
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("no objects passed to autoscale")
	}
	return nil
}

func (o *AutoscaleOptions) canCreateHPAV1() (bool, error) {
	// 仅在以下条件下允许回退到 v1 HPA：
	// 1. 设置了 CPUPercent 且未设置 Memory。
	// 2. 或者，未设置 Memory 且指标类型是 UtilizationMetricType。
	_, _, metricsType, err := parseResourceInput(o.CPU, corev1.ResourceCPU)
	if err != nil {
		return false, err
	}
	return (o.CPUPercent >= 0 && o.Memory == "") ||
		(o.Memory == "" && metricsType == autoscalingv2.UtilizationMetricType), nil
}

// handleHPA 处理单个 HPA 对象的创建和管理。
func (o *AutoscaleOptions) handleHPA(hpa runtime.Object) error {
	if err := o.Recorder.Record(hpa); err != nil {
		return fmt.Errorf("error recording current command: %w", err)
	}

	if o.dryRunStrategy == cmdutil.DryRunClient {
		printer, err := o.ToPrinter("created")
		if err != nil {
			return err
		}
		return printer.PrintObj(hpa, o.Out)
	}

	if err := util.CreateOrUpdateAnnotation(o.createAnnotation, hpa, scheme.DefaultJSONEncoder()); err != nil {
		return err
	}

	createOptions := metav1.CreateOptions{}
	if o.fieldManager != "" {
		createOptions.FieldManager = o.fieldManager
	}
	if o.dryRunStrategy == cmdutil.DryRunServer {
		createOptions.DryRun = []string{metav1.DryRunAll}
	}

	var actualHPA runtime.Object
	var err error
	switch typedHPA := hpa.(type) {
	case *autoscalingv2.HorizontalPodAutoscaler:
		actualHPA, err = o.HPAClientV2.HorizontalPodAutoscalers(o.namespace).Create(context.TODO(), typedHPA, createOptions)
	case *autoscalingv1.HorizontalPodAutoscaler:
		actualHPA, err = o.HPAClientV1.HorizontalPodAutoscalers(o.namespace).Create(context.TODO(), typedHPA, createOptions)
	default:
		return fmt.Errorf("unsupported HorizontalPodAutoscaler type %T", hpa)
	}
	if err != nil {
		return err
	}

	printer, err := o.ToPrinter("autoscaled")
	if err != nil {
		return err
	}
	return printer.PrintObj(actualHPA, o.Out)
}

func (o *AutoscaleOptions) createHorizontalPodAutoscalerV2(refName string, mapping *meta.RESTMapping) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	name := o.Name
	if len(name) == 0 {
		name = refName
	}

	scaler := autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: mapping.GroupVersionKind.GroupVersion().String(),
				Kind:       mapping.GroupVersionKind.Kind,
				Name:       refName,
			},
			MaxReplicas: o.Max,
		},
	}

	if o.Min > 0 {
		scaler.Spec.MinReplicas = &o.Min
	}

	if o.enforceNamespace {
		scaler.ObjectMeta.Namespace = o.namespace
	}

	metrics := []autoscalingv2.MetricSpec{}

	// 如果指定了任何 CPU 目标，则添加 CPU 指标
	if o.CPUPercent > 0 {
		cpuMetric := autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name:   corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{},
			},
		}
		cpuMetric.Resource.Target.Type = autoscalingv2.UtilizationMetricType
		cpuMetric.Resource.Target.AverageUtilization = &o.CPUPercent
		metrics = append(metrics, cpuMetric)
	}

	// 如果指定了任何 cpu 目标，则添加 Cpu 指标
	if o.CPU != "" {
		cpuMetric := autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name:   corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{},
			},
		}

		quantity, value, metricsType, err := parseResourceInput(o.CPU, corev1.ResourceCPU)
		if err != nil {
			return nil, err
		}
		switch metricsType {
		case autoscalingv2.UtilizationMetricType:
			cpuMetric.Resource.Target.Type = autoscalingv2.UtilizationMetricType
			cpuMetric.Resource.Target.AverageUtilization = &value
		case autoscalingv2.AverageValueMetricType:
			cpuMetric.Resource.Target.Type = autoscalingv2.AverageValueMetricType
			cpuMetric.Resource.Target.AverageValue = &quantity
		default:
			return nil, fmt.Errorf("unsupported metric type: %v", metricsType)
		}
		metrics = append(metrics, cpuMetric)
	}

	// 如果指定了任何 memory 目标，则添加 Memory 指标
	if o.Memory != "" {
		memoryMetric := autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name:   corev1.ResourceMemory,
				Target: autoscalingv2.MetricTarget{},
			},
		}
		quantity, value, metricsType, err := parseResourceInput(o.Memory, corev1.ResourceMemory)
		if err != nil {
			return nil, err
		}
		switch metricsType {
		case autoscalingv2.UtilizationMetricType:
			memoryMetric.Resource.Target.Type = autoscalingv2.UtilizationMetricType
			memoryMetric.Resource.Target.AverageUtilization = &value
		case autoscalingv2.AverageValueMetricType:
			memoryMetric.Resource.Target.Type = autoscalingv2.AverageValueMetricType
			memoryMetric.Resource.Target.AverageValue = &quantity
		default:
			return nil, fmt.Errorf("unsupported metric type: %v", metricsType)
		}
		metrics = append(metrics, memoryMetric)
	}

	// 仅在定义了指标时才设置 Metrics
	if len(metrics) > 0 {
		scaler.Spec.Metrics = metrics
	} else {
		scaler.Spec.Metrics = nil
	}

	return &scaler, nil
}

func (o *AutoscaleOptions) createHorizontalPodAutoscalerV1(refName string, mapping *meta.RESTMapping) *autoscalingv1.HorizontalPodAutoscaler {
	name := o.Name
	if len(name) == 0 {
		name = refName
	}

	scaler := autoscalingv1.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: autoscalingv1.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv1.CrossVersionObjectReference{
				APIVersion: mapping.GroupVersionKind.GroupVersion().String(),
				Kind:       mapping.GroupVersionKind.Kind,
				Name:       refName,
			},
			MaxReplicas: o.Max,
		},
	}

	if o.Min > 0 {
		v := int32(o.Min)
		scaler.Spec.MinReplicas = &v
	}
	if o.CPUPercent >= 0 {
		c := int32(o.CPUPercent)
		scaler.Spec.TargetCPUUtilizationPercentage = &c
	}

	if o.enforceNamespace {
		scaler.ObjectMeta.Namespace = o.namespace
	}

	return &scaler
}

// parseResourceInput 将资源输入字符串解析为利用率百分比或数量值。
// 它支持：
// - 百分比值（例如 "70%"），用于 UtilizationMetricType
// - 带单位的数量值（例如 "500m"、"2Gi"）
// - 不带单位的裸数字，其解释如下：
//   - CPU：milliCPU（"500" → "500m"）
//   - Memory：Mebibytes（"512" → "512Mi"）
func parseResourceInput(input string, resourceType corev1.ResourceName) (apiresource.Quantity, int32, autoscalingv2.MetricTargetType, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return apiresource.Quantity{}, 0, "", fmt.Errorf("empty input")
	}

	// 情况 1：处理类似 "70%" 的基于百分比的指标
	percentValue, isPercent, err := parsePercentage(input)
	if isPercent {
		if err != nil {
			return apiresource.Quantity{}, 0, "", err
		}
		return apiresource.Quantity{}, percentValue, autoscalingv2.UtilizationMetricType, nil
	}

	// 情况 2：尝试将输入解释为裸数字（例如 "500"），并应用默认单位
	valueFloat, err := strconv.ParseFloat(input, 64)
	if err == nil {
		unit, err := getDefaultUnitForResource(resourceType)
		if err != nil {
			return apiresource.Quantity{}, 0, "", err
		}

		inputWithUnit := fmt.Sprintf("%g%s", valueFloat, unit)
		quantity, err := apiresource.ParseQuantity(inputWithUnit)
		if err != nil {
			return apiresource.Quantity{}, 0, "", err
		}
		return quantity, 0, autoscalingv2.AverageValueMetricType, nil
	}

	// 情况 3：输入带有有效单位时按常规解析（例如 "500m"、"2Gi"）
	quantity, err := apiresource.ParseQuantity(input)
	if err != nil {
		return apiresource.Quantity{}, 0, "", fmt.Errorf("invalid resource %s value: %s", resourceType, input)
	}
	return quantity, 0, autoscalingv2.AverageValueMetricType, nil
}

func getDefaultUnitForResource(resourceType corev1.ResourceName) (string, error) {
	switch resourceType {
	case corev1.ResourceCPU:
		return "m", nil
	case corev1.ResourceMemory:
		return "Mi", nil
	default:
		return "", fmt.Errorf("unsupported resource type: %v", resourceType)
	}
}

func parsePercentage(input string) (int32, bool, error) {
	if !strings.HasSuffix(input, "%") {
		return 0, false, nil
	}
	trimmed := strings.TrimSuffix(input, "%")
	valueInt64, err := strconv.ParseInt(trimmed, 10, 32)
	if err != nil || valueInt64 < 0 {
		return 0, true, fmt.Errorf("invalid percentage value: %s", trimmed)
	}
	return int32(valueInt64), true, nil
}
