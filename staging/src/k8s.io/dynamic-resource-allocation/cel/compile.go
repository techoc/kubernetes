/*
Copyright 2022 The Kubernetes Authors.

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

// Package cel 实现 DRA（动态资源分配）中 CEL 选择器表达式与派生属性表达式的编译和求值。
package cel

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/ext"

	"k8s.io/utils/ptr"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/version"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/library"
)

// 本包在 CEL 表达式中暴露的预定义变量名，用于环境声明与求值输入绑定。
// deviceVar 是代表设备对象的根变量名。
// driverVar 是设备所属驱动的域名，同时作为无域名前缀属性的默认域。
// multiAllocVar 表示是否允许同一设备被多次分配的布尔字段名。
// attributesVar 是设备属性集合的字段名。
// capacityVar 是设备容量集合的字段名。
const (
	deviceVar     = "device"
	driverVar     = "driver"
	multiAllocVar = "allowMultipleAllocations"
	attributesVar = "attributes"
	capacityVar   = "capacity"
)

// 包级共享状态与预定义 CEL 类型（均带元素数上限约束，用于成本估算）。
// layzCompilerMutex 保护懒加载编译器的并发访问（名称中的拼写为历史遗留）。
// lazyCompiler 缓存最近一次按特性组合构建出的编译器。
// lazyFeatures 记录缓存编译器对应的特性组合，用于判断缓存是否失效。
// idType 表示属性或容量的 ID 字符串，最大长度为设备 ID 的上限。
// driverType 表示驱动名称字符串，最大长度为驱动名的上限。
var (
	layzCompilerMutex sync.Mutex
	lazyCompiler      *compiler
	lazyFeatures      Features

	domainType = withMaxElements(apiservercel.StringType, resourceapi.DeviceMaxDomainLength)
	idType     = withMaxElements(apiservercel.StringType, resourceapi.DeviceMaxIDLength)
	driverType = withMaxElements(apiservercel.StringType, resourceapi.DriverNameMaxLength)

	// multiAllocType 是带已知上限的布尔类型，避免默认 BoolType 低估成本。
	multiAllocType = withMaxElements(apiservercel.BoolType, 1)

	// 容量使用两层映射（域名 → ID → 数量），每层元素数上限均为单设备容量上限。
	innerCapacityMapType = apiservercel.NewMapType(idType, apiservercel.QuantityDeclType, resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice)
	// outerCapacityMapType 表示域名到内层容量映射的外层映射类型。
	outerCapacityMapType = apiservercel.NewMapType(domainType, innerCapacityMapType, resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice)
)

// Features 是特性开关的集合，决定表达式可见的字段与函数。
// Features 汇总本包支持的特性开关，决定表达式可见的字段与函数。
type Features struct {
	// EnableConsumableCapacity 控制是否暴露 device.allowMultipleAllocations 字段。
	EnableConsumableCapacity bool
	// EnableListTypeAttributes 控制属性是否按列表处理，并启用相应宏与 includes 函数。
	EnableListTypeAttributes bool
}

// GetCompiler 返回与给定特性组合对应的编译器，相同组合复用缓存实例。
func GetCompiler(features Features) *compiler {
	// 加锁保护懒加载缓存，避免并发调用重复创建编译器。
	layzCompilerMutex.Lock()
	defer layzCompilerMutex.Unlock()

	// 特性组合变化时才重建；实践中特性不会来回切换，因此只缓存一个实例。
	if lazyCompiler == nil || lazyFeatures != features {
		lazyCompiler = newCompiler(features)
		lazyFeatures = features
	}
	// 返回缓存的编译器。
	return lazyCompiler
}

// CompilationResult 是已编译表达式的封装，包含程序、类型、成本上限与错误。
// CompilationResult 保存一次编译的产物：可执行程序、类型信息、成本上限与可能的错误。
// Program 是编译得到的可执行 CEL 程序。
// Error 记录编译阶段（含成本估算）的错误分类与详情，成功时为 nil。
// Expression 保存原始表达式文本，便于诊断。
// OutputType 是表达式静态推导出的输出类型。
// Environment 是编译该表达式时使用的 CEL 环境。
type CompilationResult struct {
	Program     cel.Program
	Error       *apiservercel.Error
	Expression  string
	OutputType  *cel.Type
	Environment *cel.Env

	// MaxCost 以 CEL 成本单位表示表达式的最坏情况成本，供 cel.EstimateCost 使用；
	// 估算被禁用或失败时取 math.MaxUint64。
	MaxCost uint64

	// typesFromEnv 内嵌从环境解析出的属性类型信息，供尺寸估算与输出校验使用。
	typesFromEnv
	// emptyMapVal 是空映射的 CEL 值，作为未知键查找时的默认返回值。
	emptyMapVal ref.Val
}

// Device 是一次 CEL 求值所需的设备输入。
// Device 描述一次 CEL 求值所需的设备输入。
// Driver 是设备所属驱动的域名，未限定域名的属性使用它作为默认域；非空时也会作为字符串属性暴露给表达式。
// AllowMultipleAllocations 表示是否允许多次分配，nil 按 false 处理。
// Attributes 是设备属性集合，键为限定名（域名/ID 或裸 ID）。
// Capacity 是设备容量集合，值携带 Kubernetes quantity。
type Device struct {
	Driver                   string
	AllowMultipleAllocations *bool
	Attributes               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute
	Capacity                 map[resourceapi.QualifiedName]resourceapi.DeviceCapacity
}

// compiler 持有一套按特性扩展后的 CEL 环境及其声明的类型。
// envset 是按兼容版本与特性开关扩展得到的 CEL 环境集合。
// declaredTypes 收集本编译器声明的所有类型，用于把 cel.Type 反查为 DeclType。
// features 是创建本编译器时使用的特性组合。
type compiler struct {
	envset        *environment.EnvSet
	declaredTypes []*apiservercel.DeclType
	features      Features
}

// Options 是编译表达式时的可选参数集合。
// Options 为 CompileCELExpression 提供可选参数，零值即获得合理默认值。
type Options struct {
	// EnvType 指定使用存储表达式还是新表达式环境，默认 environment.StoredExpressions。
	EnvType *environment.Type

	// CostLimit 覆盖运行时成本上限，默认 resourceapi.CELSelectorExpressionMaxCost。
	CostLimit *uint64

	// DisableCostEstimation 为 true 时跳过高成本估算，MaxCost 置为 math.MaxUint64。
	DisableCostEstimation bool

	// DerivedAttribute 表示表达式用于派生属性，输出类型校验规则与选择器不同。
	DerivedAttribute bool
}

// TODO（https://github.com/kubernetes/kubernetes/issues/125826）：校验 AST，检测无效属性名。
// CompileCELExpression 编译表达式并返回封装结果，错误通过结果中的 Error 分类表达。
// CompileCELExpression 按 options 编译表达式并返回封装结果，错误通过结果中的 Error 分类表达。
func (c compiler) CompileCELExpression(expression string, options Options) CompilationResult {
	// resultError 统一构造失败结果：归类错误、记录表达式并把成本上限设为最大值。
	resultError := func(errorString string, errType apiservercel.ErrorType) CompilationResult {
		return CompilationResult{
			Error: &apiservercel.Error{
				Type:   errType,
				Detail: errorString,
			},
			Expression: expression,
			MaxCost:    math.MaxUint64,
		}
	}

	// 默认使用存储表达式环境，再按 options 覆盖。
	envType := ptr.Deref(options.EnvType, environment.StoredExpressions)
	// 从环境集合中取出对应兼容版本的 CEL 环境。
	env, err := c.envset.Env(envType)
	// 环境加载失败属于内部错误。
	if err != nil {
		return resultError(fmt.Sprintf("unexpected error loading CEL environment: %v", err), apiservercel.ErrorTypeInternal)
	}

	// 编译为 AST；任何 issue 都视为表达式非法。
	ast, issues := env.Compile(expression)
	if issues != nil {
		return resultError("compilation failed: "+issues.String(), apiservercel.ErrorTypeInvalid)
	}

	// 从实际环境中解析属性各层的类型，供估算与校验使用。
	typesFromEnv, err := c.getTypesFromEnv(env)
	if err != nil {
		return resultError("unexpected error loading CEL environment: "+err.Error(), apiservercel.ErrorTypeInternal)
	}

	// 校验输出类型是否符合选择器或派生属性的约束。
	if outputTypeErr := c.validateOutputType(typesFromEnv, ast.OutputType(), envType, options.DerivedAttribute); outputTypeErr != nil {
		return resultError(outputTypeErr.Detail, outputTypeErr.Type)
	}

	// AST 转 CheckedExpr 不应失败（编译阶段已无 issue）。
	_, err = cel.AstToCheckedExpr(ast)
	if err != nil {
		return resultError("unexpected compilation error: "+err.Error(), apiservercel.ErrorTypeInternal)
	}
	// 实例化可执行程序，注入 DRA 自己的成本上限与中断检查频率。
	prog, err := env.Program(ast,
		cel.CostLimit(ptr.Deref(options.CostLimit, resourceapi.CELSelectorExpressionMaxCost)),
		cel.InterruptCheckFrequency(celconfig.CheckFrequency),
	)
	if err != nil {
		return resultError("program instantiation failed: "+err.Error(), apiservercel.ErrorTypeInternal)
	}

	// 组装成功结果；此时成本未知，先置为最大值。
	// 空映射值用于访问不存在的属性域时返回空映射，避免 nil。
	compilationResult := CompilationResult{
		Program:      prog,
		Expression:   expression,
		OutputType:   ast.OutputType(),
		Environment:  env,
		emptyMapVal:  env.CELTypeAdapter().NativeToValue(map[string]any{}),
		MaxCost:      math.MaxUint64,
		typesFromEnv: typesFromEnv,
	}

	// 除非调用方禁用，否则估算表达式的最坏情况成本。
	if !options.DisableCostEstimation {
		// 不需要额外的 SizeEstimator：输入设备的潜在大小已在环境类型中声明。
		estimator := c.newCostEstimator(typesFromEnv)
		// 使用带尺寸估算器的成本估算器进行估算。
		costEst, err := env.EstimateCost(ast, estimator)
		// 估算失败记为内部错误，但保留已编译的程序和其余结果字段。
		if err != nil {
			compilationResult.Error = &apiservercel.Error{Type: apiservercel.ErrorTypeInternal, Detail: "cost estimation failed: " + err.Error()}
			return compilationResult
		}
		// 记录估算出的最大成本。
		compilationResult.MaxCost = costEst.Max
	}

	return compilationResult
}

// newCostEstimator 构造基于环境类型声明的 CEL 成本估算器。
func (c *compiler) newCostEstimator(typesFromEnv typesFromEnv) checker.CostEstimator {
	// 复用 apiserver 的通用实现，仅注入本包的尺寸估算逻辑。
	return &library.CostEstimator{SizeEstimator: &sizeEstimator{typesFromEnv}}
}

// typesFromEnv 记录属性映射各层的实际 DeclType，来自当前生效的 CEL 环境。
// deviceType 对应根变量 device 的对象类型。
// outerAttributesType 对应 device.attributes，即域名到属性映射的映射。
// innerAttributesType 对应 device.attributes[域名]，即 ID 到属性值的映射。
// attributeType 对应属性叶子的值类型。
type typesFromEnv struct {
	deviceType          *apiservercel.DeclType // "device"
	outerAttributesType *apiservercel.DeclType // "device.attributes" = map to map to value
	innerAttributesType *apiservercel.DeclType // "device.attributes[foo]" = map to value
	attributeType       *apiservercel.DeclType // "device.attributes[foo].bar" = value
}

// getTypesFromEnv 从环境解析属性各层类型，结果取决于特性与表达式版本。
// getTypesFromEnv 从环境中逐层解析出属性相关类型，结果取决于特性开关与表达式版本。
func (c *compiler) getTypesFromEnv(env *cel.Env) (typesFromEnv, error) {
	// 解析顺序：device → attributes → 内层映射 → 属性值。
	var result typesFromEnv

	// 同名变量可能被多次声明，反向遍历以取最后一个生效的声明。
	for _, variable := range slices.Backward(env.Variables()) {
		if variable.Name() == deviceVar {
			result.deviceType = c.getDeclType(variable.Type())
			break
		}
	}
	// 未声明 device 属于环境定义异常。
	if result.deviceType == nil {
		return result, fmt.Errorf("%q variable is not declared", deviceVar)
	}

	// 通过结构化字段查找定位 attributes 字段的类型。
	fieldType, ok := env.CELTypeProvider().FindStructFieldType(result.deviceType.TypeName(), attributesVar)
	if !ok {
		return result, fmt.Errorf("%q variable does not declare %q field", deviceVar, attributesVar)
	}
	result.outerAttributesType = c.getDeclType(fieldType.Type)
	// 字段类型缺失属于环境定义错误。
	if result.outerAttributesType == nil {
		return result, fmt.Errorf("%q field has no type", attributesVar)
	}

	// 外层必须是带两个类型参数的映射。
	outerMapParams := result.outerAttributesType.CelType().Parameters()
	if len(outerMapParams) != 2 {
		return result, fmt.Errorf("%q field should be a map, got %s", attributesVar, result.outerAttributesType.String())
	}
	// 取出外层映射的值类型，即内层属性映射。
	result.innerAttributesType = c.getDeclType(outerMapParams[1])
	if result.innerAttributesType == nil {
		return result, fmt.Errorf("%q field does not map to type", attributesVar)
	}

	// 内层同样必须是映射。
	innerMapParams := result.innerAttributesType.CelType().Parameters()
	if len(innerMapParams) != 2 {
		return result, fmt.Errorf("%q field values should be maps, got %s", attributesVar, result.innerAttributesType.String())
	}
	// 取出内层映射的值类型，即属性叶子类型。
	result.attributeType = c.getDeclType(innerMapParams[1])
	if result.attributeType == nil {
		return result, fmt.Errorf("%q field mapped to no type at leafs", attributesVar)
	}

	return result, nil
}

// getDeclType 按 cel.Type 反查本编译器声明的 DeclType。
// getDeclType 在编译器声明的类型中查找与给定 cel.Type 对应的 DeclType。
func (c *compiler) getDeclType(t *cel.Type) *apiservercel.DeclType {
	// 只能识别 newCompiler 自身声明的类型。
	for _, dt := range c.declaredTypes {
		if dt.CelType() == t {
			return dt
		}
	}
	return nil
}

// getAttributeValue 把设备属性转换为 CEL 可用的原生 Go 值。
// getAttributeValue 把 DeviceAttribute 的取值转换为 CEL 可直接使用的原生 Go 值。
func (c CompilationResult) getAttributeValue(attr resourceapi.DeviceAttribute) (any, error) {
	// 按实际设置的字段分支；多值字段优先于标量字段。
	switch {
	case attr.IntValues != nil:
		return attr.IntValues, nil
	case attr.BoolValues != nil:
		return attr.BoolValues, nil
	case attr.StringValues != nil:
		return attr.StringValues, nil
	case attr.VersionValues != nil:
		// 版本列表需逐个解析为 semver 类型。
		semVers := make([]apiservercel.Semver, len(attr.VersionValues))
		for i, versionStr := range attr.VersionValues {
			v, err := semver.Parse(versionStr)
			if err != nil {
				return nil, fmt.Errorf("parse semantic version: %w", err)
			}
			semVers[i] = apiservercel.Semver{Version: v}
		}
		return semVers, nil
	// 以下为标量取值分支。
	case attr.IntValue != nil:
		return *attr.IntValue, nil
	case attr.BoolValue != nil:
		return *attr.BoolValue, nil
	case attr.StringValue != nil:
		return *attr.StringValue, nil
	// 标量版本值解析为单个 semver。
	case attr.VersionValue != nil:
		v, err := semver.Parse(*attr.VersionValue)
		if err != nil {
			return nil, fmt.Errorf("parse semantic version: %w", err)
		}
		return apiservercel.Semver{Version: v}, nil
	// 未设置任何受支持的值则报错。
	default:
		return nil, errors.New("unsupported attribute value")
	}
}

// boolType 是把 CEL 结果转换为原生值时使用的目标类型。
var boolType = reflect.TypeOf(true)

// DeviceMatches 以给定设备求值已编译的选择器表达式，返回是否匹配、求值细节与错误。
func (c CompilationResult) DeviceMatches(ctx context.Context, input Device) (bool, *cel.EvalDetails, error) {
	// TODO（后续）：避免构建这些映射，改用代理。
	attributes := make(map[string]any)
	// 遍历设备属性，把 API 表示转换为 domain → id → 值 的两层映射。
	for name, attr := range input.Attributes {
		value, err := c.getAttributeValue(attr)
		if err != nil {
			return false, nil, fmt.Errorf("attribute %s: %w", name, err)
		}
		// 按限定名拆出域名与 ID，缺省域名用驱动域名补齐。
		domain, id := parseQualifiedName(name, input.Driver)
		// 域名首次出现时创建内层映射。
		if attributes[domain] == nil {
			attributes[domain] = make(map[string]any)
		}
		attributes[domain].(map[string]any)[id] = value
	}

	// 容量同样按域名分组，值包装为 apiservercel.Quantity。
	capacity := make(map[string]any)
	for name, cap := range input.Capacity {
		domain, id := parseQualifiedName(name, input.Driver)
		if capacity[domain] == nil {
			capacity[domain] = make(map[string]apiservercel.Quantity)
		}
		capacity[domain].(map[string]apiservercel.Quantity)[id] = apiservercel.Quantity{Quantity: &cap.Value}
	}

	// 组装 CEL 求值变量：只暴露 device 根变量。
	// 多次分配标志未设置时按 false 参与求值。
	// 属性与容量映射使用未知键返回空映射的包装，避免 nil。
	variables := map[string]any{
		deviceVar: map[string]any{
			driverVar:     input.Driver,
			multiAllocVar: ptr.Deref(input.AllowMultipleAllocations, false),
			attributesVar: newStringInterfaceMapWithDefault(c.Environment.CELTypeAdapter(), attributes, c.emptyMapVal),
			capacityVar:   newStringInterfaceMapWithDefault(c.Environment.CELTypeAdapter(), capacity, c.emptyMapVal),
		},
	}

	// 执行带上下文的求值，支持超时与取消中断。
	result, details, err := c.Program.ContextEval(ctx, variables)
	// CEL 不会包装上下文错误，需要自行推断中断原因。
	// 参见 https://github.com/google/cel-go/issues/1195。
	if err != nil {
		// 报错含 "operation interrupted" 且上下文已结束时，还原真实的上下文原因。
		if strings.Contains(err.Error(), "operation interrupted") && ctx.Err() != nil {
			return false, details, fmt.Errorf("%w: %w", err, context.Cause(ctx))
		}
		return false, details, err
	}
	// 把 CEL 结果转换为原生 bool。
	resultAny, err := result.ConvertToNative(boolType)
	if err != nil {
		return false, details, fmt.Errorf("CEL result of type %s could not be converted to bool: %w", result.Type().TypeName(), err)
	}
	// 二次断言确认原生结果确实是 bool。
	resultBool, ok := resultAny.(bool)
	if !ok {
		return false, details, fmt.Errorf("CEL native result value should have been a bool, got instead: %T", resultAny)
	}
	return resultBool, details, nil
}

// EvaluateDerivedAttribute 求值派生属性表达式并返回 DeviceAttribute。
// EvaluateDerivedAttribute 以给定设备求值派生属性表达式，并把结果转换为 DeviceAttribute。
func (c CompilationResult) EvaluateDerivedAttribute(ctx context.Context, input Device) (*resourceapi.DeviceAttribute, *cel.EvalDetails, error) {
	// 构造属性映射，转换逻辑与 DeviceMatches 相同。
	attributes := make(map[string]any)
	for name, attr := range input.Attributes {
		value, err := c.getAttributeValue(attr)
		if err != nil {
			return nil, nil, fmt.Errorf("attribute %s: %w", name, err)
		}
		domain, id := parseQualifiedName(name, input.Driver)
		if attributes[domain] == nil {
			attributes[domain] = make(map[string]any)
		}
		attributes[domain].(map[string]any)[id] = value
	}

	// 构造容量映射。
	capacity := make(map[string]any)
	for name, cap := range input.Capacity {
		domain, id := parseQualifiedName(name, input.Driver)
		if capacity[domain] == nil {
			capacity[domain] = make(map[string]apiservercel.Quantity)
		}
		capacity[domain].(map[string]apiservercel.Quantity)[id] = apiservercel.Quantity{Quantity: &cap.Value}
	}

	// 组装 device 变量并绑定两层映射。
	variables := map[string]any{
		deviceVar: map[string]any{
			driverVar:     input.Driver,
			multiAllocVar: ptr.Deref(input.AllowMultipleAllocations, false),
			attributesVar: newStringInterfaceMapWithDefault(c.Environment.CELTypeAdapter(), attributes, c.emptyMapVal),
			capacityVar:   newStringInterfaceMapWithDefault(c.Environment.CELTypeAdapter(), capacity, c.emptyMapVal),
		},
	}

	// 执行求值并识别上下文中断。
	result, details, err := c.Program.ContextEval(ctx, variables)
	if err != nil {
		if strings.Contains(err.Error(), "operation interrupted") && ctx.Err() != nil {
			return nil, details, fmt.Errorf("%w: %w", err, context.Cause(ctx))
		}
		return nil, details, err
	}

	// 将 CEL 返回值转换为 API 的 DeviceAttribute 表示。
	attr, err := valToDeviceAttribute(result)
	if err != nil {
		return nil, details, err
	}
	return attr, details, nil
}

// valToDeviceAttribute 把 CEL 结果转换为 DeviceAttribute：标量直接转换，列表按元素类型转成多值字段。
func valToDeviceAttribute(val ref.Val) (*resourceapi.DeviceAttribute, error) {
	// 先处理标量：布尔、整数、字符串与 semver。
	switch v := val.Value().(type) {
	case bool:
		return &resourceapi.DeviceAttribute{BoolValue: &v}, nil
	case int64:
		return &resourceapi.DeviceAttribute{IntValue: &v}, nil
	case string:
		return &resourceapi.DeviceAttribute{StringValue: &v}, nil
	// semver 需格式化回字符串存入 VersionValue。
	case semver.Version:
		s := v.String()
		return &resourceapi.DeviceAttribute{VersionValue: &s}, nil
	}

	// 非标量时尝试按 CEL 列表（traits.Lister）处理。
	if lister, ok := val.(traits.Lister); ok {
		// 列表大小在 CEL 中是 int64。
		sz, ok := lister.Size().Value().(int64)
		if !ok {
			return nil, fmt.Errorf("expected list size to be int64, got %T", lister.Size().Value())
		}

		// CEL 列表大小是 int64，而 Go 切片长度与索引必须是 int。
		// 实际 CEL 列表不会超过 32 位 int 上限，转成 int 可简化切片分配与 vals[i] 访问；
		// lister.Get() 仍需要使用 cel.Types.Int(i)。
		length := int(sz)
		// 空列表没有元素类型信息，约定返回空字符串列表。
		if length == 0 {
			return &resourceapi.DeviceAttribute{StringValues: []string{}}, nil
		}

		// 用首元素判定整个列表的元素类型。
		first := lister.Get(types.Int(0))
		// 各分支逐项读取并校验类型一致性。
		switch first.Value().(type) {
		case bool:
			vals := make([]bool, length)
			for i := range length {
				elem := lister.Get(types.Int(i))
				b, ok := elem.Value().(bool)
				if !ok {
					return nil, fmt.Errorf("expected list element at index %d to be bool, got %T", i, elem.Value())
				}
				vals[i] = b
			}
			return &resourceapi.DeviceAttribute{BoolValues: vals}, nil

		case int64:
			vals := make([]int64, length)
			for i := range length {
				elem := lister.Get(types.Int(i))
				v, ok := elem.Value().(int64)
				if !ok {
					return nil, fmt.Errorf("expected list element at index %d to be int, got %T", i, elem.Value())
				}
				vals[i] = v
			}
			return &resourceapi.DeviceAttribute{IntValues: vals}, nil

		case string:
			vals := make([]string, length)
			for i := range length {
				elem := lister.Get(types.Int(i))
				s, ok := elem.Value().(string)
				if !ok {
					return nil, fmt.Errorf("expected list element at index %d to be string, got %T", i, elem.Value())
				}
				vals[i] = s
			}
			return &resourceapi.DeviceAttribute{StringValues: vals}, nil

		// semver 列表逐项格式化为字符串。
		case semver.Version:
			vals := make([]string, length)
			for i := range length {
				elem := lister.Get(types.Int(i))
				v, ok := elem.Value().(semver.Version)
				if !ok {
					return nil, fmt.Errorf("expected list element at index %d to be semver, got %T", i, elem.Value())
				}
				vals[i] = v.String()
			}
			return &resourceapi.DeviceAttribute{VersionValues: vals}, nil

		// 首元素类型不受支持。
		default:
			return nil, fmt.Errorf("unsupported list element type: %T", first.Value())
		}
	}

	// 既不是标量也不是列表。
	return nil, fmt.Errorf("unsupported CEL return type: %T", val.Value())
}

// newCompiler 基于特性开关构建 CEL 环境集合，声明各版本 device 类型并收集声明类型。
func newCompiler(features Features) *compiler {
	// 从 Kubernetes 默认兼容版本的基础环境集开始。
	envset := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion())
	// field 简化必填字段的声明。
	field := func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField {
		return apiservercel.NewDeclField(name, declType, required, nil, nil)
	}
	// fields 把字段列表按名字组装成 map。
	fields := func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField {
		result := make(map[string]*apiservercel.DeclField, len(fields))
		for _, f := range fields {
			result[f.Name] = f
		}
		return result
	}

	// declaredTypes 收集环境中声明的所有类型。
	var declaredTypes []*apiservercel.DeclType
	// declareType 在声明的同时记录类型，便于之后按 cel.Type 反查。
	declareType := func(t *apiservercel.DeclType) *apiservercel.DeclType {
		declaredTypes = append(declaredTypes, t)
		return t
	}

	// 1.31 的属性叶子是 Any 类型，最大长度受属性值长度限制。
	attributeTypeV131 := declareType(withMaxElements(
		apiservercel.AnyType,
		resourceapi.DeviceAttributeMaxValueLength,
	))
	// 启用列表属性后改用 DynType，使 exists/all 等迭代函数可用。
	attributeTypeV136ListTypeAttributes := declareType(withMaxElements(
		apiservercel.DynType,
		// 编译时无法预知属性是标量还是列表，取两者上限作为元素数上限。
		uint64(max(resourceapi.DeviceAttributeMaxValueLength, resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
	))
	// 每层映射的元素数上限为单设备允许的属性/容量总数。
	innerAttributesMapTypeV131 := declareType(apiservercel.NewMapType(idType, attributeTypeV131, resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice))
	outerAttributesMapTypeV131 := declareType(apiservercel.NewMapType(domainType, innerAttributesMapTypeV131, resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice))
	innerAttributesMapTypeV136ListTypeAttributes := declareType(apiservercel.NewMapType(
		idType, attributeTypeV136ListTypeAttributes, resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice,
	))
	outerAttributesMapTypeV136ListTypeAttributes := declareType(apiservercel.NewMapType(
		domainType, innerAttributesMapTypeV136ListTypeAttributes, resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice,
	))

	// 1.31 版本的 device 字段集合。
	fieldsV131 := []*apiservercel.DeclField{
		field(driverVar, driverType, true),
		field(attributesVar, outerAttributesMapTypeV131, true),
		field(capacityVar, outerCapacityMapType, true),
	}
	// 声明 1.31 的 device 对象类型。
	deviceTypeV131 := declareType(apiservercel.NewObjectType("kubernetes.DRADevice", fields(fieldsV131...)))

	// 1.34 起新增 allowMultipleAllocations 字段。
	fieldsV134ConsumableCapacity := []*apiservercel.DeclField{field(multiAllocVar, multiAllocType, true)}
	fieldsV134ConsumableCapacity = append(fieldsV134ConsumableCapacity, fieldsV131...)
	// 声明包含可分配容量字段的 device 类型。
	deviceTypeV134ConsumableCapacity := declareType(apiservercel.NewObjectType("kubernetes.DRADevice", fields(fieldsV134ConsumableCapacity...)))

	// 1.36 启用列表属性时的字段集合（不含 allowMultipleAllocations）。
	fieldsV136ListTypeAttributes := []*apiservercel.DeclField{
		field(driverVar, driverType, true),
		field(attributesVar, outerAttributesMapTypeV136ListTypeAttributes, true),
		field(capacityVar, outerCapacityMapType, true),
	}
	// 声明仅启用列表属性的 device 类型。
	deviceTypeV136ListTypeAttributes := declareType(apiservercel.NewObjectType("kubernetes.DRADevice", fields(fieldsV136ListTypeAttributes...)))

	// 同时启用列表属性与可分配容量时的字段集合。
	fieldsV136ConsumableCapacityListTypeAttributes := []*apiservercel.DeclField{field(multiAllocVar, multiAllocType, true)}
	fieldsV136ConsumableCapacityListTypeAttributes = append(fieldsV136ConsumableCapacityListTypeAttributes, fieldsV136ListTypeAttributes...)
	// 声明同时启用两个特性的 device 类型。
	deviceTypeV136ConsumableCapacityListTypeAttributes := declareType(apiservercel.NewObjectType("kubernetes.DRADevice", fields(fieldsV136ConsumableCapacityListTypeAttributes...)))

	// versioned 是按引入版本与 FeatureEnabled 条件组织的版本化环境选项。
	versioned := []environment.VersionedOptions{
		// 1.31 起启用 cel.bind，简化同一域下属性的连续访问。
		{
			IntroducedVersion: version.MajorMinor(1, 31),
			EnvOptions: []cel.EnvOption{
				// 注册 Bindings 扩展；同一域的属性只需给出一次域名，便于连续访问。
				// https://pkg.go.dev/github.com/google/cel-go/ext#Bindings
				// 示例：cel.bind(dra, device.attributes["dra.example.com"], dra.oneBool && dra.anotherBool)
				ext.Bindings(ext.BindingsVersion(0)),
			},
		},
		// 按特性组合选择对应的 device 类型声明，顺序必须由旧到新。
		// 第 2 项：1.31 默认组合，两个特性均关闭。
		{
			IntroducedVersion: version.MajorMinor(1, 31),
			FeatureEnabled: func() bool {
				return !features.EnableConsumableCapacity && !features.EnableListTypeAttributes
			},
			EnvOptions: []cel.EnvOption{
				cel.Variable(deviceVar, deviceTypeV131.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				deviceTypeV131,
			},
		},
		// 第 3 项：1.34 起仅启用可分配容量。
		{
			IntroducedVersion: version.MajorMinor(1, 34),
			FeatureEnabled: func() bool {
				return features.EnableConsumableCapacity && !features.EnableListTypeAttributes
			},
			EnvOptions: []cel.EnvOption{
				cel.Variable(deviceVar, deviceTypeV134ConsumableCapacity.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				deviceTypeV134ConsumableCapacity,
			},
		},
		// 第 4 项：1.36 起仅启用列表属性（对应类型在 1.37 正式新增）。
		{
			IntroducedVersion: version.MajorMinor(1, 36),
			FeatureEnabled: func() bool {
				return !features.EnableConsumableCapacity && features.EnableListTypeAttributes
			},
			EnvOptions: []cel.EnvOption{
				cel.Variable(deviceVar, deviceTypeV136ListTypeAttributes.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				deviceTypeV136ListTypeAttributes,
			},
		},
		// 第 5 项：1.36 起同时启用两个特性。
		{
			IntroducedVersion: version.MajorMinor(1, 36),
			FeatureEnabled: func() bool {
				return features.EnableConsumableCapacity && features.EnableListTypeAttributes
			},
			EnvOptions: []cel.EnvOption{
				cel.Variable(deviceVar, deviceTypeV136ConsumableCapacityListTypeAttributes.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				deviceTypeV136ConsumableCapacityListTypeAttributes,
			},
		},
		// 1.36 起启用列表属性时注册 includes 扩展函数。
		{
			IntroducedVersion: version.MajorMinor(1, 36),
			FeatureEnabled: func() bool {
				return features.EnableListTypeAttributes
			},
			EnvOptions: []cel.EnvOption{
				// TODO（https://github.com/kubernetes/kubernetes/issues/140074）：在 1.38 移除；届时已放弃 1.36 支持，且从 1.37 起 Lists(ListsVersion(1)) 已原生提供 includes。
				library.IncludesOption(),
			},
		},
	}
	// 用版本化选项扩展基础环境集。
	envset, err := envset.Extend(versioned...)
	// 构建失败说明环境定义有内部错误，直接 panic。
	if err != nil {
		panic(fmt.Errorf("internal error building CEL environment: %w", err))
	}

	// 特性仍可在使用前变更，FeatureEnabled 保证环境按需配置；编译器同时支持存储表达式与新表达式。
	return &compiler{
		// 需要知道属性实际类型的代码必须从当前生效的环境中查询。
		envset:        envset,
		declaredTypes: declaredTypes,
		features:      features,
	}
}

// withMaxElements 复制 DeclType 并设置元素数上限，用于成本估算中的尺寸约束。
func withMaxElements(in *apiservercel.DeclType, maxElements uint64) *apiservercel.DeclType {
	// 复制而非原地修改，避免污染基础环境中共享的类型定义。
	out := *in
	out.MaxElements = int64(maxElements)
	return &out
}

// parseQualifiedName 把限定名拆成域名与 ID，无域名时用默认域名。
// parseQualifiedName 把限定名拆成域名与 ID，没有域名时使用默认域名。
func parseQualifiedName(name resourceapi.QualifiedName, defaultDomain string) (string, string) {
	// 以第一个 "/" 作为域名与 ID 的分隔符。
	sep := strings.Index(string(name), "/")
	// 没有分隔符时整串当作 ID，域名取默认值。
	if sep == -1 {
		return defaultDomain, string(name)
	}
	return string(name[0:sep]), string(name[sep+1:])
}

// newStringInterfaceMapWithDefault 包装映射，使未知键查找返回默认值。
// newStringInterfaceMapWithDefault 类似
// https://pkg.go.dev/github.com/google/cel-go@v0.20.1/common/types#NewStringInterfaceMap，
// 但查找未知键时返回默认值。
// newStringInterfaceMapWithDefault 包装 CEL 字符串映射，使未知键查找返回默认值而非 "not found"。
func newStringInterfaceMapWithDefault(adapter types.Adapter, value map[string]any, defaultValue ref.Val) traits.Mapper {
	// 复用标准映射实现，只覆盖 Find 行为。
	return mapper{
		Mapper:       types.NewStringInterfaceMap(adapter, value),
		defaultValue: defaultValue,
	}
}

// mapper 在标准 CEL 映射之上改写 Find：查不到键时返回预设的空映射。
type mapper struct {
	// Mapper 是被包装的标准 CEL 映射。
	traits.Mapper
	// defaultValue 是键不存在时返回的 CEL 值，通常是空映射。
	defaultValue ref.Val
}

// Find 在查找失败时返回默认空映射，并始终报告"找到"。
// Find 在底层查找失败时返回默认空映射，并始终报告"找到"以避免表达式因缺键而报错。
func (m mapper) Find(key ref.Val) (ref.Val, bool) {
	// 先按标准映射查找。
	value, found := m.Mapper.Find(key)
	if found {
		return value, true
	}

	// 未找到时返回默认空映射并视作查找成功。
	return m.defaultValue, true
}

// sizeEstimator 根据环境类型声明提供 device 可达值的最大尺寸估计。
// sizeEstimator 根据环境声明的类型，为成本估算器提供经 device 可见的映射/字符串/列表的最大尺寸。
type sizeEstimator struct {
	// typesFromEnv 提供从环境解析出的各层属性类型。
	typesFromEnv
}

// EstimateSize 沿 AST 节点路径遍历类型声明，返回该节点的最大尺寸估计。
func (s *sizeEstimator) EstimateSize(element checker.AstNode) (res *checker.SizeEstimate) {
	// 路径是相对根变量的字段/操作序列。
	path := element.Path()
	// 空路径无法估算，直接放弃。
	if len(path) == 0 {
		return nil
	}

	// 只识别 device 这一个根变量。
	var currentNode *apiservercel.DeclType
	// 按路径首段定位根类型。
	switch path[0] {
	case deviceVar:
		currentNode = s.deviceType
	default:
		return nil
	}

	// 逐段沿类型声明向下走。
	for _, name := range path[1:] {
		switch name {
		// 遍历集合的元素类型。
		case "@items", "@values":
			if currentNode.ElemType == nil {
				return nil
			}
			currentNode = currentNode.ElemType
		// 遍历映射的键类型。
		case "@keys":
			if currentNode.KeyType == nil {
				return nil
			}
			currentNode = currentNode.KeyType
		// 其余段按结构体字段名查找。
		default:
			field, ok := currentNode.Fields[name]
			if !ok {
				// 属性映射中所有元素共享相同最大尺寸，与具体键名无关。
				if currentNode.ElemType == s.attributeType {
					currentNode = s.attributeType
					continue
				}
				return nil
			}
			// 字段类型缺失则放弃估算。
			if field.Type == nil {
				return nil
			}
			currentNode = field.Type
		}
	}
	// 最小尺寸为 0，最大尺寸取声明的 MaxElements。
	return &checker.SizeEstimate{Min: 0, Max: uint64(currentNode.MaxElements)}
}

// EstimateCallCost 不对函数调用给出特殊成本估计，交由默认逻辑处理。
func (s *sizeEstimator) EstimateCallCost(function, overloadID string, target *checker.AstNode, args []checker.AstNode) *checker.CallEstimate {
	// 返回 nil 表示没有额外估计。
	return nil
}

// validateOutputType 校验表达式输出类型是否符合派生属性或选择器的约束。
// validateOutputType 校验输出类型：派生属性必须是标量或标量列表，选择器必须是布尔或属性类型。
func (c compiler) validateOutputType(types typesFromEnv, t *cel.Type, envType environment.Type, isDerivedAttribute bool) *apiservercel.Error {
	// 派生属性需能被调度器用相等或不相交比较处理，因此限制为标量。
	// 存储表达式即使特性关闭也允许列表，避免升级后旧对象失效。
	if isDerivedAttribute {
		allowListType := c.features.EnableListTypeAttributes || envType == environment.StoredExpressions
		// 类型合法则通过。
		if c.isAllowedDerivedAttributeType(t, allowListType) {
			return nil
		}
		// 按是否允许列表生成不同的错误提示。
		detail := "must evaluate to a primitive scalar (string, integer, boolean, semver), not %v"
		if allowListType {
			detail = "must evaluate to a primitive scalar (string, integer, boolean, semver) or a list of these scalars, not %v"
		}
		// 类型非法：归类为 Invalid。
		return &apiservercel.Error{
			Type:   apiservercel.ErrorTypeInvalid,
			Detail: fmt.Sprintf(detail, t.String()),
		}
	}

	// 选择器表达式允许布尔或属性类型。
	expectedReturnType := cel.BoolType
	// 精确匹配其一即通过。
	if t.IsExactType(expectedReturnType) || t.IsExactType(types.attributeType.CelType()) {
		return nil
	}
	// 其余类型非法。
	return &apiservercel.Error{
		Type:   apiservercel.ErrorTypeInvalid,
		Detail: fmt.Sprintf("must evaluate to %v or the unknown type, not %v", expectedReturnType.String(), t.String()),
	}
}

// isAllowedDerivedAttributeType 判断类型是否为合法的派生属性类型（标量，或允许时的标量列表）。
func (c compiler) isAllowedDerivedAttributeType(t *cel.Type, allowListType bool) bool {
	// 标量直接判定通过。
	if c.isAllowedDerivedAttributeElementType(t) {
		return true
	}
	// CEL list 是只带一个类型参数的泛型，确认形状后再检查元素类型。
	if allowListType && t.TypeName() == "list" && len(t.Parameters()) == 1 {
		// 递归检查列表元素类型是否合法。
		return c.isAllowedDerivedAttributeElementType(t.Parameters()[0])
	}
	return false
}

// isAllowedDerivedAttributeElementType 判断类型是否是允许作为派生属性（或列表元素）的标量类型。
func (c compiler) isAllowedDerivedAttributeElementType(t *cel.Type) bool {
	// 允许布尔、整数、字符串、any、dyn 与 semver。
	return t.IsExactType(cel.BoolType) ||
		t.IsExactType(cel.IntType) ||
		t.IsExactType(cel.StringType) ||
		t.IsExactType(cel.AnyType) ||
		t.IsExactType(cel.DynType) ||
		t.IsExactType(apiservercel.SemverType)
}
