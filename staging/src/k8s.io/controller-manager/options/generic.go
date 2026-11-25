/*
Copyright 2018 The Kubernetes Authors.

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

package options

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/config/options"
	cmconfig "k8s.io/controller-manager/config"
	migration "k8s.io/controller-manager/pkg/leadermigration/options"
)

// GenericControllerManagerConfigurationOptions 持有通用的选项。
// 该结构聚合了 controller-manager 的“通用”命令行选项：
// 通用配置、调试选项以及领导者迁移选项。KCM 与 CCM 共享这部分定义。
type GenericControllerManagerConfigurationOptions struct {
	*cmconfig.GenericControllerManagerConfiguration
	Debugging *DebuggingOptions
	// LeaderMigration 是领导者迁移选项；为 nil 表示应应用默认选项。
	// nil 表示走默认配置（即关闭迁移）。
	LeaderMigration *migration.LeaderMigrationOptions
}

// NewGenericControllerManagerConfigurationOptions 为 kube-controller-manager 和
// cloud-controller-manager 返回通用的配置默认值。
// 任何公共修改都应放在这里；各个控制器的个性化修改放在各自组件中。
// 基于传入的通用配置对象创建选项，并填入推荐的默认值。
// 两个 controller-manager 的公共修改都应放在这里，个性化修改放在各自的组件里。
func NewGenericControllerManagerConfigurationOptions(cfg *cmconfig.GenericControllerManagerConfiguration) *GenericControllerManagerConfigurationOptions {
	o := &GenericControllerManagerConfigurationOptions{
		GenericControllerManagerConfiguration: cfg,
		Debugging:                             RecommendedDebuggingOptions(),
		// 调试选项使用推荐默认值（默认开启 profiling）。
		LeaderMigration: &migration.LeaderMigrationOptions{},
		// 这里给空选项，ApplyTo 会按“关闭迁移”处理；并非 nil。
	}

	return o
}

// AddFlags 将与 controller manager 通用配置相关的 flag 注册到指定的 FlagSet。
// 向 NamedFlagSets 注册通用 flag，按主题分组为 debugging / leader-migration / generic。
func (o *GenericControllerManagerConfigurationOptions) AddFlags(fss *cliflag.NamedFlagSets, allControllers, disabledByDefaultControllers []string, controllerAliases map[string]string) {
	if o == nil {
		return
	}

	// 调试 flag 单独成组，便于 --help 分类展示。
	o.Debugging.AddFlags(fss.FlagSet("debugging"))
	o.LeaderMigration.AddFlags(fss.FlagSet("leader-migration"))
	// 其余通用 flag 统一放到 generic 分组。
	genericfs := fss.FlagSet("generic")
	genericfs.DurationVar(&o.MinResyncPeriod.Duration, "min-resync-period", o.MinResyncPeriod.Duration, "The resync period in reflectors will be random between MinResyncPeriod and 2*MinResyncPeriod.")
	genericfs.StringVar(&o.ClientConnection.ContentType, "kube-api-content-type", o.ClientConnection.ContentType, "Content type of requests sent to apiserver.")
	genericfs.Float32Var(&o.ClientConnection.QPS, "kube-api-qps", o.ClientConnection.QPS, "QPS to use while talking with kubernetes apiserver.")
	genericfs.Int32Var(&o.ClientConnection.Burst, "kube-api-burst", o.ClientConnection.Burst, "Burst to use while talking with kubernetes apiserver.")
	genericfs.DurationVar(&o.ControllerStartInterval.Duration, "controller-start-interval", o.ControllerStartInterval.Duration, "Interval between starting controller managers.")
	// --controllers 的帮助文本会动态列出所有已知控制器及默认禁用列表。
	genericfs.StringSliceVar(&o.Controllers, "controllers", o.Controllers, fmt.Sprintf(""+
		"A list of controllers to enable. '*' enables all on-by-default controllers, 'foo' enables the controller "+
		"named 'foo', '-foo' disables the controller named 'foo'.\nAll controllers: %s\nDisabled-by-default controllers: %s",
		strings.Join(allControllers, ", "), strings.Join(disabledByDefaultControllers, ", ")))

	// 领导者选举 flag 由 component-base 统一提供，避免各组件重复定义。
	options.BindLeaderElectionFlags(&o.LeaderElection, genericfs)
}

// ApplyTo 用选项填充通用配置。
// 把命令行选项落地到正式配置对象：先应用子选项，再复制标量字段，
// 最后把控制器别名解析成规范名称（保留启用/禁用前缀）。
func (o *GenericControllerManagerConfigurationOptions) ApplyTo(cfg *cmconfig.GenericControllerManagerConfiguration, allControllers []string, disabledByDefaultControllers []string, controllerAliases map[string]string) error {
	if o == nil {
		return nil
	}

	// 调试配置优先应用，出错则立即返回。
	if err := o.Debugging.ApplyTo(&cfg.Debugging); err != nil {
		return err
	}
	// 领导者迁移配置会被写入 cfg 的 LeaderMigrationEnabled / LeaderMigration 字段。
	if err := o.LeaderMigration.ApplyTo(cfg); err != nil {
		return err
	}
	// 以下都是简单的值拷贝，不涉及转换或校验。
	cfg.Port = o.Port
	cfg.Address = o.Address
	cfg.MinResyncPeriod = o.MinResyncPeriod
	cfg.ClientConnection = o.ClientConnection
	cfg.ControllerStartInterval = o.ControllerStartInterval
	cfg.LeaderElection = o.LeaderElection

	// 复制控制器名称，并把别名替换为规范名称。
	// 用户可能输入别名，这里统一替换为规范名称，保证后续判断（IsControllerEnabled 等）一致。
	cfg.Controllers = make([]string, len(o.Controllers))
	for i, initialName := range o.Controllers {
		initialNameWithoutPrefix := strings.TrimPrefix(initialName, "-")
		// 先去掉禁用前缀，才能正确匹配别名表。
		controllerName := initialNameWithoutPrefix
		if canonicalName, ok := controllerAliases[controllerName]; ok {
			// 命中别名则替换为规范名称。
			controllerName = canonicalName
		}
		if strings.HasPrefix(initialName, "-") {
			// 如果原来带禁用前缀，替换后要把 "-" 补回去，保持语义不变。
			controllerName = fmt.Sprintf("-%s", controllerName)
		}
		cfg.Controllers[i] = controllerName
	}

	return nil
}

// Validate 校验 GenericOptions。
// 校验选项：调试选项、领导者选举锁类型，以及每个控制器名是否在已知列表中。
func (o *GenericControllerManagerConfigurationOptions) Validate(allControllers []string, disabledByDefaultControllers []string, controllerAliases map[string]string) []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	// 汇总子选项的错误，而不是遇到第一个就返回，便于一次性展示所有问题。
	errs = append(errs, o.Debugging.Validate()...)

	// TODO: This can be removed when ResourceLock is not available
	// 临时约束：开启领导者选举时只允许使用 leases 锁（迁移期兼容性的考虑），
	// 即把 ResourceLock 锁定为 leases。
	if o.LeaderElection.LeaderElect && o.LeaderElection.ResourceLock != "leases" {
		errs = append(errs, fmt.Errorf(`resourceLock value must be "leases"`))
	}

	allControllersSet := sets.NewString(allControllers...)
	// 用集合加速“控制器是否存在”的查询。
	for _, initialName := range o.Controllers {
		if initialName == "*" {
			// 通配符不需要匹配具体控制器名，跳过。
			continue
		}
		initialNameWithoutPrefix := strings.TrimPrefix(initialName, "-")
		controllerName := initialNameWithoutPrefix
		if canonicalName, ok := controllerAliases[controllerName]; ok {
			controllerName = canonicalName
		}
		// 解析别名后仍不在名单中，说明用户拼错或该控制器未编译进本组件。
		if !allControllersSet.Has(controllerName) {
			errs = append(errs, fmt.Errorf("%q is not in the list of known controllers", initialNameWithoutPrefix))
		}
	}

	return errs
}
