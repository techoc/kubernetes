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

package options

import (
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/controller-manager/config"
	migrationconfig "k8s.io/controller-manager/pkg/leadermigration/config"
)

// LeaderMigrationOptions 是领导者迁移的一组命令行选项，经 flag 传给 controller-manager。
// 注意：nil 的 *LeaderMigrationOptions 有特殊语义，表示“使用默认选项（关闭迁移）”。
type LeaderMigrationOptions struct {
	// Enabled 表示是否通过 --enable-leader-migration 开启领导者迁移。
	Enabled bool

	// ControllerMigrationConfig 是 LeaderMigrationConfiguration 类型配置文件的路径，
	// 可通过 --leader-migration-config 设置；路径为 ""（默认值）时，
	// 使用 controller-manager 推荐的默认配置。
	ControllerMigrationConfig string
}

// DefaultLeaderMigrationOptions 返回一个取默认值的 LeaderMigrationOptions：关闭迁移且不指定配置文件。
func DefaultLeaderMigrationOptions() *LeaderMigrationOptions {
	return &LeaderMigrationOptions{
		Enabled:                   false,
		ControllerMigrationConfig: "",
	}
}

// AddFlags 把所有领导者迁移相关 flag 注册到给定的 flag set。
// 注册两个 flag：开启开关与配置文件路径。nil 接收者直接跳过。
func (o *LeaderMigrationOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		// nil 表示使用默认选项，即关闭迁移。
		return
	}
	fs.BoolVar(&o.Enabled, "enable-leader-migration", false, "Whether to enable controller leader migration.")
	fs.StringVar(&o.ControllerMigrationConfig, "leader-migration-config", "",
		"Path to the config file for controller leader migration, "+
			"or empty to use the value that reflects default configuration of the controller manager. "+
			"The config file should be of type LeaderMigrationConfiguration, group controllermanager.config.k8s.io, version v1alpha1.")
}

// ApplyTo 把领导者迁移选项应用到通用配置：
// 1) nil 选项 -> 关闭迁移；
// 2) 未开启 -> 直接返回；
// 3) 开启但未指定文件 -> 使用默认迁移配置；
// 4) 开启且指定文件 -> 读取并校验后写入配置。
func (o *LeaderMigrationOptions) ApplyTo(cfg *config.GenericControllerManagerConfiguration) error {
	if o == nil {
		// nil 的 LeaderMigrationOptions 表示使用默认选项，即关闭领导者迁移。
		cfg.LeaderMigrationEnabled = false
		return nil
	}
	cfg.LeaderMigrationEnabled = o.Enabled
	if !cfg.LeaderMigrationEnabled {
		// 开关未打开，无需读取/校验配置，避免不必要的文件错误。
		return nil
	}
	if o.ControllerMigrationConfig == "" {
		// 没有指定文件时使用本版本的默认迁移配置。
		cfg.LeaderMigration = *migrationconfig.DefaultLeaderMigrationConfiguration()
		return nil
	}
	leaderMigrationConfig, err := migrationconfig.ReadLeaderMigrationConfiguration(o.ControllerMigrationConfig)
	// 指定了文件：读取（解析）配置，失败直接返回。
	if err != nil {
		return err
	}
	errs := migrationconfig.ValidateLeaderMigrationConfiguration(leaderMigrationConfig)
	// 解析成功后还要做语义校验，聚合所有错误后一次性返回。
	if len(errs) != 0 {
		return fmt.Errorf("failed to parse leader migration configuration: %v", errs)
	}
	cfg.LeaderMigration = *leaderMigrationConfig
	return nil
}
