/*
Copyright 2014 The Kubernetes Authors.

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

package clientcmd

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"k8s.io/klog/v2"

	restclient "k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// ConfigAccess 供本包的子命令与方法使用，用于加载并修改相应的配置文件。
type ConfigAccess interface {
	// GetLoadingPrecedence 返回加载与检查配置时应使用的文件列表
	GetLoadingPrecedence() []string
	// GetStartingConfig 返回子命令实际操作所用的配置。是否合并取决于加载规则
	GetStartingConfig() (*clientcmdapi.Config, error)
	// GetDefaultFilename 返回新建（而非更新已有）条目时应写入的文件名（必要时创建）。
	GetDefaultFilename() string
	// IsExplicitFile 表示该命令是否只关心单个文件。本实现只通过 flag 做到这一点，
	// 但处理本地、全局与 flag 的实现可能有更多来源。
	IsExplicitFile() bool
	// GetExplicitFile 返回该命令实际操作的那个文件。本实现只有一个，
	// 但处理本地、全局与 flag 的实现可能有更多。
	GetExplicitFile() string
}

// PathOptions 汇总决定 kubeconfig 读取路径的各项来源：显式文件、环境变量、
// 默认全局文件，以及底层 LoadingRules。解析优先级在 GetLoadingPrecedence 中定义。
type PathOptions struct {
	// GlobalFile 是作为全局（最后）选项加载的文件完整路径
	GlobalFile string
	// EnvVar 是指向待加载 kubeconfig 文件列表的环境变量名
	EnvVar string
	// ExplicitFileFlag 是用于提示输入 kubeconfig 文件的 flag 名称
	ExplicitFileFlag string

	// GlobalFileSubpath 是用于展示帮助信息的可选值
	GlobalFileSubpath string

	LoadingRules *ClientConfigLoadingRules
}

var (
	// UseModifyConfigLock 确保通过 ModifyConfig 方法访问 kubeconfig 文件时
	// 由锁文件保护。该变量特意公开，以便本库的其他使用方可以修改其默认行为，
	// 但禁用时需小心，因为这会使代码不再是线程安全的。
	// 关闭后多个进程/协程并发写 kubeconfig 可能互相覆盖；仅在确定无并发时禁用。
	UseModifyConfigLock = true
)

// GetEnvVarFiles 返回环境变量中列出的 kubeconfig 文件列表（已去重）。
func (o *PathOptions) GetEnvVarFiles() []string {
	if len(o.EnvVar) == 0 {
		return []string{}
	}

	envVarValue := os.Getenv(o.EnvVar)
	if len(envVarValue) == 0 {
		return []string{}
	}

	fileList := filepath.SplitList(envVarValue)
	// 防止同一路径被加载多次
	return deduplicate(fileList)
}

// GetLoadingPrecedence 返回读取顺序：显式文件 > 环境变量列表 > 全局文件。
func (o *PathOptions) GetLoadingPrecedence() []string {
	if o.IsExplicitFile() {
		return []string{o.GetExplicitFile()}
	}

	if envVarFiles := o.GetEnvVarFiles(); len(envVarFiles) > 0 {
		return envVarFiles
	}
	return []string{o.GlobalFile}
}

// GetStartingConfig 按上述优先级加载并合并 kubeconfig；文件不存在视为空配置而非错误。
func (o *PathOptions) GetStartingConfig() (*clientcmdapi.Config, error) {
	// 不要修改原始对象。
	// 复制一份 rules 再改 Precedence，避免污染调用方共享的 LoadingRules。
	loadingRules := *o.LoadingRules
	loadingRules.Precedence = o.GetLoadingPrecedence()

	clientConfig := NewNonInteractiveDeferredLoadingClientConfig(&loadingRules, &ConfigOverrides{})
	rawConfig, err := clientConfig.RawConfig()
	if os.IsNotExist(err) {
		return clientcmdapi.NewConfig(), nil
	}
	if err != nil {
		return nil, err
	}

	return &rawConfig, nil
}

// GetDefaultFilename 返回新建条目应写入的文件：显式文件优先，其次环境变量列表中
// 已存在或最后一个文件，最后回退到全局文件。
func (o *PathOptions) GetDefaultFilename() string {
	if o.IsExplicitFile() {
		return o.GetExplicitFile()
	}

	if envVarFiles := o.GetEnvVarFiles(); len(envVarFiles) > 0 {
		if len(envVarFiles) == 1 {
			return envVarFiles[0]
		}

		// 环境变量列出的文件中有已存在的，就返回它
		for _, envVarFile := range envVarFiles {
			if _, err := os.Stat(envVarFile); err == nil {
				return envVarFile
			}
		}

		// 否则返回列表中的最后一个
		return envVarFiles[len(envVarFiles)-1]
	}

	return o.GlobalFile
}

// IsExplicitFile 表示是否通过 --kubeconfig 之类的参数显式指定了单个文件。
func (o *PathOptions) IsExplicitFile() bool {
	return len(o.LoadingRules.ExplicitPath) > 0
}

// GetExplicitFile 返回显式指定的 kubeconfig 路径。
func (o *PathOptions) GetExplicitFile() string {
	return o.LoadingRules.ExplicitPath
}

// NewDefaultPathOptions 返回采用 kubectl 约定路径与环境变量的 PathOptions。
func NewDefaultPathOptions() *PathOptions {
	ret := &PathOptions{
		GlobalFile:       RecommendedHomeFile,
		EnvVar:           RecommendedConfigPathEnvVar,
		ExplicitFileFlag: RecommendedConfigPathFlag,

		GlobalFileSubpath: filepath.Join(RecommendedHomeDir, RecommendedFileName),

		LoadingRules: NewDefaultClientConfigLoadingRules(),
	}
	// 保留用户写入的相对路径，避免加载时解析成基于当前工作目录的绝对路径。
	ret.LoadingRules.DoNotResolvePaths = true

	return ret
}

// ModifyConfig 接收 Config 对象，遍历 Clusters、AuthInfos 与 Contexts，
// 若指定了 LocationOfOrigin 就写回该文件，否则写入默认目标文件。这会产生多次
// 文件读取，但逻辑很好跟进。Preferences 和 CurrentContext 总是应写入默认目标
// 文件；由于无法区分“空值”与“缺失值”（字符串没有 nil），必须对它们单独处理。
// 在 kubeconfig 场景下，newConfig 最多应只有一处差异，也就是说这段代码只会写
// 一个文件。若想相对化路径（relativizePaths），必须在被修改的元素中提供完整路径。
// 算法：逐项对比新旧配置，只把发生变化的 stanza 写回其 LocationOfOrigin（未记录来源
// 时写默认文件），删除项同理。CurrentContext/Preferences 是全局字段，单独写入默认文件。
func ModifyConfig(configAccess ConfigAccess, newConfig clientcmdapi.Config, relativizePaths bool) error {
	if UseModifyConfigLock {
		possibleSources := configAccess.GetLoadingPrecedence()
		// 对候选 kubeconfig 文件排序，保证总是按同一顺序“加锁”以避免死锁
		//（注意：符号链接场景可能失效，但……别闹了）。
		// 固定加锁顺序，防止并发进程以不同顺序加锁而死锁。
		sort.Strings(possibleSources)
		for _, filename := range possibleSources {
			if err := lockFile(filename); err != nil {
				return err
			}
			defer unlockFile(filename)
		}
	}

	startingConfig, err := configAccess.GetStartingConfig()
	if err != nil {
		return err
	}

	// 我们需要找出所有差异、定位其来源文件、读取局部配置只修改该条目，然后写回文件。
	// current context 与 preferences 的判定需要特殊处理，因为它们总是写入默认文件。
	if reflect.DeepEqual(*startingConfig, newConfig) {
		// 无需处理
		return nil
	}

	if startingConfig.CurrentContext != newConfig.CurrentContext {
		if err := writeCurrentContext(configAccess, newConfig.CurrentContext); err != nil {
			return err
		}
	}

	if !reflect.DeepEqual(startingConfig.Preferences, newConfig.Preferences) {
		if err := writePreferences(configAccess, newConfig.Preferences); err != nil {
			return err
		}
	}

	// 遍历每个 cluster、authInfo 和 context：先从新到旧找差异，再从旧到新找删除项
	for key, cluster := range newConfig.Clusters {
		startingCluster, exists := startingConfig.Clusters[key]
		if !reflect.DeepEqual(cluster, startingCluster) || !exists {
			// 写回原文件，保持多文件 kubeconfig 的条目归属不变。
			destinationFile := cluster.LocationOfOrigin
			if len(destinationFile) == 0 {
				destinationFile = configAccess.GetDefaultFilename()
			}

			configToWrite, err := getConfigFromFile(destinationFile)
			if err != nil {
				return err
			}
			t := *cluster

			configToWrite.Clusters[key] = &t
			configToWrite.Clusters[key].LocationOfOrigin = destinationFile
			if relativizePaths {
				if err := RelativizeClusterLocalPaths(configToWrite.Clusters[key]); err != nil {
					return err
				}
			}

			if err := WriteToFile(*configToWrite, destinationFile); err != nil {
				return err
			}
		}
	}

	// seenConfigs 保存“配置文件名 -> 已计算配置对象”的映射。
	// 同一文件里可能有多处改动，先累积到内存，最后每个文件只写一次。
	seenConfigs := map[string]*clientcmdapi.Config{}

	for key, context := range newConfig.Contexts {
		startingContext, exists := startingConfig.Contexts[key]
		if !reflect.DeepEqual(context, startingContext) || !exists {
			destinationFile := context.LocationOfOrigin
			if len(destinationFile) == 0 {
				destinationFile = configAccess.GetDefaultFilename()
			}

			// 只有在没见过该文件时才从其源文件读取新的配置对象——
			// 这避免了当多个/全部上下文共享同一目标文件时，
			// 反复读写同样多的文件。
			configToWrite, seen := seenConfigs[destinationFile]
			if !seen {
				var err error
				configToWrite, err = getConfigFromFile(destinationFile)
				if err != nil {
					return err
				}
				seenConfigs[destinationFile] = configToWrite
			}

			configToWrite.Contexts[key] = context
		}
	}

	// 真正持久化配置对象的改动
	for destinationFile, configToWrite := range seenConfigs {
		if err := WriteToFile(*configToWrite, destinationFile); err != nil {
			return err
		}
	}

	for key, authInfo := range newConfig.AuthInfos {
		startingAuthInfo, exists := startingConfig.AuthInfos[key]
		if !reflect.DeepEqual(authInfo, startingAuthInfo) || !exists {
			destinationFile := authInfo.LocationOfOrigin
			if len(destinationFile) == 0 {
				destinationFile = configAccess.GetDefaultFilename()
			}

			configToWrite, err := getConfigFromFile(destinationFile)
			if err != nil {
				return err
			}
			t := *authInfo
			configToWrite.AuthInfos[key] = &t
			configToWrite.AuthInfos[key].LocationOfOrigin = destinationFile
			if relativizePaths {
				if err := RelativizeAuthInfoLocalPaths(configToWrite.AuthInfos[key]); err != nil {
					return err
				}
			}

			if err := WriteToFile(*configToWrite, destinationFile); err != nil {
				return err
			}
		}
	}

	// 第二阶段：处理删除——旧配置中存在、新配置中已消失的条目。
	for key, cluster := range startingConfig.Clusters {
		if _, exists := newConfig.Clusters[key]; !exists {
			destinationFile := cluster.LocationOfOrigin
			if len(destinationFile) == 0 {
				destinationFile = configAccess.GetDefaultFilename()
			}

			configToWrite, err := getConfigFromFile(destinationFile)
			if err != nil {
				return err
			}
			delete(configToWrite.Clusters, key)

			if err := WriteToFile(*configToWrite, destinationFile); err != nil {
				return err
			}
		}
	}

	for key, context := range startingConfig.Contexts {
		if _, exists := newConfig.Contexts[key]; !exists {
			destinationFile := context.LocationOfOrigin
			if len(destinationFile) == 0 {
				destinationFile = configAccess.GetDefaultFilename()
			}

			configToWrite, err := getConfigFromFile(destinationFile)
			if err != nil {
				return err
			}
			delete(configToWrite.Contexts, key)

			if err := WriteToFile(*configToWrite, destinationFile); err != nil {
				return err
			}
		}
	}

	for key, authInfo := range startingConfig.AuthInfos {
		if _, exists := newConfig.AuthInfos[key]; !exists {
			destinationFile := authInfo.LocationOfOrigin
			if len(destinationFile) == 0 {
				destinationFile = configAccess.GetDefaultFilename()
			}

			configToWrite, err := getConfigFromFile(destinationFile)
			if err != nil {
				return err
			}
			delete(configToWrite.AuthInfos, key)

			if err := WriteToFile(*configToWrite, destinationFile); err != nil {
				return err
			}
		}
	}

	return nil
}

// PersisterForUser 返回把 AuthProvider 的临时配置（如刷新后的 token）写回 kubeconfig
// 的持久化器，user 为 kubeconfig 中 authInfo 的条目名。
func PersisterForUser(configAccess ConfigAccess, user string) restclient.AuthProviderConfigPersister {
	return &persister{configAccess, user}
}

// persister 实现 restclient.AuthProviderConfigPersister。
type persister struct {
	configAccess ConfigAccess
	user         string
}

// Persist 把新的 provider 配置合并进对应用户的 authInfo 并落盘；用户不存在时静默忽略。
func (p *persister) Persist(config map[string]string) error {
	newConfig, err := p.configAccess.GetStartingConfig()
	if err != nil {
		return err
	}
	authInfo, ok := newConfig.AuthInfos[p.user]
	if ok && authInfo.AuthProvider != nil {
		authInfo.AuthProvider.Config = config
		return ModifyConfig(p.configAccess, *newConfig, false)
	}
	return nil
}

// writeCurrentContext 有三条可能的路径。
// 若 newCurrentContext 与 startingConfig 的当前上下文相同，直接返回；
// 若 newCurrentContext 有值，则写入默认目标文件；
// 若 newCurrentContext 为空，则找到设置了 CurrentContext 的配置文件并清除其值。
func writeCurrentContext(configAccess ConfigAccess, newCurrentContext string) error {
	if startingConfig, err := configAccess.GetStartingConfig(); err != nil {
		return err
	} else if startingConfig.CurrentContext == newCurrentContext {
		return nil
	}

	// 显式文件模式下只操作该文件，不触碰其他 kubeconfig。
	if configAccess.IsExplicitFile() {
		file := configAccess.GetExplicitFile()
		currConfig, err := getConfigFromFile(file)
		if err != nil {
			return err
		}
		currConfig.CurrentContext = newCurrentContext
		if err := WriteToFile(*currConfig, file); err != nil {
			return err
		}

		return nil
	}

	if len(newCurrentContext) > 0 {
		destinationFile := configAccess.GetDefaultFilename()
		config, err := getConfigFromFile(destinationFile)
		if err != nil {
			return err
		}
		config.CurrentContext = newCurrentContext

		if err := WriteToFile(*config, destinationFile); err != nil {
			return err
		}

		return nil
	}

	// 这里要清空当前上下文：需找到优先级链中第一个设置了它的文件并清除。
	// 清空时要找到优先级链中第一个真正设置了 current-context 的文件，而不是盲目写默认文件。
	for _, file := range configAccess.GetLoadingPrecedence() {
		if _, err := os.Stat(file); err == nil {
			currConfig, err := getConfigFromFile(file)
			if err != nil {
				return err
			}

			if len(currConfig.CurrentContext) > 0 {
				currConfig.CurrentContext = newCurrentContext
				if err := WriteToFile(*currConfig, file); err != nil {
					return err
				}

				return nil
			}
		}
	}

	return errors.New("no config found to write context")
}

// writePreferences 把全局 Preferences 写入显式文件，或优先级链中第一个值不同的文件。
func writePreferences(configAccess ConfigAccess, newPrefs clientcmdapi.Preferences) error {
	if startingConfig, err := configAccess.GetStartingConfig(); err != nil {
		return err
	} else if reflect.DeepEqual(startingConfig.Preferences, newPrefs) {
		return nil
	}

	if configAccess.IsExplicitFile() {
		file := configAccess.GetExplicitFile()
		currConfig, err := getConfigFromFile(file)
		if err != nil {
			return err
		}
		currConfig.Preferences = newPrefs
		if err := WriteToFile(*currConfig, file); err != nil {
			return err
		}

		return nil
	}

	for _, file := range configAccess.GetLoadingPrecedence() {
		currConfig, err := getConfigFromFile(file)
		if err != nil {
			return err
		}

		// 写到第一个（优先级最高的）能够覆盖旧值的文件。
		if !reflect.DeepEqual(currConfig.Preferences, newPrefs) {
			currConfig.Preferences = newPrefs
			if err := WriteToFile(*currConfig, file); err != nil {
				return err
			}

			return nil
		}
	}

	return errors.New("no config found to write preferences")
}

// getConfigFromFile 尝试读取 kubeconfig 文件，失败则返回错误。例外：文件缺失时
// 返回空配置而不是错误。
func getConfigFromFile(filename string) (*clientcmdapi.Config, error) {
	config, err := LoadFromFile(filename)
	// 目标文件可能尚不存在（新建 kubeconfig 的常见场景），以空配置继续。
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if config == nil {
		config = clientcmdapi.NewConfig()
	}
	return config, nil
}

// GetConfigFromFileOrDie 尝试读取 kubeconfig 文件，失败则退出进程。例外：
// 文件缺失时返回空配置而不是退出。
func GetConfigFromFileOrDie(filename string) *clientcmdapi.Config {
	config, err := getConfigFromFile(filename)
	if err != nil {
		//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
		klog.FatalDepth(1, err)
	}

	return config
}
