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

// ConfigAccess is used by subcommands and methods in this package to load and modify the appropriate config files
type ConfigAccess interface {
	// GetLoadingPrecedence returns the slice of files that should be used for loading and inspecting the config
	GetLoadingPrecedence() []string
	// GetStartingConfig returns the config that subcommands should being operating against.  It may or may not be merged depending on loading rules
	GetStartingConfig() (*clientcmdapi.Config, error)
	// GetDefaultFilename returns the name of the file you should write into (create if necessary), if you're trying to create a new stanza as opposed to updating an existing one.
	GetDefaultFilename() string
	// IsExplicitFile indicates whether or not this command is interested in exactly one file.  This implementation only ever does that  via a flag, but implementations that handle local, global, and flags may have more
	IsExplicitFile() bool
	// GetExplicitFile returns the particular file this command is operating against.  This implementation only ever has one, but implementations that handle local, global, and flags may have more
	GetExplicitFile() string
}

// PathOptions 汇总决定 kubeconfig 读取路径的各项来源：显式文件、环境变量、
// 默认全局文件，以及底层 LoadingRules。解析优先级在 GetLoadingPrecedence 中定义。
type PathOptions struct {
	// GlobalFile is the full path to the file to load as the global (final) option
	GlobalFile string
	// EnvVar is the env var name that points to the list of kubeconfig files to load
	EnvVar string
	// ExplicitFileFlag is the name of the flag to use for prompting for the kubeconfig file
	ExplicitFileFlag string

	// GlobalFileSubpath is an optional value used for displaying help
	GlobalFileSubpath string

	LoadingRules *ClientConfigLoadingRules
}

var (
	// UseModifyConfigLock ensures that access to kubeconfig file using ModifyConfig method
	// is being guarded by a lock file.
	// This variable is intentionaly made public so other consumers of this library
	// can modify its default behavior, but be caution when disabling it since
	// this will make your code not threadsafe.
	//
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
	// prevent the same path load multiple times
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
	// don't mutate the original
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

		// if any of the envvar files already exists, return it
		for _, envVarFile := range envVarFiles {
			if _, err := os.Stat(envVarFile); err == nil {
				return envVarFile
			}
		}

		// otherwise, return the last one in the list
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

// ModifyConfig takes a Config object, iterates through Clusters, AuthInfos, and Contexts, uses the LocationOfOrigin if specified or
// uses the default destination file to write the results into.  This results in multiple file reads, but it's very easy to follow.
// Preferences and CurrentContext should always be set in the default destination file.  Since we can't distinguish between empty and missing values
// (no nil strings), we're forced have separate handling for them.  In the kubeconfig cases, newConfig should have at most one difference,
// that means that this code will only write into a single file.  If you want to relativizePaths, you must provide a fully qualified path in any
// modified element.
//
// 算法：逐项对比新旧配置，只把发生变化的 stanza 写回其 LocationOfOrigin（未记录来源
// 时写默认文件），删除项同理。CurrentContext/Preferences 是全局字段，单独写入默认文件。
func ModifyConfig(configAccess ConfigAccess, newConfig clientcmdapi.Config, relativizePaths bool) error {
	if UseModifyConfigLock {
		possibleSources := configAccess.GetLoadingPrecedence()
		// sort the possible kubeconfig files so we always "lock" in the same order
		// to avoid deadlock (note: this can fail w/ symlinks, but... come on).
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

	// We need to find all differences, locate their original files, read a partial config to modify only that stanza and write out the file.
	// Special case the test for current context and preferences since those always write to the default file.
	if reflect.DeepEqual(*startingConfig, newConfig) {
		// nothing to do
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

	// Search every cluster, authInfo, and context.  First from new to old for differences, then from old to new for deletions
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

	// seenConfigs stores a map of config source filenames to computed config objects
	// 同一文件里可能有多处改动，先累积到内存，最后每个文件只写一次。
	seenConfigs := map[string]*clientcmdapi.Config{}

	for key, context := range newConfig.Contexts {
		startingContext, exists := startingConfig.Contexts[key]
		if !reflect.DeepEqual(context, startingContext) || !exists {
			destinationFile := context.LocationOfOrigin
			if len(destinationFile) == 0 {
				destinationFile = configAccess.GetDefaultFilename()
			}

			// we only obtain a fresh config object from its source file
			// if we have not seen it already - this prevents us from
			// reading and writing to the same number of files repeatedly
			// when multiple / all contexts share the same destination file.
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

	// actually persist config object changes
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

// writeCurrentContext takes three possible paths.
// If newCurrentContext is the same as the startingConfig's current context, then we exit.
// If newCurrentContext has a value, then that value is written into the default destination file.
// If newCurrentContext is empty, then we find the config file that is setting the CurrentContext and clear the value from that file
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

	// we're supposed to be clearing the current context.  We need to find the first spot in the chain that is setting it and clear it
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

// getConfigFromFile tries to read a kubeconfig file and if it can't, returns an error.  One exception, missing files result in empty configs, not an error.
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

// GetConfigFromFileOrDie tries to read a kubeconfig file and if it can't, it calls exit.  One exception, missing files result in empty configs, not an exit
func GetConfigFromFileOrDie(filename string) *clientcmdapi.Config {
	config, err := getConfigFromFile(filename)
	if err != nil {
		//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
		klog.FatalDepth(1, err)
	}

	return config
}
