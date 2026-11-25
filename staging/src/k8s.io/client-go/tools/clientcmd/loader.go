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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	restclient "k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdlatest "k8s.io/client-go/tools/clientcmd/api/latest"
	"k8s.io/client-go/util/homedir"
)

const (
	// RecommendedConfigPathFlag 是建议的 kubeconfig 命令行 flag 名称。
	RecommendedConfigPathFlag = "kubeconfig"
	// RecommendedConfigPathEnvVar 是建议的 kubeconfig 环境变量名。
	RecommendedConfigPathEnvVar = "KUBECONFIG"
	// RecommendedHomeDir 是默认的配置目录名。
	RecommendedHomeDir = ".kube"
	// RecommendedFileName 是默认的配置文件名。
	RecommendedFileName = "config"
	// RecommendedSchemaName 是默认 schema 文件名。
	RecommendedSchemaName = "schema"
)

var (
	RecommendedConfigDir  = filepath.Join(homedir.HomeDir(), RecommendedHomeDir)
	RecommendedHomeFile   = filepath.Join(RecommendedConfigDir, RecommendedFileName)
	RecommendedSchemaFile = filepath.Join(RecommendedConfigDir, RecommendedSchemaName)
)

// currentMigrationRules 返回一个映射，记录历史版本使用过的推荐配置目录。
// 今后对 RecommendedHomeFile 及相关路径的任何改动，都应在此添加迁移规则，
// 以确保已有配置文件被正确迁移到新位置。
// 迁移只做“目标不存在时从源复制”，不删除源文件，避免破坏旧版本的并行使用。
func currentMigrationRules() map[string]string {
	var oldRecommendedHomeFileName string
	if goruntime.GOOS == "windows" {
		oldRecommendedHomeFileName = RecommendedFileName
	} else {
		oldRecommendedHomeFileName = ".kubeconfig"
	}
	return map[string]string{
		RecommendedHomeFile: filepath.Join(os.Getenv("HOME"), RecommendedHomeDir, oldRecommendedHomeFileName),
	}
}

// ClientConfigLoader 抽象“取得原始 kubeconfig 并判断是否为默认配置”的能力，
// 便于用内存实现替换文件系统实现。
type ClientConfigLoader interface {
	ConfigAccess
	// IsDefaultConfig 在返回的配置与默认配置一致时返回 true。
	IsDefaultConfig(*restclient.Config) bool
	// Load 返回最新的配置
	Load() (*clientcmdapi.Config, error)
}

// KubeconfigGetter 是延迟获取 kubeconfig 的回调类型。
type KubeconfigGetter func() (*clientcmdapi.Config, error)

// ClientConfigGetter 把 KubeconfigGetter 适配为 ClientConfigLoader。
type ClientConfigGetter struct {
	kubeconfigGetter KubeconfigGetter
}

// ClientConfigGetter 实现了 ClientConfigLoader 接口。
var _ ClientConfigLoader = &ClientConfigGetter{}

// Load 调用底层 getter 取得配置。
func (g *ClientConfigGetter) Load() (*clientcmdapi.Config, error) {
	return g.kubeconfigGetter()
}

// GetLoadingPrecedence 对内存 getter 无意义，返回 nil。
func (g *ClientConfigGetter) GetLoadingPrecedence() []string {
	return nil
}

// GetStartingConfig 与 Load 等价。
func (g *ClientConfigGetter) GetStartingConfig() (*clientcmdapi.Config, error) {
	return g.kubeconfigGetter()
}

// GetDefaultFilename 对内存 getter 无意义，返回空串。
func (g *ClientConfigGetter) GetDefaultFilename() string {
	return ""
}

// IsExplicitFile 对内存 getter 恒为 false。
func (g *ClientConfigGetter) IsExplicitFile() bool {
	return false
}

// GetExplicitFile 对内存 getter 恒为空。
func (g *ClientConfigGetter) GetExplicitFile() string {
	return ""
}

// IsDefaultConfig 对内存 getter 恒为 false，无法与默认配置比对。
func (g *ClientConfigGetter) IsDefaultConfig(config *restclient.Config) bool {
	return false
}

// ClientConfigLoadingRules 由一个 ExplicitPath 和一组特定位置的切片组成，用于合并出 Config。
// 调用方可以自行组织加载链，但我们推荐：
// 若设置了 EnvVarPathFiles 则用它（一个文件列表），否则用 HomeDirectoryPath。
// ExplicitPath 是特殊的：用户明确指定了某个文件，若该文件不存在就报错。
// 合并优先级遵循“前面的文件优先”：map 条目先到先得，标量字段则由后到前合并，
// 详见 Load。
type ClientConfigLoadingRules struct {
	ExplicitPath string
	Precedence   []string

	// MigrationRules 是目标文件到源文件的映射。若目标文件不存在，则检查源文件；
	// 若源文件存在，则在任何后续加载发生之前把它复制到目标文件。
	MigrationRules map[string]string

	// DoNotResolvePaths 表示是否不相对于来源文件解析路径。特意用否定形式命名，
	// 这样未设置该字段的默认对象通常就能得到期望的行为。
	DoNotResolvePaths bool

	// DefaultClientConfig 是可选字段，指示用哪些规则计算默认配置。
	// 它应当与传给 ClientConfig 加载器的 overrides 一致。
	DefaultClientConfig ClientConfig

	// WarnIfAllMissing 控制是否检查 KUBECONFIG 环境变量指向的配置文件是否存在，
	// 若有缺失文件则向用户发出警告。
	WarnIfAllMissing bool

	// Warner 是发生文件缺失时使用的警告日志回调。
	Warner WarningHandler
}

// WarningHandler 允许设置所使用的日志函数。
type WarningHandler func(error)

// Warn 在未设置回调时退化为 klog V(1) 日志，保证默认路径不会 panic。
func (handler WarningHandler) Warn(err error) {
	if handler == nil {
		//nolint:logcheck // This is the fallback when logging is not initialized. With nothing provided, using the global logger is the only option.
		klog.V(1).Info(err)
	} else {
		handler(err)
	}
}

// MissingConfigError 汇总所有缺失的 kubeconfig 路径，便于一次性提示用户。
type MissingConfigError struct {
	Missing []string
}

func (c MissingConfigError) Error() string {
	return fmt.Sprintf("Config not found: %s", strings.Join(c.Missing, ", "))
}

// ClientConfigLoadingRules 实现了 ClientConfigLoader 接口。
var _ ClientConfigLoader = &ClientConfigLoadingRules{}

// NewDefaultClientConfigLoadingRules 返回填好默认字段的 ClientConfigLoadingRules 对象。
// 并非必须使用该构造函数。
// KUBECONFIG 支持用 filepath.SplitList 分隔的多个路径；重复路径会去重，
// 避免同一文件被合并两次。未设置时退回 ~/.kube/config。
func NewDefaultClientConfigLoadingRules() *ClientConfigLoadingRules {
	chain := []string{}
	warnIfAllMissing := false

	envVarFiles := os.Getenv(RecommendedConfigPathEnvVar)
	if len(envVarFiles) != 0 {
		fileList := filepath.SplitList(envVarFiles)
		// 防止同一路径被加载多次
		chain = append(chain, deduplicate(fileList)...)
		warnIfAllMissing = true

	} else {
		chain = append(chain, RecommendedHomeFile)
	}

	return &ClientConfigLoadingRules{
		Precedence:       chain,
		MigrationRules:   currentMigrationRules(),
		WarnIfAllMissing: warnIfAllMissing,
	}
}

// Load 先执行 MigrationRules，然后根据以下规则按加载规则返回一个 Config 对象：
//
//	若有 ExplicitPath，返回未合并的显式文件；
//	否则返回按 Precedence 切片合并出的配置。
//
// ExplicitPath 文件缺失会产生错误；空文件名和其他缺失文件会被忽略。
// 读取错误或内容无法反序列化的文件会产生错误。
// 第一个设置某个 map 键的文件胜出，且该键的值不会再被改动。
// 但是，若设置的是 map 之外的结构体值，该值会被后面的文件覆盖。
// 这导致看起来有些奇怪的逻辑：向一个方向合并 map、向另一个方向合并其余部分，再把两者合并。
// 这也意味着，若两个文件都指定了 "red-user"，只会使用第一个文件中 red-user 的值，
// 甚至第二个文件 "red-user" 中不冲突的条目也会被丢弃。
// .kubeconfig 文件内的相对路径会相对该文件的父目录解析，最终只返回绝对文件路径。
func (rules *ClientConfigLoadingRules) Load() (*clientcmdapi.Config, error) {
	// 迁移必须在读取之前完成，否则新路径读不到旧数据。
	if err := rules.Migrate(); err != nil {
		return nil, err
	}

	errlist := []error{}
	missingList := []string{}

	kubeConfigFiles := []string{}

	// 确保用户显式指定的文件存在
	// ExplicitPath 由用户显式指定，缺失应报错而不是静默忽略。
	if len(rules.ExplicitPath) > 0 {
		if _, err := os.Stat(rules.ExplicitPath); os.IsNotExist(err) {
			return nil, err
		}
		kubeConfigFiles = append(kubeConfigFiles, rules.ExplicitPath)

	} else {
		kubeConfigFiles = append(kubeConfigFiles, rules.Precedence...)
	}

	kubeconfigs := []*clientcmdapi.Config{}
	// 读取并缓存配置文件，保证每个文件只被读取一次
	for _, filename := range kubeConfigFiles {
		if len(filename) == 0 {
			// 无需处理
			continue
		}

		config, err := LoadFromFile(filename)

		if os.IsNotExist(err) {
			// 跳过缺失的文件
			// 加入缺失列表以便产生警告
			missingList = append(missingList, filename)
			continue
		}

		if err != nil {
			errlist = append(errlist, fmt.Errorf("error loading config file \"%s\": %v", filename, err))
			continue
		}

		kubeconfigs = append(kubeconfigs, config)
	}

	if rules.WarnIfAllMissing && len(missingList) > 0 && len(kubeconfigs) == 0 {
		rules.Warner.Warn(MissingConfigError{Missing: missingList})
	}

	// 先合并所有的 map：
	// map 合并按优先级正序，使靠前的文件“先写入者胜出”。
	mapConfig := clientcmdapi.NewConfig()

	for _, kubeconfig := range kubeconfigs {
		if err := merge(mapConfig, kubeconfig); err != nil {
			return nil, err
		}
	}

	// 以相反的顺序合并所有结构体值，从而正确处理优先级；
	// 第二次合并时错误不再追加到列表。
	// 标量/结构体字段用反向合并：后合并者覆盖，因此先合并低优先级文件。
	nonMapConfig := clientcmdapi.NewConfig()
	for i := len(kubeconfigs) - 1; i >= 0; i-- {
		kubeconfig := kubeconfigs[i]
		if err := merge(nonMapConfig, kubeconfig); err != nil {
			return nil, err
		}
	}

	// 由于（非 map 的）值会被覆盖而 map 的值不会，把非 map 配置合并到 map 配置之上，
	// 就能得到期望的结果。
	// 最后把非 map 结果覆盖到 map 结果上，得到“map 先到先得 + 标量高优先级覆盖”的组合语义。
	config := clientcmdapi.NewConfig()
	if err := merge(config, mapConfig); err != nil {
		return nil, err
	}
	if err := merge(config, nonMapConfig); err != nil {
		return nil, err
	}

	if rules.ResolvePaths() {
		if err := ResolveLocalPaths(config); err != nil {
			errlist = append(errlist, err)
		}
	}
	return config, utilerrors.NewAggregate(errlist)
}

// Migrate 使用 MigrationRules 映射。若目标文件不存在，则检查源文件；
// 若源文件存在，则在任何后续加载发生之前把它复制到目标文件。
func (rules *ClientConfigLoadingRules) Migrate() error {
	if rules.MigrationRules == nil {
		return nil
	}

	for destination, source := range rules.MigrationRules {
		if _, err := os.Stat(destination); err == nil {
			// 目标已存在则什么都不做
			continue
		} else if os.IsPermission(err) {
			// 无法访问文件则跳过
			continue
		} else if !os.IsNotExist(err) {
			// 若不是“不存在”错误，则直接失败
			return err
		}

		sourceInfo, err := os.Stat(source)
		if err != nil {
			if os.IsNotExist(err) || os.IsPermission(err) {
				// 源文件不存在或无法访问时，无需处理。
				continue
			}

			// 若不是“不存在”错误，则直接失败
			return err
		} else if sourceInfo.IsDir() {
			return fmt.Errorf("cannot migrate %v to %v because it is a directory", source, destination)
		}

		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		// 目标文件按源文件权限创建，但永不含可执行位，且仍受 umask 影响。
		// kubeconfig 可能含凭证，去掉可执行位，并仍受 umask 限制。
		err = os.WriteFile(destination, data, sourceInfo.Mode().Perm()&0666)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetLoadingPrecedence 实现 ConfigAccess 接口。
// 返回副本，防止调用方排序等操作污染内部 Precedence。
func (rules *ClientConfigLoadingRules) GetLoadingPrecedence() []string {
	if len(rules.ExplicitPath) > 0 {
		return []string{rules.ExplicitPath}
	}

	// 创建副本，以防调用方对返回的切片排序。
	precedence := make([]string, len(rules.Precedence))
	copy(precedence, rules.Precedence)
	return precedence
}

// GetStartingConfig 实现 ConfigAccess 接口。
// 文件不存在时返回空配置而不是错误，符合“首次使用时自动创建”的语义。
func (rules *ClientConfigLoadingRules) GetStartingConfig() (*clientcmdapi.Config, error) {
	clientConfig := NewNonInteractiveDeferredLoadingClientConfig(rules, &ConfigOverrides{})
	rawConfig, err := clientConfig.RawConfig()
	if os.IsNotExist(err) {
		return clientcmdapi.NewConfig(), nil
	}
	if err != nil {
		return nil, err
	}

	return &rawConfig, nil
}

// GetDefaultFilename 实现 ConfigAccess 接口。
func (rules *ClientConfigLoadingRules) GetDefaultFilename() string {
	// 有显式文件就用显式文件。
	if rules.IsExplicitFile() {
		return rules.GetExplicitFile()
	}
	// 否则用优先级列表中第一个存在的文件。
	for _, filename := range rules.GetLoadingPrecedence() {
		if _, err := os.Stat(filename); err == nil {
			return filename
		}
	}
	// 若都不存在，用优先级列表的第一项。
	if len(rules.Precedence) > 0 {
		return rules.Precedence[0]
	}
	return ""
}

// IsExplicitFile 实现 ConfigAccess 接口。
func (rules *ClientConfigLoadingRules) IsExplicitFile() bool {
	return len(rules.ExplicitPath) > 0
}

// GetExplicitFile 实现 ConfigAccess 接口。
func (rules *ClientConfigLoadingRules) GetExplicitFile() string {
	return rules.ExplicitPath
}

// IsDefaultConfig 在提供的配置与默认配置一致时返回 true。
func (rules *ClientConfigLoadingRules) IsDefaultConfig(config *restclient.Config) bool {
	if rules.DefaultClientConfig == nil {
		return false
	}
	defaultConfig, err := rules.DefaultClientConfig.ClientConfig()
	if err != nil {
		return false
	}
	return reflect.DeepEqual(config, defaultConfig)
}

// LoadFromFile 读取指定文件并把内容反序列化为 Config 对象。
// 为每个条目记录 LocationOfOrigin，后续才能相对其所在文件解析相对路径。
func LoadFromFile(filename string) (*clientcmdapi.Config, error) {
	kubeconfigBytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	config, err := Load(kubeconfigBytes)
	if err != nil {
		return nil, err
	}
	//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
	klog.V(6).Infoln("Config loaded from file: ", filename)

	// 为每个 Cluster、User、Context 设置 LocationOfOrigin
	for key, obj := range config.AuthInfos {
		obj.LocationOfOrigin = filename
		config.AuthInfos[key] = obj
	}
	for key, obj := range config.Clusters {
		obj.LocationOfOrigin = filename
		config.Clusters[key] = obj
	}
	for key, obj := range config.Contexts {
		obj.LocationOfOrigin = filename
		config.Contexts[key] = obj
	}

	// 反序列化可能产生 nil map；统一初始化为空 map，避免调用方写入时 panic。
	if config.AuthInfos == nil {
		config.AuthInfos = map[string]*clientcmdapi.AuthInfo{}
	}
	if config.Clusters == nil {
		config.Clusters = map[string]*clientcmdapi.Cluster{}
	}
	if config.Contexts == nil {
		config.Contexts = map[string]*clientcmdapi.Context{}
	}

	return config, nil
}

// Load 接收字节切片并把内容反序列化为 Config 对象。
// 把反序列化封装起来，不假设数据来源是文件。
func Load(data []byte) (*clientcmdapi.Config, error) {
	config := clientcmdapi.NewConfig()
	// 若文件无数据，返回默认对象而不是失败（DecodeInto 拒绝空输入）
	if len(data) == 0 {
		return config, nil
	}
	decoded, _, err := clientcmdlatest.Codec.Decode(data, &schema.GroupVersionKind{Version: clientcmdlatest.Version, Kind: "Config"}, config)
	if err != nil {
		return nil, err
	}
	return decoded.(*clientcmdapi.Config), nil
}

// WriteToFile 把配置序列化为 yaml 并写入文件。文件不存在时以 0600 权限创建；
// 已存在则直接覆盖其内容。
// 0600 权限防止其他用户读取文件中的凭证；目录用 0755 以便正常的目录遍历。
func WriteToFile(config clientcmdapi.Config, filename string) error {
	content, err := Write(config)
	if err != nil {
		return err
	}
	dir := filepath.Dir(filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	if err := os.WriteFile(filename, content, 0600); err != nil {
		return err
	}
	return nil
}

// lockFile 以独占方式创建 "<file>.lock" 作为跨进程锁。
func lockFile(filename string) error {
	// TODO：找到使用真正文件锁的办法。Windows 和 Linux
	// 可能需要各自的方案。

	// 创建锁文件之前先确保目录存在。
	dir := filepath.Dir(filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(lockName(filename), os.O_CREATE|os.O_EXCL, 0)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}

// unlockFile 删除锁文件。
func unlockFile(filename string) error {
	return os.Remove(lockName(filename))
}

// lockName 约定锁文件名为原文件加 ".lock" 后缀。
func lockName(filename string) string {
	return filename + ".lock"
}

// Write 把配置序列化为 yaml。
// 把序列化封装起来，不假设目标是文件。
func Write(config clientcmdapi.Config) ([]byte, error) {
	return runtime.Encode(clientcmdlatest.Codec, &config)
}

// ResolvePaths 报告是否需要把配置中的相对路径解析为绝对路径。
func (rules ClientConfigLoadingRules) ResolvePaths() bool {
	return !rules.DoNotResolvePaths
}

// ResolveLocalPaths 把配置对象中所有相对路径按其所属条目的 LocationOfOrigin 解析。
// 不能直接在 LoadFromFile 里做这件事，因为那样会导致无法在不改动文件内容的前提下加载文件。
func ResolveLocalPaths(config *clientcmdapi.Config) error {
	for _, cluster := range config.Clusters {
		if len(cluster.LocationOfOrigin) == 0 {
			continue
		}
		base, err := filepath.Abs(filepath.Dir(cluster.LocationOfOrigin))
		if err != nil {
			return fmt.Errorf("could not determine the absolute path of config file %s: %v", cluster.LocationOfOrigin, err)
		}

		if err := ResolvePaths(GetClusterFileReferences(cluster), base); err != nil {
			return err
		}
	}
	for _, authInfo := range config.AuthInfos {
		if len(authInfo.LocationOfOrigin) == 0 {
			continue
		}
		base, err := filepath.Abs(filepath.Dir(authInfo.LocationOfOrigin))
		if err != nil {
			return fmt.Errorf("could not determine the absolute path of config file %s: %v", authInfo.LocationOfOrigin, err)
		}

		if err := ResolvePaths(GetAuthInfoFileReferences(authInfo), base); err != nil {
			return err
		}
	}

	return nil
}

// RelativizeClusterLocalPaths 先调用 ResolveLocalPaths 把路径转为绝对路径。
// 这假定任何新增路径已经是绝对路径，而已有路径会相对 LocationOfOrigin 解析。
func RelativizeClusterLocalPaths(cluster *clientcmdapi.Cluster) error {
	if len(cluster.LocationOfOrigin) == 0 {
		return fmt.Errorf("no location of origin for %s", cluster.Server)
	}
	base, err := filepath.Abs(filepath.Dir(cluster.LocationOfOrigin))
	if err != nil {
		return fmt.Errorf("could not determine the absolute path of config file %s: %v", cluster.LocationOfOrigin, err)
	}

	if err := ResolvePaths(GetClusterFileReferences(cluster), base); err != nil {
		return err
	}
	if err := RelativizePathWithNoBacksteps(GetClusterFileReferences(cluster), base); err != nil {
		return err
	}

	return nil
}

// RelativizeAuthInfoLocalPaths 先调用 ResolveLocalPaths 把路径转为绝对路径。
// 这假定任何新增路径已经是绝对路径，而已有路径会相对 LocationOfOrigin 解析。
func RelativizeAuthInfoLocalPaths(authInfo *clientcmdapi.AuthInfo) error {
	if len(authInfo.LocationOfOrigin) == 0 {
		return fmt.Errorf("no location of origin for %v", authInfo)
	}
	base, err := filepath.Abs(filepath.Dir(authInfo.LocationOfOrigin))
	if err != nil {
		return fmt.Errorf("could not determine the absolute path of config file %s: %v", authInfo.LocationOfOrigin, err)
	}

	if err := ResolvePaths(GetAuthInfoFileReferences(authInfo), base); err != nil {
		return err
	}
	if err := RelativizePathWithNoBacksteps(GetAuthInfoFileReferences(authInfo), base); err != nil {
		return err
	}

	return nil
}

// RelativizeConfigPaths 把整个配置中的引用路径相对化到 base。
func RelativizeConfigPaths(config *clientcmdapi.Config, base string) error {
	return RelativizePathWithNoBacksteps(GetConfigFileReferences(config), base)
}

// ResolveConfigPaths 把整个配置中的引用路径解析为相对 base 的绝对路径。
func ResolveConfigPaths(config *clientcmdapi.Config, base string) error {
	return ResolvePaths(GetConfigFileReferences(config), base)
}

// GetConfigFileReferences 汇总配置中所有需要路径处理的字段指针。
func GetConfigFileReferences(config *clientcmdapi.Config) []*string {
	refs := []*string{}

	for _, cluster := range config.Clusters {
		refs = append(refs, GetClusterFileReferences(cluster)...)
	}
	for _, authInfo := range config.AuthInfos {
		refs = append(refs, GetAuthInfoFileReferences(authInfo)...)
	}

	return refs
}

// GetClusterFileReferences 返回集群配置中需要解析的路径字段。
func GetClusterFileReferences(cluster *clientcmdapi.Cluster) []*string {
	return []*string{&cluster.CertificateAuthority}
}

// GetAuthInfoFileReferences 返回用户配置中需要解析的路径字段。
func GetAuthInfoFileReferences(authInfo *clientcmdapi.AuthInfo) []*string {
	s := []*string{&authInfo.ClientCertificate, &authInfo.ClientKey, &authInfo.TokenFile}
	// 仅当 exec 命令不是基于 PATH 查找时才解析。
	// 不含路径分隔符的命令依赖 PATH 查找，不能被改写为绝对路径。
	if authInfo.Exec != nil && strings.ContainsRune(authInfo.Exec.Command, filepath.Separator) {
		s = append(s, &authInfo.Exec.Command)
	}
	return s
}

// ResolvePaths 把给定的引用更新为相对给定基准目录的绝对路径。
func ResolvePaths(refs []*string, base string) error {
	for _, ref := range refs {
		// 不解析空路径
		if len(*ref) > 0 {
			// 不解析绝对路径
			if !filepath.IsAbs(*ref) {
				*ref = filepath.Join(base, *ref)
			}
		}
	}
	return nil
}

// RelativizePathWithNoBacksteps 在不需要回溯（backstep）的前提下，把给定引用更新为
// 相对给定基准目录的相对路径。需要回溯的路径若已是绝对路径则保持不变；
// 无法相对化的非绝对路径会产生错误。
// 禁止 ".." 前缀是为了让配置文件在移动/分发时不意外指向父目录之外。
func RelativizePathWithNoBacksteps(refs []*string, base string) error {
	for _, ref := range refs {
		// 不相对化空路径
		if len(*ref) > 0 {
			rel, err := MakeRelative(*ref, base)
			if err != nil {
				return err
			}

			// 若出现回溯，就不改动该路径
			if strings.HasPrefix(rel, "../") || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
				if filepath.IsAbs(*ref) {
					continue
				}

				return fmt.Errorf("%v requires backsteps and is not absolute", *ref)
			}

			*ref = rel
		}
	}
	return nil
}

// MakeRelative 返回 path 相对 base 的路径；空 path 原样返回。
func MakeRelative(path, base string) (string, error) {
	if len(path) > 0 {
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return path, err
		}
		return rel, nil
	}
	return path, nil
}

// deduplicate 去除重复值并返回新切片，顺序保持不变。
func deduplicate(s []string) []string {
	encountered := map[string]bool{}
	ret := make([]string, 0)
	for i := range s {
		if encountered[s[i]] {
			continue
		}
		encountered[s[i]] = true
		ret = append(ret, s[i])
	}
	return ret
}
