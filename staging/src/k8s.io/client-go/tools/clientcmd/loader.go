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

// currentMigrationRules returns a map that holds the history of recommended home directories used in previous versions.
// Any future changes to RecommendedHomeFile and related are expected to add a migration rule here, in order to make
// sure existing config files are migrated to their new locations properly.
//
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
	// IsDefaultConfig returns true if the returned config matches the defaults.
	IsDefaultConfig(*restclient.Config) bool
	// Load returns the latest config
	Load() (*clientcmdapi.Config, error)
}

// KubeconfigGetter 是延迟获取 kubeconfig 的回调类型。
type KubeconfigGetter func() (*clientcmdapi.Config, error)

// ClientConfigGetter 把 KubeconfigGetter 适配为 ClientConfigLoader。
type ClientConfigGetter struct {
	kubeconfigGetter KubeconfigGetter
}

// ClientConfigGetter implements the ClientConfigLoader interface.
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

// ClientConfigLoadingRules is an ExplicitPath and string slice of specific locations that are used for merging together a Config
// Callers can put the chain together however they want, but we'd recommend:
// EnvVarPathFiles if set (a list of files if set) OR the HomeDirectoryPath
// ExplicitPath is special, because if a user specifically requests a certain file be used and error is reported if this file is not present
//
// 合并优先级遵循“前面的文件优先”：map 条目先到先得，标量字段则由后到前合并，
// 详见 Load。
type ClientConfigLoadingRules struct {
	ExplicitPath string
	Precedence   []string

	// MigrationRules is a map of destination files to source files.  If a destination file is not present, then the source file is checked.
	// If the source file is present, then it is copied to the destination file BEFORE any further loading happens.
	MigrationRules map[string]string

	// DoNotResolvePaths indicates whether or not to resolve paths with respect to the originating files.  This is phrased as a negative so
	// that a default object that doesn't set this will usually get the behavior it wants.
	DoNotResolvePaths bool

	// DefaultClientConfig is an optional field indicating what rules to use to calculate a default configuration.
	// This should match the overrides passed in to ClientConfig loader.
	DefaultClientConfig ClientConfig

	// WarnIfAllMissing indicates whether the configuration files pointed by KUBECONFIG environment variable are present or not.
	// In case of missing files, it warns the user about the missing files.
	WarnIfAllMissing bool

	// Warner is the warning log callback to use in case of missing files.
	Warner WarningHandler
}

// WarningHandler allows to set the logging function to use
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

// ClientConfigLoadingRules implements the ClientConfigLoader interface.
var _ ClientConfigLoader = &ClientConfigLoadingRules{}

// NewDefaultClientConfigLoadingRules returns a ClientConfigLoadingRules object with default fields filled in.  You are not required to
// use this constructor
//
// KUBECONFIG 支持用 filepath.SplitList 分隔的多个路径；重复路径会去重，
// 避免同一文件被合并两次。未设置时退回 ~/.kube/config。
func NewDefaultClientConfigLoadingRules() *ClientConfigLoadingRules {
	chain := []string{}
	warnIfAllMissing := false

	envVarFiles := os.Getenv(RecommendedConfigPathEnvVar)
	if len(envVarFiles) != 0 {
		fileList := filepath.SplitList(envVarFiles)
		// prevent the same path load multiple times
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

// Load starts by running the MigrationRules and then
// takes the loading rules and returns a Config object based on following rules.
//
//	if the ExplicitPath, return the unmerged explicit file
//	Otherwise, return a merged config based on the Precedence slice
//
// A missing ExplicitPath file produces an error. Empty filenames or other missing files are ignored.
// Read errors or files with non-deserializable content produce errors.
// The first file to set a particular map key wins and map key's value is never changed.
// BUT, if you set a struct value that is NOT contained inside of map, the value WILL be changed.
// This results in some odd looking logic to merge in one direction, merge in the other, and then merge the two.
// It also means that if two files specify a "red-user", only values from the first file's red-user are used.  Even
// non-conflicting entries from the second file's "red-user" are discarded.
// Relative paths inside of the .kubeconfig files are resolved against the .kubeconfig file's parent folder
// and only absolute file paths are returned.
func (rules *ClientConfigLoadingRules) Load() (*clientcmdapi.Config, error) {
	// 迁移必须在读取之前完成，否则新路径读不到旧数据。
	if err := rules.Migrate(); err != nil {
		return nil, err
	}

	errlist := []error{}
	missingList := []string{}

	kubeConfigFiles := []string{}

	// Make sure a file we were explicitly told to use exists
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
	// read and cache the config files so that we only look at them once
	for _, filename := range kubeConfigFiles {
		if len(filename) == 0 {
			// no work to do
			continue
		}

		config, err := LoadFromFile(filename)

		if os.IsNotExist(err) {
			// skip missing files
			// Add to the missing list to produce a warning
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

	// first merge all of our maps
	// map 合并按优先级正序，使靠前的文件“先写入者胜出”。
	mapConfig := clientcmdapi.NewConfig()

	for _, kubeconfig := range kubeconfigs {
		if err := merge(mapConfig, kubeconfig); err != nil {
			return nil, err
		}
	}

	// merge all of the struct values in the reverse order so that priority is given correctly
	// errors are not added to the list the second time
	// 标量/结构体字段用反向合并：后合并者覆盖，因此先合并低优先级文件。
	nonMapConfig := clientcmdapi.NewConfig()
	for i := len(kubeconfigs) - 1; i >= 0; i-- {
		kubeconfig := kubeconfigs[i]
		if err := merge(nonMapConfig, kubeconfig); err != nil {
			return nil, err
		}
	}

	// since values are overwritten, but maps values are not, we can merge the non-map config on top of the map config and
	// get the values we expect.
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

// Migrate uses the MigrationRules map.  If a destination file is not present, then the source file is checked.
// If the source file is present, then it is copied to the destination file BEFORE any further loading happens.
func (rules *ClientConfigLoadingRules) Migrate() error {
	if rules.MigrationRules == nil {
		return nil
	}

	for destination, source := range rules.MigrationRules {
		if _, err := os.Stat(destination); err == nil {
			// if the destination already exists, do nothing
			continue
		} else if os.IsPermission(err) {
			// if we can't access the file, skip it
			continue
		} else if !os.IsNotExist(err) {
			// if we had an error other than non-existence, fail
			return err
		}

		sourceInfo, err := os.Stat(source)
		if err != nil {
			if os.IsNotExist(err) || os.IsPermission(err) {
				// if the source file doesn't exist or we can't access it, there's no work to do.
				continue
			}

			// if we had an error other than non-existence, fail
			return err
		} else if sourceInfo.IsDir() {
			return fmt.Errorf("cannot migrate %v to %v because it is a directory", source, destination)
		}

		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		// destination created with source perm, but never executable, and subject to umask
		// kubeconfig 可能含凭证，去掉可执行位，并仍受 umask 限制。
		err = os.WriteFile(destination, data, sourceInfo.Mode().Perm()&0666)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetLoadingPrecedence implements ConfigAccess
//
// 返回副本，防止调用方排序等操作污染内部 Precedence。
func (rules *ClientConfigLoadingRules) GetLoadingPrecedence() []string {
	if len(rules.ExplicitPath) > 0 {
		return []string{rules.ExplicitPath}
	}

	// Create a copy in case something tries to sort the returned slice.
	precedence := make([]string, len(rules.Precedence))
	copy(precedence, rules.Precedence)
	return precedence
}

// GetStartingConfig implements ConfigAccess
//
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

// GetDefaultFilename implements ConfigAccess
func (rules *ClientConfigLoadingRules) GetDefaultFilename() string {
	// Explicit file if we have one.
	if rules.IsExplicitFile() {
		return rules.GetExplicitFile()
	}
	// Otherwise, first existing file from precedence.
	for _, filename := range rules.GetLoadingPrecedence() {
		if _, err := os.Stat(filename); err == nil {
			return filename
		}
	}
	// If none exists, use the first from precedence.
	if len(rules.Precedence) > 0 {
		return rules.Precedence[0]
	}
	return ""
}

// IsExplicitFile implements ConfigAccess
func (rules *ClientConfigLoadingRules) IsExplicitFile() bool {
	return len(rules.ExplicitPath) > 0
}

// GetExplicitFile implements ConfigAccess
func (rules *ClientConfigLoadingRules) GetExplicitFile() string {
	return rules.ExplicitPath
}

// IsDefaultConfig returns true if the provided configuration matches the default
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

// LoadFromFile takes a filename and deserializes the contents into Config object
//
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

	// set LocationOfOrigin on every Cluster, User, and Context
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

// Load takes a byte slice and deserializes the contents into Config object.
// Encapsulates deserialization without assuming the source is a file.
func Load(data []byte) (*clientcmdapi.Config, error) {
	config := clientcmdapi.NewConfig()
	// if there's no data in a file, return the default object instead of failing (DecodeInto reject empty input)
	if len(data) == 0 {
		return config, nil
	}
	decoded, _, err := clientcmdlatest.Codec.Decode(data, &schema.GroupVersionKind{Version: clientcmdlatest.Version, Kind: "Config"}, config)
	if err != nil {
		return nil, err
	}
	return decoded.(*clientcmdapi.Config), nil
}

// WriteToFile serializes the config to yaml and writes it out to a file.  If not present, it creates the file with the mode 0600.  If it is present
// it stomps the contents
//
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
	// TODO: find a way to do this with actual file locks. Will
	// probably need separate solution for windows and Linux.

	// Make sure the dir exists before we try to create a lock file.
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

// Write serializes the config to yaml.
// Encapsulates serialization without assuming the destination is a file.
func Write(config clientcmdapi.Config) ([]byte, error) {
	return runtime.Encode(clientcmdlatest.Codec, &config)
}

// ResolvePaths 报告是否需要把配置中的相对路径解析为绝对路径。
func (rules ClientConfigLoadingRules) ResolvePaths() bool {
	return !rules.DoNotResolvePaths
}

// ResolveLocalPaths resolves all relative paths in the config object with respect to the stanza's LocationOfOrigin
// this cannot be done directly inside of LoadFromFile because doing so there would make it impossible to load a file without
// modification of its contents.
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

// RelativizeClusterLocalPaths first absolutizes the paths by calling ResolveLocalPaths.  This assumes that any NEW path is already
// absolute, but any existing path will be resolved relative to LocationOfOrigin
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

// RelativizeAuthInfoLocalPaths first absolutizes the paths by calling ResolveLocalPaths.  This assumes that any NEW path is already
// absolute, but any existing path will be resolved relative to LocationOfOrigin
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
	// Only resolve exec command if it isn't PATH based.
	// 不含路径分隔符的命令依赖 PATH 查找，不能被改写为绝对路径。
	if authInfo.Exec != nil && strings.ContainsRune(authInfo.Exec.Command, filepath.Separator) {
		s = append(s, &authInfo.Exec.Command)
	}
	return s
}

// ResolvePaths updates the given refs to be absolute paths, relative to the given base directory
func ResolvePaths(refs []*string, base string) error {
	for _, ref := range refs {
		// Don't resolve empty paths
		if len(*ref) > 0 {
			// Don't resolve absolute paths
			if !filepath.IsAbs(*ref) {
				*ref = filepath.Join(base, *ref)
			}
		}
	}
	return nil
}

// RelativizePathWithNoBacksteps updates the given refs to be relative paths, relative to the given base directory as long as they do not require backsteps.
// Any path requiring a backstep is left as-is as long it is absolute.  Any non-absolute path that can't be relativized produces an error
//
// 禁止 ".." 前缀是为了让配置文件在移动/分发时不意外指向父目录之外。
func RelativizePathWithNoBacksteps(refs []*string, base string) error {
	for _, ref := range refs {
		// Don't relativize empty paths
		if len(*ref) > 0 {
			rel, err := MakeRelative(*ref, base)
			if err != nil {
				return err
			}

			// if we have a backstep, don't mess with the path
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

// deduplicate removes any duplicated values and returns a new slice, keeping the order unchanged
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
