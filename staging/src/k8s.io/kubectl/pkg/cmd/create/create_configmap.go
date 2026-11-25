/*
Copyright 2016 The Kubernetes Authors.

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

package create

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/hash"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	configMapLong = templates.LongDesc(i18n.T(`
		Create a config map based on a file, directory, or specified literal value.

		A single config map may package one or more key/value pairs.

		When creating a config map based on a file, the key will default to the basename of the file, and the value will
		default to the file content.  If the basename is an invalid key, you may specify an alternate key.

		When creating a config map based on a directory, each file whose basename is a valid key in the directory will be
		packaged into the config map.  Any directory entries except regular files are ignored (e.g. subdirectories,
		symlinks, devices, pipes, etc).`))

	configMapExample = templates.Examples(i18n.T(`
		  # Create a new config map named my-config based on folder bar
		  kubectl create configmap my-config --from-file=path/to/bar

		  # Create a new config map named my-config with specified keys instead of file basenames on disk
		  kubectl create configmap my-config --from-file=key1=/path/to/bar/file1.txt --from-file=key2=/path/to/bar/file2.txt

		  # Create a new config map named my-config with key1=config1 and key2=config2
		  kubectl create configmap my-config --from-literal=key1=config1 --from-literal=key2=config2

		  # Create a new config map named my-config with key=value pairs from an env file
		  kubectl create configmap my-config --from-env-file=path/to/bar.env`))
)

// ConfigMapOptions 保存 create configmap 子命令的属性
type ConfigMapOptions struct {
	// PrintFlags 保存获取 printer 所需的选项
	PrintFlags *genericclioptions.PrintFlags
	PrintObj   func(obj runtime.Object) error

	// configMap 的名称（必填）
	Name string
	// configMap 的类型（可选）
	Type string
	// 派生 configMap 所用的 FileSources（可选）
	FileSources []string
	// 派生 configMap 所用的 LiteralSources（可选）
	LiteralSources []string
	// 派生 configMap 所用的 EnvFileSources（可选）
	EnvFileSources []string
	// AppendHash；为 true 时，根据 ConfigMap 派生哈希并追加到名称后
	AppendHash bool

	FieldManager     string
	CreateAnnotation bool
	Namespace        string
	EnforceNamespace bool

	Client              corev1client.CoreV1Interface
	DryRunStrategy      cmdutil.DryRunStrategy
	ValidationDirective string

	genericiooptions.IOStreams
}

// NewConfigMapOptions 创建一个带有默认值的 *ConfigMapOptions
func NewConfigMapOptions(ioStreams genericiooptions.IOStreams) *ConfigMapOptions {
	return &ConfigMapOptions{
		PrintFlags: genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		IOStreams:  ioStreams,
	}
}

// NewCmdCreateConfigMap 创建 `create configmap` Cobra 命令
func NewCmdCreateConfigMap(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewConfigMapOptions(ioStreams)

	cmd := &cobra.Command{
		Use:                   "configmap NAME [--from-file=[key=]source] [--from-literal=key1=value1] [--dry-run=server|client|none]",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"cm"},
		Short:                 i18n.T("Create a config map from a local file, directory or literal value"),
		Long:                  configMapLong,
		Example:               configMapExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}
	o.PrintFlags.AddFlags(cmd)

	cmdutil.AddApplyAnnotationFlags(cmd)
	cmdutil.AddValidateFlags(cmd)
	cmdutil.AddDryRunFlag(cmd)

	cmd.Flags().StringSliceVar(&o.FileSources, "from-file", o.FileSources, "Key file can be specified using its file path, in which case file basename will be used as configmap key, or optionally with a key and file path, in which case the given key will be used.  Specifying a directory will iterate each named file in the directory whose basename is a valid configmap key.")
	cmd.Flags().StringArrayVar(&o.LiteralSources, "from-literal", o.LiteralSources, "Specify a key and literal value to insert in configmap (i.e. mykey=somevalue)")
	cmd.Flags().StringSliceVar(&o.EnvFileSources, "from-env-file", o.EnvFileSources, "Specify the path to a file to read lines of key=val pairs to create a configmap.")
	cmd.Flags().BoolVar(&o.AppendHash, "append-hash", o.AppendHash, "Append a hash of the configmap to its name.")

	cmdutil.AddFieldManagerFlagVar(cmd, &o.FieldManager, "kubectl-create")

	return cmd
}

// Complete 从命令行环境加载数据
func (o *ConfigMapOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	var err error
	o.Name, err = NameFromCommandArgs(cmd, args)
	if err != nil {
		return err
	}

	restConfig, err := f.ToRESTConfig()
	if err != nil {
		return err
	}

	o.Client, err = corev1client.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	o.CreateAnnotation = cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag)

	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}

	o.Namespace, o.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)
	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}
	o.PrintObj = func(obj runtime.Object) error {
		return printer.PrintObj(obj, o.Out)
	}

	o.ValidationDirective, err = cmdutil.GetValidationDirective(cmd)
	if err != nil {
		return err
	}

	return nil
}

// Validate 检查 ConfigMapOptions 是否具备运行所需的足够值
func (o *ConfigMapOptions) Validate() error {
	if len(o.Name) == 0 {
		return fmt.Errorf("name must be specified")
	}
	if len(o.EnvFileSources) > 0 && (len(o.FileSources) > 0 || len(o.LiteralSources) > 0) {
		return fmt.Errorf("from-env-file cannot be combined with from-file or from-literal")
	}
	return nil
}

// Run 调用 createConfigMap 并为 configMap 对象填充值
func (o *ConfigMapOptions) Run() error {
	configMap, err := o.createConfigMap()
	if err != nil {
		return err
	}
	if err := util.CreateOrUpdateAnnotation(o.CreateAnnotation, configMap, scheme.DefaultJSONEncoder()); err != nil {
		return err
	}
	if o.DryRunStrategy != cmdutil.DryRunClient {
		createOptions := metav1.CreateOptions{}
		if o.FieldManager != "" {
			createOptions.FieldManager = o.FieldManager
		}
		createOptions.FieldValidation = o.ValidationDirective
		if o.DryRunStrategy == cmdutil.DryRunServer {
			createOptions.DryRun = []string{metav1.DryRunAll}
		}
		configMap, err = o.Client.ConfigMaps(o.Namespace).Create(context.TODO(), configMap, createOptions)
		if err != nil {
			return fmt.Errorf("failed to create configmap: %v", err)
		}
	}

	return o.PrintObj(configMap)
}

// createConfigMap 将 ConfigMapOptions 中给出的信息
// 填充为键值对写入 *corev1.ConfigMap
func (o *ConfigMapOptions) createConfigMap() (*corev1.ConfigMap, error) {
	namespace := ""
	if o.EnforceNamespace {
		namespace = o.Namespace
	}

	configMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      o.Name,
			Namespace: namespace,
		},
	}
	configMap.Name = o.Name
	configMap.Data = map[string]string{}
	configMap.BinaryData = map[string][]byte{}

	if len(o.FileSources) > 0 {
		if err := handleConfigMapFromFileSources(configMap, o.FileSources); err != nil {
			return nil, err
		}
	}
	if len(o.LiteralSources) > 0 {
		if err := handleConfigMapFromLiteralSources(configMap, o.LiteralSources); err != nil {
			return nil, err
		}
	}
	if len(o.EnvFileSources) > 0 {
		if err := handleConfigMapFromEnvFileSources(configMap, o.EnvFileSources); err != nil {
			return nil, err
		}
	}
	if o.AppendHash {
		hash, err := hash.ConfigMapHash(configMap)
		if err != nil {
			return nil, err
		}
		configMap.Name = fmt.Sprintf("%s-%s", configMap.Name, hash)
	}

	return configMap, nil
}

// handleConfigMapFromLiteralSources 将指定的字面量来源信息
// 添加到给定的 configMap 中。
func handleConfigMapFromLiteralSources(configMap *corev1.ConfigMap, literalSources []string) error {
	for _, literalSource := range literalSources {
		keyName, value, err := util.ParseLiteralSource(literalSource)
		if err != nil {
			return err
		}
		err = addKeyFromLiteralToConfigMap(configMap, keyName, value)
		if err != nil {
			return err
		}
	}

	return nil
}

// handleConfigMapFromFileSources 将指定的文件来源信息
// 添加到给定的 configMap 中
func handleConfigMapFromFileSources(configMap *corev1.ConfigMap, fileSources []string) error {
	for _, fileSource := range fileSources {
		keyName, filePath, err := util.ParseFileSource(fileSource)
		if err != nil {
			return err
		}
		info, err := os.Stat(filePath)
		if err != nil {
			switch err := err.(type) {
			case *os.PathError:
				return fmt.Errorf("error reading %s: %v", filePath, err.Err)
			default:
				return fmt.Errorf("error reading %s: %v", filePath, err)
			}

		}
		if info.IsDir() {
			if strings.Contains(fileSource, "=") {
				return fmt.Errorf("cannot give a key name for a directory path")
			}
			fileList, err := os.ReadDir(filePath)
			if err != nil {
				return fmt.Errorf("error listing files in %s: %v", filePath, err)
			}
			for _, item := range fileList {
				itemPath := filepath.Join(filePath, item.Name())
				if item.Type().IsRegular() {
					keyName = item.Name()
					err = addKeyFromFileToConfigMap(configMap, keyName, itemPath)
					if err != nil {
						return err
					}
				}
			}
		} else {
			if err := addKeyFromFileToConfigMap(configMap, keyName, filePath); err != nil {
				return err
			}

		}
	}
	return nil
}

// handleConfigMapFromEnvFileSources 将指定的 env 文件来源信息
// 添加到给定的 configMap 中
func handleConfigMapFromEnvFileSources(configMap *corev1.ConfigMap, envFileSources []string) error {
	for _, envFileSource := range envFileSources {
		info, err := os.Stat(envFileSource)
		if err != nil {
			switch err := err.(type) {
			case *os.PathError:
				return fmt.Errorf("error reading %s: %v", envFileSource, err.Err)
			default:
				return fmt.Errorf("error reading %s: %v", envFileSource, err)
			}
		}
		if info.IsDir() {
			return fmt.Errorf("env config file cannot be a directory")
		}
		err = cmdutil.AddFromEnvFile(envFileSource, func(key, value string) error {
			return addKeyFromLiteralToConfigMap(configMap, key, value)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// addKeyFromFileToConfigMap 使用给定名称向 ConfigMap 添加一个 key，值取自给定文件路径的内容；
// 若失败则返回错误。
func addKeyFromFileToConfigMap(configMap *corev1.ConfigMap, keyName, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	if utf8.Valid(data) {
		return addKeyFromLiteralToConfigMap(configMap, keyName, string(data))
	}
	err = validateNewConfigMap(configMap, keyName)
	if err != nil {
		return err
	}
	configMap.BinaryData[keyName] = data

	return nil
}

// addKeyFromLiteralToConfigMap 将给定的 key 和数据添加到给定的 config map，
// 如果 key 无效或已存在则返回错误。
func addKeyFromLiteralToConfigMap(configMap *corev1.ConfigMap, keyName, data string) error {
	err := validateNewConfigMap(configMap, keyName)
	if err != nil {
		return err
	}
	configMap.Data[keyName] = data

	return nil
}

// validateNewConfigMap 检查 key 名称是否有效
// 注意，ConfigMap key 的规则与 SecretKey 的规则完全相同。
func validateNewConfigMap(configMap *corev1.ConfigMap, keyName string) error {
	if errs := validation.IsConfigMapKey(keyName); len(errs) > 0 {
		return fmt.Errorf("%q is not a valid key name for a ConfigMap: %s", keyName, strings.Join(errs, ","))
	}
	if _, exists := configMap.Data[keyName]; exists {
		return fmt.Errorf("cannot add key %q, another key by that name already exists in Data for ConfigMap %q", keyName, configMap.Name)
	}
	if _, exists := configMap.BinaryData[keyName]; exists {
		return fmt.Errorf("cannot add key %q, another key by that name already exists in BinaryData for ConfigMap %q", keyName, configMap.Name)
	}

	return nil
}
