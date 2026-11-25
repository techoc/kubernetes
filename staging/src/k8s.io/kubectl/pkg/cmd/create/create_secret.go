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

package create

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

// NewCmdCreateSecret 将用于创建各种类型 secret 的子命令组合在一起。
// 这是 create_secret.go 的入口，将由 create.go 调用
func NewCmdCreateSecret(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "secret (docker-registry | generic | tls)",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Create a secret using a specified subcommand"),
		Long:                  secretLong,
		Run:                   cmdutil.DefaultSubCommandRun(ioStreams.ErrOut),
	}
	cmd.AddCommand(NewCmdCreateSecretDockerRegistry(f, ioStreams))
	cmd.AddCommand(NewCmdCreateSecretTLS(f, ioStreams))
	cmd.AddCommand(NewCmdCreateSecretGeneric(f, ioStreams))

	return cmd
}

var (
	secretLong = templates.LongDesc(i18n.T(`
		Create a secret with specified type.
		
		A docker-registry type secret is for accessing a container registry.

		A generic type secret indicate an Opaque secret type.

		A tls type secret holds TLS certificate and its associated key.`))

	secretForGenericLong = templates.LongDesc(i18n.T(`
		Create a secret based on a file, directory, or specified literal value.

		A single secret may package one or more key/value pairs.

		When creating a secret based on a file, the key will default to the basename of the file, and the value will
		default to the file content. If the basename is an invalid key or you wish to chose your own, you may specify
		an alternate key.

		When creating a secret based on a directory, each file whose basename is a valid key in the directory will be
		packaged into the secret. Any directory entries except regular files are ignored (e.g. subdirectories,
		symlinks, devices, pipes, etc).`))

	secretForGenericExample = templates.Examples(i18n.T(`
	  # Create a new secret named my-secret with keys for each file in folder bar
	  kubectl create secret generic my-secret --from-file=path/to/bar

	  # Create a new secret named my-secret with specified keys instead of names on disk
	  kubectl create secret generic my-secret --from-file=ssh-privatekey=path/to/id_rsa --from-file=ssh-publickey=path/to/id_rsa.pub

	  # Create a new secret named my-secret with key1=supersecret and key2=topsecret
	  kubectl create secret generic my-secret --from-literal=key1=supersecret --from-literal=key2=topsecret

	  # Create a new secret named my-secret using a combination of a file and a literal
	  kubectl create secret generic my-secret --from-file=ssh-privatekey=path/to/id_rsa --from-literal=passphrase=topsecret

	  # Create a new secret named my-secret from env files
	  kubectl create secret generic my-secret --from-env-file=path/to/foo.env --from-env-file=path/to/bar.env`))
)

// CreateSecretOptions 保存 'create secret' 子命令的选项
type CreateSecretOptions struct {
	// PrintFlags 保存获取 printer 所需的选项
	PrintFlags *genericclioptions.PrintFlags
	PrintObj   func(obj runtime.Object) error

	// secret 的名称（必填）
	Name string
	// secret 的类型（可选）
	Type string
	// 派生 secret 所用的 FileSources（可选）
	FileSources []string
	// 派生 secret 所用的 LiteralSources（可选）
	LiteralSources []string
	// 派生 secret 所用的 EnvFileSources（可选）
	EnvFileSources []string
	// AppendHash；为 true 时，根据 Secret 的数据和类型派生哈希并追加到名称后
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

// NewSecretOptions 创建一个带有默认值的 *CreateSecretOptions
func NewSecretOptions(ioStreams genericiooptions.IOStreams) *CreateSecretOptions {
	return &CreateSecretOptions{
		PrintFlags: genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		IOStreams:  ioStreams,
	}
}

// NewCmdCreateSecretGeneric 是一个根据文件、目录或字面值创建通用 secret 的命令
func NewCmdCreateSecretGeneric(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewSecretOptions(ioStreams)

	cmd := &cobra.Command{
		Use:                   "generic NAME [--type=string] [--from-file=[key=]source] [--from-literal=key1=value1] [--dry-run=server|client|none]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Create a secret from a local file, directory, or literal value"),
		Long:                  secretForGenericLong,
		Example:               secretForGenericExample,
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

	cmd.Flags().StringSliceVar(&o.FileSources, "from-file", o.FileSources, "Key files can be specified using their file path, in which case a default name will be given to them, or optionally with a name and file path, in which case the given name will be used.  Specifying a directory will iterate each named file in the directory that is a valid secret key.")
	cmd.Flags().StringArrayVar(&o.LiteralSources, "from-literal", o.LiteralSources, "Specify a key and literal value to insert in secret (i.e. mykey=somevalue)")
	cmd.Flags().StringSliceVar(&o.EnvFileSources, "from-env-file", o.EnvFileSources, "Specify the path to a file to read lines of key=val pairs to create a secret.")
	cmd.Flags().StringVar(&o.Type, "type", o.Type, i18n.T("The type of secret to create"))
	cmd.Flags().BoolVar(&o.AppendHash, "append-hash", o.AppendHash, "Append a hash of the secret to its name.")

	cmdutil.AddFieldManagerFlagVar(cmd, &o.FieldManager, "kubectl-create")

	return cmd
}

// Complete 从命令行环境加载数据
func (o *CreateSecretOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
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

// Validate 检查 CreateSecretOptions 是否具备运行所需的足够值
func (o *CreateSecretOptions) Validate() error {
	if len(o.Name) == 0 {
		return fmt.Errorf("name must be specified")
	}
	if len(o.EnvFileSources) > 0 && (len(o.FileSources) > 0 || len(o.LiteralSources) > 0) {
		return fmt.Errorf("from-env-file cannot be combined with from-file or from-literal")
	}
	return nil
}

// Run 调用 createSecret，后者根据 CreateSecretOptions 创建 secret，
// 并向服务端发起 API 调用
func (o *CreateSecretOptions) Run() error {
	secret, err := o.createSecret()
	if err != nil {
		return err
	}
	err = util.CreateOrUpdateAnnotation(o.CreateAnnotation, secret, scheme.DefaultJSONEncoder())
	if err != nil {
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
		secret, err = o.Client.Secrets(o.Namespace).Create(context.TODO(), secret, createOptions)
		if err != nil {
			return fmt.Errorf("failed to create secret %v", err)
		}
	}

	return o.PrintObj(secret)
}

// createSecret 将 CreateSecretOptions 中给出的信息
// 填充为键值对写入 *corev1.Secret
func (o *CreateSecretOptions) createSecret() (*corev1.Secret, error) {
	namespace := ""
	if o.EnforceNamespace {
		namespace = o.Namespace
	}
	secret := newSecretObj(o.Name, namespace, corev1.SecretType(o.Type))
	if len(o.LiteralSources) > 0 {
		if err := handleSecretFromLiteralSources(secret, o.LiteralSources); err != nil {
			return nil, err
		}
	}
	if len(o.FileSources) > 0 {
		if err := handleSecretFromFileSources(secret, o.FileSources); err != nil {
			return nil, err
		}
	}
	if len(o.EnvFileSources) > 0 {
		if err := handleSecretFromEnvFileSources(secret, o.EnvFileSources); err != nil {
			return nil, err
		}
	}
	if o.AppendHash {
		hash, err := hash.SecretHash(secret)
		if err != nil {
			return nil, err
		}
		secret.Name = fmt.Sprintf("%s-%s", secret.Name, hash)
	}

	return secret, nil
}

// newSecretObj 根据给定的 name、namespace 和 secretType 创建一个新的 Secret 对象
func newSecretObj(name, namespace string, secretType corev1.SecretType) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Type: secretType,
		Data: map[string][]byte{},
	}
}

// handleSecretFromLiteralSources 将指定的字面量来源信息
// 添加到给定的 secret 中
func handleSecretFromLiteralSources(secret *corev1.Secret, literalSources []string) error {
	for _, literalSource := range literalSources {
		keyName, value, err := util.ParseLiteralSource(literalSource)
		if err != nil {
			return err
		}
		if err = addKeyFromLiteralToSecret(secret, keyName, []byte(value)); err != nil {
			return err
		}
	}

	return nil
}

// handleSecretFromFileSources 将指定的文件来源信息添加到给定的 secret 中
func handleSecretFromFileSources(secret *corev1.Secret, fileSources []string) error {
	for _, fileSource := range fileSources {
		keyName, filePath, err := util.ParseFileSource(fileSource)
		if err != nil {
			return err
		}
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			switch err := err.(type) {
			case *os.PathError:
				return fmt.Errorf("error reading %s: %v", filePath, err.Err)
			default:
				return fmt.Errorf("error reading %s: %v", filePath, err)
			}
		}
		// 如果 filePath 是目录
		if fileInfo.IsDir() {
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
					if err := addKeyFromFileToSecret(secret, keyName, itemPath); err != nil {
						return err
					}
				}
			}
			// 如果 filepath 是文件
		} else {
			if err := addKeyFromFileToSecret(secret, keyName, filePath); err != nil {
				return err
			}
		}

	}

	return nil
}

// handleSecretFromEnvFileSources 将指定的 env 文件来源信息
// 添加到给定的 secret 中
func handleSecretFromEnvFileSources(secret *corev1.Secret, envFileSources []string) error {
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
			return fmt.Errorf("env secret file cannot be a directory")
		}
		err = cmdutil.AddFromEnvFile(envFileSource, func(key, value string) error {
			return addKeyFromLiteralToSecret(secret, key, []byte(value))
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// addKeyFromFileToSecret 使用给定名称向 Secret 添加一个 key，值取自给定文件路径的内容；
// 若失败则返回错误。
func addKeyFromFileToSecret(secret *corev1.Secret, keyName, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	return addKeyFromLiteralToSecret(secret, keyName, data)
}

// addKeyFromLiteralToSecret 将给定的 key 和数据添加到给定的 secret，
// 如果 key 无效或已存在则返回错误。
func addKeyFromLiteralToSecret(secret *corev1.Secret, keyName string, data []byte) error {
	if errs := validation.IsConfigMapKey(keyName); len(errs) != 0 {
		return fmt.Errorf("%q is not valid key name for a Secret %s", keyName, strings.Join(errs, ";"))
	}
	if _, entryExists := secret.Data[keyName]; entryExists {
		return fmt.Errorf("cannot add key %s, another key by that name already exists", keyName)
	}
	secret.Data[keyName] = data

	return nil
}
