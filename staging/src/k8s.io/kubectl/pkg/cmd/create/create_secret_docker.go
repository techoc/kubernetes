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

package create

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	secretForDockerRegistryLong = templates.LongDesc(i18n.T(`
		Create a new secret for use with Docker registries.

		Dockercfg secrets are used to authenticate against Docker registries.

		When using the Docker command line to push images, you can authenticate to a given registry by running:
			'$ docker login DOCKER_REGISTRY_SERVER --username=DOCKER_USER --password=DOCKER_PASSWORD --email=DOCKER_EMAIL'.

	That produces a ~/.dockercfg file that is used by subsequent 'docker push' and 'docker pull' commands to
		authenticate to the registry. The email address is optional.

		When creating applications, you may have a Docker registry that requires authentication.  In order for the
		nodes to pull images on your behalf, they must have the credentials.  You can provide this information
		by creating a dockercfg secret and attaching it to your service account.`))

	secretForDockerRegistryExample = templates.Examples(i18n.T(`
		  # If you do not already have a .dockercfg file, create a dockercfg secret directly
		  kubectl create secret docker-registry my-secret --docker-server=DOCKER_REGISTRY_SERVER --docker-username=DOCKER_USER --docker-password=DOCKER_PASSWORD --docker-email=DOCKER_EMAIL

		  # Create a new secret named my-secret from ~/.docker/config.json
		  kubectl create secret docker-registry my-secret --from-file=path/to/.docker/config.json`))
)

// DockerConfigJSON 表示用于拉取镜像的本地 docker 认证配置文件
// 文件。
type DockerConfigJSON struct {
	Auths DockerConfig `json:"auths" datapolicy:"token"`
	// +optional
	HttpHeaders map[string]string `json:"HttpHeaders,omitempty" datapolicy:"token"`
}

// DockerConfig 表示 docker CLI 使用的配置文件。
// 该配置表示从特定镜像仓库拉取镜像时
// 应使用的凭据。
type DockerConfig map[string]DockerConfigEntry

// DockerConfigEntry 保存授予 docker registry 访问权限的用户信息
type DockerConfigEntry struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty" datapolicy:"password"`
	Email    string `json:"email,omitempty"`
	Auth     string `json:"auth,omitempty" datapolicy:"token"`
}

// CreateSecretDockerRegistryOptions 保存 'create secret docker-registry' 子命令的选项
type CreateSecretDockerRegistryOptions struct {
	// PrintFlags 保存获取 printer 所需的选项
	PrintFlags *genericclioptions.PrintFlags
	PrintObj   func(obj runtime.Object) error

	// secret 的名称（必填）
	Name string
	// 派生 secret 所用的 FileSources（可选）
	FileSources []string
	// registry 的用户名（必填）
	Username string
	// registry 的邮箱（可选）
	Email string
	// registry 的密码（必填）
	Password string `datapolicy:"password"`
	// registry 的服务端（必填）
	Server string
	// AppendHash；为 true 时，根据 Secret 派生哈希并追加到名称后
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

// NewSecretDockerRegistryOptions 创建一个带有默认值的 *CreateSecretDockerRegistryOptions
func NewSecretDockerRegistryOptions(ioStreams genericiooptions.IOStreams) *CreateSecretDockerRegistryOptions {
	return &CreateSecretDockerRegistryOptions{
		Server:     "https://index.docker.io/v1/",
		PrintFlags: genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		IOStreams:  ioStreams,
	}
}

// NewCmdCreateSecretDockerRegistry 是一个用于创建配合 Docker registry 使用的 secret 的宏命令
func NewCmdCreateSecretDockerRegistry(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewSecretDockerRegistryOptions(ioStreams)

	cmd := &cobra.Command{
		Use:                   "docker-registry NAME --docker-username=user --docker-password=password --docker-email=email [--docker-server=string] [--from-file=[key=]source] [--dry-run=server|client|none]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Create a secret for use with a Docker registry"),
		Long:                  secretForDockerRegistryLong,
		Example:               secretForDockerRegistryExample,
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

	cmd.Flags().StringVar(&o.Username, "docker-username", o.Username, i18n.T("Username for Docker registry authentication"))
	cmd.Flags().StringVar(&o.Password, "docker-password", o.Password, i18n.T("Password for Docker registry authentication"))
	cmd.Flags().StringVar(&o.Email, "docker-email", o.Email, i18n.T("Email for Docker registry"))
	cmd.Flags().StringVar(&o.Server, "docker-server", o.Server, i18n.T("Server location for Docker registry"))
	cmd.Flags().BoolVar(&o.AppendHash, "append-hash", o.AppendHash, "Append a hash of the secret to its name.")
	cmd.Flags().StringSliceVar(&o.FileSources, "from-file", o.FileSources, "Key files can be specified using their file path, "+
		"in which case a default name of "+corev1.DockerConfigJsonKey+" will be given to them, "+
		"or optionally with a name and file path, in which case the given name will be used. "+
		"Specifying a directory will iterate each named file in the directory that is a valid secret key. "+
		"For this command, the key should always be "+corev1.DockerConfigJsonKey+".")

	cmdutil.AddFieldManagerFlagVar(cmd, &o.FieldManager, "kubectl-create")

	return cmd
}

// Complete 从命令行环境加载数据
func (o *CreateSecretDockerRegistryOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
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

	for i := range o.FileSources {
		if !strings.Contains(o.FileSources[i], "=") {
			o.FileSources[i] = corev1.DockerConfigJsonKey + "=" + o.FileSources[i]
		}
	}
	return nil
}

// Validate 检查 CreateSecretDockerRegistryOptions 是否具备运行所需的足够值
func (o *CreateSecretDockerRegistryOptions) Validate() error {
	if len(o.Name) == 0 {
		return fmt.Errorf("name must be specified")
	}
	if len(o.FileSources) == 0 && (len(o.Username) == 0 || len(o.Password) == 0 || len(o.Server) == 0) {
		return fmt.Errorf("either --from-file or the combination of --docker-username, --docker-password and --docker-server is required")
	}
	return nil
}

// Run 调用 createSecretDockerRegistry，后者根据 CreateSecretDockerRegistryOptions 创建 secretDockerRegistry，
// 并向服务端发起 API 调用
func (o *CreateSecretDockerRegistryOptions) Run() error {
	secretDockerRegistry, err := o.createSecretDockerRegistry()
	if err != nil {
		return err
	}
	err = util.CreateOrUpdateAnnotation(o.CreateAnnotation, secretDockerRegistry, scheme.DefaultJSONEncoder())
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
		secretDockerRegistry, err = o.Client.Secrets(o.Namespace).Create(context.TODO(), secretDockerRegistry, createOptions)
		if err != nil {
			return fmt.Errorf("failed to create secret %v", err)
		}
	}

	return o.PrintObj(secretDockerRegistry)
}

// createSecretDockerRegistry 将 CreateSecretDockerRegistryOptions 中给出的信息
// 填充为键值对写入 *corev1.Secret
func (o *CreateSecretDockerRegistryOptions) createSecretDockerRegistry() (*corev1.Secret, error) {
	namespace := ""
	if o.EnforceNamespace {
		namespace = o.Namespace
	}
	secretDockerRegistry := newSecretObj(o.Name, namespace, corev1.SecretTypeDockerConfigJson)
	if len(o.FileSources) > 0 {
		if err := handleSecretFromFileSources(secretDockerRegistry, o.FileSources); err != nil {
			return nil, err
		}
	} else {
		dockerConfigJSONContent, err := handleDockerCfgJSONContent(o.Username, o.Password, o.Email, o.Server)
		if err != nil {
			return nil, err
		}
		secretDockerRegistry.Data[corev1.DockerConfigJsonKey] = dockerConfigJSONContent
	}
	if o.AppendHash {
		hash, err := hash.SecretHash(secretDockerRegistry)
		if err != nil {
			return nil, err
		}
		secretDockerRegistry.Name = fmt.Sprintf("%s-%s", secretDockerRegistry.Name, hash)
	}
	return secretDockerRegistry, nil
}

// handleDockerCfgJSONContent 序列化 ~/.docker/config.json 文件
func handleDockerCfgJSONContent(username, password, email, server string) ([]byte, error) {
	dockerConfigAuth := DockerConfigEntry{
		Username: username,
		Password: password,
		Email:    email,
		Auth:     encodeDockerConfigFieldAuth(username, password),
	}
	dockerConfigJSON := DockerConfigJSON{
		Auths: map[string]DockerConfigEntry{server: dockerConfigAuth},
	}

	return json.Marshal(dockerConfigJSON)
}

// encodeDockerConfigFieldAuth 返回用户名和密码字符串的 base64 编码
func encodeDockerConfigFieldAuth(username, password string) string {
	fieldValue := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(fieldValue))
}
