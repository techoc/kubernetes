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

package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	cliflag "k8s.io/component-base/cli/flag"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

type setCredentialsOptions struct {
	configAccess      clientcmd.ConfigAccess
	name              string
	clientCertificate cliflag.StringFlag
	clientKey         cliflag.StringFlag
	token             cliflag.StringFlag `datapolicy:"token"`
	username          cliflag.StringFlag
	password          cliflag.StringFlag `datapolicy:"password"`
	embedCertData     cliflag.Tristate
	authProvider      cliflag.StringFlag

	authProviderArgs         map[string]string
	authProviderArgsToRemove []string

	execCommand            cliflag.StringFlag
	execAPIVersion         cliflag.StringFlag
	execInteractiveMode    cliflag.StringFlag
	execProvideClusterInfo cliflag.Tristate
	execArgs               []string
	execEnv                map[string]string
	execEnvToRemove        []string
}

const (
	flagAuthProvider    = "auth-provider"
	flagAuthProviderArg = "auth-provider-arg"

	flagExecCommand            = "exec-command"
	flagExecAPIVersion         = "exec-api-version"
	flagExecArg                = "exec-arg"
	flagExecEnv                = "exec-env"
	flagExecInteractiveMode    = "exec-interactive-mode"
	flagExecProvideClusterInfo = "exec-provide-cluster-info"
)

var (
	setCredentialsLong = fmt.Sprintf(templates.LongDesc(i18n.T(`
		Set a user entry in kubeconfig.

		Specifying a name that already exists will merge new fields on top of existing values.

		    Client-certificate flags:
		    --%v=certfile --%v=keyfile

		    Bearer token flags:
			  --%v=bearer_token

		    Basic auth flags:
			  --%v=basic_user --%v=basic_password

		Bearer token and basic auth are mutually exclusive.`)), clientcmd.FlagCertFile, clientcmd.FlagKeyFile, clientcmd.FlagBearerToken, clientcmd.FlagUsername, clientcmd.FlagPassword)

	setCredentialsExample = templates.Examples(`
		# Set only the "client-key" field on the "cluster-admin"
		# entry, without touching other values
		kubectl config set-credentials cluster-admin --client-key=~/.kube/admin.key

		# Set basic auth for the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --username=admin --password=uXFGweU9l35qcif

		# Embed client certificate data in the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --client-certificate=~/.kube/admin.crt --embed-certs=true

		# Enable the Google Compute Platform auth provider for the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --auth-provider=gcp

		# Enable the OpenID Connect auth provider for the "cluster-admin" entry with additional arguments
		kubectl config set-credentials cluster-admin --auth-provider=oidc --auth-provider-arg=client-id=foo --auth-provider-arg=client-secret=bar

		# Remove the "client-secret" config value for the OpenID Connect auth provider for the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --auth-provider=oidc --auth-provider-arg=client-secret-

		# Enable new exec auth plugin for the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --exec-command=/path/to/the/executable --exec-api-version=client.authentication.k8s.io/v1beta1

		# Enable new exec auth plugin for the "cluster-admin" entry with interactive mode
		kubectl config set-credentials cluster-admin --exec-command=/path/to/the/executable --exec-api-version=client.authentication.k8s.io/v1beta1 --exec-interactive-mode=Never

		# Define new exec auth plugin arguments for the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --exec-arg=arg1 --exec-arg=arg2

		# Create or update exec auth plugin environment variables for the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --exec-env=key1=val1 --exec-env=key2=val2

		# Remove exec auth plugin environment variables for the "cluster-admin" entry
		kubectl config set-credentials cluster-admin --exec-env=var-to-remove-`)
)

// NewCmdConfigSetCredentials 返回 'config set-credentials' 子命令的 Command 实例
func NewCmdConfigSetCredentials(out io.Writer, configAccess clientcmd.ConfigAccess) *cobra.Command {
	options := &setCredentialsOptions{configAccess: configAccess}
	return newCmdConfigSetCredentials(out, options)
}

// NewCmdConfigSetAuthInfo 返回 'config set-credentials' 子命令的 Command 实例
//
// Deprecated: Use NewCmdConfigSetCredentials instead
func NewCmdConfigSetAuthInfo(out io.Writer, configAccess clientcmd.ConfigAccess) *cobra.Command {
	return NewCmdConfigSetCredentials(out, configAccess)
}

func newCmdConfigSetCredentials(out io.Writer, options *setCredentialsOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use: fmt.Sprintf(
			"set-credentials NAME [--%v=path/to/certfile] "+
				"[--%v=path/to/keyfile] "+
				"[--%v=bearer_token] "+
				"[--%v=basic_user] "+
				"[--%v=basic_password] "+
				"[--%v=provider_name] "+
				"[--%v=key=value] "+
				"[--%v=exec_command] "+
				"[--%v=exec_api_version] "+
				"[--%v=arg] "+
				"[--%v=key=value]",
			clientcmd.FlagCertFile,
			clientcmd.FlagKeyFile,
			clientcmd.FlagBearerToken,
			clientcmd.FlagUsername,
			clientcmd.FlagPassword,
			flagAuthProvider,
			flagAuthProviderArg,
			flagExecCommand,
			flagExecAPIVersion,
			flagExecArg,
			flagExecEnv,
		),
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Set a user entry in kubeconfig"),
		Long:                  setCredentialsLong,
		Example:               setCredentialsExample,
		Run: func(cmd *cobra.Command, args []string) {
			err := options.complete(cmd)
			if err != nil {
				cmd.Help()
				cmdutil.CheckErr(err)
			}
			cmdutil.CheckErr(options.run())
			fmt.Fprintf(out, "User %q set.\n", options.name)
		},
	}

	cmd.Flags().Var(&options.clientCertificate, clientcmd.FlagCertFile, "Path to "+clientcmd.FlagCertFile+" file for the user entry in kubeconfig")
	cmd.MarkFlagFilename(clientcmd.FlagCertFile)
	cmd.Flags().Var(&options.clientKey, clientcmd.FlagKeyFile, "Path to "+clientcmd.FlagKeyFile+" file for the user entry in kubeconfig")
	cmd.MarkFlagFilename(clientcmd.FlagKeyFile)
	cmd.Flags().Var(&options.token, clientcmd.FlagBearerToken, clientcmd.FlagBearerToken+" for the user entry in kubeconfig")
	cmd.Flags().Var(&options.username, clientcmd.FlagUsername, clientcmd.FlagUsername+" for the user entry in kubeconfig")
	cmd.Flags().Var(&options.password, clientcmd.FlagPassword, clientcmd.FlagPassword+" for the user entry in kubeconfig")
	cmd.Flags().Var(&options.authProvider, flagAuthProvider, "Auth provider for the user entry in kubeconfig")
	cmd.Flags().StringSlice(flagAuthProviderArg, nil, "'key=value' arguments for the auth provider")
	cmd.Flags().Var(&options.execCommand, flagExecCommand, "Command for the exec credential plugin for the user entry in kubeconfig")
	cmd.Flags().Var(&options.execAPIVersion, flagExecAPIVersion, "API version of the exec credential plugin for the user entry in kubeconfig")
	cmd.Flags().Var(&options.execInteractiveMode, flagExecInteractiveMode, "InteractiveMode of the exec credentials plugin for the user entry in kubeconfig")
	flagClusterInfo := cmd.Flags().VarPF(&options.execProvideClusterInfo, flagExecProvideClusterInfo, "", "ProvideClusterInfo of the exec credentials plugin for the user entry in kubeconfig")
	flagClusterInfo.NoOptDefVal = "true"
	cmd.Flags().StringSlice(flagExecArg, nil, "New arguments for the exec credential plugin command for the user entry in kubeconfig")
	cmd.Flags().StringArray(flagExecEnv, nil, "'key=value' environment values for the exec credential plugin")
	f := cmd.Flags().VarPF(&options.embedCertData, clientcmd.FlagEmbedCerts, "", "Embed client cert/key for the user entry in kubeconfig")
	f.NoOptDefVal = "true"

	return cmd
}

func (o setCredentialsOptions) run() error {
	err := o.validate()
	if err != nil {
		return err
	}

	config, err := o.configAccess.GetStartingConfig()
	if err != nil {
		return err
	}

	startingStanza, exists := config.AuthInfos[o.name]
	if !exists {
		startingStanza = clientcmdapi.NewAuthInfo()
	}
	authInfo := o.modifyAuthInfo(*startingStanza)
	config.AuthInfos[o.name] = &authInfo

	if err := clientcmd.ModifyConfig(o.configAccess, *config, true); err != nil {
		return err
	}

	return nil
}

func (o *setCredentialsOptions) modifyAuthInfo(existingAuthInfo clientcmdapi.AuthInfo) clientcmdapi.AuthInfo {
	modifiedAuthInfo := existingAuthInfo

	var setToken, setBasic bool

	if o.clientCertificate.Provided() {
		certPath := o.clientCertificate.Value()
		if o.embedCertData.Value() {
			modifiedAuthInfo.ClientCertificateData, _ = os.ReadFile(certPath)
			modifiedAuthInfo.ClientCertificate = ""
		} else {
			certPath, _ = filepath.Abs(certPath)
			modifiedAuthInfo.ClientCertificate = certPath
			if len(modifiedAuthInfo.ClientCertificate) > 0 {
				modifiedAuthInfo.ClientCertificateData = nil
			}
		}
	}
	if o.clientKey.Provided() {
		keyPath := o.clientKey.Value()
		if o.embedCertData.Value() {
			modifiedAuthInfo.ClientKeyData, _ = os.ReadFile(keyPath)
			modifiedAuthInfo.ClientKey = ""
		} else {
			keyPath, _ = filepath.Abs(keyPath)
			modifiedAuthInfo.ClientKey = keyPath
			if len(modifiedAuthInfo.ClientKey) > 0 {
				modifiedAuthInfo.ClientKeyData = nil
			}
		}
	}

	if o.token.Provided() {
		modifiedAuthInfo.Token = o.token.Value()
		setToken = len(modifiedAuthInfo.Token) > 0
	}

	if o.username.Provided() {
		modifiedAuthInfo.Username = o.username.Value()
		setBasic = setBasic || len(modifiedAuthInfo.Username) > 0
	}
	if o.password.Provided() {
		modifiedAuthInfo.Password = o.password.Value()
		setBasic = setBasic || len(modifiedAuthInfo.Password) > 0
	}
	if o.authProvider.Provided() {
		newName := o.authProvider.Value()

		// 仅在现有 auth-provider 为 nil 或不同于新指定的 auth-provider 时覆盖。
		if modifiedAuthInfo.AuthProvider == nil || modifiedAuthInfo.AuthProvider.Name != newName {
			modifiedAuthInfo.AuthProvider = &clientcmdapi.AuthProviderConfig{
				Name: newName,
			}
		}
	}

	if modifiedAuthInfo.AuthProvider != nil {
		if modifiedAuthInfo.AuthProvider.Config == nil {
			modifiedAuthInfo.AuthProvider.Config = make(map[string]string)
		}
		for _, toRemove := range o.authProviderArgsToRemove {
			delete(modifiedAuthInfo.AuthProvider.Config, toRemove)
		}
		for key, value := range o.authProviderArgs {
			modifiedAuthInfo.AuthProvider.Config[key] = value
		}
	}

	if o.execCommand.Provided() {
		newExecCommand := o.execCommand.Value()

		// 如果 Exec 不存在则创建，否则只修改命令
		if modifiedAuthInfo.Exec == nil {
			modifiedAuthInfo.Exec = &clientcmdapi.ExecConfig{
				Command: newExecCommand,
			}
		} else {
			modifiedAuthInfo.Exec.Command = newExecCommand
			// 显式重置 exec 参数
			modifiedAuthInfo.Exec.Args = nil
		}
	}

	// 仅在 Exec 存在时修改后续值，否则忽略这些更改
	if modifiedAuthInfo.Exec != nil {
		if o.execAPIVersion.Provided() {
			modifiedAuthInfo.Exec.APIVersion = o.execAPIVersion.Value()
		}

		// 用新值重写 exec 参数列表
		if o.execArgs != nil {
			modifiedAuthInfo.Exec.Args = o.execArgs
		}

		if o.execInteractiveMode.Provided() {
			modifiedAuthInfo.Exec.InteractiveMode = clientcmdapi.ExecInteractiveMode(o.execInteractiveMode.Value())
		}

		if o.execProvideClusterInfo.Provided() {
			modifiedAuthInfo.Exec.ProvideClusterInfo = o.execProvideClusterInfo.Value()
		}

		// 遍历现有的 exec 环境变量值并移除指定的变量
		if o.execEnvToRemove != nil {
			newExecEnv := []clientcmdapi.ExecEnvVar{}
			for _, value := range modifiedAuthInfo.Exec.Env {
				needToRemove := false
				for _, elemToRemove := range o.execEnvToRemove {
					if value.Name == elemToRemove {
						needToRemove = true
						break
					}
				}
				if !needToRemove {
					newExecEnv = append(newExecEnv, value)
				}
			}
			modifiedAuthInfo.Exec.Env = newExecEnv
		}

		// 为 exec 插件更新或创建指定的环境变量
		if o.execEnv != nil {
			newEnv := []clientcmdapi.ExecEnvVar{}
			for newEnvName, newEnvValue := range o.execEnv {
				needToCreate := true
				for i := 0; i < len(modifiedAuthInfo.Exec.Env); i++ {
					if modifiedAuthInfo.Exec.Env[i].Name == newEnvName {
						// 更新现有值
						needToCreate = false
						modifiedAuthInfo.Exec.Env[i].Value = newEnvValue
						break
					}
				}
				if needToCreate {
					// 创建新的环境变量值
					newEnv = append(newEnv, clientcmdapi.ExecEnvVar{Name: newEnvName, Value: newEnvValue})
				}
			}
			modifiedAuthInfo.Exec.Env = append(modifiedAuthInfo.Exec.Env, newEnv...)
		}
	}

	// 如果设置了任何认证信息，请确保清除其他现有的认证类型
	if setToken || setBasic {
		if !setToken {
			modifiedAuthInfo.Token = ""
		}
		if !setBasic {
			modifiedAuthInfo.Username = ""
			modifiedAuthInfo.Password = ""
		}
	}

	return modifiedAuthInfo
}

func (o *setCredentialsOptions) complete(cmd *cobra.Command) error {
	args := cmd.Flags().Args()
	if len(args) != 1 {
		return fmt.Errorf("unexpected args: %v", args)
	}

	authProviderArgs, err := cmd.Flags().GetStringSlice(flagAuthProviderArg)
	if err != nil {
		return err
	}

	if len(authProviderArgs) > 0 {
		newPairs, removePairs, err := cmdutil.ParsePairs(authProviderArgs, flagAuthProviderArg, true)
		if err != nil {
			return err
		}
		o.authProviderArgs = newPairs
		o.authProviderArgsToRemove = removePairs
	}

	execArgs, err := cmd.Flags().GetStringSlice(flagExecArg)
	if err != nil {
		return err
	}
	if len(execArgs) > 0 {
		o.execArgs = execArgs
	}

	execEnv, err := cmd.Flags().GetStringArray(flagExecEnv)
	if err != nil {
		return err
	}
	if len(execEnv) > 0 {
		newPairs, removePairs, err := cmdutil.ParsePairs(execEnv, flagExecEnv, true)
		if err != nil {
			return err
		}
		o.execEnv = newPairs
		o.execEnvToRemove = removePairs
	}

	o.name = args[0]
	return nil
}

func (o setCredentialsOptions) validate() error {
	if len(o.name) == 0 {
		return errors.New("you must specify a non-empty user name")
	}
	methods := []string{}
	if len(o.token.Value()) > 0 {
		methods = append(methods, fmt.Sprintf("--%v", clientcmd.FlagBearerToken))
	}
	if len(o.username.Value()) > 0 || len(o.password.Value()) > 0 {
		methods = append(methods, fmt.Sprintf("--%v/--%v", clientcmd.FlagUsername, clientcmd.FlagPassword))
	}
	if len(methods) > 1 {
		return fmt.Errorf("you cannot specify more than one authentication method at the same time: %v", strings.Join(methods, ", "))
	}
	if o.embedCertData.Value() {
		certPath := o.clientCertificate.Value()
		keyPath := o.clientKey.Value()
		if certPath == "" && keyPath == "" {
			return fmt.Errorf("you must specify a --%s or --%s to embed", clientcmd.FlagCertFile, clientcmd.FlagKeyFile)
		}
		if certPath != "" {
			if _, err := os.Stat(certPath); err != nil {
				return fmt.Errorf("could not stat %s file %s: %v", clientcmd.FlagCertFile, certPath, err)
			}
		}
		if keyPath != "" {
			if _, err := os.Stat(keyPath); err != nil {
				return fmt.Errorf("could not stat %s file %s: %v", clientcmd.FlagKeyFile, keyPath, err)
			}
		}
	}

	if o.execInteractiveMode.Provided() {
		interactiveMode := o.execInteractiveMode.Value()
		if interactiveMode != string(clientcmdapi.IfAvailableExecInteractiveMode) &&
			interactiveMode != string(clientcmdapi.AlwaysExecInteractiveMode) &&
			interactiveMode != string(clientcmdapi.NeverExecInteractiveMode) {
			return fmt.Errorf("invalid interactive mode type, can be only IfAvailable, Never, Always")
		}
	}

	return nil
}
