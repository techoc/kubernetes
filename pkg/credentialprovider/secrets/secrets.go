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

package secrets

import (
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/credentialprovider"
)

// MakeDockerKeyring 从传入的 Secret 中提取 Docker 凭证，并与默认 keyring 合并；
// 没有可用凭证时直接返回默认 keyring。
func MakeDockerKeyring(passedSecrets []v1.Secret, defaultKeyring credentialprovider.DockerKeyring) (credentialprovider.DockerKeyring, error) {
	providerFromSecrets, err := secretsToTrackedDockerConfigs(passedSecrets)
	if err != nil {
		return nil, err
	}

	if providerFromSecrets == nil {
		return defaultKeyring, nil
	}

	return credentialprovider.UnionDockerKeyring{providerFromSecrets, defaultKeyring}, nil
}

// secretsToTrackedDockerConfigs 把 Secret 中的凭证转换成带来源信息的 keyring。
func secretsToTrackedDockerConfigs(secrets []v1.Secret) (credentialprovider.DockerKeyring, error) {
	provider := &credentialprovider.BasicDockerKeyring{}
	validSecretsFound := 0
	for _, passedSecret := range secrets {
		// 只接受类型正确且 data 非空的 Secret，避免把无关 Secret 当成凭证。
		if dockerConfigJSONBytes, dockerConfigJSONExists := passedSecret.Data[v1.DockerConfigJsonKey]; (passedSecret.Type == v1.SecretTypeDockerConfigJson) && dockerConfigJSONExists && (len(dockerConfigJSONBytes) > 0) {
			dockerConfigJSON := credentialprovider.DockerConfigJSON{}
			if err := json.Unmarshal(dockerConfigJSONBytes, &dockerConfigJSON); err != nil {
				return nil, err
			}

			// 记录来源坐标，供 kubelet 校验镜像是否由该 Secret 提供凭证。
			coords := &credentialprovider.SecretCoordinates{
				UID:       string(passedSecret.UID),
				Namespace: passedSecret.Namespace,
				Name:      passedSecret.Name}

			provider.Add(&credentialprovider.CredentialSource{Secret: coords}, dockerConfigJSON.Auths)
			validSecretsFound++
		} else if dockercfgBytes, dockercfgExists := passedSecret.Data[v1.DockerConfigKey]; (passedSecret.Type == v1.SecretTypeDockercfg) && dockercfgExists && (len(dockercfgBytes) > 0) {
			dockercfg := credentialprovider.DockerConfig{}
			if err := json.Unmarshal(dockercfgBytes, &dockercfg); err != nil {
				return nil, err
			}

			coords := &credentialprovider.SecretCoordinates{
				UID:       string(passedSecret.UID),
				Namespace: passedSecret.Namespace,
				Name:      passedSecret.Name}
			provider.Add(&credentialprovider.CredentialSource{Secret: coords}, dockercfg)
			validSecretsFound++
		}
	}

	// 没有可用 Secret 时返回 nil，让调用方直接使用默认 keyring。
	if validSecretsFound == 0 {
		return nil, nil
	}
	return provider, nil
}
