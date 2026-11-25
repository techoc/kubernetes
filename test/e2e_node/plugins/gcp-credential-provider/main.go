/*
Copyright 2022 The Kubernetes Authors.

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

package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"gopkg.in/go-jose/go-jose.v2/jwt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	credentialproviderv1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
)

const (
	metadataTokenEndpoint = "http://metadata.google.internal./computeMetadata/v1/instance/service-accounts/default/token"

	pluginModeEnvVar = "PLUGIN_MODE"
)

func main() {
	if err := getCredentials(metadataTokenEndpoint, os.Stdin, os.Stdout); err != nil {
		klog.Fatalf("failed to get credentials: %v", err)
	}
}

func getCredentials(tokenEndpoint string, r io.Reader, w io.Writer) error {
	provider := &provider{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		tokenEndpoint: tokenEndpoint,
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	var authRequest credentialproviderv1.CredentialProviderRequest
	err = json.Unmarshal(data, &authRequest)
	if err != nil {
		return err
	}

	pluginUsingServiceAccount := os.Getenv(pluginModeEnvVar) == "serviceaccount"
	if pluginUsingServiceAccount {
		if len(authRequest.ServiceAccountToken) == 0 {
			return errors.New("service account token is empty")
		}
		expectedAnnotations := map[string]string{
			"domain.io/identity-id":   "123456",
			"domain.io/identity-type": "serviceaccount",
		}
		if !reflect.DeepEqual(authRequest.ServiceAccountAnnotations, expectedAnnotations) {
			return fmt.Errorf("unexpected service account annotations, want: %v, got: %v", expectedAnnotations, authRequest.ServiceAccountAnnotations)
		}
		// 此测试插件实际上并不使用 service account token 进行认证。
		// 我们从 token 中提取 claims 以验证 audience。
		// 这仅用于测试断言，并非真正的安全层。
		// 在此块中完成验证后，继续按默认流程获取凭据。
		c, err := getClaims(authRequest.ServiceAccountToken)
		if err != nil {
			return err
		}
		// token 中的 audience 应与 CredentialProviderConfig 中
		// tokenAttributes.serviceAccountTokenAudience 配置的 audience 一致。
		if len(c.Audience) != 1 || c.Audience[0] != "test-audience" {
			return fmt.Errorf("unexpected audience: %v", c.Audience)
		}
	} else {
		if len(authRequest.ServiceAccountToken) > 0 {
			return errors.New("service account token is not expected")
		}
		if len(authRequest.ServiceAccountAnnotations) > 0 {
			return errors.New("service account annotations are not expected")
		}
	}

	auth, err := provider.Provide(authRequest.Image)
	if err != nil {
		return err
	}

	response := &credentialproviderv1.CredentialProviderResponse{
		TypeMeta: metav1.TypeMeta{
			Kind:       "CredentialProviderResponse",
			APIVersion: "credentialprovider.kubelet.k8s.io/v1",
		},
		CacheKeyType: credentialproviderv1.RegistryPluginCacheKeyType,
		Auth:         auth,
	}

	if pluginUsingServiceAccount {
		response.CacheKeyType = credentialproviderv1.GlobalPluginCacheKeyType
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		// 有意不包含 json.Marshal 的错误，以免将凭据泄露到日志中
		return errors.New("error marshaling response")
	}

	return nil
}

// 当插件以 service account 模式运行时，getClaims 用于从 service account token 中提取 claims
// 这仅用于测试断言，并非真正的安全层。
// 我们获取 claims 并验证 token 的 audience（token 中的 audience 与
// CredentialProviderConfig 中 tokenAttributes.serviceAccountTokenAudience 配置的 audience 一致）。
func getClaims(tokenData string) (claims, error) {
	if strings.HasPrefix(strings.TrimSpace(tokenData), "{") {
		return claims{}, errors.New("token is not a JWS")
	}
	parts := strings.Split(tokenData, ".")
	if len(parts) != 3 {
		return claims{}, errors.New("token is not a JWS")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return claims{}, fmt.Errorf("error decoding token payload: %w", err)
	}

	var c claims
	d := json.NewDecoder(strings.NewReader(string(payload)))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return claims{}, fmt.Errorf("error decoding token payload: %w", err)
	}

	return c, nil
}

type claims struct {
	jwt.Claims
	privateClaims
}

// 复制自 https://github.com/kubernetes/kubernetes/blob/60c4c2b2521fb454ce69dee737e3eb91a25e0535/pkg/serviceaccount/claims.go#L51-L67

type privateClaims struct {
	Kubernetes kubernetes `json:"kubernetes.io,omitempty"`
}

type kubernetes struct {
	Namespace string           `json:"namespace,omitempty"`
	Svcacct   ref              `json:"serviceaccount,omitempty"`
	Pod       *ref             `json:"pod,omitempty"`
	Secret    *ref             `json:"secret,omitempty"`
	Node      *ref             `json:"node,omitempty"`
	WarnAfter *jwt.NumericDate `json:"warnafter,omitempty"`
}

type ref struct {
	Name string `json:"name,omitempty"`
	UID  string `json:"uid,omitempty"`
}
