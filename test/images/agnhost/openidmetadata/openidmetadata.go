/*
Copyright 2020 The Kubernetes Authors.

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

// Package openidmetadata 测试 OIDC 发现端点，
// 它们是 ServiceAccountIssuerDiscovery 特性的一部分。
package openidmetadata

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"time"

	"github.com/coreos/go-oidc"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"
	"gopkg.in/go-jose/go-jose.v2/jwt"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
)

// CmdTestServiceAccountIssuerDiscovery 由 agnhost Cobra 使用。
var CmdTestServiceAccountIssuerDiscovery = &cobra.Command{
	Use:   "test-service-account-issuer-discovery",
	Short: "Tests the ServiceAccountIssuerDiscovery feature",
	Long: "Reads in a mounted token and attempts to verify it against the API server's " +
		"OIDC endpoints, using a third-party OIDC implementation.",
	Args: cobra.MaximumNArgs(0),
	Run:  main,
}

var (
	tokenPath string
	audience  string
)

func init() {
	fs := CmdTestServiceAccountIssuerDiscovery.Flags()
	fs.StringVar(&tokenPath, "token-path", "", "Path to read service account token from.")
	fs.StringVar(&audience, "audience", "", "Audience to check on received token.")
}

func main(cmd *cobra.Command, args []string) {
	raw, err := gettoken()
	if err != nil {
		log.Fatal(err)
	}
	log.Print("OK: Got token")

	/*
		  为同时支持集群内发现与外部（非 kube-apiserver）
		  发现：
		  1. 先尝试集群内发现。仅信任集群 CA。
		     若验证通过，则提前成功退出。此次尝试携带
			 bearer token，因此只信任集群 CA，以避免
			 意外将 token 发送给某个外部端点。
		  2. 若集群内发现未通过，则假定发现文档和 JWKS 端点
		     均由外部提供，而不是由 kube-apiserver 提供，
			 再重试一次。此次尝试完全不传递
			 bearer token。
	*/

	log.Print("validating with in-cluster discovery")
	inClusterCtx, err := withInClusterOauth2Client(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if err := validate(inClusterCtx, raw); err == nil {
		os.Exit(0)
	} else {
		log.Print("failed to validate with in-cluster discovery: ", err)
	}

	log.Print("falling back to validating with external discovery")
	externalCtx, err := withExternalOAuth2Client(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if err := validate(externalCtx, raw); err != nil {
		log.Fatal(err)
	}
}

func validate(ctx context.Context, raw string) error {
	tok, err := jwt.ParseSigned(raw)
	if err != nil {
		log.Fatal(err)
	}
	var unsafeClaims claims
	if err := tok.UnsafeClaimsWithoutVerification(&unsafeClaims); err != nil {
		log.Fatal(err)
	}
	log.Printf("OK: got issuer %s", unsafeClaims.Issuer)
	log.Printf("Full, not-validated claims: \n%#v", unsafeClaims)

	if runtime.GOOS == "windows" {
		if err := ensureWindowsDNSAvailability(unsafeClaims.Issuer); err != nil {
			log.Fatal(err)
		}
	}

	iss, err := oidc.NewProvider(ctx, unsafeClaims.Issuer)
	if err != nil {
		return err
	}
	log.Printf("OK: Constructed OIDC provider for issuer %v", unsafeClaims.Issuer)

	// 重要：下面列出的算法必须与以下内容保持同步：
	// - pkg/serviceaccount/externaljwt/plugin/plugin.go validateJWTHeader
	// - pkg/serviceaccount/jwt.go signerFromRSAPrivateKey
	// - pkg/serviceaccount/jwt.go signerFromECDSAPrivateKey
	// - test/images/agnhost/openidmetadata/openidmetadata.go validate SupportedSigningAlgs

	validTok, err := iss.Verifier(&oidc.Config{
		ClientID:             audience,
		SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256, oidc.ES384, oidc.ES512},
	}).Verify(ctx, raw)
	if err != nil {
		return err
	}
	log.Print("OK: Validated signature on JWT")

	var safeClaims claims
	if err := validTok.Claims(&safeClaims); err != nil {
		return err
	}
	log.Print("OK: Got valid claims from token!")
	log.Printf("Full, validated claims: \n%#v", &safeClaims)
	return nil
}

type kubeName struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

type kubeClaims struct {
	Namespace      string   `json:"namespace"`
	ServiceAccount kubeName `json:"serviceaccount"`
}

type claims struct {
	jwt.Claims

	Kubernetes kubeClaims `json:"kubernetes.io"`
}

func (k *claims) String() string {
	return fmt.Sprintf("%s/%s for %s", k.Kubernetes.Namespace, k.Kubernetes.ServiceAccount.Name, k.Audience)
}

func gettoken() (string, error) {
	b, err := os.ReadFile(tokenPath)
	return string(b), err
}

func withExternalOAuth2Client(ctx context.Context) (context.Context, error) {
	// 使用带有系统根证书的默认 http transport，
	// 因为这是针对外部互联网进行验证。
	return context.WithValue(ctx,
		// `oidc` 库会识别 oauth2.HTTPClient context key；若设置了该 key，
		// 库将使用提供的 http.Client，而不是默认的 HTTP client。
		oauth2.HTTPClient, &http.Client{
			Transport: http.DefaultTransport,
		}), nil
}

func withInClusterOauth2Client(ctx context.Context) (context.Context, error) {
	// 使用集群内配置，以便信任 kube-apiserver 并向其进行身份验证
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	rt, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("could not get roundtripper: %v", err)
	}

	return context.WithValue(ctx,
		// `oidc` 库会识别 oauth2.HTTPClient context key；若设置了该 key，
		// 库将使用提供的 http.Client，而不是默认的 HTTP client。
		oauth2.HTTPClient, &http.Client{
			Transport: rt,
		}), nil
}

// 由于以 dockershim 为容器运行时的 Windows 节点的网络设置方式，
// DNS 可能在容器启动一段时间后才可用。此时，在调用 oidc.NewProvider
// 之前，我们应确保能够解析 issuer，
// 然后再发起发现请求。
// See https://github.com/kubernetes/kubernetes/issues/99470 for more details.
func ensureWindowsDNSAvailability(issuer string) error {
	log.Println("Ensuring Windows DNS availability")

	u, err := url.Parse(issuer)
	if err != nil {
		return err
	}

	return wait.PollImmediate(5*time.Second, 20*time.Second, func() (bool, error) {
		ips, err := net.LookupHost(u.Host)
		if err != nil {
			log.Println(err)
			return false, nil
		}
		log.Printf("OK: Resolved host %s: %v", u.Host, ips)
		return true, nil
	})
}
