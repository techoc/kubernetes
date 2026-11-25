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

package version

import (
	"fmt"
	"math"

	"k8s.io/apimachinery/pkg/util/version"
	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// supportedMinorVersionSkew 是客户端与服务端 minor 版本之间支持的最大差异。
// 例如：对于服务端版本 1.19，客户端版本 1.18、1.19 和 1.20 都在支持的版本偏差范围内；
// 对于客户端版本 1.19，服务端版本 1.18、1.19 和 1.20 也都在支持的版本偏差范围内。
const supportedMinorVersionSkew = 1

// 如果客户端与服务端的版本差异超过
// 支持的版本偏差，getVersionSkewWarning 会返回一条警告消息。
func getVersionSkewWarning(clientVersion, serverVersion apimachineryversion.Info) (string, error) {
	parsedClientVersion, err := version.ParseSemantic(clientVersion.GitVersion)
	if err != nil {
		return "", fmt.Errorf("client version error: %w", err)
	}

	parsedServerVersion, err := version.ParseSemantic(serverVersion.GitVersion)
	if err != nil {
		return "", fmt.Errorf("server version error: %w", err)
	}

	majorVersionDifference := math.Abs(float64(parsedClientVersion.Major()) - float64(parsedServerVersion.Major()))
	minorVersionDifference := math.Abs(float64(parsedClientVersion.Minor()) - float64(parsedServerVersion.Minor()))

	if majorVersionDifference > 0 || minorVersionDifference > supportedMinorVersionSkew {
		warningMessage := fmt.Sprintf("version difference between client (%d.%d) and server (%d.%d) exceeds the supported minor version skew of +/-%d",
			parsedClientVersion.Major(), parsedClientVersion.Minor(), parsedServerVersion.Major(), parsedServerVersion.Minor(), supportedMinorVersionSkew)
		return warningMessage, nil
	}
	return "", nil
}
