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

package parsers

import (
	"fmt"
	// 注册 sha256 算法，docker 镜像解析器依赖它。
	_ "crypto/sha256"
	// 注册 sha512 算法，供 docker 镜像解析器处理 384/512 位哈希。
	_ "crypto/sha512"

	dockerref "github.com/distribution/reference"
	"github.com/robfig/cron/v3"
)

// ParseImageName 将 docker 镜像字符串解析为三部分：repo、tag 和 digest。
// 若 tag 和 digest 都为空，则返回默认的镜像 tag。
func ParseImageName(image string) (string, string, string, error) {
	named, err := dockerref.ParseNormalizedNamed(image)
	if err != nil {
		return "", "", "", fmt.Errorf("couldn't parse image name %q: %v", image, err)
	}

	repoToPull := named.Name()
	var tag, digest string

	tagged, ok := named.(dockerref.Tagged)
	if ok {
		tag = tagged.Tag()
	}

	digested, ok := named.(dockerref.Digested)
	if ok {
		digest = digested.Digest().String()
	}
	// 既没有 tag 也没有 digest 时，默认使用 "latest"。
	if len(tag) == 0 && len(digest) == 0 {
		tag = "latest"
	}
	return repoToPull, tag, digest, nil
}

// ParseCronScheduleWithPanicRecovery 安全地解析 cron 表达式，
// 捕获 cron.ParseStandard 遇到 "TZ=0" 等畸形表达式时产生的 panic。
func ParseCronScheduleWithPanicRecovery(schedule string) (sched cron.Schedule, err error) {
	defer func() {
		if r := recover(); r != nil {
			sched = nil
			err = fmt.Errorf("invalid schedule format: %v", r)
		}
	}()

	sched, err = cron.ParseStandard(schedule)
	return
}
