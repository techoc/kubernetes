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

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

var (
	// 从中加载 profile 的目录。
	dirs []string
	poll = flag.Duration("poll", -1, "Poll the directories for new profiles with this interval. Values < 0 disable polling, and exit after loading the profiles.")
)

const (
	parser     = "apparmor_parser"
	apparmorfs = "/sys/kernel/security/apparmor"
)

func main() {
	klog.InitFlags(nil)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [FLAG]... [PROFILE_DIR]...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Load the AppArmor profiles specified in the PROFILE_DIR directories.\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	dirs = flag.Args()
	if len(dirs) == 0 {
		klog.Errorf("Must specify at least one directory.")
		flag.Usage()
		os.Exit(1)
	}

	// 检查是否能找到所需的 parser 二进制文件。
	if _, err := exec.LookPath(parser); err != nil {
		klog.Exitf("Required binary %s not found in PATH", parser)
	}

	// 检查是否能读取已加载的 profile。
	if _, err := getLoadedProfiles(); err != nil {
		klog.Exitf("Unable to access apparmor profiles: %v", err)
	}

	if *poll < 0 {
		runOnce()
	} else {
		pollForever()
	}
}

// 不轮询：运行一次后退出。
func runOnce() {
	if success, newProfiles := loadNewProfiles(); !success {
		if len(newProfiles) > 0 {
			klog.Exitf("Not all profiles were successfully loaded. Loaded: %v", newProfiles)
		} else {
			klog.Exit("Error loading profiles.")
		}
	} else {
		if len(newProfiles) > 0 {
			klog.Infof("Successfully loaded profiles: %v", newProfiles)
		} else {
			klog.Warning("No new profiles found.")
		}
	}
}

// 无限期轮询这些目录。
func pollForever() {
	klog.V(2).Infof("Polling %s every %s", strings.Join(dirs, ", "), poll.String())
	pollFn := func() {
		_, newProfiles := loadNewProfiles()
		if len(newProfiles) > 0 {
			klog.V(2).Infof("Successfully loaded profiles: %v", newProfiles)
		}
	}
	pollFn() // 立即运行一次。
	ticker := time.NewTicker(*poll)
	for range ticker.C {
		pollFn()
	}
}

func loadNewProfiles() (success bool, newProfiles []string) {
	loadedProfiles, err := getLoadedProfiles()
	if err != nil {
		klog.Errorf("Error reading loaded profiles: %v", err)
		return false, nil
	}

	success = true
	for _, dir := range dirs {
		infos, err := os.ReadDir(dir)
		if err != nil {
			klog.Warningf("Error reading %s: %v", dir, err)
			success = false
			continue
		}

		for _, info := range infos {
			path := filepath.Join(dir, info.Name())
			// 如果是目录，或指向目录的符号链接，则跳过。
			resolvedInfo, err := resolveSymlink(dir, info)
			if err != nil {
				klog.Warningf("Error resolving symlink: %v", err)
				continue
			}
			if resolvedInfo.IsDir() {
				// 目录列表只遍历一层。
				klog.V(4).Infof("Skipping directory %s", path)
				continue
			}

			klog.V(4).Infof("Scanning %s for new profiles", path)
			profiles, err := getProfileNames(path)
			if err != nil {
				klog.Warningf("Error reading %s: %v", path, err)
				success = false
				continue
			}

			if unloadedProfiles(loadedProfiles, profiles) {
				if err := loadProfiles(path); err != nil {
					klog.Errorf("Could not load profiles: %v", err)
					success = false
					continue
				}
				// 将新 profile 加入已加载 profile 的列表。
				newProfiles = append(newProfiles, profiles...)
				for _, profile := range profiles {
					loadedProfiles[profile] = true
				}
			}
		}
	}

	return success, newProfiles
}

func getProfileNames(path string) ([]string, error) {
	cmd := exec.Command(parser, "--names", path)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	out, err := cmd.Output()
	if err != nil {
		if stderr.Len() > 0 {
			klog.Warning(stderr.String())
		}
		return nil, fmt.Errorf("error reading profiles from %s: %v", path, err)
	}

	trimmed := strings.TrimSpace(string(out)) // 去掉末尾的 \n
	return strings.Split(trimmed, "\n"), nil
}

func unloadedProfiles(loadedProfiles map[string]bool, profiles []string) bool {
	for _, profile := range profiles {
		if !loadedProfiles[profile] {
			return true
		}
	}
	return false
}

func loadProfiles(path string) error {
	cmd := exec.Command(parser, "--verbose", path)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	out, err := cmd.Output()
	klog.V(2).Infof("Loading profiles from %s:\n%s", path, out)
	if err != nil {
		if stderr.Len() > 0 {
			klog.Warning(stderr.String())
		}
		return fmt.Errorf("error loading profiles from %s: %v", path, err)
	}
	return nil
}

// 如果给定的 fileinfo 是符号链接，则返回其目标的 FileInfo。否则返回给定的 fileinfo。
func resolveSymlink(basePath string, entry os.DirEntry) (os.FileInfo, error) {
	info, err := entry.Info()
	if err != nil {
		return nil, fmt.Errorf("error getting the fileInfo: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		// 不是符号链接。
		return info, nil
	}

	fpath := filepath.Join(basePath, entry.Name())
	resolvedName, err := filepath.EvalSymlinks(fpath)
	if err != nil {
		return nil, fmt.Errorf("error resolving symlink %s: %v", fpath, err)
	}
	resolvedInfo, err := os.Stat(resolvedName)
	if err != nil {
		return nil, fmt.Errorf("error calling stat on %s: %v", resolvedName, err)
	}
	return resolvedInfo, nil
}

// TODO: 这段代码复制自 k8s.io/kubernetes/pkg/security/apparmor.getLoadedProfiles。
//
//	重构该方法，使其以可复用的方式暴露出来，然后删除这个版本。
func getLoadedProfiles() (map[string]bool, error) {
	profilesPath := path.Join(apparmorfs, "profiles")
	profilesFile, err := os.Open(profilesPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %v", profilesPath, err)
	}
	defer profilesFile.Close()

	profiles := map[string]bool{}
	scanner := bufio.NewScanner(profilesFile)
	for scanner.Scan() {
		profileName := parseProfileName(scanner.Text())
		if profileName == "" {
			// 未知的行格式，跳过。
			continue
		}
		profiles[profileName] = true
	}
	return profiles, nil
}

// profiles 文件的格式为每行一个 profile，形式如下：
//
//	namespace://profile-name (mode)
//	profile-name (mode)
//
// 其中 mode 为 {enforce, complain, kill} 之一。"namespace://" 只会出现在带命名空间的 profile 中。
// 就 Kubernetes 而言，我们将命名空间视为 profile 名称的一部分。
func parseProfileName(profileLine string) string {
	modeIndex := strings.IndexRune(profileLine, '(')
	if modeIndex < 0 {
		return ""
	}
	return strings.TrimSpace(profileLine[:modeIndex])
}
