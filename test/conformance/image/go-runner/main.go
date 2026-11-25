/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
)

func main() {
	if strings.Contains(os.Args[0], "run_e2e.sh") || strings.Contains(os.Args[0], "gorunner") {
		log.Print("warn: calling test with e2e.test is deprecated and will be removed in 1.25, please rely on container manifest to invoke executable")
	}
	env := envWithDefaults(map[string]string{
		resultsDirEnvKey: defaultResultsDir,
		skipEnvKey:       defaultSkip,
		focusEnvKey:      defaultFocus,
		providerEnvKey:   defaultProvider,
		parallelEnvKey:   defaultParallel,
		ginkgoEnvKey:     defaultGinkgoBinary,
		testBinEnvKey:    defaultTestBinary,
	})

	if err := configureAndRunWithEnv(env); err != nil {
		log.Fatal(err)
	}
}

// configureAndRunWithEnv 使用给定的环境进行配置，
// 然后启动测试运行。它会优雅地处理 TERM 信号并终止测试进程，
// 并将日志和结果保存到 RESULTS_DIR 环境变量指定的位置。
// 该位置由用户通过环境变量提供。
func configureAndRunWithEnv(env Getenver) error {
	// 无论是否发生其他错误，都要确保保存结果。这样可以帮助
	// 可能正在轮询结果的使用方。
	resultsDir := env.Getenv(resultsDirEnvKey)
	defer saveResults(resultsDir)

	// 将输出同时打印到 stdout 和日志文件中，该日志文件将作为
	// 结果 tarball 的一部分返回。
	logFilePath := filepath.Join(resultsDir, logFileName)
	// 确保 resultsDir 确实存在
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		log.Printf("The resultsDir %v does not exist, will create it", resultsDir)
		if mkdirErr := os.Mkdir(resultsDir, 0755); mkdirErr != nil {
			return fmt.Errorf("failed to create log directory %v: %w", resultsDir, mkdirErr)
		}
	}
	logFile, err := os.Create(logFilePath)
	if err != nil {
		return fmt.Errorf("failed to create log file %v: %w", logFilePath, err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	cmd := getCmd(env, mw)

	log.Printf("Running command:\n%v\n", cmdInfo(cmd))
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("starting command: %w", err)
	}

	// 优雅地处理信号并关闭进程。
	go setupSigHandler(cmd.Process.Pid)

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("running command: %w", err)
	}

	return nil
}

// setupSigHandler 在收到 TERM 信号时
// 终止给定 PID 对应的进程。
func setupSigHandler(pid int) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// 阻塞直到收到信号。
	log.Println("Now listening for interrupts")
	s := <-c
	log.Printf("Got signal: %v. Shutting down test process (PID: %v)\n", s, pid)
	p, err := os.FindProcess(pid)
	if err != nil {
		log.Printf("Could not find process %v to shut down.\n", pid)
		return
	}
	if err := p.Signal(s); err != nil {
		log.Printf("Failed to signal test process to terminate: %v\n", err)
		return
	}
	log.Printf("Signalled process %v to terminate successfully.\n", pid)
}

// saveResults 将结果目录打包，
// 并把生成的 tarball 路径写入 donefile。
func saveResults(resultsDir string) error {
	log.Printf("Saving results at %v\n", resultsDir)

	err := tarDir(resultsDir, filepath.Join(resultsDir, resultsTarballName))
	if err != nil {
		return fmt.Errorf("tar directory %v: %w", resultsDir, err)
	}

	doneFile := filepath.Join(resultsDir, doneFileName)

	resultsTarball := filepath.Join(resultsDir, resultsTarballName)
	resultsTarball, err = filepath.Abs(resultsTarball)
	if err != nil {
		return fmt.Errorf("failed to find absolute path for %v: %w", resultsTarball, err)
	}

	err = os.WriteFile(doneFile, []byte(resultsTarball), os.FileMode(0777))
	if err != nil {
		return fmt.Errorf("writing donefile: %w", err)
	}

	return nil
}
