//go:build integration
// +build integration

package nbd

import (
	"flag"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"text/template"
	"time"
)

const integrationConfigTemplate = `
servers:
- protocol: unix
  address: {{.TempDir}}/nbd.sock
  exports:
  - name: foo
    driver: {{.Driver}}
    path: {{.TempDir}}/nbd.img
logging:
`

type TestConfig struct {
	TempDir string
	Driver  string
}

type NbdInstance struct {
	t           *testing.T
	quit        chan struct{}
	closed      bool
	closedMutex sync.Mutex
	TestConfig
}

func StartNbd(t *testing.T, tc TestConfig) *NbdInstance {
	t.Helper()

	if tc.Driver == "" {
		tc.Driver = "file"
	}

	ni := &NbdInstance{
		t:          t,
		quit:       make(chan struct{}),
		TestConfig: tc,
	}

	tempDir, err := os.MkdirTemp("", "gonbdserver-integration-")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	ni.TempDir = tempDir

	confFile := filepath.Join(ni.TempDir, "gonbdserver.conf")
	tpl := template.Must(template.New("config").Parse(integrationConfigTemplate))
	cf, err := os.Create(confFile)
	if err != nil {
		t.Fatalf("creating config file: %v", err)
	}
	if err := tpl.Execute(cf, ni.TestConfig); err != nil {
		cf.Close()
		t.Fatalf("executing config template: %v", err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("closing config file: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{
		"gonbdserver",
		"-f",
		"-c",
		confFile,
	}
	flag.Parse()
	os.Args = oldArgs

	control := &Control{quit: ni.quit}
	go Run(control)
	time.Sleep(100 * time.Millisecond)

	return ni
}

func (ni *NbdInstance) CloseConnection() {
	ni.closedMutex.Lock()
	defer ni.closedMutex.Unlock()
	if ni.closed {
		return
	}
	close(ni.quit)
	ni.closed = true
}

func (ni *NbdInstance) Close() {
	ni.CloseConnection()
	time.Sleep(100 * time.Millisecond)
	_ = os.RemoveAll(ni.TempDir)
}

func (ni *NbdInstance) CreateFile(t *testing.T, size int64) error {
	t.Helper()
	filename := filepath.Join(ni.TempDir, "nbd.img")
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
}
