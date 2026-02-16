package proto

// RPCService is the name used with net/rpc to register the test daemon service.
const RPCService = "Testd"

// APIVersion is the net/rpc protocol version implemented by the privileged test daemon.
const APIVersion = 2

// DefaultSockPath is the default unix socket path used by the test daemon and tests.
const DefaultSockPath = "/tmp/gonbdserver-testd.sock"

type PingArgs struct{}

type PingReply struct {
	APIVersion int
}

type StartArgs struct {
	NbdSock string
	Export  string
}

type StartReply struct {
	MountDir string

	// Device is provided for debugging/logging only. Clients should not depend on it.
	Device string
}

type RemountArgs struct{}
type RemountReply struct{}

type CleanupArgs struct{}
type CleanupReply struct{}
