# gonbdserver

`gonbdserver/nbd` is a small NBD server library in Go.

It aims to be easy to embed: you accept connections and call `nbd.ServeConn`,
or use `nbd.ServeListener` as a convenience accept loop.

## Example

```go
package main

import (
	"context"
	"log"
	"net"

	"github.com/andrewchambers/gonbdserver/nbd"
)

func main() {
	ln, err := net.Listen("unix", "/tmp/nbd.sock")
	if err != nil {
		panic(err)
	}

	opts := nbd.Options{
		Logger: log.Default(),
		ResolveExport: func(ctx context.Context, name string) (*nbd.ExportOptions, error) {
			_ = ctx
			if name != "foo" {
				return nil, nbd.ErrNoSuchExport
			}
			return &nbd.ExportOptions{
				Name: "foo",
				OpenBackend: nbd.OpenFileBackend(nbd.FileBackendOptions{
					Path: "/tmp/nbd.img",
				}),
			}, nil
		},
		ListExports: func(ctx context.Context, yield func(name string) bool) error {
			_ = ctx
			yield("foo")
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := nbd.ServeListener(ctx, ln, opts); err != nil {
		panic(err)
	}
}
```

## Tests

Kernel-level integration tests are behind the `integration` build tag and
require Linux plus root privileges (via `sudo -n`) to run:

```bash
go test -tags=integration ./...
```
