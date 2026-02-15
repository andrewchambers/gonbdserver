# gonbdserver

`gonbdserver/nbd` is a small NBD server library in Go.

It aims to be easy to embed: you create a `nbd.Server` with `nbd.Options` and
call `Serve` with a context.

## Example

```go
package main

import (
	"context"
	"log"

	"github.com/andrewchambers/gonbdserver/nbd"
)

func main() {
	srv, err := nbd.NewServer(nbd.Options{
		Logger: log.Default(),
		Listeners: []nbd.ListenerOptions{
			{
				Network: "unix",
				Address: "/tmp/nbd.sock",
				Exports: []nbd.ExportOptions{
					{
						Name: "foo",
						OpenBackend: nbd.OpenFileBackend(nbd.FileBackendOptions{
							Path: "/tmp/nbd.img",
						}),
					},
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Serve(ctx); err != nil {
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
