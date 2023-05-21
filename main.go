package main

import (
	"fmt"
	"net/http"

	"github.com/krake-labs/krake/gen/krake/v1/krakev1connect"
	"github.com/krake-labs/krake/pkg"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const address = "localhost:8080"

func main() {
	mux := http.NewServeMux()
	path, handler := krakev1connect.NewKrakeBrokerServiceHandler(pkg.NewKrakeServiceServer())
	mux.Handle(path, handler)
	fmt.Println("... Listening on", address)

	err := http.ListenAndServe(
		address,
		// Use h2c so we can serve HTTP/2 without TLS.
		h2c.NewHandler(mux, &http2.Server{}),
	)
	if err != nil {
		panic(err)
	}
}
