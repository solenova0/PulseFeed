// order submits or cancels one order through the engine's gRPC command API.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	pb "pulsefeed/gateway/gen"
)

func fallback(key, value string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return value
}

func main() {
	useTLS, err := strconv.ParseBool(fallback("ENGINE_TLS", "false"))
	if err != nil {
		log.Fatal("ENGINE_TLS must be true or false")
	}
	address := flag.String("addr", fallback("ENGINE_ADDR", "localhost:50051"), "Engine gRPC address")
	id := flag.Uint64("id", 0, "Nonzero unique order ID")
	side := flag.String("side", "buy", "Order side: buy or sell")
	price := flag.Uint64("price", 0, "Limit price in integer ticks")
	quantity := flag.Uint64("qty", 0, "Quantity in integer lots")
	cancelOrder := flag.Bool("cancel", false, "Cancel the specified order ID")
	timeout := flag.Duration("timeout", 3*time.Second, "RPC deadline")
	tlsEnabled := flag.Bool("tls", useTLS, "Use TLS to engine")
	ca := flag.String("ca", os.Getenv("ENGINE_CA_FILE"), "Optional trusted PEM CA file")
	serverName := flag.String("server-name", os.Getenv("ENGINE_SERVER_NAME"), "TLS server name")
	token := flag.String("token", os.Getenv("ENGINE_TOKEN"), "Optional engine Bearer token, requires TLS")
	flag.Parse()
	if *id == 0 || *timeout <= 0 {
		log.Fatal("nonzero -id and positive -timeout required")
	}
	if !*tlsEnabled && (*ca != "" || *serverName != "" || *token != "") {
		log.Fatal("-ca, -server-name and -token require -tls")
	}
	var transport credentials.TransportCredentials = insecure.NewCredentials()
	if *tlsEnabled {
		configuration := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: *serverName}
		if *ca != "" {
			pem, err := os.ReadFile(*ca)
			if err != nil {
				log.Fatal(err)
			}
			roots, err := x509.SystemCertPool()
			if err != nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM(pem) {
				log.Fatal("CA file contains no certificates")
			}
			configuration.RootCAs = roots
		}
		transport = credentials.NewTLS(configuration)
	}
	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(transport))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if *token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*token)
	}
	client := pb.NewMarketDataClient(conn)
	var response *pb.CommandReply
	if *cancelOrder {
		response, err = client.Cancel(ctx, &pb.CancelRequest{OrderId: *id}, grpc.WaitForReady(true))
	} else {
		if *price == 0 || *price > math.MaxUint32 || *quantity == 0 || *quantity > math.MaxUint32 {
			log.Fatal("-price and -qty must be 1..4294967295")
		}
		var orderSide pb.Side
		switch strings.ToLower(*side) {
		case "buy":
			orderSide = pb.Side_BUY
		case "sell":
			orderSide = pb.Side_SELL
		default:
			log.Fatal("-side must be buy or sell")
		}
		response, err = client.Submit(ctx, &pb.OrderRequest{OrderId: *id, Side: orderSide, Price: uint32(*price), Quantity: uint32(*quantity)}, grpc.WaitForReady(true))
	}
	if err != nil {
		log.Fatal(err)
	}
	encoded, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(response)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(encoded))
	if !response.Accepted {
		os.Exit(1)
	}
}
