// Trustline file size: 3.943158636 GB
// Non-trustline size: 44935076 MB, 95 MB per key

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"math/rand"
	"time"

	"github.com/stellar/go/clients/stellarcore"
	"github.com/stellar/go/xdr"
)

func usage() {
	fmt.Println("Usage: ./submit-requests <core url> <ledger key file> [threads=1]")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	coreUrl := os.Args[1]
	ledgerKeyFile := os.Args[2]
	var threads int = 1

	if len(os.Args) >= 4 {
		threadsArg := os.Args[3]
		threadsParsed, err := strconv.ParseUint(threadsArg, 10, 32)
		if err != nil || threadsParsed <= 0 {
			fmt.Printf("Error parsing thread count (%s): %v\n", threadsArg, err)
			usage()
		}

		threads = int(threadsParsed)
	}

	ledgerKeyBytes, err := os.ReadFile(ledgerKeyFile)
	if err != nil {
		fmt.Printf("Error reading ledger key file (%s): %v\n", ledgerKeyFile, err)
		usage()
	}

	ledgerKeyStrings := strings.Split(string(ledgerKeyBytes), "\n")
	ledgerKeys := make([]xdr.LedgerKey, 0, len(ledgerKeyStrings))
	for i, s := range ledgerKeyStrings {
		trimmed := strings.TrimSpace(s)
		if trimmed == "" {
			continue
		}

		var key xdr.LedgerKey
		if err := xdr.SafeUnmarshalBase64(trimmed, &key); err != nil {
			fmt.Printf("Error reading ledger key #%d (%s), skipping: %v\n", i, s, err)
			continue
		}

		ledgerKeys = append(ledgerKeys, key)
	}

	rand.Seed(time.Now().Unix()) // initialize global pseudo random generator

	wg := sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(j int) {
	        client := stellarcore.Client{URL: coreUrl}
			// You can also use context.WithTimeout()
			ctx := context.Background()

			totalQueries := uint64(0)
			failed := uint64(0)
			success := uint64(0)

			latency := time.Duration(0)

			for {
				start := time.Now()
				_, err := client.GetLedgerEntry(ctx, ledgerKeys[rand.Intn(len(ledgerKeys))])
				latency += time.Since(start)
				totalQueries++

				if err != nil {
					//fmt.Printf("Goroutine #%d: error got an error: %v\n", j, err)
					failed++
				} else {
					//fmt.Printf("Goroutine #%d: Core response: %+v\n", j, resp)
					success++
				}

				if totalQueries % 10 == 0 && j == 0 {
					fmt.Printf("Goroutine #%d: total queries: %d, failed: %d, success: %d\n", j, totalQueries, failed, success)
					fmt.Printf("Goroutine #%d: average latency: %v\n", j, latency / time.Duration(totalQueries))
					fmt.Printf("Goroutine #%d: success rate: %f\n", j, float64(success) / float64(totalQueries) * 100.0)
					fmt.Printf("Goroutine request rate: %f\n", (float64(totalQueries) * float64(threads)) / latency.Seconds())
				}
			}

			wg.Done()
		}(i)
	}

	wg.Wait()
	fmt.Println("Bye!")
}