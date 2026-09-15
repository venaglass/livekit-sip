// sip-api-probe is a read-only diagnostic for the LiveKit SIP list APIs.
// It prints trunk/rule IDs and matching number fields, never trunk credentials.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func main() {
	number := flag.String("number", "", "inbound destination number to look up")
	trunkID := flag.String("trunk-id", "", "specific trunk ID to request dispatch rules for")
	flag.Parse()

	apiKey, apiSecret, wsURL := os.Getenv("LIVEKIT_API_KEY"), os.Getenv("LIVEKIT_API_SECRET"), os.Getenv("LIVEKIT_WS_URL")
	if apiKey == "" || apiSecret == "" || wsURL == "" {
		log.Fatal("set LIVEKIT_API_KEY, LIVEKIT_API_SECRET, and LIVEKIT_WS_URL")
	}
	if *number == "" && *trunkID == "" {
		log.Fatal("provide -number and/or -trunk-id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sipClient := lksdk.NewSIPClient(wsURL, apiKey, apiSecret)

	ids := []string{}
	if *number != "" {
		trunks, err := sipClient.ListSIPInboundTrunk(ctx, &livekit.ListSIPInboundTrunkRequest{Numbers: []string{*number}})
		if err != nil {
			log.Fatalf("list inbound trunks for number: %v", err)
		}
		fmt.Printf("Inbound trunks returned for number %q (%d):\n", *number, len(trunks.GetItems()))
		for _, trunk := range trunks.GetItems() {
			if trunk == nil {
				continue
			}
			fmt.Printf("  trunk_id=%q numbers=%q\n", trunk.GetSipTrunkId(), trunk.GetNumbers())
			ids = append(ids, trunk.GetSipTrunkId())
		}
	}
	if *trunkID != "" {
		ids = []string{*trunkID}
	}

	for _, id := range ids {
		rules, err := sipClient.ListSIPDispatchRule(ctx, &livekit.ListSIPDispatchRuleRequest{TrunkIds: []string{id}})
		if err != nil {
			log.Fatalf("list dispatch rules for trunk %q: %v", id, err)
		}
		fmt.Printf("Dispatch rules returned for requested trunk %q (%d):\n", id, len(rules.GetItems()))
		for _, rule := range rules.GetItems() {
			if rule == nil {
				continue
			}
			fmt.Printf("  rule_id=%q name=%q trunk_ids=%q numbers=%q inbound_numbers=%q\n",
				rule.GetSipDispatchRuleId(), rule.GetName(), rule.GetTrunkIds(), rule.GetNumbers(), rule.GetInboundNumbers())
		}
	}
}
