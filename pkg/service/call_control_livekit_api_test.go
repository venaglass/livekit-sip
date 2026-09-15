// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/sip"
)

type fakeLiveKitSIPClient struct {
	inboundReqs []*livekit.ListSIPInboundTrunkRequest
	ruleReqs    []*livekit.ListSIPDispatchRuleRequest
	trunks      []*livekit.SIPInboundTrunkInfo
	rules       []*livekit.SIPDispatchRuleInfo
	err         error
}

func (f *fakeLiveKitSIPClient) ListSIPInboundTrunk(_ context.Context, in *livekit.ListSIPInboundTrunkRequest) (*livekit.ListSIPInboundTrunkResponse, error) {
	f.inboundReqs = append(f.inboundReqs, in)
	if f.err != nil {
		return nil, f.err
	}
	return &livekit.ListSIPInboundTrunkResponse{Items: f.trunks}, nil
}

func (f *fakeLiveKitSIPClient) ListSIPDispatchRule(_ context.Context, in *livekit.ListSIPDispatchRuleRequest) (*livekit.ListSIPDispatchRuleResponse, error) {
	f.ruleReqs = append(f.ruleReqs, in)
	if f.err != nil {
		return nil, f.err
	}
	return &livekit.ListSIPDispatchRuleResponse{Items: f.rules}, nil
}

type fakeLiveKitRoomClient struct {
	reqs []*livekit.CreateRoomRequest
	err  error
}

func (f *fakeLiveKitRoomClient) CreateRoom(_ context.Context, req *livekit.CreateRoomRequest) (*livekit.Room, error) {
	f.reqs = append(f.reqs, req)
	if f.err != nil {
		return nil, f.err
	}
	return &livekit.Room{Name: req.Name}, nil
}

type fakeLiveKitAgentDispatchClient struct {
	reqs []*livekit.CreateAgentDispatchRequest
	err  error
}

func (f *fakeLiveKitAgentDispatchClient) CreateDispatch(_ context.Context, req *livekit.CreateAgentDispatchRequest) (*livekit.AgentDispatch, error) {
	f.reqs = append(f.reqs, req)
	if f.err != nil {
		return nil, f.err
	}
	return &livekit.AgentDispatch{Room: req.Room, AgentName: req.AgentName}, nil
}

func TestLiveKitAPICallControlAuthNoTrunk(t *testing.T) {
	provider, sipClient, _, _ := newTestLiveKitAPIProvider(nil, nil)

	auth, err := provider.GetAuthCredentials(context.Background(), testSIPCall())
	require.NoError(t, err)
	require.Equal(t, sip.AuthNoTrunkFound, auth.Result)
	require.Equal(t, "project-a", auth.ProjectID)
	require.Equal(t, []string{"+15550001111"}, sipClient.inboundReqs[0].Numbers)
}

func TestLiveKitAPICallControlAuthPassword(t *testing.T) {
	trunk := testInboundTrunk()
	trunk.AuthUsername = "u"
	trunk.AuthPassword = "p"
	trunk.AuthRealm = "r"
	provider, sipClient, _, _ := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, nil)

	auth, err := provider.GetAuthCredentials(context.Background(), testSIPCall())
	require.NoError(t, err)
	require.Equal(t, sip.AuthPassword, auth.Result)
	require.Equal(t, "trunk-a", auth.TrunkID)
	require.Equal(t, "u", auth.Auth.Username)
	require.Equal(t, "p", auth.Auth.Password)
	require.Equal(t, "r", auth.Auth.Realm)
	require.Equal(t, []string{"+15550001111"}, sipClient.inboundReqs[0].Numbers)
}

func TestLiveKitAPICallControlDispatchMatchesRequestURI(t *testing.T) {
	trunk := testInboundTrunk()
	rule := testDirectRule("room-a", "")
	rule.Numbers = []string{"+15550001111"}
	call := testSIPCall()
	call.To.User = "+19999999999" // The To header can differ from the INVITE Request-URI.
	provider, _, _, _ := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, []*livekit.SIPDispatchRuleInfo{rule})

	dispatch := provider.DispatchCall(context.Background(), &sip.CallInfo{TrunkID: trunk.SipTrunkId, Call: call})
	require.Equal(t, sip.DispatchAccept, dispatch.Result)
}

func TestLiveKitAPICallControlAuthAllowedFilters(t *testing.T) {
	trunk := testInboundTrunk()
	trunk.AllowedNumbers = []string{"+15550000000"}
	provider, _, _, _ := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, nil)

	auth, err := provider.GetAuthCredentials(context.Background(), testSIPCall())
	require.NoError(t, err)
	require.Equal(t, sip.AuthNoTrunkFound, auth.Result)

	trunk.AllowedNumbers = []string{"+15551112222"}
	trunk.AllowedAddresses = []string{"203.0.113.0/24"}
	auth, err = provider.GetAuthCredentials(context.Background(), testSIPCall())
	require.NoError(t, err)
	require.Equal(t, sip.AuthAccept, auth.Result)
}

func TestLiveKitAPICallControlDispatchDirectRuleCreatesRoomAndAgentDispatch(t *testing.T) {
	trunk := testInboundTrunk()
	rule := testDirectRule("room-a", "")
	rule.RoomConfig = &livekit.RoomConfiguration{
		EmptyTimeout:    30,
		MaxParticipants: 2,
		Metadata:        "room-meta",
		Agents: []*livekit.RoomAgentDispatch{
			{AgentName: "agent-a", Metadata: `{"x":1}`},
		},
	}
	provider, sipClient, roomClient, dispatchClient := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, []*livekit.SIPDispatchRuleInfo{rule})

	dispatch := provider.DispatchCall(context.Background(), &sip.CallInfo{
		TrunkID: trunk.SipTrunkId,
		Call:    testSIPCall(),
	})

	require.Equal(t, sip.DispatchAccept, dispatch.Result)
	require.Equal(t, "room-a", dispatch.Room.RoomName)
	require.Equal(t, "sip-call-a", dispatch.Room.Participant.Identity)
	require.Equal(t, "Phone +15551112222", dispatch.Room.Participant.Name)
	require.Equal(t, "rule-meta", dispatch.Room.Participant.Metadata)
	require.Equal(t, "v", dispatch.Room.Participant.Attributes["k"])
	require.Equal(t, "trunk-a", dispatch.TrunkID)
	require.Equal(t, "rule-a", dispatch.DispatchRuleID)
	require.Equal(t, "x-v", dispatch.Headers["X-Test"])
	require.Len(t, sipClient.ruleReqs, 1)
	require.Equal(t, []string{"trunk-a"}, sipClient.ruleReqs[0].TrunkIds)
	require.Len(t, roomClient.reqs, 1)
	require.Equal(t, "room-a", roomClient.reqs[0].Name)
	require.Equal(t, uint32(30), roomClient.reqs[0].EmptyTimeout)
	require.Len(t, dispatchClient.reqs, 1)
	require.Equal(t, "room-a", dispatchClient.reqs[0].Room)
	require.Equal(t, "agent-a", dispatchClient.reqs[0].AgentName)
	require.Equal(t, `{"x":1}`, dispatchClient.reqs[0].Metadata)
}

func TestLiveKitAPICallControlDispatchIndividualRuleCreatesRoomAndAgentDispatch(t *testing.T) {
	trunk := testInboundTrunk()
	rule := &livekit.SIPDispatchRuleInfo{
		SipDispatchRuleId: "rule-individual",
		RoomConfig: &livekit.RoomConfiguration{
			Agents: []*livekit.RoomAgentDispatch{
				{AgentName: "agent-a", Metadata: `{"call":"individual"}`},
			},
		},
		Rule: &livekit.SIPDispatchRule{
			Rule: &livekit.SIPDispatchRule_DispatchRuleIndividual{
				DispatchRuleIndividual: &livekit.SIPDispatchRuleIndividual{
					RoomPrefix:   "call-room",
					NoRandomness: true,
				},
			},
		},
	}
	provider, _, roomClient, dispatchClient := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, []*livekit.SIPDispatchRuleInfo{rule})

	dispatch := provider.DispatchCall(context.Background(), &sip.CallInfo{
		TrunkID: trunk.SipTrunkId,
		Call:    testSIPCall(),
	})

	require.Equal(t, sip.DispatchAccept, dispatch.Result)
	require.Equal(t, "call-room", dispatch.Room.RoomName)
	require.Len(t, roomClient.reqs, 1)
	require.Equal(t, "call-room", roomClient.reqs[0].Name)
	require.Len(t, dispatchClient.reqs, 1)
	require.Equal(t, "call-room", dispatchClient.reqs[0].Room)
	require.Equal(t, "agent-a", dispatchClient.reqs[0].AgentName)
	require.Equal(t, `{"call":"individual"}`, dispatchClient.reqs[0].Metadata)
}

func TestLiveKitAPICallControlDispatchPinRequiredThenAccepted(t *testing.T) {
	trunk := testInboundTrunk()
	rule := testDirectRule("room-a", "1234")
	provider, _, _, _ := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, []*livekit.SIPDispatchRuleInfo{rule})

	dispatch := provider.DispatchCall(context.Background(), &sip.CallInfo{
		TrunkID: trunk.SipTrunkId,
		Call:    testSIPCall(),
		Pin:     "9999",
	})
	require.Equal(t, sip.DispatchRequestPin, dispatch.Result)

	dispatch = provider.DispatchCall(context.Background(), &sip.CallInfo{
		TrunkID: trunk.SipTrunkId,
		Call:    testSIPCall(),
		Pin:     "1234",
	})
	require.Equal(t, sip.DispatchAccept, dispatch.Result)
}

func TestLiveKitAPICallControlDispatchSkipsUnsupportedRules(t *testing.T) {
	trunk := testInboundTrunk()
	individual := &livekit.SIPDispatchRuleInfo{
		SipDispatchRuleId: "rule-individual",
		Rule: &livekit.SIPDispatchRule{
			Rule: &livekit.SIPDispatchRule_DispatchRuleIndividual{
				DispatchRuleIndividual: &livekit.SIPDispatchRuleIndividual{RoomPrefix: "room"},
			},
		},
	}
	provider, _, _, _ := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, []*livekit.SIPDispatchRuleInfo{individual})

	dispatch := provider.DispatchCall(context.Background(), &sip.CallInfo{
		TrunkID: trunk.SipTrunkId,
		Call:    testSIPCall(),
	})
	require.Equal(t, sip.DispatchNoRuleReject, dispatch.Result)
}

func TestLiveKitAPICallControlDispatchExistingRoomIsSuccess(t *testing.T) {
	trunk := testInboundTrunk()
	rule := testDirectRule("room-a", "")
	provider, _, roomClient, _ := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, []*livekit.SIPDispatchRuleInfo{rule})
	roomClient.err = twirp.NewError(twirp.AlreadyExists, "room exists")

	dispatch := provider.DispatchCall(context.Background(), &sip.CallInfo{
		TrunkID: trunk.SipTrunkId,
		Call:    testSIPCall(),
	})
	require.Equal(t, sip.DispatchAccept, dispatch.Result)
}

func TestLiveKitAPICallControlDispatchAgentFailureRejectsServiceUnavailable(t *testing.T) {
	trunk := testInboundTrunk()
	rule := testDirectRule("room-a", "")
	rule.RoomConfig = &livekit.RoomConfiguration{
		Agents: []*livekit.RoomAgentDispatch{{AgentName: "agent-a"}},
	}
	provider, _, _, dispatchClient := newTestLiveKitAPIProvider([]*livekit.SIPInboundTrunkInfo{trunk}, []*livekit.SIPDispatchRuleInfo{rule})
	dispatchClient.err = twirp.NewError(twirp.Internal, "failed")

	dispatch := provider.DispatchCall(context.Background(), &sip.CallInfo{
		TrunkID: trunk.SipTrunkId,
		Call:    testSIPCall(),
	})
	require.Equal(t, sip.DispatchServiceUnavailable, dispatch.Result)
}

func newTestLiveKitAPIProvider(
	trunks []*livekit.SIPInboundTrunkInfo,
	rules []*livekit.SIPDispatchRuleInfo,
) (*LiveKitAPICallControl, *fakeLiveKitSIPClient, *fakeLiveKitRoomClient, *fakeLiveKitAgentDispatchClient) {
	conf := &config.Config{
		WsUrl: "wss://example.livekit.cloud",
		Control: config.ControlConfig{
			Provider: config.CallControlProviderLiveKitAPI,
			LiveKitAPI: config.LiveKitAPIControlConfig{
				ProjectID:                        "project-a",
				DefaultParticipantIdentityPrefix: "sip",
			},
		},
	}
	sipClient := &fakeLiveKitSIPClient{trunks: trunks, rules: rules}
	roomClient := &fakeLiveKitRoomClient{}
	dispatchClient := &fakeLiveKitAgentDispatchClient{}
	return NewLiveKitAPICallControlWithClients(conf, logger.GetLogger(), sipClient, roomClient, dispatchClient), sipClient, roomClient, dispatchClient
}

func testSIPCall() *rpc.SIPCall {
	return &rpc.SIPCall{
		LkCallId:  "call-a",
		SipCallId: "sip-call-a",
		SourceIp:  "203.0.113.10",
		Address:   &livekit.SIPUri{User: "+15550001111"},
		From:      &livekit.SIPUri{User: "+15551112222"},
		To:        &livekit.SIPUri{User: "+15553334444"},
	}
}

func testInboundTrunk() *livekit.SIPInboundTrunkInfo {
	return &livekit.SIPInboundTrunkInfo{
		SipTrunkId:          "trunk-a",
		Numbers:             []string{"+15553334444"},
		Headers:             map[string]string{"X-Test": "x-v"},
		HeadersToAttributes: map[string]string{"X-Attr": "attr"},
		AttributesToHeaders: map[string]string{"attr": "X-Attr"},
	}
}

func testDirectRule(room, pin string) *livekit.SIPDispatchRuleInfo {
	return &livekit.SIPDispatchRuleInfo{
		SipDispatchRuleId: "rule-a",
		Metadata:          "rule-meta",
		Attributes:        map[string]string{"k": "v"},
		Rule: &livekit.SIPDispatchRule{
			Rule: &livekit.SIPDispatchRule_DispatchRuleDirect{
				DispatchRuleDirect: &livekit.SIPDispatchRuleDirect{
					RoomName: room,
					Pin:      pin,
				},
			},
		},
	}
}
