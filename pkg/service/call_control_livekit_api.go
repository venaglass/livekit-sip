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
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/twitchtv/twirp"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/sip"
)

type liveKitSIPClient interface {
	ListSIPInboundTrunk(context.Context, *livekit.ListSIPInboundTrunkRequest) (*livekit.ListSIPInboundTrunkResponse, error)
	ListSIPDispatchRule(context.Context, *livekit.ListSIPDispatchRuleRequest) (*livekit.ListSIPDispatchRuleResponse, error)
}

type liveKitRoomClient interface {
	CreateRoom(context.Context, *livekit.CreateRoomRequest) (*livekit.Room, error)
}

type liveKitAgentDispatchClient interface {
	CreateDispatch(context.Context, *livekit.CreateAgentDispatchRequest) (*livekit.AgentDispatch, error)
}

type LiveKitAPICallControl struct {
	conf           *config.Config
	log            logger.Logger
	sipClient      liveKitSIPClient
	roomClient     liveKitRoomClient
	dispatchClient liveKitAgentDispatchClient
}

func NewLiveKitAPICallControl(conf *config.Config, log logger.Logger) *LiveKitAPICallControl {
	return &LiveKitAPICallControl{conf: conf, log: log}
}

func NewLiveKitAPICallControlWithClients(conf *config.Config, log logger.Logger, sipClient liveKitSIPClient, roomClient liveKitRoomClient, dispatchClient liveKitAgentDispatchClient) *LiveKitAPICallControl {
	return &LiveKitAPICallControl{
		conf:           conf,
		log:            log,
		sipClient:      sipClient,
		roomClient:     roomClient,
		dispatchClient: dispatchClient,
	}
}

func (p *LiveKitAPICallControl) Init(_ context.Context) error {
	if p.sipClient == nil {
		p.sipClient = lksdk.NewSIPClient(p.conf.WsUrl, p.conf.ApiKey, p.conf.ApiSecret)
	}
	if p.roomClient == nil {
		p.roomClient = lksdk.NewRoomServiceClient(p.conf.WsUrl, p.conf.ApiKey, p.conf.ApiSecret)
	}
	if p.dispatchClient == nil {
		p.dispatchClient = lksdk.NewAgentDispatchServiceClient(p.conf.WsUrl, p.conf.ApiKey, p.conf.ApiSecret)
	}
	return nil
}

func (p *LiveKitAPICallControl) Start(_ rpc.SIPInternalServerImpl) error {
	return nil
}

func (p *LiveKitAPICallControl) Stop() {
}

func (p *LiveKitAPICallControl) StateHandler(_ string, _ *rpc.SIPCallObservability, _ *livekit.SIPCallInfo) sip.StateHandler {
	return sip.NewRPCStateHandler(nil)
}

func (p *LiveKitAPICallControl) GetAuthCredentials(ctx context.Context, call *rpc.SIPCall) (sip.AuthInfo, error) {
	resp, err := p.sipClient.ListSIPInboundTrunk(ctx, &livekit.ListSIPInboundTrunkRequest{
		Numbers: []string{call.To.User},
	})
	if err != nil {
		return sip.AuthInfo{}, err
	}

	projectID := p.conf.Control.LiveKitAPI.ProjectID
	if projectID == "" {
		projectID = call.ProjectId
	}
	for _, trunk := range resp.GetItems() {
		if !matchesInboundTrunk(trunk, call) {
			continue
		}
		if trunk.AuthUsername != "" && trunk.AuthPassword != "" {
			return sip.AuthInfo{
				ProjectID: projectID,
				TrunkID:   trunk.SipTrunkId,
				Result:    sip.AuthPassword,
				Auth: sip.InboundAuth{
					Username: trunk.AuthUsername,
					Password: trunk.AuthPassword,
					Realm:    trunk.AuthRealm,
				},
			}, nil
		}
		return sip.AuthInfo{
			ProjectID: projectID,
			TrunkID:   trunk.SipTrunkId,
			Result:    sip.AuthAccept,
		}, nil
	}

	return sip.AuthInfo{
		ProjectID: projectID,
		Result:    sip.AuthNoTrunkFound,
	}, nil
}

func (p *LiveKitAPICallControl) DispatchCall(ctx context.Context, info *sip.CallInfo) sip.CallDispatch {
	call := info.Call
	projectID := p.conf.Control.LiveKitAPI.ProjectID
	if projectID == "" {
		projectID = call.ProjectId
	}

	trunkResp, err := p.sipClient.ListSIPInboundTrunk(ctx, &livekit.ListSIPInboundTrunkRequest{
		TrunkIds: []string{info.TrunkID},
	})
	if err != nil {
		p.log.Warnw("SIP livekit_api trunk lookup failed", err, "trunkID", info.TrunkID)
		return sip.CallDispatch{Result: sip.DispatchServiceUnavailable, ProjectID: projectID, TrunkID: info.TrunkID}
	}
	var trunk *livekit.SIPInboundTrunkInfo
	for _, item := range trunkResp.GetItems() {
		if item != nil && item.SipTrunkId == info.TrunkID {
			trunk = item
			break
		}
	}
	if trunk == nil {
		return sip.CallDispatch{Result: sip.DispatchNoRuleReject, ProjectID: projectID, TrunkID: info.TrunkID}
	}

	ruleResp, err := p.sipClient.ListSIPDispatchRule(ctx, &livekit.ListSIPDispatchRuleRequest{
		TrunkIds: []string{info.TrunkID},
	})
	if err != nil {
		p.log.Warnw("SIP livekit_api dispatch rule lookup failed", err, "trunkID", info.TrunkID)
		return sip.CallDispatch{Result: sip.DispatchServiceUnavailable, ProjectID: projectID, TrunkID: info.TrunkID}
	}

	for _, rule := range ruleResp.GetItems() {
		if !matchesDispatchRule(rule, call) {
			continue
		}

		roomName, pin, ok := dispatchRuleRoom(rule)
		if !ok {
			p.log.Infow("SIP livekit_api skipping unsupported dispatch rule type", "ruleID", rule.SipDispatchRuleId)
			continue
		}
		if pin != "" && !info.NoPin && info.Pin != pin {
			return sip.CallDispatch{
				ProjectID:      projectID,
				TrunkID:        trunk.SipTrunkId,
				DispatchRuleID: rule.SipDispatchRuleId,
				Result:         sip.DispatchRequestPin,
				MediaConfig:    rule.Media,
			}
		}
		if roomName == "" {
			p.log.Warnw("SIP livekit_api dispatch rule has empty room name", nil, "ruleID", rule.SipDispatchRuleId)
			continue
		}
		if err := p.prepareRoom(ctx, roomName, rule); err != nil {
			p.log.Warnw("SIP livekit_api room preparation failed", err, "room", roomName, "ruleID", rule.SipDispatchRuleId)
			return sip.CallDispatch{Result: sip.DispatchServiceUnavailable, ProjectID: projectID, TrunkID: trunk.SipTrunkId, DispatchRuleID: rule.SipDispatchRuleId}
		}

		participantID := call.LkCallId
		if participantID == "" {
			participantID = call.SipCallId
		}
		participantName := "Phone"
		if call.From.User != "" {
			participantName = "Phone " + call.From.User
		}
		media := trunk.Media
		if rule.Media != nil {
			media = rule.Media
		}
		if media == nil {
			media = &livekit.SIPMediaConfig{}
		}

		return sip.CallDispatch{
			ProjectID: projectID,
			Result:    sip.DispatchAccept,
			Room: sip.RoomConfig{
				WsUrl:      p.conf.WsUrl,
				RoomName:   roomName,
				RoomPreset: rule.RoomPreset,
				RoomConfig: rule.RoomConfig,
				Participant: sip.ParticipantConfig{
					Identity:   p.conf.Control.LiveKitAPI.DefaultParticipantIdentityPrefix + "-" + participantID,
					Name:       participantName,
					Metadata:   rule.Metadata,
					Attributes: rule.Attributes,
				},
			},
			TrunkID:             trunk.SipTrunkId,
			DispatchRuleID:      rule.SipDispatchRuleId,
			Headers:             trunk.Headers,
			HeadersToAttributes: trunk.HeadersToAttributes,
			IncludeHeaders:      trunk.IncludeHeaders,
			AttributesToHeaders: trunk.AttributesToHeaders,
			RingingTimeout:      trunk.RingingTimeout.AsDuration(),
			MaxCallDuration:     trunk.MaxCallDuration.AsDuration(),
			MediaConfig:         media,
		}
	}

	return sip.CallDispatch{Result: sip.DispatchNoRuleReject, ProjectID: projectID, TrunkID: trunk.SipTrunkId}
}

func dispatchRuleRoom(rule *livekit.SIPDispatchRuleInfo) (roomName, pin string, ok bool) {
	if rule == nil || rule.Rule == nil {
		return "", "", false
	}

	switch typed := rule.Rule.Rule.(type) {
	case *livekit.SIPDispatchRule_DispatchRuleDirect:
		if typed.DispatchRuleDirect == nil {
			return "", "", false
		}
		return typed.DispatchRuleDirect.RoomName, typed.DispatchRuleDirect.Pin, true
	case *livekit.SIPDispatchRule_DispatchRuleIndividual:
		if typed.DispatchRuleIndividual == nil {
			return "", "", false
		}
		roomName = typed.DispatchRuleIndividual.RoomPrefix
		if roomName == "" {
			roomName = "sip-individual"
		}
		if !typed.DispatchRuleIndividual.NoRandomness {
			roomName += "-" + randomStaticRoomSuffix(6)
		}
		return roomName, typed.DispatchRuleIndividual.Pin, true
	default:
		return "", "", false
	}
}

func (p *LiveKitAPICallControl) GetMediaProcessor(_ []livekit.SIPFeature, _ map[string]string, _ string, _ sip.MediaProcessorOpts) msdk.PCM16Processor {
	return nil
}

func (p *LiveKitAPICallControl) RegisterTransferSIPParticipantTopic(_ string) error {
	return nil
}

func (p *LiveKitAPICallControl) DeregisterTransferSIPParticipantTopic(_ string) {
}

func (p *LiveKitAPICallControl) OnInboundInfo(_ logger.Logger, _ *rpc.SIPCall, _ sip.Headers) {
}

func (p *LiveKitAPICallControl) OnSessionEnd(_ context.Context, id *sip.CallIdentifier, _ *sip.CallState, reason string) {
	p.log.Infow("SIP call ended", "callID", id.CallID, "sipCallID", id.SipCallID, "reason", reason)
}

func (p *LiveKitAPICallControl) prepareRoom(ctx context.Context, roomName string, rule *livekit.SIPDispatchRuleInfo) error {
	req := &livekit.CreateRoomRequest{
		Name:       roomName,
		RoomPreset: rule.RoomPreset,
	}
	if rc := rule.RoomConfig; rc != nil {
		req.EmptyTimeout = rc.EmptyTimeout
		req.DepartureTimeout = rc.DepartureTimeout
		req.MaxParticipants = rc.MaxParticipants
		req.Metadata = rc.Metadata
		req.Egress = rc.Egress
		req.MinPlayoutDelay = rc.MinPlayoutDelay
		req.MaxPlayoutDelay = rc.MaxPlayoutDelay
		req.SyncStreams = rc.SyncStreams
		req.Tags = rc.Tags
	}

	_, err := p.roomClient.CreateRoom(ctx, req)
	if err != nil {
		var twerr twirp.Error
		if !errors.As(err, &twerr) || twerr.Code() != twirp.AlreadyExists {
			return err
		}
	}
	if rule.RoomConfig == nil {
		return nil
	}
	for _, agent := range rule.RoomConfig.Agents {
		if agent.AgentName == "" {
			continue
		}
		_, err = p.dispatchClient.CreateDispatch(ctx, &livekit.CreateAgentDispatchRequest{
			Room:      roomName,
			AgentName: agent.AgentName,
			Metadata:  agent.Metadata,
		})
		if err != nil {
			return fmt.Errorf("create agent dispatch %q: %w", agent.AgentName, err)
		}
	}
	return nil
}

func matchesInboundTrunk(trunk *livekit.SIPInboundTrunkInfo, call *rpc.SIPCall) bool {
	if trunk == nil {
		return false
	}
	if len(trunk.AllowedNumbers) != 0 && !slices.Contains(trunk.AllowedNumbers, call.From.User) {
		return false
	}
	if len(trunk.AllowedAddresses) != 0 && !addressAllowed(call.SourceIp, trunk.AllowedAddresses) {
		return false
	}
	return true
}

func matchesDispatchRule(rule *livekit.SIPDispatchRuleInfo, call *rpc.SIPCall) bool {
	if rule == nil || rule.Rule == nil || rule.Rule.Rule == nil {
		return false
	}
	if len(rule.InboundNumbers) != 0 && !slices.Contains(rule.InboundNumbers, call.From.User) {
		return false
	}
	if len(rule.Numbers) != 0 && !slices.Contains(rule.Numbers, call.To.User) {
		return false
	}
	return true
}

func addressAllowed(src string, allowed []string) bool {
	srcAddr, err := netip.ParseAddr(src)
	if err != nil {
		return false
	}
	for _, entry := range allowed {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if prefix, err := netip.ParsePrefix(entry); err == nil && prefix.Contains(srcAddr) {
			return true
		}
		if addr, err := netip.ParseAddr(entry); err == nil && addr == srcAddr {
			return true
		}
	}
	return false
}
