// Copyright (c) 2025-2026 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package service

import (
	"context"
	"encoding/json"
	pb "extend-custom-guild-service/pkg/pb"
	asyncMessaging "extend-custom-guild-service/pkg/pb/async_messaging"
	"log"
	"time"

	"github.com/AccelByte/accelbyte-go-sdk/cloudsave-sdk/pkg/cloudsaveclient/admin_game_record"
	"github.com/AccelByte/accelbyte-go-sdk/services-api/pkg/service/cloudsave"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MyServiceServerImpl struct {
	pb.UnimplementedServiceServer
	publisherClient asyncMessaging.AsyncMessagingPublisherServiceClient
	csStorage       *cloudsave.AdminGameRecordService
	namespace       string
	publishEnabled  bool
}

type PlayerJoinedEvent struct {
	EventType string `json:"eventType"`
	PlayerId  string `json:"playerId"`
	Timestamp string `json:"timestamp"`
}

func NewMyServiceServer(
	publisherClient asyncMessaging.AsyncMessagingPublisherServiceClient,
	publishEnabled bool,
	csStorage *cloudsave.AdminGameRecordService,
	namespace string,
) *MyServiceServerImpl {
	return &MyServiceServerImpl{
		publisherClient: publisherClient,
		publishEnabled:  publishEnabled,
		csStorage:       csStorage,
		namespace:       namespace,
	}
}

func (g MyServiceServerImpl) Join(
	ctx context.Context, req *pb.JoinRequest,
) (*pb.JoinResponse, error) {
	topic := "PlayerJoined"
	event := PlayerJoinedEvent{
		EventType: "PlayerJoined",
		PlayerId:  req.PlayerId,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	bodyBytes, err := json.Marshal(event)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal event: %v", err)
	}

	msg := &asyncMessaging.PublishMessageRequest{
		Body:  string(bodyBytes),
		Topic: topic,
		Metadata: map[string]string{
			"PlayerId": req.PlayerId,
		},
	}

	if !g.publishEnabled {
		log.Printf("Publishing disabled - message would be published: %+v", msg)
	} else {
		_, err = g.publisherClient.PublishMessage(ctx, msg)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to publish message: %v", err)
		}
	}

	return &pb.JoinResponse{}, nil
}

func (g MyServiceServerImpl) Check(
	ctx context.Context, req *pb.CheckRequest,
) (*pb.CheckResponse, error) {

	key := "player_joined_event_" + req.PlayerId
	input := &admin_game_record.AdminGetGameRecordHandlerV1Params{
		Key:       key,
		Namespace: req.Namespace,
		Context:   ctx,
	}

	//We just want to make sure it exists.

	_, err := g.csStorage.AdminGetGameRecordHandlerV1Short(input)
	if err != nil {
		return &pb.CheckResponse{
			Status: "data not found",
		}, nil
	} else {
		return &pb.CheckResponse{
			Status: "joined",
		}, nil
	}
}
