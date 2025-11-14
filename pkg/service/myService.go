// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MyServiceServerImpl struct {
	pb.UnimplementedServiceServer
	publisherClient asyncMessaging.AsyncMessagingPublisherServiceClient
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
) *MyServiceServerImpl {
	return &MyServiceServerImpl{
		publisherClient: publisherClient,
		publishEnabled:  publishEnabled,
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
		Body:     string(bodyBytes),
		Topic:    topic,
		Metadata: map[string]string{}, // Empty metadata
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
