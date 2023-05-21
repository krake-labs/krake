package pkg

import (
	"context"

	connect_go "github.com/bufbuild/connect-go"
	"github.com/krake-labs/krake/api"
	v1 "github.com/krake-labs/krake/gen/krake/v1"
)

type KrakeServiceServer struct {
	api.KrakeBroker
}

func NewKrakeServiceServer() *KrakeServiceServer {
	return &KrakeServiceServer{}
}

func (k KrakeServiceServer) Produce(ctx context.Context, c *connect_go.Request[v1.ProduceRequest]) (*connect_go.Response[v1.ProduceResponse], error) {
	k.KrakeBroker.Produce("", nil)
	panic("implement me")
}

func (k KrakeServiceServer) RegisterConsumer(ctx context.Context, c *connect_go.Request[v1.RegisterConsumerRequest]) (*connect_go.Response[v1.RegisterConsumerResponse], error) {
	//TODO implement me
	panic("implement me")
}

func (k KrakeServiceServer) AddSubscriptions(ctx context.Context, c *connect_go.Request[v1.AddSubscriptionsRequest]) (*connect_go.Response[v1.AddSubscriptionsResponse], error) {
	//TODO implement me
	panic("implement me")
}

func (k KrakeServiceServer) ReadMessage(ctx context.Context, c *connect_go.Request[v1.ReadMessageRequest]) (*connect_go.Response[v1.ReadMessageResponse], error) {
	//TODO implement me
	panic("implement me")
}
