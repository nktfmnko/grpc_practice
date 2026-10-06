package main

import (
	"context"
	"fmt"
	spaceship_v1 "grpc_practice/pkg/proto/spaceship/v1"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	serverAddr     = "127.0.0.1:50051"
	requestTimeout = 10 * time.Second
)

type SpaceshipClient struct {
	client spaceship_v1.SpaceshipServiceV1Client
}

func NewSpaceshipClient(serverAddr string) (*SpaceshipClient, error) {
	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect server %w", err)
	}

	return &SpaceshipClient{
		client: spaceship_v1.NewSpaceshipServiceV1Client(conn),
	}, nil
}

func (c *SpaceshipClient) CreateSpaceship(
	ctx context.Context,
	name, description, model, manufacturer string,
	price float64,
) (*spaceship_v1.Spaceship, error) {

	req := &spaceship_v1.CreateRequest{Info: &spaceship_v1.SpaceshipInfo{
		Name:         name,
		Description:  wrapperspb.String(description),
		Model:        model,
		Manufacturer: manufacturer,
		Price:        price,
	}}

	resp, err := c.client.Create(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create: %w", err)
	}

	ship := resp.GetSpaceship()

	fmt.Println(ship)
	return ship, nil
}

func (c *SpaceshipClient) GetSpaceship(ctx context.Context, uuid string) (*spaceship_v1.Spaceship, error) {
	req := &spaceship_v1.GetRequest{Uuid: uuid}

	resp, err := c.client.Get(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				fmt.Printf("ship not found: %s", st.Message())
				return nil, err
			}
		}
		return nil, fmt.Errorf("failed to get: %w", err)
	}

	ship := resp.GetSpaceship()
	fmt.Println(ship)
	return ship, nil
}

func (c *SpaceshipClient) UpdateSpaceship(ctx context.Context, uuid, newName string, newPrice float64) (*spaceship_v1.Spaceship, error) {
	req := &spaceship_v1.UpdateRequest{
		Uuid: uuid,
		UpdateInfo: &spaceship_v1.SpaceshipUpdateInfo{
			Name:  wrapperspb.String(newName),
			Price: wrapperspb.Double(newPrice),
		},
	}

	resp, err := c.client.Update(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				fmt.Printf("ship not found: %s", st.Message())
				return nil, err
			}
		}
		return nil, fmt.Errorf("failed to update: %w", err)
	}

	ship := resp.GetSpaceship()
	fmt.Println(ship)
	return ship, nil
}

func (c *SpaceshipClient) DeleteSpaceship(ctx context.Context, uuid string) error {
	req := &spaceship_v1.DeleteRequest{Uuid: uuid}

	_, err := c.client.Delete(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				fmt.Printf("ship not found: %s", st.Message())
				return err
			}
		}
		return fmt.Errorf("failed to delete: %w", err)
	}

	return nil
}

func (c *SpaceshipClient) RunDemo(ctx context.Context) error {
	ship, err := c.CreateSpaceship(ctx, "name", "desc", "model", "manu", 250)
	if err != nil {
		return fmt.Errorf("demo failed to create: %w", err)
	}

	_, err = c.GetSpaceship(ctx, ship.GetUuid())
	if err != nil {
		return fmt.Errorf("demo failed to get: %w", err)
	}

	_, err = c.UpdateSpaceship(ctx, ship.GetUuid(), "newName", 123)
	if err != nil {
		return fmt.Errorf("demo failed to update: %w", err)
	}

	err = c.DeleteSpaceship(ctx, ship.GetUuid())
	if err != nil {
		return fmt.Errorf("demo failed to delete: %w", err)
	}

	return nil
}

func main() {
	ctx := context.Background()
	client, err := NewSpaceshipClient(serverAddr)
	if err != nil {
		log.Fatalf("не удалось создать клиент: %v", err)
	}

	err = client.RunDemo(ctx)
	if err != nil {
		log.Fatalf("ошибка в демонстрации: %v", err)
	}
}
