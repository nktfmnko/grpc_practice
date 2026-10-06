package main

import (
	"context"
	"grpc_practice/internal/interceptor"
	spaceship_v1 "grpc_practice/pkg/proto/spaceship/v1"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const grpcAddr = "127.0.0.1:50051"

type spaceshipService struct {
	spaceship_v1.UnimplementedSpaceshipServiceV1Server

	mu         sync.RWMutex
	spaceships map[string]*spaceship_v1.Spaceship
}

func (s *spaceshipService) Create(_ context.Context, req *spaceship_v1.CreateRequest) (*spaceship_v1.CreateResponse, error) {
	newUUID := uuid.NewString()

	spaceship := &spaceship_v1.Spaceship{
		Uuid:      newUUID,
		Info:      req.Info,
		CreatedAt: timestamppb.New(time.Now()),
		UpdatedAt: nil,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.spaceships[newUUID] = spaceship

	log.Printf("Создан корабль с UUID: %s", newUUID)

	return &spaceship_v1.CreateResponse{Spaceship: spaceship}, nil
}

func (s *spaceshipService) Get(_ context.Context, req *spaceship_v1.GetRequest) (*spaceship_v1.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if req.GetUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uuid cannot be empty")
	}

	ship, ok := s.spaceships[req.Uuid]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "spaceship with UUID %s not found", req.GetUuid())
	}

	return &spaceship_v1.GetResponse{Spaceship: ship}, nil
}

func (s *spaceshipService) Update(_ context.Context, req *spaceship_v1.UpdateRequest) (*spaceship_v1.UpdateResponse, error) {
	if req.GetUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uuid cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	ship, ok := s.spaceships[req.GetUuid()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "spaceship with UUID %s not found", req.GetUuid())
	}

	if req.UpdateInfo == nil {
		return nil, status.Errorf(codes.InvalidArgument, "update cannot be nil")
	}

	if updateName := req.GetUpdateInfo().Name; updateName != nil {
		ship.Info.Name = updateName.Value
	}

	if updateDescription := req.GetUpdateInfo().Description; updateDescription != nil {
		ship.Info.Description = updateDescription
	}

	if updateModel := req.GetUpdateInfo().Name; updateModel != nil {
		ship.Info.Model = updateModel.Value
	}

	if updateManufacturer := req.GetUpdateInfo().Manufacturer; updateManufacturer != nil {
		ship.Info.Name = updateManufacturer.Value
	}

	if updatePrice := req.GetUpdateInfo().Manufacturer; updatePrice != nil {
		ship.Info.Name = updatePrice.Value
	}

	ship.UpdatedAt = timestamppb.New(time.Now())

	s.spaceships[req.GetUuid()] = ship
	log.Printf("Обновлен корабль с UUID %s", req.GetUuid())

	return &spaceship_v1.UpdateResponse{Spaceship: ship}, nil
}

func (s *spaceshipService) Delete(_ context.Context, req *spaceship_v1.DeleteRequest) (*emptypb.Empty, error) {
	if req.GetUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uuid cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.spaceships, req.GetUuid())

	log.Printf("Удален корабль с UUID: %s", req.GetUuid())

	return &emptypb.Empty{}, nil
}

func main() {
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Printf("failed to listen: %v\n", err)
		return
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.ValidationUnaryServerInterceptor()),
	)

	service := &spaceshipService{
		spaceships: make(map[string]*spaceship_v1.Spaceship),
	}

	spaceship_v1.RegisterSpaceshipServiceV1Server(s, service)

	reflection.Register(s)

	go func() {
		log.Printf("gRPC server listening on %s\n", grpcAddr)
		err = s.Serve(lis)
		if err != nil {
			log.Printf("failed to serve: %v\n", err)
			return
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gRPC server...")
	s.GracefulStop()

	log.Println("Server stopped")
}
