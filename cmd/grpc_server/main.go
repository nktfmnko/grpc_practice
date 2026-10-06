package main

import (
	"context"
	"errors"
	"fmt"
	"grpc_practice/internal/interceptor"
	spaceship_v1 "grpc_practice/pkg/proto/spaceship/v1"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	grpcAddr              = "localhost:50051"
	httpAddr              = "localhost:8081"
	httpReadHeaderTimeout = 10 * time.Second
)

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

func initGRPCServer(grpcAddr string, service spaceship_v1.SpaceshipServiceV1Server) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.ValidationUnaryServerInterceptor(),
			interceptor.ResponseTimeUnaryServerInterceptor(),
		),
	)

	spaceship_v1.RegisterSpaceshipServiceV1Server(s, service)

	reflection.Register(s)

	return s, lis, nil
}

func initSwaggerUI(gwMux *runtime.ServeMux) http.Handler {
	fileServer := http.FileServer(http.Dir("pkg/api"))
	httpMux := http.NewServeMux()

	httpMux.Handle("/api/", gwMux)

	httpMux.Handle("/swagger-ui.html", fileServer)
	httpMux.Handle("/swagger/swagger.swagger.json", fileServer)

	httpMux.Handle("/", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/" {
			http.Redirect(writer, request, "/swagger-ui.html", http.StatusMovedPermanently)
			return
		}
		fileServer.ServeHTTP(writer, request)
	}))
	return httpMux
}

func initHTTPServer(ctx context.Context, httpAddr, grpcAddr string) (*http.Server, error) {
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	err := spaceship_v1.RegisterSpaceshipServiceV1HandlerFromEndpoint(
		ctx,
		mux,
		grpcAddr,
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway: %w", err)
	}

	handler := initSwaggerUI(mux)

	gwServer := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
	}

	return gwServer, nil
}

func main() {
	service := &spaceshipService{
		spaceships: make(map[string]*spaceship_v1.Spaceship),
	}

	grpcServer, lis, err := initGRPCServer(grpcAddr, service)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	go func() {
		log.Printf("gRPC server listening on %s\n", grpcAddr)
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Printf("failed to serve: %v\n", err)
			return
		}
	}()

	ctx := context.Background()
	gwServer, err := initHTTPServer(ctx, httpAddr, grpcAddr)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	go func() {
		log.Printf("HTTP server with gRPC-Gateway listening on %s\n", httpAddr)
		err = gwServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("failed to serve HTTP: %v\n", err)
			return
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	if gwServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = gwServer.Shutdown(shutdownCtx)
		if err != nil {
			log.Printf("HTTP server shutdown err: %v\n", err)
			return
		}
		log.Printf("HTTP server stopped")
	}

	log.Println("Shutting down gRPC server...")
	grpcServer.GracefulStop()

	log.Println("Server stopped")
}
