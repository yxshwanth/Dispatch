package grpcapi

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/yash/dispatch/internal/genproto/dispatchv1"
	"github.com/yash/dispatch/internal/ingest"
	"github.com/yash/dispatch/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Server exposes the internal IngestionService.
type Server struct {
	dispatchv1.UnimplementedIngestionServiceServer
	svc   *ingest.Service
	store *store.Store
	log   *slog.Logger
}

func NewServer(svc *ingest.Service, st *store.Store, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{svc: svc, store: st, log: log}
}

func (s *Server) IngestEvent(ctx context.Context, req *dispatchv1.IngestEventRequest) (*dispatchv1.IngestEventResponse, error) {
	tenantID, err := uuid.Parse(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tenant_id")
	}
	if _, err := s.store.GetTenant(ctx, tenantID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "tenant not found")
		}
		return nil, status.Error(codes.Internal, "tenant lookup failed")
	}

	res, err := s.svc.CreateEvent(ctx, tenantID, req.GetEventType(), req.GetPayload(), req.GetIdempotencyKey())
	if err != nil {
		if errors.Is(err, ingest.ErrInvalid) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		s.log.Error("grpc ingest failed", "err", err)
		return nil, status.Error(codes.Internal, "ingest failed")
	}
	return &dispatchv1.IngestEventResponse{
		EventId:  res.Event.ID.String(),
		Replayed: res.Replayed,
	}, nil
}

// AuthInterceptor checks a static internal token from metadata x-internal-token.
// Deliberate simplification for an internal service; production would use mTLS/SPIFFE.
func AuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/grpc.health.v1.Health/Check" ||
			info.FullMethod == "/grpc.health.v1.Health/Watch" {
			return handler(ctx, req)
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok || len(md.Get("x-internal-token")) != 1 || md.Get("x-internal-token")[0] != token {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(ctx, req)
	}
}

// NewGRPCServer builds a gRPC server with auth, health, and reflection.
func NewGRPCServer(token string, ingestSrv *Server) *grpc.Server {
	gs := grpc.NewServer(grpc.UnaryInterceptor(AuthInterceptor(token)))
	dispatchv1.RegisterIngestionServiceServer(gs, ingestSrv)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)
	return gs
}
