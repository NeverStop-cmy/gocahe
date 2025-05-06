package service

import (
	"context"
	"time"

	v1 "gocache-service/api/cache/v1"
	"gocache-service/internal/biz"
)

type CacheService struct {
	v1.UnimplementedCacheServiceServer
	uc *biz.GoCacheUsecase
}

func NewCacheService(uc *biz.GoCacheUsecase) *CacheService {
	return &CacheService{uc: uc}
}

func (s *CacheService) SetString(ctx context.Context, req *v1.SetStringRequest) (*v1.SetStringResponse, error) {
	ttl := time.Duration(req.TtlSeconds) * time.Second
	err := s.uc.Set(ctx, req.Key, req.Value, ttl)
	return &v1.SetStringResponse{}, err
}

func (s *CacheService) GetString(ctx context.Context, req *v1.GetStringRequest) (*v1.GetStringResponse, error) {
	val, err := s.uc.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	return &v1.GetStringResponse{Value: val}, nil
}

func (s *CacheService) DelString(ctx context.Context, req *v1.DelStringRequest) (*v1.DelStringResponse, error) {
	err := s.uc.Delete(ctx, req.Key)
	return &v1.DelStringResponse{}, err
}
