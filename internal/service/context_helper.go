package service

import "github.com/go-tangra/go-tangra-common/grpcx"

var (
	getTenantIDFromContext = grpcx.GetTenantIDFromContext
	getUsernameFromContext = grpcx.GetUsernameFromContext
	isPlatformAdmin       = grpcx.IsPlatformAdmin
)
