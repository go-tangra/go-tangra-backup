package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/transport/grpc"

	conf "github.com/tx7do/kratos-bootstrap/api/gen/go/conf/v1"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-common/registration"
	"github.com/go-tangra/go-tangra-common/service"
	"github.com/go-tangra/go-tangra-backup/cmd/server/assets"
	backupService "github.com/go-tangra/go-tangra-backup/internal/service"
)

var (
	moduleID    = "backup"
	moduleName  = "Backup"
	version     = "1.0.0"
	commitHash  = "unknown"
	buildDate   = "unknown"
	description = "Centralized backup and restore orchestration for all platform modules"
)

var globalRegHelper *registration.RegistrationHelper

func newApp(
	ctx *bootstrap.Context,
	gs *grpc.Server,
) *kratos.App {
	globalRegHelper = registration.StartRegistration(ctx, ctx.GetLogger(), &registration.Config{
		ModuleID:          moduleID,
		ModuleName:        moduleName,
		Version:           version,
		Description:       description,
		GRPCEndpoint:      registration.GetGRPCAdvertiseAddr(ctx, "0.0.0.0:10100"),
		AdminEndpoint:     registration.GetEnvOrDefault("ADMIN_GRPC_ENDPOINT", ""),
		OpenapiSpec:       assets.OpenApiData,
		ProtoDescriptor:   assets.DescriptorData,
		MenusYaml:         assets.MenusData,
		HeartbeatInterval: 30 * time.Second,
		RetryInterval:     5 * time.Second,
		MaxRetries:        60,
	})

	return bootstrap.NewApp(ctx, gs)
}

func runApp() error {
	ctx := bootstrap.NewContext(
		context.Background(),
		&conf.AppInfo{
			Project: service.Project,
			AppId:   "backup.service",
			Version: version,
		},
	)

	defer globalRegHelper.Stop()

	return bootstrap.RunApp(ctx, initApp)
}

func runDecrypt() error {
	fs := flag.NewFlagSet("decrypt", flag.ExitOnError)
	fileName := fs.String("file", "", "path to encrypted backup file (.enc)")
	password := fs.String("password", "", "decryption password")
	output := fs.String("output", "", "output file path (default: input without .enc suffix)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s decrypt --file <path> --password <password> [--output <path>]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Decrypt an AES-256-GCM encrypted backup file.\n\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}

	if *fileName == "" || *password == "" {
		fs.Usage()
		return fmt.Errorf("both --file and --password are required")
	}

	encrypted, err := os.ReadFile(*fileName)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	compressed, err := backupService.DecryptData(encrypted, *password)
	if err != nil {
		return fmt.Errorf("decrypt: %w", err)
	}

	// Decompress gzip
	gr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	plaintext, err := io.ReadAll(gr)
	if err != nil {
		return fmt.Errorf("decompress: %w", err)
	}

	// Determine output path
	outPath := *output
	if outPath == "" {
		outPath = strings.TrimSuffix(*fileName, ".enc")
		// If the file was .json.gz.enc, strip to .json
		outPath = strings.TrimSuffix(outPath, ".gz")
	}

	if err := os.WriteFile(outPath, plaintext, 0o644); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Printf("Decrypted %s -> %s (%d bytes)\n", *fileName, outPath, len(plaintext))
	return nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "decrypt" {
		if err := runDecrypt(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := runApp(); err != nil {
		panic(err)
	}
}
