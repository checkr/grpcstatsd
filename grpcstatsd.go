package grpcstatsd

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type grpcStats struct {
	metricName string
	service    string
	method     string
	duration   time.Duration
	code       codes.Code
}

func (g grpcStats) toTags() []string {
	return []string{
		fmt.Sprintf("grpc.service:%s", g.service),
		fmt.Sprintf("grpc.method:%s", g.method),
		fmt.Sprintf("grpc.code:%s", g.code),
	}
}

func (g grpcStats) send(client *statsd.Client) {
	if client == nil {
		fmt.Println("statsd client is nil in grpcstatsd middleware")
		return
	}
	client.Timing(
		g.metricName,
		g.duration,
		g.toTags(),
		float64(1.0),
	)
}

// ErrorToCode function determines the error code of an error
// This makes using custom errors with grpc middleware easier
type ErrorToCode func(err error) codes.Code

// DefaultErrorToCode determines the error code of an error by using grpc.Code
func DefaultErrorToCode(err error) codes.Code {
	return grpc.Code(err)
}

// Option represents the option for configuration
type Option struct {
	MetricName  string
	ErrorToCode ErrorToCode
}

func setDefaults(o *Option) *Option {
	if o == nil {
		o = &Option{}
	}
	if o.MetricName == "" {
		o.MetricName = "grpc_middleware"
	}
	if o.ErrorToCode == nil {
		o.ErrorToCode = DefaultErrorToCode
	}
	return o
}

// UnaryServerInterceptor returns a new unary server interceptors that sends statsd
func UnaryServerInterceptor(client *statsd.Client, o *Option) grpc.UnaryServerInterceptor {
	o = setDefaults(o)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// before
		startTime := time.Now()

		// processing
		resp, err := handler(ctx, req)

		// after
		service := path.Dir(info.FullMethod)[1:]
		method := path.Base(info.FullMethod)
		gs := grpcStats{
			metricName: o.MetricName,
			service:    service,
			method:     method,
			duration:   time.Since(startTime),
			code:       o.ErrorToCode(err),
		}
		gs.send(client)
		return resp, err
	}
}

// StreamServerInterceptor returns a new streaming server interceptor for panic recovery.
func StreamServerInterceptor(client *statsd.Client, o *Option) grpc.StreamServerInterceptor {
	o = setDefaults(o)
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		// before
		startTime := time.Now()

		// processing
		err = handler(srv, stream)

		// after
		service := path.Dir(info.FullMethod)[1:]
		method := path.Base(info.FullMethod)
		gs := grpcStats{
			metricName: o.MetricName,
			service:    service,
			method:     method,
			duration:   time.Since(startTime),
			code:       o.ErrorToCode(err),
		}
		gs.send(client)
		return err
	}
}
