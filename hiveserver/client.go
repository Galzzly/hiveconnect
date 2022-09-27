package hiveserver

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
)

type TCLIServiceClient struct {
	c    thrift.TClient
	meta thrift.ResponseMeta
}

func NewTCLIServiceClientFactory(t thrift.TTransport, f thrift.TProtocolFactory) *TCLIServiceClient {
	return &TCLIServiceClient{
		c: thrift.NewTStandardClient(f.GetProtocol(t), f.GetProtocol(t)),
	}
}

func (c *TCLIServiceClient) OpenSession(ctx context.Context, req *TOpenSessionReq) (r *TOpenSessionResp, err error) {
	var args TCLIServiceOpenSessionArgs
	args.Req = req

	var result TCLIServiceOpenSessionResult
	var meta thrift.ResponseMeta
	meta, err = c.Client().Call(ctx, "OpenSession", &args, &result)
	c.SetLastResponse(meta)
	if err != nil {
		return
	}
	return result.GetSuccess(), nil
}

func (c *TCLIServiceClient) Client() thrift.TClient {
	return c.c
}

func (c *TCLIServiceClient) SetLastResponse(meta thrift.ResponseMeta) {
	c.meta = meta
}
