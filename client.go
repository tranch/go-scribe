package scribe

import (
	"expvar"
	"log"
	"net/rpc"

	"github.com/samuel/go-metrics/metrics"
	"github.com/tranch/go-rpcext"
	"github.com/samuel/go-thrift/thrift"
)

var (
	statResponseOk       = metrics.NewCounter()
	statResponseTryLater = metrics.NewCounter()
)

func init() {
	m := expvar.NewMap("scribe")
	m.Set("response.ok", statResponseOk)
	m.Set("response.try_later", statResponseTryLater)
}

type ScribeClient struct {
	network string
	addr    string
	client  *rpcext.RPCExt
}

func NewScribeClient(network string, addr string, maxConnections int) (*ScribeClient, error) {
	s := &ScribeClient{
		network: network,
		addr:    addr,
	}
	s.client = rpcext.NewRPCExt("scribe", maxConnections, s)
	return s, nil
}

func (s *ScribeClient) NewClient() (*rpc.Client, error) {
	return thrift.Dial(s.network, s.addr, true, thrift.NewBinaryProtocol(true, false, 128))
}

func (s *ScribeClient) Log(entries []*LogEntry) (ResultCode, error) {
	var err error
	req := &ScribeLogRequest{}
	res := &ScribeLogResponse{Result: ResultCodeTryLater}
	for {
		req.Messages = entries
		err = s.client.Call("Log", req, res)
		if err == nil {
			switch res.Result {
			case ResultCodeOk:
				statResponseOk.Inc(1)
			case ResultCodeTryLater:
				statResponseTryLater.Inc(1)
			}
		} else {
			log.Printf("Scribe returned error: %+v", err)
		}
		return res.Result, err
	}
	panic("Should never be reached")
}
