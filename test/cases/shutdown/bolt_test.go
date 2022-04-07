// +build MOSNTest

package shutdown

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"mosn.io/api"
	"mosn.io/pkg/buffer"

	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/network"
	"mosn.io/mosn/pkg/protocol"
	"mosn.io/mosn/pkg/protocol/xprotocol"
	"mosn.io/mosn/pkg/protocol/xprotocol/bolt"
	"mosn.io/mosn/pkg/stream"
	xstream "mosn.io/mosn/pkg/stream/xprotocol"
	"mosn.io/mosn/pkg/trace"
	xtrace "mosn.io/mosn/pkg/trace/sofa/xprotocol"
	"mosn.io/mosn/pkg/types"
	. "mosn.io/mosn/test/framework"
	"mosn.io/mosn/test/lib/mosn"
)

type BoltClient struct {
	proto  types.ProtocolName
	Client stream.Client
	conn   types.ClientConnection
	Id     uint64
	goaway bool
}

func NewBoltClient(addr string, proto types.ProtocolName) *BoltClient {
	c := &BoltClient{}
	stopChan := make(chan struct{})
	remoteAddr, _ := net.ResolveTCPAddr("tcp", addr)
	conn := network.NewClientConnection(0, nil, remoteAddr, stopChan)
	if err := conn.Connect(); err != nil {
		fmt.Println(err)
		return nil
	}
	// pass sub protocol to stream client
	c.Client = stream.NewStreamClient(context.Background(), proto, conn, nil)
	c.conn = conn
	c.proto = proto
	return c
}

func (c *BoltClient) OnReceive(ctx context.Context, headers types.HeaderMap, data types.IoBuffer, trailers types.HeaderMap) {
	fmt.Printf("[Xprotocol RPC BoltClient] Receive Data:")
	if cmd, ok := headers.(api.XFrame); ok {
		streamID := protocol.StreamIDConv(cmd.GetRequestId())

		if predicate, ok := cmd.(api.GoAwayPredicate); ok {
			if predicate.IsGoAwayFrame() {
				log.DefaultLogger.Infof("got goaway frame, stream:", streamID)
				c.goaway = true
			}
		}
		if resp, ok := cmd.(api.XRespFrame); ok {
			fmt.Println("stream:", streamID, " status:", resp.GetStatusCode())
		} else {
			fmt.Println("resp is not api.XRespFrame")
			return
		}
	}
}

func (c *BoltClient) OnDecodeError(context context.Context, err error, headers types.HeaderMap) {}

func (c *BoltClient) Request(header map[string]string) {
	c.Id++
	requestEncoder := c.Client.NewStream(context.Background(), c)

	var request api.XFrame
	switch c.proto {
	case bolt.ProtocolName:
		request = bolt.NewRpcRequest(uint32(c.Id), protocol.CommonHeader(header), nil)
	default:
		panic("unknown protocol, please complete the protocol-switch in BoltClient.Request method")
	}

	err := requestEncoder.AppendHeaders(context.Background(), request.GetHeader(), true)
	if err != nil {
		fmt.Println("[Xprotocol RPC BoltClient] AppendHeaders error:", err)
	}
}

type BoltServer struct {
	Listener     net.Listener
	protocolName api.ProtocolName
	protocol     api.XProtocol
	connected    int
	closed       int
	Mode         int
}

func NewBoltServer(addr string, proto api.XProtocol) *BoltServer {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return &BoltServer{
		Listener:     ln,
		protocolName: proto.Name(),
		protocol:     proto,
	}
}

func (s *BoltServer) Run() {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				fmt.Printf("[Xprotocol RPC BoltServer] Accept temporary error: %v\n", ne)
				continue
			} else {
				//not temporary error, exit
				fmt.Printf("[Xprotocol RPC BoltServer] Accept error: %v\n", err)
				return
			}
		}
		s.connected++
		fmt.Println("[Xprotocol RPC BoltServer] get connection :", conn.RemoteAddr().String())
		go s.Serve(conn)
	}
}

func (s *BoltServer) Serve(conn net.Conn) {
	iobuf := buffer.NewIoBuffer(102400)
	defer func() {
		s.closed++
		conn.Close()
	}()
	for {
		now := time.Now()
		conn.SetReadDeadline(now.Add(30 * time.Second))
		buf := make([]byte, 10*1024)
		bytesRead, err := conn.Read(buf)
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				fmt.Printf("[Xprotocol RPC BoltServer] Connect read error: %v\n", err)
				continue
			} else {
				fmt.Printf("[Xprotocol RPC BoltServer] Connect read error: %v\n", err)
				return
			}

		}
		if s.Mode == GoAwayBeforeResponse {
			if err := sendBoltGoAway(conn); err != nil {
				fmt.Printf("[Xprotocol RPC BoltServer] send goaway error: %v\n", err)
				return
			}
		}
		if bytesRead > 0 {
			iobuf.Write(buf[:bytesRead])
			for iobuf.Len() > 1 {
				cmd, _ := s.protocol.Decode(nil, iobuf)
				if cmd == nil {
					break
				}

				// handle request
				resp, err := s.HandleRequest(conn, cmd)
				if err != nil {
					fmt.Printf("[Xprotocol RPC BoltServer] handle request error: %v\n", err)
					return
				}
				respData, err := s.protocol.Encode(context.Background(), resp)
				if err != nil {
					fmt.Printf("[Xprotocol RPC BoltServer] encode response error: %v\n", err)
					return
				}

				// sleep 500 ms
				time.Sleep(time.Millisecond * 500)

				conn.Write(respData.Bytes())

				if s.Mode == GoAwayAfterResponse {
					// sleep a while to wait the previous request finished in the mosn side.
					time.Sleep(time.Millisecond * 10)

					if err := sendBoltGoAway(conn); err != nil {
						fmt.Printf("[Xprotocol RPC BoltServer] send goaway error: %v\n", err)
						return
					}
				}
			}
		}
	}
}

func (s *BoltServer) HandleRequest(conn net.Conn, cmd interface{}) (api.XRespFrame, error) {
	switch s.protocolName {
	case bolt.ProtocolName:
		if req, ok := cmd.(*bolt.Request); ok {
			switch req.CmdCode {
			case bolt.CmdCodeHeartbeat:
				hbAck := s.protocol.Reply(context.TODO(), req)
				fmt.Printf("[Xprotocol RPC BoltServer] response bolt heartbeat, connection: %s, requestId: %d\n", conn.RemoteAddr().String(), req.GetRequestId())
				return hbAck, nil
			case bolt.CmdCodeRpcRequest:
				resp := bolt.NewRpcResponse(req.RequestId, bolt.ResponseStatusSuccess, nil, nil)
				fmt.Printf("[Xprotocol RPC BoltServer] response bolt request, connection: %s, requestId: %d\n", conn.RemoteAddr().String(), req.GetRequestId())
				return resp, nil
			}
		}
	}
	return nil, errors.New("unknown protocol:" + string(s.protocolName))
}

func sendBoltGoAway(c net.Conn) error {
	buf := make([]byte, 0)
	buf = append(buf, bolt.ProtocolCode)
	buf = append(buf, bolt.CmdTypeRequest)

	tempBytes := make([]byte, 4)
	binary.BigEndian.PutUint16(tempBytes, bolt.CmdCodeGoAway)
	buf = append(buf, tempBytes...)

	buf = append(buf, bolt.ProtocolVersion)

	tempBytes = make([]byte, 4)
	binary.BigEndian.PutUint32(tempBytes, 0)
	buf = append(buf, tempBytes...)

	buf = append(buf, bolt.Hessian2Serialize)

	tempBytes = make([]byte, 4)
	binary.BigEndian.PutUint32(tempBytes, 0)
	buf = append(buf, tempBytes...)

	if _, err := c.Write(buf); err != nil {
		return err
	}
	fmt.Println("[Xprotocol RPC BoltServer] GoAway has been sent")
	return nil
}
func stopBoltServer(s *BoltServer) {
	s.Listener.Close()
}

func TestBoltClientAndServer(t *testing.T) {
	Scenario(t, "test bolt request and response", func() {
		server := NewBoltServer("127.0.0.1:8080", (&bolt.XCodec{}).NewXProtocol(context.Background()))
		if server == nil {
			t.Fatalf("create bolt server failed")
		}
		Setup(func() {
			// start server
			go server.Run()

			// register bolt
			// tracer driver register
			trace.RegisterDriver("SOFATracer", trace.NewDefaultDriverImpl())
			// xprotocol action register
			xprotocol.ResgisterXProtocolAction(xstream.NewConnPool, xstream.NewStreamFactory, func(codec api.XProtocolCodec) {
				name := codec.ProtocolName()
				trace.RegisterTracerBuilder("SOFATracer", name, xtrace.NewTracer)
			})
			// xprotocol register
			_ = xprotocol.RegisterXProtocolCodec(&bolt.XCodec{})
		})
		Case("client-server", func() {
			testcases := []struct {
				reqHeader map[string]string
			}{
				{
					reqHeader: map[string]string{
						"test-key": "test-value",
					},
				},
			}
			for _, tc := range testcases {
				client := NewBoltClient("127.0.0.1:8080", bolt.ProtocolName)
				if client == nil {
					t.Fatalf("create bolt client failed")
				}
				client.Request(tc.reqHeader)
				Verify(server.connected, Equal, 1)
			}
		})
		TearDown(func() {
			stopBoltServer(server)
		})
	})

}

// server(mosn) send goaway frame when start graceful stop
func TestBoltGracefulStop(t *testing.T) {
	Scenario(t, "bolt graceful stop", func() {
		var m *mosn.MosnOperator
		server := NewBoltServer("127.0.0.1:8080", (&bolt.XCodec{}).NewXProtocol(context.Background()))
		if server == nil {
			t.Fatalf("create bolt server failed")
		}
		server.Mode = NoGoAway
		Setup(func() {
			// start server
			go server.Run()

			// register bolt
			// tracer driver register
			trace.RegisterDriver("SOFATracer", trace.NewDefaultDriverImpl())
			// xprotocol action register
			xprotocol.ResgisterXProtocolAction(xstream.NewConnPool, xstream.NewStreamFactory, func(codec api.XProtocolCodec) {
				name := codec.ProtocolName()
				trace.RegisterTracerBuilder("SOFATracer", name, xtrace.NewTracer)
			})
			// xprotocol register
			_ = xprotocol.RegisterXProtocolCodec(&bolt.XCodec{})

			m = mosn.StartMosn(ConfigSimpleBoltExample)
			Verify(m, NotNil)
			time.Sleep(15 * time.Second) // wait mosn start
		})
		Case("client-mosn-server", func() {
			testcases := []struct {
				reqHeader map[string]string
			}{
				{
					reqHeader: map[string]string{
						"test-key": "test-value",
					},
				},
			}

			for _, tc := range testcases {
				client := NewBoltClient("127.0.0.1:2046", bolt.ProtocolName)
				if client == nil {
					t.Fatalf("create bolt client failed")
				}
				// 1. simple
				var start time.Time
				start = time.Now()
				client.Request(tc.reqHeader)
				log.DefaultLogger.Infof("request cost %v", time.Since(start))
				time.Sleep(3 * time.Second) // wait request
				//Verify(err, Equal, nil)
				//Verify(resp, Equal, tc.reqHeader)
				Verify(client.goaway, Equal, false)
				Verify(server.connected, Equal, 1)

				// 2. graceful stop after send request and before received the response
				go func() {
					time.Sleep(time.Millisecond * 10)
					m.GracefulStop()
				}()
				time.Sleep(10 * time.Millisecond) // wait mosn stop
				start = time.Now()
				//resp, err = boltRequest(client, tc.reqHeader)
				client.Request(tc.reqHeader)
				log.DefaultLogger.Infof("request cost %v", time.Since(start))
				time.Sleep(3 * time.Second) // wait request
				//Verify(err, Equal, nil)
				//Verify(resp, Equal, tc.reqHeader)
				Verify(client.goaway, Equal, true)
				Verify(server.connected, Equal, 1)
			}
		})
		TearDown(func() {
			stopBoltServer(server)
			m.Stop()
		})
	})
}

// client(mosn) got away frame before get response
func TestBoltServerGoAwayBeforeResponse(t *testing.T) {
	Scenario(t, "bolt server goaway", func() {
		var m *mosn.MosnOperator
		server := NewBoltServer("127.0.0.1:8080", (&bolt.XCodec{}).NewXProtocol(context.Background()))
		if server == nil {
			t.Fatalf("create bolt server failed")
		}
		server.Mode = GoAwayBeforeResponse
		Setup(func() {
			// start server
			go server.Run()

			// register bolt
			// tracer driver register
			trace.RegisterDriver("SOFATracer", trace.NewDefaultDriverImpl())
			// xprotocol action register
			xprotocol.ResgisterXProtocolAction(xstream.NewConnPool, xstream.NewStreamFactory, func(codec api.XProtocolCodec) {
				name := codec.ProtocolName()
				trace.RegisterTracerBuilder("SOFATracer", name, xtrace.NewTracer)
			})
			// xprotocol register
			_ = xprotocol.RegisterXProtocolCodec(&bolt.XCodec{})

			m = mosn.StartMosn(ConfigSimpleBoltExample)
			Verify(m, NotNil)
			time.Sleep(5 * time.Second) // wait mosn start
		})
		Case("client-mosn-server", func() {
			testcases := []struct {
				reqHeader map[string]string
			}{
				{
					reqHeader: map[string]string{
						"test-key": "test-value",
					},
				},
			}

			for _, tc := range testcases {
				client := NewBoltClient("127.0.0.1:2046", bolt.ProtocolName)
				if client == nil {
					t.Fatalf("create bolt client failed")
				}
				// 1. simple
				var start time.Time
				start = time.Now()
				client.Request(tc.reqHeader)
				log.DefaultLogger.Infof("request cost %v", time.Since(start))
				time.Sleep(3 * time.Second) // wait request
				//Verify(err, Equal, nil)
				//Verify(resp, Equal, tc.reqBody)
				Verify(client.goaway, Equal, false)
				Verify(server.connected, Equal, 1)

				// sleep a while
				time.Sleep(time.Millisecond * 10)
				Verify(server.closed, Equal, 1)

				// 2. try again
				start = time.Now()
				client.Request(tc.reqHeader)
				log.DefaultLogger.Infof("request cost %v", time.Since(start))
				time.Sleep(3 * time.Second) // wait request
				//Verify(err, Equal, nil)
				//Verify(resp, Equal, tc.reqBody)
				Verify(client.goaway, Equal, false)
				//Verify(server.connected, Equal, 2)

				// sleep a while
				time.Sleep(time.Millisecond * 10)
				Verify(server.closed, Equal, 2)
			}
		})
		TearDown(func() {
			stopBoltServer(server)
			m.Stop()
		})
	})
}

// client(mosn) got away frame after get response
func TestBoltServerGoAwayAfterResponse(t *testing.T) {
	Scenario(t, "bolt server goaway", func() {
		var m *mosn.MosnOperator
		server := NewBoltServer("127.0.0.1:8080", (&bolt.XCodec{}).NewXProtocol(context.Background()))
		if server == nil {
			t.Fatalf("create bolt server failed")
		}
		server.Mode = GoAwayAfterResponse
		Setup(func() {
			// start server
			go server.Run()

			// register bolt
			// tracer driver register
			trace.RegisterDriver("SOFATracer", trace.NewDefaultDriverImpl())
			// xprotocol action register
			xprotocol.ResgisterXProtocolAction(xstream.NewConnPool, xstream.NewStreamFactory, func(codec api.XProtocolCodec) {
				name := codec.ProtocolName()
				trace.RegisterTracerBuilder("SOFATracer", name, xtrace.NewTracer)
			})
			// xprotocol register
			_ = xprotocol.RegisterXProtocolCodec(&bolt.XCodec{})

			m = mosn.StartMosn(ConfigSimpleBoltExample)
			Verify(m, NotNil)
			time.Sleep(5 * time.Second) // wait mosn start
		})
		Case("client-mosn-server", func() {
			testcases := []struct {
				reqHeader map[string]string
			}{
				{
					reqHeader: map[string]string{
						"test-key": "test-value",
					},
				},
			}

			for _, tc := range testcases {
				client := NewBoltClient("127.0.0.1:2046", bolt.ProtocolName)
				if client == nil {
					t.Fatalf("create bolt client failed")
				}
				// 1. simple
				var start time.Time
				start = time.Now()
				client.Request(tc.reqHeader)
				log.DefaultLogger.Infof("request cost %v", time.Since(start))
				time.Sleep(3 * time.Second) // wait request
				//Verify(err, Equal, nil)
				//Verify(resp, Equal, tc.reqBody)
				Verify(client.goaway, Equal, false)
				Verify(server.connected, Equal, 1)

				// sleep a while
				time.Sleep(time.Millisecond * 100)
				Verify(server.closed, Equal, 1)

				// 2. try again
				start = time.Now()
				client.Request(tc.reqHeader)
				log.DefaultLogger.Infof("request cost %v", time.Since(start))
				time.Sleep(3 * time.Second) // wait request
				//Verify(err, Equal, nil)
				//Verify(resp, Equal, tc.reqBody)
				Verify(client.goaway, Equal, false)
				Verify(server.connected, Equal, 2)

				// sleep a while
				time.Sleep(time.Millisecond * 100)
				Verify(server.closed, Equal, 2)
			}
		})
		TearDown(func() {
			stopBoltServer(server)
			m.Stop()
		})
	})
}

const ConfigSimpleBoltExample = `{
    "servers": [
        {
            "default_log_path": "stdout",
            "default_log_level": "ERROR",
            "routers": [
                {
                    "router_config_name": "router_to_server",
                    "virtual_hosts": [
                        {
                            "name": "server_hosts",
                            "domains": [
                                "*"
                            ],
                            "routers": [
                                {
                                    "route": {
                                        "cluster_name": "server_cluster"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],
            "listeners": [
                {
                    "address": "127.0.0.1:2046",
                    "bind_port": true,
                    "filter_chains": [
                        {
                            "filters": [
                                {
                                    "type": "proxy",
                                    "config": {
                                        "downstream_protocol": "bolt",
                                        "router_config_name": "router_to_server",
                                        "extend_config": {
                                            "enable_bolt_go_away": true
                                        }
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ],
    "cluster_manager": {
        "clusters": [
            {
                "name": "server_cluster",
                "type": "SIMPLE",
                "lb_type": "LB_RANDOM",
                "hosts": [
                    {
                        "address": "127.0.0.1:8080"
                    }
                ]
            }
        ]
    },
	"admin": {
		"address": {
			"socket_address": {
				"address": "0.0.0.0",
				"port_value": 34909
			}
		}
	}
}`
