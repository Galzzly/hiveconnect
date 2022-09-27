package hiveconnect

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os/user"
	"strconv"
	"strings"
	"time"

	hiveserver "github.com/Galzzly/hiveconnect/hiveserver"
	sasl "github.com/Galzzly/hiveconnect/sasl"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
)

const DEFAULT_FETCH_SIZE int64 = 1000
const ZOOKEEPER_DEFAULT_NAMESPACE = "hiveserver2"
const DEFAULT_MAX_LENGTH = 16384000

type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

type Connection struct {
	host                string
	port                int
	username            string
	database            string
	auth                string
	kerberosServiceName string
	password            string
	// SessionHandle *hiveserver.TSessionHandle
	// client *hiveserver.TCLIServiceClient
	configuration *ConnectionConfiguration
	transport     thrift.TTransport
}

type ConnectionConfiguration struct {
	Username           string
	Principal          string
	Password           string
	Service            string
	HiveConfiguration  map[string]string
	PollIntervalInMS   int
	FetchSize          int64
	TransportMode      string
	HTTPPath           string
	TLSConfig          *tls.Config
	ZookeeperNamespace string
	Database           string
	ConnectTimeout     time.Duration
	SocketTimeout      time.Duration
	HttpTimeout        time.Duration
	DialContext        DialContextFunc
	DisableKeepAlives  bool
	MaxSize            uint32
}

func NewConnectionConfiguration() *ConnectionConfiguration {
	return &ConnectionConfiguration{
		Username:           "",
		Password:           "",
		Service:            "",
		HiveConfiguration:  nil,
		PollIntervalInMS:   200,
		FetchSize:          DEFAULT_FETCH_SIZE,
		TransportMode:      "binary",
		HTTPPath:           "cliservice",
		TLSConfig:          nil,
		ZookeeperNamespace: ZOOKEEPER_DEFAULT_NAMESPACE,
		MaxSize:            DEFAULT_MAX_LENGTH,
	}
}

type HiveError struct {
	error

	Message   string
	ErrorCode int
}

type inMemoryCookieJar struct {
	given   *bool
	storage map[string][]http.Cookie
}

func ConnectZookeeper(hosts, auth string,
	configuration *ConnectionConfiguration) (conn *Connection, err error) {
	zkHosts := strings.Split(hosts, ",")
	zkConn, _, err := zk.Connect(zkHosts, time.Second)
	if err != nil {
		return nil, err
	}

	hsInfos, _, err := zkConn.Children("/" + configuration.ZookeeperNamespace)
	if err != nil {
		panic(err)
	}

	if len(hsInfos) < 1 {
		return nil, errors.Errorf("no Hive server is registered in the specified Zookeeper namespace %s",
			configuration.ZookeeperNamespace)
	}

	nodes := parseHiveServer2Info(hsInfos)
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	for _, node := range nodes {
		port, err := strconv.Atoi(node["port"])
		if err != nil {
			continue
		}
		conn, err := innerConnect(context.TODO(), node["host"], port, auth, configuration)
	}
}

func parseHiveServer2Info(hsInfos []string) []map[string]string {
	results := make([]map[string]string, len(hsInfos))
	validCount := 0

	for _, hsInfo := range hsInfos {
		validFormat := false
		node := make(map[string]string)

		for _, param := range strings.Split(hsInfo, ";") {
			kvPair := strings.Split(param, "=")
			if len(kvPair) != 2 {
				break
			}
			if kvPair[0] == "serverUri" {
				hostPort := strings.Split(kvPair[1], ":")
				if len(hostPort) != 2 {
					break
				}
				node["host"] = hostPort[0]
				node["port"] = hostPort[1]
				validFormat = len(node["host"]) != 0 && len(node["port"]) != 0
			}
		}
		if validFormat {
			results[validCount] = node
			validCount++
		}
	}

	return results[0:validCount]
}

func innerConnect(ctx context.Context, host string, port int, auth string,
	configuration *ConnectionConfiguration) (conn *Connection, err error) {

	var socket thrift.TTransport
	addr := fmt.Sprintf("%s:%d", host, port)
	if configuration.DialContext != nil {
		socket, err = noDialContextSocket(addr, configuration, ctx)
	} else {
		socket = withDialContextSocket(addr, configuration)
		if err = socket.Open(); err != nil {
			return
		}
	}

	var transport thrift.TTransport
	if configuration == nil {
		configuration = NewConnectionConfiguration()
	}
	if configuration.Username == "" {
		_user, err := user.Current()
		if err != nil {
			return nil, errors.New("Unable to determine username")
		}
		configuration.Username = strings.Replace(_user.Name, " ", "", -1)
	}
	if configuration.Password == "" {
		configuration.Password = "x"
	}

	switch configuration.TransportMode {
	case "http":
		transport, err = httpTransport(socket, configuration, auth, host, port)
	case "binary":
		transport, err = binaryTransport(socket, configuration, auth, host, port)
	default:
		panic("Unsupported transport mode")
	}

	if err != nil {
		return nil, err
	}

	protoFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := hiveserver.NewTCLIServiceClientFactory(transport, protoFactory)

	openSession := hiveserver.NewTOpenSessionReq()
	openSession.ClientProtocol = hiveserver.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V6
	openSession.Configuration = configuration.HiveConfiguration
	openSession.Username = &configuration.Username
	openSession.Password = &configuration.Password

	res, err := client.OpenSession(context.Background(), openSession)
	return
}

func dial(ctx context.Context, addr string, dialFn DialContextFunc, timeout time.Duration) (net.Conn, error) {
	dctx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return dialFn(dctx, "tcp", addr)
}

func noDialContextSocket(addr string, configuration *ConnectionConfiguration,
	ctx context.Context) (socket thrift.TTransport, err error) {
	var netConn net.Conn
	netConn, err = dial(ctx, addr, configuration.DialContext, configuration.ConnectTimeout)
	if err != nil {
		return
	}
	if configuration.TLSConfig != nil {
		return thrift.NewTSSLSocketFromConnConf(netConn, &thrift.TConfiguration{
			ConnectTimeout: configuration.ConnectTimeout,
			SocketTimeout:  configuration.SocketTimeout,
			TLSConfig:      configuration.TLSConfig,
		}), nil
	}

	return thrift.NewTSocketFromConnConf(netConn, &thrift.TConfiguration{
		ConnectTimeout: configuration.ConnectTimeout,
		SocketTimeout:  configuration.SocketTimeout,
	}), nil
}

func withDialContextSocket(addr string, configuration *ConnectionConfiguration) thrift.TTransport {
	if configuration.TLSConfig != nil {
		return thrift.NewTSSLSocketConf(addr, &thrift.TConfiguration{
			ConnectTimeout: configuration.ConnectTimeout,
			SocketTimeout:  configuration.SocketTimeout,
			TLSConfig:      configuration.TLSConfig,
		})
	}
	return thrift.NewTSocketConf(addr, &thrift.TConfiguration{
		ConnectTimeout: configuration.ConnectTimeout,
		SocketTimeout:  configuration.SocketTimeout,
	})
}

func httpTransport(socket thrift.TTransport, configuration *ConnectionConfiguration, auth, host string, port int) (transport thrift.TTransport, err error) {
	switch auth {
	case "NONE":
		httpClient, protocol := getHTTPClient(configuration)
		httpOpts := thrift.THttpClientOptions{Client: httpClient}
		transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(
			fmt.Sprintf(protocol+"://%s:%s@%s:%d/"+configuration.HTTPPath,
				url.QueryEscape(configuration.Username),
				url.QueryEscape(configuration.Password),
				host,
				port),
			httpOpts).GetTransport(socket)
		if err != nil {
			return nil, err
		}
	case "KERBEROS":
		mechanism := sasl.NewGSSAPIMechanism(configuration.Service)
		saslClient := sasl.NewSaslClient(host, mechanism)
		token, err := saslClient.Start()
		if err != nil {
			return nil, err
		}
		if len(token) == 0 {
			return nil, errors.New("Empty token returned. Service configuration may be empty")

		}

		httpClient, protocol := getHTTPClient(configuration)
		httpClient.Jar = newCookieJar()
		httpOpts := thrift.THttpClientOptions{
			Client: httpClient,
		}

		transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(fmt.Sprintf(protocol+
			"://%s:%d/"+configuration.HTTPPath, host, port), httpOpts).GetTransport(socket)
		if err != nil {
			return nil, err
		}
		httpTransport, ok := transport.(*thrift.THttpClient)
		if ok {
			httpTransport.SetHeader("Authorization", "Negotiate "+base64.StdEncoding.EncodeToString(token))
		}
	default:
		panic("Unrecognized auth")
	}

	return transport, nil
}

func binaryTransport(socket thrift.TTransport, configuration *ConnectionConfiguration, auth, host string, port int) (transport thrift.TTransport, err error) {
	switch auth {
	case "NOSASL":
		transport = thrift.NewTBufferedTransport(socket, 4096)
		if transport == nil {
			return nil, errors.New("BufferedTransport was nil")
		}
	case "NONE":
		saslConfiguration := map[string]string{"username": configuration.Username,
			"password": configuration.Password,
		}
		transport = sasl.NewTSaslTransport(socket, host, "PLAIN", saslConfiguration, configuration.MaxSize)
	case "KERBEROS":
		saslConfiguration := map[string]string{"service": configuration.Service}
		transport = sasl.NewTSaslTransport(socket, host, "GSSAPI", saslConfiguration, configuration.MaxSize)
	case "DIGEST-MD5":
		saslConfiguration := map[string]string{"username": configuration.Username,
			"password": configuration.Password,
			"service":  configuration.Service,
		}
		transport = sasl.NewTSaslTransport(socket, host, "DIGEST-MD5", saslConfiguration, configuration.MaxSize)
	default:
		panic("Unrecognized auth")
	}

	if !transport.IsOpen() {
		if err = transport.Open(); err != nil {
			return
		}
	}
	return
}

func getHTTPClient(configuration *ConnectionConfiguration) (httpClient *http.Client, protocol string) {
	if configuration.TLSConfig != nil {
		return &http.Client{
			Timeout: configuration.HttpTimeout,
			Transport: &http.Transport{
				TLSClientConfig:   configuration.TLSConfig,
				DialContext:       configuration.DialContext,
				DisableKeepAlives: configuration.DisableKeepAlives,
			},
		}, "https"
	}
	return &http.Client{
		Timeout: configuration.HttpTimeout,
		Transport: &http.Transport{
			DialContext:       configuration.DialContext,
			DisableKeepAlives: configuration.DisableKeepAlives,
		},
	}, "http"
}

func newCookieJar() inMemoryCookieJar {
	f := false
	return inMemoryCookieJar{&f, make(map[string][]http.Cookie)}

}

func (cj inMemoryCookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	for _, c := range cookies {
		cj.storage["cliservice"] = []http.Cookie{*c}
	}
	*cj.given = false
}

func (cj inMemoryCookieJar) Cookies(u *url.URL) []*http.Cookie {
	cArray := []*http.Cookie{}
	for p, c := range cj.storage {
		if strings.Contains(u.String(), p) {
			for i := range c {
				cArray = append(cArray, &c[i])
			}
		}
	}
	if !*cj.given {
		*cj.given = true
		return cArray
	}
	return nil
}
