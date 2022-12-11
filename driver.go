package dtm_driver_micro

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/dtm-labs/dtmdriver"
	"github.com/go-micro/plugins/v4/registry/consul"
	"go-micro.dev/v4/registry"
)

const (
	DriverName = "dtm-driver-micro"
	kindConsul = "consul"
)

type (
	microDriver struct{}
)

func (z *microDriver) GetName() string {
	return DriverName
}

func (z *microDriver) RegisterAddrResolver() {
}

func (z *microDriver) RegisterService(target string, endpoint string) error {
	if target == "" {
		return nil
	}

	u, err := url.Parse(target)
	if err != nil {
		return err
	}

	switch u.Scheme {
	case kindConsul:
		r := consul.NewRegistry(registry.Addrs(u.Host))
		serverName := strings.TrimPrefix(u.Path, "/")
		r.Register(&registry.Service{
			Name:    serverName,
			Version: "v1.0.0",
			Nodes: []*registry.Node{
				{
					Id:      fmt.Sprintf("%s.1", serverName),
					Address: u.Host,
					Metadata: map[string]string{
						"protocol": "grpc",
					},
				},
			},
		})
	default:
		return fmt.Errorf("unknown scheme: %s", u.Scheme)
	}

	return nil
}

func (z *microDriver) ParseServerMethod(uri string) (server string, method string, err error) {
	if !strings.Contains(uri, "//") {
		sep := strings.IndexByte(uri, '/')
		if sep == -1 {
			return "", "", fmt.Errorf("bad url: '%s'. no '/' found", uri)
		}
		return uri[:sep], uri[sep:], nil

	}
	//resolve gozero consul wait=xx url.Parse no standard
	if strings.Contains(uri, kindConsul) {
		tmp := strings.Split(uri, "?")
		sep := strings.IndexByte(tmp[1], '/')
		if sep == -1 {
			return "", "", fmt.Errorf("bad url: '%s'. no '/' found", uri)
		}
		uri = tmp[0] + tmp[1][sep:]
	}

	u, err := url.Parse(uri)
	if err != nil {
		return "", "", nil
	}
	index := strings.IndexByte(u.Path[1:], '/') + 1

	return u.Scheme + "://" + u.Host + u.Path[:index], u.Path[index:], nil
}

func init() {
	dtmdriver.Register(&microDriver{})
}

