package dtm_driver_micro

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-micro/plugins/v4/registry/consul"
	"go-micro.dev/v4/registry"

	"github.com/dtm-labs/dtmdriver"
)

const (
	DriverName = "dtm-driver-micro"
	kindConsul = "consul"
)

type (
	microDriver struct {
	}
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
		return r.Register(&registry.Service{
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

func methodToGRPC(service, method string) string {
	// no method or already grpc method
	if len(method) == 0 || method[0] == '/' {
		return method
	}

	// assume method is Foo.Bar
	mParts := strings.Split(method, ".")
	if len(mParts) != 2 {
		return method
	}

	if len(service) == 0 {
		return fmt.Sprintf("/%s/%s", mParts[0], mParts[1])
	}

	// return /pkg.Foo/Bar
	return fmt.Sprintf("/%s.%s/%s", service, mParts[0], mParts[1])
}

func (z *microDriver) ParseServerMethod(uri string) (server string, method string, err error) {
	if !strings.Contains(uri, "//") {
		sep := strings.IndexByte(uri, '/')
		if sep == -1 {
			return "", "", fmt.Errorf("bad url: '%s'. no '/' found", uri)
		}
		return uri[:sep], uri[sep:], nil

	}
	if strings.Contains(uri, kindConsul) {
		tmp := strings.Split(uri, "//")
		subStr := strings.Split(tmp[1], "/")
		if len(subStr) < 3 {
			return "", "", fmt.Errorf("bad url: %s", uri)
		}

		cRegistry := consul.NewRegistry(func(options *registry.Options) {
			options.Addrs = append(options.Addrs, subStr[0])
		})

		sName := subStr[1]
		services, err := cRegistry.GetService(sName)
		if err != nil || len(services) == 0 {
			return "", "", fmt.Errorf("inavlid service name: %s", sName)
		}

		sEndpoint := ""
		for _, ep := range services[0].Endpoints {
			if strings.Contains(ep.Name, subStr[2]) {
				sEndpoint = ep.Name
			}
		}

		return services[0].Nodes[0].Address, methodToGRPC(sName, sEndpoint), nil
	}

	return "", "", fmt.Errorf("bad url because of invalid scheme: %s", uri)
}

func init() {
	dtmdriver.Register(&microDriver{})
}
