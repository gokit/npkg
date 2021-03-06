package nhttp

import (
	"crypto/tls"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/crypto/acme/autocert"
)

// LetsEncryptTLS returns a tls.Config instance which retrieves its
// its tls certificate from LetsEncrypt service.
func LetsEncryptTLS(http2 bool) (*autocert.Manager, *tls.Config) {
	manager := &autocert.Manager{
		Prompt: autocert.AcceptTOS,
	}

	var tlsConfig tls.Config
	tlsConfig.GetCertificate = manager.GetCertificate

	if http2 {
		tlsConfig.NextProtos = append(tlsConfig.NextProtos, "h2")
	}

	return manager, &tlsConfig
}

//LoadTLS loads a tls.Config from a key and cert file path
func LoadTLS(cert, key string) (*tls.Config, error) {
	var config = &tls.Config{}
	config.Certificates = make([]tls.Certificate, 1)

	c, err := tls.LoadX509KeyPair(cert, key)

	if err != nil {
		return nil, err
	}

	config.Certificates[0] = c
	return config, nil
}

// WaitOnInterrupt will register the needed signals to wait until it recieves
// a os interrupt singnal and calls any provided functions later.
func WaitOnInterrupt(cbs ...func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, os.Interrupt, syscall.SIGSTOP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-ch

	for _, cb := range cbs {
		cb()
	}
}
