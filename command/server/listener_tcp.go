// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package server

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/cli"
	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/internalshared/configutil"
	"github.com/openbao/openbao/internalshared/listenerutil"
)

func tcpListenerFactory(l *configutil.Listener, logger hclog.Logger, _ io.Writer, ui cli.Ui) (net.Listener, map[string]string, listenerutil.ReloadableCertGetter, error) {
	addr := l.Address
	if addr == "" {
		addr = "127.0.0.1:8200"
	}

	bindProto := "tcp"
	// If they've passed 0.0.0.0, we only want to bind on IPv4
	// rather than golang's dual stack default
	if strings.HasPrefix(addr, "0.0.0.0:") {
		bindProto = "tcp4"
	}

	ln, err := net.Listen(bindProto, addr)
	if err != nil {
		return nil, nil, nil, err
	}

	ln = TCPKeepAliveListener{ln.(*net.TCPListener)}

	ln, err = listenerWrapProxy(ln, l)
	if err != nil {
		return nil, nil, nil, err
	}

	props := map[string]string{"addr": addr}

	// X-Forwarded-For props
	{
		if len(l.XForwardedForAuthorizedAddrs) > 0 {
			props["x_forwarded_for_authorized_addrs"] = fmt.Sprintf("%v", l.XForwardedForAuthorizedAddrs)
		}

		if l.XForwardedForHopSkips > 0 {
			props["x_forwarded_for_hop_skips"] = fmt.Sprintf("%d", l.XForwardedForHopSkips)
		} else if len(l.XForwardedForAuthorizedAddrs) > 0 {
			props["x_forwarded_for_hop_skips"] = "0"
		}

		if len(l.XForwardedForAuthorizedAddrs) > 0 {
			props["x_forwarded_for_reject_not_present"] = strconv.FormatBool(l.XForwardedForRejectNotPresent)
		}

		if len(l.XForwardedForAuthorizedAddrs) > 0 {
			props["x_forwarded_for_reject_not_authorized"] = strconv.FormatBool(l.XForwardedForRejectNotAuthorized)
		}
	}

	tlsConfig, cg, err := listenerutil.TLSConfig(l, props, ui, logger)
	if err != nil {
		return nil, nil, nil, err
	}
	if tlsConfig != nil {
		ln = tls.NewListener(ln, tlsConfig)
	}

	return ln, props, cg, nil
}

// TCPKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
//
// This is copied directly from the Go source code.
type TCPKeepAliveListener struct {
	*net.TCPListener
}

func (ln TCPKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
