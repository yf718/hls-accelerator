package downloader

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type aria2Notification struct {
	Method string                   `json:"method"`
	Params []map[string]interface{} `json:"params"`
}

func (c *Aria2Client) ListenNotifications(ctx context.Context, handler func(method, gid string)) error {
	if c == nil {
		return fmt.Errorf("aria2 client is nil")
	}

	conn, err := c.dialWebSocket(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	for {
		payload, opcode, err := readWebSocketFrame(conn)
		if err != nil {
			return err
		}

		switch opcode {
		case 0x1:
			var msg aria2Notification
			if err := json.Unmarshal(payload, &msg); err != nil {
				continue
			}
			if len(msg.Params) == 0 {
				continue
			}
			gid, _ := msg.Params[0]["gid"].(string)
			handler(msg.Method, gid)
		case 0x8:
			return io.EOF
		case 0x9:
			if err := writeWebSocketFrame(conn, 0xA, payload); err != nil {
				return err
			}
		}
	}
}

func (c *Aria2Client) dialWebSocket(ctx context.Context) (net.Conn, error) {
	wsURL, err := c.websocketURL()
	if err != nil {
		return nil, err
	}

	host := wsURL.Host
	if !strings.Contains(host, ":") {
		switch wsURL.Scheme {
		case "wss":
			host += ":443"
		default:
			host += ":80"
		}
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var conn net.Conn
	switch wsURL.Scheme {
	case "wss":
		conn, err = tls.DialWithDialer(dialer, "tcp", host, &tls.Config{
			ServerName: strings.Split(wsURL.Host, ":")[0],
		})
	default:
		conn, err = dialer.DialContext(ctx, "tcp", host)
	}
	if err != nil {
		return nil, err
	}

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, err
	}
	secKey := base64.StdEncoding.EncodeToString(keyBytes)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, wsURL.String(), nil)
	if err != nil {
		conn.Close()
		return nil, err
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", secKey)

	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, err
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		resp.Body.Close()
		conn.Close()
		return nil, fmt.Errorf("websocket upgrade failed: %s", resp.Status)
	}

	accept := resp.Header.Get("Sec-WebSocket-Accept")
	if accept != expectedAcceptKey(secKey) {
		resp.Body.Close()
		conn.Close()
		return nil, fmt.Errorf("invalid websocket accept key")
	}

	return &bufferedConn{Conn: conn, reader: br}, nil
}

func (c *Aria2Client) websocketURL() (*url.URL, error) {
	rpcURL, err := url.Parse(c.RPCUrl)
	if err != nil {
		return nil, err
	}
	switch rpcURL.Scheme {
	case "https":
		rpcURL.Scheme = "wss"
	default:
		rpcURL.Scheme = "ws"
	}
	return rpcURL, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func expectedAcceptKey(secKey string) string {
	hash := sha1.Sum([]byte(secKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(hash[:])
}

func readWebSocketFrame(r io.Reader) ([]byte, byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, 0, err
	}

	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	if !fin {
		return nil, 0, fmt.Errorf("fragmented websocket frames are not supported")
	}

	masked := header[1]&0x80 != 0
	payloadLen := uint64(header[1] & 0x7F)
	switch payloadLen {
	case 126:
		var ext uint16
		if err := binary.Read(r, binary.BigEndian, &ext); err != nil {
			return nil, 0, err
		}
		payloadLen = uint64(ext)
	case 127:
		if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
			return nil, 0, err
		}
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(r, maskKey[:]); err != nil {
			return nil, 0, err
		}
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}

	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return payload, opcode, nil
}

func writeWebSocketFrame(w io.Writer, opcode byte, payload []byte) error {
	header := []byte{0x80 | opcode}
	payloadLen := len(payload)
	maskKey := make([]byte, 4)
	if _, err := rand.Read(maskKey); err != nil {
		return err
	}

	switch {
	case payloadLen < 126:
		header = append(header, byte(payloadLen)|0x80)
	case payloadLen <= 0xFFFF:
		header = append(header, 126|0x80)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(payloadLen))
		header = append(header, ext...)
	default:
		header = append(header, 127|0x80)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(payloadLen))
		header = append(header, ext...)
	}

	maskedPayload := make([]byte, payloadLen)
	copy(maskedPayload, payload)
	for i := range maskedPayload {
		maskedPayload[i] ^= maskKey[i%4]
	}

	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(maskKey); err != nil {
		return err
	}
	_, err := w.Write(maskedPayload)
	return err
}
