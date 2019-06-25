package beanstalk

import (
  "bufio"
  "encoding/base64"
  "errors"
  "fmt"
  "io"
  "net"
  "strings"
  "time"

  "github.com/kwf2030/commons/base"
)

const (
  mtu     = 1450
  tubeLen = 200
)

var (
  ErrOutOfMemory    = errors.New("out of memory")
  ErrInternalError  = errors.New("internal error")
  ErrBadFormat      = errors.New("bad format")
  ErrUnknownCommand = errors.New("unknown command")

  ErrExpectedCRLF = errors.New("expected CRLF")
  ErrJobTooBig    = errors.New("job too big")
  ErrDraining     = errors.New("draining")

  ErrDeadlineSoon = errors.New("deadline soon")
  ErrTimedOut     = errors.New("timed out")

  ErrNotFound = errors.New("not found")

  ErrNotIgnored = errors.New("not ignored")

  ErrBuried = errors.New("buried")
)

var errMap = map[string]error{
  "OUT_OF_MEMORY\r\n":   ErrOutOfMemory,
  "INTERNAL_ERROR\r\n":  ErrInternalError,
  "BAD_FORMAT\r\n":      ErrBadFormat,
  "UNKNOWN_COMMAND\r\n": ErrUnknownCommand,

  "EXPECTED_CRLF\r\n": ErrExpectedCRLF,
  "JOB_TOO_BIG\r\n":   ErrJobTooBig,
  "DRAINING\r\n":      ErrDraining,

  "DEADLINE_SOON\r\n": ErrDeadlineSoon,
  "TIMED_OUT\r\n":     ErrTimedOut,

  "NOT_FOUND\r\n": ErrNotFound,

  "NOT_IGNORED\r\n": ErrNotIgnored,

  "BURIED\r\n": ErrBuried,
}

func parseError(str string) error {
  if e, ok := errMap[str]; ok {
    return e
  }
  return errors.New("unknown error: " + str)
}

func isNetErrorTemporary(err error) bool {
  e, ok := err.(net.Error)
  return ok && e.Temporary()
}

type Conn struct {
  conn net.Conn
  addr string

  bufReader *bufio.Reader
  bufWriter *bufio.Writer

  heartbeat int
  ticker    *time.Ticker
}

func Dial(host string, port int) (*Conn, error) {
  if port <= 0 {
    return nil, base.ErrInvalidArgument
  }
  addr := fmt.Sprintf("%s:%d", host, port)
  conn, e := net.Dial("tcp", addr)
  if e != nil {
    return nil, e
  }
  return &Conn{
    conn:      conn,
    addr:      addr,
    bufReader: bufio.NewReader(conn),
    bufWriter: bufio.NewWriter(conn),
  }, nil
}

func (c *Conn) EnableHeartbeat(sec int) {
  if c.ticker != nil {
    c.ticker.Stop()
    c.ticker = nil
  }
  if sec <= 0 {
    c.heartbeat = 0
    return
  }
  c.ticker = time.NewTicker(time.Second * time.Duration(sec))
  go func() {
    for range c.ticker.C {
      c.Ignore("__heartbeat__")
    }
  }()
}

func (c *Conn) Put(priority, delay, ttr int, data []byte) (string, error) {
  if priority < 0 {
    priority = 0
  }
  if delay < 0 {
    delay = 0
  }
  if ttr < 0 {
    ttr = 1
  }
  var str string
  if len(data) > 0 {
    body := base64.RawStdEncoding.EncodeToString(data)
    str = fmt.Sprintf("put %d %d %d %d\r\n%s\r\n", priority, delay, ttr, len(body), body)
  } else {
    str = fmt.Sprintf("put %d %d %d %d\r\n\r\n", priority, delay, ttr, 0)
  }
  resp, e := c.sendAndRecv(str)
  if e != nil {
    return "", e
  }
  if !strings.HasPrefix(resp, "INSERTED") {
    return "", parseError(resp)
  }
  var id string
  _, e = fmt.Sscanf(resp, "INSERTED %s\r\n", &id)
  return id, e
}

func (c *Conn) Use(tube string) error {
  if tube == "" || len(tube) > tubeLen {
    return base.ErrInvalidArgument
  }
  return c.sendAndRecvExpect(fmt.Sprintf("use %s\r\n", tube), fmt.Sprintf("USING %s\r\n", tube))
}

func (c *Conn) Reserve() (string, []byte, error) {
  return c.reqJob("reserve\r\n", "RESERVED")
}

func (c *Conn) ReserveWithTimeout(timeout int) (string, []byte, error) {
  if timeout < 0 {
    timeout = 0
  }
  return c.reqJob(fmt.Sprintf("reserve-with-timeout %d\r\n", timeout), "RESERVED")
}

func (c *Conn) Delete(id string) error {
  if id == "" {
    return base.ErrInvalidArgument
  }
  return c.sendAndRecvExpect(fmt.Sprintf("delete %s\r\n", id), "DELETED\r\n")
}

func (c *Conn) Release(id string, priority, delay int) error {
  if id == "" {
    return base.ErrInvalidArgument
  }
  if priority < 0 {
    priority = 0
  }
  if delay < 0 {
    delay = 0
  }
  return c.sendAndRecvExpect(fmt.Sprintf("release %s %d %d\r\n", id, priority, delay), "RELEASED\r\n")
}

func (c *Conn) Bury(id string, priority int) error {
  if id == "" {
    return base.ErrInvalidArgument
  }
  if priority < 0 {
    priority = 0
  }
  return c.sendAndRecvExpect(fmt.Sprintf("bury %s %d\r\n", id, priority), "BURIED\r\n")
}

func (c *Conn) Touch(id string) error {
  if id == "" {
    return base.ErrInvalidArgument
  }
  return c.sendAndRecvExpect(fmt.Sprintf("touch %s\r\n", id), "TOUCHED\r\n")
}

func (c *Conn) watch(data string) (int, error) {
  resp, e := c.sendAndRecv(data)
  if e != nil {
    return 0, e
  }
  if !strings.HasPrefix(resp, "WATCHING") {
    return 0, parseError(resp)
  }
  var count int
  _, e = fmt.Sscanf(resp, "WATCHING %d\r\n", &count)
  if e != nil {
    return 0, e
  }
  return count, nil
}

func (c *Conn) Watch(tube string) (int, error) {
  if tube == "" || len(tube) > tubeLen {
    return 0, base.ErrInvalidArgument
  }
  return c.watch(fmt.Sprintf("watch %s\r\n", tube))
}

func (c *Conn) Ignore(tube string) (int, error) {
  if tube == "" || len(tube) > tubeLen {
    return 0, base.ErrInvalidArgument
  }
  return c.watch(fmt.Sprintf("ignore %s\r\n", tube))
}

func (c *Conn) Peek(id string) (string, []byte, error) {
  if id == "" {
    return "", nil, base.ErrInvalidArgument
  }
  return c.reqJob(fmt.Sprintf("peek %s\r\n", id), "FOUND")
}

func (c *Conn) PeekReady() (string, []byte, error) {
  return c.reqJob("peek-ready\r\n", "FOUND")
}

func (c *Conn) PeekDelayed() (string, []byte, error) {
  return c.reqJob("peek-delayed\r\n", "FOUND")
}

func (c *Conn) PeekBuried() (string, []byte, error) {
  return c.reqJob("peek-buried\r\n", "FOUND")
}

func (c *Conn) Kick(bound int) (int, error) {
  if bound <= 0 {
    return 0, base.ErrInvalidArgument
  }
  resp, e := c.sendAndRecv(fmt.Sprintf("kick %d\r\n", bound))
  if e != nil {
    return 0, e
  }
  if !strings.HasPrefix(resp, "KICKED") {
    return 0, parseError(resp)
  }
  var id int
  _, e = fmt.Sscanf(resp, "KICKED %d\r\n", &id)
  if e != nil {
    return 0, e
  }
  return id, nil
}

func (c *Conn) KickJob(id string) error {
  if id == "" {
    return base.ErrInvalidArgument
  }
  return c.sendAndRecvExpect(fmt.Sprintf("kick-job %s\r\n", id), "KICKED\r\n")
}

func (c *Conn) StatsJob(id string) ([]byte, error) {
  if id == "" {
    return nil, base.ErrInvalidArgument
  }
  return c.reqYaml(fmt.Sprintf("stats-job %s\r\n", id))
}

func (c *Conn) StatsTube(tube string) ([]byte, error) {
  if tube == "" || len(tube) > tubeLen {
    return nil, base.ErrInvalidArgument
  }
  return c.reqYaml(fmt.Sprintf("stats-tube %s\r\n", tube))
}

func (c *Conn) Stats() ([]byte, error) {
  return c.reqYaml("stats\r\n")
}

func (c *Conn) ListTubes() ([]byte, error) {
  return c.reqYaml("list-tubes\r\n")
}

func (c *Conn) ListTubeUsed() (string, error) {
  resp, e := c.sendAndRecv("list-tube-used\r\n")
  if e != nil {
    return "", e
  }
  if !strings.HasPrefix(resp, "USING") {
    return "", parseError(resp)
  }
  var tube string
  _, e = fmt.Sscanf(resp, "USING %s\r\n", &tube)
  if e != nil {
    return "", e
  }
  return tube, nil
}

func (c *Conn) ListTubesWatched() ([]byte, error) {
  return c.reqYaml("list-tubes-watched\r\n")
}

func (c *Conn) Quit() error {
  c.send([]byte("quit\r\n"))
  return c.conn.Close()
}

func (c *Conn) PauseTube(tube string, delay int) error {
  if tube == "" || len(tube) > tubeLen {
    return base.ErrInvalidArgument
  }
  if delay < 0 {
    delay = 0
  }
  return c.sendAndRecvExpect(fmt.Sprintf("pause-tube %s %d\r\n", tube, delay), "PAUSED\r\n")
}

func (c *Conn) reqJob(data, prefix string) (string, []byte, error) {
  resp, e := c.sendAndRecv(data)
  if e != nil {
    return "", nil, e
  }
  if !strings.HasPrefix(resp, prefix) {
    return "", nil, parseError(resp)
  }
  var id string
  var l int
  _, e = fmt.Sscanf(resp, prefix+" %s %d\r\n", &id, &l)
  if e != nil {
    return "", nil, e
  }
  body, e := c.readBody(l)
  if e != nil {
    return "", nil, e
  }
  job := make([]byte, base64.RawStdEncoding.DecodedLen(len(body)))
  _, e = base64.RawStdEncoding.Decode(job, body)
  if e != nil {
    return "", nil, e
  }
  return id, job, nil
}

func (c *Conn) reqYaml(data string) ([]byte, error) {
  resp, e := c.sendAndRecv(data)
  if e != nil {
    return nil, e
  }
  if !strings.HasPrefix(resp, "OK") {
    return nil, parseError(resp)
  }
  var l int
  _, e = fmt.Sscanf(resp, "OK %d\r\n", &l)
  if e != nil {
    return nil, e
  }
  body, e := c.readBody(l)
  return body, e
}

func (c *Conn) readBody(l int) ([]byte, error) {
  body := make([]byte, l+2)
  n, e := io.ReadFull(c.bufReader, body)
  if e != nil {
    return nil, e
  }
  return body[:n-2], nil
}

func (c *Conn) sendAndRecvExpect(data, expected string) error {
  resp, e := c.sendAndRecv(data)
  if e != nil {
    return e
  }
  if resp != expected {
    return parseError(resp)
  }
  return nil
}

func (c *Conn) sendAndRecv(data string) (string, error) {
  _, e := c.send([]byte(data))
  if e != nil {
    return "", e
  }
  resp, e := c.bufReader.ReadString('\n')
  if e != nil {
    return "", e
  }
  return resp, nil
}

func (c *Conn) send(data []byte) (int, error) {
  total := 0
  l := len(data)
  if l <= mtu {
    for total < l {
      n, e := c.conn.Write(data)
      total += n
      if e != nil && !isNetErrorTemporary(e) {
        return total, e
      }
      data = data[n:]
    }
  } else {
    for total < l {
      n, e := c.bufWriter.Write(data)
      total += n
      if e != nil && !isNetErrorTemporary(e) {
        return total, e
      }
      e = c.bufWriter.Flush()
      if e != nil && !isNetErrorTemporary(e) {
        return total, e
      }
      data = data[n:]
    }
  }
  return total, nil
}
