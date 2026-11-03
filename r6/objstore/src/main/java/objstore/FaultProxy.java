package objstore;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-JVM HTTP/1.1-aware TCP proxy between the S3 client and the S3 server. It frames every request and
 * response (Content-Length / chunked / 1xx interim), classifies the S3 operation from method + path +
 * query, and asks a {@link Decider} whether to inject a fault. Every injected fault gets an id and is
 * recorded together with the parsed operation, key, upload id, part number, range, and the final
 * server status the proxy observed - attribution is structural, never by exception text.
 */
final class FaultProxy implements Closeable {

  enum Kind {
    /** forward request headers + K body bytes, then RST both sides: server never sees a full request. */
    CUT_REQ,
    /** forward the full request, let the server answer, swallow the answer, RST the client (applied, ack lost). */
    DROP_RESP,
    /** forward the full request, hold the server's answer for stallMs before forwarding it. */
    STALL_RESP,
    /** forward response headers + K body bytes, then RST the client. */
    RESET_RESP,
    /** forward response headers + K body bytes, then close the client socket cleanly (FIN). */
    FIN_RESP;

    static Kind parse(String s) {
      for (Kind k : values()) {
        if (k.name().equalsIgnoreCase(s)) {
          return k;
        }
      }
      throw new IllegalArgumentException("Unknown fault kind: " + s);
    }
  }

  static final class Fault {
    final Kind kind;
    final long k;
    final long stallMs;
    int id;

    Fault(Kind kind, long k, long stallMs) {
      this.kind = kind;
      this.k = k;
      this.stallMs = stallMs;
    }
  }

  /** Parsed request. */
  static final class Req {
    int seq;
    int conn;
    String method;
    String path;
    String query;
    String op;
    String key;
    String uploadId;
    int partNumber;
    String range;
    long bodyLen; // -1 = chunked
    Fault fault;
    volatile int status = -1;
    volatile long faultBytes = -1; // body bytes forwarded before the fault cut
    volatile String effect = "";
    volatile String respChecksum = "";

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "#%d c%d %s key=%s%s%s%s len=%d status=%d", seq, conn, op, key,
          uploadId == null ? "" : " upload=" + uploadId.substring(0, Math.min(8, uploadId.length())),
          partNumber > 0 ? " part=" + partNumber : "", range == null ? "" : " range=" + range, bodyLen, status);
    }
  }

  interface Decider {
    /** Return a fault for this request, or null. Called once per request, before anything is forwarded. */
    Fault decide(Req req);
  }

  private final ServerSocket server;
  private final int backendPort;
  private volatile Decider decider = r -> null;
  private final AtomicInteger seq = new AtomicInteger();
  private final AtomicInteger connSeq = new AtomicInteger();
  private final AtomicInteger faultSeq = new AtomicInteger();
  private final List<Req> log = new ArrayList<>();
  private final List<Socket> open = new ArrayList<>();
  private volatile boolean closed = false;

  FaultProxy(int backendPort) throws IOException {
    this.backendPort = backendPort;
    this.server = new ServerSocket(0, 200, InetAddress.getLoopbackAddress());
    Thread t = new Thread(this::acceptLoop, "proxy-accept");
    t.setDaemon(true);
    t.start();
  }

  int port() {
    return server.getLocalPort();
  }

  void setDecider(Decider d) {
    this.decider = d;
  }

  synchronized List<Req> log() {
    return new ArrayList<>(log);
  }

  synchronized void clearLog() {
    log.clear();
  }

  List<Req> faults() {
    List<Req> out = new ArrayList<>();
    for (Req r : log()) {
      if (r.fault != null) {
        out.add(r);
      }
    }
    return out;
  }

  private void acceptLoop() {
    while (!closed) {
      try {
        Socket c = server.accept();
        Socket b = new Socket(InetAddress.getLoopbackAddress(), backendPort);
        c.setTcpNoDelay(true);
        b.setTcpNoDelay(true);
        synchronized (this) {
          open.add(c);
          open.add(b);
        }
        Conn conn = new Conn(connSeq.incrementAndGet(), c, b);
        Thread t1 = new Thread(conn::requestPump, "proxy-req-" + conn.id);
        Thread t2 = new Thread(conn::responsePump, "proxy-resp-" + conn.id);
        t1.setDaemon(true);
        t2.setDaemon(true);
        t1.start();
        t2.start();
      } catch (IOException e) {
        if (!closed) {
          System.err.println("proxy accept failed: " + e);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    server.close();
    synchronized (this) {
      for (Socket s : open) {
        try {
          s.close();
        } catch (IOException ignored) {
          // closing
        }
      }
      open.clear();
    }
  }

  private final class Conn {
    final int id;
    final Socket client;
    final Socket backend;
    final LinkedBlockingQueue<Req> inflight = new LinkedBlockingQueue<>();
    volatile boolean dead = false;

    Conn(int id, Socket client, Socket backend) {
      this.id = id;
      this.client = client;
      this.backend = backend;
    }

    void kill(boolean rstClient) {
      dead = true;
      try {
        if (rstClient) {
          client.setSoLinger(true, 0);
        }
        client.close();
      } catch (IOException ignored) {
        // closing
      }
      try {
        backend.setSoLinger(true, 0);
        backend.close();
      } catch (IOException ignored) {
        // closing
      }
    }

    void finClient() {
      dead = true;
      try {
        client.shutdownOutput();
        client.close();
      } catch (IOException ignored) {
        // closing
      }
      try {
        backend.close();
      } catch (IOException ignored) {
        // closing
      }
    }

    void requestPump() {
      try {
        InputStream in = client.getInputStream();
        OutputStream out = backend.getOutputStream();
        while (!dead) {
          byte[] head = readHead(in);
          if (head == null) {
            backend.shutdownOutput();
            return;
          }
          String headStr = new String(head, StandardCharsets.ISO_8859_1);
          Req r = parseRequest(headStr);
          r.conn = id;
          r.seq = seq.incrementAndGet();
          Fault f;
          synchronized (FaultProxy.this) {
            f = decider.decide(r);
            if (f != null) {
              f.id = faultSeq.incrementAndGet();
              r.fault = f;
            }
            log.add(r);
          }
          inflight.put(r);
          if (f != null && f.kind == Kind.CUT_REQ) {
            if (r.bodyLen == 0) {
              r.faultBytes = 0;
              r.effect = "cut-before-headers";
              kill(true);
              return;
            }
            out.write(head);
            long limit = r.bodyLen > 0 ? Math.min(f.k, r.bodyLen - 1) : f.k;
            long sent = copyBody(in, out, r.bodyLen, limit);
            out.flush();
            r.faultBytes = sent;
            r.effect = "cut-after-" + sent + "B";
            // give the server a moment to have read what was sent, then RST both
            Thread.sleep(20);
            kill(true);
            return;
          }
          out.write(head);
          copyBody(in, out, r.bodyLen, Long.MAX_VALUE);
          out.flush();
        }
      } catch (IOException | InterruptedException e) {
        kill(false);
      }
    }

    void responsePump() {
      try {
        InputStream in = backend.getInputStream();
        OutputStream out = client.getOutputStream();
        Req cur = null;
        while (!dead) {
          byte[] head = readHead(in);
          if (head == null) {
            client.shutdownOutput();
            return;
          }
          String headStr = new String(head, StandardCharsets.ISO_8859_1);
          int status = parseStatus(headStr);
          if (cur == null) {
            cur = inflight.poll(60, TimeUnit.SECONDS);
            if (cur == null) {
              throw new IOException("response without request");
            }
          }
          if (status >= 100 && status < 200) {
            out.write(head);
            out.flush();
            continue; // interim; same request
          }
          Req r = cur;
          cur = null;
          r.status = status;
          Map<String, String> h = headers(headStr);
          StringBuilder cks = new StringBuilder();
          for (String hk : h.keySet()) {
            if (hk.startsWith("x-amz-checksum-") && !hk.equals("x-amz-checksum-type")) {
              cks.append(hk.substring("x-amz-checksum-".length()));
            }
          }
          r.respChecksum = cks.toString();
          long len;
          if ("HEAD".equals(r.method) || status == 204 || status == 304) {
            len = 0;
          } else if (h.containsKey("content-length")) {
            len = Long.parseLong(h.get("content-length").trim());
          } else if ("chunked".equalsIgnoreCase(h.getOrDefault("transfer-encoding", "").trim())) {
            len = -1;
          } else {
            len = -2; // until close
          }
          Fault f = r.fault;
          Kind kind = f == null ? null : f.kind;
          if (kind == Kind.DROP_RESP) {
            copyBody(in, OutputStream.nullOutputStream(), len, Long.MAX_VALUE);
            r.faultBytes = 0;
            r.effect = "applied-status-" + status + "-ack-dropped";
            kill(true);
            return;
          } else if (kind == Kind.STALL_RESP) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            copyBody(in, buf, len, Long.MAX_VALUE);
            r.effect = "applied-status-" + status + "-stalled-" + f.stallMs + "ms";
            Thread.sleep(f.stallMs);
            out.write(head);
            buf.writeTo(out);
            out.flush();
          } else if (kind == Kind.RESET_RESP || kind == Kind.FIN_RESP) {
            out.write(head);
            long limit = len > 0 ? Math.min(f.k, len - 1) : f.k;
            long sent = len == 0 ? 0 : copyBody(in, out, len, limit);
            out.flush();
            r.faultBytes = sent;
            r.effect = (kind == Kind.RESET_RESP ? "reset" : "fin") + "-after-" + sent + "B-of-" + len;
            if (kind == Kind.RESET_RESP) {
              kill(true);
            } else {
              finClient();
            }
            return;
          } else {
            out.write(head);
            copyBody(in, out, len, Long.MAX_VALUE);
            out.flush();
            if (len == -2) {
              client.shutdownOutput();
              return;
            }
          }
        }
      } catch (IOException | InterruptedException e) {
        kill(false);
      }
    }
  }

  /** Copies a body framed as len (>=0 fixed, -1 chunked, -2 until EOF), at most limit raw bytes. */
  static long copyBody(InputStream in, OutputStream out, long len, long limit) throws IOException {
    if (len >= 0) {
      return copyN(in, out, Math.min(len, limit), len <= limit);
    } else if (len == -2) {
      return copyN(in, out, limit, false);
    }
    // chunked: copy raw, counting raw bytes against limit
    long sent = 0;
    while (true) {
      String line = readLine(in);
      if (line == null) {
        throw new IOException("EOF in chunk header");
      }
      byte[] lb = (line + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
      if (sent + lb.length > limit) {
        out.write(lb, 0, (int) (limit - sent));
        return limit;
      }
      out.write(lb);
      sent += lb.length;
      int semi = line.indexOf(';');
      long size = Long.parseLong((semi >= 0 ? line.substring(0, semi) : line).trim(), 16);
      if (size == 0) {
        // trailers until empty line
        while (true) {
          String t = readLine(in);
          if (t == null) {
            throw new IOException("EOF in trailers");
          }
          byte[] tb = (t + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
          out.write(tb);
          sent += tb.length;
          if (t.isEmpty()) {
            return sent;
          }
        }
      }
      long want = size + 2; // data + CRLF
      long n = Math.min(want, limit - sent);
      long got = copyN(in, out, n, n == want);
      sent += got;
      if (got < want) {
        return sent;
      }
    }
  }

  private static long copyN(InputStream in, OutputStream out, long n, boolean mustComplete) throws IOException {
    byte[] buf = new byte[65536];
    long done = 0;
    while (done < n) {
      int r = in.read(buf, 0, (int) Math.min(buf.length, n - done));
      if (r < 0) {
        if (mustComplete && n != Long.MAX_VALUE) {
          throw new IOException("EOF in body");
        }
        return done;
      }
      out.write(buf, 0, r);
      done += r;
    }
    return done;
  }

  private static String readLine(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    int prev = -1;
    while (true) {
      int c = in.read();
      if (c < 0) {
        return sb.length() == 0 ? null : sb.toString();
      }
      if (c == '\n' && prev == '\r') {
        sb.setLength(sb.length() - 1);
        return sb.toString();
      }
      sb.append((char) c);
      prev = c;
    }
  }

  /** Reads up to and including CRLFCRLF; null on clean EOF before any byte. */
  private static byte[] readHead(InputStream in) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream(512);
    int state = 0;
    while (true) {
      int c;
      try {
        c = in.read();
      } catch (SocketException e) {
        if (b.size() == 0) {
          return null;
        }
        throw e;
      }
      if (c < 0) {
        if (b.size() == 0) {
          return null;
        }
        throw new IOException("EOF in header");
      }
      b.write(c);
      if ((state == 0 || state == 2) && c == '\r') {
        state++;
      } else if (state == 1 && c == '\n') {
        state = 2;
      } else if (state == 3 && c == '\n') {
        return b.toByteArray();
      } else {
        state = c == '\r' ? 1 : 0;
      }
      if (b.size() > 1 << 20) {
        throw new IOException("header too large");
      }
    }
  }

  private static int parseStatus(String head) {
    String first = head.substring(0, head.indexOf("\r\n"));
    String[] p = first.split(" ");
    return Integer.parseInt(p[1]);
  }

  static Map<String, String> headers(String head) {
    Map<String, String> h = new LinkedHashMap<>();
    String[] lines = head.split("\r\n");
    for (int i = 1; i < lines.length; i++) {
      int c = lines[i].indexOf(':');
      if (c > 0) {
        h.put(lines[i].substring(0, c).trim().toLowerCase(Locale.ROOT), lines[i].substring(c + 1).trim());
      }
    }
    return h;
  }

  static Req parseRequest(String head) {
    Req r = new Req();
    String first = head.substring(0, head.indexOf("\r\n"));
    String[] p = first.split(" ");
    r.method = p[0];
    String uri = p[1];
    int q = uri.indexOf('?');
    r.path = q >= 0 ? uri.substring(0, q) : uri;
    r.query = q >= 0 ? uri.substring(q + 1) : "";
    Map<String, String> h = headers(head);
    if (h.containsKey("content-length")) {
      r.bodyLen = Long.parseLong(h.get("content-length"));
    } else if ("chunked".equalsIgnoreCase(h.getOrDefault("transfer-encoding", ""))) {
      r.bodyLen = -1;
    } else {
      r.bodyLen = 0;
    }
    r.range = h.get("range");
    Map<String, String> qp = new LinkedHashMap<>();
    if (!r.query.isEmpty()) {
      for (String kv : r.query.split("&")) {
        int e = kv.indexOf('=');
        qp.put(e >= 0 ? kv.substring(0, e) : kv, e >= 0 ? kv.substring(e + 1) : "");
      }
    }
    r.uploadId = qp.get("uploadId");
    if (qp.containsKey("partNumber")) {
      r.partNumber = Integer.parseInt(qp.get("partNumber"));
    }
    String path = r.path.startsWith("/") ? r.path.substring(1) : r.path;
    int slash = path.indexOf('/');
    boolean hasKey = slash >= 0 && slash < path.length() - 1;
    r.key = hasKey ? java.net.URLDecoder.decode(path.substring(slash + 1), StandardCharsets.UTF_8) : "";
    r.op = classify(r.method, hasKey, qp, h);
    return r;
  }

  private static String classify(String m, boolean hasKey, Map<String, String> qp, Map<String, String> h) {
    switch (m) {
      case "PUT":
        if (!hasKey) {
          return "CreateBucket";
        }
        if (qp.containsKey("uploadId") && qp.containsKey("partNumber")) {
          return h.containsKey("x-amz-copy-source") ? "UploadPartCopy" : "UploadPart";
        }
        if (qp.containsKey("tagging")) {
          return "PutObjectTagging";
        }
        return h.containsKey("x-amz-copy-source") ? "CopyObject" : "PutObject";
      case "POST":
        if (!hasKey && qp.containsKey("delete")) {
          return "DeleteObjects";
        }
        if (qp.containsKey("uploads")) {
          return "CreateMultipartUpload";
        }
        if (qp.containsKey("uploadId")) {
          return "CompleteMultipartUpload";
        }
        return "Post?";
      case "DELETE":
        if (qp.containsKey("uploadId")) {
          return "AbortMultipartUpload";
        }
        return hasKey ? "DeleteObject" : "DeleteBucket";
      case "GET":
        if (!hasKey) {
          if (qp.containsKey("uploads")) {
            return "ListMultipartUploads";
          }
          return "ListObjects";
        }
        if (qp.containsKey("uploadId")) {
          return "ListParts";
        }
        if (qp.containsKey("tagging")) {
          return "GetObjectTagging";
        }
        return "GetObject";
      case "HEAD":
        return hasKey ? "HeadObject" : "HeadBucket";
      default:
        return m + "?";
    }
  }
}
