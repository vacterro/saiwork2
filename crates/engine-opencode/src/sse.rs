//! Minimal, dependency-free SSE parser (TASK 11 §26–§34).
//!
//! Incremental byte-stream parser with the required edge-case behavior:
//! chunk boundaries, multiple events in one read, one event split across
//! reads, blank lines, comments/keepalive, `data:` field assembly (including
//! multi-line data), CRLF, and EOF dispatch. It never assumes one TCP chunk
//! == one event. Data is delivered as raw strings; JSON decoding and
//! protocol validation happen in `events.rs`.
//!
//! Line length is bounded so a pathological peer cannot grow memory without
//! limit (§110).

/// A parsed SSE event: the joined `data:` payload plus the last `id:` field
/// (used for duplicate-event policy, §33).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseEvent {
    pub data: String,
    pub id: Option<String>,
}

/// Incremental parser state.
#[derive(Default)]
pub(crate) struct SseParser {
    /// Partial line (no terminating `\n` yet).
    line_buf: Vec<u8>,
    /// `data:` lines of the current event (multi-line data, §108).
    data_lines: Vec<String>,
    /// Last `id:` field (resets per event).
    last_id: Option<String>,
    /// Running byte total of accumulated `data:` lines for the in-flight
    /// event (PERF-006); reset to 0 when the event is dispatched.
    data_bytes: usize,
}

/// Why `push` stopped: normal, or the stream hit a pathological line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushResult {
    Ok,
    /// A single line exceeded the bound; the stream must be treated as
    /// protocol-broken (caller decides recovery).
    LineTooLong,
    /// The cumulative buffered event data (partial line + accumulated `data:`
    /// lines) exceeded `MAX_EVENT_BUFFER` with no terminating blank line; the
    /// stream is runaway/broken and must not be allowed to grow memory (PERF-006).
    BufferOverflow,
}

const MAX_LINE: usize = 8 * 1024 * 1024;

/// Cumulative cap on buffered, unterminated event data (§110 / PERF-006): the
/// sum of the partial line and all accumulated `data:` lines. A hostile or
/// runaway stream that never sends a blank-line terminator can otherwise grow
/// `data_lines` without bound; once this is exceeded `push` returns
/// `BufferOverflow` so the caller can treat the stream as protocol-broken.
const MAX_EVENT_BUFFER: usize = 64 * 1024 * 1024;

impl SseParser {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Feed raw bytes; `on_event` is invoked for each complete event in
    /// arrival order (possibly several per call).
    pub(crate) fn push(&mut self, bytes: &[u8], on_event: &mut dyn FnMut(SseEvent)) -> PushResult {
        // TASK 24 perf: ONE linear scan with a consumed cursor — complete
        // line slices are processed in order without per-line Vec
        // allocation or repeated front-compaction; the consumed prefix is
        // drained exactly once per push, retaining only the final partial
        // line. The buffer is taken into a local so line slices do not
        // borrow `self` while `process_line` mutates the event state.
        let mut buf = std::mem::take(&mut self.line_buf);
        buf.extend_from_slice(bytes);
        let mut consumed = 0usize;
        while let Some(rel) = buf[consumed..].iter().position(|&b| b == b'\n') {
            let line_end = consumed + rel;
            let mut line = &buf[consumed..line_end];
            consumed = line_end + 1; // skip the \n
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1]; // CRLF
            }
            self.process_line(line, on_event);
        }
        if consumed > 0 {
            buf.drain(..consumed);
        }
        self.line_buf = buf;
        if self.line_buf.len() > MAX_LINE {
            return PushResult::LineTooLong;
        }
        // PERF-006: bound the cumulative in-flight event buffer (partial line
        // + all accumulated `data:` lines). A runaway stream without a
        // terminating blank line can otherwise grow `data_lines` forever.
        if self.line_buf.len() + self.data_bytes > MAX_EVENT_BUFFER {
            return PushResult::BufferOverflow;
        }
        PushResult::Ok
    }

    /// Flush any trailing event at EOF (spec: a final event may lack the
    /// terminating blank line; dispatching it is defensive, §108). Also
    /// processes a final unterminated line (no trailing `\n`).
    pub(crate) fn finish(&mut self, on_event: &mut dyn FnMut(SseEvent)) {
        if !self.line_buf.is_empty() {
            let line = std::mem::take(&mut self.line_buf);
            self.process_line(&line, on_event);
        }
        if !self.data_lines.is_empty() {
            let data = self.data_lines.join("\n");
            let id = self.last_id.take();
            self.data_lines.clear();
            self.data_bytes = 0;
            on_event(SseEvent { data, id });
        }
    }

    fn process_line(&mut self, line: &[u8], on_event: &mut dyn FnMut(SseEvent)) {
        if line.is_empty() {
            // Blank line terminates the current event.
        if !self.data_lines.is_empty() {
            let data = self.data_lines.join("\n");
            let id = self.last_id.take();
            self.data_lines.clear();
            self.data_bytes = 0;
            on_event(SseEvent { data, id });
        } else {
            self.last_id = None;
        }
            return;
        }
        if line.first() == Some(&b':') {
            // Comment / keepalive: ignored (§107).
            return;
        }
        let (field, value) = match line.iter().position(|&b| b == b':') {
            Some(pos) => {
                let field = String::from_utf8_lossy(&line[..pos]).into_owned();
                // Per spec, a single leading space after the colon is
                // stripped from the value.
                let mut value = &line[pos + 1..];
                if value.first() == Some(&b' ') {
                    value = &value[1..];
                }
                (field, String::from_utf8_lossy(value).into_owned())
            }
            None => (String::from_utf8_lossy(line).into_owned(), String::new()),
        };
        match field.as_str() {
            "data" => {
                self.data_bytes += value.len();
                self.data_lines.push(value)
            }
            "id" => self.last_id = Some(value),
            "event" | "retry" => {} // unused; data is the only carrier here
            _ => {}                 // unknown fields are ignored per spec
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(chunks: &[&[u8]]) -> Vec<SseEvent> {
        let mut parser = SseParser::new();
        let mut out = Vec::new();
        for chunk in chunks {
            parser.push(chunk, &mut |e| out.push(e));
        }
        parser.finish(&mut |e| out.push(e));
        out
    }

    fn ev(data: &str) -> SseEvent {
        SseEvent {
            data: data.into(),
            id: None,
        }
    }

    #[test]
    fn simple_event() {
        let out = parse_all(&[b"data: hello\n\n"]);
        assert_eq!(out, vec![ev("hello")]);
    }

    #[test]
    fn event_split_across_reads() {
        // §105: `da` / `ta:` split at arbitrary byte boundaries.
        let out = parse_all(&[b"da", b"ta: hel", b"lo\n", b"\n"]);
        assert_eq!(out, vec![ev("hello")]);
    }

    #[test]
    fn multiple_events_one_chunk() {
        // §106: two complete events in one read.
        let out = parse_all(&[b"data: one\n\ndata: two\n\n"]);
        assert_eq!(out, vec![ev("one"), ev("two")]);
    }

    #[test]
    fn keepalive_comments_ignored() {
        // §107: comments between events do not corrupt parsing.
        let out = parse_all(&[b": ping\n\n: keepalive\ndata: x\n\n"]);
        assert_eq!(out, vec![ev("x")]);
    }

    #[test]
    fn crlf_and_multi_line_data() {
        // §108: CRLF line endings + multi-line data joined with \n.
        let out = parse_all(&[b"data: line1\r\ndata: line2\r\n\r\n"]);
        assert_eq!(out, vec![ev("line1\nline2")]);
    }

    #[test]
    fn leading_space_stripped_from_value() {
        let out = parse_all(&[b"data:  spaced\n\n"]);
        assert_eq!(out, vec![ev(" spaced")]);
    }

    #[test]
    fn id_field_is_captured() {
        let out = parse_all(&[b"id: evt_1\ndata: x\n\n"]);
        assert_eq!(
            out,
            vec![SseEvent {
                data: "x".into(),
                id: Some("evt_1".into())
            }]
        );
    }

    #[test]
    fn data_without_blank_line_dispatched_at_eof() {
        let out = parse_all(&[b"data: tail"]);
        assert_eq!(out, vec![ev("tail")]);
    }

    #[test]
    fn empty_data_field_is_not_an_event() {
        let out = parse_all(&[b"data:\n\n"]);
        assert_eq!(out, vec![ev("")]);
    }

    #[test]
    fn field_without_colon_is_empty_value() {
        let out = parse_all(&[b"data\n\n"]);
        assert_eq!(out, vec![ev("")]);
    }

    #[test]
    fn unknown_fields_ignored() {
        let out = parse_all(&[b"event: custom\ndata: x\n\n"]);
        assert_eq!(out, vec![ev("x")]);
    }

    // -----------------------------------------------------------------
    // TASK 12 fragmentation matrix (§7): the parser must not assume network
    // packet boundaries. Every byte of the stream is a potential split
    // point; the result must be identical to the unsplit parse.
    // -----------------------------------------------------------------

    #[test]
    fn one_byte_at_a_time_matches_unsplit() {
        // The most extreme fragmentation: each byte is its own read.
        let wire = b"data: {\"type\":\"message.part.delta\"}\n\n";
        let expected = parse_all(&[wire]);
        let bytes: Vec<&[u8]> = wire.iter().map(std::slice::from_ref).collect();
        assert_eq!(parse_all(&bytes), expected);
    }

    #[test]
    fn split_at_every_byte_boundary_parses_identically() {
        // §7: for a fixed event, try every single split point (1..len) and
        // a representative sample of multi-point splits; the parsed output
        // must be byte-identical to the unsplit stream.
        let wire = b"data: one\n\ndata: two\n\n";
        let expected = parse_all(&[wire]);
        for i in 1..wire.len() {
            let (a, b) = wire.split_at(i);
            assert_eq!(parse_all(&[a, b]), expected, "split at {i}");
        }
        // Multi-point splits across both events.
        let mid1 = wire.len() / 3;
        let mid2 = 2 * wire.len() / 3;
        let (a, rest) = wire.split_at(mid1);
        let (b, c) = rest.split_at(mid2 - mid1);
        assert_eq!(parse_all(&[a, b, c]), expected);
    }

    #[test]
    fn field_name_split_at_every_position() {
        // The `data:` field name itself split at every position (§7).
        let wire = b"data: payload\n\n";
        let expected = parse_all(&[wire]);
        for i in 1..wire.len() {
            let (a, b) = wire.split_at(i);
            assert_eq!(parse_all(&[a, b]), expected, "field split at {i}");
        }
    }

    #[test]
    fn json_token_split_across_chunks() {
        // §7: a JSON token (`message.part.delta`) split mid-token.
        let wire = b"data: {\"type\":\"message.part.delta\"}\n\n";
        let expected = parse_all(&[wire]);
        let at = wire
            .windows(b"message.part.delta".len())
            .position(|w| w == b"message.part.delta")
            .unwrap()
            + 3; // inside the token
        let (a, b) = wire.split_at(at);
        assert_eq!(parse_all(&[a, b]), expected);
    }

    #[test]
    fn utf8_multibyte_split_across_chunks() {
        // §7/§13: a multi-byte UTF-8 character split across chunk boundaries
        // must reassemble losslessly (the parser buffers raw bytes until the
        // line is complete; conversion is lossy only for genuinely invalid
        // bytes). "Δ" is 2 bytes; "€" is 3 bytes.
        let wire = "data: Δ€\n\n".as_bytes();
        let expected = parse_all(&[wire]);
        assert_eq!(expected, vec![ev("Δ€")]);
        for i in 1..wire.len() {
            let (a, b) = wire.split_at(i);
            assert_eq!(parse_all(&[a, b]), expected, "utf8 split at {i}");
        }
    }

    #[test]
    fn crlf_split_across_chunks() {
        // §7: CR and LF in separate chunks must still be handled.
        let wire = b"data: a\r\ndata: b\r\n\r\n";
        let expected = parse_all(&[wire]);
        assert_eq!(expected, vec![ev("a\nb")]);
        // Split exactly between \r and \n.
        let cr_pos = wire.iter().position(|&b| b == b'\r').unwrap();
        let (a, b) = wire.split_at(cr_pos + 1);
        assert_eq!(parse_all(&[a, b]), expected, "CR/LF split");
    }

    #[test]
    fn invalid_utf8_bytes_are_lossy_never_panic() {
        // §13: malformed bytes in the stream must not panic; the parser
        // converts lossily (U+FFFD), and the router treats the event as a
        // malformed-JSON diagnostic.
        let mut wire = b"data: ".to_vec();
        wire.push(0xFF);
        wire.push(0xFE);
        wire.extend_from_slice(b"{\"type\":\"x\"}\n\n");
        let out = parse_all(&[&wire]);
        assert_eq!(out.len(), 1);
        assert!(out[0].data.contains('\u{FFFD}'));
    }

    #[test]
    fn line_overflow_is_detected_not_unbounded() {
        // §110: a pathological single line must be flagged, never buffered
        // without limit. MAX_LINE is 8 MiB; feed a larger unterminated line
        // in pieces and expect LineTooLong.
        let mut parser = SseParser::new();
        let mut out = Vec::new();
        let chunk = vec![b'x'; 1024 * 1024];
        let mut result = PushResult::Ok;
        for _ in 0..9 {
            result = parser.push(&chunk, &mut |e| out.push(e));
            if result == PushResult::LineTooLong {
                break;
            }
        }
        assert_eq!(result, PushResult::LineTooLong);
        assert!(out.is_empty(), "nothing dispatched from the giant line");
    }
}
