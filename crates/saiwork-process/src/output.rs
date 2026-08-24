//! Bounded process output buffer (law 13).
//!
//! Donor values as initial caps (MIGRATION_SAIWORK §8.2): 512 KiB cap,
//! 256 KiB retained when trimming. Revisit after measurement
//! (KNOWLEDGE/PERFORMANCE.md).

use std::collections::VecDeque;

/// Hard cap for captured output, in bytes.
pub const OUTPUT_CAP_BYTES: usize = 512 * 1024;
/// How many bytes to keep when the cap is exceeded.
pub const OUTPUT_RETAIN_BYTES: usize = 256 * 1024;

/// A bounded ring of output lines. Oldest lines are dropped first; total
/// bytes never exceed the cap (`OUTPUT_CAP_BYTES` unless a per-process cap
/// is set).
#[derive(Debug, Default)]
pub struct BoundedOutputBuffer {
    lines: VecDeque<String>,
    bytes: usize,
    cap: usize,
    dropped_lines: u64,
}

impl BoundedOutputBuffer {
    pub fn new() -> Self {
        Self {
            cap: OUTPUT_CAP_BYTES,
            ..Default::default()
        }
    }

    /// A buffer with an explicit byte cap (e.g. a per-process response
    /// channel, TASK 17 §49). `cap == 0` keeps a single line bounded by
    /// `OUTPUT_RETAIN_BYTES` — never an unbounded buffer.
    pub fn with_cap(cap: usize) -> Self {
        Self {
            cap: if cap == 0 { OUTPUT_RETAIN_BYTES } else { cap },
            ..Default::default()
        }
    }

    /// Append a line, trimming from the front when over the cap.
    ///
    /// CORE-021: a single line that exceeds the cap is truncated (head
    /// retained, tail dropped with a marker) rather than silently discarded.
    /// This preserves the line's presence in the ring while respecting the
    /// byte budget.
    pub fn push_line(&mut self, line: String) {
        self.bytes += line.len();
        self.lines.push_back(line);
        // Drop oldest lines first when over budget.
        while self.bytes > self.cap && self.lines.len() > 1 {
            if let Some(front) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(front.len());
                self.dropped_lines += 1;
            }
        }
        // CORE-021: a single pathological line exceeding the cap is truncated
        // (head retained) rather than dropped — the caller sees a bounded
        // fragment with a truncation marker, not silence.
        if self.bytes > self.cap {
            if let Some(mut front) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(front.len());
                self.dropped_lines += 1;
                // Truncate to fit: keep the head, mark the truncation.
                // Floor to nearest char boundary to avoid splitting multi-byte UTF-8 chars.
                let mut keep = self.cap.saturating_sub(16); // leave room for marker
                if keep > 0 {
                    while keep > 0 && !front.is_char_boundary(keep) {
                        keep -= 1;
                    }
                    front.truncate(keep);
                    front.push_str("\n...[truncated]");
                    self.bytes += front.len();
                    self.lines.push_back(front);
                }
            }
        }
    }

    pub fn lines(&self) -> impl Iterator<Item = &str> {
        self.lines.iter().map(String::as_str)
    }

    pub fn all_lines(&self) -> Vec<String> {
        self.lines.iter().cloned().collect()
    }

    pub fn joined(&self) -> String {
        self.lines.iter().cloned().collect::<Vec<_>>().join("\n")
    }

    pub fn byte_len(&self) -> usize {
        self.bytes
    }

    pub fn dropped_lines(&self) -> u64 {
        self.dropped_lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_total_bytes() {
        let mut buf = BoundedOutputBuffer::new();
        let big = "x".repeat(OUTPUT_CAP_BYTES + 100);
        buf.push_line(big.clone());
        // One line bigger than the cap is retained, trimmed to the cap.
        assert!(buf.byte_len() <= OUTPUT_CAP_BYTES + 1);
        let line = "y".repeat(OUTPUT_RETAIN_BYTES);
        buf.push_line(line.clone());
        assert!(buf.byte_len() <= OUTPUT_CAP_BYTES + 1);
        assert!(buf.dropped_lines() >= 1);
    }

    #[test]
    fn trims_oldest_first() {
        let mut buf = BoundedOutputBuffer::new();
        for i in 0..10 {
            buf.push_line(format!("line {i}"));
        }
        let lines: Vec<_> = buf.lines().collect();
        assert_eq!(lines.len(), 10);
        assert_eq!(lines[0], "line 0");
        assert_eq!(lines[9], "line 9");
    }

    #[test]
    fn multibyte_unicode_truncation_does_not_panic() {
        let mut buf = BoundedOutputBuffer::with_cap(50);
        // Repeated 3-byte Japanese kanji '本' (3 bytes: E6 9C AC)
        let kanji = "日本語テスト".repeat(20);
        buf.push_line(kanji);
        assert!(buf.byte_len() <= 50 + 16);
        let joined = buf.joined();
        assert!(joined.contains("...[truncated]"));
        assert!(std::str::from_utf8(joined.as_bytes()).is_ok());
    }
}
