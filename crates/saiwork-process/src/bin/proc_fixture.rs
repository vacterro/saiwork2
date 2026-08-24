//! Deterministic child-process fixture for `saiwork-process` tests
//! (TASK 06 §48). Integration tests locate it via
//! `env!("CARGO_BIN_EXE_proc_fixture")` — the tests never depend on random
//! external programs.
//!
//! Flags:
//!   --exit N            exit with code N (default 0)
//!   --sleep SECS        sleep before exiting
//!   --echo-out TEXT     print TEXT to stdout
//!   --echo-err TEXT     print TEXT to stderr
//!   --spam-out N        print N lines ("line i") to stdout (bounded-output test)
//!   --partial           write "abc" (no newline), wait 1 s, then "def\n"
//!   --raw-bytes         write invalid-UTF-8 bytes + newline to stdout
//!   --child-sleep SECS  spawn a sleeping child (prints CHILD_PID=...) and wait on it

use std::io::Write;
use std::process::exit;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut exit_code: i32 = 0;
    let mut sleep_secs: f64 = 0.0;
    let mut echo_out: Vec<String> = Vec::new();
    let mut echo_err: Vec<String> = Vec::new();
    let mut spam_out: usize = 0;
    let mut partial = false;
    let mut raw_bytes = false;
    let mut child_sleep: Option<f64> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--exit" => {
                i += 1;
                exit_code = args[i].parse().expect("--exit needs a number");
            }
            "--sleep" => {
                i += 1;
                sleep_secs = args[i].parse().expect("--sleep needs seconds");
            }
            "--echo-out" => {
                i += 1;
                echo_out.push(args[i].clone());
            }
            "--echo-err" => {
                i += 1;
                echo_err.push(args[i].clone());
            }
            "--spam-out" => {
                i += 1;
                spam_out = args[i].parse().expect("--spam-out needs a count");
            }
            "--partial" => partial = true,
            "--raw-bytes" => raw_bytes = true,
            "--child-sleep" => {
                i += 1;
                child_sleep = Some(args[i].parse().expect("--child-sleep needs seconds"));
            }
            other => {
                eprintln!("proc_fixture: unknown argument {other}");
                exit(2);
            }
        }
        i += 1;
    }

    // Tree test: spawn a sleeping child, announce its PID, and stay alive
    // waiting for it. The child inherits the parent's Job Object, so killing
    // the parent's tree must take the child too.
    if let Some(secs) = child_sleep {
        let exe = std::env::current_exe().expect("current_exe");
        let mut child = std::process::Command::new(exe)
            .arg("--sleep")
            .arg(secs.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn child");
        println!("CHILD_PID={}", child.id());
        let _ = std::io::stdout().flush();
        let _ = child.wait();
        exit(0);
    }

    for line in &echo_out {
        println!("{line}");
    }
    for line in &echo_err {
        eprintln!("{line}");
    }
    if partial {
        // "abc" without a newline, a pause, then "def\n": the line-based
        // reader must deliver all bytes exactly once (documented merge).
        print!("abc");
        let _ = std::io::stdout().flush();
        std::thread::sleep(Duration::from_secs(1));
        println!("def");
    }
    if raw_bytes {
        let _ = std::io::stdout().write_all(&[0xFF, 0xFE, 0x01, b'\n']);
        let _ = std::io::stdout().flush();
    }
    if spam_out > 0 {
        let stdout = std::io::stdout();
        let mut out = std::io::BufWriter::new(stdout.lock());
        for i in 0..spam_out {
            let _ = writeln!(out, "line {i}");
            if i % 1000 == 0 {
                let _ = out.flush();
            }
        }
        let _ = out.flush();
    }
    if sleep_secs > 0.0 {
        std::thread::sleep(Duration::from_secs_f64(sleep_secs));
    }
    exit(exit_code);
}
