# PRODUCT.md

## What SAIWORK2 is

SAIWORK2 is a desktop **orchestration cockpit** for working with coding agents.
It gives the user one place to:

- open a project/workspace;
- work with coding agents and OpenCode sessions;
- choose engine/provider/model;
- stream conversations;
- hold a persistent prompt queue;
- run multiple sessions;
- control processes;
- see SAIPEN state;
- see project progress and agent execution status.

## Who the user is

A developer running AI coding agents (OpenCode today, others later) inside a
SAIPEN-structured workflow, on a desktop OS (Windows first), who wants a
durable, predictable control plane instead of a pile of terminal sessions.

## Primary user workflow

```
open project
→ choose/use engine
→ work in session
→ observe agent/tool activity
→ queue prompts if needed
→ inspect SAIPEN state
→ stop/resume/recover predictably
→ close cleanly (0 orphans)
```

## Core capabilities (v1)

- project/workspace registry with stable identity, recent list, Git and SAIPEN detection;
- engine selection with normalized capability discovery;
- sessions with streaming conversation, tool and permission activity;
- durable prompt queue (phase 2);
- SAIPEN read/watch/validate control plane (phase 3);
- one predictable process lifecycle and clean shutdown;
- bounded diagnostics with secret redaction.

## What SAIWORK2 is NOT

- a new OpenCode;
- an AI inference server;
- a provider aggregator;
- a new SAIPEN implementation;
- a copy of Freebuff Desktop;
- a CodeNomad fork;
- an Electron wrapper around localhost servers;
- a full IDE;
- a cloud collaboration platform;
- a browser / remote desktop / voice assistant / account ecosystem.

## Scope boundaries

In: the core workflow above and nothing else until a gate passes. Out (never
planned core): cloud account platform, voice, remote access, collaboration,
plugin marketplace, browser, full IDE, complex file explorer, multiple
visual themes, workflow editor, mobile client, telemetry cloud, custom
updater ecosystem.

## Scope gates

Every feature must answer: *"Which concrete part of the core workflow does it
improve?"* — if there is no clear answer, it does not enter core (law 10).

## Guiding principles

1. SAIWORK2 is a small managing layer over powerful engines. Complex AI
   capability stays in the systems that already own it.
2. What SAIWORK2 adds is: one workspace cockpit, one queue, one process
   lifecycle, one event model, one SAIPEN control plane, one predictable UX.
3. Win by the absence of unnecessary code, not by volume of code.
4. A boring, reliable skeleton beats a spectacular demo.
5. `堅牢` (kenrō) — a reliable core the user can trust for hours and days.
