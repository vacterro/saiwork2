//! OpenCode session-layer DTOs (TASK 11 §4 — verified against 1.18.18).
//!
//! These are the wire shapes of the OpenCode server API only. Nothing from
//! this module crosses the generic EventBus: the adapter maps these into
//! canonical events and the generic `EngineAdapter` surface (law 3).
//!
//! Fields that the adapter does not yet consume are still parsed on purpose:
//! they document the verified wire shape and keep future capabilities
//! parseable without a protocol bump. Hence the module-level dead-code
//! allowance — this is a contract mirror, not behavior.
//!
//! The JSON keys use OpenCode's camelCase (`providerID`, `modelID`,
//! `sessionID`…); the Rust fields mirror them verbatim so a drift between
//! this file and the wire shape is impossible to misread.
#![allow(dead_code, non_snake_case)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// `GET /provider` — `{ all, default, connected }`.
///
/// `connected` is `Option<Vec<String>>` on purpose: its PRESENCE (not
/// emptiness) is the authority signal. A primary `/provider` response that
/// reports `connected: []` is an AUTHORITATIVE "nothing is connected" answer
/// and must filter the catalog down to auth-backed providers only. A
/// `/config/providers` fallback (which has no `connected` field at all)
/// deserializes to `None`, meaning "connected authority is unknown" — the
/// degraded full catalog must be kept. Inferring "missing" from vector
/// emptiness would erase exactly that distinction (CORE-006).
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderList {
    pub all: Vec<Provider>,
    #[serde(default)]
    pub connected: Option<Vec<String>>,
    #[serde(default)]
    pub default: HashMap<String, String>,
}

/// `GET /config/providers` — `{ providers, default }` (strict compatibility
/// fallback for OpenCode builds whose `/provider` route is absent).
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigProviders {
    pub providers: Vec<Provider>,
    #[serde(default)]
    pub default: HashMap<String, String>,
}

impl ProviderList {
    /// ONE normalization path for the provider catalog: accepts both
    /// documented wire shapes (`/provider`'s `all` and `/config/providers`'s
    /// `providers`) and rejects everything else with a typed protocol error.
    /// Never returns a fake empty success.
    ///
    /// PERF-04: the body is deserialized ONCE into the typed `CatalogWire`
    /// untagged enum and normalized into `ProviderList` by MOVING its vectors
    /// and maps — no full generic `serde_json::Value` DOM, no
    /// `serde_json::from_value(value.clone())` deep clone. This matters on the
    /// hot path: the real 1.18.18 catalog is ~5 MiB / 6615 models and is read
    /// at engine startup and every model refresh.
    pub fn from_wire(body: &[u8]) -> Result<Self, String> {
        let wire: CatalogWire = serde_json::from_slice(body)
            .map_err(|e| format!("provider catalog is not valid JSON: {e}"))?;
        Ok(match wire {
            CatalogWire::Primary(p) => p,
            CatalogWire::Fallback(c) => ProviderList {
                all: c.providers,
                connected: None,
                default: c.default,
            },
        })
    }
}

/// The two accepted provider-catalog wire shapes, parsed ONCE into typed data
/// (PERF-04). `Primary` matches `GET /provider` (`{all, default, connected}`);
/// `Fallback` matches `GET /config/providers` (`{providers, default}`).
///
/// Untagged: serde tries `Primary` first, so a body that carries `all` is
/// authoritative even if it also contains a stray `providers` key. A body with
/// neither `all` nor `providers` fails both variants → a typed protocol error
/// (never a fake empty list). `connected: Option<Vec<String>>` keeps the
/// `None` (authority unknown) vs `Some([])` (authoritative "nothing connected")
/// distinction; on the fallback shape it is always `None`.
#[derive(Deserialize)]
#[serde(untagged)]
enum CatalogWire {
    Primary(ProviderList),
    Fallback(ConfigProviders),
}

/// Providers from OpenCode's local credential file (`auth.json`, e.g.
/// `~/.local/share/opencode/auth.json`). Merged into the catalog by the
/// adapter so a provider the server's catalog does not expose can still
/// offer its declared models.
///
/// SECURITY: this file holds API keys. The DTO reads ONLY the provider id
/// and the `models` value — key/access/refresh/token fields are never
/// deserialized, never logged, never crossed (they stay on disk). The
/// merge policy additionally drops any provider that ends up with no
/// models, so a credential-only entry (e.g. a broken custom provider) can
/// never appear as an empty shell in the model list.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuthProviders {
    /// One entry per provider id; models are empty when the entry declares
    /// none or the file is malformed (auth.json is a hint, not a contract —
    /// the server catalog remains the authority).
    pub providers: Vec<Provider>,
}

impl AuthProviders {
    /// Parse the flat auth.json shape (`{ "<id>": { "type": ..., "key": ...,
    /// "models": [...] | { ... } } }`) or the legacy wrapper
    /// (`{ "providers": { ... } }`). A `models` value may be an array of
    /// model ids or a map `id → options`. Anything unreadable → empty
    /// (never an error: a broken credential file must not break discovery).
    pub fn parse(bytes: &[u8]) -> Self {
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
            return Self::default();
        };
        let map = match value.as_object() {
            Some(m) if m.contains_key("providers") => {
                match m.get("providers").and_then(|p| p.as_object()) {
                    Some(p) => p,
                    None => return Self::default(),
                }
            }
            Some(m) => m,
            None => return Self::default(),
        };
        let mut providers = Vec::new();
        for (id, entry) in map {
            if entry.as_object().is_none() {
                continue;
            }
            let model_keys: Vec<String> = match entry.get("models").and_then(|m| m.as_object()) {
                Some(obj) => {
                    // Meta keys (dynamic-fetch config) are NOT model ids.
                    let meta = ["url", "baseURL", "headers", "options"];
                    obj.keys()
                        .filter(|k| !meta.contains(&k.as_str()))
                        .cloned()
                        .collect()
                }
                None => match entry.get("models").and_then(|m| m.as_array()) {
                    Some(arr) => arr
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect(),
                    None => Vec::new(),
                },
            };
            let mut map = HashMap::new();
            for key in model_keys {
                map.insert(
                    key.clone(),
                    Model {
                        id: key.clone(),
                        providerID: id.clone(),
                        name: String::new(),
                        family: None,
                        capabilities: None,
                    },
                );
            }
            providers.push(Provider {
                id: id.clone(),
                name: id.clone(),
                models: map,
                connected: Some(true),
            });
        }
        Self { providers }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Provider {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub models: HashMap<String, Model>,
    pub connected: Option<bool>,
}

/// A model as reported by OpenCode inside `Provider.models`.
///
/// Canonical identity rule (TASK 24 §9): the **map KEY of `Provider.models`
/// plus the enclosing provider's `id`** is the ONLY canonical model identity
/// (`<provider>/<model>` style) — the exact pair the message API needs.
/// The inner `id`/`providerID` fields are optional/redundant legacy values on
/// real provider shapes and are NEVER trusted as authority (the fixture
/// deliberately sets them to different values so a substitution fails the
/// discriminating wire assertion). Capabilities are present when OpenCode
/// reports them; unknown = absent.
#[derive(Debug, Clone, Deserialize)]
pub struct Model {
    /// Legacy/redundant inner id. NOT canonical — see the struct doc.
    #[serde(default)]
    pub id: String,
    /// Legacy/redundant inner provider id. NOT canonical — see the struct doc.
    #[serde(default)]
    pub providerID: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub family: Option<String>,
    #[serde(default)]
    pub capabilities: Option<ModelCapabilities>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ModelCapabilities {
    #[serde(default)]
    pub temperature: Option<bool>,
    #[serde(default)]
    pub reasoning: Option<bool>,
    #[serde(default)]
    pub attachment: Option<bool>,
    #[serde(default)]
    pub toolcall: Option<bool>,
    #[serde(default)]
    pub input: Option<CapabilityChannels>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CapabilityChannels {
    #[serde(default)]
    pub text: Option<bool>,
    #[serde(default)]
    pub image: Option<bool>,
}

/// The `model` object accepted by `POST /session/{id}/message` — OpenCode
/// requires `providerID` + `modelID` (verified: missing either → 400
/// `Missing key at ["model"]["modelID"]`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ModelRef {
    pub providerID: String,
    pub modelID: String,
}

/// `GET|POST /session` item. Only the fields the adapter consumes.
#[derive(Debug, Clone, Deserialize)]
pub struct Session {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub directory: String,
    #[serde(default)]
    pub revert: Option<SessionRevert>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionRevert {
    pub messageID: String,
    #[serde(default)]
    pub partID: Option<String>,
}

/// The final assistant message returned by `POST /session/{id}/message`
/// (verified: blocks until the run ends, then returns the complete message).
#[derive(Debug, Clone, Deserialize)]
pub struct Message {
    pub info: MessageInfo,
    #[serde(default)]
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessageInfo {
    pub id: String,
    #[serde(default)]
    pub sessionID: String,
    #[serde(default)]
    pub role: String,
    /// Present (`"stop"` etc.) when the run completed normally; absent after
    /// an abort or a provider failure (verified 1.18.18).
    #[serde(default)]
    pub finish: Option<String>,
    #[serde(default)]
    pub modelID: Option<String>,
    #[serde(default)]
    pub providerID: Option<String>,
    #[serde(default)]
    pub tokens: Option<Tokens>,
    #[serde(default)]
    pub cost: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Tokens {
    #[serde(default)]
    pub input: Option<u64>,
    #[serde(default)]
    pub output: Option<u64>,
    #[serde(default)]
    pub reasoning: Option<u64>,
    #[serde(default)]
    pub total: Option<u64>,
}

/// A message part (text, step-start/step-finish, reasoning, tool…).
#[derive(Debug, Clone, Deserialize)]
pub struct Part {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub tool: Option<String>,
    #[serde(default)]
    pub callID: Option<String>,
    #[serde(default)]
    pub state: Option<ToolState>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Tool part state — `running` → `completed` | `error` | `cancelled`.
#[derive(Debug, Clone, Deserialize)]
pub struct ToolState {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// One envelope on the global `GET /event` SSE stream.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerEvent {
    pub id: String,
    pub r#type: String,
    #[serde(default)]
    pub properties: serde_json::Value,
}

impl ServerEvent {
    /// The session this event belongs to (most events carry `sessionID`).
    pub fn session_id(&self) -> Option<&str> {
        self.properties.get("sessionID").and_then(|v| v.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Flat auth.json with an array-of-strings `models` value.
    #[test]
    fn auth_parses_flat_shape_with_array_models() {
        let json = br#"{
            "extra-corp": {
                "type": "api",
                "key": "sk-secret-abc",
                "models": ["m-alpha", "m-beta"]
            }
        }"#;
        let auth = AuthProviders::parse(json);
        assert_eq!(auth.providers.len(), 1);
        let p = &auth.providers[0];
        assert_eq!(p.id, "extra-corp");
        let mut ids: Vec<&String> = p.models.keys().collect();
        ids.sort();
        assert_eq!(ids, vec!["m-alpha", "m-beta"]);
        assert_eq!(p.models["m-alpha"].providerID, "extra-corp");
        assert_eq!(p.models["m-alpha"].id, "m-alpha");
    }

    /// Object-valued `models` (`id → options`) is normalized to ids; meta
    /// keys (dynamic-fetch config) are NOT treated as models.
    #[test]
    fn auth_parses_object_models_and_skips_meta_keys() {
        let json = br#"{
            "obj-corp": {
                "type": "api",
                "key": "sk-2",
                "models": {
                    "m-x": {"name": "X"},
                    "url": "https://fetch.example/models",
                    "baseURL": "https://fetch.example"
                }
            }
        }"#;
        let auth = AuthProviders::parse(json);
        let p = &auth.providers[0];
        assert_eq!(p.models.len(), 1);
        assert!(p.models.contains_key("m-x"));
        assert!(!p.models.contains_key("url"));
        assert!(!p.models.contains_key("baseURL"));
    }

    /// Legacy `{ "providers": { ... } }` wrapper is accepted.
    #[test]
    fn auth_parses_legacy_wrapper() {
        let json = br#"{
            "providers": {
                "legacy-corp": {
                    "type": "api",
                    "key": "sk-3",
                    "models": ["m-old"]
                }
            }
        }"#;
        let auth = AuthProviders::parse(json);
        assert_eq!(auth.providers.len(), 1);
        assert_eq!(auth.providers[0].id, "legacy-corp");
        assert!(auth.providers[0].models.contains_key("m-old"));
    }

    /// Credential-only entries (no `models`) surface as EMPTY providers —
    /// the merge policy drops them. Never an error.
    #[test]
    fn auth_credential_only_entry_has_no_models() {
        // The sambanova-free case from the real machine: type=api, key only.
        let json = br#"{"broken-corp": {"type": "api", "key": "sk-4"}}"#;
        let auth = AuthProviders::parse(json);
        assert_eq!(auth.providers.len(), 1);
        assert!(auth.providers[0].models.is_empty());
    }

    /// Malformed / non-object / garbage input → empty result, never error.
    #[test]
    fn auth_malformed_input_is_empty() {
        assert!(AuthProviders::parse(b"not json").providers.is_empty());
        assert!(AuthProviders::parse(b"42").providers.is_empty());
        assert!(AuthProviders::parse(b"[]").providers.is_empty());
        assert!(AuthProviders::parse(br#"{"providers": 7}"#)
            .providers
            .is_empty());
        assert!(AuthProviders::parse(b"").providers.is_empty());
    }

    /// The parse result must never contain credential material: the DTO
    /// reads only id + models; a Debug dump of a parsed secret-bearing file
    /// must not echo the key.
    #[test]
    fn auth_never_surfaces_secrets() {
        let json = br#"{
            "sec-corp": {
                "type": "api",
                "key": "sk-super-secret-42",
                "access": "acc-99",
                "refresh": "ref-1",
                "models": ["m-sec"]
            }
        }"#;
        let auth = AuthProviders::parse(json);
        let dump = format!("{auth:?}");
        assert!(!dump.contains("sk-super-secret-42"));
        assert!(!dump.contains("acc-99"));
        assert!(!dump.contains("ref-1"));
    }

    // ---- PERF-04: typed provider-catalog parse, no deep clone ------------

    /// Primary `/provider` shape (`{all, default, connected}`) normalizes
    /// directly into `ProviderList` with vectors/maps moved, not cloned.
    #[test]
    fn catalog_primary_shape_normalizes_moved() {
        let json = br#"{
            "all": [
                {"id": "p1", "models": {"m1": {"id": "m1", "providerID": "p1"}}},
                {"id": "p2", "models": {}}
            ],
            "default": {"gpt": "openai"},
            "connected": ["p1"]
        }"#;
        let list = ProviderList::from_wire(json).unwrap();
        assert_eq!(list.all.len(), 2);
        assert_eq!(list.default.get("gpt"), Some(&"openai".to_string()));
        // `connected` PRESENT with a non-empty vector -> Some.
        assert_eq!(list.connected, Some(vec!["p1".to_string()]));
        assert_eq!(list.all[0].models.get("m1").unwrap().id, "m1");
    }

    /// `connected: []` is an AUTHORITATIVE "nothing connected" -> Some([]),
    /// distinct from a missing `connected` (None = authority unknown).
    #[test]
    fn catalog_connected_empty_vs_missing_distinction() {
        let empty = br#"{"all":[],"connected":[]}"#;
        let list = ProviderList::from_wire(empty).unwrap();
        assert_eq!(list.connected, Some(vec![]));

        let missing = br#"{"all":[]}"#;
        let list = ProviderList::from_wire(missing).unwrap();
        assert_eq!(list.connected, None);
    }

    /// Fallback `/config/providers` shape (`{providers, default}`) normalizes
    /// to `connected: None` (it carries no connection authority).
    #[test]
    fn catalog_fallback_shape_normalizes_without_connected() {
        let json = br#"{"providers":[{"id":"p9","models":{"m":{}}}],"default":{}}"#;
        let list = ProviderList::from_wire(json).unwrap();
        assert_eq!(list.all.len(), 1);
        assert_eq!(list.all[0].id, "p9");
        assert_eq!(list.connected, None);
    }

    /// Malformed root (neither `all` nor `providers`) is rejected with a typed
    /// protocol error, never a fake empty list (CORE-006 guardrail).
    #[test]
    fn catalog_malformed_root_is_rejected() {
        assert!(ProviderList::from_wire(br#"{"foo":1}"#).is_err());
        assert!(ProviderList::from_wire(br#"[]"#).is_err());
        assert!(ProviderList::from_wire(br#""nope""#).is_err());
        // Valid JSON but `all` is the wrong type -> still a typed error.
        assert!(ProviderList::from_wire(br#"{"all":"not-a-list"}"#).is_err());
    }

    /// Canonical model identity is `<provider.id>/<models key>`, so duplicate
    /// raw model keys across providers stay distinct (the discriminating
    /// wire assertion from the fixture contract).
    #[test]
    fn catalog_duplicate_model_keys_across_providers_stay_distinct() {
        let json = br#"{
            "all": [
                {"id": "provA", "models": {"shared": {"id": "legacyA", "providerID": "WRONG"}}},
                {"id": "provB", "models": {"shared": {"id": "legacyB", "providerID": "WRONG"}}}
            ]
        }"#;
        let list = ProviderList::from_wire(json).unwrap();
        // Both providers expose their own "shared" model under their own id.
        assert_eq!(list.all.len(), 2);
        let a = &list.all[0];
        let b = &list.all[1];
        assert_eq!(a.id, "provA");
        assert_eq!(b.id, "provB");
        assert!(a.models.contains_key("shared"));
        assert!(b.models.contains_key("shared"));
        // Canonical identity is the map key, not the redundant inner id.
        assert_eq!(a.models["shared"].providerID, "WRONG");
        assert_eq!(b.models["shared"].providerID, "WRONG");
    }

    /// Large-catalog fixture: the new path performs ONE typed parse of a
    /// multi-megabyte catalog near the configured bound (PERF-04). No full
    /// `serde_json::Value` DOM and no `from_value(value.clone())` deep clone
    /// exist on this path (verified by code review + the sized parse below).
    #[test]
    fn catalog_large_parse_is_single_typed_pass() {
        let providers = 200usize;
        let models_each = 300usize; // 200 * 300 = 60_000 models (~5 MiB)
        let mut json = String::from("{\"all\":[");
        for p in 0..providers {
            if p > 0 {
                json.push(',');
            }
            json.push_str(&format!(
                "{{\"id\":\"p{p:03}\",\"name\":\"Provider {p:03}\",\"models\":{{"
            ));
            for m in 0..models_each {
                if m > 0 {
                    json.push(',');
                }
                let id = format!("model-{p:03}-{m:04}");
                json.push_str(&format!(
                    "\"{id}\":{{\"id\":\"{id}\",\"providerID\":\"p{p:03}\",\"name\":\"{id}\"}}"
                ));
            }
            json.push_str("}}");
        }
        json.push_str("],\"default\":{}}");
        let bytes = json.into_bytes();
        assert!(bytes.len() > 4 * 1024 * 1024, "fixture should approach the 16 MiB bound (~5 MiB)");

        let start = std::time::Instant::now();
        let list = ProviderList::from_wire(&bytes).expect("large catalog must parse");
        let elapsed = start.elapsed();

        assert_eq!(list.all.len(), providers);
        let total_models: usize = list.all.iter().map(|p| p.models.len()).sum();
        assert_eq!(total_models, providers * models_each);
        // Structural sanity: canonical identity preserved at scale.
        assert!(list.all[0].models.contains_key("model-000-0000"));
        // A single typed pass over ~5 MiB must be cheap (generous bound to
        // avoid CI flakiness; the point is no OOM / no quadratic clone).
        assert!(elapsed.as_secs() < 10, "parse took too long: {elapsed:?}");
    }
}
