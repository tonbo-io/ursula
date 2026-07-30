//! Restricted in-process WebAssembly reducers for Ursula.
//!
//! Module map:
//!
//! - [`ReducerCatalog`]: precompiled, immutable component catalog shared when
//!   constructing per-core runtimes.
//! - [`ReducerRuntime`]: per-core Wasmtime execution state with bounded guest
//!   memory and fuel.
//! - [`ReducerContext`] and [`ReducerOutput`]: host-facing ABI values.

use std::collections::HashMap;

use wasmtime::Config;
use wasmtime::Engine;
use wasmtime::Store;
use wasmtime::StoreLimits;
use wasmtime::StoreLimitsBuilder;
use wasmtime::component::Component;
use wasmtime::component::Linker;

wasmtime::component::bindgen!({
    path: "wit",
    world: "reducer",
});

const DEFAULT_MAX_MEMORY_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_FUEL: u64 = 10_000_000;
const DEFAULT_MAX_RECORDS: usize = 64;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerContext {
    pub bucket: String,
    pub stream: String,
    pub next_offset: u64,
    pub next_record: u64,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerOutput {
    pub state: Vec<u8>,
    pub records: Vec<Vec<u8>>,
    pub response: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReducerLimits {
    pub max_memory_bytes: usize,
    pub max_fuel: u64,
    pub max_records: usize,
    pub max_output_bytes: usize,
}

impl Default for ReducerLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_fuel: DEFAULT_MAX_FUEL,
            max_records: DEFAULT_MAX_RECORDS,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReducerError {
    #[error("invalid reducer component: {0}")]
    InvalidComponent(String),
    #[error("unknown reducer module '{0}'")]
    UnknownModule(String),
    #[error("reducer execution failed: {0}")]
    Execution(String),
    #[error("reducer rejected intent: {0}")]
    Rejected(String),
    #[error("reducer returned {actual} records; limit is {limit}")]
    TooManyRecords { actual: usize, limit: usize },
    #[error("reducer returned no records")]
    NoRecords,
    #[error("reducer returned {actual} bytes; limit is {limit}")]
    OutputTooLarge { actual: usize, limit: usize },
}

#[derive(Clone)]
pub struct ReducerCatalog {
    engine: Engine,
    components: HashMap<String, Component>,
}

impl std::fmt::Debug for ReducerCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReducerCatalog")
            .field("modules", &self.components.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl ReducerCatalog {
    pub fn compile(
        modules: impl IntoIterator<Item = (String, Vec<u8>)>,
    ) -> Result<Self, ReducerError> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)
            .map_err(|error| ReducerError::InvalidComponent(error.to_string()))?;
        let mut components = HashMap::new();
        for (module_id, bytes) in modules {
            let component = Component::from_binary(&engine, &bytes)
                .map_err(|error| ReducerError::InvalidComponent(error.to_string()))?;
            components.insert(module_id, component);
        }
        Ok(Self { engine, components })
    }

    pub fn runtime(&self, limits: ReducerLimits) -> Result<ReducerRuntime, ReducerError> {
        ReducerRuntime::new(self, limits)
    }

    pub fn contains(&self, module_id: &str) -> bool {
        self.components.contains_key(module_id)
    }

    pub fn reduce(
        &self,
        module_id: &str,
        state: &[u8],
        intent: &[u8],
        context: &ReducerContext,
        limits: ReducerLimits,
    ) -> Result<ReducerOutput, ReducerError> {
        self.runtime(limits)?
            .reduce(module_id, state, intent, context)
    }
}

pub struct ReducerRuntime {
    store: Store<StoreState>,
    instances: HashMap<String, Reducer>,
    limits: ReducerLimits,
}

impl std::fmt::Debug for ReducerRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReducerRuntime")
            .field("modules", &self.instances.keys().collect::<Vec<_>>())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

struct StoreState {
    limits: StoreLimits,
}

impl ReducerRuntime {
    fn new(catalog: &ReducerCatalog, limits: ReducerLimits) -> Result<Self, ReducerError> {
        let instance_limit = catalog.components.len().max(1);
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(limits.max_memory_bytes)
            .instances(instance_limit)
            .memories(instance_limit)
            .tables(instance_limit)
            .build();
        let mut store = Store::new(&catalog.engine, StoreState {
            limits: store_limits,
        });
        store.limiter(|state| &mut state.limits);
        let linker = Linker::new(&catalog.engine);
        let mut instances = HashMap::new();
        for (module_id, component) in &catalog.components {
            let instance = Reducer::instantiate(&mut store, component, &linker)
                .map_err(|error| ReducerError::Execution(error.to_string()))?;
            instances.insert(module_id.clone(), instance);
        }
        Ok(Self {
            store,
            instances,
            limits,
        })
    }

    pub fn reduce(
        &mut self,
        module_id: &str,
        state: &[u8],
        intent: &[u8],
        context: &ReducerContext,
    ) -> Result<ReducerOutput, ReducerError> {
        let bindings = self
            .instances
            .get(module_id)
            .ok_or_else(|| ReducerError::UnknownModule(module_id.to_owned()))?;
        let guest_context = self::Context {
            bucket: context.bucket.clone(),
            stream_id: context.stream.clone(),
            next_offset: context.next_offset,
            next_record: context.next_record,
            now_ms: context.now_ms,
        };
        let result = bindings
            .call_reduce(&mut self.store, state, intent, &guest_context)
            .map_err(|error| ReducerError::Execution(error.to_string()))?;
        let result = result.map_err(ReducerError::Rejected)?;
        if result.records.is_empty() {
            return Err(ReducerError::NoRecords);
        }
        if result.records.len() > self.limits.max_records {
            return Err(ReducerError::TooManyRecords {
                actual: result.records.len(),
                limit: self.limits.max_records,
            });
        }
        let output_bytes = result
            .state
            .len()
            .saturating_add(result.response.len())
            .saturating_add(result.records.iter().map(Vec::len).sum::<usize>());
        if output_bytes > self.limits.max_output_bytes {
            return Err(ReducerError::OutputTooLarge {
                actual: output_bytes,
                limit: self.limits.max_output_bytes,
            });
        }
        Ok(ReducerOutput {
            state: result.state,
            records: result.records,
            response: result.response,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_module_is_rejected_before_execution() {
        let catalog = ReducerCatalog::compile(Vec::<(String, Vec<u8>)>::new())
            .expect("empty reducer catalog is valid");
        let error = catalog
            .reduce(
                "missing",
                &[],
                b"intent",
                &ReducerContext {
                    bucket: "bucket".to_owned(),
                    stream: "stream".to_owned(),
                    next_offset: 0,
                    next_record: 0,
                    now_ms: 0,
                },
                ReducerLimits::default(),
            )
            .expect_err("missing reducer must fail");

        assert!(matches!(error, ReducerError::UnknownModule(module) if module == "missing"));
    }

    #[test]
    fn invalid_component_is_rejected_at_catalog_load() {
        let error = ReducerCatalog::compile([("broken".to_owned(), b"not-wasm".to_vec())])
            .expect_err("invalid component must fail startup compilation");

        assert!(matches!(error, ReducerError::InvalidComponent(_)));
    }
}
