wit_bindgen::generate!({
    path: "../../../wit",
    world: "reducer",
});

struct TestReducer;

impl Guest for TestReducer {
    fn reduce(
        state: Vec<u8>,
        intent: Vec<u8>,
        _context: Context,
    ) -> Result<Reduction, String> {
        let current = state
            .get(..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        let sequence = current.saturating_add(1);
        let intent = String::from_utf8(intent).map_err(|error| error.to_string())?;
        let record = serde_json::to_vec(&serde_json::json!({
            "type": "wasm_transition",
            "sequence": sequence,
            "intent": intent,
        }))
        .map_err(|error| error.to_string())?;
        let response = serde_json::to_vec(&serde_json::json!({
            "sequence": sequence,
        }))
        .map_err(|error| error.to_string())?;
        Ok(Reduction {
            state: sequence.to_le_bytes().to_vec(),
            records: vec![record],
            response,
        })
    }
}

export!(TestReducer);
