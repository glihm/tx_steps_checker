use account_sdk::abigen::controller::OutsideExecutionV3;
use cainome_cairo_serde::CairoSerde;
use clap::Parser;
use clap::ValueEnum;
use once_cell::sync::Lazy;
use starknet::core::types::ContractClass;
use starknet::core::types::InvokeTransaction;
use starknet::core::types::Transaction;
use starknet::core::types::contract::AbiEntry;
use starknet::core::types::contract::AbiFunction;
use starknet::core::types::requests::GetTransactionReceiptRequest;
use starknet::core::types::{BlockId, BlockTag, EventFilter, Felt, TransactionReceipt};
use starknet::core::utils::get_selector_from_name;
use starknet::macros::felt;
use starknet::macros::selector;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider, ProviderRequestData, ProviderResponseData};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use thiserror::Error;
use tracing::trace;
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

const CARTRIDGE_NODE_MAINNET: &str = "https://api.cartridge.gg/x/starknet/mainnet";
const RYO_KATANA_RPC: &str = "https://api.cartridge.gg/x/dopewars/katana";

/// The threshold for a transaction to be considered high step count.
const TX_STEPS_THRESHOLD: u64 = 2_000_000;

/// The batch size for fetching transaction receipts.
const TX_HASH_BATCH_SIZE: usize = 1000;

/// The chunk size for fetching events, 1024 being the maximum value for pathfinder.
const EVENT_CHUNK_SIZE: u64 = 1024;

/// Cache for storing contract function selectors mapped to function names.
/// We don't need to keep the contract context since selectors are unique across the whole network.
type FunctionCache = Arc<Mutex<HashMap<Felt, String>>>;

/// Initialize the function cache
fn init_function_cache() -> FunctionCache {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Get function name from cache, or return None if not found
fn get_function_name(cache: &FunctionCache, selector: Felt) -> Option<String> {
    cache.lock().unwrap().get(&selector).cloned()
}

/// Store function name in cache
fn store_function_name(cache: &FunctionCache, selector: Felt, function_name: String) {
    let mut cache_guard = cache.lock().unwrap();
    cache_guard.insert(selector, function_name);
}

/// The transaction info gathered for high step count transactions.
struct HighStepTxInfo {
    pub entrypoints_names: Vec<String>,
}

pub static CONTRACTS: Lazy<HashMap<&str, (Felt, u64, String)>> = Lazy::new(|| {
    HashMap::from([
        (
            "Eternum_s0",
            (
                felt!("0x6a9e4c6f0799160ea8ddc43ff982a5f83d7f633e9732ce42701de1288ff705f"),
                948010,
                String::from(CARTRIDGE_NODE_MAINNET),
            ),
        ),
        (
            "Eternum_s1",
            (
                felt!("0x5c6d0020a9927edca9ddc984b97305439c0b32a1ec8d3f0eaf6291074cc9799"),
                1435856,
                String::from(CARTRIDGE_NODE_MAINNET),
            ),
        ),
        (
            "Zkube",
            (
                felt!("0x5c6d0020a9927edca9ddc984b97305439c0b32a1ec8d3f0eaf6291074cc9799"),
                1386668,
                String::from(CARTRIDGE_NODE_MAINNET),
            ),
        ),
        (
            "Ryo",
            (
                felt!("0x4f3dccb47477c087ad9c76b8067b8aadded57f8df7f2d7543e6066bcb25332c"),
                889000,
                String::from(CARTRIDGE_NODE_MAINNET),
            ),
        ),
        (
            "RyoKatana",
            (
                felt!("0x05d133456420b786bca33391f7b472b4f7af4fdd9d507ef50d85cc5cc19f8e56"),
                0,
                String::from(RYO_KATANA_RPC),
            ),
        ),
    ])
});

#[derive(Debug, Clone, ValueEnum)]
enum Contract {
    EternumS0,
    EternumS1,
    Zkube,
    Ryo,
    RyoKatana,
}

impl Contract {
    fn get_details(&self) -> (Felt, u64, String) {
        match self {
            Contract::EternumS0 => CONTRACTS["Eternum_s0"].clone(),
            Contract::EternumS1 => CONTRACTS["Eternum_s1"].clone(),
            Contract::Zkube => CONTRACTS["Zkube"].clone(),
            Contract::Ryo => CONTRACTS["Ryo"].clone(),
            Contract::RyoKatana => CONTRACTS["RyoKatana"].clone(),
        }
    }
}

#[derive(Parser)]
#[command(name = "sn_events_batch")]
#[command(about = "Fetch events and transaction receipts for the contract.")]
struct Args {
    #[command(flatten)]
    contract_config: ContractConfig,
}

#[derive(clap::Args)]
struct ContractConfig {
    /// Use a predefined contract.
    #[arg(short, long)]
    #[arg(conflicts_with("contract_address"))]
    contract: Option<Contract>,

    /// Custom contract address.
    #[arg(long)]
    #[arg(conflicts_with("contract"))]
    contract_address: Option<Felt>,

    /// Custom start block number.
    #[arg(long)]
    #[arg(default_value = "0")]
    start_block: u64,

    /// Custom RPC URL.
    #[arg(long)]
    #[arg(default_value = CARTRIDGE_NODE_MAINNET)]
    rpc_url: String,
}

#[derive(Error, Debug)]
pub enum FetchError {
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
}

#[tokio::main]
async fn main() -> Result<(), FetchError> {
    let args = Args::parse();

    let (contract_address, start_block, rpc, contract_name) = parse_contract_config(&args);

    let mut output_file = setup_file(&format!("/tmp/{}.log", contract_name));
    let mut function_cache = init_function_cache();

    let filter_layer = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(filter_layer)
        .init();

    let transport = HttpTransport::new(Url::parse(&rpc).unwrap());
    let provider: Arc<_> = JsonRpcClient::new(transport).into();

    let to_block = BlockId::Tag(BlockTag::Latest);

    let mut continuation_token = None;
    let mut tx_hashes = HashSet::new();
    let mut tx_hash_queue = VecDeque::new();
    let mut total_events = 0;

    info!(
        "Starting event fetch for contract: {} (address: {:#x}, start_block: {})",
        contract_name, contract_address, start_block
    );

    loop {
        let event_filter = EventFilter {
            from_block: Some(BlockId::Number(start_block)),
            to_block: Some(to_block),
            address: Some(contract_address),
            keys: None,
        };

        let events_page = provider
            .get_events(event_filter, continuation_token, EVENT_CHUNK_SIZE)
            .await?;
        let current_block = if events_page.events.is_empty() {
            start_block
        } else {
            // Pending is not being indexed -> so the block number is always set.
            events_page.events[events_page.events.len() - 1]
                .block_number
                .unwrap()
        };

        if !events_page.events.is_empty() {
            debug!(
                "Fetched {} events (highest block: {})",
                events_page.events.len(),
                current_block
            );
        } else {
            debug!("Empty page");
        }

        // Adds the tx hashes to the queue to be batched later.
        for event in &events_page.events {
            total_events += 1;

            if tx_hashes.insert(event.transaction_hash) {
                tx_hash_queue.push_back(event.transaction_hash);
            }
        }

        if tx_hash_queue.len() >= TX_HASH_BATCH_SIZE {
            let batch: Vec<Felt> = tx_hash_queue.drain(..TX_HASH_BATCH_SIZE).collect();
            fetch_and_process_txs_receipts(
                &provider,
                &batch,
                &mut output_file,
                &mut function_cache,
            )
            .await?;
        }

        continuation_token = events_page.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    // We need to make sure we drain remainig tx if we don't reach the tx batch size:
    if !tx_hash_queue.is_empty() {
        let batch: Vec<Felt> = tx_hash_queue.drain(..).collect();
        fetch_and_process_txs_receipts(&provider, &batch, &mut output_file, &mut function_cache)
            .await?;
    }

    info!("Total events fetched: {total_events}");
    info!("Total tx hashes fetched: {}", tx_hashes.len());

    Ok(())
}

/// Process the batch of txs receipts and write to the output file.
async fn fetch_and_process_txs_receipts<P: Provider + Sync + Send + 'static>(
    provider: &Arc<P>,
    tx_hashes: &[Felt],
    output_file: &mut File,
    function_cache: &mut FunctionCache,
) -> Result<(), FetchError> {
    let receipts = fetch_receipts_batch(provider, tx_hashes).await?;
    let mut high_step_count = 0;

    for r in &receipts {
        if let TransactionReceipt::Invoke(r) = r {
            let steps = r.execution_resources.computation_resources.steps;

            if steps > TX_STEPS_THRESHOLD {
                high_step_count += 1;
                debug!("{:#066x} ({} steps)", r.transaction_hash, steps);

                if let Some(tx_info) =
                    fetch_and_parse_transaction(r.transaction_hash, function_cache, provider)
                        .await?
                {
                    writeln!(
                        output_file,
                        "{:#066x} {} * {}",
                        r.transaction_hash,
                        steps,
                        tx_info.entrypoints_names.join(", ")
                    )
                    .unwrap();
                } else {
                    writeln!(output_file, "{:#066x} {} *", r.transaction_hash, steps).unwrap();
                }
            } else {
                writeln!(output_file, "{:#066x} {}", r.transaction_hash, steps).unwrap();
            }
        }
    }

    debug!(
        "Fetched {} receipts (batch), {} with > {} steps",
        receipts.len(),
        high_step_count,
        TX_STEPS_THRESHOLD,
    );

    Ok(())
}

/// Fetches transaction receipts in batch.
async fn fetch_receipts_batch<P: Provider + Sync + Send + 'static>(
    provider: &Arc<P>,
    tx_hashes: &[Felt],
) -> Result<Vec<TransactionReceipt>, FetchError> {
    let requests: Vec<ProviderRequestData> = tx_hashes
        .iter()
        .map(|tx_hash| {
            ProviderRequestData::GetTransactionReceipt(GetTransactionReceiptRequest {
                transaction_hash: *tx_hash,
            })
        })
        .collect();

    let results = provider.batch_requests(&requests).await?;
    let receipts: Vec<TransactionReceipt> = results
        .into_iter()
        .map(|result| match result {
            ProviderResponseData::GetTransactionReceipt(r) => r.receipt,
            _ => unreachable!(),
        })
        .collect();

    Ok(receipts)
}

/// Parse and display transaction calldata with function names
async fn fetch_and_parse_transaction<P: Provider + Sync + Send + 'static>(
    tx_hash: Felt,
    function_cache: &mut FunctionCache,
    provider: &Arc<P>,
) -> Result<Option<HighStepTxInfo>, FetchError> {
    trace!(tx_hash = ?tx_hash, "Fetching full transaction.");

    let tx = match provider.get_transaction_by_hash(tx_hash).await? {
        Transaction::Invoke(InvokeTransaction::V3(tx)) => Some(tx),
        _ => None,
    };

    if tx.is_none() {
        debug!(tx_hash = ?tx_hash, "Not an invoke v3 transaction.");
        return Ok(None);
    }

    let tx = tx.unwrap();

    if tx.calldata.is_empty() {
        debug!(tx_hash = ?tx_hash, "Calldata is empty.");
        return Ok(None);
    }

    // We're looking for `execution_from_outside_v3` since most of txs are controller txs.
    // The first call is being parsed, and the selector is the third element in this array (since execution from outside itself is not multicalled).
    if tx.calldata[2] != selector!("execute_from_outside_v3") {
        debug!(tx_hash = ?tx_hash, "Not an execution from outside v3 transaction.");
        return Ok(None);
    }

    let mut entrypoints_names: Vec<String> = vec![];
    let efo = OutsideExecutionV3::cairo_deserialize(&tx.calldata, 4).unwrap();

    for call in &efo.calls {
        let function_name = if let Some(n) = get_function_name(function_cache, call.selector) {
            n
        } else {
            fetch_and_decode_entrypoints(call.to.into(), provider, function_cache).await?;
            get_function_name(function_cache, call.selector).unwrap()
        };

        entrypoints_names.push(function_name);
    }

    Ok(Some(HighStepTxInfo { entrypoints_names }))
}

/// Ensures the file is deleted and recreated blank since we only append after.
fn setup_file(file_name: &str) -> File {
    if let Err(e) = std::fs::remove_file(file_name) {
        error!("Error deleting file: {}", e);
    }

    OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)
        .expect("Failed to open file")
}

/// Parse command line arguments and return contract configuration
fn parse_contract_config(args: &Args) -> (Felt, u64, String, String) {
    if let Some(contract) = &args.contract_config.contract {
        let (addr, block, rpc_url) = contract.get_details();
        (addr, block, rpc_url, format!("{:?}", contract))
    } else if let Some(address) = args.contract_config.contract_address {
        (
            address,
            args.contract_config.start_block,
            args.contract_config.rpc_url.clone(),
            "Custom".to_string(),
        )
    } else {
        error!("No contract specified");
        std::process::exit(1);
    }
}

/// Fetch the ABI of a contract and store the function selectors mapped to their names in the cache.
async fn fetch_and_decode_entrypoints<P: Provider + Sync + Send + 'static>(
    contract_address: Felt,
    provider: &Arc<P>,
    function_cache: &mut FunctionCache,
) -> Result<(), FetchError> {
    /// An AbiEntry that is a function can be embedded in an interface (which is then implemented).
    /// This function is handy to re-use the same code for both functions and implementations.
    fn decode_function(function: &AbiFunction, function_cache: &mut FunctionCache) {
        let name = function.name.clone();
        let selector = get_selector_from_name(&function.name).expect("Invalid function name");
        debug!(function_name = name, selector = ?selector, "Registering mapping for function");
        store_function_name(function_cache, selector, name);
    }
    trace!("Fetching ABI for contract: {:#x}", contract_address);

    let class = provider
        .get_class_at(BlockId::Tag(BlockTag::Latest), contract_address)
        .await?;

    if let ContractClass::Sierra(class) = class {
        // Parse the string into a vector of ABI entries.
        let abi = serde_json::from_str::<Vec<AbiEntry>>(&class.abi).unwrap();
        for entry in abi {
            if let AbiEntry::Function(function) = entry {
                decode_function(&function, function_cache);
            } else if let AbiEntry::Interface(intf) = entry {
                for item in &intf.items {
                    if let AbiEntry::Function(function) = item {
                        decode_function(function, function_cache);
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::vec;

    use cainome_cairo_serde::ContractAddress;

    use super::*;

    #[test]
    fn test_parse_transaction() {
        let calldata = vec![
            felt!("0x1"),
            felt!("0x599fba85695cd0256d941058dc671865a579caf1bb2f3db48a8104049c40f76"),
            felt!("0x3dbc508ba4afd040c8dc4ff8a61113a7bcaf5eae88a6ba27b3c50578b3587e3"),
            felt!("0x80"),
            felt!("0x414e595f43414c4c4552"),
            felt!("0x712319389626fc77df104be2dff5130fdda066744f23be18451e4bbd0be1a0b"),
            felt!("0x10000000"),
            felt!("0x0"),
            felt!("0x673b1e09"),
            felt!("0x1"),
            felt!("0x412445e644070c69fea16b964cc81cd6debf6a4dbf683e2e9686a45ad088de8"),
            felt!("0x3566d0e92bdb2d2b96b481893c1a23f1526a9009dbc1d59cc137b805bf1ef09"),
            felt!("0x3"),
            felt!("0x53b"),
            felt!("0x0"),
            felt!("0x0"),
            felt!("0x73"),
            felt!("0x73657373696f6e2d746f6b656e"),
            felt!("0x673b221e"),
            felt!("0x6bfdbfca6eb646cfacdcc3132b026ddbf4f87ffc21c4d4d8cda4730a4317896"),
            felt!("0x0"),
            felt!("0x1ccbac3cd14161657b3a4f6d0deeb2ffdd0c7a13c70150d980ee81bd200573a"),
            felt!("0x0"),
            felt!("0x1"),
            felt!("0x5d"),
            felt!("0x1"),
            felt!("0x4"),
            felt!("0x16"),
            felt!("0x68"),
            felt!("0x74"),
            felt!("0x74"),
            felt!("0x70"),
            felt!("0x73"),
            felt!("0x3a"),
            felt!("0x2f"),
            felt!("0x2f"),
            felt!("0x78"),
            felt!("0x2e"),
            felt!("0x63"),
            felt!("0x61"),
            felt!("0x72"),
            felt!("0x74"),
            felt!("0x72"),
            felt!("0x69"),
            felt!("0x64"),
            felt!("0x67"),
            felt!("0x65"),
            felt!("0x2e"),
            felt!("0x67"),
            felt!("0x67"),
            felt!("0x9d0aec9905466c9adf79584fa75fed3"),
            felt!("0x20a97ec3f8efbc2aca0cf7cabb420b4a"),
            felt!("0x652ab69a7689cb2beec6a231a6967b6"),
            felt!("0x519fef2adbadba35febf8242b5e6c95e"),
            felt!("0x38"),
            felt!("0x2c"),
            felt!("0x22"),
            felt!("0x63"),
            felt!("0x72"),
            felt!("0x6f"),
            felt!("0x73"),
            felt!("0x73"),
            felt!("0x4f"),
            felt!("0x72"),
            felt!("0x69"),
            felt!("0x67"),
            felt!("0x69"),
            felt!("0x6e"),
            felt!("0x22"),
            felt!("0x3a"),
            felt!("0x74"),
            felt!("0x72"),
            felt!("0x75"),
            felt!("0x65"),
            felt!("0x2c"),
            felt!("0x22"),
            felt!("0x74"),
            felt!("0x6f"),
            felt!("0x70"),
            felt!("0x4f"),
            felt!("0x72"),
            felt!("0x69"),
            felt!("0x67"),
            felt!("0x69"),
            felt!("0x6e"),
            felt!("0x22"),
            felt!("0x3a"),
            felt!("0x22"),
            felt!("0x68"),
            felt!("0x74"),
            felt!("0x74"),
            felt!("0x70"),
            felt!("0x73"),
            felt!("0x3a"),
            felt!("0x2f"),
            felt!("0x2f"),
            felt!("0x64"),
            felt!("0x6f"),
            felt!("0x70"),
            felt!("0x65"),
            felt!("0x77"),
            felt!("0x61"),
            felt!("0x72"),
            felt!("0x73"),
            felt!("0x2e"),
            felt!("0x67"),
            felt!("0x61"),
            felt!("0x6d"),
            felt!("0x65"),
            felt!("0x22"),
            felt!("0x7d"),
            felt!("0x1d"),
            felt!("0x0"),
            felt!("0x490ca32eb2f25fed754f38b80a8caf3b"),
            felt!("0x45344d3726eeb7dd1a587c9fceb1f52c"),
            felt!("0x9d2fbf5ba26dfcb01ddfbb4777b9267a"),
            felt!("0x4e523bb067e9278bbf6a3ab674141199"),
            felt!("0x1"),
            felt!("0x0"),
            felt!("0x387ceb5dba2c8421dd8d9079bda028b2114ec2dcdf6d08804384ed957fa4811"),
            felt!("0x9f29837fded7fd9a880618130ff34e40719ba5b5e08904ebc230b540031677"),
            felt!("0x3334974a55f177bb9360a3fdd9842d12b04da19205f659ed0fbc3d09e8c1907"),
            felt!("0x0"),
            felt!("0x1e6a6f52e47fe42e024287b729bc47e58019fcc7e1cc8b141bb8d669b779b49"),
            felt!("0x7584ecaf1a8ea43fc320afd26a3aa30e06ce9e66c7072df16b1c8ffeeaf25f8"),
            felt!("0x3fbda068d411a3a073ace1ac743313fc824e934c7225a496ebf2f236f40878d"),
            felt!("0x1"),
            felt!("0x4"),
            felt!("0x6a27fe137c608490340c28b0ab69a0abe8d4790b073025223cf8ff2b4a7ceb4"),
            felt!("0x4db42e8fa0ddd7f00d3d27a6e870d22c12e22b0e2f2ed5397e39d0a3489d34f"),
            felt!("0x42f12923b5c50589fe41fde45543cc7482389d0b3280de1217170d19fddb80a"),
            felt!("0x3523e5773ef65babf5af31cadd832d3268613d2aabedfc9528f294cb43c17b8"),
        ];

        let efo = OutsideExecutionV3::cairo_deserialize(&calldata, 4).unwrap();
        assert_eq!(
            efo.caller,
            ContractAddress::from(felt!("0x414e595f43414c4c4552"))
        );
        assert_eq!(
            efo.nonce,
            (
                felt!("0x712319389626fc77df104be2dff5130fdda066744f23be18451e4bbd0be1a0b"),
                0x10000000_u128
            )
        );
        assert_eq!(efo.execute_after, 0_u64);
        assert_eq!(efo.execute_before, 0x673b1e09_u64);
        assert_eq!(efo.calls.len(), 1);

        assert_eq!(
            efo.calls[0].to,
            ContractAddress::from(felt!(
                "0x412445e644070c69fea16b964cc81cd6debf6a4dbf683e2e9686a45ad088de8"
            ))
        );
        assert_eq!(efo.calls[0].selector, selector!("register_score"));
    }
}
