use clap::Parser;
use clap::ValueEnum;
use once_cell::sync::Lazy;
use starknet::core::types::requests::GetTransactionReceiptRequest;
use starknet::core::types::{
    BlockId, BlockTag, EventFilter, Felt, TransactionReceipt,
};
use starknet::macros::felt;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider, ProviderRequestData, ProviderResponseData};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

pub const LOG_TARGET: &str = "sn_tx_steps";
const CARTRIDGE_NODE_MAINNET: &str = "https://api.cartridge.gg/x/starknet/mainnet";
const RYO_KATANA_RPC: &str = "https://api.cartridge.gg/x/dopewars/katana";

/// The threshold for a transaction to be considered high step count.
const TX_STEPS_THRESHOLD: u64 = 2_000_000;

/// The batch size for fetching transaction receipts.
const TX_HASH_BATCH_SIZE: usize = 1000;

/// The chunk size for fetching events, 1024 being the maximum value for pathfinder.
const EVENT_CHUNK_SIZE: u64 = 1024;


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
    #[arg(short, long)]
    contract: Contract,
}

#[derive(Error, Debug)]
pub enum FetchError {
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
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

#[tokio::main]
async fn main() -> Result<(), FetchError> {
    let args = Args::parse();

    let (contract_address, start_block, rpc) = args.contract.get_details();

    let mut output_file = setup_file(&format!("/tmp/{:?}.log", args.contract));

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
        "Starting event fetch for contract: {:?} (address: {:#x}, start_block: {})",
        args.contract, contract_address, start_block
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
        debug!(
            "Fetched {} events (highest block: {})",
            events_page.events.len(),
            current_block
        );

        // Adds the tx hashes to the queue to be batched later.
        for event in &events_page.events {
            total_events += 1;

            if tx_hashes.insert(event.transaction_hash) {
                tx_hash_queue.push_back(event.transaction_hash);
            }
        }

        if tx_hash_queue.len() >= TX_HASH_BATCH_SIZE {
            let batch: Vec<Felt> = tx_hash_queue.drain(..TX_HASH_BATCH_SIZE).collect();
            batch_txs_receipts(&provider, &batch, &mut output_file).await?;
        }

        continuation_token = events_page.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    // We need to make sure we drain remainig tx if we don't reach the tx batch size:
    if !tx_hash_queue.is_empty() {
        let batch: Vec<Felt> = tx_hash_queue.drain(..).collect();
        batch_txs_receipts(&provider, &batch, &mut output_file).await?;
    }

    info!("Total events fetched: {total_events}");
    info!("Total tx hashes fetched: {}", tx_hashes.len());

    Ok(())
}

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

async fn batch_txs_receipts<P: Provider + Sync + Send + 'static>(
    provider: &Arc<P>,
    tx_hashes: &[Felt],
    output_file: &mut File,
) -> Result<(), FetchError> {
    let receipts = fetch_receipts_batch(&provider, &tx_hashes).await?;
    let mut high_step_count = 0;

    for r in &receipts {
        match r {
            TransactionReceipt::Invoke(r) => {
                let steps = r.execution_resources.computation_resources.steps;

                if steps > TX_STEPS_THRESHOLD {
                    high_step_count += 1;
                    println!("{:#066x} ({} steps)", r.transaction_hash, steps);
                    writeln!(output_file, "{:#066x} {} *", r.transaction_hash, steps).unwrap();
                } else {
                    writeln!(output_file, "{:#066x} {}", r.transaction_hash, steps).unwrap();
                }
            }
            _ => {}
        }
    }

    info!(
        "Fetched {} receipts (batch), {} with > {} steps",
        receipts.len(),
        high_step_count,
        TX_STEPS_THRESHOLD,
    );

    Ok(())
}