use bitflags::bitflags;
use hashlink::LinkedHashMap;
use hashlink::lru_cache::Entry;
use starknet::core::types::requests::{
    GetBlockWithTxHashesRequest, GetEventsRequest, GetTransactionByHashRequest,
};
use starknet::core::types::{
    BlockId, BlockTag, EmittedEvent, Event, EventFilter, EventFilterWithPage, Felt,
    MaybePendingBlockWithTxHashes, ResultPageRequest, Transaction,
};
use starknet::macros::selector;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider, ProviderRequestData, ProviderResponseData};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, trace};
use futures_util::future::try_join_all;
use tokio::time::{sleep, Instant};
use serde::{Serialize, Deserialize};
use url::Url;

pub const LOG_TARGET: &str = "torii::fetcher";

bitflags! {
    #[derive(Debug, Clone)]
    pub struct IndexingFlags: u32 {
        const TRANSACTIONS = 0b00000001;
        const RAW_EVENTS = 0b00000010;
    }
}

type BlockNumber = u64;
type TransactionHash = Felt;
type EventIdx = u64;


#[derive(Error, Debug)]
pub enum FetchError {
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
}

#[derive(Debug, Clone)]
pub struct FetchRangeBlock {
    // For pending blocks, this is None.
    // We check the parent hash of the pending block to the latest block
    // to see if we need to re fetch the pending block.
    pub block_hash: Option<Felt>,
    pub timestamp: u64,
    pub transactions: LinkedHashMap<Felt, FetchRangeTransaction>,
}

#[derive(Debug, Clone)]
pub struct FetchRangeTransaction {
    // this is Some if the transactions indexing flag
    // is enabled
    pub transaction: Option<Transaction>,
    pub events: Vec<EmittedEvent>,
}

#[derive(Debug, Clone)]
pub struct FetchRangeResult {
    // block_number -> block and transactions
    pub blocks: BTreeMap<u64, FetchRangeBlock>,
    // contract_address -> transaction count
    pub num_transactions: HashMap<Felt, u64>,
    // new updated cursors
    pub cursors: HashMap<Felt, Cursor>,
}

#[derive(Default, Debug, Clone)]
pub struct Cursor {
    pub last_pending_block_event_id: Option<String>,
    pub head: Option<u64>,
    pub last_block_timestamp: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContractType {
    WORLD,
    ERC20,
    ERC721,
    ERC1155,
    UDC,
}


pub struct Fetcher<P: Provider + Send + Sync + std::fmt::Debug + 'static> {
    pub batch_chunk_size: usize,
    pub blocks_chunk_size: u64,
    pub events_chunk_size: u64,
    pub max_concurrent_tasks: usize,
    pub start_blocks: HashMap<Felt, u64>,
    pub contracts: HashMap<Felt, ContractType>,
    pub provider: Arc<P>,
    pub flags: IndexingFlags,
}

impl<P: Provider + Send + Sync + std::fmt::Debug + 'static> Fetcher<P> {
    pub async fn fetch_range(
        &self,
        cursors: &HashMap<Felt, Cursor>,
        latest_block_number: u64,
    ) -> Result<FetchRangeResult, FetchError> {
        let mut events = vec![];
        let mut cursors = cursors.clone();
        let mut blocks = BTreeMap::new();
        let mut block_numbers = BTreeSet::new();
        let mut num_transactions = HashMap::new();

        // Step 1: Create initial batch requests for events from all contracts
        let mut event_requests = Vec::new();

        for (contract_address, cursor) in cursors.iter() {
            let contract_start_block = *(self.start_blocks.get(contract_address).unwrap_or(&0));

            let from = cursor
                .head
                .map_or(contract_start_block, |h| if h == 0 { h } else { h + 1 });
            let to = from + self.blocks_chunk_size;

            let events_filter = EventFilter {
                from_block: Some(BlockId::Number(from)),
                to_block: Some(BlockId::Number(to)),
                address: Some(*contract_address),
                keys: None,
            };

            event_requests.push((
                *contract_address,
                to,
                ProviderRequestData::GetEvents(GetEventsRequest {
                    filter: EventFilterWithPage {
                        event_filter: events_filter,
                        result_page_request: ResultPageRequest {
                            continuation_token: None,
                            chunk_size: self.events_chunk_size,
                        },
                    },
                }),
            ));
        }

        // Step 2: Fetch all events recursively
        events.extend(
            self.fetch_events(event_requests, &mut cursors, latest_block_number)
                .await?,
        );

        // Step 3: Collect unique block numbers from events and cursors
        for event in &events {
            let block_number = match event.block_number {
                Some(block_number) => block_number,
                None => latest_block_number + 1, // Pending block
            };
            block_numbers.insert(block_number);
        }
        for (_, cursor) in cursors.iter() {
            if let Some(head) = cursor.head {
                block_numbers.insert(head);
            }
        }

        // Step 4: Fetch block data (timestamps and transaction hashes)
        let mut block_requests = Vec::new();
        for block_number in &block_numbers {
            block_requests.push(ProviderRequestData::GetBlockWithTxHashes(
                GetBlockWithTxHashesRequest {
                    block_id: if *block_number > latest_block_number {
                        BlockId::Tag(BlockTag::Pending)
                    } else {
                        BlockId::Number(*block_number)
                    },
                },
            ));
        }

        // Step 5: Execute block requests in batch and initialize blocks with transaction order
        if !block_requests.is_empty() {
            let block_results = self.chunked_batch_requests(&block_requests).await?;
            for (block_number, result) in block_numbers.iter().zip(block_results) {
                match result {
                    ProviderResponseData::GetBlockWithTxHashes(block) => {
                        let (timestamp, tx_hashes, block_hash) = match block {
                            MaybePendingBlockWithTxHashes::Block(block) => {
                                (block.timestamp, block.transactions, Some(block.block_hash))
                            }
                            MaybePendingBlockWithTxHashes::PendingBlock(block) => {
                                let latest_block: &FetchRangeBlock =
                                    blocks.get(&latest_block_number).unwrap();
                                if block.parent_hash != latest_block.block_hash.unwrap() {
                                    // if the parent hash is not the same as the previous block,
                                    // we need to re fetch the pending block with a specific block number
                                    let block = self
                                        .provider
                                        .get_block_with_tx_hashes(BlockId::Number(*block_number))
                                        .await?;
                                    match block {
                                        // we assume that our pending block has now been validated.
                                        MaybePendingBlockWithTxHashes::Block(block) => (
                                            block.timestamp,
                                            block.transactions,
                                            Some(block.block_hash),
                                        ),
                                        _ => unreachable!(),
                                    }
                                } else {
                                    (block.timestamp, block.transactions, None)
                                }
                            }
                        };
                        // Initialize block with transactions in the order provided by the block
                        let mut transactions = LinkedHashMap::new();
                        for tx_hash in tx_hashes {
                            transactions.insert(
                                tx_hash,
                                FetchRangeTransaction {
                                    transaction: None,
                                    events: vec![],
                                },
                            );
                        }
                        blocks.insert(
                            *block_number,
                            FetchRangeBlock {
                                block_hash,
                                timestamp,
                                transactions,
                            },
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }

        // Step 6: Assign events to their respective blocks and transactions
        for event in events {
            let block_number = match event.block_number {
                Some(block_number) => block_number,
                None => latest_block_number + 1, // Pending block
            };

            let block = blocks.get_mut(&block_number).expect("Block not found");
            match block.transactions.entry(event.transaction_hash) {
                Entry::Occupied(mut tx) => {
                    // Increment transaction count for the contract
                    let entry = num_transactions.entry(event.from_address).or_insert(0);
                    *entry += 1;
                    // Add event to the transaction
                    tx.get_mut().events.push(event);
                }
                Entry::Vacant(tx) => {
                    let address = event.from_address;
                    tx.insert(FetchRangeTransaction {
                        transaction: None,
                        events: vec![event],
                    });

                    // Increment transaction count for the contract
                    let entry = num_transactions.entry(address).or_insert(0);
                    *entry += 1;
                }
            }
        }

        // Step 7: Fetch transaction details if enabled
        if self.flags.contains(IndexingFlags::TRANSACTIONS) && !blocks.is_empty() {
            let mut transaction_requests = Vec::new();
            let mut block_numbers_for_tx = Vec::new();
            for (block_number, block) in &blocks {
                for (transaction_hash, tx) in &block.transactions {
                    if tx.events.is_empty() {
                        continue;
                    }

                    transaction_requests.push(ProviderRequestData::GetTransactionByHash(
                        GetTransactionByHashRequest {
                            transaction_hash: *transaction_hash,
                        },
                    ));
                    block_numbers_for_tx.push(*block_number);
                }
            }

            let transaction_results = self.chunked_batch_requests(&transaction_requests).await?;
            for (block_number, result) in block_numbers_for_tx.into_iter().zip(transaction_results)
            {
                match result {
                    ProviderResponseData::GetTransactionByHash(transaction) => {
                        if let Some(block) = blocks.get_mut(&block_number) {
                            if let Some(tx) =
                                block.transactions.get_mut(transaction.transaction_hash())
                            {
                                tx.transaction = Some(transaction);
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        // Step 8: Update cursor timestamps
        for (_, cursor) in cursors.iter_mut() {
            if let Some(head) = cursor.head {
                if let Some(block) = blocks.get(&head) {
                    cursor.last_block_timestamp = Some(block.timestamp);
                }
            }
        }

        trace!(target: LOG_TARGET, "Blocks: {}", blocks.len());

        Ok(FetchRangeResult {
            blocks,
            num_transactions,
            cursors,
        })
    }

    async fn fetch_events(
        &self,
        initial_requests: Vec<(Felt, u64, ProviderRequestData)>,
        cursors: &mut HashMap<Felt, Cursor>,
        latest_block_number: u64,
    ) -> Result<Vec<EmittedEvent>, FetchError> {
        let mut all_events = Vec::new();
        let mut current_requests = initial_requests;
        let mut old_cursors = cursors.clone();

        while !current_requests.is_empty() {
            let mut next_requests = Vec::new();
            let mut events = Vec::new();

            // Extract just the requests without the contract addresses
            let batch_requests: Vec<ProviderRequestData> = current_requests
                .iter()
                .map(|(_, _, req)| req.clone())
                .collect();

            debug!(target: LOG_TARGET, "Retrieving events for contracts.");
            let instant = Instant::now();
            let batch_results = self.chunked_batch_requests(&batch_requests).await?;
            debug!(target: LOG_TARGET, duration = ?instant.elapsed(), "Retrieved events for contracts.");

            // Process results and prepare next batch of requests if needed
            for ((contract_address, to, original_request), result) in
                current_requests.into_iter().zip(batch_results)
            {
                let contract_type = self.contracts.get(&contract_address).unwrap();
                debug!(target: LOG_TARGET, address = format!("{:#x}", contract_address), r#type = ?contract_type, "Pre-processing events for contract.");

                let old_cursor = old_cursors.get_mut(&contract_address).unwrap();
                let new_cursor = cursors.get_mut(&contract_address).unwrap();
                let mut previous_contract_tx = None;
                let mut event_idx = 0;
                let mut last_pending_block_event_id_tmp =
                    old_cursor.last_pending_block_event_id.clone();
                let mut last_validated_block_number = None;
                let mut last_block_number = None;
                let mut is_pending = false;

                match result {
                    ProviderResponseData::GetEvents(events_page) => {
                        // Process events for this page, only including events up to our target
                        // block
                        for event in events_page.events.clone() {
                            let block_number = match event.block_number {
                                Some(block_number) => {
                                    last_validated_block_number = Some(block_number);
                                    block_number
                                }
                                // If we don't have a block number, this must be a pending block event
                                None => latest_block_number + 1,
                            };

                            last_block_number = Some(block_number);
                            is_pending = event.block_number.is_none();

                            if previous_contract_tx != Some(event.transaction_hash) {
                                event_idx = 0;
                                previous_contract_tx = Some(event.transaction_hash);
                            }
                            let event_id =
                                format_event_id(block_number, &event.transaction_hash, event_idx);
                            event_idx += 1;

                            // Then we skip all transactions until we reach the last pending
                            // processed transaction (if any)
                            if let Some(last_pending_block_event_id) =
                                last_pending_block_event_id_tmp.clone()
                            {
                                let (cursor_block_number, _, _) =
                                    parse_event_id(&last_pending_block_event_id);
                                if event_id != last_pending_block_event_id
                                    && cursor_block_number == block_number
                                {
                                    continue;
                                }
                                last_pending_block_event_id_tmp = None;
                            }

                            // Skip the latest pending block transaction events
                            // * as we might have multiple events for the same transaction
                            if let Some(last_contract_tx) =
                                old_cursor.last_pending_block_event_id.take()
                            {
                                let (cursor_block_number, _, _) = parse_event_id(&last_contract_tx);
                                if event_id == last_contract_tx
                                    && cursor_block_number == block_number
                                {
                                    continue;
                                }
                                new_cursor.last_pending_block_event_id = None;
                            }

                            if is_pending {
                                new_cursor.last_pending_block_event_id = Some(event_id.clone());
                            }

                            events.push(event);
                        }

                        // Add continuation request to next_requests instead of recursing
                        // We only fetch next events if;
                        // - we have a continuation token
                        // - we have a last block number (which means we have processed atleast one event)
                        // - the last block number is less than the to block
                        if events_page.continuation_token.is_some()
                            && (last_block_number.is_none() || last_block_number.unwrap() < to)
                        {
                            debug!(target: LOG_TARGET, address = format!("{:#x}", contract_address), r#type = ?contract_type, "Adding continuation request for contract.");
                            if let ProviderRequestData::GetEvents(mut next_request) =
                                original_request
                            {
                                next_request.filter.result_page_request.continuation_token =
                                    events_page.continuation_token;
                                next_requests.push((
                                    contract_address,
                                    to,
                                    ProviderRequestData::GetEvents(next_request),
                                ));
                            }
                        } else {
                            let new_head = to.max(last_block_number.unwrap_or(0));
                            let new_head = new_head
                                .min(last_validated_block_number.unwrap_or(latest_block_number));
                            // We only reset the last pending block contract tx if we are not
                            // processing pending events anymore. It can happen that during a short lapse,
                            // we can have some pending events while the latest block number has been incremented.
                            // So we want to make sure we don't reset the last pending block contract tx
                            if new_cursor.head != Some(new_head) && !is_pending {
                                new_cursor.last_pending_block_event_id = None;
                            }
                            new_cursor.head = Some(new_head);

                            debug!(target: LOG_TARGET, address = format!("{:#x}", contract_address), r#type = ?contract_type, head = new_cursor.head, last_pending_block_event_id = new_cursor.last_pending_block_event_id, "Fetched and pre-processed events for contract.");
                        }
                    }
                    _ => unreachable!(),
                }
            }

            all_events.extend(events);
            current_requests = next_requests;
        }

        Ok(all_events)
    }

    async fn chunked_batch_requests(
        &self,
        requests: &[ProviderRequestData],
    ) -> Result<Vec<ProviderResponseData>, FetchError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut futures = Vec::new();
        for chunk in requests.chunks(self.batch_chunk_size) {
            futures.push(async move { self.provider.batch_requests(chunk).await });
        }

        let results_of_chunks: Vec<Vec<ProviderResponseData>> = try_join_all(futures)
            .await
            .map_err(|e| {
                error!(
                    target: LOG_TARGET,
                    error = ?e,
                    total_requests = requests.len(),
                    batch_chunk_size = self.batch_chunk_size,
                    "One or more batch requests failed during chunked execution. This could be due to the provider being overloaded. You can try reducing the batch chunk size."
                );
                e
            })?;

        let flattened_results = results_of_chunks.into_iter().flatten().collect();

        Ok(flattened_results)
    }
}

pub fn format_event_id(block_number: u64, transaction_hash: &Felt, event_idx: u64) -> String {
    format!(
        "{:#064x}:{:#x}:{:#04x}",
        block_number, transaction_hash, event_idx
    )
}

pub fn parse_event_id(event_id: &str) -> (BlockNumber, TransactionHash, EventIdx) {
    let parts: Vec<&str> = event_id.split(':').collect();
    (
        u64::from_str_radix(parts[0].trim_start_matches("0x"), 16).unwrap(),
        Felt::from_str(parts[1]).unwrap(),
        u64::from_str_radix(parts[2].trim_start_matches("0x"), 16).unwrap(),
    )
}

#[tokio::main]
async fn main() {
    let transport = HttpTransport::new(Url::parse("https://api.cartridge.gg/x/starknet/mainnet").unwrap()).with_header(
        "User-Agent".to_string(),
        format!("Torii/{}", "0.1.0"),
    );
    let provider: Arc<_> = JsonRpcClient::new(transport).into();

    let contracts = HashMap::from([
        (Felt::from_str("0x8b4838140a3cbd36ebe64d4b5aaf56a30cc3753c928a79338bf56c53f506c5").unwrap(), ContractType::WORLD),
        (Felt::from_str("0x41a78e741e5af2fec34b695679bc6891742439f7afb8484ecd7766661ad02bf").unwrap(), ContractType::UDC),
    ]);
    let cursors = contracts.iter().map(|(contract_address, _)| {
        (contract_address.clone(), Cursor {
            last_pending_block_event_id: None,
            head: None,
            last_block_timestamp: None,
        })
    }).collect();

    let fetcher = Fetcher {
        batch_chunk_size: 100,
        blocks_chunk_size: 100,
        events_chunk_size: 100,
        max_concurrent_tasks: 10,
        start_blocks: HashMap::new(),
        contracts,
        provider: provider.clone(),
        flags: IndexingFlags::all(),
    };

    let latest_block_number = provider.block_number().await.unwrap();

    let result = fetcher.fetch_range(&cursors, latest_block_number).await.unwrap();
    println!("{:?}", result);
}
