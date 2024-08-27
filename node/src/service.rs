//! Service and ServiceFactory implementation. Specialized wrapper over substrate service.

use std::{path::Path, sync::Arc, time::Duration};

use futures::prelude::*;
// Substrate
use sc_client_api::{Backend as BackendT, BlockBackend};
use sc_consensus::{BasicQueue, BoxBlockImport};
use sc_consensus_babe::{BabeLink, BabeWorkerHandle, SlotProportion};
use sc_consensus_grandpa::BlockNumberOps;
use sc_executor::HostFunctions as HostFunctionsT;
use sc_network_sync::strategy::warp::{WarpSyncParams, WarpSyncProvider};
use sc_service::{error::Error as ServiceError, Configuration, PartialComponents, TaskManager};
use sc_telemetry::{Telemetry, TelemetryHandle, TelemetryWorker};
use sc_transaction_pool::FullPool;
use sc_transaction_pool_api::OffchainTransactionPoolFactory;
use sp_api::ConstructRuntimeApi;
use sp_core::U256;
use sp_runtime::traits::{Block as BlockT, NumberFor};
// Runtime
use planck_runtime::{opaque::Block, AccountId, Balance, Nonce, RuntimeApi, TransactionConverter};

pub use crate::eth::{db_config_dir, EthConfiguration};
use crate::{
	client::{BaseRuntimeApiCollection, FullBackend, FullClient, RuntimeApiCollection},
	eth::{
		new_frontier_partial, spawn_frontier_tasks, BackendType, EthCompatRuntimeApiCollection,
		FrontierBackend, FrontierBlockImport, FrontierPartialComponents, StorageOverride,
		StorageOverrideHandler,
	},
	rpc,
};

/// Only enable the benchmarking host functions when we actually want to benchmark.
#[cfg(feature = "runtime-benchmarks")]
pub type HostFunctions = (
	sp_io::SubstrateHostFunctions,
	frame_benchmarking::benchmarking::HostFunctions,
);
/// Otherwise we use empty host functions for ext host functions.
#[cfg(not(feature = "runtime-benchmarks"))]
pub type HostFunctions = sp_io::SubstrateHostFunctions;

pub type Backend = FullBackend;
pub type Client = FullClient<RuntimeApi, HostFunctions>;

type FullSelectChain = sc_consensus::LongestChain<FullBackend, Block>;
type GrandpaBlockImport<C> =
	sc_consensus_grandpa::GrandpaBlockImport<FullBackend, Block, C, FullSelectChain>;
type GrandpaLinkHalf<C> = sc_consensus_grandpa::LinkHalf<Block, C, FullSelectChain>;

/// The minimum period of blocks on which justifications will be
/// imported and generated.
const GRANDPA_JUSTIFICATION_PERIOD: u32 = 512;

pub fn new_partial<RA, HF, BIQ>(
	config: &Configuration,
	eth_config: &EthConfiguration,
	build_import_queue: BIQ,
) -> Result<
	PartialComponents<
		FullClient<RA, HF>,
		FullBackend,
		FullSelectChain,
		BasicQueue<Block>,
		FullPool<Block, FullClient<RA, HF>>,
		(
			Option<Telemetry>,
			BoxBlockImport<Block>,
			(BabeLink<Block>, BabeWorkerHandle<Block>),
			GrandpaLinkHalf<FullClient<RA, HF>>,
			FrontierBackend<FullClient<RA, HF>>,
			Arc<dyn StorageOverride<Block>>,
		),
	>,
	ServiceError,
>
where
	RA: ConstructRuntimeApi<Block, FullClient<RA, HF>>,
	RA: Send + Sync + 'static,
	RA::RuntimeApi: BaseRuntimeApiCollection<Block> + EthCompatRuntimeApiCollection<Block>,
	HF: HostFunctionsT + 'static,
	BIQ: FnOnce(
		Arc<FullClient<RA, HF>>,
		&Configuration,
		&EthConfiguration,
		&TaskManager,
		&FullSelectChain,
		Option<TelemetryHandle>,
		GrandpaBlockImport<FullClient<RA, HF>>,
	) -> Result<
		(
			BasicQueue<Block>,
			BoxBlockImport<Block>,
			(BabeLink<Block>, BabeWorkerHandle<Block>),
		),
		ServiceError,
	>,
{
	let telemetry = config
		.telemetry_endpoints
		.clone()
		.filter(|x| !x.is_empty())
		.map(|endpoints| -> Result<_, sc_telemetry::Error> {
			let worker = TelemetryWorker::new(16)?;
			let telemetry = worker.handle().new_telemetry(endpoints);
			Ok((worker, telemetry))
		})
		.transpose()?;

	let executor = sc_service::new_wasm_executor(config);

	let (client, backend, keystore_container, task_manager) =
		sc_service::new_full_parts::<Block, RA, _>(
			config,
			telemetry.as_ref().map(|(_, telemetry)| telemetry.handle()),
			executor,
		)?;
	let client = Arc::new(client);

	let telemetry = telemetry.map(|(worker, telemetry)| {
		task_manager
			.spawn_handle()
			.spawn("telemetry", None, worker.run());
		telemetry
	});

	let select_chain = sc_consensus::LongestChain::new(backend.clone());
	let (grandpa_block_import, grandpa_link) = sc_consensus_grandpa::block_import(
		client.clone(),
		GRANDPA_JUSTIFICATION_PERIOD,
		&(client.clone() as Arc<_>),
		select_chain.clone(),
		telemetry.as_ref().map(|x| x.handle()),
	)?;

	let storage_override = Arc::new(StorageOverrideHandler::<Block, _, _>::new(client.clone()));
	let frontier_backend = match eth_config.frontier_backend_type {
		BackendType::KeyValue => FrontierBackend::KeyValue(Arc::new(fc_db::kv::Backend::open(
			Arc::clone(&client),
			&config.database,
			&db_config_dir(config),
		)?)),
		BackendType::Sql => {
			let db_path = db_config_dir(config).join("sql");
			std::fs::create_dir_all(&db_path).expect("failed creating sql db directory");
			let backend = futures::executor::block_on(fc_db::sql::Backend::new(
				fc_db::sql::BackendConfig::Sqlite(fc_db::sql::SqliteBackendConfig {
					path: Path::new("sqlite:///")
						.join(db_path)
						.join("frontier.db3")
						.to_str()
						.unwrap(),
					create_if_missing: true,
					thread_count: eth_config.frontier_sql_backend_thread_count,
					cache_size: eth_config.frontier_sql_backend_cache_size,
				}),
				eth_config.frontier_sql_backend_pool_size,
				std::num::NonZeroU32::new(eth_config.frontier_sql_backend_num_ops_timeout),
				storage_override.clone(),
			))
			.unwrap_or_else(|err| panic!("failed creating sql backend: {:?}", err));
			FrontierBackend::Sql(Arc::new(backend))
		}
	};

	let (import_queue, block_import, babe_deps) = build_import_queue(
		client.clone(),
		config,
		eth_config,
		&task_manager,
		&select_chain,
		telemetry.as_ref().map(|x| x.handle()),
		grandpa_block_import,
	)?;

	let transaction_pool = sc_transaction_pool::BasicPool::new_full(
		config.transaction_pool.clone(),
		config.role.is_authority().into(),
		config.prometheus_registry(),
		task_manager.spawn_essential_handle(),
		client.clone(),
	);

	Ok(PartialComponents {
		client,
		backend,
		keystore_container,
		task_manager,
		select_chain,
		import_queue,
		transaction_pool,
		other: (
			telemetry,
			block_import,
			babe_deps,
			grandpa_link,
			frontier_backend,
			storage_override,
		),
	})
}

/// Builds a new service for a full client.
pub async fn new_full<RA, HF, NB>(
	mut config: Configuration,
	eth_config: EthConfiguration,
) -> Result<TaskManager, ServiceError>
where
	NumberFor<Block>: BlockNumberOps,
	<Block as BlockT>::Header: Unpin,
	RA: ConstructRuntimeApi<Block, FullClient<RA, HF>>,
	RA: Send + Sync + 'static,
	RA::RuntimeApi: RuntimeApiCollection<Block, AccountId, Nonce, Balance>,
	HF: HostFunctionsT + 'static,
	NB: sc_network::NetworkBackend<Block, <Block as BlockT>::Hash>,
{
	let build_import_queue = build_babe_grandpa_import_queue::<RA, HF>;

	let PartialComponents {
		client,
		backend,
		mut task_manager,
		import_queue,
		keystore_container,
		select_chain,
		transaction_pool,
		other:
			(mut telemetry, block_import, babe_deps, grandpa_link, frontier_backend, storage_override),
	} = new_partial(&config, &eth_config, build_import_queue)?;

	let (babe_link, babe_worker_handle) = babe_deps;

	let FrontierPartialComponents {
		filter_pool,
		fee_history_cache,
		fee_history_cache_limit,
	} = new_frontier_partial(&eth_config)?;

	let mut net_config =
		sc_network::config::FullNetworkConfiguration::<_, _, NB>::new(&config.network);
	let peer_store_handle = net_config.peer_store_handle();
	let metrics = NB::register_notification_metrics(
		config.prometheus_config.as_ref().map(|cfg| &cfg.registry),
	);

	let grandpa_protocol_name = sc_consensus_grandpa::protocol_standard_name(
		&client
			.block_hash(0_u32.into())?
			.expect("Genesis block exists; qed"),
		&config.chain_spec,
	);
	let (grandpa_protocol_config, grandpa_notification_service) =
		sc_consensus_grandpa::grandpa_peers_set_config::<_, NB>(
			grandpa_protocol_name.clone(),
			metrics.clone(),
			peer_store_handle,
		);

	let warp_sync_params = {
		net_config.add_notification_protocol(grandpa_protocol_config);
		let warp_sync: Arc<dyn WarpSyncProvider<Block>> =
			Arc::new(sc_consensus_grandpa::warp_proof::NetworkProvider::new(
				backend.clone(),
				grandpa_link.shared_authority_set().clone(),
				Vec::new(),
			));
		Some(WarpSyncParams::WithProvider(warp_sync))
	};

	let (network, system_rpc_tx, tx_handler_controller, network_starter, sync_service) =
		sc_service::build_network(sc_service::BuildNetworkParams {
			config: &config,
			net_config,
			client: client.clone(),
			transaction_pool: transaction_pool.clone(),
			spawn_handle: task_manager.spawn_handle(),
			import_queue,
			block_announce_validator_builder: None,
			warp_sync_params,
			block_relay: None,
			metrics,
		})?;

	if config.offchain_worker.enabled {
		task_manager.spawn_handle().spawn(
			"offchain-workers-runner",
			"offchain-worker",
			sc_offchain::OffchainWorkers::new(sc_offchain::OffchainWorkerOptions {
				runtime_api_provider: client.clone(),
				is_validator: config.role.is_authority(),
				keystore: Some(keystore_container.keystore()),
				offchain_db: backend.offchain_storage(),
				transaction_pool: Some(OffchainTransactionPoolFactory::new(
					transaction_pool.clone(),
				)),
				network_provider: Arc::new(network.clone()),
				enable_http_requests: true,
				custom_extensions: |_| vec![],
			})
			.run(client.clone(), task_manager.spawn_handle())
			.boxed(),
		);
	}

	let role = config.role.clone();
	let force_authoring = config.force_authoring;
	let name = config.network.node_name.clone();
	let frontier_backend = Arc::new(frontier_backend);
	let enable_grandpa = !config.disable_grandpa;
	let prometheus_registry = config.prometheus_registry().cloned();

	// Sinks for pubsub notifications.
	// Everytime a new subscription is created, a new mpsc channel is added to the sink pool.
	// The MappingSyncWorker sends through the channel on block import and the subscription emits a notification to the subscriber on receiving a message through this channel.
	// This way we avoid race conditions when using native substrate block import notification stream.
	let pubsub_notification_sinks: fc_mapping_sync::EthereumBlockNotificationSinks<
		fc_mapping_sync::EthereumBlockNotification<Block>,
	> = Default::default();
	let pubsub_notification_sinks = Arc::new(pubsub_notification_sinks);

	// for ethereum-compatibility rpc.
	config.rpc_id_provider = Some(Box::new(fc_rpc::EthereumSubIdProvider));

	let justification_stream = grandpa_link.justification_stream();
	let shared_authority_set = grandpa_link.shared_authority_set().clone();
	let shared_voter_state = sc_consensus_grandpa::SharedVoterState::empty();
	let shared_voter_state2 = shared_voter_state.clone();

	let finality_proof_provider = sc_consensus_grandpa::FinalityProofProvider::new_for_service(
		backend.clone(),
		Some(shared_authority_set.clone()),
	);

	let rpc_builder = {
		let client = client.clone();
		let pool = transaction_pool.clone();
		let network = network.clone();
		let sync_service = sync_service.clone();

		let is_authority = role.is_authority();
		let enable_dev_signer = eth_config.enable_dev_signer;
		let max_past_logs = eth_config.max_past_logs;
		let execute_gas_limit_multiplier = eth_config.execute_gas_limit_multiplier;
		let filter_pool = filter_pool.clone();
		let frontier_backend = frontier_backend.clone();
		let pubsub_notification_sinks = pubsub_notification_sinks.clone();
		let storage_override = storage_override.clone();
		let fee_history_cache = fee_history_cache.clone();
		let block_data_cache = Arc::new(fc_rpc::EthBlockDataCacheTask::new(
			task_manager.spawn_handle(),
			storage_override.clone(),
			eth_config.eth_log_block_cache,
			eth_config.eth_statuses_cache,
			prometheus_registry.clone(),
		));

		let slot_duration = babe_link.config().slot_duration();
		let target_gas_price = eth_config.target_gas_price;
		let chain_spec = config.chain_spec.cloned_box();
		// let _rpc_backend = backend.clone();
		let keystore = keystore_container.keystore();
		let rpc_select_chain = select_chain.clone();

		Box::new(
			move |deny_unsafe, subscription_task_executor: rpc::SubscriptionTaskExecutor| {
				let eth_deps = crate::rpc::EthDeps {
					client: client.clone(),
					pool: pool.clone(),
					graph: pool.pool().clone(),
					converter: Some(TransactionConverter::<Block>::default()),
					is_authority,
					enable_dev_signer,
					network: network.clone(),
					sync: sync_service.clone(),
					frontier_backend: match &*frontier_backend {
						fc_db::Backend::KeyValue(b) => b.clone(),
						fc_db::Backend::Sql(b) => b.clone(),
					},
					storage_override: storage_override.clone(),
					block_data_cache: block_data_cache.clone(),
					filter_pool: filter_pool.clone(),
					max_past_logs,
					fee_history_cache: fee_history_cache.clone(),
					fee_history_cache_limit,
					execute_gas_limit_multiplier,
					forced_parent_hashes: None,
					pending_create_inherent_data_providers: move |_, ()| async move {
						let current = sp_timestamp::InherentDataProvider::from_system_time();
						let next_slot = current.timestamp().as_millis() + slot_duration.as_millis();
						let timestamp = sp_timestamp::InherentDataProvider::new(next_slot.into());
						let slot = sp_consensus_babe::inherents::InherentDataProvider::from_timestamp_and_slot_duration(
						*timestamp,
						slot_duration,
					);
						let dynamic_fee =
							fp_dynamic_fee::InherentDataProvider(U256::from(target_gas_price));
						Ok((slot, timestamp, dynamic_fee))
					},
				};
				let deps = crate::rpc::FullDeps {
					client: client.clone(),
					select_chain: rpc_select_chain.clone(),
					chain_spec: chain_spec.cloned_box(),
					pool: pool.clone(),
					deny_unsafe,
					eth: eth_deps,
					babe: rpc::BabeDeps {
						babe_worker_handle: babe_worker_handle.clone(),
						keystore: keystore.clone(),
					},
					grandpa: rpc::GrandpaDeps {
						shared_voter_state: shared_voter_state.clone(),
						shared_authority_set: shared_authority_set.clone(),
						justification_stream: justification_stream.clone(),
						finality_provider: finality_proof_provider.clone(),
						subscription_executor: subscription_task_executor.clone(),
					},
				};
				crate::rpc::create_full(
					deps,
					subscription_task_executor,
					pubsub_notification_sinks.clone(),
				)
				.map_err(Into::into)
			},
		)
	};

	let _rpc_handlers = sc_service::spawn_tasks(sc_service::SpawnTasksParams {
		config,
		client: client.clone(),
		backend: backend.clone(),
		task_manager: &mut task_manager,
		keystore: keystore_container.keystore(),
		transaction_pool: transaction_pool.clone(),
		rpc_builder,
		network: network.clone(),
		system_rpc_tx,
		tx_handler_controller,
		sync_service: sync_service.clone(),
		telemetry: telemetry.as_mut(),
	})?;

	spawn_frontier_tasks(
		&task_manager,
		client.clone(),
		backend,
		frontier_backend,
		filter_pool,
		storage_override,
		fee_history_cache,
		fee_history_cache_limit,
		sync_service.clone(),
		pubsub_notification_sinks,
	)
	.await;

	if role.is_authority() {
		let proposer_factory = sc_basic_authorship::ProposerFactory::new(
			task_manager.spawn_handle(),
			client.clone(),
			transaction_pool.clone(),
			prometheus_registry.as_ref(),
			telemetry.as_ref().map(|x| x.handle()),
		);

		let slot_duration = babe_link.config().slot_duration();
		let target_gas_price = eth_config.target_gas_price;
		let create_inherent_data_providers = move |_, ()| async move {
			let timestamp = sp_timestamp::InherentDataProvider::from_system_time();
			let slot = sp_consensus_babe::inherents::InherentDataProvider::from_timestamp_and_slot_duration(
				*timestamp,
				slot_duration,
			);
			let dynamic_fee = fp_dynamic_fee::InherentDataProvider(U256::from(target_gas_price));
			Ok((slot, timestamp, dynamic_fee))
		};

		let babe = sc_consensus_babe::start_babe(sc_consensus_babe::BabeParams {
			keystore: keystore_container.keystore(),
			client,
			select_chain,
			env: proposer_factory,
			block_import,
			sync_oracle: sync_service.clone(),
			justification_sync_link: sync_service.clone(),
			create_inherent_data_providers,
			force_authoring,
			backoff_authoring_blocks: Option::<()>::None,
			babe_link,
			block_proposal_slot_portion: SlotProportion::new(0.5),
			max_block_proposal_slot_portion: None,
			telemetry: telemetry.as_ref().map(|x| x.handle()),
		})?;
		// the babe authoring task is considered essential, i.e. if it
		// fails we take down the service with it.
		task_manager
			.spawn_essential_handle()
			.spawn_blocking("babe", Some("block-authoring"), babe);
	}

	if enable_grandpa {
		// if the node isn't actively participating in consensus then it doesn't
		// need a keystore, regardless of which protocol we use below.
		let keystore = if role.is_authority() {
			Some(keystore_container.keystore())
		} else {
			None
		};

		let grandpa_config = sc_consensus_grandpa::Config {
			// FIXME #1578 make this available through chainspec
			gossip_duration: Duration::from_millis(333),
			justification_generation_period: GRANDPA_JUSTIFICATION_PERIOD,
			name: Some(name),
			observer_enabled: false,
			keystore,
			local_role: role,
			telemetry: telemetry.as_ref().map(|x| x.handle()),
			protocol_name: grandpa_protocol_name,
		};

		// start the full GRANDPA voter
		// NOTE: non-authorities could run the GRANDPA observer protocol, but at
		// this point the full voter should provide better guarantees of block
		// and vote data availability than the observer. The observer has not
		// been tested extensively yet and having most nodes in a network run it
		// could lead to finality stalls.
		let grandpa_voter =
			sc_consensus_grandpa::run_grandpa_voter(sc_consensus_grandpa::GrandpaParams {
				config: grandpa_config,
				link: grandpa_link,
				network,
				sync: sync_service,
				notification_service: grandpa_notification_service,
				voting_rule: sc_consensus_grandpa::VotingRulesBuilder::default().build(),
				prometheus_registry,
				shared_voter_state: shared_voter_state2,
				telemetry: telemetry.as_ref().map(|x| x.handle()),
				offchain_tx_pool_factory: OffchainTransactionPoolFactory::new(transaction_pool),
			})?;

		// the GRANDPA voter task is considered infallible, i.e.
		// if it fails we take down the service with it.
		task_manager
			.spawn_essential_handle()
			.spawn_blocking("grandpa-voter", None, grandpa_voter);
	}

	network_starter.start_network();
	Ok(task_manager)
}

/// Build the import queue for the planck runtime babe(frontier(grandpa(client))).
pub fn build_babe_grandpa_import_queue<RA, HF>(
	client: Arc<FullClient<RA, HF>>,
	config: &Configuration,
	eth_config: &EthConfiguration,
	task_manager: &TaskManager,
	select_chain: &FullSelectChain,
	telemetry: Option<TelemetryHandle>,
	grandpa_block_import: GrandpaBlockImport<FullClient<RA, HF>>,
) -> Result<
	(
		BasicQueue<Block>,
		BoxBlockImport<Block>,
		(BabeLink<Block>, BabeWorkerHandle<Block>),
	),
	ServiceError,
>
where
	NumberFor<Block>: BlockNumberOps,
	RA: ConstructRuntimeApi<Block, FullClient<RA, HF>>,
	RA: Send + Sync + 'static,
	RA::RuntimeApi: RuntimeApiCollection<Block, AccountId, Nonce, Balance>,
	HF: HostFunctionsT + 'static,
{
	let frontier_block_import =
		FrontierBlockImport::new(grandpa_block_import.clone(), client.clone());

	let (babe_block_import, babe_link) = sc_consensus_babe::block_import(
		sc_consensus_babe::configuration(&*client)?,
		frontier_block_import.clone(),
		client.clone(),
	)?;

	let slot_duration = babe_link.config().slot_duration();

	let target_gas_price = eth_config.target_gas_price;
	let create_inherent_data_providers = move |_, ()| async move {
		let timestamp = sp_timestamp::InherentDataProvider::from_system_time();
		let slot =
			sp_consensus_babe::inherents::InherentDataProvider::from_timestamp_and_slot_duration(
				*timestamp,
				slot_duration,
			);
		let dynamic_fee = fp_dynamic_fee::InherentDataProvider(U256::from(target_gas_price));
		Ok((slot, timestamp, dynamic_fee))
	};

	let transaction_pool = sc_transaction_pool::BasicPool::new_full(
		config.transaction_pool.clone(),
		config.role.is_authority().into(),
		config.prometheus_registry(),
		task_manager.spawn_essential_handle(),
		client.clone(),
	);

	let (import_queue, babe_worker_handle) =
		sc_consensus_babe::import_queue(sc_consensus_babe::ImportQueueParams {
			link: babe_link.clone(),
			block_import: babe_block_import.clone(),
			justification_import: Some(Box::new(grandpa_block_import.clone())),
			client,
			select_chain: select_chain.clone(),
			create_inherent_data_providers,
			spawner: &task_manager.spawn_essential_handle(),
			registry: config.prometheus_registry(),
			telemetry,
			offchain_tx_pool_factory: OffchainTransactionPoolFactory::new(transaction_pool.clone()),
		})
		.map_err::<ServiceError, _>(Into::into)?;

	Ok((
		import_queue,
		Box::new(babe_block_import),
		(babe_link, babe_worker_handle),
	))
}

pub async fn build_full(
	config: Configuration,
	eth_config: EthConfiguration,
) -> Result<TaskManager, ServiceError> {
	new_full::<planck_runtime::RuntimeApi, HostFunctions, sc_network::NetworkWorker<_, _>>(
		config, eth_config,
	)
	.await
}

pub fn new_chain_ops(
	config: &mut Configuration,
	eth_config: &EthConfiguration,
) -> Result<
	(
		Arc<Client>,
		Arc<Backend>,
		BasicQueue<Block>,
		TaskManager,
		FrontierBackend<Client>,
	),
	ServiceError,
> {
	config.keystore = sc_service::config::KeystoreConfig::InMemory;
	let PartialComponents {
		client,
		backend,
		import_queue,
		task_manager,
		other,
		..
	} = new_partial::<planck_runtime::RuntimeApi, HostFunctions, _>(
		config,
		eth_config,
		build_babe_grandpa_import_queue,
	)?;
	Ok((client, backend, import_queue, task_manager, other.4))
}
