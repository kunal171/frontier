//! A collection of node-specific RPC methods.

use std::sync::Arc;

use jsonrpsee::RpcModule;
// Substrate
use sc_client_api::{
	backend::{Backend, StateBackend, StorageProvider},
	client::BlockchainEvents,
	AuxStore, BlockBackend, UsageProvider,
};
use sc_consensus_babe::BabeWorkerHandle;
use sc_consensus_grandpa::{
	BlockNumberOps, FinalityProofProvider, GrandpaJustificationStream, SharedAuthoritySet,
	SharedVoterState,
};
pub(crate) use sc_rpc::SubscriptionTaskExecutor;
use sc_rpc_api::DenyUnsafe;
use sc_service::TransactionPool;
use sc_transaction_pool::ChainApi;
use sp_api::{CallApiAt, ProvideRuntimeApi};
use sp_blockchain::{Error as BlockChainError, HeaderBackend, HeaderMetadata};
use sp_consensus_babe::BabeApi;
use sp_core::H256;
use sp_inherents::CreateInherentDataProviders;
use sp_keystore::KeystorePtr;
use sp_runtime::traits::{Block as BlockT, NumberFor};

// Runtime
use planck_runtime::{AccountId, Balance, Hash, Nonce};

mod eth;

pub use self::eth::{create_eth, EthDeps};

/// Extra dependencies for BABE.
pub struct BabeDeps<B: BlockT> {
	/// A handle to the BABE worker for issuing requests.
	pub babe_worker_handle: BabeWorkerHandle<B>,
	/// The keystore that manages the keys of the node.
	pub keystore: KeystorePtr,
}

/// Extra dependencies for GRANDPA
pub struct GrandpaDeps<B: BlockT, BE> {
	/// Voting round info.
	pub shared_voter_state: SharedVoterState,
	/// Authority set info.
	pub shared_authority_set: SharedAuthoritySet<Hash, NumberFor<B>>,
	/// Receives notifications about justification events from Grandpa.
	pub justification_stream: GrandpaJustificationStream<B>,
	/// Executor to drive the subscription manager in the Grandpa RPC handler.
	pub subscription_executor: SubscriptionTaskExecutor,
	/// Finality proof provider.
	pub finality_provider: Arc<FinalityProofProvider<BE, B>>,
}

/// Full client dependencies.
pub struct FullDeps<B: BlockT, C, P, BE, A: ChainApi, CT, CIDP, SC> {
	/// The client instance to use.
	pub client: Arc<C>,
	/// The backend used by the node.
	/// Reserved for future rpc
	// pub backend: Arc<BE>,
	/// The SelectChain Strategy
	pub select_chain: SC,
	/// A copy of the chain spec.
	pub chain_spec: Box<dyn sc_chain_spec::ChainSpec>,
	/// Transaction pool instance.
	pub pool: Arc<P>,
	/// Whether to deny unsafe calls
	pub deny_unsafe: DenyUnsafe,
	/// BABE specific dependencies.
	pub babe: BabeDeps<B>,
	/// GRANDPA specific dependencies.
	pub grandpa: GrandpaDeps<B, BE>,
	/// Ethereum-compatibility specific dependencies.
	pub eth: EthDeps<B, C, P, A, CT, CIDP>,
}

pub struct DefaultEthConfig<C, BE>(std::marker::PhantomData<(C, BE)>);

impl<B, C, BE> fc_rpc::EthConfig<B, C> for DefaultEthConfig<C, BE>
where
	B: BlockT,
	C: StorageProvider<B, BE> + Sync + Send + 'static,
	BE: Backend<B> + 'static,
{
	type EstimateGasAdapter = ();
	type RuntimeStorageOverride =
		fc_rpc::frontier_backend_client::SystemAccountId20StorageOverride<B, C, BE>;
}

/// Instantiate all Full RPC extensions.
pub fn create_full<B, C, P, BE, A, CT, CIDP, SC>(
	FullDeps {
		client,
		pool,
		select_chain,
		deny_unsafe,
		babe,
		grandpa,
		eth,
		chain_spec,
	}: FullDeps<B, C, P, BE, A, CT, CIDP, SC>,
	subscription_task_executor: SubscriptionTaskExecutor,
	pubsub_notification_sinks: Arc<
		fc_mapping_sync::EthereumBlockNotificationSinks<
			fc_mapping_sync::EthereumBlockNotification<B>,
		>,
	>,
) -> Result<RpcModule<()>, Box<dyn std::error::Error + Send + Sync>>
where
	B: BlockT<Hash = H256>,
	NumberFor<B>: BlockNumberOps,
	C: CallApiAt<B> + ProvideRuntimeApi<B>,
	C::Api: sp_block_builder::BlockBuilder<B>
		+ BabeApi<B>
		+ substrate_frame_rpc_system::AccountNonceApi<B, AccountId, Nonce>
		+ pallet_transaction_payment_rpc::TransactionPaymentRuntimeApi<B, Balance>
		+ fp_rpc::ConvertTransactionRuntimeApi<B>
		+ fp_rpc::EthereumRuntimeRPCApi<B>,
	C: BlockBackend<B>
		+ HeaderBackend<B>
		+ HeaderMetadata<B, Error = BlockChainError>
		+ BlockchainEvents<B>
		+ AuxStore
		+ UsageProvider<B>
		+ StorageProvider<B, BE>
		+ 'static,
	BE: Backend<B> + 'static,
	BE::State: StateBackend<sp_runtime::traits::HashingFor<B>>,
	P: TransactionPool<Block = B> + 'static,
	A: ChainApi<Block = B> + 'static,
	CIDP: CreateInherentDataProviders<B, ()> + Send + 'static,
	CT: fp_rpc::ConvertTransaction<<B as BlockT>::Extrinsic> + Send + Sync + 'static,
	SC: sp_consensus::SelectChain<B> + 'static,
{
	use pallet_transaction_payment_rpc::{TransactionPayment, TransactionPaymentApiServer};
	use sc_consensus_babe_rpc::{Babe, BabeApiServer};
	use sc_consensus_grandpa_rpc::{Grandpa, GrandpaApiServer};
	use sc_rpc_spec_v2::chain_spec::{ChainSpec, ChainSpecApiServer};
	use sc_sync_state_rpc::{SyncState, SyncStateApiServer};
	use substrate_frame_rpc_system::{System, SystemApiServer};

	let mut io = RpcModule::new(());

	let chain_name = chain_spec.name().to_string();
	let genesis_hash = client
		.block_hash(0_u32.into())
		.ok()
		.flatten()
		.expect("Genesis block exists; qed");
	let properties = chain_spec.properties();
	io.merge(ChainSpec::new(chain_name, genesis_hash, properties).into_rpc())?;

	let BabeDeps {
		keystore,
		babe_worker_handle,
	} = babe;

	let GrandpaDeps {
		shared_voter_state,
		shared_authority_set,
		subscription_executor,
		justification_stream,
		finality_provider,
	} = grandpa;

	io.merge(System::new(client.clone(), pool.clone(), deny_unsafe).into_rpc())?;
	io.merge(TransactionPayment::new(client.clone()).into_rpc())?;
	io.merge(
		Babe::new(
			client.clone(),
			babe_worker_handle.clone(),
			keystore,
			select_chain,
			deny_unsafe,
		)
		.into_rpc(),
	)?;
	io.merge(
		Grandpa::new(
			subscription_executor,
			shared_authority_set.clone(),
			shared_voter_state,
			justification_stream,
			finality_provider,
		)
		.into_rpc(),
	)?;

	io.merge(
		SyncState::new(
			chain_spec,
			client.clone(),
			shared_authority_set,
			babe_worker_handle,
		)?
		.into_rpc(),
	)?;
	// io.merge(Dev::new(client.clone(), deny_unsafe).into_rpc())?;

	// Ethereum compatibility RPCs
	let io = create_eth::<_, _, _, _, _, _, _, DefaultEthConfig<C, BE>>(
		io,
		eth,
		subscription_task_executor,
		pubsub_notification_sinks,
	)?;

	Ok(io)
}
