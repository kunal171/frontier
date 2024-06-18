use std::{marker::PhantomData, sync::Arc};
use fc_rpc::pending::ConsensusDataProvider;
use sc_client_api::{AuxStore, UsageProvider};
use sp_api:: ProvideRuntimeApi;
use sc_consensus_babe::{CompatibleDigestItem, PreDigest, SecondaryPlainPreDigest};
use sp_consensus_babe::inherents::BabeInherentData;
use sp_consensus_babe::{BabeApi, SlotDuration};
use sp_runtime::{traits::Block as BlockT, DigestItem};


#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("BABE inherent data missing")]
    MissingInherent,
}

impl From<Error> for sp_inherents::Error {
    fn from(err: Error) -> Self {
        sp_inherents::Error::Application(Box::new(err))
    }
}

#[derive(Clone)] 
pub struct BabeConsensusDataProvider<B, C> {
    	// slot duration
		slot_duration: SlotDuration,
		// phantom data for required generics
		_phantom: PhantomData<(B, C)>,
}

impl<B, C> BabeConsensusDataProvider<B, C>
    where
		B: BlockT,
		C: AuxStore + ProvideRuntimeApi<B> + UsageProvider<B>,
		C::Api: BabeApi<B>, 
    {
        pub fn new(client: Arc<C>) -> Self {

            let babe_configuration = sc_consensus_babe::configuration(&*client).unwrap();
            let slot_duration: SlotDuration = babe_configuration.slot_duration();
                // .expect("slot_duration is always present; qed.");
            Self {
                slot_duration: slot_duration,
                _phantom: PhantomData,
            }
        }
}

impl<B: BlockT, C: Send + Sync> ConsensusDataProvider<B> for BabeConsensusDataProvider<B, C>
{
    fn create_digest(
        &self,
        _parent: &<B as BlockT>::Header,
        data: &sp_inherents::InherentData,
    ) -> Result<sp_runtime::Digest, sp_inherents::Error> {
        let slot = data
            .babe_inherent_data()?
            .ok_or(sp_inherents::Error::Application(Box::new(
                Error::MissingInherent,
            )))?;

        let predigest = PreDigest::SecondaryPlain(SecondaryPlainPreDigest {
            slot,
            authority_index: 0,
        });

        let logs = vec![<DigestItem as CompatibleDigestItem>::babe_pre_digest(
            predigest,
        )];

        Ok(sp_runtime::Digest { logs })
    }
}