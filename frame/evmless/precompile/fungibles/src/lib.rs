// SPDX-License-Identifier: Apache-2.0
// This file is part of Frontier.
//
// Copyright (c) 2020-2022 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(test)]
mod mock;

#[cfg(test)]
mod tests;

use core::marker::PhantomData;
use fp_evm::{ExitSucceed, Precompile, PrecompileHandle, PrecompileOutput, PrecompileResult};
use frame_support::traits::tokens::fungibles::Inspect;

pub type AssetIdOf<T> =
		<<T as pallet_evmless::Config>::Fungibles as Inspect<<T as frame_system::Config>::AccountId>>::AssetId;

pub struct Fungibles<T> {
    _marker: PhantomData<T>
}

impl<T> Precompile for Fungibles<T>
where
    T: pallet_evmless::Config,
    AssetIdOf<T>: From<u32>,
{
	fn execute(_handle: &mut impl PrecompileHandle) -> PrecompileResult {
        let a: AssetIdOf<T> = 0u32.into();
        let _b = T::Fungibles::minimum_balance(a);
		Ok(PrecompileOutput {
			exit_status: ExitSucceed::Stopped,
			output: Default::default(),
		})
	}
}