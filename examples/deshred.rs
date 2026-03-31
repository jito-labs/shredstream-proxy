use jito_protos::shredstream::{
    shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use solana_sdk::message::VersionedMessage;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let mut client = ShredstreamProxyClient::connect("http://X.X.X.X:Y")
        .await
        .unwrap();
    let mut stream = client
        .subscribe_entries(SubscribeEntriesRequest {})
        .await
        .unwrap()
        .into_inner();

    while let Some(slot_entry) = stream.message().await.unwrap() {
        // Create a hash identifier for this message batch
        let mut hasher = DefaultHasher::new();
        slot_entry.entries.hash(&mut hasher);
        let message_hash = hasher.finish();
        let message_id = format!("{:08x}", message_hash & 0xFFFFFFFF); // Use last 8 hex digits
        
        let entries =
            match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&slot_entry.entries) {
                Ok(e) => e,
                Err(e) => {
                    println!("Deserialization failed with err: {e}");
                    continue;
                }
            };
        println!(
            "[{}] slot {}, entries: {}, transactions: {}",
            message_id,
            slot_entry.slot,
            entries.len(),
            entries.iter().map(|e| e.transactions.len()).sum::<usize>()
        );
        // Print transaction count for each individual entry
        for (i, entry) in entries.iter().enumerate() {
            println!("  [{}] Entry {}: {} transactions", message_id, i + 1, entry.transactions.len());
            
            // Print details for each transaction in this entry
            for (j, transaction) in entry.transactions.iter().enumerate() {
                println!("    ╔══ Transaction {} ══", j + 1);
                
                // Show all signatures
                println!("    ║ Signatures ({}): ", transaction.signatures.len());
                for (sig_idx, signature) in transaction.signatures.iter().enumerate() {
                    println!("    ║   {}: {}", sig_idx + 1, signature);
                }
                
                // Show message details
                println!("    ║ Message:");
                
                // Handle VersionedMessage - get detailed info
                match &transaction.message {
                    VersionedMessage::Legacy(legacy_msg) => {
                        println!("    ║   Type: Legacy");
                        println!("    ║   Recent Blockhash: {}", legacy_msg.recent_blockhash);
                        println!("    ║   Header: req_sigs={}, readonly_signed={}, readonly_unsigned={}", 
                            legacy_msg.header.num_required_signatures,
                            legacy_msg.header.num_readonly_signed_accounts,
                            legacy_msg.header.num_readonly_unsigned_accounts
                        );
                        
                        // Show account keys
                        println!("    ║   Account Keys ({}):", legacy_msg.account_keys.len());
                        for (acc_idx, account) in legacy_msg.account_keys.iter().enumerate() {
                            println!("    ║     {}: {}", acc_idx, account);
                        }
                        
                        // Show instructions
                        println!("    ║   Instructions ({}):", legacy_msg.instructions.len());
                        for (inst_idx, instruction) in legacy_msg.instructions.iter().enumerate() {
                            println!("    ║     Instruction {}:", inst_idx + 1);
                            println!("    ║       Program ID Index: {}", instruction.program_id_index);
                            println!("    ║       Account Indices: {:?}", instruction.accounts);
                            println!("    ║       Data: {} bytes", instruction.data.len());
                            if instruction.data.len() <= 32 {
                                println!("    ║       Data Hex: {}", hex::encode(&instruction.data));
                            } else {
                                println!("    ║       Data Hex (first 32 bytes): {}", hex::encode(&instruction.data[..32]));
                            }
                        }
                    }
                    VersionedMessage::V0(v0_msg) => {
                        println!("    ║   Type: V0");
                        println!("    ║   Recent Blockhash: {}", v0_msg.recent_blockhash);
                        println!("    ║   Header: req_sigs={}, readonly_signed={}, readonly_unsigned={}", 
                            v0_msg.header.num_required_signatures,
                            v0_msg.header.num_readonly_signed_accounts,
                            v0_msg.header.num_readonly_unsigned_accounts
                        );
                        
                        // Show account keys
                        println!("    ║   Account Keys ({}):", v0_msg.account_keys.len());
                        for (acc_idx, account) in v0_msg.account_keys.iter().enumerate() {
                            println!("    ║     {}: {}", acc_idx, account);
                        }
                        
                        // Show address table lookups if any
                        if !v0_msg.address_table_lookups.is_empty() {
                            println!("    ║   Address Table Lookups ({}):", v0_msg.address_table_lookups.len());
                            for (lookup_idx, lookup) in v0_msg.address_table_lookups.iter().enumerate() {
                                println!("    ║     {}: table={}, writable={:?}, readonly={:?}", 
                                    lookup_idx, lookup.account_key, lookup.writable_indexes, lookup.readonly_indexes);
                            }
                        }
                        
                        // Show instructions
                        println!("    ║   Instructions ({}):", v0_msg.instructions.len());
                        for (inst_idx, instruction) in v0_msg.instructions.iter().enumerate() {
                            println!("    ║     Instruction {}:", inst_idx + 1);
                            println!("    ║       Program ID Index: {}", instruction.program_id_index);
                            println!("    ║       Account Indices: {:?}", instruction.accounts);
                            println!("    ║       Data: {} bytes", instruction.data.len());
                            if instruction.data.len() <= 32 {
                                println!("    ║       Data Hex: {}", hex::encode(&instruction.data));
                            } else {
                                println!("    ║       Data Hex (first 32 bytes): {}", hex::encode(&instruction.data[..32]));
                            }
                        }
                    }
                }
                
                println!("    ╚══════════════════════════════════");
            }
        }
    }
    Ok(())
}
