//! Provides ClientBuilder extension for super easy use with serenity
use futures::executor;
use std::sync::Arc;

use crate::{init_charcoal, Charcoal, CharcoalConfig};
use serenity::prelude::TypeMapKey;
// pub use serenity::client::ClientBuilder;
pub use serenity::client::ClientBuilder;
use serenity::*;
use tokio::sync::Mutex;

pub struct CharcoalKey;

impl TypeMapKey for CharcoalKey {
    type Value = Arc<Mutex<Charcoal>>;
}

pub trait SerenityInit {
    #[must_use]
    /// Initializes charcoal and registers it in the Serenity type-map
    fn register_charcoal(self, broker: String, config: CharcoalConfig) -> Self;
}

impl SerenityInit for ClientBuilder {
    fn register_charcoal(self, broker: String, config: CharcoalConfig) -> Self {
        let c = init_charcoal(broker, config);
        self.type_map_insert::<CharcoalKey>(executor::block_on(c))
    }
}


#[macro_export]
macro_rules! get_handler_from_interaction_mutable {
    ($ctx: expr, $interaction: expr, $reference: ident) => {
        let r = $ctx.data.read().await;
        let guild_id = match $interaction.guild_id {
            Some(gid) => gid,
            None => {
                eprintln!("No guild ID found in interaction");
                return Ok(());
            }
        };
        let manager = r.get::<CharcoalKey>();
        let mut mx = manager.unwrap().lock().await;
        let mut players = mx.players.write().await;
        $reference = players.get_mut(&guild_id.to_string());
    };
}
#[macro_export]
macro_rules! get_handler_from_interaction {
    ($ctx: expr, $interaction: expr, $reference: ident) => {
        let r = $ctx.data.read().await;
        let guild_id = match $interaction.guild_id {
            Some(gid) => gid,
            None => {
                eprintln!("No guild ID found in interaction");
                return Ok(());
            }
        };
        let manager = r.get::<CharcoalKey>();
        let mut mx = manager.unwrap().lock().await;
        let mut players = mx.players.write().await;
        $reference = players.get_mut(&guild_id.to_string());
    };
}