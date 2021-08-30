mod adapter;
mod capabilities;
mod data_source;
mod near_adapter;
mod trigger;

// StreamingFast components
pub mod chain;
pub mod runtime;
pub mod sf;

pub use self::near_adapter::NearAdapter;
pub use self::runtime::RuntimeAdapter;

// ETHDEP: These concrete types should probably not be exposed.
pub use data_source::{DataSource, DataSourceTemplate};
pub use trigger::MappingTrigger;

pub use crate::adapter::{NearAdapter as NearAdapterTrait, TriggerFilter};
pub use crate::chain::Chain;
