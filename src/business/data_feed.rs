use super::SubBusiness;
use cache::JointData;
use config;
use error::Result;
use spec::{Message, Payload};

#[derive(Default)]
pub struct TimerCache {
    cur_time: u64,
}

impl SubBusiness for TimerCache {
    fn validate_message_basic(message: &Message) -> Result<()> {
        validate_datafeed(message)
    }

    fn check_business(_joint: &JointData, _message_idx: usize) -> Result<()> {
        // TODO: check if the time is bigger than current saved time
        Ok(())
    }

    fn validate_message(&self, _joint: &JointData, _message_idx: usize) -> Result<()> {
        // we no longer need to check the basic things
        // since that already done in temp_validate_message
        Ok(())
    }

    fn apply_message(&mut self, _joint: &JointData, _message_idx: usize) -> Result<()> {
        // TODO: update the current time
        self.cur_time = crate::time::now();
        unimplemented!()
    }

    fn revert_message(&mut self, _joint: &JointData, _message_idx: usize) -> Result<()> {
        unreachable!("data_feed revert message")
    }
}

fn validate_datafeed(message: &Message) -> Result<()> {
    match message.payload.as_ref() {
        Some(Payload::Other(ref v)) => {
            if let Some(map) = v.as_object() {
                if map.is_empty() {
                    bail!("data feed payload is empty object")
                }

                for (k, v) in map {
                    if k.len() > config::MAX_DATA_FEED_NAME_LENGTH {
                        bail!("feed name {} too long", k);
                    }

                    if let Some(s) = v.as_str() {
                        if s.len() > config::MAX_DATA_FEED_VALUE_LENGTH {
                            bail!("value {} too long", s);
                        }
                    } else if v.is_number() {
                        if v.is_f64() {
                            bail!("fractional numbers not allowed in data feeds");
                        }
                    } else {
                        bail!("data feed {} must be string or number", k);
                    }
                }
            } else {
                bail!("data feed payload is not object")
            }
        }
        _ => bail!("data feed payload is not data_feed"),
    }

    Ok(())
}
