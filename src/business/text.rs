use super::SubBusiness;
use cache::JointData;
use error::Result;
use spec::{Message, Payload};

#[derive(Default)]
pub struct TextCache;

impl SubBusiness for TextCache {
    fn validate_message_basic(message: &Message) -> Result<()> {
        match message.payload {
            Some(Payload::Text(ref text)) => info!("validate text message: text = {:?}", text),
            _ => bail!("payload is not a text"),
        }
        Ok(())
    }

    fn check_business(_joint: &JointData, _message_idx: usize) -> Result<()> {
        Ok(())
    }

    fn validate_message(&self, _joint: &JointData, _message_idx: usize) -> Result<()> {
        Ok(())
    }

    fn apply_message(&mut self, _joint: &JointData, _message_idx: usize) -> Result<()> {
        Ok(())
    }

    fn revert_message(&mut self, _joint: &JointData, _message_idx: usize) -> Result<()> {
        unreachable!("text revert message")
    }
}
