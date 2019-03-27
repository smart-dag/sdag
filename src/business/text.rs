use super::SubBusiness;
use cache::JointData;
use error::Result;
use light;
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

pub fn get_text(unit: &str) -> Result<light::Text> {
    use cache::SDAG_CACHE;
    let joint = SDAG_CACHE.get_joint(unit)?.read()?;

    let mut text = String::from("");
    let from_addr = joint
        .unit
        .authors
        .iter()
        .map(|v| v.address.clone())
        .collect::<Vec<_>>();
    let mut to_address = vec![];
    for message in &joint.unit.messages {
        match message.payload {
            Some(Payload::Text(ref t)) => {
                text = t.to_owned();
            }
            Some(Payload::Payment(ref payment)) => {
                for output in &payment.outputs {
                    if !to_address.contains(&output.address) && !from_addr.contains(&output.address)
                    {
                        to_address.push(output.address.clone());
                    }
                }
            }
            _ => continue,
        }
    }

    Ok(light::Text {
        from_addr,
        to_addr: to_address,
        text: text,
        time: joint.unit.timestamp,
    })
}
