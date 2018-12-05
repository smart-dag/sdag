use cache::SDAG_CACHE;
use config;
use error::Result;

#[derive(Serialize, Deserialize)]
pub struct ParentsAndLastBall {
    pub parents: Vec<String>,
    pub last_ball: String,
}

pub fn pick_parents_and_last_ball(_address: &str) -> Result<ParentsAndLastBall> {
    let free_joints = SDAG_CACHE.get_free_joints()?;
    let last_stable_joint = ::main_chain::get_last_stable_joint();

    for group in free_joints.chunks(config::MAX_PARENT_PER_UNIT) {
        if ::main_chain::is_stable_in_later_joints(&last_stable_joint, &group)? {
            let mut parents = group.iter().map(|p| p.key.to_string()).collect::<Vec<_>>();
            parents.sort();

            return Ok(ParentsAndLastBall {
                parents,
                last_ball: last_stable_joint.key.to_string(),
            });
        }
    }

    bail!("fail to choose parents")
}
