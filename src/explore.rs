use std::collections::VecDeque;

use cache::{JointData, SDAG_CACHE};
use error::Result;
use hashbrown::HashSet;
use joint::JointSequence;
use joint::Level;

#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize)]
pub enum Author {
    Normal,
    Witness(usize),
}

impl Author {
    fn get_author_type(joint: &JointData) -> Self {
        use my_witness::MY_WITNESSES;
        for (i, v) in MY_WITNESSES.iter().enumerate() {
            if v == &joint.unit.authors[0].address {
                return Author::Witness(i);
            }
        }

        Author::Normal
    }
}

/// if unit is on main chain, limci = mci
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayUnit {
    pub unit: String,
    pub best_parent: String,
    pub parents: Vec<String>,
    pub is_on_mc: bool,
    pub level: Level,
    pub is_stable: bool,
    pub author: Author,
    pub sequence: JointSequence,
}

impl PartialEq for DisplayUnit {
    fn eq(&self, other: &Self) -> bool {
        self.unit == other.unit
    }
}

impl<'a> From<&'a JointData> for DisplayUnit {
    fn from(joint: &'a JointData) -> Self {
        let props = joint.get_props();

        DisplayUnit {
            unit: joint.unit.unit.clone(),
            best_parent: joint.get_best_parent().key.to_string(),
            parents: joint.unit.parent_units.clone(),
            is_on_mc: props.mci == props.limci,
            is_stable: props.is_stable,
            level: props.level,
            sequence: props.sequence,
            author: Author::get_author_type(&joint),
        }
    }
}

// from min to max by default
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExploreBuilder {
    min_level: Level,
    max_level: Level,
    units: Vec<Vec<DisplayUnit>>,
}

impl ExploreBuilder {
    fn new(min_level: Level, max_level: Level) -> Self {
        ExploreBuilder {
            min_level,
            max_level,
            units: vec![vec![]; max_level - min_level + 1],
        }
    }

    fn append_unstable_units(&mut self) -> Result<()> {
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();

        for joint in SDAG_CACHE.get_all_free_joints() {
            queue.push_back(joint);
        }

        while let Some(joint) = queue.pop_front() {
            let joint_data = joint.read()?;

            let prop = joint_data.get_props();
            if prop.is_stable || prop.level < self.min_level {
                continue;
            }

            if prop.level <= self.max_level {
                let display_unit = DisplayUnit::from(&*joint_data);
                self.units[prop.level - self.min_level].push(display_unit)
            }

            for parent in joint_data.parents.iter() {
                if visited.insert(parent.key.clone()) {
                    queue.push_back(parent.clone());
                }
            }
        }

        Ok(())
    }

    fn append_stable_units(&mut self) -> Result<()> {
        // loop until the nearby mc joints
        let max_level = self.max_level + 10;
        let mut mci = max_level;
        let mut stable_joint = loop {
            match SDAG_CACHE.get_mc_unit_hash(mci)? {
                None => {
                    if mci == Level::ZERO {
                        // there is no stable unit yet
                        return Ok(());
                    }
                    // this mci level is not available, try from the last stable one
                    mci = ::main_chain::get_last_stable_mci();
                    continue;
                }

                Some(unit) => {
                    let joint = SDAG_CACHE.get_joint(&unit)?;

                    if joint.read()?.get_level() <= max_level {
                        // the first mc joint level less that less than max level
                        break joint;
                    }
                }
            }

            if mci == Level::ZERO {
                // not find any
                error!("not find stable units max_level={:?}", self.max_level);
                return Ok(());
            }
            mci -= 1;
        };

        // revert back one on mc if possible
        // still we can't make sure that current mc joint include all
        // joints whoes level is less than current mc joint level
        for child in stable_joint.read()?.children.iter() {
            let child_data = child.read()?;
            if child_data.is_on_main_chain() {
                stable_joint = (*child).clone();
                break;
            }
        }

        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        queue.push_back(stable_joint);

        while let Some(joint) = queue.pop_front() {
            let joint_data = joint.read()?;

            let level = joint_data.get_level();
            if level < self.min_level {
                continue;
            }

            if level <= self.max_level {
                let display_unit = DisplayUnit::from(&*joint_data);
                let level_joints = &mut self.units[level - self.min_level];
                if !level_joints.contains(&display_unit) {
                    level_joints.push(display_unit)
                }
            }

            for parent in joint_data.parents.iter() {
                if visited.insert(parent.key.clone()) {
                    queue.push_back(parent.clone());
                }
            }
        }

        Ok(())
    }
}

pub fn get_joints_by_level(min_level: Level, max_level: Level) -> Result<Vec<Vec<DisplayUnit>>> {
    if max_level - min_level > 300 {
        bail!(
            "get_joints_by_level range should be within 300, min={:?}, max={:?}",
            min_level,
            max_level
        );
    }
    let mut builder = ExploreBuilder::new(min_level, max_level);
    builder.append_unstable_units()?;
    builder.append_stable_units()?;

    Ok(builder.units)
}
