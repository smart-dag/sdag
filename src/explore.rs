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
    unit: String,
    best_parent: String,
    parents: Vec<String>,
    is_on_mc: bool,
    level: Level,
    is_stable: bool,
    author: Author,
    sequence: JointSequence,
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
        let mut mci = self.max_level;
        let stable_joint = loop {
            match SDAG_CACHE.get_mc_unit_hash(mci)? {
                None => {
                    // this mci level is not available, try a less one
                    mci -= 1;
                    continue;
                }

                Some(unit) => {
                    let joint = SDAG_CACHE.get_joint(&unit)?;

                    if joint.read()?.get_level() <= self.max_level {
                        // the first mc joint level less that less than max level
                        break joint;
                    }
                }
            }

            if mci == Level::ZERO {
                // not find any
                return Ok(());
            }
            mci -= 1;
        };

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
                if level_joints.contains(&display_unit) {
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

pub fn get_joints_in_range(min_level: Level, max_level: Level) -> Result<Vec<Vec<DisplayUnit>>> {
    let mut builder = ExploreBuilder::new(min_level, max_level);
    builder.append_unstable_units()?;
    builder.append_stable_units()?;

    Ok(builder.units)
}
