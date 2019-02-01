use std::cmp;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use cache::{CachedJoint, SDAG_CACHE};
use error::Result;
use failure::ResultExt;
use joint::{Joint, JointProperty, JointSequence, Level};
use kv_store::{LoadFromKv, KV_STORE};
use may::sync::{RwLock, SyncFlag};
use rcu_cell::RcuReader;
use utils::{AppendList, AppendListExt};

//---------------------------------------------------------------------------------------
// UnitProps
//---------------------------------------------------------------------------------------
#[derive(Debug, Serialize, Deserialize)]
pub struct UnitProps {
    pub key: String,
    pub level: Level,
    pub mci: Level,
    pub limci: Level,
    pub wl: Level,
    pub is_stable: bool,
    pub sequence: JointSequence,
}

impl UnitProps {
    // return true if not included for sure
    // check if other is not ancestor of self
    #[inline]
    fn not_included(&self, other: &Self) -> bool {
        // fast not include conditions
        // 1) self.level <= earlier_unit.level;
        // 2) self.wl < earlier_unit.wl;
        // 3) self.mci < earlier_unit.mci;
        // 4) self.limci < earlier_unit.limci

        // 5) self.is_stable && !earlier_unit.is_stable; NOT TRUE for dynamic situation! (#208)
        /* self is one of the later units, other is ancestor unit.
         * but other's prop is ready earlier than self's prop, earlier may got unstable,
         * and later may got stable! thus cause the branch returns wrong value! */

        self.level <= other.level
            || self.wl < other.wl
            || self.mci < other.mci
            || self.limci < other.limci
    }

    /// return true if any given joint include or equal to the current one
    pub fn is_ancestor<'a, I: IntoIterator<Item = &'a CachedJoint>>(
        &self,
        later_joints: I,
    ) -> Result<bool> {
        let mut joints = Vec::new();
        let mut visited = HashSet::new();

        for joint in later_joints {
            if visited.insert(joint.key.clone()) {
                joints.push(joint.read()?);
            }
        }

        while let Some(joint) = joints.pop() {
            let props = joint.get_props();

            // fast include detection
            match PartialOrd::partial_cmp(&props, self) {
                Some(cmp::Ordering::Equal) => return Ok(true),
                Some(cmp::Ordering::Greater) => return Ok(true),
                Some(cmp::Ordering::Less) => {
                    warn!("is_ancestor detect self as descendant!");
                    continue;
                }
                None => {}
            }

            // fast not include detection
            if props.not_included(self) {
                continue;
            }

            // still need to compare parents
            for parent in joint.parents.iter() {
                if visited.insert(parent.key.clone()) {
                    joints.push(parent.read()?);
                }
            }
        }

        Ok(false)
    }
}

// compare if two joint are included
// C > P || P < C : if P is C's ancestor, C is P's descendant
// C == P : if they are equal
// None if can't detect the relationship need further cmp

// impl Ord for UnitProps (must be stable) (Note: add sub_mci)
impl PartialOrd for UnitProps {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self == other {
            return Some(cmp::Ordering::Equal);
        }

        if other.limci >= self.mci {
            return Some(cmp::Ordering::Less);
        }

        if self.limci >= other.mci {
            return Some(cmp::Ordering::Greater);
        }

        // we can't know if the two units include each other
        None
    }
}

impl PartialEq for UnitProps {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

//---------------------------------------------------------------------------------------
// JointData
//---------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct JointData {
    pub parents: Arc<AppendList<CachedJoint>>,
    pub children: Arc<AppendListExt<CachedJoint>>,
    best_parent: Arc<AppendList<CachedJoint>>,
    valid_parent_num: Arc<AtomicUsize>,
    unhandled_refs: Arc<AtomicUsize>,
    joint: Joint,
    stable_flag: Arc<SyncFlag>,
    peer_id: Option<Arc<String>>,
    props: Arc<RwLock<JointProperty>>,
}

// impl the property access
impl JointData {
    /// get all properties
    pub fn get_props(&self) -> UnitProps {
        let props = self.props.read().unwrap();
        UnitProps {
            key: self.unit.unit.clone(),
            level: props.level,
            mci: props.mci,
            limci: props.limci,
            wl: props.wl,
            is_stable: props.is_stable,
            sequence: props.sequence,
        }
    }

    pub fn get_all_props(&self) -> &RwLock<JointProperty> {
        &*self.props
    }

    pub fn get_level(&self) -> Level {
        self.props.read().unwrap().level
    }

    pub fn set_level(&self, level: Level) {
        self.props.write().unwrap().level = level;
    }

    pub fn get_limci(&self) -> Level {
        self.props.read().unwrap().limci
    }

    pub fn set_limci(&self, limci: Level) {
        self.props.write().unwrap().limci = limci;
    }

    pub fn get_mci(&self) -> Level {
        self.props.read().unwrap().mci
    }

    pub fn set_mci(&self, mci: Level) {
        self.props.write().unwrap().mci = mci;
        // FIXME: mci should be on the property,
        // but the client is still using it in Unit
        unsafe {
            let mci_ptr = &self.joint.unit.main_chain_index as *const _ as *mut Option<u32>;
            mci_ptr.replace(Some(mci.value() as u32));
        }
        debug!("Joint {} mci is set {:?}", self.unit.unit, self.props);
    }

    pub fn get_sub_mci(&self) -> Level {
        self.props.read().unwrap().sub_mci
    }

    pub fn set_sub_mci(&self, sub_mci: Level) {
        self.props.write().unwrap().sub_mci = sub_mci;
    }

    pub fn get_wl(&self) -> Level {
        self.props.read().unwrap().wl
    }

    pub fn set_wl(&self, witnessed_level: Level) {
        self.props.write().unwrap().wl = witnessed_level;
    }

    pub fn get_min_wl(&self) -> Level {
        self.props.read().unwrap().min_wl
    }

    pub fn set_min_wl(&self, min_witnessed_level: Level) {
        self.props.write().unwrap().min_wl = min_witnessed_level;
    }

    pub fn get_sequence(&self) -> JointSequence {
        self.props.read().unwrap().sequence
    }

    pub fn set_sequence(&self, sequence: JointSequence) {
        match sequence {
            JointSequence::Good | JointSequence::TempBad => {}
            _ => warn!("unit={}, set sequence={:?}", self.unit.unit, sequence),
        }
        self.props.write().unwrap().sequence = sequence;
    }

    /// is_on_main_chain can be calculated out by other props
    /// thus there is no set API for it
    pub fn is_on_main_chain(&self) -> bool {
        let props = self.props.read().unwrap();
        props.mci == props.limci
    }

    pub fn is_stable(&self) -> bool {
        self.stable_flag.is_fired()
    }

    pub fn wait_stable(&self, waiter: &str) {
        use std::time::Duration;

        while !self.stable_flag.wait_timeout(Duration::from_secs(1)) {
            error!(
                "wait stable timeout! unit={}, waiter={}",
                self.unit.unit, waiter
            );
        }
    }

    pub fn set_stable(&self) {
        self.props.write().unwrap().is_stable = true;
        self.stable_flag.fire();
        debug!("Joint {} is stable {:?}", self.unit.unit, self.props);
    }

    pub fn is_wl_increased(&self) -> bool {
        self.props.read().unwrap().is_wl_increased
    }

    pub fn set_wl_increased(&self) {
        self.props.write().unwrap().is_wl_increased = true;
    }

    pub fn is_min_wl_increased(&self) -> bool {
        self.props.read().unwrap().is_min_wl_increased
    }

    pub fn set_min_wl_increased(&self) {
        self.props.write().unwrap().is_min_wl_increased = true;
    }

    pub fn get_best_parent(&self) -> CachedJoint {
        match self.best_parent.iter().next() {
            // genesis has no parents, so just return a fake one
            None => {
                use rcu_cell::RcuCell;
                let mut joint_data = self.make_copy();
                // need to use it's own property, not the shared one
                joint_data.props = Default::default();

                joint_data.set_mci(Level::MINIMUM);
                joint_data.set_limci(Level::MINIMUM);
                joint_data.set_level(Level::MINIMUM);
                joint_data.set_wl(Level::ZERO);
                // trigger genesis increase min_wl_increased
                joint_data.set_min_wl(Level::MINIMUM);
                // clear all it's children to break visit loop
                joint_data.children = Default::default();

                CachedJoint {
                    key: Arc::new(self.unit.unit.to_owned()),
                    data: RcuCell::new(Some(joint_data)),
                }
            }
            Some(p) => p.clone(),
        }
    }

    pub fn set_best_parent(&self, parent: CachedJoint) {
        if let Some(j) = self.best_parent.iter().next() {
            panic!(
                "unit={} best parent is already set {}",
                self.unit.unit, j.key
            );
        }
        self.props.write().unwrap().best_parent_unit = parent.key.to_string();
        self.best_parent.append(parent);
    }

    // to make sure we only ready once, after return true,
    // the next time call would always return fasle
    pub fn is_ready(&self) -> bool {
        let len = self.joint.unit.parent_units.len();
        // only ready once!
        len == self
            .valid_parent_num
            .compare_and_swap(len, 0, Ordering::AcqRel)
    }

    pub fn get_missing_parents<'a>(&'a self) -> Result<impl Iterator<Item = &'a String>> {
        let mut missing_parents: HashSet<_> = self.unit.parent_units.iter().collect();
        for parent in self.parents.iter() {
            missing_parents.remove(&parent.read()?.unit.unit);
        }
        Ok(missing_parents.into_iter())
    }

    pub fn add_parent(&self, parent: CachedJoint) {
        self.valid_parent_num.fetch_add(1, Ordering::Release);
        self.parents.append(parent);
    }

    pub fn add_child(&self, child: CachedJoint) {
        self.children.append(child);
        // child remove from unhandled to normal
        self.unhandled_refs.fetch_sub(1, Ordering::Release);
    }

    pub fn inc_unhandled_refs(&self) {
        self.unhandled_refs.fetch_add(1, Ordering::Release);
    }

    pub fn dec_unhandled_refs(&self) {
        self.unhandled_refs.fetch_sub(1, Ordering::Release);
    }

    pub fn has_unhandled_refs(&self) -> bool {
        self.unhandled_refs.load(Ordering::Acquire) != 0
    }

    pub fn is_free(&self) -> bool {
        self.children.is_empty()
    }

    pub fn get_create_time(&self) -> u64 {
        self.props.read().unwrap().create_time
    }

    pub fn get_peer_id(&self) -> Option<Arc<String>> {
        self.peer_id.clone()
    }

    pub fn set_stable_prev_self_unit(&self, unit: String) {
        self.props.write().unwrap().prev_stable_self_unit = Some(unit);
    }

    pub fn get_stable_prev_self_unit(&self) -> Option<String> {
        self.props.read().unwrap().prev_stable_self_unit.clone()
    }

    pub fn set_related_units(&self, units: Vec<String>) {
        self.props.write().unwrap().related_units = units;
    }

    pub fn get_related_units(&self) -> Vec<String> {
        self.props.read().unwrap().related_units.clone()
    }

    pub fn set_balance(&self, balance: u64) {
        self.props.write().unwrap().balance = balance;
    }

    pub fn get_balance(&self) -> u64 {
        self.props.read().unwrap().balance
    }

    pub fn set_validate_authors_state(&self, state: u8) {
        self.props.write().unwrap().validate_authors_state = state;
    }

    pub fn get_validate_authors_state(&self) -> u8 {
        self.props.read().unwrap().validate_authors_state
    }
}

impl JointData {
    fn calc_level(&self) -> Result<()> {
        let mut max_parent_level = Level::MINIMUM;
        for parent in self.parents.iter() {
            let level = parent.read().context("calc_level")?.get_level();
            assert_eq!(level.is_valid(), true);
            if max_parent_level < level {
                max_parent_level = level;
            }
        }
        // Note: the genesis unit not has no parents
        // minimum + 1 = 0
        self.set_level(max_parent_level + 1);
        Ok(())
    }

    fn calc_best_parent(&self) -> Result<()> {
        use main_chain::find_best_joint;
        if let Some(best_parent) = find_best_joint(self.parents.iter())? {
            let best_parent = SDAG_CACHE.get_joint(&best_parent.unit.unit)?;
            self.set_best_parent(best_parent);
        }
        Ok(())
    }

    fn calc_witnessed_level(&self) -> Result<()> {
        let joint = self.find_relative_stable_joint()?.read()?;
        let wl = joint.get_level();
        self.set_wl(wl);

        let min_wl = joint.get_wl();
        self.set_min_wl(min_wl);

        let bp = self.get_best_parent().read()?;

        if wl > bp.get_wl() {
            self.set_wl_increased();
        }

        if min_wl > bp.get_min_wl() {
            self.set_min_wl_increased();
        }

        Ok(())
    }

    /// cacl and update the basic joint property after all parents got ready
    pub fn cacl_static_props(&self) -> Result<()> {
        self.calc_level()?;
        self.calc_best_parent()?;
        self.calc_witnessed_level()?;

        info!(
            "After Calc unit = {} static props: {:?}",
            self.unit.unit, self.props
        );
        Ok(())
    }

    /// find the relative stable joint along the best parent
    pub fn find_relative_stable_joint(&self) -> Result<CachedJoint> {
        use my_witness::MY_WITNESSES;
        let mut valid_witnesses = Vec::new();

        let mut best_parent = self.get_best_parent();
        loop {
            let joint = best_parent.read().context("find_relative_stable_joint")?;
            for author in &joint.unit.authors {
                if valid_witnesses.contains(&author.address) {
                    continue;
                }
                if MY_WITNESSES.contains(&author.address) {
                    valid_witnesses.push(author.address.to_owned());
                }
            }

            if valid_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES {
                // genesis would return itself since it has all witnesses
                return Ok(best_parent);
            }

            best_parent = joint.get_best_parent();
        }
    }

    pub fn get_last_ball_joint(&self) -> Result<RcuReader<JointData>> {
        // only genesis has no last ball unit
        match self.unit.last_ball_unit {
            Some(ref unit) => SDAG_CACHE.get_joint(unit)?.read(),
            None => SDAG_CACHE.get_joint(&::spec::GENESIS_UNIT)?.read(),
        }
    }

    #[inline]
    pub fn update_joint(&mut self, joint: Joint) {
        self.joint = joint
    }

    #[inline]
    pub(super) fn make_copy(&self) -> Self {
        JointData {
            parents: self.parents.clone(),
            children: self.children.clone(),
            best_parent: self.best_parent.clone(),
            valid_parent_num: self.valid_parent_num.clone(),
            unhandled_refs: self.unhandled_refs.clone(),
            stable_flag: self.stable_flag.clone(),
            peer_id: self.peer_id.clone(),
            joint: self.joint.clone(),
            props: self.props.clone(),
        }
    }

    /// return true if self is more precedence than other
    pub fn is_precedence_than(&self, other: &Self) -> bool {
        let self_prop = self.props.read().unwrap();
        let other_prop = other.props.read().unwrap();

        if self_prop.wl != other_prop.wl {
            return self_prop.wl > other_prop.wl;
        }

        if self_prop.level != other_prop.level {
            return self_prop.level < other_prop.level;
        }

        self.unit.unit < other.unit.unit
    }
}

impl JointData {
    pub fn from_joint(joint: Joint, peer_id: Option<Arc<String>>) -> Self {
        JointData {
            joint,
            parents: Default::default(),
            best_parent: Default::default(),
            children: Default::default(),
            props: Default::default(),
            valid_parent_num: Default::default(),
            unhandled_refs: Default::default(),
            stable_flag: Arc::new(SyncFlag::new()),
            peer_id,
        }
    }
}

impl ::std::ops::Deref for JointData {
    type Target = Joint;
    fn deref(&self) -> &Joint {
        &self.joint
    }
}

impl LoadFromKv<String> for JointData {
    fn load_from_kv<T: ::std::borrow::Borrow<String>>(key: &T) -> Result<Self> {
        let key = key.borrow();
        // load joint
        let joint = KV_STORE.read_joint(key)?;

        // prepare parents, must be already exist
        let valid_parent_num = joint.unit.parent_units.len();
        let parents = joint
            .unit
            .parent_units
            .iter()
            .map(|key| SDAG_CACHE.get_joint_or_none(key))
            .collect();

        // prepare children, must be already exist
        let children = KV_STORE
            .read_joint_children(key)?
            .iter()
            .map(|key| SDAG_CACHE.get_joint_or_none(key))
            .collect::<AppendListExt<_>>();

        let props = KV_STORE.read_joint_property(key)?;

        let best_parent = AppendList::new();
        best_parent.append(SDAG_CACHE.get_joint_or_none(&props.best_parent_unit));

        let stable_flag = Arc::new(SyncFlag::new());
        if props.is_stable {
            stable_flag.fire();
        }

        Ok(JointData {
            joint,
            parents: Arc::new(parents),
            children: Arc::new(children),
            best_parent: Arc::new(best_parent),
            stable_flag,
            props: Arc::new(RwLock::new(props)),
            valid_parent_num: Arc::new(AtomicUsize::new(valid_parent_num)),
            unhandled_refs: Arc::new(AtomicUsize::new(0)),
            peer_id: None,
        })
    }

    fn save_to_kv<T: ::std::borrow::Borrow<String>>(&self, key: &T) -> Result<()> {
        // self is the data, need to save to kv
        let key = key.borrow();
        KV_STORE.save_joint(key, &self.joint)?;
        KV_STORE.save_joint_children(
            key,
            self.children
                .iter()
                .map(|c| c.key.as_ref().to_owned())
                .collect::<Vec<_>>(),
        )?;
        KV_STORE.save_joint_property(key, &self.props.read().unwrap())
    }
}

// compare if two joint are included
// C > P || P < C : if P is C's ancestor, C is P's descendant
// C == P : if they are equal
// None if can't detect the relationship need further cmp
impl PartialOrd for JointData {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self == other {
            return Some(cmp::Ordering::Equal);
        }
        // free joints are sure not contains each other
        if self.is_free() && other.is_free() {
            return None;
        }

        let props_a = self.get_props();
        let props_b = other.get_props();

        // fast include detection
        match PartialOrd::partial_cmp(&props_a, &props_b) {
            Some(cmp::Ordering::Equal) => unreachable!(), // already checked at beginning
            Some(cmp::Ordering::Less) => return Some(cmp::Ordering::Less),
            Some(cmp::Ordering::Greater) => return Some(cmp::Ordering::Greater),
            None => {}
        }

        match PartialOrd::partial_cmp(&props_a.level, &props_b.level) {
            // when Level are equal they must not connect
            Some(cmp::Ordering::Equal) => return None,
            Some(cmp::Ordering::Greater) => {
                // fast not include detection
                if props_a.not_included(&props_b) {
                    return None;
                }
                // Note: for better interface we use expect here
                // or we have to return Result<Option<Ordering>>
                // here we need further compare by parents
                if props_b
                    .is_ancestor(self.parents.iter())
                    .expect("is_ancestor failed")
                {
                    return Some(cmp::Ordering::Greater);
                }
            }
            Some(cmp::Ordering::Less) => {
                if props_b.not_included(&props_a) {
                    return None;
                }

                if props_a
                    .is_ancestor(other.parents.iter())
                    .expect("is_ancestor failed")
                {
                    return Some(cmp::Ordering::Less);
                }
            }
            // None and Equal are not possible here
            // None means joint level is not set
            _ => unreachable!("invalid level can't compared!"),
        }

        None
    }
}

impl PartialEq for JointData {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.unit.unit == other.unit.unit
    }
}
