use std::cmp;

use may::sync::Mutex;
use spec::*;

lazy_static! {
    pub static ref WRITER_MUTEX: Mutex<()> = Mutex::new(());
}

//---------------------------------------------------------------------------------------
// Level
//---------------------------------------------------------------------------------------

/// special isize with default level set to -1 which is less than any valid usize
#[derive(Debug, Clone, Copy, Eq, Serialize, Deserialize)]
pub struct Level(isize);

const INVALID_LEVEL: isize = -2;
const MINIMUM_LEVEL: isize = -1;

impl std::hash::Hash for Level {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_isize(self.0);
    }
}

impl PartialOrd for Level {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self.0 == INVALID_LEVEL || other.0 == INVALID_LEVEL {
            return None;
        }
        Some(self.0.cmp(&other.0))
    }
}

impl PartialEq for Level {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if self.0 == INVALID_LEVEL || other.0 == INVALID_LEVEL {
            return false;
        }
        self.0 == other.0
    }
}

impl ::std::ops::Add<usize> for Level {
    type Output = Level;
    // Note: default + 1 = 0
    #[inline]
    fn add(self, rhs: usize) -> Level {
        Level(self.0 + rhs as isize)
    }
}

impl ::std::ops::Sub for Level {
    type Output = usize;
    #[inline]
    fn sub(self, rhs: Self) -> usize {
        assert!(
            self.is_valid() && rhs.is_valid() && self.0 >= rhs.0,
            "Level sub"
        );
        (self.0 - rhs.0) as usize
    }
}

impl ::std::ops::AddAssign<usize> for Level {
    #[inline]
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as isize
    }
}

impl ::std::ops::SubAssign<usize> for Level {
    #[inline]
    fn sub_assign(&mut self, rhs: usize) {
        assert!(
            self.is_valid() && rhs as isize <= self.0,
            "Level sub_assign"
        );
        self.0 -= rhs as isize
    }
}

impl Level {
    pub const ZERO: Level = Level(0);
    pub const INVALID: Level = Level(INVALID_LEVEL);
    // minimum + 1 = 0
    pub const MINIMUM: Level = Level(MINIMUM_LEVEL);

    pub fn new(l: usize) -> Self {
        Level(l as isize)
    }

    pub fn value(self) -> usize {
        // assert!(self.0 >= 0);
        self.0 as usize
    }

    /// contains a valid value
    #[inline]
    pub fn is_valid(self) -> bool {
        self.0 >= 0
    }
}

impl From<usize> for Level {
    fn from(v: usize) -> Self {
        Level(v as isize)
    }
}

impl Default for Level {
    fn default() -> Self {
        Level(INVALID_LEVEL)
    }
}

//---------------------------------------------------------------------------------------
// JointSequence
//---------------------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
// | non-serial | business | state         |
// |------------|----------|---------------|
// | good       | good     | Good          |
// | good       | bad      | TempBad       |
// | bad        | bad      | NonserialBad  |
// | good       | nocommit | NoCommisssion |

pub enum JointSequence {
    Good,
    NonserialBad,
    TempBad,
    FinalBad,
    NoCommission,
}

impl JointSequence {
    pub fn is_temp_bad(self) -> bool {
        self == JointSequence::NonserialBad || self == JointSequence::TempBad
    }
}

//---------------------------------------------------------------------------------------
// JointProperty
//---------------------------------------------------------------------------------------
#[derive(Debug, Serialize, Deserialize)]
pub struct JointProperty {
    pub level: Level,
    pub best_parent_unit: String,
    // witnessed level
    pub wl: Level,
    // min witnessed level
    pub min_wl: Level,
    // if the joint witnessed_level is bigger than it's best parent's witnessed level
    pub is_wl_increased: bool,
    pub is_min_wl_increased: bool,
    // when it's usize::MAX means no value
    pub mci: Level,
    pub limci: Level,
    pub sub_mci: Level,
    pub is_stable: bool,
    pub sequence: JointSequence,
    pub create_time: u64,
    #[serde(skip)]
    pub prev_stable_self_unit: Option<String>,
    #[serde(skip)]
    pub related_units: Vec<String>,
    #[serde(skip)]
    pub balance: u64,
    // 0x00(init), 0x11(validate ok), 0x10(re check)
    #[serde(skip)]
    pub validate_authors_state: u8,
}

impl Default for JointProperty {
    fn default() -> Self {
        JointProperty {
            level: Default::default(),
            wl: Default::default(),
            min_wl: Default::default(),
            mci: Default::default(),
            limci: Default::default(),
            sub_mci: Default::default(),
            is_stable: false,
            is_wl_increased: false,
            is_min_wl_increased: false,
            sequence: JointSequence::TempBad,
            best_parent_unit: String::new(),
            create_time: crate::time::now(),
            prev_stable_self_unit: None,
            related_units: Vec::new(),
            balance: 0,
            validate_authors_state: 0x00,
        }
    }
}

//---------------------------------------------------------------------------------------
// Joint
//---------------------------------------------------------------------------------------
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Joint {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ball: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub skiplist_units: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsigned: Option<bool>,
    pub unit: Unit,
}

#[test]
fn test_write() {
    let unit = Unit {
        alt: String::from("1"),
        authors: Vec::new(),
        content_hash: None,
        earned_headers_commission_recipients: Vec::new(),
        headers_commission: None,
        last_ball: Some(String::from("oiIA6Y+87fk6/QyrbOlwqsQ/LLr82Rcuzcr1G/GoHlA=")),
        last_ball_unit: Some(String::from("vxrlKyY517Z+BGMNG35ExiQsYv3ncp/KU414SqXKXTk=")),
        main_chain_index: None,
        messages: Vec::new(),
        parent_units: vec![
            "uPbobEuZL+FY1ujTNiYZnM9lgC3xysxuDIpSbvnmbac=".into(),
            "vxrlKyY517Z+BGMNG35ExiQsYv3ncp/KU414SqXKXTk=".into(),
        ],
        payload_commission: None,
        timestamp: None,
        unit: String::from("5CYeTTa4VQxgF4b1Tn33NBlKilJadddwBMLvtp1HIus="),
        version: String::from("1.0"),
        witnesses: Vec::new(),
        witness_list_unit: Some(String::from("MtzrZeOHHjqVZheuLylf0DX7zhp10nBsQX5e/+cA3PQ=")),
        ..Default::default()
    };
    let joint = Joint {
        ball: None,
        skiplist_units: Vec::new(),
        unit,
        unsigned: None,
    };
    let parents_set = joint
        .unit
        .parent_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    println!("{}", parents_set);
    // joint.save().unwrap();
}
