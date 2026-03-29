use serde::{Deserialize, Serialize};

use crate::types::LockMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DmlKind {
    #[default]
    Select,
    SelectForUpdate,
    Insert,
    Update,
    Delete,
}

impl DmlKind {
    pub fn lock_mode(self) -> LockMode {
        match self {
            Self::Select => LockMode::AccessShare,
            Self::SelectForUpdate => LockMode::RowShare,
            Self::Insert | Self::Update | Self::Delete => LockMode::RowExclusive,
        }
    }
}

impl std::fmt::Display for DmlKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Select => write!(f, "SELECT"),
            Self::SelectForUpdate => write!(f, "SELECT FOR UPDATE"),
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
        }
    }
}

const CONFLICT: [[bool; 8]; 8] = [
    //  AS     RS     RE     SUE    S      SRE    E      AE
    [false, false, false, false, false, false, false, true ],  // AccessShare
    [false, false, false, false, false, false, true,  true ],  // RowShare
    [false, false, false, false, true,  true,  true,  true ],  // RowExclusive
    [false, false, false, true,  true,  true,  true,  true ],  // ShareUpdateExclusive
    [false, false, true,  true,  false, true,  true,  true ],  // Share
    [false, false, true,  true,  true,  true,  true,  true ],  // ShareRowExclusive
    [false, true,  true,  true,  true,  true,  true,  true ],  // Exclusive
    [true,  true,  true,  true,  true,  true,  true,  true ],  // AccessExclusive
];

fn lock_index(mode: LockMode) -> usize {
    match mode {
        LockMode::AccessShare => 0,
        LockMode::RowShare => 1,
        LockMode::RowExclusive => 2,
        LockMode::ShareUpdateExclusive => 3,
        LockMode::Share => 4,
        LockMode::ShareRowExclusive => 5,
        LockMode::Exclusive => 6,
        LockMode::AccessExclusive => 7,
    }
}

pub fn conflicts(requested: LockMode, held: LockMode) -> bool {
    CONFLICT[lock_index(requested)][lock_index(held)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_share_only_conflicts_with_access_exclusive() {
        assert!(!conflicts(LockMode::AccessShare, LockMode::AccessShare));
        assert!(!conflicts(LockMode::AccessShare, LockMode::RowShare));
        assert!(!conflicts(LockMode::AccessShare, LockMode::RowExclusive));
        assert!(!conflicts(LockMode::AccessShare, LockMode::ShareUpdateExclusive));
        assert!(!conflicts(LockMode::AccessShare, LockMode::Share));
        assert!(!conflicts(LockMode::AccessShare, LockMode::ShareRowExclusive));
        assert!(!conflicts(LockMode::AccessShare, LockMode::Exclusive));
        assert!(conflicts(LockMode::AccessShare, LockMode::AccessExclusive));
    }

    #[test]
    fn access_exclusive_conflicts_with_everything() {
        assert!(conflicts(LockMode::AccessExclusive, LockMode::AccessShare));
        assert!(conflicts(LockMode::AccessExclusive, LockMode::RowShare));
        assert!(conflicts(LockMode::AccessExclusive, LockMode::RowExclusive));
        assert!(conflicts(LockMode::AccessExclusive, LockMode::ShareUpdateExclusive));
        assert!(conflicts(LockMode::AccessExclusive, LockMode::Share));
        assert!(conflicts(LockMode::AccessExclusive, LockMode::ShareRowExclusive));
        assert!(conflicts(LockMode::AccessExclusive, LockMode::Exclusive));
        assert!(conflicts(LockMode::AccessExclusive, LockMode::AccessExclusive));
    }

    #[test]
    fn matrix_is_symmetric() {
        let modes = [
            LockMode::AccessShare,
            LockMode::RowShare,
            LockMode::RowExclusive,
            LockMode::ShareUpdateExclusive,
            LockMode::Share,
            LockMode::ShareRowExclusive,
            LockMode::Exclusive,
            LockMode::AccessExclusive,
        ];
        for &a in &modes {
            for &b in &modes {
                assert_eq!(
                    conflicts(a, b),
                    conflicts(b, a),
                    "conflict matrix is not symmetric for {a} vs {b}"
                );
            }
        }
    }

    #[test]
    fn row_exclusive_conflicts_with_share() {
        assert!(conflicts(LockMode::RowExclusive, LockMode::Share));
        assert!(conflicts(LockMode::Share, LockMode::RowExclusive));
    }

    #[test]
    fn share_does_not_conflict_with_share() {
        assert!(!conflicts(LockMode::Share, LockMode::Share));
    }

    #[test]
    fn dml_lock_modes() {
        assert_eq!(DmlKind::Select.lock_mode(), LockMode::AccessShare);
        assert_eq!(DmlKind::SelectForUpdate.lock_mode(), LockMode::RowShare);
        assert_eq!(DmlKind::Insert.lock_mode(), LockMode::RowExclusive);
        assert_eq!(DmlKind::Update.lock_mode(), LockMode::RowExclusive);
        assert_eq!(DmlKind::Delete.lock_mode(), LockMode::RowExclusive);
    }
}
