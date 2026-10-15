//! Data models returned by OneBot actions (e.g. `get_group_member_info`).

use serde::{Deserialize, Serialize};

/// `get_login_info` / `get_stranger_info` result.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_id: i64,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub sex: String,
    #[serde(default)]
    pub age: i32,
}

/// `get_group_info` result.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct GroupInfo {
    pub group_id: i64,
    #[serde(default)]
    pub group_name: String,
    #[serde(default)]
    pub member_count: i32,
    #[serde(default)]
    pub max_member_count: i32,
}

/// `get_group_member_info` result.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct GroupMemberInfo {
    pub group_id: i64,
    pub user_id: i64,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub card: String,
    #[serde(default)]
    pub sex: String,
    #[serde(default)]
    pub age: i32,
    /// `owner` | `admin` | `member`
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub title: String,
}

impl GroupMemberInfo {
    pub fn is_admin(&self) -> bool {
        self.role == "owner" || self.role == "admin"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_group_member_info() {
        let info: GroupMemberInfo = serde_json::from_str(
            r#"{"group_id":30001,"user_id":20001,"nickname":"Alice","card":"","sex":"female","age":18,"role":"admin","title":""}"#,
        )
        .unwrap();
        assert_eq!(info.user_id, 20001);
        assert!(info.is_admin());
    }
}
