//! Redpanda events' topic.

pub mod auth_topics {
    pub const ORGS_REGISTRATION: &str = "orgs_topic.registration";
    pub const ORGS_LOGIN: &str = "orgs_topic.org_login";

    pub const USER_REGISTRATION: &str = "user_topic.registration";
    pub const USER_LOGIN: &str = "user_topic.user_login";
}

pub mod access_topics {
    pub const ORGS_UPDATE: &str = "orgs_topic.update";
    pub const ORGS_REMOVE: &str = "orgs_topic.remove";

    pub const USER_UPDATE: &str = "user_topic.update";
    pub const USER_REMOVE: &str = "user_topic.remove";
}
