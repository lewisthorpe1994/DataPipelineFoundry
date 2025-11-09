use connector_versioning::Version;

pub const DBZ_VERSION_3_0: Version = Version::new(3, 0);
pub const DBZ_VERSION_3_1: Version = Version::new(3, 1);
pub const DBZ_VERSION_3_2: Version = Version::new(3, 2);
pub const DBZ_VERSION_3_3: Version = Version::new(3, 3);

pub const DBZ_ALL_SUPPORTED_VERSIONS: &[Version] = &[
    DBZ_VERSION_3_0,
    DBZ_VERSION_3_1,
    DBZ_VERSION_3_2,
    DBZ_VERSION_3_3,
];
