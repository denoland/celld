use siphasher::sip::SipHasher;

pub type ConsistentHash<T> = hashring::HashRing<T, HashBuilderWrapper>;

/// Create a new [`ConsistentHash`], optionally using a seeded hasher for
/// deterministic behavior (primarily for testing)
pub fn create_consistent_hash<T>(seed: Option<u64>) -> ConsistentHash<T> {
  let builder = match seed {
    Some(seed) => HashBuilderWrapper::Seeded(SipHasherKeys {
      key0: seed,
      key1: 42, // arbitrarily chosen value
    }),
    None => HashBuilderWrapper::Default(hashring::DefaultHashBuilder),
  };
  ConsistentHash::with_hasher(builder)
}

pub enum HashBuilderWrapper {
  Default(hashring::DefaultHashBuilder),
  Seeded(SipHasherKeys),
}

impl std::hash::BuildHasher for HashBuilderWrapper {
  type Hasher = SipHasher;

  fn build_hasher(&self) -> Self::Hasher {
    match self {
      HashBuilderWrapper::Default(builder) => builder.build_hasher(),
      HashBuilderWrapper::Seeded(keys) => {
        SipHasher::new_with_keys(keys.key0, keys.key1)
      }
    }
  }
}

#[derive(Clone, Debug)]
pub struct SipHasherKeys {
  key0: u64,
  key1: u64,
}
