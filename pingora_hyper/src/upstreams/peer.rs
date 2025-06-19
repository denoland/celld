pub struct HttpPeer {
  pub address: String,
  pub is_uds: bool,
}

impl HttpPeer {
  pub fn new(address: String, _tls: bool, _sni: String) -> Self {
    Self {
      address,
      is_uds: false,
    }
  }

  pub fn new_uds(
    path: &str,
    _tls: bool,
    _sni: String,
  ) -> Result<Self, Box<crate::error::Error>> {
    Ok(Self {
      address: path.to_string(),
      is_uds: true,
    })
  }
}
