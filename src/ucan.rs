use crate::fs::OkuFs;
use anyhow::anyhow;
use async_trait::async_trait;
use iroh::{
    docs::{Author, AuthorPublicKey},
    net::key::Signature,
};
use ucan::builder::UcanBuilder;
use ucan::crypto::did::DidParser;
use ucan::crypto::did::ED25519_MAGIC_BYTES;
use ucan::crypto::{JwtSignatureAlgorithm, KeyMaterial};
use ucan::Ucan;

#[derive(Clone)]
/// A representation of a node's author keypair for UCAN creation & verification.
pub struct AuthorKeyMaterial(pub AuthorPublicKey, pub Option<Author>);

fn bytes_to_author_key(bytes: Vec<u8>) -> anyhow::Result<Box<dyn KeyMaterial>> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|e| anyhow!("{:#?}", e))?;
    Ok(Box::new(AuthorKeyMaterial(
        AuthorPublicKey::from_bytes(&bytes)?,
        None,
    )))
}

impl OkuFs {
    /// Retrieve the node's default author keypair for UCAN creation & verification.
    pub async fn get_key_material(&self) -> anyhow::Result<AuthorKeyMaterial> {
        let default_author = self.get_author().await?;
        Ok(AuthorKeyMaterial(
            default_author.id().into_public_key()?,
            Some(default_author),
        ))
    }

    /// Create an encoded UCAN token where the issuer is the current node's default author.
    ///
    /// # Arguments
    ///
    /// `audience_did` - The DID of the intended audience of the token.
    ///
    /// # Returns
    ///
    /// An encoded UCAN token issued by the current node's default author to the given audience.
    pub async fn generate_token<'a, K: KeyMaterial>(
        &self,
        audience_did: &'a str,
    ) -> Result<String, anyhow::Error> {
        UcanBuilder::default()
            .issued_by(&self.get_key_material().await?)
            .for_audience(audience_did)
            .with_lifetime(60)
            .build()?
            .sign()
            .await?
            .encode()
    }

    /// Verify an encoded UCAN token.
    ///
    /// # Arguments
    ///
    /// * `encoded_token` - An encoded UCAN token.
    pub async fn verify_token(&self, encoded_token: String) -> anyhow::Result<()> {
        let mut did_parser = DidParser::new(&[(ED25519_MAGIC_BYTES, bytes_to_author_key)]);
        let ucan = Ucan::try_from(encoded_token)?;
        ucan.check_signature(&mut did_parser).await
    }
}

#[cfg_attr(target_arch="wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl KeyMaterial for AuthorKeyMaterial {
    fn get_jwt_algorithm_name(&self) -> String {
        JwtSignatureAlgorithm::EdDSA.to_string()
    }

    async fn get_did(&self) -> anyhow::Result<String> {
        Ok(format!(
            "did:key:z{}",
            bs58::encode(self.0.as_bytes()).into_string()
        ))
    }

    async fn sign(&self, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        match &self.1 {
            Some(author) => Ok(author.sign(payload).to_vec()),
            None => Err(anyhow!("Missing private key for author.")),
        }
    }

    async fn verify(&self, payload: &[u8], signature: &[u8]) -> anyhow::Result<()> {
        Ok(self.0.verify(payload, &Signature::from_slice(signature)?)?)
    }
}
