use crate::fs::OkuFs;
use anyhow::anyhow;
use async_trait::async_trait;
use iroh::net::key::Signature;
use ucan::crypto::{JwtSignatureAlgorithm, KeyMaterial};

impl OkuFs {}

#[cfg_attr(target_arch="wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl KeyMaterial for OkuFs {
    fn get_jwt_algorithm_name(&self) -> String {
        JwtSignatureAlgorithm::EdDSA.to_string()
    }

    async fn get_did(&self) -> anyhow::Result<String> {
        let default_author_id = self.node.authors().default().await?;
        Ok(format!(
            "did:key:z{}",
            bs58::encode(default_author_id.as_bytes()).into_string()
        ))
    }

    async fn sign(&self, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        let default_author_id = self.node.authors().default().await?;
        let default_author =
            self.node
                .authors()
                .export(default_author_id)
                .await?
                .ok_or(anyhow!(
                    "Missing private key for default author ({}).",
                    self.get_did().await?
                ))?;
        Ok(default_author.sign(payload).to_vec())
    }

    async fn verify(&self, payload: &[u8], signature: &[u8]) -> anyhow::Result<()> {
        let default_author_id = self.node.authors().default().await?;
        let public_key = default_author_id.into_public_key()?;
        Ok(public_key.verify(payload, &Signature::from_slice(signature)?)?)
    }
}
