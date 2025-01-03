use super::core::{home_replica_filters, EmbeddingModality};
use crate::fs::OkuFs;
use iroh_docs::DocTicket;
use miette::IntoDiagnostic;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{collections::HashMap, path::PathBuf};
use zebra::model::core::{DIM_BGESMALL_EN_1_5, DIM_VIT_BASE_PATCH16_224};

impl OkuFs {
    /// Fetch an embedding file associated with a post.
    ///
    /// # Arguments
    ///
    /// * `ticket` - A ticket for the replica containing the file to retrieve.
    ///
    /// * `path` - The path to the file to retrieve.
    ///
    /// * `uri` - The URI associated with the OkuNet post.
    pub async fn fetch_post_embeddings(
        &self,
        ticket: &DocTicket,
        path: PathBuf,
        uri: String,
    ) -> miette::Result<()> {
        if let Ok(bytes) = self
            .fetch_file_with_ticket(ticket, path.clone(), Some(home_replica_filters()))
            .await
        {
            let embeddings = toml::from_str::<HashMap<EmbeddingModality, Vec<f32>>>(
                String::from_utf8_lossy(&bytes).as_ref(),
            )
            .into_diagnostic()?;
            let text_db = zebra::database::default::text::DefaultTextDatabase::open_or_create(
                &"text.zebra".into(),
            );
            let image_db = zebra::database::default::image::DefaultImageDatabase::open_or_create(
                &"image.zebra".into(),
            );
            let audio_db = zebra::database::default::audio::DefaultAudioDatabase::open_or_create(
                &"audio.zebra".into(),
            );
            embeddings
                .into_par_iter()
                .map(|(modality, embedding)| -> miette::Result<()> {
                    match modality {
                        EmbeddingModality::Text => {
                            text_db
                                .insert_records(
                                    &vec![embedding
                                        .try_into()
                                        .unwrap_or([0.0; DIM_BGESMALL_EN_1_5])],
                                    &vec![uri.clone().into()],
                                )
                                .map_err(|e| miette::miette!("{e}"))?;
                        }
                        EmbeddingModality::Image => {
                            image_db
                                .insert_records(
                                    &vec![embedding
                                        .try_into()
                                        .unwrap_or([0.0; DIM_VIT_BASE_PATCH16_224])],
                                    &vec![uri.clone().into()],
                                )
                                .map_err(|e| miette::miette!("{e}"))?;
                        }
                        EmbeddingModality::Audio => {
                            audio_db
                                .insert_records(
                                    &vec![embedding
                                        .try_into()
                                        .unwrap_or([0.0; DIM_VIT_BASE_PATCH16_224])],
                                    &vec![uri.clone().into()],
                                )
                                .map_err(|e| miette::miette!("{e}"))?;
                        }
                    }
                    Ok(())
                })
                .collect::<miette::Result<Vec<_>>>()?;
            text_db
                .index
                .deduplicate()
                .map_err(|e| miette::miette!("{e}"))?;
            image_db
                .index
                .deduplicate()
                .map_err(|e| miette::miette!("{e}"))?;
            audio_db
                .index
                .deduplicate()
                .map_err(|e| miette::miette!("{e}"))?;
        }
        Ok(())
    }
}
