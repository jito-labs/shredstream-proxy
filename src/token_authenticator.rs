use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, SystemTime},
};

use jito_protos::auth::{
    auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
    GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
};
use prost_types::Timestamp;
use solana_metrics::datapoint_info;
use solana_sdk::signature::{Keypair, Signer};
use thiserror::Error;
use tokio::{task::JoinHandle, time::sleep};
use tonic::{
    service::Interceptor,
    transport,
    transport::{Channel, Endpoint},
    Request, Status,
};

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER: &str = "Bearer ";

/// Adds the token to each requests' authorization header.

#[derive(Debug, Error)]
pub enum BlockEngineConnectionError {
    #[error("transport error {0}")]
    TransportError(#[from] transport::Error),
    #[error("client error {0}")]
    ClientError(#[from] Status),
}

pub type BlockEngineConnectionResult<T> = Result<T, BlockEngineConnectionError>;

/// Manages refreshing the token in a separate thread.
#[derive(Clone)]
pub struct ClientInterceptor {
    /// The token added to each request header.
    bearer_token: Arc<RwLock<String>>,
}

impl ClientInterceptor {
    pub async fn new(
        mut auth_service_client: AuthServiceClient<Channel>,
        keypair: &Arc<Keypair>,
        role: Role,
        exit: Arc<AtomicBool>,
    ) -> BlockEngineConnectionResult<Self> {
        let (access_token, refresh_token) =
            Self::auth(&mut auth_service_client, keypair, role).await?;

        let bearer_token = Arc::new(RwLock::new(access_token.value.clone()));

        let _refresh_token_thread = Self::spawn_token_refresh_thread(
            auth_service_client,
            bearer_token.clone(),
            refresh_token,
            access_token.expires_at_utc.unwrap(),
            keypair.clone(),
            role,
            exit,
        );

        Ok(Self { bearer_token })
    }

    async fn auth(
        auth_service_client: &mut AuthServiceClient<Channel>,
        keypair: &Keypair,
        role: Role,
    ) -> BlockEngineConnectionResult<(Token, Token)> {
        let challenge_resp = auth_service_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: role as i32,
                pubkey: keypair.pubkey().as_ref().to_vec(),
            })
            .await?
            .into_inner();
        let challenge = format!("{}-{}", keypair.pubkey(), challenge_resp.challenge);
        let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();

        let tokens = auth_service_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge,
                client_pubkey: keypair.pubkey().as_ref().to_vec(),
                signed_challenge,
            })
            .await?
            .into_inner();

        Ok((tokens.access_token.unwrap(), tokens.refresh_token.unwrap()))
    }

    fn spawn_token_refresh_thread(
        mut auth_service_client: AuthServiceClient<Channel>,
        bearer_token: Arc<RwLock<String>>,
        refresh_token: Token,
        access_token_expiration: Timestamp,
        keypair: Arc<Keypair>,
        role: Role,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<BlockEngineConnectionResult<()>> {
        tokio::spawn(async move {
            let mut refresh_token = refresh_token;
            let mut access_token_expiration = access_token_expiration;

            while !exit.load(Ordering::Relaxed) {
                let access_token_ttl = SystemTime::try_from(access_token_expiration.clone())
                    .unwrap()
                    .duration_since(SystemTime::now())
                    .unwrap_or_else(|_| Duration::from_secs(0));
                let refresh_token_ttl =
                    SystemTime::try_from(refresh_token.expires_at_utc.as_ref().unwrap().clone())
                        .unwrap()
                        .duration_since(SystemTime::now())
                        .unwrap_or_else(|_| Duration::from_secs(0));

                let does_access_token_expire_soon = access_token_ttl < Duration::from_secs(5 * 60);
                let does_refresh_token_expire_soon =
                    refresh_token_ttl < Duration::from_secs(5 * 60);

                match (
                    does_refresh_token_expire_soon,
                    does_access_token_expire_soon,
                ) {
                    // re-run entire auth workflow is refresh token expiring soon
                    (true, _) => {
                        let is_error = {
                            if let Ok((new_access_token, new_refresh_token)) =
                                Self::auth(&mut auth_service_client, &keypair, role).await
                            {
                                *bearer_token.write().unwrap() = new_access_token.value.clone();
                                access_token_expiration = new_access_token.expires_at_utc.unwrap();
                                refresh_token = new_refresh_token;
                                false
                            } else {
                                true
                            }
                        };
                        datapoint_info!(
                            "shredstream_proxy-full_auth",
                            ("is_error", is_error, bool)
                        );
                    }
                    // re-up the access token if it expires soon
                    (_, true) => {
                        let is_error = {
                            if let Ok(refresh_resp) = auth_service_client
                                .refresh_access_token(RefreshAccessTokenRequest {
                                    refresh_token: refresh_token.value.clone(),
                                })
                                .await
                            {
                                let access_token = refresh_resp.into_inner().access_token.unwrap();
                                *bearer_token.write().unwrap() = access_token.value.clone();
                                access_token_expiration = access_token.expires_at_utc.unwrap();
                                false
                            } else {
                                true
                            }
                        };

                        datapoint_info!(
                            "shredstream_proxy-refresh_auth",
                            ("is_error", is_error, bool)
                        );
                    }
                    _ => {
                        if !exit.load(Ordering::Relaxed) {
                            sleep(Duration::from_secs(60)).await
                        };
                    }
                }
            }
            Ok(())
        })
    }
}

impl Interceptor for ClientInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let l_token = self.bearer_token.read().unwrap();
        if !l_token.is_empty() {
            request.metadata_mut().insert(
                AUTHORIZATION_HEADER,
                format!("{}{}", BEARER, l_token).parse().unwrap(),
            );
        }

        Ok(request)
    }
}
pub async fn create_grpc_channel(url: &str) -> BlockEngineConnectionResult<Channel> {
    let mut endpoint = Endpoint::from_shared(url.to_string()).expect("invalid url");
    if url.contains("https") {
        endpoint = endpoint.tls_config(tonic::transport::ClientTlsConfig::new())?;
    }
    Ok(endpoint.connect().await?)
}
