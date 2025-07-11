use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[async_trait]
pub trait JsonTransport: Sized + Sync {
    async fn send<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> anyhow::Result<()>;
    async fn recv<R: AsyncRead + Unpin + Send>(r: &mut R) -> anyhow::Result<Self>;
}

#[async_trait]
impl<M: Serialize + DeserializeOwned + Sync> JsonTransport for M {
    async fn send<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> anyhow::Result<()> {
        // Frame and serialize
        // use JSON serialization to avoid pulling in another dependency
        let msg = serde_json::to_vec(self)?;
        // frame it by a 2 byte length
        let len = msg.len() as u16;
        let mut pkt = Vec::with_capacity(2 + msg.len());
        pkt.extend_from_slice(&len.to_le_bytes());
        pkt.extend_from_slice(&msg);
        w.write_all(&pkt).await?;
        Ok(())
    }

    async fn recv<R: AsyncRead + Unpin + Send>(r: &mut R) -> anyhow::Result<Self> {
        let mut len_buf = [0u8; 2];
        r.read_exact(&mut len_buf).await?;
        let len = u16::from_le_bytes(len_buf);

        let mut msg = vec![0u8; len as usize];
        r.read_exact(&mut msg).await?;

        let req: Self = serde_json::from_slice(&msg)?;
        Ok(req)
    }
}
