//! The Signal wire every harness socket speaks.
//!
//! One frame is a four-byte big-endian length and the rkyv archive of one
//! contract root: `signal-harness` `Query` and `Response` on the ordinary
//! socket, `meta-signal-harness` `Query` and `Response` on the meta socket,
//! and the `signal-persona` engine-management `Query` and `Response` on the
//! supervision socket. There is no envelope, exchange identifier or contract
//! discriminator; each socket carries exactly one contract.

use signal::{
    AsyncFrameReading, AsyncFrameWriting, ByteViewable, FrameCapacity, FrameReading, FrameWriting,
    Restorable, Signal, Signalizable,
};

use crate::{Error, Result};

/// The framing of one contract root on a socket, with the frame capacity it
/// admits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SignalWire {
    capacity: FrameCapacity,
}

impl SignalWire {
    /// Read one frame and restore the contract root it archives.
    pub fn read<Root>(&self, stream: &mut impl std::io::Read) -> Result<Root>
    where
        Signal<Root>: Restorable<Root>,
    {
        let body = stream.read_frame(self.capacity)?;
        Self::restore(Vec::from(body))
    }

    /// Archive one contract root and write it as one frame.
    pub fn write<Root>(&self, stream: &mut impl std::io::Write, root: &Root) -> Result<()>
    where
        Root: Signalizable,
    {
        stream.write_frame(&Self::archive(root)?, self.capacity)?;
        Ok(())
    }

    /// Read one frame from an asynchronous stream.
    pub async fn read_async<Root>(
        &self,
        stream: &mut (impl tokio::io::AsyncRead + Unpin + Send),
    ) -> Result<Root>
    where
        Signal<Root>: Restorable<Root>,
    {
        let body = stream.read_frame(self.capacity).await?;
        Self::restore(Vec::from(body))
    }

    /// Write one frame to an asynchronous stream.
    pub async fn write_async<Root>(
        &self,
        stream: &mut (impl tokio::io::AsyncWrite + Unpin + Send),
        root: &Root,
    ) -> Result<()>
    where
        Root: Signalizable,
    {
        let signal = Self::archive(root)?;
        stream
            .write_frame(&FramedBytes(signal.bytes().to_vec()), self.capacity)
            .await?;
        Ok(())
    }

    fn restore<Root>(bytes: Vec<u8>) -> Result<Root>
    where
        Signal<Root>: Restorable<Root>,
    {
        Signal::<Root>::from(bytes)
            .restore()
            .map_err(|_| Error::UnreadableSignalFrame)
    }

    fn archive<Root>(root: &Root) -> Result<Signal<Root>>
    where
        Root: Signalizable,
    {
        root.signalize().map_err(|_| Error::UnarchivableSignalValue)
    }
}

/// Archived bytes handed to the asynchronous writer, which needs `Sync`.
struct FramedBytes(Vec<u8>);

impl ByteViewable for FramedBytes {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}
