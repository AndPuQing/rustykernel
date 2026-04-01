pub const IMPLEMENTATION: &str = "rustykernel";
pub const LANGUAGE: &str = "python";
pub const JUPYTER_PROTOCOL_VERSION: &str = "5.3";

use std::borrow::Cow;

use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use uuid::Uuid;

const DELIMITER: &[u8] = b"<IDS|MSG>";
const KERNEL_USERNAME: &str = "kernel";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageHeader {
    pub msg_id: String,
    pub session: String,
    pub username: String,
    pub date: String,
    pub msg_type: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subshell_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct JupyterMessage {
    pub identities: Vec<Vec<u8>>,
    pub header: MessageHeader,
    header_bytes: Vec<u8>,
    pub parent_header: Value,
    pub metadata: Value,
    pub content: Value,
    pub buffers: Vec<Vec<u8>>,
}

impl PartialEq for JupyterMessage {
    fn eq(&self, other: &Self) -> bool {
        self.identities == other.identities
            && self.header == other.header
            && self.parent_header == other.parent_header
            && self.metadata == other.metadata
            && self.content == other.content
            && self.buffers == other.buffers
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ParentHeader<'a> {
    Message(&'a JupyterMessage),
    Value(&'a Value),
}

#[derive(Debug)]
pub enum ProtocolError {
    InvalidMessage(&'static str),
    InvalidHeader(serde_json::Error),
    InvalidParentHeader(serde_json::Error),
    InvalidMetadata(serde_json::Error),
    InvalidContent(serde_json::Error),
    InvalidSignature,
    UnsupportedSignatureScheme(String),
    Serialization(serde_json::Error),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMessage(message) => f.write_str(message),
            Self::InvalidHeader(error) => write!(f, "invalid message header JSON: {error}"),
            Self::InvalidParentHeader(error) => {
                write!(f, "invalid message parent header JSON: {error}")
            }
            Self::InvalidMetadata(error) => write!(f, "invalid message metadata JSON: {error}"),
            Self::InvalidContent(error) => write!(f, "invalid message content JSON: {error}"),
            Self::InvalidSignature => f.write_str("invalid Jupyter message signature"),
            Self::UnsupportedSignatureScheme(scheme) => {
                write!(f, "unsupported Jupyter signature scheme: {scheme}")
            }
            Self::Serialization(error) => {
                write!(f, "failed to serialize Jupyter message JSON: {error}")
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Clone, Debug)]
pub struct MessageSigner {
    key: Vec<u8>,
    scheme: SignatureScheme,
}

#[derive(Clone, Debug)]
enum SignatureScheme {
    HmacSha256,
}

impl MessageHeader {
    pub fn new(msg_type: impl Into<String>, session: impl Into<String>) -> Self {
        Self {
            msg_id: Uuid::new_v4().to_string(),
            session: session.into(),
            username: KERNEL_USERNAME.to_owned(),
            date: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            msg_type: msg_type.into(),
            version: JUPYTER_PROTOCOL_VERSION.to_owned(),
            subshell_id: None,
        }
    }
}

impl JupyterMessage {
    #[allow(dead_code)]
    pub fn new(
        identities: Vec<Vec<u8>>,
        header: MessageHeader,
        parent_header: Value,
        metadata: Value,
        content: Value,
    ) -> Self {
        Self {
            identities,
            header,
            header_bytes: Vec::new(),
            parent_header,
            metadata,
            content,
            buffers: Vec::new(),
        }
    }

    fn decoded(
        identities: Vec<Vec<u8>>,
        header: MessageHeader,
        header_bytes: Vec<u8>,
        parent_header: Value,
        metadata: Value,
        content: Value,
        buffers: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            identities,
            header,
            header_bytes,
            parent_header,
            metadata,
            content,
            buffers,
        }
    }

    fn header_bytes(&self) -> Result<Cow<'_, [u8]>, ProtocolError> {
        if self.header_bytes.is_empty() {
            Ok(Cow::Owned(
                serde_json::to_vec(&self.header).map_err(ProtocolError::Serialization)?,
            ))
        } else {
            Ok(Cow::Borrowed(&self.header_bytes))
        }
    }
}

impl MessageSigner {
    pub fn new(signature_scheme: &str, key: &str) -> Result<Self, ProtocolError> {
        let scheme = match signature_scheme {
            "hmac-sha256" => SignatureScheme::HmacSha256,
            _ => {
                return Err(ProtocolError::UnsupportedSignatureScheme(
                    signature_scheme.to_owned(),
                ));
            }
        };

        Ok(Self {
            key: key.as_bytes().to_vec(),
            scheme,
        })
    }

    pub fn decode(&self, frames: Vec<Vec<u8>>) -> Result<JupyterMessage, ProtocolError> {
        let delimiter_index = frames
            .iter()
            .position(|frame| frame.as_slice() == DELIMITER)
            .ok_or(ProtocolError::InvalidMessage(
                "missing Jupyter message delimiter",
            ))?;
        let identities = frames[..delimiter_index].to_vec();
        let message_frames = &frames[(delimiter_index + 1)..];

        if message_frames.len() < 5 {
            return Err(ProtocolError::InvalidMessage(
                "Jupyter message must include signature, header, parent_header, metadata, and content",
            ));
        }

        let signature = &message_frames[0];
        let header_frame = &message_frames[1];
        let parent_header_frame = &message_frames[2];
        let metadata_frame = &message_frames[3];
        let content_frame = &message_frames[4];
        let buffers = message_frames[5..].to_vec();

        self.verify_signature(
            signature,
            [
                header_frame,
                parent_header_frame,
                metadata_frame,
                content_frame,
            ],
        )?;

        let header: MessageHeader =
            serde_json::from_slice(header_frame).map_err(ProtocolError::InvalidHeader)?;
        let parent_header = serde_json::from_slice(parent_header_frame)
            .map_err(ProtocolError::InvalidParentHeader)?;
        let metadata =
            serde_json::from_slice(metadata_frame).map_err(ProtocolError::InvalidMetadata)?;
        let content =
            serde_json::from_slice(content_frame).map_err(ProtocolError::InvalidContent)?;

        Ok(JupyterMessage::decoded(
            identities,
            header,
            header_frame.clone(),
            parent_header,
            metadata,
            content,
            buffers,
        ))
    }

    #[allow(dead_code)]
    pub fn encode(&self, message: &JupyterMessage) -> Result<Vec<Vec<u8>>, ProtocolError> {
        self.encode_with_parts(
            &message.identities,
            &message.header,
            ParentHeader::Value(&message.parent_header),
            &message.metadata,
            &message.content,
            &message.buffers,
        )
    }

    pub fn encode_with_parts(
        &self,
        identities: &[Vec<u8>],
        header: &MessageHeader,
        parent_header: ParentHeader<'_>,
        metadata: &Value,
        content: &Value,
        buffers: &[Vec<u8>],
    ) -> Result<Vec<Vec<u8>>, ProtocolError> {
        let header = serde_json::to_vec(header).map_err(ProtocolError::Serialization)?;
        let parent_header = match parent_header {
            ParentHeader::Message(message) => message.header_bytes()?,
            ParentHeader::Value(value) => {
                Cow::Owned(serde_json::to_vec(value).map_err(ProtocolError::Serialization)?)
            }
        };
        let metadata = serde_json::to_vec(metadata).map_err(ProtocolError::Serialization)?;
        let content = serde_json::to_vec(content).map_err(ProtocolError::Serialization)?;
        let signature = self.sign([&header, &parent_header, &metadata, &content]);

        let mut frames = Vec::with_capacity(identities.len() + buffers.len() + 6);
        frames.extend(identities.iter().cloned());
        frames.push(DELIMITER.to_vec());
        frames.push(signature.into_bytes());
        frames.push(header);
        frames.push(parent_header.into_owned());
        frames.push(metadata);
        frames.push(content);
        frames.extend(buffers.iter().cloned());
        Ok(frames)
    }

    fn verify_signature(&self, signature: &[u8], parts: [&[u8]; 4]) -> Result<(), ProtocolError> {
        if self.key.is_empty() {
            return Ok(());
        }

        let expected = self.sign(parts);
        if expected.as_bytes() == signature {
            Ok(())
        } else {
            Err(ProtocolError::InvalidSignature)
        }
    }

    fn sign(&self, parts: [&[u8]; 4]) -> String {
        if self.key.is_empty() {
            return String::new();
        }

        match self.scheme {
            SignatureScheme::HmacSha256 => {
                let mut mac = HmacSha256::new_from_slice(&self.key)
                    .expect("HMAC accepts arbitrary key sizes");
                for part in parts {
                    mac.update(part);
                }
                hex_lower(&mac.finalize().into_bytes())
            }
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DELIMITER, JupyterMessage, MessageHeader, MessageSigner, ParentHeader};

    #[test]
    fn round_trips_signed_messages() {
        let signer = MessageSigner::new("hmac-sha256", "secret").unwrap();
        let message = JupyterMessage::new(
            vec![b"client".to_vec()],
            MessageHeader::new("kernel_info_request", "session-1"),
            json!({}),
            json!({}),
            json!({}),
        );

        let encoded = signer.encode(&message).unwrap();
        let decoded = signer.decode(encoded).unwrap();

        assert_eq!(decoded.identities, vec![b"client".to_vec()]);
        assert_eq!(decoded.header.msg_type, "kernel_info_request");
        assert_eq!(decoded.header.session, "session-1");
    }

    #[test]
    fn rejects_bad_signatures() {
        let signer = MessageSigner::new("hmac-sha256", "secret").unwrap();
        let mut encoded = signer
            .encode(&JupyterMessage::new(
                Vec::new(),
                MessageHeader::new("kernel_info_request", "session-1"),
                json!({}),
                json!({}),
                json!({}),
            ))
            .unwrap();
        encoded[1] = b"bad-signature".to_vec();

        let error = signer.decode(encoded).unwrap_err();
        assert!(matches!(error, super::ProtocolError::InvalidSignature));
    }

    #[test]
    fn preserves_unknown_header_fields_in_parent_header() {
        let signer = MessageSigner::new("hmac-sha256", "secret").unwrap();
        let header = br#"{
            "msg_id":"request-1",
            "session":"session-1",
            "username":"test-client",
            "date":"2026-04-01T00:00:00Z",
            "msg_type":"kernel_info_request",
            "version":"5.3",
            "extra":"preserve-me"
        }"#
        .to_vec();
        let parent_header = br#"{}"#.to_vec();
        let metadata = br#"{}"#.to_vec();
        let content = br#"{}"#.to_vec();
        let signature = signer.sign([&header, &parent_header, &metadata, &content]);

        let decoded = signer
            .decode(vec![
                b"client".to_vec(),
                DELIMITER.to_vec(),
                signature.into_bytes(),
                header,
                parent_header,
                metadata,
                content,
            ])
            .unwrap();

        let reply_header = MessageHeader::new("kernel_info_reply", "session-1");
        let reply = signer
            .encode_with_parts(
                &decoded.identities,
                &reply_header,
                ParentHeader::Message(&decoded),
                &json!({}),
                &json!({}),
                &[],
            )
            .unwrap();
        let decoded_reply = signer.decode(reply).unwrap();

        assert_eq!(
            decoded_reply.parent_header.get("extra"),
            Some(&json!("preserve-me"))
        );
    }
}
