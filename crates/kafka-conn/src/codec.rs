//! Wire framing and header handling.
//!
//! Two things in here are easy to get subtly wrong, and both produce an
//! off-by-a-few-bytes failure rather than a clear error:
//!
//! 1. **The response header version is not the request's api version.** It is
//!    a per-api, per-version mapping. We ask [`ApiKey::response_header_version`]
//!    rather than deriving it, because deriving it is how you end up two bytes
//!    into the body.
//! 2. **`ApiVersions` responses always use response header v0**, even once the
//!    connection is flexible. That is a genuine special case in the protocol —
//!    a chicken-and-egg escape hatch, since the client does not yet know what
//!    the broker speaks. The crate encodes it in
//!    `ApiVersionsResponse::header_version`, which is another reason to go
//!    through the helper instead of computing it.
//!
//! Frames themselves are a 4-byte big-endian length prefix followed by the
//! header and body — `LengthDelimitedCodec`'s default configuration, stated
//! explicitly here so a future edit cannot silently change endianness.

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio_util::codec::LengthDelimitedCodec;

use crate::api_key::ApiKey;
use crate::error::{Error, Result};
use crate::rpc::Rpc;

/// Kafka's own `socket.request.max.bytes` default, and a sane ceiling for
/// responses too. A frame larger than this is a protocol desync, not a big
/// fetch, and reading it would be an unbounded allocation driven by the peer.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// The length-delimited codec every connection uses.
pub(crate) fn frame_codec(max_frame_bytes: usize) -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_offset(0)
        .length_field_length(4)
        .length_adjustment(0)
        .num_skip(4)
        .max_frame_length(max_frame_bytes)
        .new_codec()
}

/// Encode a request header and body into a frame payload.
///
/// The length prefix is added by the codec, not here.
pub(crate) fn encode_request<R: Rpc>(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    request: &R,
) -> Result<BytesMut> {
    let upstream = protocol_key(api_key)?;
    let header = RequestHeader::default()
        .with_request_api_key(api_key.code())
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(client_id.map(|id| StrBytes::from_string(id.to_owned())));

    let header_version = upstream.request_header_version(version);
    let mut buf = BytesMut::new();
    header
        .encode(&mut buf, header_version)
        .map_err(|e| Error::decode("encoding request header", e))?;
    request
        .encode(&mut buf, version)
        .map_err(|e| Error::decode("encoding request body", e))?;
    Ok(buf)
}

/// Read the correlation id out of a response frame without decoding it.
///
/// The correlation id is the first field of the response header in every
/// header version, so the read loop can route a frame to its waiting caller
/// before it knows — or needs to know — which api and version produced it.
/// That is what keeps decoding on the calling task, where a decode failure
/// belongs to one request instead of killing the connection.
pub(crate) fn peek_correlation_id(frame: &[u8]) -> Result<i32> {
    if frame.len() < 4 {
        return Err(Error::decode(
            "reading response correlation id",
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("response frame is {} bytes, need at least 4", frame.len()),
            ),
        ));
    }
    let mut head = frame.get(..4).unwrap_or_default();
    Ok(head.get_i32())
}

/// Decode a response frame into its typed body.
pub(crate) fn decode_response<R: Rpc>(
    api_key: ApiKey,
    version: i16,
    frame: Bytes,
) -> Result<R::Response> {
    let mut buf = frame;
    let upstream = protocol_key(api_key)?;
    // Not `version`, and not our own arithmetic on it.
    let header_version = upstream.response_header_version(version);
    ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| Error::decode("decoding response header", e))?;
    R::Response::decode(&mut buf, version).map_err(|e| Error::decode("decoding response body", e))
}

/// Skip the response header, leaving the body.
///
/// The `ApiVersions` bootstrap needs this: it has to inspect the body's error
/// code before it knows which version the body was encoded at.
pub(crate) fn split_response_body(api_key: ApiKey, version: i16, frame: Bytes) -> Result<Bytes> {
    let mut buf = frame;
    let upstream = protocol_key(api_key)?;
    let header_version = upstream.response_header_version(version);
    ResponseHeader::decode(&mut buf, header_version)
        .map_err(|e| Error::decode("decoding response header", e))?;
    Ok(buf)
}

/// Map our api key onto the codec's, or explain why we cannot.
fn protocol_key(api_key: ApiKey) -> Result<kafka_protocol::messages::ApiKey> {
    kafka_protocol::messages::ApiKey::try_from(api_key.code()).map_err(|_| Error::UnsupportedApi {
        api_key,
        broker: None,
        ours: None,
    })
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{ApiVersionsRequest, MetadataRequest};

    use super::*;

    #[test]
    fn request_frames_start_with_key_version_and_correlation() {
        let req = MetadataRequest::default();
        let buf =
            encode_request(ApiKey::Metadata, 12, 0x0102_0304, Some("kaas"), &req).expect("encodes");
        assert_eq!(&buf[..2], &ApiKey::Metadata.code().to_be_bytes());
        assert_eq!(&buf[2..4], &12i16.to_be_bytes());
        assert_eq!(&buf[4..8], &0x0102_0304i32.to_be_bytes());
    }

    #[test]
    fn correlation_id_is_readable_without_knowing_the_version() {
        // Round-trip through the *response* header at both header versions:
        // the peek must work for either, since the read loop does not know
        // which one applies until it has found the request.
        for header_version in [0i16, 1] {
            let mut buf = BytesMut::new();
            ResponseHeader::default()
                .with_correlation_id(4242)
                .encode(&mut buf, header_version)
                .expect("encodes");
            assert_eq!(peek_correlation_id(&buf).ok(), Some(4242));
        }
    }

    #[test]
    fn a_short_frame_is_a_decode_error_not_a_panic() {
        assert!(peek_correlation_id(&[0, 1]).is_err());
    }

    #[test]
    fn api_versions_responses_use_header_v0_at_every_api_version() {
        // The special case. If this ever stops holding, the very first round
        // trip against a flexible-versions broker desyncs by two bytes.
        let upstream = kafka_protocol::messages::ApiKey::ApiVersions;
        for version in 0..=4 {
            assert_eq!(
                upstream.response_header_version(version),
                0,
                "api version {version}"
            );
        }
        // ... while the *request* header does become flexible at v3.
        assert_eq!(upstream.request_header_version(2), 1);
        assert_eq!(upstream.request_header_version(3), 2);
    }

    #[test]
    fn response_header_version_is_not_the_api_version() {
        // Metadata v9 is where the response header goes flexible. Deriving the
        // header version from the api version would give 9.
        let upstream = kafka_protocol::messages::ApiKey::Metadata;
        assert_eq!(upstream.response_header_version(8), 0);
        assert_eq!(upstream.response_header_version(9), 1);
    }

    #[test]
    fn header_and_body_round_trip_through_the_decoder() {
        let req = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("kaas-lib"))
            .with_client_software_version(StrBytes::from_static_str("0.1.0"));
        let frame = encode_request(ApiKey::ApiVersions, 3, 7, Some("kaas"), &req).expect("encodes");
        assert_eq!(peek_correlation_id(&frame[4..8]).ok(), Some(7));

        // The response is hand-assembled from wire bytes rather than produced
        // by the crate's encoder: response encoders live behind
        // `feature = "broker"`, which this workspace deliberately does not
        // build. Writing the bytes out is the more honest test anyway — it
        // checks our decoder against the protocol, not against its own encoder.
        let mut response = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(7)
            .encode(&mut response, 0) // ApiVersions: always response header v0
            .expect("encodes");
        response.extend_from_slice(&0i16.to_be_bytes()); // error_code
        response.extend_from_slice(&[2]); // compact array: 1 entry (len + 1)
        response.extend_from_slice(&ApiKey::Metadata.code().to_be_bytes());
        response.extend_from_slice(&0i16.to_be_bytes()); // min_version
        response.extend_from_slice(&13i16.to_be_bytes()); // max_version
        response.extend_from_slice(&[0]); // entry tagged fields
        response.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms
        response.extend_from_slice(&[0]); // response tagged fields

        let decoded =
            decode_response::<ApiVersionsRequest>(ApiKey::ApiVersions, 3, response.freeze())
                .expect("decodes");
        assert_eq!(decoded.error_code, 0);
        assert_eq!(decoded.api_keys.len(), 1);
        assert_eq!(decoded.api_keys[0].max_version, 13);
    }

    #[test]
    fn the_frame_codec_is_big_endian_with_a_four_byte_prefix() {
        use tokio_util::codec::Decoder;
        let mut codec = frame_codec(DEFAULT_MAX_FRAME_BYTES);
        let mut buf = BytesMut::from(&[0u8, 0, 0, 3, 0xAA, 0xBB, 0xCC][..]);
        let frame = codec.decode(&mut buf).expect("decodes").expect("a frame");
        assert_eq!(&frame[..], &[0xAA, 0xBB, 0xCC]);
    }
}
