use super::*;
use crate::data_channel::{RTCDataChannelId, RTCDataChannelInit};
use crate::peer_connection::event::TaggedRTCEventInternal;
use crate::peer_connection::transport::RTCDtlsRole;
use ::datachannel::data_channel::DataChannelMessage;
use ::sctp::PayloadProtocolIdentifier;

const FIRST_WATCH: &[u8] = br#"{"kind":"watch","id":1,"views":[]}"#;

fn negotiated_peer(now: Instant) -> (RTCPeerConnection, RTCDataChannelId) {
    let mut peer = RTCPeerConnectionBuilder::new().build(now).expect("peer");
    let id = peer
        .create_data_channel(
            "control",
            Some(RTCDataChannelInit {
                negotiated: Some(0),
                ordered: true,
                ..Default::default()
            }),
        )
        .expect("negotiated channel")
        .id();
    while peer.poll_event().is_some() {}
    peer.dtls_transport_mut().dtls_role = RTCDtlsRole::Client;
    (peer, id)
}

fn connected(peer: &mut RTCPeerConnection, now: Instant) {
    peer.pipeline_context
        .sctp_handler_context
        .event_outs
        .push_back(TaggedRTCEventInternal {
            now,
            event: RTCEventInternal::SCTPHandshakeComplete(0),
        });
}

fn received(peer: &mut RTCPeerConnection, now: Instant, stream_id: u16) {
    peer.pipeline_context
        .sctp_handler_context
        .read_outs
        .push_back(TaggedRTCMessageInternal {
            now,
            transport: TransportContext::default(),
            message: RTCMessageInternal::Dtls(DTLSMessage::Sctp(DataChannelMessage {
                association_handle: 0,
                stream_id,
                ppi: PayloadProtocolIdentifier::String,
                payload: BytesMut::from(FIRST_WATCH),
                negotiated: false,
            })),
        });
}

fn count_opens(peer: &mut RTCPeerConnection, id: RTCDataChannelId) -> usize {
    let mut opened = 0;
    while let Some(event) = peer.poll_event() {
        if matches!(
            event,
            RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(opened_id))
                if opened_id == id
        ) {
            opened += 1;
        }
    }
    opened
}

/// SCTP can queue Connected and the first readable payload in one input flight.
/// A caller may drain reads before events; that must not discard the payload.
#[test]
fn connected_and_first_payload_in_one_flight_deliver_once() {
    let now = Instant::now();
    let (mut peer, id) = negotiated_peer(now);
    connected(&mut peer, now);
    received(&mut peer, now, 0);

    let first = peer
        .poll_read()
        .expect("first watch must survive SCTP open");
    assert_eq!(first.now, now, "preserve the received timestamp");
    match first.message {
        RTCMessage::DataChannelMessage(received_id, message) => {
            assert_eq!(received_id, id);
            assert!(message.is_string);
            assert_eq!(message.data.as_ref(), FIRST_WATCH);
        }
        other => panic!("unexpected message: {other:?}"),
    }
    assert!(peer.poll_read().is_none(), "the payload is delivered once");
    assert_eq!(count_opens(&mut peer, id), 1, "one open notification");

    connected(&mut peer, now);
    assert!(peer.poll_read().is_none());
    assert_eq!(
        count_opens(&mut peer, id),
        0,
        "repeated connection is idempotent"
    );
}

#[test]
fn connected_event_does_not_accept_user_data_on_an_unknown_stream() {
    let now = Instant::now();
    let (mut peer, id) = negotiated_peer(now);
    connected(&mut peer, now);
    received(&mut peer, now, 1);

    assert!(peer.poll_read().is_none());
    assert_eq!(count_opens(&mut peer, id), 1);
    assert!(peer.data_channels.handle_of_stream(&1).is_none());

    // The rejected stream must not poison the registered channel.
    received(&mut peer, now, 0);
    assert!(matches!(
        peer.poll_read().map(|message| message.message),
        Some(RTCMessage::DataChannelMessage(received_id, message))
            if received_id == id && message.data.as_ref() == FIRST_WATCH
    ));
    assert!(peer.poll_read().is_none());
    assert_eq!(count_opens(&mut peer, id), 0);
}
