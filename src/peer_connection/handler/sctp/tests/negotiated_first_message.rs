use super::*;

fn local_open(
    now: Instant,
    association: AssociationHandle,
    negotiated: bool,
) -> TaggedRTCMessageInternal {
    let payload = Message::DataChannelOpen(DataChannelOpen {
        channel_type: ChannelType::Reliable,
        priority: CHANNEL_PRIORITY_NORMAL,
        reliability_parameter: 0,
        label: b"control".to_vec(),
        protocol: Vec::new(),
    })
    .marshal()
    .expect("local open encoding");
    TaggedRTCMessageInternal {
        now,
        transport: TransportContext::default(),
        message: RTCMessageInternal::Dtls(DTLSMessage::Sctp(DataChannelMessage {
            association_handle: association.0,
            stream_id: 0,
            ppi: PayloadProtocolIdentifier::Dcep,
            payload,
            negotiated,
        })),
    }
}

/// Real SCTP DATA creates its incoming stream before the channel's queued local
/// open is consumed. Negotiated open must keep that stream and its received data.
#[test]
fn negotiated_open_reuses_incoming_stream_and_can_reply() {
    let now = Instant::now();
    let mut established = establish();
    let datagrams = server_datagrams_carrying(&mut established, 0, 1, now);
    let association = established.client_ch;
    let mut ctx = client_ctx(
        now,
        established.client_ep,
        association,
        established.client_conn,
    );
    {
        let mut handler = SctpHandler::new(&mut ctx, 0);
        for datagram in datagrams {
            handler
                .handle_read(raw_read(now, datagram))
                .expect("first DATA");
        }
        handler
            .handle_write(local_open(now, association, true))
            .expect("negotiated open must reuse the incoming SCTP stream");
    }
    assert_eq!(ctx.read_outs.len(), 1, "the first payload is preserved");
    let first = ctx.read_outs.pop_front().expect("first payload");
    assert!(matches!(
        first.message,
        RTCMessageInternal::Dtls(DTLSMessage::Sctp(message))
            if message.stream_id == 0 && message.payload.as_ref() == [0; 64]
    ));
    let connection = ctx
        .sctp_transport
        .sctp_associations
        .get_mut(&association)
        .unwrap();
    assert_eq!(connection.stream_ids(), vec![0]);
    assert_eq!(
        connection.stream(0).unwrap().buffered_amount().unwrap(),
        0,
        "a negotiated open must not send DCEP"
    );

    let acknowledgment = br#"{"id":1,"accepted":true}"#;
    let mut handler = SctpHandler::new(&mut ctx, 0);
    handler
        .handle_write(TaggedRTCMessageInternal {
            now,
            transport: TransportContext::default(),
            message: RTCMessageInternal::Dtls(DTLSMessage::Sctp(DataChannelMessage {
                association_handle: association.0,
                stream_id: 0,
                ppi: PayloadProtocolIdentifier::String,
                payload: BytesMut::from(&acknowledgment[..]),
                negotiated: false,
            })),
        })
        .expect("reply on the reused stream");
    while let Some(message) = handler.poll_write() {
        if let RTCMessageInternal::Dtls(DTLSMessage::Raw(raw)) = message.message {
            if let Some((received, DatagramEvent::AssociationEvent(event))) = established
                .server_ep
                .handle(now, client_addr(), None, raw.freeze())
            {
                assert_eq!(received, established.server_ch);
                established.server_conn.handle_event(event);
            }
        }
    }
    while established.server_conn.poll().is_some() {}
    let reply = established
        .server_conn
        .stream(0)
        .unwrap()
        .read_sctp()
        .unwrap()
        .expect("peer receives reply");
    assert_eq!(reply.ppi, PayloadProtocolIdentifier::String);
    assert_eq!(reply.to_payload(1024).unwrap().as_ref(), acknowledgment);
    assert!(
        established
            .server_conn
            .stream(0)
            .unwrap()
            .read_sctp()
            .unwrap()
            .is_none()
    );
}

#[test]
fn in_band_duplicate_open_remains_an_error() {
    let now = Instant::now();
    let mut established = establish();
    let datagrams = server_datagrams_carrying(&mut established, 0, 1, now);
    let association = established.client_ch;
    let (mut ctx, _) = run_client(now, established, datagrams, 0);
    let before = ctx.read_outs.len();
    let error = SctpHandler::new(&mut ctx, 0)
        .handle_write(local_open(now, association, false))
        .expect_err("an in-band duplicate must not adopt the incoming stream");
    assert_eq!(error, Error::ErrStreamAlreadyExist);
    assert_eq!(ctx.read_outs.len(), before);
    assert_eq!(
        ctx.sctp_transport
            .sctp_associations
            .get(&association)
            .unwrap()
            .stream_ids(),
        vec![0]
    );
}

#[test]
fn negotiated_open_still_creates_a_missing_stream_without_sending_dcep() {
    let now = Instant::now();
    let established = establish();
    let association = established.client_ch;
    let mut ctx = client_ctx(
        now,
        established.client_ep,
        association,
        established.client_conn,
    );
    SctpHandler::new(&mut ctx, 0)
        .handle_write(local_open(now, association, true))
        .expect("new negotiated stream");
    let connection = ctx
        .sctp_transport
        .sctp_associations
        .get_mut(&association)
        .unwrap();
    assert_eq!(connection.stream_ids(), vec![0]);
    assert_eq!(connection.stream(0).unwrap().buffered_amount().unwrap(), 0);
}
