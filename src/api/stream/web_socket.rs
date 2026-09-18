use std::{
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::api::{
    bindings::{
        StreamStatsClientboundMessage, StreamStatsServerboundMessage, WebSocketChannel,
        WebSocketClientboundMessage, WebSocketServerboundMessage, WebSocketStreamResponse,
    },
    stream::apply_role_restrictions,
};
use actix_web::{Error, HttpRequest, HttpResponse, get, rt::spawn, web::Payload};
use actix_ws::{Message, MessageStream, Session};
use bytes::Bytes;
use moonlight_common::{
    AppId,
    crypto::rustcrypto::RustCryptoBackend,
    stream::{
        AesIv, AesKey, EncryptionFlags, MoonlightStreamSettings, StreamingConfig,
        audio::AudioConfig,
        control::ActiveGamepads,
        proto::{
            MoonlightStreamSetup,
            audio::AudioStreamEvent,
            control::{
                ControlStreamEvent,
                packet::{ControlPacket, ControlPacketConfig, PacketDirection},
            },
            video::VideoStreamEvent,
        },
        tokio::{MoonlightStream, MoonlightStreamEvent},
        video::{ColorRange, ColorSpace, VideoCapabilities, VideoFormats},
    },
};
use tokio::{
    select,
    sync::mpsc::{UnboundedSender, unbounded_channel},
    time::{interval, sleep},
};
use tracing::{Instrument, debug, debug_span, error, info, instrument, trace, warn};

use crate::{
    api::stream::create_control_packet_config,
    app::{AppError, host::HostId, user::AuthenticatedUser},
};

const MAX_OUTBOUND_BACKLOG_BYTES: usize = 1024 * 1024;
const RESUME_OUTBOUND_BACKLOG_BYTES: usize = MAX_OUTBOUND_BACKLOG_BYTES / 4;

enum WsData {
    Bytes(Bytes),
    Video(Bytes),
    Text(String),
}

impl WsData {
    fn len(&self) -> usize {
        match self {
            WsData::Bytes(bytes) | WsData::Video(bytes) => bytes.len(),
            WsData::Text(text) => text.len(),
        }
    }
}

#[get("/host/stream/web_socket")]
#[instrument(skip(user, req, body_stream), fields(user = %user.id()))]
pub async fn web_socket_stream(
    mut user: AuthenticatedUser,
    req: HttpRequest,
    body_stream: Payload,
) -> Result<HttpResponse, Error> {
    if !user
        .role()
        .await?
        .permissions()
        .await?
        .allow_transport_websockets
    {
        return Err(AppError::Forbidden.into());
    }

    // upgrade connection to web socket connection
    let (res, ws_sender, ws_receiver) = actix_ws::handle(&req, body_stream)?;

    spawn(
        async move {
            match handle_ws(user, ws_sender, ws_receiver).await {
                Ok(_) => {}
                Err(err) => {
                    error!(error = %err, "stream failed");
                }
            }
        }
        .instrument(debug_span!("ws handler")),
    );

    Ok(res)
}

async fn handle_ws(
    mut user: AuthenticatedUser,
    mut ws_sender: Session,
    mut ws_receiver: MessageStream,
) -> Result<(), AppError> {
    let control_config = create_control_packet_config();

    // See if the user is allowed to use web sockets
    let permissions = user.role().await?.permissions().await?;
    if !permissions.allow_transport_websockets {
        return Err(AppError::Forbidden);
    }

    // Wait for stream request
    let stream_request = select! {
        _ = sleep(Duration::from_secs(10)) => {
            return Err(AppError::StreamClosed);
        }
        request = ws_receiver.recv() => request
    };

    // Deserialize message
    let stream_request = match stream_request.expect("stream request") {
        Ok(Message::Text(text)) => text,
        Ok(message) => {
            error!(message = ?message, "web socket received unexpected start message");
            return Err(AppError::StreamClosed);
        }
        Err(err) => {
            error!(error = %err, "web socket protocol error");
            return Err(AppError::StreamClosed);
        }
    };
    let stream_request = match serde_json::from_str::<WebSocketServerboundMessage>(&stream_request)
    {
        Ok(WebSocketServerboundMessage::Request(request)) => request,
        Ok(message) => {
            error!(message = ?message, "expected web socket stream request but got another message");
            return Err(AppError::StreamClosed);
        }
        Err(err) => {
            error!(error = %err, "failed to deserialize json");
            return Err(AppError::StreamClosed);
        }
    };

    // -- Get host
    let host_id = HostId(stream_request.host_id);
    let mut host = user.host(host_id).await?;

    let host = host.use_host(&mut user).await?;

    if !host.is_paired().await.map_err(AppError::from)? {
        return Err(AppError::HostNotPaired);
    }

    // -- Get Apps
    let app_id = AppId(stream_request.app_id);
    let apps = host.app_list().await?;
    let app_title = apps
        .into_iter()
        .find(|app| app.id == app_id)
        .map(|app| app.title);

    // -- Start stream
    // get settings
    let mut settings = MoonlightStreamSettings {
        width: stream_request.width,
        height: stream_request.height,
        fps: stream_request.fps,
        fps_x100: stream_request.fps * 100,
        bitrate: stream_request.bitrate,
        packet_size: 2048,
        encryption_flags: EncryptionFlags::AUDIO | EncryptionFlags::FOUNDATION_MICROPHONE,
        streaming_remotely: StreamingConfig::Auto,
        sops: true,
        hdr: stream_request.hdr,
        supported_video_formats: VideoFormats::from_bits_retain(stream_request.supported_codecs),
        // TODO: color range?
        color_space: ColorSpace::Rec709,
        color_range: ColorRange::Limited,
        local_audio_play_mode: stream_request.local_audio_play_mode,
        audio_config: AudioConfig::STEREO,
        gamepads_attached: ActiveGamepads::empty(),
        gamepads_persist_after_disconnect: false,
        // TODO: mic?
        enable_mic: false,
    };

    // apply permissions
    apply_role_restrictions(&permissions, &mut settings);

    // adjust settings
    let server_version = host.version().await?;
    let gfe_version = host.gfe_version().await?;
    let server_codec_mode_support = host.server_codec_mode_support().await?;
    settings.adjust_for_server(server_version, &gfe_version, server_codec_mode_support)?;

    // encryption
    let aes_key = AesKey::new_random(&RustCryptoBackend)?;
    let aes_iv = AesIv::new_random(&RustCryptoBackend)?;

    info!(settings = ?settings, "starting stream");

    // start stream
    let config = host
        .start_stream(
            app_id,
            &settings,
            aes_key,
            aes_iv,
            MoonlightStreamSetup::launch_query_parameters(),
        )
        .await?;

    let stream = MoonlightStream::connect(
        config,
        settings,
        Arc::new(RustCryptoBackend),
        VideoCapabilities::default(),
    )
    .await?;

    // send stream start response
    let audio_setup = stream.audio_setup();
    let video_setup = stream.video_setup();

    let response = WebSocketClientboundMessage::Response(WebSocketStreamResponse {
        video_codec: video_setup.format as u32,
        audio_sample_rate: audio_setup.sample_rate,
        audio_channel_count: audio_setup.channel_count,
        audio_streams: audio_setup.streams,
        audio_coupled_streams: audio_setup.coupled_streams,
        audio_samples_per_frame: audio_setup.samples_per_frame,
        audio_mapping: audio_setup.mapping,
        app_name: app_title,
    });
    info!(response = ?response, "sending response to client");

    let (mut ws_channel_sender, mut ws_channel_receiver) = unbounded_channel();

    let outbound_backlog = Arc::new(AtomicUsize::new(0));
    let dropping_video = Arc::new(AtomicBool::new(false));

    let outbound_backlog_sender = outbound_backlog.clone();
    let dropping_video_sender = dropping_video.clone();
    spawn(
        async move {
            while let Some(data) = ws_channel_receiver.recv().await {
                if let WsData::Video(_) = &data
                    && dropping_video_sender.load(Ordering::Relaxed)
                {
                    outbound_backlog_sender.fetch_sub(data.len(), Ordering::Relaxed);
                    continue;
                }

                let len = data.len();
                let result = match data {
                    WsData::Bytes(bytes) | WsData::Video(bytes) => ws_sender.binary(bytes).await,
                    WsData::Text(text) => ws_sender.text(text).await,
                };
                outbound_backlog_sender.fetch_sub(len, Ordering::Relaxed);

                if result.is_err() {
                    break;
                }
            }

            debug!("stopped web socket sending task");
        }
        .instrument(debug_span!("ws_sender")),
    );

    // send response
    send_ws_message(&mut ws_channel_sender, &outbound_backlog, response);

    // main loop
    if let Err(err) = ws_loop(
        ws_channel_sender,
        outbound_backlog,
        dropping_video,
        ws_receiver,
        stream,
        control_config,
    )
    .await
    {
        error!(error = %err, "web socket main loop errored, closing stream");
    }

    Ok(())
}

async fn ws_loop(
    mut ws_sender: UnboundedSender<WsData>,
    outbound_backlog: Arc<AtomicUsize>,
    dropping_video: Arc<AtomicBool>,
    mut ws_receiver: MessageStream,
    mut stream: MoonlightStream,
    control_config: ControlPacketConfig,
) -> Result<(), AppError> {
    let mut relay_stats_ticker = pin!(interval(Duration::from_secs(1)));

    let mut ws_stopped = false;

    loop {
        if !stream.is_alive() {
            break;
        }

        select! {
            // drive the moonlight stream forward
            result = stream.drive() => {
                let event = result?;

                match event {
                    MoonlightStreamEvent::Audio(AudioStreamEvent::OnFrame(frame)) => {
                        let mut buffer = vec![0; 1 + frame.buffer.len()];
                        buffer[1..].copy_from_slice(&frame.buffer);

                        buffer[0] = WebSocketChannel::AUDIO;

                        let _ = queue_ws_data(&ws_sender, &outbound_backlog, WsData::Bytes(buffer.into()));
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::SignalIdr) => {
                        if let Err(err)=  stream.send_raw(ControlPacket::RequestIdr) {
                            warn!(error = %err, "failed to request idr after the moonlight video stream requested an idr");
                        }
                    }
                    MoonlightStreamEvent::Video(VideoStreamEvent::OnFrame(frame)) => {
                        // TODO: make frame type from video packet public, 2==Idr
                        let is_idr = frame.metadata().frame_type.serialize() == 2;
                        let backlog = outbound_backlog.load(Ordering::Relaxed);

                        if dropping_video.load(Ordering::Relaxed) {
                            if !is_idr || backlog > RESUME_OUTBOUND_BACKLOG_BYTES {
                                if is_idr
                                    && let Err(err) = stream.send_raw(ControlPacket::RequestIdr)
                                {
                                    warn!(error = %err, "failed to request idr after dropping video frames");
                                }

                                continue;
                            }

                            dropping_video.store(false, Ordering::Relaxed);
                        } else if backlog > MAX_OUTBOUND_BACKLOG_BYTES {
                            warn!(
                                backlog = backlog,
                                "web socket video backlog too large, dropping video until the next idr"
                            );
                            dropping_video.store(true, Ordering::Relaxed);

                            if let Err(err) = stream.send_raw(ControlPacket::RequestIdr) {
                                warn!(error = %err, "failed to request idr after dropping video frames");
                            }

                            continue;
                        }

                        // TODO: avoid using payloading and depayloading the frame like this
                        let mut buffer = vec![0; 1 + 5 + frame.raw().len()];
                        buffer[(1 + 5)..].copy_from_slice(frame.raw());

                        buffer[0] = WebSocketChannel::VIDEO;
                        buffer[1] = if is_idr { 1 } else { 0 };
                        buffer[2..6].copy_from_slice(
                            &(frame.metadata().timestamp.as_micros() as u32).to_be_bytes(),
                        );

                        let _ = queue_ws_data(
                            &ws_sender,
                            &outbound_backlog,
                            WsData::Video(buffer.into()),
                        );
                    }
                    MoonlightStreamEvent::Control(ControlStreamEvent::Packet(packet)) => {
                        let mut buffer = vec![0; ControlPacket::MAX_SIZE + 1];

                        buffer[0] = WebSocketChannel::CONTROL;

                        #[allow(clippy::unwrap_used)]
                        let packet_len = packet
                            .serialize(&control_config, buffer[1..].as_mut_array().unwrap())
                            .unwrap();

                        buffer.truncate(1 + packet_len);
                        let _ = queue_ws_data(&ws_sender, &outbound_backlog, WsData::Bytes(buffer.into()));
                    }
                    _ => {}
                }
            }
            // relay stats
            _ = relay_stats_ticker.tick() => {
                let rtt = match stream.estimated_rtt() {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(error = %err, "failed to send rtt to client");
                        break;
                    }
                };

                send_ws_message(
                    &mut ws_sender,
                    &outbound_backlog,
                    WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::RelayRtt {
                        rtt_ms: rtt.rtt.as_millis() as u32,
                        rtt_variance_ms: rtt.rtt_variance.as_millis() as u32,
                    })
                );
            }
            // Handle incoming ws requests
            result = ws_receiver.recv(), if !ws_stopped => {
                let Some(Ok(message)) = result else {
                    ws_stopped = true;
                    let _ = stream.disconnect();
                    continue;
                };

                match message {
                    Message::Binary(message) => {
                        if message.is_empty() {
                            continue;
                        }

                        if message[0] == WebSocketChannel::CONTROL {
                            let Some(packet) = ControlPacket::deserialize(
                                PacketDirection::ServerBound,
                                &control_config,
                                &message[1..],
                            ) else {
                                warn!(message = ?message, "received unknown control packet");
                                continue;
                            };

                            if let Err(err) = stream.send_raw(packet) {
                                warn!(error = %err, "failed to send control packet");
                            }
                        }
                    }
                    Message::Text(text) => {
                        let message = match serde_json::from_str::<WebSocketServerboundMessage>(&text) {
                            Ok(value) => value,
                            Err(err) => {
                                warn!(error = %err, "failed to deserialize serverbound web socket message");
                                continue;
                            }
                        };

                        if let WebSocketServerboundMessage::Stats(StreamStatsServerboundMessage::Ping(id)) =
                            message
                        {
                            send_ws_message(&mut ws_sender, &outbound_backlog, WebSocketClientboundMessage::Stats(StreamStatsClientboundMessage::Pong(id)));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

fn queue_ws_data(
    sender: &UnboundedSender<WsData>,
    outbound_backlog: &AtomicUsize,
    data: WsData,
) -> bool {
    let len = data.len();
    outbound_backlog.fetch_add(len, Ordering::Relaxed);

    if let Err(err) = sender.send(data) {
        outbound_backlog.fetch_sub(len, Ordering::Relaxed);
        warn!(error = %err, "failed to send web socket message");
        return false;
    }

    true
}

fn send_ws_message(
    sender: &mut UnboundedSender<WsData>,
    outbound_backlog: &AtomicUsize,
    message: WebSocketClientboundMessage,
) -> bool {
    trace!(message = ?message, "sending text message to client");

    let text = match serde_json::to_string(&message) {
        Ok(value) => value,
        Err(err) => {
            warn!(error = %err, "failed to send web socket message");
            return false;
        }
    };

    queue_ws_data(sender, outbound_backlog, WsData::Text(text))
}
