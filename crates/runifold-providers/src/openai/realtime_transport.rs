//! Target-specific bounded WebSocket transport for `OpenAI` Realtime.

use std::collections::BTreeMap;

use thiserror::Error;

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub(crate) enum RealtimeTransportError {
    #[error("Realtime WebSocket connection failed")]
    Connect,
    #[error("Realtime WebSocket transport failed")]
    Transport,
    #[error("Realtime WebSocket closed (code {code}, clean: {clean})")]
    Closed {
        code: u16,
        clean: bool,
        reason: String,
    },
    #[error("Realtime WebSocket received a non-text frame")]
    BinaryFrame,
    #[error("Realtime WebSocket frame exceeds the {MAX_FRAME_BYTES} byte limit")]
    FrameTooLarge,
    #[cfg(target_arch = "wasm32")]
    #[error("Realtime browser receive queue exceeded its bounded capacity")]
    ReceiveOverflow,
}

#[derive(Debug)]
pub(crate) struct RealtimeConnectOptions {
    pub(crate) url: String,
    pub(crate) headers: BTreeMap<String, String>,
}

#[cfg(not(target_arch = "wasm32"))]
mod target {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        MaybeTlsStream, WebSocketStream, connect_async,
        tungstenite::{
            Message,
            client::IntoClientRequest,
            http::{HeaderName, HeaderValue},
        },
    };

    use super::{MAX_FRAME_BYTES, RealtimeConnectOptions, RealtimeTransportError};

    type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

    #[derive(Debug)]
    pub(crate) struct RealtimeTransport {
        socket: Socket,
    }

    impl RealtimeTransport {
        pub(crate) async fn connect(
            options: RealtimeConnectOptions,
        ) -> Result<Self, RealtimeTransportError> {
            let mut request = options
                .url
                .into_client_request()
                .map_err(|_| RealtimeTransportError::Connect)?;
            for (name, value) in options.headers {
                let name =
                    HeaderName::try_from(name).map_err(|_| RealtimeTransportError::Connect)?;
                let value =
                    HeaderValue::try_from(value).map_err(|_| RealtimeTransportError::Connect)?;
                request.headers_mut().insert(name, value);
            }
            let (socket, _) = connect_async(request)
                .await
                .map_err(|_| RealtimeTransportError::Connect)?;
            Ok(Self { socket })
        }

        pub(crate) async fn send_text(&mut self, text: &str) -> Result<(), RealtimeTransportError> {
            if text.len() > MAX_FRAME_BYTES {
                return Err(RealtimeTransportError::FrameTooLarge);
            }
            self.socket
                .send(Message::Text(text.into()))
                .await
                .map_err(|_| RealtimeTransportError::Transport)
        }

        pub(crate) async fn next_text(&mut self) -> Result<Option<String>, RealtimeTransportError> {
            while let Some(frame) = self.socket.next().await {
                match frame.map_err(|_| RealtimeTransportError::Transport)? {
                    Message::Text(text) => {
                        if text.len() > MAX_FRAME_BYTES {
                            return Err(RealtimeTransportError::FrameTooLarge);
                        }
                        return Ok(Some(text.to_string()));
                    }
                    Message::Binary(_) => return Err(RealtimeTransportError::BinaryFrame),
                    Message::Close(frame) => {
                        let (code, reason) = frame.map_or((1005, String::new()), |frame| {
                            (frame.code.into(), frame.reason.to_string())
                        });
                        return Err(RealtimeTransportError::Closed {
                            code,
                            clean: true,
                            reason,
                        });
                    }
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            Ok(None)
        }

        pub(crate) async fn close(&mut self) -> Result<(), RealtimeTransportError> {
            self.socket
                .close(None)
                .await
                .map_err(|_| RealtimeTransportError::Transport)
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod target {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    use futures_channel::{mpsc, oneshot};
    use futures_util::StreamExt;
    use js_sys::Uint8Array;
    use wasm_bindgen::{JsCast, closure::Closure};
    use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

    use super::{MAX_FRAME_BYTES, RealtimeConnectOptions, RealtimeTransportError};

    const RECEIVE_QUEUE_CAPACITY: usize = 32;
    const MAX_BROWSER_BUFFERED_BYTES: u32 = 1024 * 1024;

    enum Incoming {
        Text(String),
        Binary,
        Closed {
            code: u16,
            clean: bool,
            reason: String,
        },
        TransportError,
    }

    pub(crate) struct RealtimeTransport {
        socket: WebSocket,
        receiver: mpsc::Receiver<Incoming>,
        overflowed: Rc<Cell<bool>>,
        _on_open: Closure<dyn FnMut(Event)>,
        _on_message: Closure<dyn FnMut(MessageEvent)>,
        _on_error: Closure<dyn FnMut(Event)>,
        _on_close: Closure<dyn FnMut(CloseEvent)>,
    }

    impl std::fmt::Debug for RealtimeTransport {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("RealtimeTransport")
                .field("ready_state", &self.socket.ready_state())
                .finish_non_exhaustive()
        }
    }

    impl RealtimeTransport {
        pub(crate) async fn connect(
            options: RealtimeConnectOptions,
        ) -> Result<Self, RealtimeTransportError> {
            if !options.headers.is_empty() {
                return Err(RealtimeTransportError::Connect);
            }
            let socket =
                WebSocket::new(&options.url).map_err(|_| RealtimeTransportError::Connect)?;
            socket.set_binary_type(BinaryType::Arraybuffer);

            let (sender, receiver) = mpsc::channel(RECEIVE_QUEUE_CAPACITY);
            let sender = Rc::new(RefCell::new(sender));
            let overflowed = Rc::new(Cell::new(false));
            let (opened_sender, opened_receiver) = oneshot::channel();
            let opened_sender = Rc::new(RefCell::new(Some(opened_sender)));

            let on_open = {
                let opened_sender = Rc::clone(&opened_sender);
                Closure::wrap(Box::new(move |_event: Event| {
                    if let Some(sender) = opened_sender.borrow_mut().take() {
                        let _ = sender.send(());
                    }
                }) as Box<dyn FnMut(Event)>)
            };
            socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));

            let on_message = {
                let sender = Rc::clone(&sender);
                let socket = socket.clone();
                let overflowed = Rc::clone(&overflowed);
                Closure::wrap(Box::new(move |event: MessageEvent| {
                    let incoming = if let Some(text) = event.data().as_string() {
                        Incoming::Text(text)
                    } else if event.data().dyn_ref::<js_sys::ArrayBuffer>().is_some()
                        || event.data().dyn_ref::<Uint8Array>().is_some()
                    {
                        Incoming::Binary
                    } else {
                        Incoming::TransportError
                    };
                    if sender.borrow_mut().try_send(incoming).is_err() {
                        overflowed.set(true);
                        let _ = socket.close_with_code_and_reason(1009, "bounded receive queue");
                    }
                }) as Box<dyn FnMut(MessageEvent)>)
            };
            socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

            let on_error = {
                let sender = Rc::clone(&sender);
                Closure::wrap(Box::new(move |_event: Event| {
                    let _ = sender.borrow_mut().try_send(Incoming::TransportError);
                }) as Box<dyn FnMut(Event)>)
            };
            socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));

            let on_close = {
                let sender = Rc::clone(&sender);
                Closure::wrap(Box::new(move |event: CloseEvent| {
                    let _ = sender.borrow_mut().try_send(Incoming::Closed {
                        code: event.code(),
                        clean: event.was_clean(),
                        reason: event.reason(),
                    });
                }) as Box<dyn FnMut(CloseEvent)>)
            };
            socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));

            opened_receiver
                .await
                .map_err(|_| RealtimeTransportError::Connect)?;
            Ok(Self {
                socket,
                receiver,
                overflowed,
                _on_open: on_open,
                _on_message: on_message,
                _on_error: on_error,
                _on_close: on_close,
            })
        }

        pub(crate) fn send_text(
            &mut self,
            text: &str,
        ) -> futures_util::future::Ready<Result<(), RealtimeTransportError>> {
            let result = if text.len() > MAX_FRAME_BYTES {
                Err(RealtimeTransportError::FrameTooLarge)
            } else if self.socket.buffered_amount() > MAX_BROWSER_BUFFERED_BYTES {
                Err(RealtimeTransportError::ReceiveOverflow)
            } else {
                self.socket
                    .send_with_str(text)
                    .map_err(|_| RealtimeTransportError::Transport)
            };
            futures_util::future::ready(result)
        }

        pub(crate) async fn next_text(&mut self) -> Result<Option<String>, RealtimeTransportError> {
            if self.overflowed.replace(false) {
                return Err(RealtimeTransportError::ReceiveOverflow);
            }
            match self.receiver.next().await {
                Some(Incoming::Text(text)) if text.len() <= MAX_FRAME_BYTES => Ok(Some(text)),
                Some(Incoming::Text(_)) => Err(RealtimeTransportError::FrameTooLarge),
                Some(Incoming::Binary) => Err(RealtimeTransportError::BinaryFrame),
                Some(Incoming::TransportError) => Err(RealtimeTransportError::Transport),
                Some(Incoming::Closed {
                    code,
                    clean,
                    reason,
                }) => Err(RealtimeTransportError::Closed {
                    code,
                    clean,
                    reason,
                }),
                None => Ok(None),
            }
        }

        pub(crate) fn close(
            &mut self,
        ) -> futures_util::future::Ready<Result<(), RealtimeTransportError>> {
            futures_util::future::ready(
                self.socket
                    .close_with_code_and_reason(1000, "client closed")
                    .map_err(|_| RealtimeTransportError::Transport),
            )
        }
    }
}

pub(crate) use target::RealtimeTransport;
