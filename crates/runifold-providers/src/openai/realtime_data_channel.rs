//! Bounded browser WebRTC data-channel transport.

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use futures_channel::{mpsc, oneshot};
use futures_util::StreamExt;
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{Event, MessageEvent, RtcDataChannel};

use super::realtime_transport::RealtimeTransportError;

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const RECEIVE_QUEUE_CAPACITY: usize = 32;
const MAX_BROWSER_BUFFERED_BYTES: u32 = 1024 * 1024;

enum Incoming {
    Text(String),
    Binary,
    Closed {
        code: u16,
        clean: bool,
        reason: &'static str,
    },
    TransportError,
}

pub(crate) struct RealtimeDataChannelTransport {
    channel: RtcDataChannel,
    receiver: mpsc::Receiver<Incoming>,
    opened: Option<oneshot::Receiver<bool>>,
    overflowed: Rc<Cell<bool>>,
    _on_open: Closure<dyn FnMut(Event)>,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_error: Closure<dyn FnMut(Event)>,
    _on_close: Closure<dyn FnMut(Event)>,
}

impl std::fmt::Debug for RealtimeDataChannelTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RealtimeDataChannelTransport")
            .field("label", &self.channel.label())
            .finish_non_exhaustive()
    }
}

impl RealtimeDataChannelTransport {
    pub(crate) fn attach(channel: RtcDataChannel) -> Self {
        let (sender, receiver) = mpsc::channel(RECEIVE_QUEUE_CAPACITY);
        let sender = Rc::new(RefCell::new(sender));
        let overflowed = Rc::new(Cell::new(false));
        let (opened_sender, opened) = oneshot::channel();
        let opened_sender = Rc::new(RefCell::new(Some(opened_sender)));

        let on_open = {
            let opened_sender = Rc::clone(&opened_sender);
            Closure::wrap(Box::new(move |_event: Event| {
                if let Some(sender) = opened_sender.borrow_mut().take() {
                    let _ = sender.send(true);
                }
            }) as Box<dyn FnMut(Event)>)
        };
        channel.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        let on_message = {
            let sender = Rc::clone(&sender);
            let channel = channel.clone();
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
                    channel.close();
                }
            }) as Box<dyn FnMut(MessageEvent)>)
        };
        channel.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        let on_error = {
            let sender = Rc::clone(&sender);
            let opened_sender = Rc::clone(&opened_sender);
            Closure::wrap(Box::new(move |_event: Event| {
                if let Some(sender) = opened_sender.borrow_mut().take() {
                    let _ = sender.send(false);
                }
                let _ = sender.borrow_mut().try_send(Incoming::Closed {
                    code: 1006,
                    clean: false,
                    reason: "WebRTC data channel failed",
                });
            }) as Box<dyn FnMut(Event)>)
        };
        channel.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        let on_close = {
            let sender = Rc::clone(&sender);
            let opened_sender = Rc::clone(&opened_sender);
            Closure::wrap(Box::new(move |_event: Event| {
                if let Some(sender) = opened_sender.borrow_mut().take() {
                    let _ = sender.send(false);
                }
                let _ = sender.borrow_mut().try_send(Incoming::Closed {
                    code: 1000,
                    clean: true,
                    reason: "WebRTC data channel closed",
                });
            }) as Box<dyn FnMut(Event)>)
        };
        channel.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        Self {
            channel,
            receiver,
            opened: Some(opened),
            overflowed,
            _on_open: on_open,
            _on_message: on_message,
            _on_error: on_error,
            _on_close: on_close,
        }
    }

    pub(crate) async fn wait_open(&mut self) -> Result<(), RealtimeTransportError> {
        let opened = self.opened.take().ok_or(RealtimeTransportError::Connect)?;
        match opened.await {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => Err(RealtimeTransportError::Connect),
        }
    }

    pub(crate) fn send_text(&mut self, text: &str) -> Result<(), RealtimeTransportError> {
        if text.len() > MAX_FRAME_BYTES {
            return Err(RealtimeTransportError::FrameTooLarge);
        }
        if self.channel.buffered_amount() > MAX_BROWSER_BUFFERED_BYTES {
            return Err(RealtimeTransportError::ReceiveOverflow);
        }
        self.channel
            .send_with_str(text)
            .map_err(|_| RealtimeTransportError::Transport)
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
                reason: reason.into(),
            }),
            None => Ok(None),
        }
    }

    pub(crate) fn close(&mut self) {
        self.channel.close();
    }
}
