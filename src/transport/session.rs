use futures::{future, Future};
use futures::unsync::oneshot;
use bytes::{Bytes, BytesMut};
use uuid::Uuid;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

use errors::*;
use types::ByteStr;
use protocol::*;
use framing::AmqpFrame;
use codec::Encode;
use super::*;

#[derive(Clone)]
pub struct Session {
    inner: Rc<RefCell<SessionInner>>,
}

impl Session {
    pub(crate) fn new(inner: Rc<RefCell<SessionInner>>) -> Session {
        Session { inner }
    }

    pub fn close() -> impl Future<Item = (), Error = Error> {
        future::ok(())
    }

    pub fn open_sender_link(&self, address: String, name: String) -> impl Future<Item = SenderLink, Error = Error> {
        self.inner.borrow_mut().open_sender_link(address, name)
    }
}

pub(crate) struct SessionInner {
    connection: Rc<RefCell<ConnectionInner>>,
    remote_channel_id: u16,
    next_outgoing_id: DeliveryNumber,
    outgoing_window: u32,
    next_incoming_id: DeliveryNumber,
    incoming_window: u32,
    unsettled_deliveries: BTreeMap<DeliveryNumber, oneshot::Sender<Result<()>>>,
    links: HandleVec<Weak<RefCell<SenderLinkInner>>>,
    handles: HandleVec<()>,
    pending_links: Vec<LinkRequest>,
    pending_transfers: VecDeque<PendingTransfer>,
}

struct PendingTransfer {
    link_handle: Handle,
    message: Message,
    promise: DeliveryPromise,
}

impl SessionInner {
    pub fn new(connection: Rc<RefCell<ConnectionInner>>, remote_channel_id: u16, outgoing_window: u32, next_incoming_id: DeliveryNumber, incoming_window: u32) -> SessionInner {
        SessionInner {
            connection,
            remote_channel_id,
            next_outgoing_id: 1,
            outgoing_window: outgoing_window,
            next_incoming_id,
            incoming_window,
            unsettled_deliveries: BTreeMap::new(),
            links: HandleVec::new(),
            handles: HandleVec::new(),
            pending_links: vec![],
            pending_transfers: VecDeque::new(),
        }
    }

    pub fn handle_frame(&mut self, frame: AmqpFrame, self_rc: Rc<RefCell<SessionInner>>, conn: &mut ConnectionInner) {
        match *frame.performative() {
            Frame::Attach(ref attach) => self.complete_link_creation(attach, self_rc),
            Frame::Disposition(ref disp) => self.settle_deliveries(disp),
            Frame::Flow(ref flow) => self.apply_flow(conn, flow),
            // todo: handle Detach, End
            _ => {
                // todo: handle unexpected frames
            }
        }
    }

    fn complete_link_creation(&mut self, attach: &Attach, self_rc: Rc<RefCell<SessionInner>>) {
        let name = attach.name();
        if let Some(index) = self.pending_links.iter().position(|r| r.name == *name) {
            let req = self.pending_links.remove(index);
            let link = Rc::new(RefCell::new(SenderLinkInner::new(self_rc, attach.handle())));
            self.links.set(req.handle, Rc::downgrade(&link));
            let _ = req.promise.send(SenderLink::new(link));
        } else {
            // todo: rogue attach right now - do nothing. in future will indicate incoming attach
        }
    }

    fn settle_deliveries(&mut self, disposition: &Disposition) {
        assert!(disposition.settled()); // we can only work with settled for now
        let from = disposition.first;
        let to = disposition.last.unwrap_or(from);
        let actionable = self.unsettled_deliveries
            .range(from..to + 1)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        for k in actionable {
            self.unsettled_deliveries.remove(&k).unwrap().send(Ok(()));
        }
    }

    fn apply_flow(&mut self, conn: &mut ConnectionInner, flow: &Flow) {
        self.outgoing_window = flow.next_incoming_id().unwrap_or(0) + flow.incoming_window() - self.next_outgoing_id;
        while let Some(t) = self.pending_transfers.pop_front() {
            self.send_transfer_conn(conn, t.link_handle, t.message, t.promise);
            if self.outgoing_window == 0 {
                break;
            }
        }
        if let Some(link) = flow.handle()
            .and_then(|h| self.links.get(h))
            .and_then(|lr| lr.upgrade())
        {
            link.borrow_mut().apply_flow(flow, self, conn);
        } else if flow.echo() {
            self.send_flow(conn);
        }
    }

    fn send_flow(&mut self, conn: &mut ConnectionInner) {
        let flow = Flow {
            next_incoming_id: Some(self.next_incoming_id), // todo: derive from begin/flow
            incoming_window: self.incoming_window,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.outgoing_window,
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: false,
            echo: false,
            properties: None,
        };
        self.post_frame_conn(conn, Frame::Flow(flow), Bytes::new());
    }

    fn post_frame(&mut self, frame: Frame, payload: Bytes) {
        self.post_frame_conn(&mut self.connection.borrow_mut(), frame, payload);
    }

    fn post_frame_conn(&self, conn: &mut ConnectionInner, frame: Frame, payload: Bytes) {
        let channel_id = self.remote_channel_id;
        conn.post_frame(AmqpFrame::new(channel_id, frame, payload));
    }

    pub fn open_sender_link(&mut self, address: String, name: String) -> impl Future<Item = SenderLink, Error = Error> {
        let local_handle = self.handles.push(());
        let (tx, rx) = oneshot::channel();
        let name = ByteStr::from(&name[..]);
        self.pending_links.push(LinkRequest {
            handle: local_handle,
            name: name.clone(),
            promise: tx,
        });

        let target = Target {
            address: Some(ByteStr::from(&address[..])),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            dynamic: false,
            dynamic_node_properties: None,
            capabilities: None,
        };
        let attach = Attach {
            name: name,
            handle: local_handle,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: Some(target),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        self.post_frame(Frame::Attach(attach), Bytes::new());
        rx.map_err(|e| "Canceled".into())
    }

    pub fn send_transfer(&mut self, link_handle: Handle, message: Message, promise: DeliveryPromise) {
        // todo: DRY
        if self.outgoing_window == 0 {
            // todo: queue up instead
            self.pending_transfers.push_back(PendingTransfer {
                link_handle,
                message,
                promise,
            });
            return;
        }
        let (frame, body) = self.prepare_transfer(link_handle, message, promise);
        self.post_frame(frame, body);
    }

    pub fn send_transfer_conn(&mut self, conn: &mut ConnectionInner, link_handle: Handle, message: Message, promise: DeliveryPromise) {
        // todo: DRY
        if self.outgoing_window == 0 {
            // todo: queue up instead
            self.pending_transfers.push_back(PendingTransfer {
                link_handle,
                message,
                promise,
            });
            return;
        }
        let (frame, body) = self.prepare_transfer(link_handle, message, promise);
        self.post_frame_conn(conn, frame, body);
    }

    pub fn prepare_transfer(&mut self, link_handle: Handle, message: Message, promise: DeliveryPromise) -> (Frame, Bytes) {
        self.outgoing_window -= 1;
        let delivery_id = self.next_outgoing_id;
        self.next_outgoing_id += 1;
        let delivery_tag = Bytes::from(&Uuid::new_v4().as_bytes()[..]);
        let transfer = Transfer {
            handle: link_handle,
            delivery_id: Some(delivery_id),
            delivery_tag: Some(delivery_tag.clone()),
            message_format: None,
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        };
        self.unsettled_deliveries.insert(delivery_id, promise);
        (Frame::Transfer(transfer), message.serialize())
    }
}

struct LinkRequest {
    handle: Handle,
    name: ByteStr,
    promise: oneshot::Sender<SenderLink>,
}
