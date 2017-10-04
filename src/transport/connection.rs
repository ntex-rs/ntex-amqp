use futures::prelude::*;
use futures::{future, AsyncSink, Future, Sink, Stream};
use futures::unsync::oneshot;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_core::reactor;

use errors::*;
use io::AmqpCodec;
use framing::AmqpFrame;
use bytes::Bytes;

use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::collections::VecDeque;

use super::session::*;
use super::*;

pub struct Connection {
    inner: Rc<RefCell<ConnectionInner>>,
}

pub(crate) struct ConnectionInner {
    write_queue: VecDeque<AmqpFrame>,
    write_task: Option<Task>,
    sessions: HandleVec<Weak<RefCell<SessionInner>>>,
    channels: HandleVec<()>,
    pending_sessions: Vec<SessionRequest>,
}

struct SessionRequest {
    channel: u16,
    promise: oneshot::Sender<Session>,
}

struct ConnectionTransport<T: Sink<SinkItem = AmqpFrame, SinkError = Error> + 'static> {
    sink: T,
    connection: Rc<RefCell<ConnectionInner>>,
    flushed: bool,
}

impl Connection {
    #[async]
    pub fn open<T: AsyncRead + AsyncWrite + 'static>(hostname: String, handle: reactor::Handle, io: T) -> Result<Connection> {
        let io = await!(negotiate_protocol(ProtocolId::Amqp, io))?;

        let io = io.framed(AmqpCodec::<AmqpFrame>::new());
        let io = await!(open_connection(hostname, io))?;
        Ok(Connection::new(handle, io))
    }

    fn new<T: AsyncRead + AsyncWrite + 'static>(handle: reactor::Handle, io: Framed<T, AmqpCodec<AmqpFrame>>) -> Connection {
        let (writer, reader) = io.split();
        let connection = Rc::new(RefCell::new(ConnectionInner::new()));
        let conn_transport = ConnectionTransport {
            sink: writer,
            connection: connection.clone(),
            flushed: true,
        };
        let reader_conn = connection.clone();
        let read_handling = reader.for_each(move |frame| {
            reader_conn
                .borrow_mut()
                .handle_frame(frame, reader_conn.clone());
            Ok(())
        });
        handle.spawn(read_handling.map_err(|e| {
            // todo: handle error while reading
            println!("Error reading: {:?}", e);
        }));
        handle.spawn(conn_transport.map_err(|e| {
            // todo: handle error while writing
            println!("Error writing: {:?}", e);
        }));
        Connection { inner: connection }
    }

    pub fn close() -> impl Future<Item = (), Error = Error> {
        future::ok(())
    }

    /// Opens the session
    pub fn open_session(&self) -> impl Future<Item = Session, Error = Error> {
        self.inner.borrow_mut().open_session()
    }
}

impl<T: Sink<SinkItem = AmqpFrame, SinkError = Error> + 'static> Future for ConnectionTransport<T> {
    type Item = ();
    type Error = Error;

    // Tick the state machine
    fn poll(&mut self) -> Poll<(), Error> {
        // TODO: Always tick the transport first -- heartbeat, etc.
        // self.dispatch.get_mut().inner.transport().tick();

        let mut conn = self.connection.borrow_mut();

        loop {
            loop {
                if let Some(frame) = conn.pop_next_frame() {
                    match self.sink.start_send(frame) {
                        Ok(AsyncSink::NotReady(frame)) => {
                            conn.prepend_frame(frame);
                            break;
                        }
                        Ok(AsyncSink::Ready) => {
                            //let _ = tx.send(Ok(())); todo: feedback for write out?
                            self.flushed = false;
                            continue;
                        }
                        Err(e) => {
                            return Err(e.into());
                            // let _ = tx.send(Err(err));
                        }
                    }
                } else {
                    break;
                }
            }

            let mut not_ready = true;

            // flush sink
            if !self.flushed {
                match self.sink.poll_complete() {
                    Ok(Async::Ready(_)) => {
                        not_ready = false;
                        self.flushed = true;
                        conn.set_write_task(task::current());
                    }
                    Ok(Async::NotReady) => (),
                    Err(e) => return Err(e.into()),
                };
            }

            if not_ready {
                return Ok(Async::NotReady);
            }
        }
    }
}

impl ConnectionInner {
    pub fn new() -> ConnectionInner {
        ConnectionInner {
            write_queue: VecDeque::new(),
            write_task: None,
            sessions: HandleVec::new(),
            channels: HandleVec::new(),
            pending_sessions: vec![],
        }
    }

    fn pop_next_frame(&mut self) -> Option<AmqpFrame> {
        self.write_queue.pop_front()
    }

    fn prepend_frame(&mut self, frame: AmqpFrame) {
        self.write_queue.push_front(frame);
    }

    pub fn post_frame(&mut self, frame: AmqpFrame) {
        self.write_queue.push_back(frame);
        if let Some(task) = self.write_task.take() {
            task.notify();
        }
    }

    fn set_write_task(&mut self, task: Task) {
        self.write_task = Some(task);
    }

    pub fn handle_frame(&mut self, frame: AmqpFrame, self_rc: Rc<RefCell<ConnectionInner>>) {
        match *frame.performative() {
            Frame::Begin(ref begin) if begin.remote_channel().is_some() => {
                self.complete_session_creation(frame.channel_id(), begin, self_rc);
                return;
            }
            // todo: handle Close, End?
            _ => {} // todo: handle unexpected frames
        }

        if let Some(session) = self.sessions
            .get(frame.channel_id() as u32)
            .and_then(|sr| sr.upgrade())
        {
            session
                .borrow_mut()
                .handle_frame(frame, session.clone(), self);
        } else {
            // todo: missing session
        }
    }

    fn complete_session_creation(&mut self, channel_id: u16, begin: &Begin, self_rc: Rc<RefCell<ConnectionInner>>) {
        if let Some(index) = self.pending_sessions
            .iter()
            .position(|r| r.channel == channel_id)
        {
            let req = self.pending_sessions.remove(index);
            let session = Rc::new(RefCell::new(SessionInner::new(
                self_rc,
                begin.remote_channel().unwrap(),
                begin.incoming_window(),
                begin.next_outgoing_id(),
                begin.outgoing_window(),
            )));
            self.sessions
                .set(req.channel as u32, Rc::downgrade(&session));
            let _ = req.promise.send(Session::new(session));
        } else {
            // todo: rogue begin right now - do nothing. in future might indicate incoming attach
        }
    }

    pub fn open_session(&mut self) -> impl Future<Item = Session, Error = Error> {
        let local_channel = self.channels.push(()) as u16;
        let (tx, rx) = oneshot::channel();
        self.pending_sessions.push(SessionRequest {
            channel: local_channel,
            promise: tx,
        });

        let begin = Begin {
            // todo: let user specify settings
            remote_channel: None,
            next_outgoing_id: 1,
            incoming_window: 0,
            outgoing_window: ::std::u32::MAX,
            handle_max: ::std::u32::MAX,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        self.post_frame(AmqpFrame::new(
            local_channel,
            Frame::Begin(begin),
            Bytes::new(),
        ));
        rx.map_err(|e| "Canceled".into())
    }
}

/// Performs connection opening.
#[async]
fn open_connection<T>(hostname: String, io: T) -> Result<T>
where
    T: Stream<Item = AmqpFrame, Error = Error> + Sink<SinkItem = AmqpFrame, SinkError = Error> + 'static,
{
    let open = Open {
        container_id: ByteStr::from(&Uuid::new_v4().simple().to_string()[..]),
        hostname: Some(ByteStr::from(&hostname[..])),
        max_frame_size: ::std::u16::MAX as u32,
        channel_max: 1,                     //::std::u16::MAX,
        idle_time_out: Some(2 * 60 * 1000), // 2 min
        outgoing_locales: None,
        incoming_locales: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    let io = await!(io.send(AmqpFrame::new(0, Frame::Open(open), Bytes::new())))?;
    let (frame_opt, io) = await!(io.into_future()).map_err(|e| e.0)?;

    if let Some(frame) = frame_opt {
        //println!("rx: {:?}", frame);
        if let Frame::Open(ref open) = *frame.performative() {
            Ok(io)
        } else {
            Err(
                format!(
                    "Expected Open performative to arrive, seen `{:?}` instead.",
                    frame
                ).into(),
            )
        }
    } else {
        Err("Connection is closed.".into())
    }
}
