use std::rc::Rc;

#[derive(Debug)]
pub struct State<St>(Rc<St>);

impl<St> State<St> {
    pub(crate) fn new(st: St) -> Self {
        State(Rc::new(st))
    }

    pub fn get_ref(&self) -> &St {
        self.0.as_ref()
    }
}

impl<St> Clone for State<St> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<St> std::ops::Deref for State<St> {
    type Target = St;

    fn deref(&self) -> &Self::Target {
        self.get_ref()
    }
}
