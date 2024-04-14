#![allow(dead_code)]

use std::future::Future;
use std::rc::Rc;

use std::task::{Poll, Context, Wake};
use std::pin::Pin;


use std::sync::Arc;

type BoxedFut = Pin<Box<dyn Future<Output = ()>>>;

pub struct CyclicRuntime{
    runnable_queue: JobQueue,
    num_to_do: usize
}


struct Node{
   job: BoxedFut,
   next: Option<Rc<Box<Node>>>
}

impl Node{
    fn new(job: BoxedFut) -> Self{
        Self {job, next: None}
    }
}


type SharedNode = Option<Rc<Box<Node>>>;
struct JobQueue{
    size: usize,
    head: SharedNode,
    tail: SharedNode
}

impl JobQueue{   
    fn new() -> Self{
        Self {size: 0usize, head: None, tail: None}
    }
    fn pop(&mut self) -> Result<BoxedFut, String>{
        match self.size{
            0 => Err(format!("nothing to pop")),
            1 => {
               drop(self.tail.take()); // decrease the ref count to strict one
               let next_to_pop = Rc::into_inner(self.head.take().unwrap()).unwrap();
               self.size = 0usize;
               Ok(next_to_pop.job)
            }
            _ => {
                let next_to_pop = self.head.take().unwrap();
                self.head = Some(Rc::clone(next_to_pop.next.as_ref().unwrap()));
                self.size -= 1;
                Ok(Rc::into_inner(next_to_pop).unwrap().job)
            }
        }
    }
    
    fn push(&mut self, job: BoxedFut){
       let new_node = Rc::new(Box::new(Node::new(job)));
       match self.size {
          0 => {
            self.head = Some(Rc::clone(&new_node));
            self.tail = self.head.clone();
          },
          1 => {
            drop(self.tail.take());
            let mut tail_raw_node = self.head.take().unwrap();
            Rc::get_mut(&mut tail_raw_node).unwrap().next = Some(Rc::clone(&new_node));
            self.head = Some(tail_raw_node);
            self.tail = Some(new_node);
          },
          _ => {
            let mut tail_raw_node = self.tail.take().unwrap();
            Rc::get_mut(&mut tail_raw_node).unwrap().next = Some(Rc::clone(&new_node));
            self.tail = Some(new_node);
          }
       }
       self.size += 1;
    }
}

struct FreePassWaker;
impl Wake for FreePassWaker{
    fn wake(self: Arc<Self>){
        println!("try to wake")
    }
}

impl CyclicRuntime{
    pub fn new(jobs: Vec<BoxedFut>) -> Self{
        let num_to_do = jobs.len();
        let mut runnable_queue = JobQueue::new();
        for job in jobs{
            runnable_queue.push(job);
        }
        Self {runnable_queue, num_to_do}
    }


    pub fn exec(mut self){
        let default_waker = Arc::new(FreePassWaker).into();
        while self.num_to_do != 0 {
            let mut next_to_do = self.runnable_queue.pop().expect("should have at least one node");
            let mut ctx = Context::from_waker(&default_waker);
                
            match next_to_do.as_mut().poll(&mut ctx) {
               Poll::Ready(_) => {
                   self.num_to_do -= 1;
               }
               Poll::Pending => {
                   self.runnable_queue.push(next_to_do);
               }
            }
        }
        
    }

    
}



use std::collections::{HashMap, VecDeque};

use std::io::Write;
use std::fs::File;
use std::os::fd::{RawFd, AsRawFd, FromRawFd};
use tempfile::tempfile;



struct BundledFut{
    job: BoxedFut,
    alarm_fd: RawFd
}

// impl BundledFut {
//     fn to_kevent(&self) -> kqueue::Event{
//         kqueue::Event{
//             ident: kqueue::Ident::Fd(self.alarm_fd.into()),
//             data: kqueue::EventData::Vnode(kqueue::Vnode::Write) 
//         }
//     }
// }


struct KqueueWaker{
   fd: RawFd 
}

impl KqueueWaker{
    fn new(fd: RawFd) -> Self{
        Self {fd}
    }
}

impl Wake for KqueueWaker{
    fn wake(self: Arc<Self>){
        // SAFETY: file associated with fd is guaranteed to exist
        let mut buf = [b'a'; 1];
        let mut f = unsafe {File::from_raw_fd(self.fd)};
        f.write(&mut buf).unwrap();
        std::thread::sleep(std::time::Duration::from_micros(50));
    }
}

pub struct KqueueRuntime{
    // futures are owned by this map
    future_alarm: HashMap<RawFd, BoxedFut>,
    created_tmp_file: Vec<File>,
    runnable_queue: VecDeque<BundledFut>,
    num_unfinished: usize,
    kq_watcher: kqueue::Watcher
}

impl KqueueRuntime{
    
   /* exec policy: if current job cannot do, push it to the back of queue 
    * improvemnt:
    *    make the executor thread block on empty runnable job queue
    *    we need to introduce a mechanism of waking up the executor thread via kqueue
    *    register a read to a pre-created tmp file, so that the event is notified via kqueue
    *    so that executor knows it can poll the associated task
    *    
    * design choice:
    *    need to know mapping between file descriptor to the underlying future 
    */
    
    pub fn new(jobs: Vec<BoxedFut>) -> Self{
        let num_jobs = jobs.len();
        let mut created_tmp_file = Vec::with_capacity(num_jobs);
        let future_alarm = HashMap::new();

        for _ in 0..num_jobs{
            created_tmp_file.push(tempfile().unwrap());
        }

        // initial state: all job can be polled
        let mut runnable_queue = VecDeque::with_capacity(num_jobs);
        for (i, fut) in jobs.into_iter().enumerate(){
            runnable_queue.push_back(
                BundledFut {job: fut, alarm_fd: created_tmp_file[i].as_raw_fd()}
            )
        }
        let kq_watcher = kqueue::Watcher::new().unwrap();
        let mut runtime = Self {
            created_tmp_file,
            runnable_queue, future_alarm,
            num_unfinished: num_jobs,
            kq_watcher
        };
        runtime.register_kevent();
        runtime
    }


    fn register_kevent(&mut self){
        for f_handler in &self.created_tmp_file {
            self.kq_watcher.add_file(
                f_handler,
                kqueue::EventFilter::EVFILT_VNODE,
                kqueue::FilterFlag::NOTE_WRITE
            ).expect("unable to register");
        }
        self.kq_watcher.watch().expect("unable to start kqueue");
    }



    fn fetch_next_runnable(&mut self) -> BundledFut{
        if self.runnable_queue.len() == 0{
           // blocking until there are job woken 
           let event = self.kq_watcher.poll_forever(None).unwrap();
           if let kqueue::Ident::Fd(fd) = event.ident{
               let associated_fut = self.future_alarm.remove(&fd).unwrap();
               self.runnable_queue.push_back(
                   BundledFut {job: associated_fut, alarm_fd: fd}
               )
           }
        }
        
        self.runnable_queue.pop_front().unwrap()
    }

    pub fn exec(mut self) {
       while self.num_unfinished > 0 {
           // blocking on this
           let mut next_to_do = self.fetch_next_runnable();
           let kq_waker = Arc::new(KqueueWaker::new(next_to_do.alarm_fd)).into();
           let mut kq_ctx = Context::from_waker(&kq_waker);

           match next_to_do.job.as_mut().poll(&mut kq_ctx){
              Poll::Ready(_) => {
                 self.num_unfinished -= 1;
                 self.kq_watcher.remove_fd(next_to_do.alarm_fd, kqueue::EventFilter::EVFILT_VNODE)
                      .expect("unsuccessful removal from the kqueue");
              },
              // if still can not finish, put it back to future_alarm & wait for next round of
              // waking up
              Poll::Pending => {
                 self.future_alarm.insert(next_to_do.alarm_fd, next_to_do.job); 
              } 
           }
       } 
    }
    
}

#[cfg(test)]
mod test{
    use super::{CyclicRuntime, BoxedFut};
    async fn spinning(sec: u64){
       let duration = std::time::Duration::from_secs(sec);
       async_std::task::sleep(duration).await;
    }
    async fn hello(){
        spinning(1).await;
        println!("hello world")
    }
    
    // #[test]
    fn test_jobqueue(){
        let jobvec: Vec<BoxedFut> = vec![
            Box::pin(hello()), Box::pin(hello())
        ];
        let executor = CyclicRuntime::new(jobvec);
        let start = std::time::Instant::now();
        executor.exec();
        let elapsed = start.elapsed();
        println!("time: {:.2?}", elapsed);
    }


    use super::KqueueRuntime;
    
    #[test]
    fn test_kq_runtime(){
        let jobvec: Vec<BoxedFut> = vec![
            Box::pin(hello()), Box::pin(hello())
        ];
        let executor = KqueueRuntime::new(jobvec);
        let start = std::time::Instant::now();
        executor.exec();
        let elapsed = start.elapsed();
        println!("time: {:.2?}", elapsed);
    }


    
}

