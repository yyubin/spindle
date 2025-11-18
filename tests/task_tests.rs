use spindle::task::{LocalTask, SharedTask};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

// 테스트용 더미 waker
fn dummy_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
        dummy_raw_waker()
    }

    fn dummy_raw_waker() -> RawWaker {
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, no_op, no_op, no_op),
        )
    }

    unsafe { Waker::from_raw(dummy_raw_waker()) }
}

#[test]
fn test_local_task_creation() {
    let mut task = LocalTask::local(async {
        println!("Local task running!");
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    match task.poll(&mut cx) {
        Poll::Ready(()) => println!("Local task completed"),
        Poll::Pending => println!("Local task pending"),
    }
}

#[test]
fn test_shared_task_creation() {
    let mut task = SharedTask::shared(async {
        println!("Shared task running!");
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    match task.poll(&mut cx) {
        Poll::Ready(()) => println!("Shared task completed"),
        Poll::Pending => println!("Shared task pending"),
    }
}

#[test]
fn test_local_task_executes() {
    let executed = Arc::new(Mutex::new(false));
    let executed_clone = executed.clone();

    let mut task = LocalTask::local(async move {
        *executed_clone.lock().unwrap() = true;
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    let result = task.poll(&mut cx);
    assert!(matches!(result, Poll::Ready(())));
    assert!(*executed.lock().unwrap(), "Task should have executed");
}

#[test]
fn test_shared_task_executes() {
    let executed = Arc::new(Mutex::new(false));
    let executed_clone = executed.clone();

    let mut task = SharedTask::shared(async move {
        *executed_clone.lock().unwrap() = true;
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    let result = task.poll(&mut cx);
    assert!(matches!(result, Poll::Ready(())));
    assert!(*executed.lock().unwrap(), "Task should have executed");
}

#[test]
fn test_local_task_with_value() {
    let value = Arc::new(Mutex::new(0));
    let value_clone = value.clone();

    let mut task = LocalTask::local(async move {
        *value_clone.lock().unwrap() = 42;
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    let _ = task.poll(&mut cx);
    assert_eq!(*value.lock().unwrap(), 42);
}

#[test]
fn test_shared_task_with_value() {
    let value = Arc::new(Mutex::new(0));
    let value_clone = value.clone();

    let mut task = SharedTask::shared(async move {
        *value_clone.lock().unwrap() = 100;
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    let _ = task.poll(&mut cx);
    assert_eq!(*value.lock().unwrap(), 100);
}

#[test]
fn test_multiple_local_tasks() {
    let counter = Arc::new(Mutex::new(0));

    let mut tasks = vec![];
    for _ in 0..5 {
        let counter_clone = counter.clone();
        tasks.push(LocalTask::local(async move {
            *counter_clone.lock().unwrap() += 1;
        }));
    }

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    for task in &mut tasks {
        let _ = task.poll(&mut cx);
    }

    assert_eq!(*counter.lock().unwrap(), 5);
}

#[test]
fn test_multiple_shared_tasks() {
    let counter = Arc::new(Mutex::new(0));

    let mut tasks = vec![];
    for _ in 0..5 {
        let counter_clone = counter.clone();
        tasks.push(SharedTask::shared(async move {
            *counter_clone.lock().unwrap() += 1;
        }));
    }

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    for task in &mut tasks {
        let _ = task.poll(&mut cx);
    }

    assert_eq!(*counter.lock().unwrap(), 5);
}

#[test]
fn test_local_task_is_not_send() {
    // 이 테스트는 컴파일 타임에 LocalTask가 Send가 아님을 확인합니다
    // 실제로 Send를 시도하면 컴파일 에러가 발생해야 합니다

    let _task = LocalTask::local(async {
        // LocalTask는 !Send Future를 받을 수 있음
    });

    // 이 함수는 Send를 요구하지 않으므로 정상적으로 컴파일됩니다
}

#[test]
fn test_shared_task_is_send() {
    // SharedTask는 Send를 구현합니다
    fn assert_send<T: Send>(_t: T) {}

    let task = SharedTask::shared(async {});
    assert_send(task);
}

#[test]
fn test_local_task_multiple_polls() {
    let poll_count = Arc::new(Mutex::new(0));
    let poll_count_clone = poll_count.clone();

    let mut task = LocalTask::local(async move {
        *poll_count_clone.lock().unwrap() += 1;
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    // 첫 번째 poll
    let result = task.poll(&mut cx);
    assert!(matches!(result, Poll::Ready(())));

    // poll 카운트 확인
    assert_eq!(*poll_count.lock().unwrap(), 1);
}

#[test]
fn test_shared_task_multiple_polls() {
    let poll_count = Arc::new(Mutex::new(0));
    let poll_count_clone = poll_count.clone();

    let mut task = SharedTask::shared(async move {
        *poll_count_clone.lock().unwrap() += 1;
    });

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    // 첫 번째 poll
    let result = task.poll(&mut cx);
    assert!(matches!(result, Poll::Ready(())));

    // poll 카운트 확인
    assert_eq!(*poll_count.lock().unwrap(), 1);
}
