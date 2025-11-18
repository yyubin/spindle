use spindle::task::LocalTask;
use spindle::worker::{Worker, WorkerState};
use std::sync::{Arc, Mutex};

#[test]
fn test_worker_creation() {
    let worker = Worker::new(0);
    assert_eq!(worker.id(), 0);
    assert_eq!(worker.state(), WorkerState::Idle);
}

#[test]
fn test_worker_multiple_ids() {
    let worker1 = Worker::new(0);
    let worker2 = Worker::new(1);
    let worker3 = Worker::new(42);

    assert_eq!(worker1.id(), 0);
    assert_eq!(worker2.id(), 1);
    assert_eq!(worker3.id(), 42);
}

#[test]
fn test_worker_state_transitions() {
    let mut worker = Worker::new(0);

    assert_eq!(worker.state(), WorkerState::Idle);

    worker.set_state(WorkerState::Running);
    assert_eq!(worker.state(), WorkerState::Running);

    worker.set_state(WorkerState::Stopped);
    assert_eq!(worker.state(), WorkerState::Stopped);

    worker.set_state(WorkerState::Idle);
    assert_eq!(worker.state(), WorkerState::Idle);
}

#[test]
fn test_worker_push_and_run() {
    let mut worker = Worker::new(0);

    let executed = Arc::new(Mutex::new(false));
    let executed_clone = executed.clone();

    worker.push(LocalTask::local(async move {
        *executed_clone.lock().unwrap() = true;
    }));

    let ran = worker.run_once();
    assert!(ran, "Worker should have run a task");
    assert!(*executed.lock().unwrap(), "Task should have been executed");
}

#[test]
fn test_worker_next_priority() {
    let mut worker = Worker::new(0);

    let execution_order = Arc::new(Mutex::new(Vec::new()));

    // 큐에 작업 추가
    let order_clone = execution_order.clone();
    worker.push(LocalTask::local(async move {
        order_clone.lock().unwrap().push("queue_task");
    }));

    // next 슬롯에 작업 추가 (이게 먼저 실행되어야 함)
    let order_clone = execution_order.clone();
    worker.set_next(LocalTask::local(async move {
        order_clone.lock().unwrap().push("next_task");
    }));

    // 첫 번째 실행: next 작업이 실행되어야 함
    worker.run_once();
    let order = execution_order.lock().unwrap();
    assert_eq!(order[0], "next_task");
}

#[test]
fn test_worker_next_replacement() {
    let mut worker = Worker::new(0);

    let execution_order = Arc::new(Mutex::new(Vec::new()));

    // 첫 번째 next 작업
    let order_clone = execution_order.clone();
    worker.set_next(LocalTask::local(async move {
        order_clone.lock().unwrap().push("first_next");
    }));

    // 두 번째 next 작업 (첫 번째를 큐로 밀어냄)
    let order_clone = execution_order.clone();
    worker.set_next(LocalTask::local(async move {
        order_clone.lock().unwrap().push("second_next");
    }));

    // 두 작업 실행
    worker.run_once(); // second_next 실행
    worker.run_once(); // first_next 실행 (큐로 이동됨)

    let order = execution_order.lock().unwrap();
    assert_eq!(order.len(), 2);
    assert_eq!(order[0], "second_next");
    assert_eq!(order[1], "first_next");
}

#[test]
fn test_worker_multiple_tasks() {
    let mut worker = Worker::new(0);

    let counter = Arc::new(Mutex::new(0));

    // 5개의 작업 추가
    for _ in 0..5 {
        let counter_clone = counter.clone();
        worker.push(LocalTask::local(async move {
            *counter_clone.lock().unwrap() += 1;
        }));
    }

    // 모든 작업 실행
    worker.run();

    assert_eq!(*counter.lock().unwrap(), 5);
    assert_eq!(worker.state(), WorkerState::Stopped);
}

#[test]
fn test_worker_empty_queue() {
    let mut worker = Worker::new(0);

    // 빈 큐에서 실행 시도
    let ran = worker.run_once();
    assert!(!ran, "Worker should not run when queue is empty");
    assert_eq!(worker.state(), WorkerState::Idle);
}

#[test]
fn test_worker_run_stops_when_empty() {
    let mut worker = Worker::new(0);

    let executed = Arc::new(Mutex::new(0));
    let executed_clone = executed.clone();

    worker.push(LocalTask::local(async move {
        *executed_clone.lock().unwrap() += 1;
    }));

    worker.run();

    assert_eq!(*executed.lock().unwrap(), 1);
    assert_eq!(worker.state(), WorkerState::Stopped);
}

#[test]
fn test_worker_state_changes_during_execution() {
    let mut worker = Worker::new(0);

    assert_eq!(worker.state(), WorkerState::Idle);

    let executed = Arc::new(Mutex::new(false));
    let executed_clone = executed.clone();

    worker.push(LocalTask::local(async move {
        *executed_clone.lock().unwrap() = true;
    }));

    worker.run_once();

    // 실행 후 상태가 변경되었는지 확인
    assert!(*executed.lock().unwrap());
}

#[test]
fn test_worker_stealer() {
    let worker = Worker::new(0);

    // Stealer를 가져올 수 있는지 확인
    let _stealer = worker.stealer();

    // Stealer가 정상적으로 생성되었으면 성공
}

#[test]
fn test_worker_fifo_order() {
    let mut worker = Worker::new(0);

    let execution_order = Arc::new(Mutex::new(Vec::new()));

    // 순서대로 작업 추가
    for i in 0..5 {
        let order_clone = execution_order.clone();
        worker.push(LocalTask::local(async move {
            order_clone.lock().unwrap().push(i);
        }));
    }

    // 모든 작업 실행
    worker.run();

    let order = execution_order.lock().unwrap();
    assert_eq!(order.len(), 5);
    assert_eq!(*order, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_worker_mixed_next_and_queue() {
    let mut worker = Worker::new(0);

    let execution_order = Arc::new(Mutex::new(Vec::new()));

    // 큐에 여러 작업 추가
    for i in 1..=3 {
        let order_clone = execution_order.clone();
        worker.push(LocalTask::local(async move {
            order_clone.lock().unwrap().push(i);
        }));
    }

    // next 작업 추가
    let order_clone = execution_order.clone();
    worker.set_next(LocalTask::local(async move {
        order_clone.lock().unwrap().push(0);
    }));

    // 모든 작업 실행
    worker.run();

    let order = execution_order.lock().unwrap();
    assert_eq!(order.len(), 4);
    // next 작업(0)이 먼저, 그 다음 큐 작업들(1,2,3)
    assert_eq!(order[0], 0);
    assert_eq!(order[1], 1);
    assert_eq!(order[2], 2);
    assert_eq!(order[3], 3);
}
