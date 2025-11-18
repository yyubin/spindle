use spindle::task::LocalTask;
use spindle::worker::Worker;

fn main() {
    println!("Spindle runtime starting...\n");

    // 워커 생성
    let mut worker = Worker::new(0);

    // 일반 작업들을 큐에 추가
    worker.push(LocalTask::local(async {
        println!("Task 1 executing");
    }));

    worker.push(LocalTask::local(async {
        println!("Task 2 executing");
    }));

    worker.push(LocalTask::local(async {
        println!("Task 3 executing");
    }));

    // 최우선 작업 설정 (next 슬롯)
    worker.set_next(LocalTask::local(async {
        println!("PRIORITY: Next task executing (runs first!)");
    }));

    println!("Starting worker execution...\n");

    // 워커 실행 - next 작업이 먼저 실행됨
    worker.run();

    println!("\nWorker finished!");
}