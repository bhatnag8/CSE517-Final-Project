import random
import time
import simpy

# Constants for the experiment
ARRIVAL_RATE = 85  # Task arrival rate (tasks per unit time)
SERVICE_RATE = 0.01  # Task service rate (tasks per unit time)
NUM_SERVERS = 100     # Number of servers (m)
BUFFER_SIZE = 0    # Buffer size (r) - how many tasks the queue can store
SIMULATION_TIME = 1000  # Total simulation time (time units)

# Performance metrics
task_arrival_count = 0  # Counter for tasks that arrived
task_completion_count = 0  # Counter for tasks that completed processing
blocked_tasks = 0  # Counter for tasks that were blocked due to a full buffer
total_response_time = 0  # Sum of response times for completed tasks


# Task class to represent each task
class Task:
    def __init__(self, task_id, arrival_time):
        self.task_id = task_id
        self.arrival_time = arrival_time
        self.start_time = None
        self.completion_time = None
        self.server_id = None

    def start_processing(self, server_id, start_time):
        self.server_id = server_id
        self.start_time = start_time

    def finish_processing(self, completion_time):
        self.completion_time = completion_time


# Server class to represent each server
class Server:
    def __init__(self, server_id):
        self.server_id = server_id
        self.is_free = True  # Initially, the server is free

    def occupy(self):
        self.is_free = False

    def release(self):
        self.is_free = True


# Logging function to track events
def log_event(event, task=None, server=None):
    timestamp = round(time.time(), 2)  # Current time
    if event == "ARRIVAL":
        print(f"TASK {task.task_id} ARRIVED TIME {task.arrival_time}s ACCEPT - SERVER {server.server_id}")
    elif event == "QUEUED":
        print(f"TASK {task.task_id} ARRIVED TIME {task.arrival_time}s ACCEPT BUFFER ({len(queue.items)}/{BUFFER_SIZE})")
    elif event == "BLOCKED":
        print(f"TASK {task.task_id} ARRIVED TIME {task.arrival_time}s BLOCKED BUFFER ({len(queue.items)}/{BUFFER_SIZE})")
    elif event == "COMPLETED":
        print(f"TASK {task.task_id} COMPLETED AT TIME {timestamp}s SERVER {server.server_id}")


# Task Arrival Process (Poisson Process)
def task_arrivals(env, queue, servers):
    global task_arrival_count, blocked_tasks
    current_time = 0  # Start simulation time at 0

    while current_time < SIMULATION_TIME:  # Continue until simulation time is reached
        inter_arrival_time = random.expovariate(ARRIVAL_RATE)  # Poisson arrival
        arrival_time = current_time + inter_arrival_time
        current_time = arrival_time  # Update current time

        # Create the new task
        task_arrival_count += 1
        task = Task(task_arrival_count, arrival_time)

        # Check if there's a free server
        server_available = None
        for server in servers:
            if server.is_free:  # If the server is free
                server_available = server
                break

        if server_available:
            # If a server is free, assign the task to it immediately
            server_available.occupy()
            task.start_processing(server_available.server_id, arrival_time)
            log_event("ARRIVAL", task, server_available)
            env.process(process_task(env, task, server_available))
        else:
            # If no servers are free, check if buffer has space
            if len(queue.items) < BUFFER_SIZE:
                queue.put(task)  # Queue the task if space is available
                log_event("QUEUED", task, None)
            else:
                blocked_tasks += 1  # Increment blocked task count
                log_event("BLOCKED", task, None)

        # Wait for the next task to arrive
        yield env.timeout(1)


# Task Processing Function
def process_task(env, task, server):
    global task_completion_count, total_response_time
    # Simulate processing time
    service_time = random.expovariate(SERVICE_RATE)
    yield env.timeout(service_time)  # Simulate service time

    # Mark task as processed
    task.finish_processing(env.now)
    server.release()  # Free the server after task completion
    task_completion_count += 1

    total_response_time += task.completion_time - task.arrival_time
    log_event("COMPLETED", task, server)


# Queue management
queue = None  # This will be set inside the environment
servers = [Server(i) for i in range(1, NUM_SERVERS + 1)]  # Create server objects


# Run the simulation using SimPy
def run_simulation():
    global queue
    print("Starting simulation...\n")

    # Initialize the simulation environment
    env = simpy.Environment()

    # Create the queue with SimPy's Store, associated with the environment
    queue = simpy.Store(env)

    # Start the task arrival process
    env.process(task_arrivals(env, queue, servers))

    # Process tasks in the queue while task arrivals happen concurrently
    while task_arrival_count < SIMULATION_TIME * ARRIVAL_RATE:
        for task in queue.items[:]:  # Loop through tasks in the queue
            # Try to process queued tasks if a server is available
            for server in servers:
                if server.is_free:  # Check if any server is free to process a queued task
                    server.occupy()
                    task.start_processing(server.server_id, env.now)
                    log_event("ARRIVAL", task, server)
                    env.process(process_task(env, task, server))
                    queue.items.remove(task)  # Remove the task from the queue once it's processed
                    break
        env.run(until=env.now + 1)  # Run the event loop for 1 more time unit
        # print("reaching=")

    # Print final statistics
    print(f"\nSimulation Complete\nTasks Arrived: {task_arrival_count}")
    print(f"Tasks Completed: {task_completion_count}")
    print(f"Tasks Blocked: {blocked_tasks}")
    print(f"Tasks Blocked PROB: {blocked_tasks/task_arrival_count}")

    print(f"Average Response Time: {total_response_time / task_completion_count if task_completion_count > 0 else 0:.2f} seconds")


if __name__ == "__main__":
    run_simulation()
