import random
import simpy
import numpy as np
import matplotlib.pyplot as plt

# Constants for the experiment
ARRIVAL_RATE = 85  # Task arrival rate (tasks per unit time)
SERVICE_RATE = 1.0  # Task service rate (tasks per unit time)
NUM_SERVERS = 100  # Number of servers (m)
BUFFER_SIZE = 0  # Buffer size (r)
SIMULATION_TIME = 1000  # Total simulation time (time units)
COV = 1.5  # Coefficient of Variation (for service times, e.g., CoV=1 means exponential)

# Performance metrics
task_arrival_count = 0
task_completion_count = 0
blocked_tasks = 0
total_response_time = 0


# Task Arrival Process (Poisson Process)
def task_arrivals(env, task_queue):
    global task_arrival_count
    while True:
        inter_arrival_time = random.expovariate(ARRIVAL_RATE)  # Poisson arrival
        yield env.timeout(inter_arrival_time)
        task_arrival_count += 1
        task_queue.put(1)  # New task arrives in the queue
        print(f"Task arrived at time {env.now}")


# Gamma distribution for service time with CoV
def service_time_with_cov(cov, service_rate):
    # We know CoV = 1 / sqrt(k), so to get a specific CoV, we adjust k
    shape = 1 / (cov ** 2)  # Shape parameter for the Gamma distribution
    scale = service_rate * cov  # Scale parameter for the Gamma distribution
    return np.random.gamma(shape, scale)


# Task Processing (M/G/m/m+r Queuing System)
def task_processing(env, task_queue, num_servers, service_rate):
    global task_completion_count, blocked_tasks, total_response_time
    servers = [simpy.Resource(env, capacity=1) for _ in range(num_servers)]  # Each server is a resource
    while True:
        task = yield task_queue.get()  # Get a task from the queue
        task_start_time = env.now  # Track when the task starts processing
        server = random.choice(servers)  # Randomly select a server to process the task

        # Wait for a server to become available
        with server.request() as req:
            yield req
            # Simulate task processing time with Gamma distribution and CoV
            service_time = service_time_with_cov(COV, service_rate)  # CoV is considered here
            yield env.timeout(service_time)

            # Calculate response time and update metrics
            response_time = env.now - task_start_time
            total_response_time += response_time
            task_completion_count += 1
            print(f"Task completed at time {env.now} with response time {response_time}")


# Blocking Condition: When buffer is full
def task_blocked(env, task_queue, num_servers, buffer_size):
    global blocked_tasks
    while True:
        if len(task_queue.items) >= buffer_size:  # If buffer is full, block the task
            blocked_tasks += 1
            print(f"Task blocked at time {env.now}, buffer size: {len(task_queue.items)}")
            yield env.timeout(1)  # Wait for some time before checking again
        else:
            yield env.timeout(1)


# Monitor performance metrics: Blocking probability, response time, throughput
def monitor_metrics():
    blocking_probability = blocked_tasks / task_arrival_count if task_arrival_count > 0 else 0
    throughput = task_completion_count / SIMULATION_TIME
    avg_response_time = total_response_time / task_completion_count if task_completion_count > 0 else 0
    print(f"\nBlocking Probability: {blocking_probability:.4f}")
    print(f"Throughput: {throughput:.4f} tasks per unit time")
    print(f"Average Response Time: {avg_response_time:.4f} time units")
    return blocking_probability, throughput, avg_response_time


# Main function to run the simulation
def run_simulation():
    global task_arrival_count, task_completion_count, blocked_tasks, total_response_time

    # Initialize the simulation environment
    env = simpy.Environment()
    task_queue = simpy.Store(env)  # Queue for incoming tasks

    # Start the processes
    env.process(task_arrivals(env, task_queue))  # Task arrival process
    env.process(task_processing(env, task_queue, NUM_SERVERS, SERVICE_RATE))  # Task processing
    env.process(task_blocked(env, task_queue, NUM_SERVERS, BUFFER_SIZE))  # Task blocking due to full buffer

    # Run the simulation
    env.run(until=SIMULATION_TIME)

    # Monitor and print the performance metrics after the simulation
    monitor_metrics()

    # Visualize the results (optional)
    # You can uncomment the following to plot graphs after running the experiment
    # plot_results()


# Optional: Function to plot graphs for the results (blocking probability, throughput, response time)
def plot_results():
    global task_arrival_count, task_completion_count, blocked_tasks, total_response_time
    blocking_probability, throughput, avg_response_time = monitor_metrics()

    # Plot the results
    fig, axs = plt.subplots(3, 1, figsize=(10, 12))

    # Blocking Probability
    axs[0].plot([blocking_probability], label='Blocking Probability', marker='o')
    axs[0].set_title('Blocking Probability')
    axs[0].set_xlabel('Simulation Runs')
    axs[0].set_ylabel('Blocking Probability')

    # Throughput
    axs[1].plot([throughput], label='Throughput', marker='o')
    axs[1].set_title('Throughput')
    axs[1].set_xlabel('Simulation Runs')
    axs[1].set_ylabel('Tasks Processed per Time Unit')

    # Response Time
    axs[2].plot([avg_response_time], label='Average Response Time', marker='o')
    axs[2].set_title('Average Response Time')
    axs[2].set_xlabel('Simulation Runs')
    axs[2].set_ylabel('Time Units')

    plt.tight_layout()
    plt.show()


# Running the experiment
if __name__ == "__main__":
    run_simulation()
