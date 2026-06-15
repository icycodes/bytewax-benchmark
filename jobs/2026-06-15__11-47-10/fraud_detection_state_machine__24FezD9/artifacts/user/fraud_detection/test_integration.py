import json
import subprocess
import pathlib

def run_test_scenario(name, events, expected_alerts):
    print(f"Running test scenario: {name}")
    input_file = pathlib.Path(f"input_{name}.jsonl")
    output_file = pathlib.Path(f"output_{name}.jsonl")

    # Write input file
    with open(input_file, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    # Run run.py
    cmd = ["python", "run.py", "--input", str(input_file), "--output", str(output_file)]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Failed to run run.py for scenario {name}:")
        print(res.stderr)
        assert False, f"run.py exited with non-zero code {res.returncode}"

    # Read output file
    output_alerts = []
    if output_file.exists():
        with open(output_file, "r") as f:
            for line in f:
                if line.strip():
                    output_alerts.append(json.loads(line))

    # Assert correctness
    assert len(output_alerts) == len(expected_alerts), f"Expected {len(expected_alerts)} alerts, got {len(output_alerts)}: {output_alerts}"
    for got, expected in zip(output_alerts, expected_alerts):
        assert got == expected, f"Expected alert {expected}, got {got}"

    # Clean up
    input_file.unlink(missing_ok=True)
    output_file.unlink(missing_ok=True)
    print(f"Scenario {name} passed successfully!\n")

def main():
    # Scenario 1: Normal user login and transactions under 1000
    events_1 = [
        {"user_id": "user1", "event_type": "login", "timestamp": 0},
        {"user_id": "user1", "event_type": "transaction", "amount": 500, "timestamp": 10},
        {"user_id": "user1", "event_type": "transaction", "amount": 999, "timestamp": 20},
        {"user_id": "user1", "event_type": "logout", "timestamp": 30},
    ]
    run_test_scenario("normal_behavior", events_1, [])

    # Scenario 2: 3 large transactions within 300 seconds
    events_2 = [
        {"user_id": "user2", "event_type": "login", "timestamp": 100},
        {"user_id": "user2", "event_type": "transaction", "amount": 1000, "timestamp": 150},
        {"user_id": "user2", "event_type": "transaction", "amount": 2000, "timestamp": 200},
        {"user_id": "user2", "event_type": "transaction", "amount": 1050, "timestamp": 250},
    ]
    run_test_scenario("fraud_pattern", events_2, [{"user_id": "user2", "alert": "FRAUD_ALERT"}])

    # Scenario 3: Transaction exactly on 300 seconds limit (300 seconds after login)
    events_3 = [
        {"user_id": "user3", "event_type": "login", "timestamp": 100},
        {"user_id": "user3", "event_type": "transaction", "amount": 1000, "timestamp": 150},
        {"user_id": "user3", "event_type": "transaction", "amount": 1000, "timestamp": 200},
        {"user_id": "user3", "event_type": "transaction", "amount": 1000, "timestamp": 400}, # exactly 300 seconds
    ]
    run_test_scenario("exact_300s_limit", events_3, [{"user_id": "user3", "alert": "FRAUD_ALERT"}])

    # Scenario 4: Transaction after 300 seconds limit (301 seconds after login)
    events_4 = [
        {"user_id": "user4", "event_type": "login", "timestamp": 100},
        {"user_id": "user4", "event_type": "transaction", "amount": 1000, "timestamp": 150},
        {"user_id": "user4", "event_type": "transaction", "amount": 1000, "timestamp": 200},
        {"user_id": "user4", "event_type": "transaction", "amount": 1000, "timestamp": 401}, # 301 seconds -> Reset and ignore!
    ]
    run_test_scenario("after_300s_limit", events_4, [])

    # Scenario 5: Multiple users interleaved
    events_5 = [
        {"user_id": "userA", "event_type": "login", "timestamp": 100},
        {"user_id": "userB", "event_type": "login", "timestamp": 110},
        {"user_id": "userA", "event_type": "transaction", "amount": 1000, "timestamp": 120},
        {"user_id": "userB", "event_type": "transaction", "amount": 1000, "timestamp": 130},
        {"user_id": "userA", "event_type": "transaction", "amount": 1000, "timestamp": 140},
        {"user_id": "userB", "event_type": "logout", "timestamp": 150},
        {"user_id": "userA", "event_type": "transaction", "amount": 1000, "timestamp": 160}, # userA alert!
        {"user_id": "userB", "event_type": "login", "timestamp": 170},
        {"user_id": "userB", "event_type": "transaction", "amount": 1000, "timestamp": 180},
        {"user_id": "userB", "event_type": "transaction", "amount": 1000, "timestamp": 190},
        {"user_id": "userB", "event_type": "transaction", "amount": 1000, "timestamp": 200}, # userB alert!
    ]
    run_test_scenario("interleaved_users", events_5, [
        {"user_id": "userA", "alert": "FRAUD_ALERT"},
        {"user_id": "userB", "alert": "FRAUD_ALERT"}
    ])

    print("All integration tests passed successfully!")

if __name__ == "__main__":
    main()
