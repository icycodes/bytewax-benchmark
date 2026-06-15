import os
import shutil
import subprocess
import sys

def main():
    project_dir = "/home/user/bytewax_project"
    input_dir = os.path.join(project_dir, "input_data")
    output_dir = os.path.join(project_dir, "output_data")
    recovery_dir = os.path.join(project_dir, "recovery_dir")

    # 1. Prepare input_data/test.txt
    os.makedirs(input_dir, exist_ok=True)
    test_input_path = os.path.join(input_dir, "test.txt")
    with open(test_input_path, "w") as f:
        for i in range(1, 11):
            f.write(f"line {i}\n")

    # 2. Clear output_data and recovery_dir
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(recovery_dir):
        shutil.rmtree(recovery_dir)
    os.makedirs(recovery_dir, exist_ok=True)

    # 3. Initialize recovery partition
    print("Initializing recovery partition...")
    subprocess.run(
        ["python3", "-m", "bytewax.recovery", recovery_dir, "1"],
        check=True,
        cwd=project_dir
    )

    # 4. Run first time with SLOW_DOWN and CRASH_ON_LINE_6
    print("\n--- Running pipeline (First Run - Expected to Crash) ---")
    env = os.environ.copy()
    env["SLOW_DOWN"] = "true"
    env["CRASH_ON_LINE_6"] = "true"

    proc1 = subprocess.run(
        ["python3", "-m", "bytewax.run", "pipeline:flow", "-r", recovery_dir, "-s", "1", "-b", "0"],
        env=env,
        cwd=project_dir,
        capture_output=True,
        text=True
    )

    print(f"First run exited with code: {proc1.returncode}")
    if "Intentional crash on line 6" in proc1.stderr:
        print("Confirmed: Pipeline crashed as expected with 'Intentional crash on line 6'")
    else:
        print("Warning: Did not find expected crash message in stderr.")
        print("Stderr was:", proc1.stderr)

    # Check intermediate output file
    test_output_path = os.path.join(output_dir, "test.txt")
    if os.path.exists(test_output_path):
        with open(test_output_path, "r") as f:
            lines = f.readlines()
        print(f"Intermediate output has {len(lines)} lines:")
        for line in lines:
            print(f"  {line.strip()}")
    else:
        print("No intermediate output file written yet (snapshotted state is 0).")

    # 5. Resume pipeline (Second Run - Expected to Succeed)
    print("\n--- Resuming pipeline (Second Run) ---")
    env["SLOW_DOWN"] = "false"
    env["CRASH_ON_LINE_6"] = "false"

    proc2 = subprocess.run(
        ["python3", "-m", "bytewax.run", "pipeline:flow", "-r", recovery_dir, "-s", "1", "-b", "0"],
        env=env,
        cwd=project_dir,
        capture_output=True,
        text=True
    )

    print(f"Second run exited with code: {proc2.returncode}")
    if proc2.returncode != 0:
        print("Error: Second run failed!")
        print("Stderr was:", proc2.stderr)
        sys.exit(1)

    # 6. Verify final output
    print("\n--- Verifying Final Output ---")
    if not os.path.exists(test_output_path):
        print("Error: Final output file does not exist!")
        sys.exit(1)

    with open(test_output_path, "r") as f:
        final_lines = [line.strip() for line in f.readlines()]

    expected_lines = [f"LINE {i}" for i in range(1, 11)]

    print(f"Final output lines (total {len(final_lines)}):")
    for line in final_lines:
        print(f"  {line}")

    if final_lines == expected_lines:
        print("\nSUCCESS: Exactly-once recovery test passed perfectly!")
    else:
        print(f"\nFAILURE: Expected {expected_lines}, but got {final_lines}")
        sys.exit(1)

if __name__ == "__main__":
    main()
