"""
Start all three voting nodes with one command:
    python start_nodes.py
Press Ctrl+C to stop all nodes.
"""
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PORTS = [5001, 5002, 5003]


def main():
    procs = []
    for port in PORTS:
        p = subprocess.Popen([sys.executable, os.path.join(HERE, "node.py"), str(port)], cwd=HERE)
        procs.append(p)
        print(f"Started node on port {port} (pid {p.pid})")
        time.sleep(0.5)
    print("\nAll nodes running. Open frontend/index.html in your browser.")
    print("Press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
            for port, p in zip(PORTS, procs):
                if p.poll() is not None:
                    print(f"Node on port {port} exited with code {p.returncode}")
                    procs.remove(p); PORTS.remove(port)
                    break
    except KeyboardInterrupt:
        print("\nStopping nodes...")
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
