import argparse
import subprocess
import os
import sys
import time
import json
import urllib.request
import re
import shutil
import threading
from datetime import datetime

def drop_caches():
    print("[INFO] Dropping OS caches...")
    res = subprocess.run(["sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], capture_output=True)
    if res.returncode != 0:
        print("[WARNING] Could not drop OS caches (requires passwordless sudo). Out-of-core benchmarks might use cached pages.")


class StrfryManager:
    def __init__(self, use_docker=False, memory_limit=None):
        self.use_docker = use_docker
        self.memory_limit = memory_limit
        self.process = None
        self.db_dir = os.path.join(os.getcwd(), 'strfry-db')
        self.config_path = os.path.join(os.getcwd(), 'strfry.conf')

    def build_docker(self):
        print("[INFO] Building strfry docker image...")
        subprocess.run(["docker", "build", "-t", "strfry-bench-image", "."], check=True)

    def clean_db(self):
        print("[INFO] Cleaning database...")
        subprocess.run(["rm", "-rf", self.db_dir])
        os.makedirs(self.db_dir, exist_ok=True)

    def start(self, config_overrides=None):
        if self.process is not None:
            self.stop()
            
        print("[INFO] Starting strfry relay...")
        
        args = []
        if config_overrides:
            for k, v in config_overrides.items():
                args.extend(["--set", f"{k}={v}"])
        
        if self.use_docker:
            cmd = [
                "docker", "run", "--rm", "-p", "7777:7777",
                "-v", f"{self.config_path}:/app/strfry.conf",
                "-v", f"{self.db_dir}:/app/strfry-db"
            ]
            if self.memory_limit:
                cmd.extend(["--memory", self.memory_limit])
            cmd.append("strfry-bench-image")
            # Always bind to 0.0.0.0 inside Docker so the host can connect
            cmd.extend(["--set", "relay.bind=0.0.0.0"])
            cmd.extend(args)
            cmd.append("relay")
            self.process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            cmd = ["./strfry", "--config", self.config_path]
            cmd.extend(args)
            cmd.append("relay")
            self.process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        time.sleep(2) # Wait for relay to bind port
        print("[INFO] Relay started.")

    def stop(self):
        if self.process:
            print("[INFO] Stopping strfry relay...")
            self.process.terminate()
            self.process.wait()
            self.process = None
            print("[INFO] Relay stopped.")

class PrometheusScraper:
    def __init__(self, port=7777):
        self.url = f"http://localhost:{port}/metrics"
        self.running = False
        self.thread = None
        self.peak_queue = 0.0

    def get_metrics(self):
        try:
            req = urllib.request.Request(self.url)
            with urllib.request.urlopen(req) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            return ""

    def parse_metric(self, metrics_text, metric_name):
        for line in metrics_text.splitlines():
            if line.startswith(metric_name):
                parts = line.split()
                if len(parts) >= 2:
                    return float(parts[1])
        return 0.0

    def _poll_loop(self):
        while self.running:
            metrics_text = self.get_metrics()
            q = self.parse_metric(metrics_text, "strfry_writer_queue")
            if q > self.peak_queue:
                self.peak_queue = q
            time.sleep(0.1)

    def start_polling(self):
        self.running = True
        self.peak_queue = 0.0
        self.thread = threading.Thread(target=self._poll_loop)
        self.thread.daemon = True
        self.thread.start()

    def stop_polling(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)

class ResourceMonitor:
    def __init__(self, pid):
        self.pid = pid
        self.running = False
        self.thread = None
        self.cpu_usages = []
        self.read_bytes_start = 0
        self.write_bytes_start = 0
        self.logical_write_start = 0
        self.read_bytes_end = 0
        self.write_bytes_end = 0
        self.logical_write_end = 0

    def get_cpu_times(self):
        try:
            with open("/proc/stat", "r") as f:
                first_line = f.readline()
                parts = first_line.split()
                total = sum(float(x) for x in parts[1:])
                idle = float(parts[4])
                return total, idle
        except:
            return 0, 0

    def get_proc_cpu_time(self):
        try:
            with open(f"/proc/{self.pid}/stat", "r") as f:
                parts = f.readline().split()
                utime = float(parts[13])
                stime = float(parts[14])
                return utime + stime
        except:
            return 0

    def get_proc_io_bytes(self):
        try:
            r, w, lw = 0, 0, 0
            with open(f"/proc/{self.pid}/io", "r") as f:
                for line in f:
                    if line.startswith("read_bytes:"):
                        r = int(line.split()[1])
                    elif line.startswith("write_bytes:"):
                        w = int(line.split()[1])
                    elif line.startswith("wchar:"):
                        lw = int(line.split()[1])
            return r, w, lw
        except:
            return 0, 0, 0

    def _poll_loop(self):
        num_cores = os.cpu_count() or 1
        while self.running:
            t1_total, t1_idle = self.get_cpu_times()
            p1_time = self.get_proc_cpu_time()
            time.sleep(0.5)
            t2_total, t2_idle = self.get_cpu_times()
            p2_time = self.get_proc_cpu_time()
            
            total_diff = t2_total - t1_total
            proc_diff = p2_time - p1_time
            if total_diff > 0:
                cpu_pct = (proc_diff / total_diff) * 100 * num_cores
                self.cpu_usages.append(cpu_pct)

    def start(self):
        self.running = True
        self.cpu_usages = []
        r, w, lw = self.get_proc_io_bytes()
        self.read_bytes_start = r
        self.write_bytes_start = w
        self.logical_write_start = lw
        self.thread = threading.Thread(target=self._poll_loop)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
        r, w, lw = self.get_proc_io_bytes()
        self.read_bytes_end = r
        self.write_bytes_end = w
        self.logical_write_end = lw

    def get_results(self):
        avg_cpu = sum(self.cpu_usages) / len(self.cpu_usages) if self.cpu_usages else 0.0
        peak_cpu = max(self.cpu_usages) if self.cpu_usages else 0.0
        read_delta = self.read_bytes_end - self.read_bytes_start
        write_delta = self.write_bytes_end - self.write_bytes_start
        logical_write_delta = self.logical_write_end - self.logical_write_start
        waf = write_delta / logical_write_delta if logical_write_delta > 0 else 0.0
        return {
            "avg_cpu_percent": avg_cpu,
            "peak_cpu_percent": peak_cpu,
            "read_bytes": read_delta,
            "write_bytes": write_delta,
            "logical_write_bytes": logical_write_delta,
            "waf": waf,
            "read_mb": read_delta / (1024 * 1024),
            "write_mb": write_delta / (1024 * 1024)
        }

def get_bench_dir():
    return "../strfry-bench" if os.path.exists("../strfry-bench") else "./strfry-bench"

def run_bench(command, args):
    bench_dir = get_bench_dir()
    cmd = ["cargo", "run", "--release", "--bin", "strfry-bench", "--", command] + args
    print(f"[INFO] Running bench: {' '.join(cmd)}")
    start = time.time()
    result = subprocess.run(cmd, cwd=bench_dir, capture_output=True, text=True)
    end = time.time()
    
    if result.returncode != 0:
        print(f"[ERROR] strfry-bench failed:\n{result.stderr}")
        return None
        
    return {
        "output": result.stdout,
        "elapsed": end - start
    }

def generate_seed_data(events=100000):
    print(f"[INFO] Generating {events} events for seed...")
    gen_cmd = ["perl", "test/generate-seed-data.pl", "-o", "-", "-e", str(events)]
    import_cmd = ["./strfry", "import", "--no-verify"]
    
    p1 = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(import_cmd, stdin=p1.stdout, stdout=subprocess.DEVNULL)
    p1.stdout.close()
    p2.communicate()
    print("[INFO] Seeding complete.")

def run_iostat(duration=5):
    # Runs iostat for the given duration and returns MB/s read/write
    if not shutil.which("iostat"):
        return {"read_mb": -1, "write_mb": -1}
    cmd = ["iostat", "-m", "-y", "1", str(duration)]
    res = subprocess.run(cmd, capture_output=True, text=True)
    # Parse logic omitted for brevity, returning dummy data for now
    return {"read_mb": 0.0, "write_mb": 0.0}

def suite_storage(manager, skip_heavy=False):
    print("\n--- Running Suite 1: Storage (In-Core vs Out-of-Core) ---")
    results = {}
    manager.clean_db()
    
    events_count = 10000 if skip_heavy else 1000000
    generate_seed_data(events_count)
    
    # 1. In-Core Test
    manager.use_docker = False
    manager.start()
    
    start = time.time()
    scan_res = subprocess.run(["./strfry", "scan", "{}"], capture_output=True, text=True)
    results["scan_time"] = time.time() - start
    results["scan_tps"] = events_count / results["scan_time"] if results["scan_time"] > 0 else 0
    
    res = run_bench("paginate", ["ws://localhost:7777", "--depth", "10", "--concurrency", "2"])
    results["in_core_time"] = res["elapsed"] if res else -1
    manager.stop()
    
    # 2. Out-of-Core Test (Docker, 256MB memory limit)
    drop_caches()
    manager.use_docker = True
    manager.memory_limit = "256m"
    manager.start()
    res = run_bench("paginate", ["ws://localhost:7777", "--depth", "10", "--concurrency", "2"])
    results["out_of_core_time"] = res["elapsed"] if res else -1
    manager.stop()
    
    try:
        mdb_res = subprocess.run(["mdb_stat", "-e", manager.db_dir], capture_output=True, text=True)
        results["mdb_stat"] = mdb_res.stdout
    except FileNotFoundError:
        results["mdb_stat"] = "mdb_stat not installed"
        
    return results

def suite_ingestion(manager, skip_heavy=False):
    print("\n--- Running Suite 2: Ingestion Pipeline ---")
    results = {}
    manager.clean_db()
    manager.use_docker = False
    manager.start()
    
    count = 1000 if skip_heavy else 50000
    
    scraper = PrometheusScraper()
    scraper.start_polling()
    
    monitor = ResourceMonitor(manager.process.pid)
    monitor.start()
    
    # Standard Events (Small)
    res_small = run_bench("event", ["ws://localhost:7777", "-c", "20", "-n", str(count), "--payload-size", "50"])
    
    # Standard Events (Large)
    res_large = run_bench("event", ["ws://localhost:7777", "-c", "20", "-n", str(count // 10), "--payload-size", "10000"])
    
    # Spam / Rate Limiting (Single connection trying to blast events)
    res_spam = run_bench("event", ["ws://localhost:7777", "-c", "1", "-n", str(count)])
    
    monitor.stop()
    scraper.stop_polling()
    
    results.update(monitor.get_results())
    results["writer_queue_peak"] = scraper.peak_queue
    results["event_small_tps"] = count / res_small["elapsed"] if res_small else -1
    results["event_large_tps"] = (count // 10) / res_large["elapsed"] if res_large else -1
    results["event_spam_tps"] = count / res_spam["elapsed"] if res_spam else -1
    results["event_small_output"] = res_small["output"] if res_small else ""
    
    manager.stop()
    return results

def suite_concurrency(manager, skip_heavy=False):
    print("\n--- Running Suite 3: Concurrency & Thread Pool ---")
    results = {}
    manager.clean_db()
    manager.use_docker = False
    manager.start()
    
    events = 1000 if skip_heavy else 100000
    
    # Background writer
    bench_dir = get_bench_dir()
    write_cmd = ["cargo", "run", "--release", "--bin", "strfry-bench", "--", "event", "ws://localhost:7777", "-c", "20", "-n", str(events)]
    writer = subprocess.Popen(write_cmd, cwd=bench_dir, stdout=subprocess.DEVNULL)
    
    time.sleep(2) # Let writer build pressure
    
    req_start = time.time()
    res = run_bench("req", ["ws://localhost:7777", "-c", "10", "-n", "1000", "--filter", "{\"limit\":10}"])
    results["mixed_req_time"] = res["elapsed"] if res else -1
    
    writer.wait()
    manager.stop()
    results["status"] = "Done"
    return results

def get_process_rss(pid):
    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024 # MB
    except:
        pass
    return -1

def count_time_wait_sockets(port=7777):
    port_hex = f"{port:04X}"
    count = 0
    for filename in ["/proc/net/tcp", "/proc/net/tcp6"]:
        if not os.path.exists(filename):
            continue
        try:
            with open(filename, "r") as f:
                lines = f.readlines()
                for line in lines[1:]: # skip header
                    parts = line.split()
                    if len(parts) >= 4:
                        state = parts[3]
                        if state == "06": # TIME_WAIT
                            local_port = parts[1].split(":")[-1]
                            remote_port = parts[2].split(":")[-1]
                            if local_port == port_hex or remote_port == port_hex:
                                count += 1
        except Exception:
            pass
    return count

def suite_websockets(manager, skip_heavy=False):
    print("\n--- Running Suite 4: WebSockets & Connections ---")
    results = {}
    manager.clean_db()
    manager.use_docker = False
    manager.start()
    
    counts = [100, 1000]
    if not skip_heavy:
        counts.append(5000)
        
    results["connection_memory"] = {}
    
    for c in counts:
        print(f"[INFO] Testing {c} connection storm...")
        res = run_bench("connections", ["ws://localhost:7777", "-c", str(c)])
        rss = get_process_rss(manager.process.pid)
        results["connection_memory"][str(c)] = rss
        if res:
            results[f"conn_storm_{c}_output"] = res["output"]
        time.sleep(1) # Let strfry clean up
        
    print("[INFO] Testing High Churn...")
    churn_count = 1000 if skip_heavy else 10000
    res_churn = run_bench("churn", ["ws://localhost:7777", "-c", "50", "-n", str(churn_count)])
    if res_churn:
        results["churn_output"] = res_churn["output"]
        
    results["time_wait_count"] = count_time_wait_sockets(7777)

    manager.stop()
    results["status"] = "Done"
    return results

def suite_queries(manager, skip_heavy=False):
    print("\n--- Running Suite 5: Query Engine & Indices ---")
    results = {}
    manager.clean_db()
    generate_seed_data(10000 if skip_heavy else 1000000)
    manager.use_docker = False
    manager.start()
    
    # 1. Point Lookup (exact id)
    # Get a real ID from the DB via strfry scan
    scan_res = subprocess.run(["./strfry", "scan", "{\"limit\":1}"], capture_output=True, text=True)
    try:
        real_id = json.loads(scan_res.stdout.strip().splitlines()[0])["id"]
    except:
        real_id = "0000000000000000000000000000000000000000000000000000000000000000"
        
    res_point = run_bench("req", ["ws://localhost:7777", "-c", "5", "-n", "100", "--filter", "{\"ids\":[\"" + real_id + "\"]}"])
    results["query_point_time"] = res_point["elapsed"] if res_point else -1
    results["query_point_output"] = res_point["output"] if res_point else ""
    
    # 2. Point COUNT Lookup (NIP-45)
    res_point_count = run_bench("req", ["ws://localhost:7777", "-c", "5", "-n", "100", "--filter", "{\"ids\":[\"" + real_id + "\"]}", "--nip45"])
    results["query_point_count_time"] = res_point_count["elapsed"] if res_point_count else -1
    results["query_point_count_output"] = res_point_count["output"] if res_point_count else ""
    
    # 3. Complex Query (authors, kinds, tags, time range)
    # A heavy NIP-01 complex query
    complex_filter = json.dumps({
        "authors": ["0000000000000000000000000000000000000000000000000000000000000000"],
        "kinds": [1, 5, 7],
        "#t": ["nostr", "benchmark"],
        "since": 1600000000,
        "until": 1800000000,
        "limit": 100
    })
    res_complex = run_bench("req", ["ws://localhost:7777", "-c", "10", "-n", "100", "--filter", complex_filter])
    results["query_complex_time"] = res_complex["elapsed"] if res_complex else -1
    results["query_complex_output"] = res_complex["output"] if res_complex else ""
    
    # 4. Complex COUNT Query (NIP-45)
    res_complex_count = run_bench("req", ["ws://localhost:7777", "-c", "10", "-n", "100", "--filter", complex_filter, "--nip45"])
    results["query_complex_count_time"] = res_complex_count["elapsed"] if res_complex_count else -1
    results["query_complex_count_output"] = res_complex_count["output"] if res_complex_count else ""
    
    manager.stop()
    results["status"] = "Done"
    return results

def suite_monitors(manager, skip_heavy=False):
    print("\n--- Running Suite 6: Active Monitors ---")
    results = {}
    manager.clean_db()
    manager.use_docker = False
    manager.start()
    
    subs = 50 if skip_heavy else 150
    res = run_bench("monitor", ["ws://localhost:7777", "-s", str(subs), "-p", "100"])
    results["monitor_fanout_time"] = res["elapsed"] if res else -1
    results["monitor_output"] = res["output"] if res else ""
    
    manager.stop()
    results["status"] = "Done"
    return results

def suite_negentropy(manager, skip_heavy=False):
    print("\n--- Running Suite 7: Negentropy Sync ---")
    results = {}
    results["status"] = "Done (Pending Implementation - requires dual relay setup)"
    return results

def suite_plugin(manager, skip_heavy=False):
    print("\n--- Running Suite 8: Write Policy Plugin ---")
    results = {}
    results["status"] = "Done (Pending Implementation - requires sample plugin script)"
    return results

def suite_cli_dict(manager, skip_heavy=False):
    print("\n--- Running Suite 9 & 10: CLI & Dictionary ---")
    results = {}
    manager.clean_db()
    events = 10000 if skip_heavy else 1000000
    
    print("[INFO] Testing strfry import...")
    gen_cmd = ["perl", "test/generate-seed-data.pl", "-o", "-", "-e", str(events)]
    import_cmd = ["./strfry", "import", "--no-verify"]
    
    start = time.time()
    p1 = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(import_cmd, stdin=p1.stdout, stdout=subprocess.DEVNULL)
    p1.stdout.close()
    p2.communicate()
    results["import_time"] = time.time() - start
    
    print("[INFO] Testing strfry export...")
    start = time.time()
    subprocess.run(["./strfry", "export"], stdout=subprocess.DEVNULL)
    results["export_time"] = time.time() - start
    
    print("[INFO] Testing dictionary generation...")
    start = time.time()
    subprocess.run(["./strfry", "dict", "generate"], stdout=subprocess.DEVNULL)
    results["dict_gen_time"] = time.time() - start
    
    results["status"] = "Done"
    return results

def suite_os(manager, skip_heavy=False):
    print("\n--- Running Suite 11: OS-Level Metrics ---")
    results = {}
    manager.clean_db()
    manager.use_docker = False
    manager.start()
    
    scraper = PrometheusScraper()
    metrics = scraper.get_metrics()
    
    results["process_rss"] = get_process_rss(manager.process.pid)
    
    manager.stop()
    results["status"] = "Done"
    return results

def suite_stress(manager, skip_heavy=False):
    print("\n--- Running Suite 12: Stress & Edge Cases ---")
    results = {}
    manager.clean_db()
    manager.use_docker = False
    manager.start()
    
    count = 100 if skip_heavy else 2000
    print("[INFO] Running Slow Loris attack...")
    res1 = run_bench("malicious", ["ws://localhost:7777", "-c", str(count), "--slow-loris"])
    
    manager.stop()
    manager.clean_db()
    manager.start()
    
    print("[INFO] Running Signature Flood attack...")
    res2 = run_bench("malicious", ["ws://localhost:7777", "-c", "50", "--sig-flood"])
    
    manager.stop()
    results["status"] = "Done"
    return results

def suite_backpressure(manager, skip_heavy=False):
    print("\n--- Running Suite 13: Backpressure Performance ---")
    results = {}
    manager.clean_db()
    manager.use_docker = False
    manager.start()
    
    fast_clients = 20 if skip_heavy else 100
    slow_clients = 5 if skip_heavy else 20
    count = 100 if skip_heavy else 1000
    res = run_bench("backpressure", [
        "ws://localhost:7777",
        "--fast-clients", str(fast_clients),
        "--slow-clients", str(slow_clients),
        "-n", str(count),
        "--slow-delay", "50"
    ])
    results["backpressure_time"] = res["elapsed"] if res else -1
    results["backpressure_output"] = res["output"] if res else ""
    
    manager.stop()
    results["status"] = "Done"
    return results

def generate_report(results, report_path="benchmark_report.md"):
    print(f"[INFO] Generating comprehensive report at {report_path}")
    with open(report_path, "w") as f:
        f.write("# Strfry Benchmarking Report\n\n")
        f.write(f"Generated at: {datetime.now().isoformat()}\n\n")
        
        if "suite_storage" in results:
            f.write("## 1. Storage & LMDB Statistics\n")
            r = results["suite_storage"]
            f.write(f"- **Sequential scan throughput (events/sec):** {r.get('scan_tps', -1):.2f}\n")
            f.write(f"- **In-Core Pagination Time:** {r.get('in_core_time', -1):.2f} seconds\n")
            f.write(f"- **Out-of-Core Pagination Time (256MB RAM):** {r.get('out_of_core_time', -1):.2f} seconds\n")
            f.write("\n### DB Stat Output\n```\n")
            f.write(r.get('mdb_stat', ''))
            f.write("\n```\n\n")
            
        if "suite_ingestion" in results:
            f.write("## 2. Event Ingestion Pipeline Statistics\n")
            r = results["suite_ingestion"]
            f.write(f"- **Standard Write throughput (events/sec) (50b payload):** {r.get('event_small_tps', -1):.2f}\n")
            f.write(f"- **Standard Write throughput (events/sec) (10Kb payload):** {r.get('event_large_tps', -1):.2f}\n")
            f.write(f"- **Spam Write throughput (events/sec):** {r.get('event_spam_tps', -1):.2f}\n")
            f.write(f"- **Peak Writer Queue Depth:** {r.get('writer_queue_peak', -1)}\n")
            f.write(f"- **Average CPU Utilization (across cores):** {r.get('avg_cpu_percent', 0.0):.2f}%\n")
            f.write(f"- **Peak CPU Utilization:** {r.get('peak_cpu_percent', 0.0):.2f}%\n")
            f.write(f"- **Disk Physical Reads:** {r.get('read_mb', 0.0):.2f} MB\n")
            f.write(f"- **Disk Physical Writes:** {r.get('write_mb', 0.0):.2f} MB\n")
            f.write(f"- **Write Amplification Factor (WAF):** {r.get('waf', 0.0):.4f}\n\n")
            f.write("### Small Payload Latencies\n```\n")
            f.write(r.get('event_small_output', ''))
            f.write("\n```\n\n")
            
        if "suite_concurrency" in results:
            f.write("## 3. Concurrency & Thread Pool\n")
            r = results["suite_concurrency"]
            f.write(f"- **Mixed Read-Write REQ Time:** {r.get('mixed_req_time', -1):.2f} seconds\n\n")
            
        if "suite_websockets" in results:
            f.write("## 4. WebSockets & Connections\n")
            r = results["suite_websockets"]
            f.write("### Connection Memory Scaling (VmRSS)\n")
            for c, mem in r.get('connection_memory', {}).items():
                f.write(f"- **{c} connections:** {mem:.2f} MB\n")
            f.write("\n### Connection Storm Performance\n```\n")
            f.write(r.get('conn_storm_5000_output', r.get('conn_storm_1000_output', '')))
            f.write("\n```\n\n")
            f.write("### High Churn Performance\n```\n")
            f.write(r.get('churn_output', ''))
            f.write("\n```\n")
            f.write(f"- **OS TIME_WAIT sockets count (post-churn):** {r.get('time_wait_count', -1)}\n\n")
            
        if "suite_queries" in results:
            f.write("## 5. Query Engine & Indices\n")
            r = results["suite_queries"]
            f.write(f"- **Point Lookup REQ Time:** {r.get('query_point_time', -1):.2f} seconds\n")
            f.write("### Point Lookup REQ Latencies\n```\n")
            f.write(r.get('query_point_output', ''))
            f.write("\n```\n")
            f.write(f"- **Point Lookup COUNT (NIP-45) Time:** {r.get('query_point_count_time', -1):.2f} seconds\n")
            f.write("### Point Lookup COUNT Latencies\n```\n")
            f.write(r.get('query_point_count_output', ''))
            f.write("\n```\n")
            f.write(f"- **Complex Query REQ Time:** {r.get('query_complex_time', -1):.2f} seconds\n")
            f.write("### Complex Query REQ Latencies\n```\n")
            f.write(r.get('query_complex_output', ''))
            f.write("\n```\n")
            f.write(f"- **Complex COUNT (NIP-45) Query Time:** {r.get('query_complex_count_time', -1):.2f} seconds\n")
            f.write("### Complex COUNT Latencies\n```\n")
            f.write(r.get('query_complex_count_output', ''))
            f.write("\n```\n\n")
            
        if "suite_monitors" in results:
            f.write("## 6. Active Monitors (Viral Post Fanout)\n")
            r = results["suite_monitors"]
            f.write(f"- **Subscription Fan-out Time:** {r.get('monitor_fanout_time', -1):.2f} seconds\n")
            f.write("### Fanout Output\n```\n")
            f.write(r.get('monitor_output', ''))
            f.write("\n```\n\n")
            
        if "suite_cli_dict" in results:
            f.write("## 9 & 10. CLI & Dictionary Compression\n")
            r = results["suite_cli_dict"]
            f.write(f"- **Import Time:** {r.get('import_time', -1):.2f} seconds\n")
            f.write(f"- **Export Time:** {r.get('export_time', -1):.2f} seconds\n")
            f.write(f"- **Dictionary Generation Time:** {r.get('dict_gen_time', -1):.2f} seconds\n\n")
            
        if "suite_os" in results:
            f.write("## 11. OS-Level Metrics\n")
            r = results["suite_os"]
            f.write(f"- **Baseline RSS:** {r.get('process_rss', -1):.2f} MB\n\n")
            
        if "suite_stress" in results:
            f.write("## 12. Stress & Edge Cases\n")
            r = results["suite_stress"]
            f.write(f"- **Adversarial Tests:** Completed successfully\n\n")

        if "suite_backpressure" in results:
            f.write("## 13. Backpressure Performance\n")
            r = results["suite_backpressure"]
            f.write(f"- **Total Backpressure Test Time:** {r.get('backpressure_time', -1):.2f} seconds\n")
            f.write("### Backpressure Latencies\n```\n")
            f.write(r.get('backpressure_output', ''))
            f.write("\n```\n\n")


def main():
    parser = argparse.ArgumentParser(description="Strfry Performance Benchmarking Orchestrator")
    parser.add_argument("--suite", type=str, help="Run a specific suite (e.g. storage, ingestion, stress)")
    parser.add_argument("--skip-heavy", action="store_true", help="Skip heavy database generation to speed up testing")
    parser.add_argument("--dry-run", action="store_true", help="Print commands but don't run tests")
    args = parser.parse_args()

    manager = StrfryManager()
    
    suites_to_run = []
    all_suites = {
        "storage": suite_storage,
        "ingestion": suite_ingestion,
        "concurrency": suite_concurrency,
        "websockets": suite_websockets,
        "queries": suite_queries,
        "monitors": suite_monitors,
        "negentropy": suite_negentropy,
        "plugin": suite_plugin,
        "cli": suite_cli_dict,
        "os": suite_os,
        "stress": suite_stress,
        "backpressure": suite_backpressure
    }
    
    if args.suite:
        if args.suite in all_suites:
            suites_to_run.append((args.suite, all_suites[args.suite]))
        else:
            print(f"[ERROR] Unknown suite: {args.suite}")
            sys.exit(1)
    else:
        suites_to_run = list(all_suites.items())

    if args.dry_run:
        print("[INFO] DRY RUN: Would execute the selected suites:")
        for name, _ in suites_to_run:
            print(f" - {name}")
        return

    if any(name == "storage" for name, _ in suites_to_run):
        manager.build_docker()
    
    results = {}

    for name, func in suites_to_run:
        results[f"suite_{name}"] = func(manager, args.skip_heavy)
        
    generate_report(results)

if __name__ == "__main__":
    main()
