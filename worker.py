import argparse
import aiohttp
import asyncio
import aiofiles
from log_entry import LogEntry
from datetime import datetime
from typing import Dict


class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, port: int, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port
    
    def start(self) -> None:
        """Start worker server"""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        asyncio.run(self.run_server())
    async def run_server(self):
        """Run the worker server to listen for tasks"""
        app = aiohttp.web.Application()
        app.router.add_post('/process', self.handle_process)
        app.router.add_get('/health', self.handle_health)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        print(f"Worker {self.worker_id} is running...")
        while True:
            await asyncio.sleep(3600)  # Keep the server running

    async def handle_process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """Handle processing requests from the coordinator"""
        data = await request.json()
        filepath = data['filepath']
        start = data['start']
        size = data['size']
        metrics = await self.process_chunk(filepath, start, size)
        await self.send_results(metrics)
        return aiohttp.web.Response(text="Chunk processed")
        

    async def process_chunk(self, filepath: str, start: int, size: int) -> dict:
        """Process a chunk of log file and return metrics"""
         metrics = {
            "error_count": 0,
            "total_response_time": 0,
            "request_count": 0
        }
        async with aiofiles.open(filepath, mode='r') as f:
            await f.seek(start)
            chunk = await f.read(size)
            lines = chunk.splitlines()
            
            for line in lines:
                if line.strip():
                    log_entry = self.parse_log_line(line)
                    if log_entry:
                        metrics["request_count"] += 1
                        if log_entry.level == "ERROR":
                            metrics["error_count"] += 1
                        if "Request processed in" in log_entry.message:
                            response_time = int(log_entry.message.split("in")[1].replace("ms", "").strip())
                            metrics["total_response_time"] += response_time
        if metrics["request_count"] > 0:
            metrics["average_response_time"] = metrics["total_response_time"] / metrics["request_count"]
        else:
            metrics["average_response_time"] = 0

        return metrics
def parse_log_line(self, line: str) -> LogEntry:
        """Parse a log line into a LogEntry object"""
        try:
            parts = line.split(" ", 3)
            timestamp = datetime.strptime(parts[0] + " " + parts[1], "%Y-%m-%d %H:%M:%S.%f")
            level = parts[2]
            message = parts[3]
            return LogEntry(timestamp, level, message)
        except Exception as e:
            return None

    async def send_results(self, metrics: Dict) -> None:
        """Send processed metrics to the coordinator"""
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.coordinator_url}/report", json={"worker_id": self.worker_id, "metrics": metrics}) as response:
                await response.text()

    async def handle_health(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """Health check endpoint"""
        return aiohttp.web.Response(text="Worker is healthy")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(port=args.port, worker_id=args.id, coordinator_url=args.coordinator)
    worker.start()
