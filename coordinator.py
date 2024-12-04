import argparse
import aiohttp
import asyncio
from aiohttp import web
import os

class Coordinator:
    """Manages workers and aggregates results"""
    
    def __init__(self, port: int):
        print(f"Starting coordinator on port {port}")
        self.workers = {}
        self.results = {}
        self.port = port

    def start(self) -> None:
        """Start coordinator server"""
        app = web.Application()
        app.router.add_post('/report', self.handle_report)
        app.router.add_post('/assign', self.handle_assign)
        app.router.add_get('/metrics', self.get_metrics)
        app.router.add_post('/health', self.handle_health)
        web.run_app(app, port=self.port)
        print(f"Starting coordinator on port {self.port}...")
        pass

    async def distribute_work(self, filepath: str) -> None:
        """Split file and assign chunks to workers"""
        if not os.path.exists(filepath):
            print(f"File {filepath} does not exist.")
            return
        file_size = os.path.getsize(filepath)
        chunk_size = file_size // len(self.workers) if self.workers else file_size
        tasks = []

        for i, (worker_id, worker_url) in enumerate(self.workers.items()):
            start = i * chunk_size
            size = chunk_size if i < len(self.workers) - 1 else file_size - start
            tasks.append(self.send_work_to_worker(worker_id, worker_url, filepath, start, size))
        await asyncio.gather(*tasks)
    async def send_work_to_worker(self, worker_id: str, worker_url: str, filepath: str, start: int, size: int) -> None:
        """Send work to a specific worker"""
        async with aiohttp.ClientSession() as session:
            await session.post(f"{worker_url}/process", json={
                "filepath": filepath,
                "start": start,
                "size": size
            })

    async def handle_worker_failure(self, worker_id: str) -> None:
        """Reassign work from failed worker"""
        print(f"Handling failure for worker {worker_id}")
        # Logic to reassign work from the failed worker would go here

    async def handle_report(self, request: web.Request) -> web.Response:
        """Handle metrics reports from workers"""
        data = await request.json()
        worker_id = data['worker_id']
        metrics = data['metrics']
        self.results[worker_id] = metrics
        return web.Response(text="Metrics received")

    async def handle_health(self, request: web.Request) -> web.Response:
        """Health check endpoint for workers"""
        data = await request.json()
        worker_id = data['worker_id']
        print(f"Received health check from {worker_id}")
        return web.Response(text="Worker is healthy")

    async def get_metrics(self, request: web.Request) -> web.Response:
        """Get aggregated metrics from all workers"""
        return web.json_response(self.results)
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    coordinator.start()
