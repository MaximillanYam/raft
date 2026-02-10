import asyncio
import random


class RaftTimer: 
    def __init__(self, callback_function, min_timeout, max_timeout): 
        self.callback = callback_function
        self.min_timeout = min_timeout 
        self.max_timeout = max_timeout
        self.task = None
    
    async def _run(self): 
        try: 
            sleep_time = random.uniform(self.min_timeout, self.max_timeout)
            await asyncio.sleep(sleep_time) 
            await self.callback() 
        except asyncio.CancelledError:
            pass
        finally:
            if self.task is asyncio.current_task(): 
                self.task = None

    def reset(self): 
        self.stop() 
        self.start()

    def stop(self): 
        if self.task: 
            self.task.cancel()

    def start(self):
        self.stop()
        self.task = asyncio.create_task(self._run())
        return 

    
        

