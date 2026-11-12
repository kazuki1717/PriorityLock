import asyncio;
import heapq;



class PriorityLock:
    """
    優先度付きの非同期排他ロック。
    priority: 数値が小さいほど高優先度（例: 0 = ユーザー、10 = バックグラウンド）
    """
    def __init__(self):
        self._locked = False;

        self._waiters = [];
        self._counter = 0;

    async def acquire(self, priority = 10):
        # ロックが空いていれば、すぐにロックを取得する
        if (not self._locked):
            self._locked = True
            return;

        # ロックが取られていたら、待機リストに加わる
        loop = asyncio.get_running_loop();
        fut = loop.create_future();

        heapq.heappush(self._waiters, (priority, self._counter, fut));
        self._counter += 1;

        # 自分の番が来るまで待機する
        await fut;
        self._locked = True;
    

    def release(self):
        if (not self._locked):
            raise RuntimeError("このロックを所有していないため解放できません。")
    
        if (self._waiters):
            # 待機者がいたら、一番優先度が高い（数値が最小の）タスクのFutureを起こす
            _, _, fut = heapq.heappop(self._waiters);
            fut.set_result(None);
        else:
            # 待機者がいなければ、ロックを空ける
            self._locked = False;

    async def __aenter__(self):
        # `async with lock` として使う場合のデフォルト優先度
        await self.acquire(priority=10)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()



    class Session:
        """
        `async with lock(priority=...)` という書き方を実現するための補助クラス
        """

        def __init__(self, lock: "PriorityLock", priority: int):
            self.lock = lock;
            self.priority = priority;

        async def __aenter__(self):
            await self.lock.acquire(priority=self.priority);
            return self;

        async def __aexit__(self, exc_type, exc, tb):
            self.lock.release();

    def lock(self, priority = 10):
        # 優先度を指定して Proxy オブジェクトを返す
        return self.Session(self, priority);



class UltimatePriorityLock:
    """
優先度付きの非同期排他ロック。

パラメンター：
- priority: 数値が小さいほど高優先度（例: 0 = ユーザー、10 = バックグラウンド）


使い方：

```
lock = UltimatePriorityLock()

session = lock.lock(priority = 10)
while (True):
    with session:
        try:
            print("do something...")
        except asyncio.CancelledError:
            continue


```



"""
    def __init__(self):
        self._locked = False;

        self._current_priority = None;
        self._current_task = None;

        self._waiters = [];
        self._counter = 0;

    async def acquire(self, priority = 10, order: int = None) -> int:# 1. ロックが空いていれば即座に取得
        current_task = asyncio.current_task();

        # 自分の順番を取得する
        if (order is None):
            order = self._counter;
            self._counter += 1;


        # ロックが空いていれば、すぐにロックを取得する
        if (not self._locked):
            self._locked = True
            self._current_priority = priority
            self._current_task = current_task;
            return order;

        
        # ロックが取られていたら、待機リストに加わる
        loop = asyncio.get_running_loop();
        fut = loop.create_future();
        
        heapq.heappush(self._waiters, (priority, order, fut));

        # 3. 【ここが重要！】もっと高い優先度の人が来た場合、今の人を中断させる
        if self._current_priority is not None and priority < self._current_priority:
            if self._current_task and not self._current_task.done():
                self._current_task.cancel();

        # 自分の番が来るまで待機する
        await fut;
        self._locked = True;
        self._current_priority = priority;
        self._current_task = current_task;
        return order;
    

    def release(self):
        if (not self._locked or self._current_task != asyncio.current_task()):
            raise RuntimeError("このロックを所有していないため解放できません。")
    
        if (self._waiters):
            # 待機者がいたら、一番優先度が高い（数値が最小の）タスクのFutureを起こす
            _, _, fut = heapq.heappop(self._waiters);
            fut.set_result(None);
        else:
            # 待機者がいなければ、ロックを空ける
            self._locked = False;
            self._current_priority = None;
            self._current_task = None;




    async def __aenter__(self):
        # コンテキストマネージャーとして使う場合のデフォルト優先度
        await self.acquire(priority=10)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()




    class Session:
        """
        async with lock(priority=...) という書き方を実現するための補助クラス
        """

        def __init__(self, lock: "PriorityLock", priority: int):
            self.lock = lock;
            self.priority = priority;
            self.order = None;

        async def __aenter__(self):
            self.order = await self.lock.acquire(
                priority = self.priority,
                order = self.order
            );
            return self;

        async def __aexit__(self, exc_type, exc, tb):
            self.lock.release();

    def lock(self, priority = 10):
        # 優先度を指定して Proxy オブジェクトを返す
        return self.Session(self, priority);


